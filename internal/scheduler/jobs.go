package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/agent"
	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/opportunity"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/sim"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

func RegisterDefaultJobs(mgr *Manager, cfg *config.Config, db *gorm.DB, marketSvc *market.Service, signalSvc *signal.Service, weatherSvc *weather.Service, chainSvc *chain.Service, simSvc *sim.Service, agentSvc *agent.Service, log *zap.Logger) {
	if mgr == nil || cfg == nil {
		return
	}
	var oppSvc *opportunity.Service
	if db != nil {
		oppSvc = opportunity.NewService(db)
		oppSvc.SetMetrics(mgr.metrics)
	}

	mc := cfg.Scheduler.Jobs.MarketSync
	if mc.Enabled && mc.Interval > 0 && marketSvc != nil {
		mgr.Register(Job{
			Name:      "market_sync",
			Interval:  mc.Interval,
			Timeout:   mc.Timeout,
			Immediate: mc.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				res, err := marketSvc.SyncMarkets(ctx)
				if err != nil {
					return JobResult{}, err
				}
				out := JobResult{Records: make([]FetchRecord, 0, 3)}
				if res.MarketsFetched == 0 {
					out.SkipReason = "no markets fetched from upstream"
					out.Errors = append(out.Errors, JobIssue{
						Code:    errCodeEmptyMarketSet,
						Message: "market sync skipped: upstream returned 0 markets",
						Source:  "market_sync",
						Count:   1,
					})
					out.Records = append(out.Records, FetchRecord{Entity: "markets", Result: "skipped", Count: 1})
				}
				out.Records = append(out.Records,
					FetchRecord{Entity: "markets", Result: "success", Count: res.MarketsUpserted},
					FetchRecord{Entity: "prices", Result: "success", Count: res.PriceSnapshots},
				)
				if len(res.Errors) > 0 {
					out.Records = append(out.Records, FetchRecord{Entity: "market_sync_errors", Result: "error", Count: len(res.Errors)})
					out.Warnings = append(out.Warnings, JobIssue{
						Code:    errCodeMarketSyncPartial,
						Message: fmt.Sprintf("market sync finished with %d warnings", len(res.Errors)),
						Source:  "market_sync",
						Count:   len(res.Errors),
					})
				}
				out.Freshness = map[string]float64{"markets": 0}
				return out, nil
			},
		})
	}

	spc := cfg.Scheduler.Jobs.MarketSpecFill
	if spc.Enabled && spc.Interval > 0 && signalSvc != nil {
		mgr.Register(Job{
			Name:      "market_spec_fill",
			Interval:  spc.Interval,
			Timeout:   spc.Timeout,
			Immediate: spc.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				limit := spc.Limit
				if limit <= 0 {
					limit = 300
				}
				onlyMissing := spc.OnlyMissing
				res, err := signalSvc.ResolveMarketCities(ctx, signal.ResolveCitiesOptions{
					Limit:       limit,
					OnlyMissing: onlyMissing,
				})
				if err != nil {
					return JobResult{}, err
				}
				out := JobResult{
					Records: []FetchRecord{
						{Entity: "markets_spec_processed", Result: "success", Count: res.Processed},
						{Entity: "markets_spec_ready", Result: "success", Count: res.SpecReady},
						{Entity: "markets_city_resolved", Result: "success", Count: res.Resolved},
						{Entity: "markets_city_unresolved", Result: "skipped", Count: res.Unresolved},
					},
					Freshness: map[string]float64{"markets": 0},
				}
				if res.Processed == 0 {
					out.SkipReason = "no eligible markets for spec fill"
					out.Errors = append(out.Errors, JobIssue{
						Code:    "spec_fill_no_eligible_markets",
						Message: "market spec fill skipped: no eligible markets",
						Source:  "market_spec_fill",
						Count:   1,
					})
				} else if res.SpecReady == 0 {
					// Empty-run success is misleading for ops; surface as skipped_no_input.
					out.SkipReason = "spec_ready remained zero"
					out.Errors = append(out.Errors, JobIssue{
						Code:    "spec_fill_no_progress",
						Message: "market spec fill skipped: processed markets but spec_ready stayed zero",
						Source:  "market_spec_fill",
						Count:   res.Processed,
					})
				}
				if len(res.Errors) > 0 {
					out.Warnings = append(out.Warnings, JobIssue{
						Code:    "market_spec_fill_partial",
						Message: fmt.Sprintf("market spec fill finished with %d errors", len(res.Errors)),
						Source:  "market_spec_fill",
						Count:   len(res.Errors),
					})
				}
				if oppSvc != nil {
					readyChanged := 0
					for _, item := range res.Items {
						prev := strings.TrimSpace(strings.ToLower(item.PrevStatus))
						next := strings.TrimSpace(strings.ToLower(item.Status))
						if prev == next {
							continue
						}
						if next == "ready" {
							readyChanged++
							_ = oppSvc.Emit(ctx, opportunity.EmitInput{
								EventType: "spec_ready_changed",
								MarketID:  item.MarketID,
								Severity:  "info",
								DedupKey:  fmt.Sprintf("spec_ready_changed:%s:%s", item.MarketID, next),
								Payload: map[string]any{
									"prev_status": prev,
									"next_status": next,
									"city":        item.City,
									"source":      item.Source,
									"confidence":  item.Confidence,
								},
								OccurredAt: time.Now().UTC(),
							})
						}
					}
					if readyChanged > 0 {
						out.Records = append(out.Records, FetchRecord{Entity: "spec_ready_changed_events", Result: "success", Count: readyChanged})
					}
				}
				if db != nil && mgr != nil && mgr.metrics != nil {
					var rows []model.Market
					_ = db.WithContext(ctx).Model(&model.Market{}).
						Select("id, market_type, question, city, spec_status").
						Where("is_active = ?", true).
						Find(&rows).Error
					var supportedTotal int64
					var specReady int64
					var cityMissing int64
					for _, row := range rows {
						if !signal.IsSupportedMarketForSignal(row) {
							continue
						}
						supportedTotal++
						if strings.EqualFold(strings.TrimSpace(row.SpecStatus), "ready") {
							specReady++
						}
						if strings.TrimSpace(row.City) == "" {
							cityMissing++
						}
					}
					successRate := 0.0
					if supportedTotal > 0 {
						successRate = float64(specReady) / float64(supportedTotal)
					}
					mgr.metrics.SetMarketSpecCoverage(float64(specReady), float64(cityMissing), successRate)
				}
				return out, nil
			},
		})
	}

	wc := cfg.Scheduler.Jobs.WeatherForecast
	if wc.Enabled && wc.Interval > 0 && weatherSvc != nil && db != nil {
		mgr.Register(Job{
			Name:      "weather_forecast",
			Interval:  wc.Interval,
			Timeout:   wc.Timeout,
			Immediate: wc.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				cities, err := loadActiveCities(ctx, db)
				if err != nil {
					return JobResult{}, err
				}
				if len(cities) == 0 {
					return JobResult{
						SkipReason: "no active market cities",
						Errors: []JobIssue{
							{
								Code:    errCodeEmptyCitySet,
								Message: "weather forecast skipped: no active cities from markets table",
								Source:  "weather_forecast",
								Count:   1,
							},
						},
					}, nil
				}

				concurrency := wc.CityConcurrency
				if concurrency <= 0 {
					concurrency = 4
				}
				days := wc.Days
				if days <= 0 {
					days = 7
				}
				source := strings.TrimSpace(wc.Source)
				if source == "" {
					source = "all"
				}

				var mu sync.Mutex
				successCity := 0
				failedCity := 0
				forecastRows := 0
				providerErrors := 0

				g, gctx := errgroup.WithContext(ctx)
				g.SetLimit(concurrency)
				for _, city := range cities {
					city := city
					g.Go(func() error {
						resp, callErr := weatherSvc.GetForecast(gctx, weather.ForecastRequest{
							Location: city,
							Source:   source,
							Days:     days,
						})
						mu.Lock()
						defer mu.Unlock()
						if callErr != nil {
							failedCity++
							return nil
						}
						successCity++
						forecastRows += len(resp.Forecasts)
						providerErrors += len(resp.Errors)
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					return JobResult{}, err
				}
				if successCity == 0 && failedCity > 0 {
					return JobResult{}, fmt.Errorf("weather forecast failed for all %d cities", failedCity)
				}
				if oppSvc != nil && successCity > 0 {
					for _, city := range cities {
						c := strings.TrimSpace(strings.ToLower(city))
						if c == "" {
							continue
						}
						var marketIDs []string
						if err := db.WithContext(ctx).Table("markets").
							Select("id").
							Where("is_active = ? AND LOWER(COALESCE(city,'')) = ?", true, c).
							Pluck("id", &marketIDs).Error; err != nil {
							continue
						}
						for _, mid := range marketIDs {
							_ = oppSvc.EmitMarketEvent(ctx, "forecast_update", mid, "info", map[string]any{"city": c})
						}
					}
				}

				out := JobResult{
					Records: []FetchRecord{
						{Entity: "cities", Result: "success", Count: successCity},
						{Entity: "cities", Result: "error", Count: failedCity},
						{Entity: "forecasts", Result: "success", Count: forecastRows},
						{Entity: "provider_errors", Result: "error", Count: providerErrors},
					},
					Freshness: map[string]float64{"forecasts": 0},
				}
				if providerErrors > 0 {
					out.Warnings = append(out.Warnings, JobIssue{
						Code:    errCodeUpstream5xx,
						Message: fmt.Sprintf("weather providers returned %d non-fatal errors", providerErrors),
						Source:  "weather_forecast",
						Count:   providerErrors,
					})
				}
				return out, nil
			},
		})
	}

	cc := cfg.Scheduler.Jobs.ChainScan
	if cc.Enabled && cc.Interval > 0 && chainSvc != nil {
		chainInterval := cc.Interval
		if chainInterval < 30*time.Second {
			if log != nil {
				log.Warn("chain_scan interval too low, raised to protect upstream rpc",
					zap.Duration("configured_interval", cc.Interval),
					zap.Duration("effective_interval", 30*time.Second))
			}
			chainInterval = 30 * time.Second
		}
		if log != nil {
			log.Info("register chain_scan scheduler job",
				zap.Duration("configured_interval", cc.Interval),
				zap.Duration("effective_interval", chainInterval),
				zap.Bool("immediate", cc.Immediate))
		}
		if strings.TrimSpace(cfg.Chain.RPCURL) == "" && log != nil {
			log.Info("register chain_scan scheduler job with skipped_no_input fallback: chain.rpc_url is empty")
		}
		mgr.Register(Job{
			Name:      "chain_scan",
			Interval:  chainInterval,
			Timeout:   cc.Timeout,
			Immediate: cc.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				if strings.TrimSpace(cfg.Chain.RPCURL) == "" {
					return JobResult{
						SkipReason: "chain.rpc_url is empty",
						Errors: []JobIssue{
							{
								Code:    errCodeChainRPCEmpty,
								Message: "chain scan skipped: chain.rpc_url is empty",
								Source:  "chain_scan",
								Count:   1,
							},
						},
					}, nil
				}
				res, err := chainSvc.Scan(ctx, chain.ScanOptions{
					LookbackBlocks: cc.LookbackBlocks,
					MaxTx:          cc.MaxTx,
				})
				if err != nil {
					return JobResult{}, err
				}
				if oppSvc != nil && res.InsertedTrades > 0 {
					_ = oppSvc.Emit(ctx, opportunity.EmitInput{
						EventType: "chain_activity_spike",
						Severity:  "info",
						Payload: map[string]any{
							"inserted_trades":  res.InsertedTrades,
							"observed_tx":      res.ObservedTx,
							"duplicate_trades": res.DuplicateTrades,
						},
						OccurredAt: time.Now().UTC(),
					})
				}
				return JobResult{
					Records: []FetchRecord{
						{Entity: "observed_logs", Result: "success", Count: res.ObservedLogs},
						{Entity: "observed_tx", Result: "success", Count: res.ObservedTx},
						{Entity: "trades_inserted", Result: "success", Count: res.InsertedTrades},
						{Entity: "trades_duplicate", Result: "skipped", Count: res.DuplicateTrades},
					},
					Freshness: map[string]float64{"chain": 0},
				}, nil
			},
		})
	}

	sc := cfg.Scheduler.Jobs.SimCycle
	if sc.Enabled && sc.Interval > 0 && simSvc != nil {
		mgr.Register(Job{
			Name:      "sim_cycle",
			Interval:  sc.Interval,
			Timeout:   sc.Timeout,
			Immediate: sc.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				res, err := simSvc.RunCycleWithOptions(ctx, 0, sim.RunCycleOptions{
					SkipMarketSync: true,
				})
				if err != nil {
					return JobResult{}, err
				}
				out := JobResult{Records: []FetchRecord{
					{Entity: "markets_synced", Result: "success", Count: res.MarketsSynced},
					{Entity: "signals_generated", Result: "success", Count: res.SignalsGenerated},
					{Entity: "trades_placed", Result: "success", Count: res.TradesPlaced},
					{Entity: "trades_settled", Result: "success", Count: res.TradesSettled},
				}}
				if len(res.Errors) > 0 {
					out.Records = append(out.Records, FetchRecord{Entity: "sim_errors", Result: "error", Count: len(res.Errors)})
				}
				out.Freshness = map[string]float64{"signals": 0}
				return out, nil
			},
		})
	}

	oc := cfg.Scheduler.Jobs.OpportunityCycle
	if oc.Enabled && oc.Interval > 0 && oppSvc != nil {
		mgr.Register(Job{
			Name:      "opportunity_cycle",
			Interval:  oc.Interval,
			Timeout:   oc.Timeout,
			Immediate: oc.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				cycleStarted := time.Now().UTC()
				cycleID := fmt.Sprintf("opcycle-%s", cycleStarted.Format("20060102T150405Z"))
				cycleRow := model.AgentEventCycle{
					CycleID:   cycleID,
					RunMode:   strings.TrimSpace(oc.AgentRunMode),
					Status:    "running",
					StartedAt: cycleStarted,
				}
				if cycleRow.RunMode == "" {
					cycleRow.RunMode = "event_driven"
				}
				if db != nil {
					_ = db.WithContext(ctx).Create(&cycleRow).Error
				}
				finalized := false
				finalizeCycle := func(status, code, msg string, eventsConsumed, candidatesSelected, signalsGenerated int, firstEventAt, firstSignalAt *time.Time) {
					if finalized {
						return
					}
					if db == nil || cycleRow.ID == 0 {
						return
					}
					finished := time.Now().UTC()
					_ = db.WithContext(ctx).Model(&model.AgentEventCycle{}).Where("id = ?", cycleRow.ID).Updates(map[string]any{
						"status":              status,
						"error_code":          code,
						"error_message":       msg,
						"events_consumed":     eventsConsumed,
						"candidates_selected": candidatesSelected,
						"signals_generated":   signalsGenerated,
						"first_event_at":      firstEventAt,
						"first_signal_at":     firstSignalAt,
						"finished_at":         finished,
					}).Error
					finalized = true
				}
				expiryEvents := 0
				expiryEmitErr := error(nil)
				if db != nil {
					n, emitErr := emitExpiryWindowEvents(ctx, db, oppSvc, cycleStarted)
					if emitErr != nil {
						expiryEmitErr = emitErr
					}
					expiryEvents = n
				}

				consumed, firstEventAt, err := oppSvc.ConsumePending(ctx, oc.ConsumeLimit)
				if err != nil {
					finalizeCycle("error", "consume_pending_failed", err.Error(), 0, 0, 0, nil, nil)
					return JobResult{}, err
				}
				decayed, err := oppSvc.DecayCandidates(ctx, oc.DecayWatchAfter, oc.DecayCooldownAfter)
				if err != nil {
					finalizeCycle("error", "decay_candidates_failed", err.Error(), consumed, 0, 0, firstEventAt, nil)
					return JobResult{}, err
				}
				recomputed, err := oppSvc.RecomputeCandidateScores(ctx, opportunity.ScoreWeights{
					EdgeRefPct:      oc.ScoreEdgeRefPct,
					WeightEdge:      oc.ScoreWeightEdge,
					WeightLiquidity: oc.ScoreWeightLiquidity,
					WeightFreshness: oc.ScoreWeightFreshness,
					WeightExpiry:    oc.ScoreWeightExpiry,
					FreshnessMaxAge: oc.ScoreFreshnessMaxAge,
				})
				if err != nil {
					finalizeCycle("error", "recompute_candidate_scores_failed", err.Error(), consumed, 0, 0, firstEventAt, nil)
					return JobResult{}, err
				}
				out := JobResult{
					Records: []FetchRecord{
						{Entity: "events_consumed", Result: "success", Count: consumed},
						{Entity: "candidates_decayed", Result: "success", Count: decayed},
						{Entity: "candidates_recomputed", Result: "success", Count: recomputed},
						{Entity: "expiry_window_events", Result: "success", Count: expiryEvents},
					},
					Freshness: map[string]float64{"events": 0},
				}
				if expiryEmitErr != nil {
					out.Warnings = append(out.Warnings, JobIssue{
						Code:    "opportunity_expiry_window_emit_failed",
						Message: expiryEmitErr.Error(),
						Source:  "opportunity_cycle",
						Count:   1,
					})
				}
				if oc.TriggerAgent && agentSvc != nil {
					ids, topErr := oppSvc.SelectCandidateMarketIDs(ctx, oc.AgentCandidateLimit, oc.AgentMaxHotMarkets)
					if topErr != nil {
						out.Warnings = append(out.Warnings, JobIssue{
							Code:    "opportunity_top_candidates_failed",
							Message: topErr.Error(),
							Source:  "opportunity_cycle",
							Count:   1,
						})
						finalizeCycle("degraded", "top_candidates_failed", topErr.Error(), consumed, 0, 0, firstEventAt, nil)
						return out, nil
					}
					minCandidates := oc.AgentMinCandidates
					if minCandidates <= 0 {
						minCandidates = 1
					}
					minConsumed := oc.AgentMinConsumedEvents
					if minConsumed <= 0 {
						minConsumed = 1
					}
					if consumed < minConsumed {
						out.Records = append(out.Records, FetchRecord{Entity: "agent_cycles_triggered", Result: "skipped", Count: 1})
						out.Warnings = append(out.Warnings, JobIssue{
							Code:    "opportunity_agent_gate_events",
							Message: fmt.Sprintf("skip agent trigger: consumed=%d below min_consumed=%d", consumed, minConsumed),
							Source:  "opportunity_cycle",
							Count:   1,
						})
						finalizeCycle("success", "", "", consumed, len(ids), 0, firstEventAt, nil)
						return out, nil
					}
					if len(ids) < minCandidates {
						out.Records = append(out.Records, FetchRecord{Entity: "agent_cycles_triggered", Result: "skipped", Count: 1})
						out.Warnings = append(out.Warnings, JobIssue{
							Code:    "opportunity_agent_gate_candidates",
							Message: fmt.Sprintf("skip agent trigger: candidates=%d below min_candidates=%d", len(ids), minCandidates),
							Source:  "opportunity_cycle",
							Count:   1,
						})
						finalizeCycle("success", "", "", consumed, len(ids), 0, firstEventAt, nil)
						return out, nil
					}
					if len(ids) > 0 {
						marketLimit := len(ids)
						if cfg.Scheduler.Jobs.AgentCycle.MarketLimit > 0 && marketLimit > cfg.Scheduler.Jobs.AgentCycle.MarketLimit {
							marketLimit = cfg.Scheduler.Jobs.AgentCycle.MarketLimit
						}
						runMode := strings.TrimSpace(oc.AgentRunMode)
						if runMode == "" {
							runMode = cfg.Scheduler.Jobs.AgentCycle.RunMode
						}
						actx := signal.WithCandidateMarketIDs(ctx, ids)
						actx = signal.WithSignalWriteLimit(actx, oc.MaxSignalWritesPerCycle)
						var signalBefore *time.Time
						if db != nil {
							var t time.Time
							if err := db.WithContext(ctx).Table("signals").Select("MAX(created_at)").Scan(&t).Error; err == nil && !t.IsZero() {
								tmp := t.UTC()
								signalBefore = &tmp
							}
						}
						res, runErr := agentSvc.RunCycle(actx, agent.CycleOptions{
							RunMode:             runMode,
							TradeEnabled:        cfg.Scheduler.Jobs.AgentCycle.TradeEnabled,
							MaxToolCalls:        cfg.Scheduler.Jobs.AgentCycle.MaxToolCalls,
							MaxExternalRequests: cfg.Scheduler.Jobs.AgentCycle.MaxExternalRequests,
							MaxTokensPerCycle:   cfg.Scheduler.Jobs.AgentCycle.MaxTokensPerCycle,
							MaxCycleDurationSec: int(cfg.Scheduler.Jobs.AgentCycle.MaxCycleDuration.Seconds()),
							MemoryWindow:        cfg.Scheduler.Jobs.AgentCycle.MemoryWindow,
							MarketLimit:         marketLimit,
						})
						if runErr != nil {
							out.Warnings = append(out.Warnings, JobIssue{
								Code:    "opportunity_agent_cycle_failed",
								Message: runErr.Error(),
								Source:  "opportunity_cycle",
								Count:   1,
							})
							if mgr != nil && mgr.metrics != nil {
								mgr.metrics.SetHotMarketHitRate(0)
							}
							finalizeCycle("degraded", "agent_cycle_failed", runErr.Error(), consumed, len(ids), 0, firstEventAt, nil)
						} else {
							signalCount := 0
							for _, rec := range res.Records {
								if rec.Entity == "signals_generated" && rec.Count > 0 {
									signalCount = rec.Count
									break
								}
							}
							out.Records = append(out.Records,
								FetchRecord{Entity: "agent_cycles_triggered", Result: "success", Count: 1},
								FetchRecord{Entity: "agent_candidates_selected", Result: "success", Count: len(ids)},
								FetchRecord{Entity: "agent_signals_generated", Result: "success", Count: signalCount},
							)
							if mgr != nil && mgr.metrics != nil {
								hitRate := 0.0
								if len(ids) > 0 {
									hitRate = float64(signalCount) / float64(len(ids))
								}
								mgr.metrics.SetHotMarketHitRate(hitRate)
							}
							var firstSignalAt *time.Time
							if db != nil && signalCount > 0 {
								var t time.Time
								q := db.WithContext(ctx).Table("signals").Select("MIN(created_at)")
								if signalBefore != nil {
									q = q.Where("created_at > ?", *signalBefore)
								} else {
									q = q.Where("created_at >= ?", cycleStarted)
								}
								if err := q.Scan(&t).Error; err == nil && !t.IsZero() {
									tmp := t.UTC()
									firstSignalAt = &tmp
								}
							}
							if mgr != nil && mgr.metrics != nil && firstEventAt != nil && firstSignalAt != nil && firstSignalAt.After(*firstEventAt) {
								mgr.metrics.ObserveTriggerToSignalLatency(firstSignalAt.Sub(*firstEventAt))
							}
							finalizeCycle("success", "", "", consumed, len(ids), signalCount, firstEventAt, firstSignalAt)
						}
					}
				}
				finalizeCycle("success", "", "", consumed, 0, 0, firstEventAt, nil)
				return out, nil
			},
		})
	}

	ac := cfg.Scheduler.Jobs.AgentCycle
	if ac.Enabled && ac.Interval > 0 && agentSvc != nil {
		mgr.Register(Job{
			Name:      "agent_cycle",
			Interval:  ac.Interval,
			Timeout:   ac.Timeout,
			Immediate: ac.Immediate,
			Run: func(ctx context.Context) (JobResult, error) {
				res, err := agentSvc.RunCycle(ctx, agent.CycleOptions{
					RunMode:             ac.RunMode,
					TradeEnabled:        ac.TradeEnabled,
					MaxToolCalls:        ac.MaxToolCalls,
					MaxExternalRequests: ac.MaxExternalRequests,
					MaxTokensPerCycle:   ac.MaxTokensPerCycle,
					MaxCycleDurationSec: int(ac.MaxCycleDuration.Seconds()),
					MemoryWindow:        ac.MemoryWindow,
					MarketLimit:         ac.MarketLimit,
				})
				if err != nil {
					return JobResult{}, err
				}
				out := JobResult{
					Records:   make([]FetchRecord, 0, len(res.Records)+1),
					Freshness: res.Freshness,
				}
				for _, rec := range res.Records {
					out.Records = append(out.Records, FetchRecord{
						Entity: rec.Entity,
						Result: rec.Result,
						Count:  rec.Count,
					})
				}
				out.Records = append(out.Records, FetchRecord{Entity: "llm_calls", Result: "success", Count: res.LLMCalls})
				out.Records = append(out.Records, FetchRecord{Entity: "llm_tokens", Result: "success", Count: res.LLMTokens})
				for _, issue := range res.Errors {
					out.Errors = append(out.Errors, JobIssue{
						Code:    issue.Code,
						Message: issue.Message,
						Source:  issue.Source,
						Count:   issue.Count,
					})
				}
				for _, warn := range res.Warnings {
					out.Warnings = append(out.Warnings, JobIssue{
						Code:    warn.Code,
						Message: warn.Message,
						Source:  warn.Source,
						Count:   warn.Count,
					})
				}
				if res.Status == "skipped_no_input" {
					out.SkipReason = res.SkipReason
				}
				if res.Status == "error" {
					msg := strings.TrimSpace(res.SkipReason)
					if msg == "" && len(res.Errors) > 0 {
						msg = strings.TrimSpace(res.Errors[0].Message)
					}
					if msg == "" {
						msg = "agent cycle failed"
					}
					return out, fmt.Errorf("agent cycle failed: %s", msg)
				}
				return out, nil
			},
		})
	}
}

func loadActiveCities(ctx context.Context, db *gorm.DB) ([]string, error) {
	type marketRow struct {
		City string `gorm:"column:city"`
		// Keep question for fallback extraction when city is empty.
		Question string `gorm:"column:question"`
		// market_type can be unknown in upstream data, so fallback cannot rely solely on this field.
		MarketType string `gorm:"column:market_type"`
	}
	var rows []marketRow
	if err := db.WithContext(ctx).
		Model(&model.Market{}).
		Select("city, question, market_type").
		Where("is_active = ?", true).
		Order("updated_at DESC").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	citySet := make(map[string]struct{}, len(rows))
	cities := make([]string, 0, len(rows))
	for _, row := range rows {
		city := normalizeCity(row.City)
		if city != "" {
			if _, exists := citySet[city]; exists {
				continue
			}
			citySet[city] = struct{}{}
			cities = append(cities, city)
		}
	}
	if len(cities) > 0 {
		sort.Strings(cities)
		return cities, nil
	}

	candidates, err := loadCityCandidates(ctx, db)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		matchCitiesFromQuestion(row.Question, row.MarketType, candidates, citySet, &cities, true)
	}
	// Last-resort supplement: also parse all active market questions (not only weather-typed rows)
	// because upstream market_type/city can be unknown/empty.
	for _, row := range rows {
		matchCitiesFromQuestion(row.Question, row.MarketType, candidates, citySet, &cities, false)
	}
	sort.Strings(cities)
	return cities, nil
}

type cityCandidate struct {
	city    string
	keyword string
}

func loadCityCandidates(ctx context.Context, db *gorm.DB) ([]cityCandidate, error) {
	out := make([]cityCandidate, 0, 32)
	seen := map[string]struct{}{}
	add := func(city string, aliases ...string) {
		c := normalizeCity(city)
		if c == "" {
			return
		}
		for _, alias := range append([]string{city}, aliases...) {
			k := normalizeText(alias)
			if k == "" || len(k) < 3 {
				continue
			}
			key := c + "|" + k
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, cityCandidate{city: c, keyword: k})
		}
	}

	// Dynamic candidates from stations preserve extensibility.
	var stationCities []string
	if err := db.WithContext(ctx).
		Model(&model.WeatherStation{}).
		Distinct("city").
		Where("city <> ''").
		Pluck("city", &stationCities).Error; err != nil {
		return nil, err
	}
	for _, city := range stationCities {
		add(city)
	}

	// Fallback aliases for common weather markets when station table is empty.
	add("new york", "nyc")
	add("chicago")
	add("miami")
	add("los angeles", "la")
	add("dallas")
	add("austin")
	add("houston")
	add("atlanta")
	add("boston")
	add("philadelphia", "philly")
	add("washington", "washington dc", "dc")
	add("san francisco", "sf")
	add("seattle")
	return out, nil
}

func looksLikeWeatherMarket(question string, marketType string) bool {
	mt := strings.ToLower(strings.TrimSpace(marketType))
	if strings.HasPrefix(mt, "temperature_") || strings.Contains(mt, "weather") {
		return true
	}
	q := normalizeText(question)
	return strings.Contains(q, "temperature") ||
		strings.Contains(q, "high temp") ||
		strings.Contains(q, "low temp") ||
		strings.Contains(q, "weather")
}

func matchCitiesFromQuestion(
	question string,
	marketType string,
	candidates []cityCandidate,
	citySet map[string]struct{},
	cities *[]string,
	weatherOnly bool,
) {
	if weatherOnly && !looksLikeWeatherMarket(question, marketType) {
		return
	}
	q := normalizeText(question)
	if q == "" {
		return
	}
	for _, c := range candidates {
		if !strings.Contains(q, c.keyword) {
			continue
		}
		if _, exists := citySet[c.city]; exists {
			continue
		}
		citySet[c.city] = struct{}{}
		*cities = append(*cities, c.city)
	}
}

func emitExpiryWindowEvents(ctx context.Context, db *gorm.DB, sink *opportunity.Service, now time.Time) (int, error) {
	if db == nil || sink == nil {
		return 0, nil
	}
	type row struct {
		ID      string    `gorm:"column:id"`
		EndDate time.Time `gorm:"column:end_date"`
	}
	var markets []row
	if err := db.WithContext(ctx).
		Model(&model.Market{}).
		Select("id, end_date").
		Where("is_active = ?", true).
		Where("COALESCE(spec_status, '') = ?", "ready").
		Where("end_date > ?", now).
		Where("end_date <= ?", now.Add(24*time.Hour)).
		Limit(500).
		Find(&markets).Error; err != nil {
		return 0, err
	}
	emitted := 0
	for _, m := range markets {
		delta := m.EndDate.Sub(now)
		window := ""
		switch {
		case delta <= 2*time.Hour:
			window = "2h"
		case delta <= 6*time.Hour:
			window = "6h"
		case delta <= 24*time.Hour:
			window = "24h"
		default:
			continue
		}
		bucket := now.UTC().Truncate(30 * time.Minute)
		if err := sink.Emit(ctx, opportunity.EmitInput{
			EventType:  "expiry_window_entered",
			MarketID:   m.ID,
			Severity:   "info",
			DedupKey:   fmt.Sprintf("expiry_window_entered:%s:%s:%s", m.ID, window, bucket.Format("200601021504")),
			OccurredAt: bucket,
			Payload: map[string]any{
				"window":         window,
				"seconds_to_end": int(delta.Seconds()),
				"end_date":       m.EndDate.UTC().Format(time.RFC3339),
			},
		}); err == nil {
			emitted++
		}
	}
	return emitted, nil
}

func normalizeCity(v string) string {
	v = normalizeText(v)
	if v == "" {
		return ""
	}
	parts := strings.Fields(v)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

func normalizeText(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return ""
	}
	r := strings.NewReplacer(
		".", " ",
		",", " ",
		";", " ",
		":", " ",
		"/", " ",
		"-", " ",
		"_", " ",
		"(", " ",
		")", " ",
		"[", " ",
		"]", " ",
		"{", " ",
		"}", " ",
		"?", " ",
		"!", " ",
		"'", " ",
		"\"", " ",
	)
	return strings.Join(strings.Fields(r.Replace(v)), " ")
}
