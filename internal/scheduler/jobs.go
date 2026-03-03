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
	"sky-alpha-pro/internal/sim"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

func RegisterDefaultJobs(mgr *Manager, cfg *config.Config, db *gorm.DB, marketSvc *market.Service, weatherSvc *weather.Service, chainSvc *chain.Service, simSvc *sim.Service, agentSvc *agent.Service, log *zap.Logger) {
	if mgr == nil || cfg == nil {
		return
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
