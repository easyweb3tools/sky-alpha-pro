package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/internal/model"
	"sky-alpha-pro/internal/sim"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

func RegisterDefaultJobs(mgr *Manager, cfg *config.Config, db *gorm.DB, marketSvc *market.Service, weatherSvc *weather.Service, chainSvc *chain.Service, simSvc *sim.Service, log *zap.Logger) {
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
				out.Records = append(out.Records,
					FetchRecord{Entity: "markets", Result: "success", Count: res.MarketsUpserted},
					FetchRecord{Entity: "prices", Result: "success", Count: res.PriceSnapshots},
				)
				if len(res.Errors) > 0 {
					out.Records = append(out.Records, FetchRecord{Entity: "market_sync_errors", Result: "error", Count: len(res.Errors)})
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
					return JobResult{}, nil
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

				return JobResult{
					Records: []FetchRecord{
						{Entity: "cities", Result: "success", Count: successCity},
						{Entity: "cities", Result: "error", Count: failedCity},
						{Entity: "forecasts", Result: "success", Count: forecastRows},
						{Entity: "provider_errors", Result: "error", Count: providerErrors},
					},
					Freshness: map[string]float64{"forecasts": 0},
				}, nil
			},
		})
	}

	cc := cfg.Scheduler.Jobs.ChainScan
	if cc.Enabled && cc.Interval > 0 && chainSvc != nil {
		if strings.TrimSpace(cfg.Chain.RPCURL) == "" {
			if log != nil {
				log.Info("skip chain_scan scheduler job: chain.rpc_url is empty")
			}
		} else {
			mgr.Register(Job{
				Name:      "chain_scan",
				Interval:  cc.Interval,
				Timeout:   cc.Timeout,
				Immediate: cc.Immediate,
				Run: func(ctx context.Context) (JobResult, error) {
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
}

func loadActiveCities(ctx context.Context, db *gorm.DB) ([]string, error) {
	type cityRow struct {
		City string `gorm:"column:city"`
	}
	var rows []cityRow
	if err := db.WithContext(ctx).
		Model(&model.Market{}).
		Select("DISTINCT city").
		Where("is_active = ?", true).
		Where("city <> ''").
		Order("city ASC").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	cities := make([]string, 0, len(rows))
	for _, row := range rows {
		city := strings.TrimSpace(row.City)
		if city != "" {
			cities = append(cities, city)
		}
	}
	return cities, nil
}
