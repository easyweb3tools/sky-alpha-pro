package server

import (
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/agent"
	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/internal/player"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/sim"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

type Services struct {
	Market  *market.Service
	Weather *weather.Service
	Signal  *signal.Service
	Agent   *agent.Service
	Trade   *trade.Service
	Chain   *chain.Service
	Player  *player.Service
	Sim     *sim.Service
}

func NewServices(cfg *config.Config, log *zap.Logger, db *gorm.DB) *Services {
	return NewServicesWithMetrics(cfg, log, db, nil)
}

func NewServicesWithMetrics(cfg *config.Config, log *zap.Logger, db *gorm.DB, metricReg *metrics.Registry) *Services {
	marketSvc := market.NewService(cfg.Market, db, log)
	weatherSvc := weather.NewService(cfg.Weather, db, log)
	signalSvc := signal.NewService(cfg.Signal, db, log)
	agentSvc := agent.NewService(cfg.Agent, db, log, weatherSvc, signalSvc)
	tradeSvc := trade.NewService(cfg.Trade, cfg.Market, db, log, signalSvc)
	chainSvc := chain.NewService(cfg.Chain, db, log)
	agentSvc.SetToolServices(marketSvc, chainSvc)
	if chainSvc != nil {
		chainSvc.SetMetrics(metricReg)
	}
	playerSvc := player.NewService(db, log)
	simSvc := sim.NewService(cfg.Sim, db, log, marketSvc, weatherSvc, signalSvc, tradeSvc)

	return &Services{
		Market:  marketSvc,
		Weather: weatherSvc,
		Signal:  signalSvc,
		Agent:   agentSvc,
		Trade:   tradeSvc,
		Chain:   chainSvc,
		Player:  playerSvc,
		Sim:     simSvc,
	}
}
