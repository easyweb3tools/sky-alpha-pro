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
	marketSvc := market.NewService(cfg.Market, db, log)
	weatherSvc := weather.NewService(cfg.Weather, db, log)
	signalSvc := signal.NewService(cfg.Signal, db, log)
	agentSvc := agent.NewService(cfg.Agent, db, log, weatherSvc, signalSvc)
	tradeSvc := trade.NewService(cfg.Trade, cfg.Market, db, log, signalSvc)
	chainSvc := chain.NewService(cfg.Chain, db, log)
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
