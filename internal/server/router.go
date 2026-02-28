package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/agent"
	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/config"
)

func NewRouter(cfg *config.Config, log *zap.Logger, db *gorm.DB) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(Recovery(log))
	router.Use(RequestLogger(log))

	router.GET("/health", HealthHandler(cfg, db))
	marketSvc := market.NewService(cfg.Market, db, log)
	weatherSvc := weather.NewService(cfg.Weather, db, log)
	signalSvc := signal.NewService(cfg.Signal, db, log)
	agentSvc := agent.NewService(cfg.Agent, db, log, weatherSvc, signalSvc)
	tradeSvc := trade.NewService(cfg.Trade, cfg.Market, db, log, signalSvc)
	chainSvc := chain.NewService(cfg.Chain, db, log)

	api := router.Group("/api/v1")
	{
		api.GET("/health", HealthHandler(cfg, db))
		api.GET("/markets", ListMarketsHandler(marketSvc))
		api.POST("/markets/sync", SyncMarketsHandler(marketSvc))
		api.GET("/weather/forecast", GetForecastHandler(weatherSvc))
		api.GET("/weather/observation/:station", GetObservationHandler(weatherSvc))
		api.GET("/signals", ListSignalsHandler(signalSvc))
		api.POST("/signals/generate", GenerateSignalsHandler(signalSvc))
		api.GET("/trades", ListTradesHandler(tradeSvc))
		api.GET("/trades/:id", GetTradeHandler(tradeSvc))
		api.POST("/trades", CreateTradeHandler(tradeSvc))
		api.DELETE("/trades/:id", CancelTradeHandler(tradeSvc))
		api.GET("/positions", ListPositionsHandler(tradeSvc))
		api.GET("/pnl", GetPnLReportHandler(tradeSvc))
		api.POST("/agent/analyze", AnalyzeAgentHandler(agentSvc))
		api.GET("/agent/signals", ListSignalsHandler(signalSvc))
		api.GET("/agent/signals/:id", GetAgentSignalHandler(signalSvc))
		api.POST("/chain/scan", ScanChainHandler(chainSvc))
		api.GET("/chain/competitors", ListCompetitorsHandler(chainSvc))
		api.GET("/chain/competitors/:address", GetCompetitorHandler(chainSvc))
		api.GET("/chain/competitors/:address/trades", ListCompetitorTradesHandler(chainSvc))
	}

	return router
}
