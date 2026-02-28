package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

func NewRouter(cfg *config.Config, log *zap.Logger, db *gorm.DB, metricReg *metrics.Registry) http.Handler {
	return NewRouterWithServices(cfg, log, db, metricReg, NewServices(cfg, log, db))
}

func NewRouterWithServices(cfg *config.Config, log *zap.Logger, db *gorm.DB, metricReg *metrics.Registry, services *Services) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(Recovery(log))
	router.Use(RequestLogger(log))

	router.GET("/health", HealthHandler(cfg, db))
	if metricReg != nil && metricReg.Enabled() {
		router.GET(metricReg.Path(), gin.WrapH(metricReg.Handler()))
	}
	if services == nil {
		services = NewServices(cfg, log, db)
	}

	api := router.Group("/api/v1")
	{
		api.GET("/health", HealthHandler(cfg, db))
		api.GET("/markets", ListMarketsHandler(services.Market))
		api.POST("/markets/sync", SyncMarketsHandler(services.Market))
		api.GET("/weather/forecast", GetForecastHandler(services.Weather))
		api.GET("/weather/observation/:station", GetObservationHandler(services.Weather))
		api.GET("/signals", ListSignalsHandler(services.Signal))
		api.POST("/signals/generate", GenerateSignalsHandler(services.Signal))
		api.GET("/trades", ListTradesHandler(services.Trade))
		api.GET("/trades/:id", GetTradeHandler(services.Trade))
		api.POST("/trades", CreateTradeHandler(services.Trade))
		api.DELETE("/trades/:id", CancelTradeHandler(services.Trade))
		api.GET("/positions", ListPositionsHandler(services.Trade))
		api.GET("/pnl", GetPnLReportHandler(services.Trade))
		api.POST("/agent/analyze", AnalyzeAgentHandler(services.Agent))
		api.GET("/agent/signals", ListSignalsHandler(services.Signal))
		api.GET("/agent/signals/:id", GetAgentSignalHandler(services.Signal))
		api.POST("/chain/scan", ScanChainHandler(services.Chain))
		api.GET("/chain/competitors", ListCompetitorsHandler(services.Chain))
		api.GET("/chain/competitors/:address", GetCompetitorHandler(services.Chain))
		api.GET("/chain/competitors/:address/trades", ListCompetitorTradesHandler(services.Chain))
		api.POST("/players/sync", SyncPlayersHandler(services.Player))
		api.GET("/players", ListPlayersHandler(services.Player))
		api.GET("/players/leaderboard", GetPlayerLeaderboardHandler(services.Player))
		api.GET("/players/:address/positions", ListPlayerPositionsHandler(services.Player))
		api.GET("/players/:address/compare", ComparePlayerHandler(services.Player))
		api.GET("/players/:address", GetPlayerHandler(services.Player))
		api.GET("/sim/report", SimReportHandler(services.Sim))
		api.POST("/sim/cycle", SimRunCycleHandler(services.Sim))
	}

	return router
}
