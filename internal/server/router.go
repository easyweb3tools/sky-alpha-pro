package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"sky-alpha-pro/pkg/config"
)

func NewRouter(cfg *config.Config, log *zap.Logger, db *gorm.DB) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(Recovery(log))
	router.Use(RequestLogger(log))

	router.GET("/health", HealthHandler(cfg, db))

	api := router.Group("/api/v1")
	{
		api.GET("/health", HealthHandler(cfg, db))
	}

	return router
}
