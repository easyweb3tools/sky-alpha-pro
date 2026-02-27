package server

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"sky-alpha-pro/pkg/config"
)

func HealthHandler(cfg *config.Config, db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		status := "ok"
		databaseStatus := "up"

		sqlDB, err := db.DB()
		if err != nil {
			status = "degraded"
			databaseStatus = "down"
		} else {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
			defer cancel()
			if err := sqlDB.PingContext(ctx); err != nil {
				status = "degraded"
				databaseStatus = "down"
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    status,
			"service":   cfg.App.Name,
			"version":   cfg.App.Version,
			"env":       cfg.App.Env,
			"database":  databaseStatus,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
}
