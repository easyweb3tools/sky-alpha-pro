package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/sim"
)

func SimReportHandler(svc *sim.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			from time.Time
			to   time.Time
			err  error
		)
		if raw := strings.TrimSpace(c.Query("from")); raw != "" {
			from, err = time.Parse("2006-01-02", raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid from, expected YYYY-MM-DD"},
				})
				return
			}
		}
		if raw := strings.TrimSpace(c.Query("to")); raw != "" {
			to, err = time.Parse("2006-01-02", raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid to, expected YYYY-MM-DD"},
				})
				return
			}
		}

		report, err := svc.GetReport(c.Request.Context(), sim.ReportOptions{
			From: from,
			To:   to,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"code": "INTERNAL_ERROR", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, report)
	}
}

func SimRunCycleHandler(svc *sim.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		result, err := svc.RunCycle(c.Request.Context(), 0)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"code": "INTERNAL_ERROR", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}
