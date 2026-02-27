package server

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/signal"
)

var signalGenerateMu sync.Mutex

func GenerateSignalsHandler(svc *signal.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !signalGenerateMu.TryLock() {
			c.JSON(http.StatusConflict, gin.H{
				"error": gin.H{"code": "SIGNAL_GENERATE_RUNNING", "message": "signal generation already running"},
			})
			return
		}
		defer signalGenerateMu.Unlock()

		limit := 0
		if raw := c.Query("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid limit query parameter"},
				})
				return
			}
			limit = n
		}

		result, err := svc.GenerateSignals(c.Request.Context(), signal.GenerateOptions{Limit: limit})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"code": "SIGNAL_GENERATE_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"result": result})
	}
}

func ListSignalsHandler(svc *signal.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 20
		if raw := c.Query("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid limit query parameter"},
				})
				return
			}
			limit = n
		}

		minEdge := 0.0
		if raw := c.Query("min_edge"); raw != "" {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil || v < 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid min_edge query parameter"},
				})
				return
			}
			minEdge = v
		}

		items, err := svc.ListSignals(c.Request.Context(), signal.ListOptions{
			Limit:   limit,
			MinEdge: minEdge,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"code": "INTERNAL_ERROR", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}
