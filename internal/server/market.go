package server

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/market"
)

func ListMarketsHandler(svc *market.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		activeOnly := true
		if raw := c.Query("active"); raw != "" {
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid active query parameter"},
				})
				return
			}
			activeOnly = parsed
		}

		limit := 20
		if raw := c.Query("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid limit query parameter"},
				})
				return
			}
			limit = parsed
		}

		items, err := svc.ListMarketSnapshots(c.Request.Context(), market.ListOptions{
			ActiveOnly: activeOnly,
			City:       c.Query("city"),
			Limit:      limit,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"code": "INTERNAL_ERROR", "message": err.Error()},
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"items": items,
			"count": len(items),
		})
	}
}

func SyncMarketsHandler(svc *market.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		result, err := svc.SyncMarkets(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": gin.H{"code": "SYNC_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"result": result,
		})
	}
}
