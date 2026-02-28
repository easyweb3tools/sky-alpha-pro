package server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/agent"
	"sky-alpha-pro/internal/signal"
)

type analyzeAgentRequest struct {
	MarketID string `json:"market_id"`
	All      bool   `json:"all"`
	Limit    int    `json:"limit"`
	Depth    string `json:"depth"`
}

func AnalyzeAgentHandler(svc *agent.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body analyzeAgentRequest
		if c.Request.ContentLength > 0 {
			if err := c.ShouldBindJSON(&body); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid json body"},
				})
				return
			}
		}

		if q := strings.TrimSpace(c.Query("market_id")); q != "" {
			body.MarketID = q
		}
		if q := strings.TrimSpace(c.Query("depth")); q != "" {
			body.Depth = q
		}
		if q := strings.TrimSpace(c.Query("all")); q != "" {
			v, err := strconv.ParseBool(q)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid all query parameter"},
				})
				return
			}
			body.All = v
		}
		if q := strings.TrimSpace(c.Query("limit")); q != "" {
			v, err := strconv.Atoi(q)
			if err != nil || v <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid limit query parameter"},
				})
				return
			}
			body.Limit = v
		}

		resp, err := svc.Analyze(c.Request.Context(), agent.AnalyzeRequest{
			MarketID: body.MarketID,
			All:      body.All,
			Limit:    body.Limit,
			Depth:    body.Depth,
		})
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": gin.H{"code": "AGENT_ANALYZE_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, resp)
	}
}

func ListAgentSignalsHandler(svc *signal.Service) gin.HandlerFunc {
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

func GetAgentSignalHandler(svc *signal.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := strings.TrimSpace(c.Param("id"))
		if raw == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "signal id is required"},
			})
			return
		}
		id, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || id == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "invalid signal id"},
			})
			return
		}

		item, err := svc.GetSignalByID(c.Request.Context(), id)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{
				"error": gin.H{"code": "NOT_FOUND", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}
