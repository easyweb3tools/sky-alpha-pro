package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/opportunity"
)

type emitEventRequest struct {
	EventType string         `json:"event_type"`
	MarketID  string         `json:"market_id"`
	Severity  string         `json:"severity"`
	DedupKey  string         `json:"dedup_key"`
	Payload   map[string]any `json:"payload"`
}

func OpsEventsSummaryHandler(svc *opportunity.Service) gin.HandlerFunc {
	requireAuth := buildOpsAuthGuard()
	return func(c *gin.Context) {
		if !requireAuth(c) {
			return
		}
		window, err := parsePositiveIntQuery(c, "window_hours", 24)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		if svc == nil {
			c.JSON(http.StatusOK, gin.H{
				"window_hours":   window,
				"total_events":   0,
				"pending_events": 0,
				"dedup_dropped":  0,
				"by_type":        []any{},
			})
			return
		}
		out, err := svc.Summary(c.Request.Context(), int64(window))
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_EVENTS_SUMMARY_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, out)
	}
}

func OpsCandidatesSummaryHandler(svc *opportunity.Service) gin.HandlerFunc {
	requireAuth := buildOpsAuthGuard()
	return func(c *gin.Context) {
		if !requireAuth(c) {
			return
		}
		limit, err := parsePositiveIntQuery(c, "limit", 20)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		if limit > 200 {
			limit = 200
		}
		if svc == nil {
			c.JSON(http.StatusOK, gin.H{
				"states":         map[string]int64{},
				"top_candidates": []any{},
			})
			return
		}
		out, err := svc.CandidatePoolSummary(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_CANDIDATES_SUMMARY_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, out)
	}
}

func OpsCandidateTransitionsHandler(svc *opportunity.Service) gin.HandlerFunc {
	requireAuth := buildOpsAuthGuard()
	return func(c *gin.Context) {
		if !requireAuth(c) {
			return
		}
		limit, err := parsePositiveIntQuery(c, "limit", 50)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		marketID := strings.TrimSpace(c.Query("market_id"))
		if svc == nil {
			c.JSON(http.StatusOK, gin.H{"items": []any{}})
			return
		}
		items, err := svc.CandidateTransitions(c.Request.Context(), marketID, limit)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_CANDIDATE_TRANSITIONS_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"count": len(items),
			"items": items,
		})
	}
}

func OpsEmitEventHandler(svc *opportunity.Service) gin.HandlerFunc {
	requireAuth := buildOpsAuthGuard()
	return func(c *gin.Context) {
		if !requireAuth(c) {
			return
		}
		if svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_EVENT_SVC_UNAVAILABLE", "message": "opportunity service unavailable"},
			})
			return
		}
		var req emitEventRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		req.EventType = strings.TrimSpace(req.EventType)
		if req.EventType == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": "event_type is required"},
			})
			return
		}
		if err := svc.Emit(c.Request.Context(), opportunity.EmitInput{
			EventType:  req.EventType,
			MarketID:   strings.TrimSpace(req.MarketID),
			Severity:   strings.TrimSpace(req.Severity),
			DedupKey:   strings.TrimSpace(req.DedupKey),
			Payload:    req.Payload,
			OccurredAt: time.Now().UTC(),
		}); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_EVENT_EMIT_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}
