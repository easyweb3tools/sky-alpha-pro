package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

type strategyParamsResponse struct {
	Timestamp string                `json:"timestamp"`
	Count     int                   `json:"count"`
	Items     []strategyParamItem   `json:"items"`
	Summary   strategyParamsSummary `json:"summary"`
}

type strategyParamsSummary struct {
	Enabled int `json:"enabled"`
}

type strategyParamItem struct {
	Key       string  `json:"key"`
	Scope     string  `json:"scope"`
	Value     float64 `json:"value"`
	Enabled   bool    `json:"enabled"`
	Reason    string  `json:"reason,omitempty"`
	UpdatedAt string  `json:"updated_at,omitempty"`
}

func OpsAgentStrategyParamsHandler(db *gorm.DB) gin.HandlerFunc {
	requireAuth := buildOpsAuthGuard()
	return func(c *gin.Context) {
		if !requireAuth(c) {
			return
		}
		if db == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_DB_UNAVAILABLE", "message": "database is unavailable"},
			})
			return
		}
		limit, err := parsePositiveIntQuery(c, "limit", 100)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		if limit > 500 {
			limit = 500
		}
		scope := strings.TrimSpace(c.Query("scope"))
		if scope == "" {
			scope = "agent_cycle"
		}

		query := db.WithContext(c.Request.Context()).
			Model(&model.AgentStrategyParam{}).
			Order("updated_at DESC, created_at DESC").
			Limit(limit)
		if scope != "" {
			query = query.Where("scope = ?", scope)
		}
		var rows []model.AgentStrategyParam
		if err := query.Find(&rows).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_STRATEGY_PARAMS_FAILED", "message": err.Error()},
			})
			return
		}
		resp := strategyParamsResponse{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Items:     make([]strategyParamItem, 0, len(rows)),
		}
		for _, row := range rows {
			item := strategyParamItem{
				Key:     strings.TrimSpace(row.Key),
				Scope:   strings.TrimSpace(row.Scope),
				Value:   row.Value,
				Enabled: row.Enabled,
				Reason:  strings.TrimSpace(row.Reason),
			}
			if !row.UpdatedAt.IsZero() {
				item.UpdatedAt = row.UpdatedAt.UTC().Format(time.RFC3339)
			}
			if row.Enabled {
				resp.Summary.Enabled++
			}
			resp.Items = append(resp.Items, item)
		}
		resp.Count = len(resp.Items)
		c.JSON(http.StatusOK, resp)
	}
}
