package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

type strategyChangesResponse struct {
	Timestamp string                `json:"timestamp"`
	Count     int                   `json:"count"`
	Items     []strategyChangeItem  `json:"items"`
	Summary   strategyChangeSummary `json:"summary"`
}

type strategyChangeSummary struct {
	Monitoring int `json:"monitoring"`
	Kept       int `json:"kept"`
	RolledBack int `json:"rolled_back"`
}

type strategyChangeItem struct {
	ID              uint64  `json:"id"`
	Scope           string  `json:"scope"`
	Subject         string  `json:"subject"`
	ParamName       string  `json:"param_name"`
	OldValue        float64 `json:"old_value"`
	NewValue        float64 `json:"new_value"`
	TargetMetric    string  `json:"target_metric"`
	BaselineValue   float64 `json:"baseline_value"`
	LatestValue     float64 `json:"latest_value"`
	ObservedRuns    int     `json:"observed_runs"`
	NoImproveStreak int     `json:"no_improve_streak"`
	Status          string  `json:"status"`
	Decision        string  `json:"decision,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at,omitempty"`
	FinalizedAt     *string `json:"finalized_at,omitempty"`
}

func OpsAgentStrategyChangesHandler(db *gorm.DB) gin.HandlerFunc {
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
		limit, err := parsePositiveIntQuery(c, "limit", 50)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		if limit > 200 {
			limit = 200
		}

		var rows []model.AgentStrategyChange
		query := db.WithContext(c.Request.Context()).
			Order("id DESC").
			Limit(limit)
		if scope := strings.TrimSpace(c.Query("scope")); scope != "" {
			query = query.Where("scope = ?", scope)
		}
		if status := strings.TrimSpace(c.Query("status")); status != "" {
			query = query.Where("status = ?", status)
		}
		if err := query.Find(&rows).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_STRATEGY_CHANGES_FAILED", "message": err.Error()},
			})
			return
		}

		resp := strategyChangesResponse{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Items:     make([]strategyChangeItem, 0, len(rows)),
		}
		for _, row := range rows {
			status := strings.TrimSpace(row.Status)
			switch status {
			case "monitoring":
				resp.Summary.Monitoring++
			case "kept":
				resp.Summary.Kept++
			case "rolled_back":
				resp.Summary.RolledBack++
			}
			item := strategyChangeItem{
				ID:              row.ID,
				Scope:           strings.TrimSpace(row.Scope),
				Subject:         strings.TrimSpace(row.Subject),
				ParamName:       strings.TrimSpace(row.ParamName),
				OldValue:        row.OldValue,
				NewValue:        row.NewValue,
				TargetMetric:    strings.TrimSpace(row.TargetMetric),
				BaselineValue:   row.BaselineValue,
				LatestValue:     row.LatestValue,
				ObservedRuns:    row.ObservedRuns,
				NoImproveStreak: row.NoImproveStreak,
				Status:          status,
				Decision:        strings.TrimSpace(row.Decision),
				Reason:          strings.TrimSpace(row.Reason),
				CreatedAt:       row.CreatedAt.UTC().Format(time.RFC3339),
			}
			if !row.UpdatedAt.IsZero() {
				item.UpdatedAt = row.UpdatedAt.UTC().Format(time.RFC3339)
			}
			if row.FinalizedAt != nil && !row.FinalizedAt.IsZero() {
				v := row.FinalizedAt.UTC().Format(time.RFC3339)
				item.FinalizedAt = &v
			}
			resp.Items = append(resp.Items, item)
		}
		resp.Count = len(resp.Items)
		c.JSON(http.StatusOK, resp)
	}
}
