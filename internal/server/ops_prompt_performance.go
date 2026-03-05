package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

type promptPerformanceResponse struct {
	Timestamp   string                  `json:"timestamp"`
	WindowHours int                     `json:"window_hours"`
	Count       int                     `json:"count"`
	Items       []promptPerformanceItem `json:"items"`
}

type promptPerformanceItem struct {
	PromptVersion       string  `json:"prompt_version"`
	PromptVariant       string  `json:"prompt_variant"`
	Runs                int64   `json:"runs"`
	SuccessRate         float64 `json:"success_rate"`
	DegradedRate        float64 `json:"degraded_rate"`
	ErrorRate           float64 `json:"error_rate"`
	AvgDurationMS       float64 `json:"avg_duration_ms"`
	AvgToolCalls        float64 `json:"avg_tool_calls"`
	AvgLLMCalls         float64 `json:"avg_llm_calls"`
	AvgLLMTokens        float64 `json:"avg_llm_tokens"`
	SignalsGeneratedSum int64   `json:"signals_generated_sum"`
	SignalsPerRun       float64 `json:"signals_per_run"`
	LastRunAt           string  `json:"last_run_at,omitempty"`
}

func OpsAgentPromptPerformanceHandler(db *gorm.DB) gin.HandlerFunc {
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
		window, err := parsePositiveIntQuery(c, "window_hours", 24)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "OPS_BAD_REQUEST", "message": err.Error()},
			})
			return
		}
		if window > 24*30 {
			window = 24 * 30
		}

		cutoff := time.Now().UTC().Add(-time.Duration(window) * time.Hour)
		var rows []model.AgentSession
		if err := db.WithContext(c.Request.Context()).
			Where("started_at >= ?", cutoff).
			Order("started_at DESC").
			Limit(10000).
			Find(&rows).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_PROMPT_PERF_FAILED", "message": err.Error()},
			})
			return
		}

		type agg struct {
			runs         int64
			success      int64
			degraded     int64
			failed       int64
			durationSum  int64
			toolCallsSum int64
			llmCallsSum  int64
			llmTokensSum int64
			signalsSum   int64
			lastRunAt    *time.Time
		}
		aggs := map[string]*agg{}
		keyOf := func(v, variant string) string {
			return strings.TrimSpace(v) + "||" + strings.TrimSpace(variant)
		}
		for _, row := range rows {
			version := strings.TrimSpace(row.PromptVersion)
			if version == "" {
				version = "unknown"
			}
			variant := strings.TrimSpace(row.PromptVariant)
			if variant == "" {
				variant = "active"
			}
			key := keyOf(version, variant)
			a, ok := aggs[key]
			if !ok {
				a = &agg{}
				aggs[key] = a
			}
			a.runs++
			switch strings.ToLower(strings.TrimSpace(row.Status)) {
			case "success":
				a.success++
			case "degraded":
				a.degraded++
			case "error", "timeout":
				a.failed++
			}
			a.durationSum += int64(row.DurationMS)
			a.toolCallsSum += int64(row.ToolCalls)
			a.llmCallsSum += int64(row.LLMCalls)
			a.llmTokensSum += int64(row.LLMTokens)
			a.signalsSum += extractSignalsGeneratedFromSummary(row.SummaryJSON)
			startedAt := row.StartedAt.UTC()
			if a.lastRunAt == nil || startedAt.After(*a.lastRunAt) {
				tmp := startedAt
				a.lastRunAt = &tmp
			}
		}

		items := make([]promptPerformanceItem, 0, len(aggs))
		for key, a := range aggs {
			parts := strings.SplitN(key, "||", 2)
			version := parts[0]
			variant := "active"
			if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
				variant = parts[1]
			}
			runs := float64(maxInt64(a.runs, 1))
			item := promptPerformanceItem{
				PromptVersion:       version,
				PromptVariant:       variant,
				Runs:                a.runs,
				SuccessRate:         roundTo4(float64(a.success) / runs),
				DegradedRate:        roundTo4(float64(a.degraded) / runs),
				ErrorRate:           roundTo4(float64(a.failed) / runs),
				AvgDurationMS:       roundTo2(float64(a.durationSum) / runs),
				AvgToolCalls:        roundTo2(float64(a.toolCallsSum) / runs),
				AvgLLMCalls:         roundTo2(float64(a.llmCallsSum) / runs),
				AvgLLMTokens:        roundTo2(float64(a.llmTokensSum) / runs),
				SignalsGeneratedSum: a.signalsSum,
				SignalsPerRun:       roundTo4(float64(a.signalsSum) / runs),
			}
			if a.lastRunAt != nil && !a.lastRunAt.IsZero() {
				item.LastRunAt = a.lastRunAt.Format(time.RFC3339)
			}
			items = append(items, item)
		}

		sort.Slice(items, func(i, j int) bool {
			if items[i].Runs == items[j].Runs {
				if items[i].PromptVersion == items[j].PromptVersion {
					return items[i].PromptVariant < items[j].PromptVariant
				}
				return items[i].PromptVersion < items[j].PromptVersion
			}
			return items[i].Runs > items[j].Runs
		})

		resp := promptPerformanceResponse{
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
			WindowHours: window,
			Count:       len(items),
			Items:       items,
		}
		c.JSON(http.StatusOK, resp)
	}
}

func extractSignalsGeneratedFromSummary(raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0
	}
	funnel, ok := obj["funnel"].(map[string]any)
	if !ok {
		return 0
	}
	v, ok := funnel["signals_generated"]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func roundTo2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

func roundTo4(v float64) float64 {
	return float64(int64(v*10000+0.5)) / 10000
}
