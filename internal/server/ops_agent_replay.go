package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"sky-alpha-pro/internal/model"
)

type replayStepView struct {
	StepNo      int             `json:"step_no"`
	Tool        string          `json:"tool"`
	Status      string          `json:"status"`
	OnFail      string          `json:"on_fail"`
	ErrorCode   string          `json:"error_code,omitempty"`
	ErrorDetail string          `json:"error_detail,omitempty"`
	Args        json.RawMessage `json:"args,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	StartedAt   time.Time       `json:"started_at"`
	FinishedAt  time.Time       `json:"finished_at"`
	DurationMS  int             `json:"duration_ms"`
}

type opsAgentReplayResponse struct {
	SessionID     string            `json:"session_id"`
	CycleID       string            `json:"cycle_id"`
	RunMode       string            `json:"run_mode"`
	Status        string            `json:"status"`
	Decision      string            `json:"decision"`
	Model         string            `json:"model,omitempty"`
	PromptVersion string            `json:"prompt_version"`
	PromptVariant string            `json:"prompt_variant,omitempty"`
	LLMCalls      int               `json:"llm_calls"`
	LLMTokens     int               `json:"llm_tokens"`
	ToolCalls     int               `json:"tool_calls"`
	ErrorCode     string            `json:"error_code,omitempty"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	StartedAt     time.Time         `json:"started_at"`
	FinishedAt    time.Time         `json:"finished_at"`
	DurationMS    int               `json:"duration_ms"`
	BrainTrace    []map[string]any  `json:"brain_trace"`
	Steps         []replayStepView  `json:"steps"`
	Summary       map[string]any    `json:"summary"`
}

func OpsAgentSessionReplayHandler(db *gorm.DB) gin.HandlerFunc {
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
		sessionID := strings.TrimSpace(c.Param("id"))
		if sessionID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "session id is required"},
			})
			return
		}

		var sess model.AgentSession
		if err := db.WithContext(c.Request.Context()).Where("id = ?", sessionID).Take(&sess).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusNotFound, gin.H{
					"error": gin.H{"code": "NOT_FOUND", "message": "agent session not found"},
				})
				return
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_AGENT_REPLAY_FAILED", "message": err.Error()},
			})
			return
		}

		var steps []model.AgentStep
		if err := db.WithContext(c.Request.Context()).
			Where("session_id = ?", sessionID).
			Order("step_no ASC, id ASC").
			Find(&steps).Error; err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "OPS_AGENT_REPLAY_FAILED", "message": err.Error()},
			})
			return
		}

		summary := map[string]any{}
		_ = json.Unmarshal(sess.SummaryJSON, &summary)
		brainTrace := extractBrainTrace(summary)

		stepViews := make([]replayStepView, 0, len(steps))
		for _, st := range steps {
			stepViews = append(stepViews, replayStepView{
				StepNo:      st.StepNo,
				Tool:        st.Tool,
				Status:      st.Status,
				OnFail:      st.OnFail,
				ErrorCode:   st.ErrorCode,
				ErrorDetail: st.ErrorDetail,
				Args:        json.RawMessage(st.ArgsJSON),
				Result:      json.RawMessage(st.ResultJSON),
				StartedAt:   st.StartedAt,
				FinishedAt:  st.FinishedAt,
				DurationMS:  st.DurationMS,
			})
		}

		c.JSON(http.StatusOK, opsAgentReplayResponse{
			SessionID:     sess.ID,
			CycleID:       sess.CycleID,
			RunMode:       sess.RunMode,
			Status:        sess.Status,
			Decision:      sess.Decision,
			Model:         sess.Model,
			PromptVersion: sess.PromptVersion,
			PromptVariant: sess.PromptVariant,
			LLMCalls:      sess.LLMCalls,
			LLMTokens:     sess.LLMTokens,
			ToolCalls:     sess.ToolCalls,
			ErrorCode:     sess.ErrorCode,
			ErrorMessage:  sess.ErrorMessage,
			StartedAt:     sess.StartedAt,
			FinishedAt:    sess.FinishedAt,
			DurationMS:    sess.DurationMS,
			BrainTrace:    brainTrace,
			Steps:         stepViews,
			Summary:       summary,
		})
	}
}

func extractBrainTrace(summary map[string]any) []map[string]any {
	if len(summary) == 0 {
		return []map[string]any{}
	}
	raw, ok := summary["brain_trace"]
	if !ok {
		return []map[string]any{}
	}
	items, ok := raw.([]any)
	if !ok {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, m)
	}
	return out
}

