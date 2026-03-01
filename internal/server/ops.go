package server

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/scheduler"
	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

type opsStatusResponse struct {
	Timestamp   time.Time                 `json:"timestamp"`
	Scheduler   scheduler.ManagerSnapshot `json:"scheduler"`
	Summary     opsSummary                `json:"summary"`
	Freshness   map[string]float64        `json:"freshness"`
	ConfigCheck opsConfigCheck            `json:"config_check"`
}

type opsConfigCheck struct {
	ChainRPCConfigured           bool   `json:"chain_rpc_configured"`
	AgentVertexProjectConfigured bool   `json:"agent_vertex_project_configured"`
	VisualCrossingKeyConfigured  bool   `json:"visualcrossing_key_configured"`
	ChainScanLookbackBlocks      uint64 `json:"chain_scan_lookback_blocks"`
}

type opsSummary struct {
	Degraded      bool         `json:"degraded"`
	TotalJobs     int          `json:"total_jobs"`
	HealthyJobs   int          `json:"healthy_jobs"`
	UnhealthyJobs int          `json:"unhealthy_jobs"`
	Blockers      []opsBlocker `json:"blockers"`
}

type opsBlocker struct {
	Job                 string `json:"job"`
	Severity            string `json:"severity"`
	Status              string `json:"status"`
	ErrorCode           string `json:"error_code"`
	ErrorMessage        string `json:"error_message"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
}

func OpsStatusHandler(cfg *config.Config, metricReg *metrics.Registry, schedulerMgr *scheduler.Manager) gin.HandlerFunc {
	requiredToken := strings.TrimSpace(os.Getenv("SKY_ALPHA_OPS_TOKEN"))
	return func(c *gin.Context) {
		if requiredToken != "" {
			auth := strings.TrimSpace(c.GetHeader("Authorization"))
			want := "Bearer " + requiredToken
			if auth != want {
				c.JSON(http.StatusUnauthorized, gin.H{
					"error": gin.H{"code": "OPS_UNAUTHORIZED", "message": "missing or invalid ops token"},
				})
				return
			}
		}

		resp := opsStatusResponse{
			Timestamp: time.Now().UTC(),
			Freshness: map[string]float64{},
			Summary: opsSummary{
				Blockers: make([]opsBlocker, 0),
			},
			ConfigCheck: opsConfigCheck{
				ChainRPCConfigured:           false,
				AgentVertexProjectConfigured: false,
				VisualCrossingKeyConfigured:  false,
				ChainScanLookbackBlocks:      0,
			},
		}
		if schedulerMgr != nil {
			resp.Scheduler = schedulerMgr.Snapshot()
			resp.Summary = summarizeSchedulerSnapshot(resp.Scheduler)
		}
		if metricReg != nil {
			resp.Freshness = metricReg.SnapshotDataFreshness()
		}
		if cfg != nil {
			lookback := cfg.Scheduler.Jobs.ChainScan.LookbackBlocks
			if lookback == 0 {
				lookback = cfg.Chain.ScanLookbackBlocks
			}
			resp.ConfigCheck = opsConfigCheck{
				ChainRPCConfigured:           strings.TrimSpace(cfg.Chain.RPCURL) != "",
				AgentVertexProjectConfigured: strings.TrimSpace(cfg.Agent.VertexProject) != "",
				VisualCrossingKeyConfigured:  strings.TrimSpace(cfg.Weather.VisualCrossingAPIKey) != "",
				ChainScanLookbackBlocks:      lookback,
			}
			if cfg.Scheduler.Jobs.ChainScan.Enabled && !resp.ConfigCheck.ChainRPCConfigured {
				resp.Summary.Blockers = append(resp.Summary.Blockers, opsBlocker{
					Job:          "chain_scan",
					Severity:     "critical",
					Status:       "config_error",
					ErrorCode:    "chain_rpc_empty",
					ErrorMessage: "chain scan enabled but chain.rpc_url is empty",
				})
				resp.Summary.UnhealthyJobs++
				resp.Summary.Degraded = true
			}
		}
		if len(resp.Summary.Blockers) > 0 {
			resp.Summary.Degraded = true
		}
		if total := resp.Summary.HealthyJobs + resp.Summary.UnhealthyJobs; total > resp.Summary.TotalJobs {
			resp.Summary.TotalJobs = total
		}
		c.JSON(200, resp)
	}
}

func summarizeSchedulerSnapshot(s scheduler.ManagerSnapshot) opsSummary {
	summary := opsSummary{
		TotalJobs: len(s.Jobs),
		Blockers:  make([]opsBlocker, 0),
	}
	for _, job := range s.Jobs {
		if isHealthyJobStatus(job.LastStatus) {
			summary.HealthyJobs++
			continue
		}
		if strings.TrimSpace(job.LastStatus) == "" {
			continue
		}
		summary.UnhealthyJobs++
		blocker := opsBlocker{
			Job:                 job.Name,
			Severity:            classifySeverity(job.LastStatus, job.LastErrorCode, job.ConsecutiveFailures),
			Status:              job.LastStatus,
			ErrorCode:           job.LastErrorCode,
			ErrorMessage:        job.LastErrorMessage,
			ConsecutiveFailures: job.ConsecutiveFailures,
		}
		summary.Blockers = append(summary.Blockers, blocker)
	}
	summary.Degraded = summary.UnhealthyJobs > 0
	return summary
}

func isHealthyJobStatus(status string) bool {
	return status == "" || status == "success"
}

func classifySeverity(status, code string, consecutive int) string {
	switch status {
	case "error", "timeout":
		if code == "upstream_429" || code == "rpc_range_limit" {
			if consecutive >= 3 {
				return "critical"
			}
			return "warning"
		}
		return "critical"
	case "skipped_no_input":
		return "warning"
	default:
		return "info"
	}
}
