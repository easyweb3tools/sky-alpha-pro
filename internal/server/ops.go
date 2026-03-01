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
	Freshness   map[string]float64        `json:"freshness"`
	ConfigCheck opsConfigCheck            `json:"config_check"`
}

type opsConfigCheck struct {
	ChainRPCConfigured           bool   `json:"chain_rpc_configured"`
	AgentVertexProjectConfigured bool   `json:"agent_vertex_project_configured"`
	VisualCrossingKeyConfigured  bool   `json:"visualcrossing_key_configured"`
	ChainScanLookbackBlocks      uint64 `json:"chain_scan_lookback_blocks"`
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
			ConfigCheck: opsConfigCheck{
				ChainRPCConfigured:           false,
				AgentVertexProjectConfigured: false,
				VisualCrossingKeyConfigured:  false,
				ChainScanLookbackBlocks:      0,
			},
		}
		if schedulerMgr != nil {
			resp.Scheduler = schedulerMgr.Snapshot()
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
		}
		c.JSON(200, resp)
	}
}
