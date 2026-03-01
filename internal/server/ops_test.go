package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"sky-alpha-pro/internal/scheduler"
	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/metrics"
)

func TestOpsStatusHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	cfg := &config.Config{}
	cfg.Metrics.Enabled = true
	cfg.Metrics.Path = "/metrics"
	cfg.Chain.RPCURL = "https://example.com/rpc"
	cfg.Chain.ScanLookbackBlocks = 20
	cfg.Agent.VertexProject = "project-1"
	cfg.Weather.VisualCrossingAPIKey = "k1"
	cfg.Scheduler.Enabled = true

	reg := metrics.New(cfg.Metrics)
	reg.SetDataFreshness("markets", 12)

	mgr := scheduler.NewManager(cfg.Scheduler, zap.NewNop(), reg)

	r := gin.New()
	r.GET("/ops/status", OpsStatusHandler(cfg, reg, mgr))

	req := httptest.NewRequest(http.MethodGet, "/ops/status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	var body struct {
		ConfigCheck struct {
			ChainRPCConfigured           bool   `json:"chain_rpc_configured"`
			AgentVertexProjectConfigured bool   `json:"agent_vertex_project_configured"`
			VisualCrossingKeyConfigured  bool   `json:"visualcrossing_key_configured"`
			ChainScanLookbackBlocks      uint64 `json:"chain_scan_lookback_blocks"`
		} `json:"config_check"`
		Freshness map[string]float64 `json:"freshness"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !body.ConfigCheck.ChainRPCConfigured {
		t.Fatalf("expected chain rpc configured")
	}
	if !body.ConfigCheck.AgentVertexProjectConfigured {
		t.Fatalf("expected vertex project configured")
	}
	if !body.ConfigCheck.VisualCrossingKeyConfigured {
		t.Fatalf("expected visual crossing key configured")
	}
	if body.ConfigCheck.ChainScanLookbackBlocks != 20 {
		t.Fatalf("expected chain lookback blocks 20, got %d", body.ConfigCheck.ChainScanLookbackBlocks)
	}
	if body.Freshness["markets"] != 12 {
		t.Fatalf("expected markets freshness 12, got %v", body.Freshness["markets"])
	}
}

func TestOpsStatusHandlerAuth(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	t.Setenv("SKY_ALPHA_OPS_TOKEN", "ops-token")

	r := gin.New()
	r.GET("/ops/status", OpsStatusHandler(&config.Config{}, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/ops/status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", w.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/ops/status", nil)
	req2.Header.Set("Authorization", "Bearer ops-token")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d", w2.Code)
	}
}
