package agent

import "time"

type AnalyzeRequest struct {
	MarketID string `json:"market_id"`
	All      bool   `json:"all"`
	Limit    int    `json:"limit"`
	Depth    string `json:"depth"`
}

type AnalyzeResponse struct {
	SessionID   string        `json:"session_id"`
	GeneratedAt time.Time     `json:"generated_at"`
	Items       []AnalyzeItem `json:"items"`
	Errors      []string      `json:"errors,omitempty"`
	Count       int           `json:"count"`
}

type AnalyzeItem struct {
	MarketID     string        `json:"market_id"`
	PolymarketID string        `json:"polymarket_id"`
	Question     string        `json:"question"`
	Analysis     AnalysisBlock `json:"analysis"`
	ToolCalls    []ToolCall    `json:"tool_calls"`
}

type AnalysisBlock struct {
	Status          string   `json:"status"`
	ForecastSummary string   `json:"forecast_summary"`
	MarketPriceYes  float64  `json:"market_price_yes"`
	OurProbability  float64  `json:"our_probability"`
	EdgePct         float64  `json:"edge_pct"`
	Confidence      float64  `json:"confidence"`
	Recommendation  string   `json:"recommendation"`
	Reasoning       string   `json:"reasoning"`
	RiskFactors     []string `json:"risk_factors,omitempty"`
}

type ToolCall struct {
	Name       string         `json:"name"`
	Args       map[string]any `json:"args"`
	Status     string         `json:"status"`
	DurationMS int64          `json:"duration_ms"`
	Error      string         `json:"error,omitempty"`
}

const (
	DepthSummary = "summary"
	DepthFull    = "full"
)

type CycleOptions struct {
	CycleID             string `json:"cycle_id"`
	RunMode             string `json:"run_mode"`
	TradeEnabled        bool   `json:"trade_enabled"`
	MaxToolCalls        int    `json:"max_tool_calls"`
	MaxExternalRequests int    `json:"max_external_requests"`
	MemoryWindow        int    `json:"memory_window"`
	MarketLimit         int    `json:"market_limit"`
}

type CycleResult struct {
	SessionID  string             `json:"session_id"`
	CycleID    string             `json:"cycle_id"`
	RunMode    string             `json:"run_mode"`
	Decision   string             `json:"decision"`
	Status     string             `json:"status"`
	SkipReason string             `json:"skip_reason,omitempty"`
	Model      string             `json:"model,omitempty"`
	LLMCalls   int                `json:"llm_calls"`
	ToolCalls  int                `json:"tool_calls"`
	Records    []CycleRecord      `json:"records"`
	Errors     []CycleIssue       `json:"errors,omitempty"`
	Warnings   []CycleIssue       `json:"warnings,omitempty"`
	Freshness  map[string]float64 `json:"freshness,omitempty"`
	StartedAt  time.Time          `json:"started_at"`
	FinishedAt time.Time          `json:"finished_at"`
	DurationMS int64              `json:"duration_ms"`
}

type CycleRecord struct {
	Entity string `json:"entity"`
	Result string `json:"result"`
	Count  int    `json:"count"`
}

type CycleIssue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Source  string `json:"source,omitempty"`
	Count   int    `json:"count,omitempty"`
}

type ListValidationsOptions struct {
	Limit int `json:"limit"`
}

type ValidationView struct {
	ID             uint64    `json:"id"`
	SessionID      string    `json:"session_id"`
	CycleID        string    `json:"cycle_id"`
	ValidatorModel string    `json:"validator_model"`
	Verdict        string    `json:"verdict"`
	Score          float64   `json:"score"`
	Summary        string    `json:"summary"`
	CreatedAt      time.Time `json:"created_at"`
}

type ValidationSummary struct {
	WindowHours int            `json:"window_hours"`
	Total       int64          `json:"total"`
	ByVerdict   map[string]int `json:"by_verdict"`
}

type ListValidationsResponse struct {
	Items   []ValidationView  `json:"items"`
	Count   int               `json:"count"`
	Summary ValidationSummary `json:"summary"`
}
