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
