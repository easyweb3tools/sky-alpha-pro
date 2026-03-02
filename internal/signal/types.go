package signal

import "time"

type GenerateOptions struct {
	Limit int
}

type GenerateResult struct {
	Processed     int            `json:"processed"`
	Generated     int            `json:"generated"`
	Skipped       int            `json:"skipped"`
	MarketsTotal  int            `json:"markets_total"`
	SpecReady     int            `json:"spec_ready"`
	ForecastReady int            `json:"forecast_ready"`
	RawEdgePass   int            `json:"raw_edge_pass"`
	ExecEdgePass  int            `json:"exec_edge_pass"`
	SkipReasons   map[string]int `json:"skip_reasons,omitempty"`
	SignalRunID   uint64         `json:"signal_run_id,omitempty"`
	Errors        []string       `json:"errors,omitempty"`
}

type SignalView struct {
	ID                    uint64    `json:"id"`
	MarketID              string    `json:"market_id"`
	SignalDate            time.Time `json:"signal_date"`
	Direction             string    `json:"direction"`
	EdgePct               float64   `json:"edge_pct"`
	EdgeExecPct           float64   `json:"edge_exec_pct"`
	Confidence            float64   `json:"confidence"`
	MarketPrice           float64   `json:"market_price"`
	MarketPriceExecutable float64   `json:"market_price_executable"`
	OurEstimate           float64   `json:"our_estimate"`
	FrictionPct           float64   `json:"friction_pct"`
	Reasoning             string    `json:"reasoning"`
	CreatedAt             time.Time `json:"created_at"`
}

type ListOptions struct {
	Limit   int
	MinEdge float64
}

type ForecastSnapshot struct {
	Source    string    `json:"source"`
	Value     float64   `json:"value"`
	FetchedAt time.Time `json:"fetched_at"`
}
