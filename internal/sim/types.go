package sim

import "time"

type RunConfig struct {
	Interval     time.Duration
	OrderSize    float64
	MinEdgePct   float64
	MaxPositions int
	AutoSettle   bool
	MaxCycles    int
}

type RunCycleOptions struct {
	SkipMarketSync bool
}

type CycleResult struct {
	Cycle            int      `json:"cycle"`
	MarketsSynced    int      `json:"markets_synced"`
	SignalsGenerated int      `json:"signals_generated"`
	TradesPlaced     int      `json:"trades_placed"`
	TradesSettled    int      `json:"trades_settled"`
	Errors           []string `json:"errors,omitempty"`
}

type ReportOptions struct {
	From time.Time
	To   time.Time
}

type SimReport struct {
	From             time.Time     `json:"from"`
	To               time.Time     `json:"to"`
	TotalTrades      int64         `json:"total_trades"`
	FilledTrades     int64         `json:"filled_trades"`
	WinTrades        int64         `json:"win_trades"`
	LossTrades       int64         `json:"loss_trades"`
	WinRate          float64       `json:"win_rate"`
	RealizedPnLUSDC  float64       `json:"realized_pnl_usdc"`
	UnrealizedPnL    float64       `json:"unrealized_pnl"`
	PendingTrades    int64         `json:"pending_trades"`
	AvgEdgeWinning   float64       `json:"avg_edge_winning"`
	AvgEdgeLosing    float64       `json:"avg_edge_losing"`
	GrossVolumeUSDC  float64       `json:"gross_volume_usdc"`
	OpenExposureUSDC float64       `json:"open_exposure_usdc"`
	Daily            []DailySimPnL `json:"daily,omitempty"`
}

type DailySimPnL struct {
	Date         string  `json:"date"`
	RealizedPnL  float64 `json:"realized_pnl"`
	GrossVolume  float64 `json:"gross_volume"`
	FilledTrades int64   `json:"filled_trades"`
}
