package chain

import "time"

type ScanOptions struct {
	LookbackBlocks       uint64  `json:"lookback_blocks"`
	MaxTx                int     `json:"max_tx"`
	BotMinTrades         int     `json:"bot_min_trades"`
	BotMaxAvgIntervalSec float64 `json:"bot_max_avg_interval_sec"`
}

type ScanResult struct {
	FromBlock         uint64    `json:"from_block"`
	ToBlock           uint64    `json:"to_block"`
	ObservedLogs      int       `json:"observed_logs"`
	ObservedTx        int       `json:"observed_tx"`
	InsertedTrades    int       `json:"inserted_trades"`
	DuplicateTrades   int       `json:"duplicate_trades"`
	UpdatedCompetitor int       `json:"updated_competitor"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
}

type ListCompetitorsOptions struct {
	OnlyBots bool
	Limit    int
}

type CompetitorView struct {
	Address             string     `json:"address"`
	Label               string     `json:"label,omitempty"`
	IsBot               bool       `json:"is_bot"`
	BotConfidence       float64    `json:"bot_confidence"`
	TotalTrades         int        `json:"total_trades"`
	TotalVolume         *float64   `json:"total_volume,omitempty"`
	AvgTradeIntervalSec float64    `json:"avg_trade_interval_sec"`
	FirstSeenAt         *time.Time `json:"first_seen_at,omitempty"`
	LastSeenAt          *time.Time `json:"last_seen_at,omitempty"`
	Notes               string     `json:"notes,omitempty"`
}

type CompetitorTradeView struct {
	TxHash       string    `json:"tx_hash"`
	BlockNumber  uint64    `json:"block_number"`
	MarketID     string    `json:"market_id,omitempty"`
	Side         string    `json:"side,omitempty"`
	Outcome      string    `json:"outcome,omitempty"`
	AmountUSDC   *float64  `json:"amount_usdc,omitempty"`
	GasPriceGwei *float64  `json:"gas_price_gwei,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
	CreatedAt    time.Time `json:"created_at"`
}
