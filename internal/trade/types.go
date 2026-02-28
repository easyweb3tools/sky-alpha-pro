package trade

import "time"

type SubmitOrderRequest struct {
	MarketRef string  `json:"market_id"`
	Side      string  `json:"side"`
	Outcome   string  `json:"outcome"`
	Price     float64 `json:"price"`
	Size      float64 `json:"size"`
	Confirm   bool    `json:"confirm"`
	SignalID  *uint64 `json:"signal_id,omitempty"`
}

type SubmitOrderResult struct {
	TradeID    uint64    `json:"trade_id"`
	OrderID    string    `json:"order_id"`
	Status     string    `json:"status"`
	MarketID   string    `json:"market_id"`
	Side       string    `json:"side"`
	Outcome    string    `json:"outcome"`
	Price      float64   `json:"price"`
	Size       float64   `json:"size"`
	CostUSDC   float64   `json:"cost_usdc"`
	Signature  string    `json:"signature,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	ExecutedAt time.Time `json:"executed_at,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type CancelOrderResult struct {
	TradeID   uint64    `json:"trade_id"`
	OrderID   string    `json:"order_id"`
	Status    string    `json:"status"`
	Cancelled bool      `json:"cancelled"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ListTradesOptions struct {
	Limit    int
	Status   string
	MarketID string
	IsPaper  *bool
}

type ListPositionsOptions struct {
	MarketID string
	IsPaper  *bool
}

type PositionView struct {
	MarketID       string    `json:"market_id"`
	Outcome        string    `json:"outcome"`
	NetSize        float64   `json:"net_size"`
	AvgEntryPrice  float64   `json:"avg_entry_price"`
	MarkPrice      *float64  `json:"mark_price,omitempty"`
	MarketValueUSD *float64  `json:"market_value_usd,omitempty"`
	UnrealizedPnL  *float64  `json:"unrealized_pnl,omitempty"`
	RealizedPnL    float64   `json:"realized_pnl"`
	IsPaper        bool      `json:"is_paper"`
	LatestTradeAt  time.Time `json:"latest_trade_at"`
}

type PnLReportOptions struct {
	From    time.Time
	To      time.Time
	IsPaper *bool
}

type DailyPnL struct {
	Date         string  `json:"date"`
	RealizedPnL  float64 `json:"realized_pnl"`
	GrossVolume  float64 `json:"gross_volume"`
	FilledTrades int64   `json:"filled_trades"`
}

type PnLReport struct {
	From             time.Time  `json:"from"`
	To               time.Time  `json:"to"`
	TotalTrades      int64      `json:"total_trades"`
	FilledTrades     int64      `json:"filled_trades"`
	WinTrades        int64      `json:"win_trades"`
	LossTrades       int64      `json:"loss_trades"`
	BreakEvenTrades  int64      `json:"break_even_trades"`
	WinRate          float64    `json:"win_rate"`
	GrossVolumeUSDC  float64    `json:"gross_volume_usdc"`
	RealizedPnLUSDC  float64    `json:"realized_pnl_usdc"`
	OpenExposureUSDC float64    `json:"open_exposure_usdc"`
	Daily            []DailyPnL `json:"daily"`
}

type TradeView struct {
	ID         uint64     `json:"id"`
	MarketID   string     `json:"market_id"`
	SignalID   *uint64    `json:"signal_id,omitempty"`
	OrderID    string     `json:"order_id"`
	Side       string     `json:"side"`
	Outcome    string     `json:"outcome"`
	Price      float64    `json:"price"`
	Size       float64    `json:"size"`
	CostUSDC   float64    `json:"cost_usdc"`
	FeeUSDC    *float64   `json:"fee_usdc,omitempty"`
	Status     string     `json:"status"`
	FillPrice  *float64   `json:"fill_price,omitempty"`
	FillSize   *float64   `json:"fill_size,omitempty"`
	PnLUSDC    *float64   `json:"pnl_usdc,omitempty"`
	TxHash     string     `json:"tx_hash,omitempty"`
	IsPaper    bool       `json:"is_paper"`
	ExecutedAt *time.Time `json:"executed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

type SignableOrder struct {
	Salt          string
	Maker         string
	Signer        string
	Taker         string
	TokenID       string
	MakerAmount   string
	TakerAmount   string
	Expiration    string
	Nonce         string
	FeeRateBps    string
	Side          uint8
	SignatureType uint8
}

type SignedClobOrder struct {
	Salt          string `json:"salt"`
	Maker         string `json:"maker"`
	Signer        string `json:"signer"`
	Taker         string `json:"taker"`
	TokenID       string `json:"tokenId"`
	MakerAmount   string `json:"makerAmount"`
	TakerAmount   string `json:"takerAmount"`
	Expiration    string `json:"expiration"`
	Nonce         string `json:"nonce"`
	FeeRateBps    string `json:"feeRateBps"`
	Side          string `json:"side"`
	SignatureType uint8  `json:"signatureType"`
	Signature     string `json:"signature"`
}
