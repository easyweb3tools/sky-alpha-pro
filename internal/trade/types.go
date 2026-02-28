package trade

import "time"

type SubmitOrderRequest struct {
	MarketRef string  `json:"market_id"`
	Side      string  `json:"side"`
	Outcome   string  `json:"outcome"`
	Price     float64 `json:"price"`
	Size      float64 `json:"size"`
	Confirm   bool    `json:"confirm"`
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
	Limit int
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
	ExecutedAt *time.Time `json:"executed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

type SignableOrder struct {
	MarketID   string
	Side       string
	Outcome    string
	Price      string
	Size       string
	Nonce      uint64
	Expiration int64
}
