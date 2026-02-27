package market

import "time"

type GammaMarket struct {
	PolymarketID  string
	ConditionID   string
	Slug          string
	Question      string
	Description   string
	City          string
	MarketType    string
	OutcomeYes    string
	OutcomeNo     string
	TokenIDYes    string
	TokenIDNo     string
	EndDate       time.Time
	IsActive      bool
	IsResolved    bool
	Resolution    string
	VolumeTotal   float64
	Liquidity     float64
	PriceYes      float64
	PriceNo       float64
	RawOutcomes   []string
	RawTokenIDs   []string
	RawPricePairs []float64
}

type SyncResult struct {
	MarketsFetched  int      `json:"markets_fetched"`
	MarketsUpserted int      `json:"markets_upserted"`
	PriceSnapshots  int      `json:"price_snapshots"`
	Errors          []string `json:"errors,omitempty"`
}

type ListOptions struct {
	ActiveOnly bool
	City       string
	Limit      int
}

type MarketSnapshot struct {
	ID           string     `json:"id"`
	PolymarketID string     `json:"polymarket_id"`
	Slug         string     `json:"slug"`
	Question     string     `json:"question"`
	City         string     `json:"city"`
	MarketType   string     `json:"market_type"`
	IsActive     bool       `json:"is_active"`
	EndDate      time.Time  `json:"end_date"`
	PriceYes     float64    `json:"price_yes,omitempty"`
	PriceNo      float64    `json:"price_no,omitempty"`
	Spread       float64    `json:"spread,omitempty"`
	Volume24h    float64    `json:"volume_24h,omitempty"`
	CapturedAt   *time.Time `json:"captured_at,omitempty"`
}
