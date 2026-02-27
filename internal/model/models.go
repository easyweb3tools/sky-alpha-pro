package model

import (
	"time"

	"gorm.io/datatypes"
)

type Market struct {
	ID               string    `gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	PolymarketID     string    `gorm:"size:255;uniqueIndex;not null"`
	ConditionID      string    `gorm:"size:66"`
	Slug             string    `gorm:"size:255"`
	Question         string    `gorm:"type:text;not null"`
	Description      string    `gorm:"type:text"`
	City             string    `gorm:"size:100;index:idx_markets_city"`
	MarketType       string    `gorm:"size:50;not null;index:idx_markets_type"`
	ResolutionSource string    `gorm:"size:255"`
	TokenIDYes       string    `gorm:"size:255"`
	TokenIDNo        string    `gorm:"size:255"`
	OutcomeYes       string    `gorm:"size:255"`
	OutcomeNo        string    `gorm:"size:255"`
	EndDate          time.Time `gorm:"index:idx_markets_active"`
	IsActive         bool      `gorm:"default:true;index:idx_markets_active"`
	IsResolved       bool      `gorm:"default:false"`
	Resolution       string    `gorm:"size:10"`
	VolumeTotal      float64   `gorm:"type:decimal(18,2)"`
	Liquidity        float64   `gorm:"type:decimal(18,2)"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type MarketPrice struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement"`
	MarketID   string    `gorm:"type:uuid;not null;index:idx_market_prices_market_time,priority:1"`
	PriceYes   float64   `gorm:"type:decimal(10,4);not null"`
	PriceNo    float64   `gorm:"type:decimal(10,4);not null"`
	BidYes     float64   `gorm:"type:decimal(10,4)"`
	AskYes     float64   `gorm:"type:decimal(10,4)"`
	Spread     float64   `gorm:"type:decimal(10,4)"`
	Volume24h  float64   `gorm:"type:decimal(18,2)"`
	Source     string    `gorm:"size:20;default:clob"`
	CapturedAt time.Time `gorm:"not null;index:idx_market_prices_market_time,sort:desc,priority:2"`
}

type Forecast struct {
	ID            uint64    `gorm:"primaryKey;autoIncrement"`
	MarketID      string    `gorm:"type:uuid;index:idx_forecasts_market,priority:1"`
	StationID     string    `gorm:"size:20;index:idx_forecasts_station_date,priority:1"`
	Location      string    `gorm:"size:255;not null"`
	ForecastDate  time.Time `gorm:"type:date;not null;index:idx_forecasts_station_date,priority:2"`
	Source        string    `gorm:"size:30;not null;index:idx_forecasts_market,priority:2"`
	TempHighF     float64   `gorm:"type:decimal(5,1)"`
	TempLowF      float64   `gorm:"type:decimal(5,1)"`
	PrecipIn      float64   `gorm:"type:decimal(6,2)"`
	SnowIn        float64   `gorm:"type:decimal(6,2)"`
	WindSpeedMPH  float64   `gorm:"type:decimal(5,1)"`
	WindGustMPH   float64   `gorm:"type:decimal(5,1)"`
	CloudCoverPct int
	HumidityPct   int
	RawData       datatypes.JSON `gorm:"type:jsonb"`
	LeadDays      int
	FetchedAt     time.Time `gorm:"not null;index:idx_forecasts_market,sort:desc,priority:3"`
}

type Observation struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	StationID    string    `gorm:"size:20;not null;index:idx_observations_station_time,priority:1"`
	ObservedAt   time.Time `gorm:"not null;index:idx_observations_station_time,sort:desc,priority:2"`
	TempF        float64   `gorm:"type:decimal(5,1)"`
	TempHighF    float64   `gorm:"type:decimal(5,1)"`
	TempLowF     float64   `gorm:"type:decimal(5,1)"`
	HumidityPct  int
	WindSpeedMPH float64        `gorm:"type:decimal(5,1)"`
	WindDir      string         `gorm:"size:10"`
	PressureMB   float64        `gorm:"type:decimal(7,1)"`
	Precip1hIn   float64        `gorm:"type:decimal(6,2)"`
	VisibilityMi float64        `gorm:"type:decimal(5,1)"`
	Description  string         `gorm:"size:255"`
	RawData      datatypes.JSON `gorm:"type:jsonb"`
	FetchedAt    time.Time      `gorm:"not null"`
}

type Signal struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	MarketID    string    `gorm:"type:uuid;not null;index:idx_signals_market,priority:1"`
	SignalType  string    `gorm:"size:30;not null;index:idx_signals_type,priority:1"`
	Direction   string    `gorm:"size:10;not null"`
	EdgePct     float64   `gorm:"type:decimal(8,4);not null"`
	Confidence  float64   `gorm:"type:decimal(5,2)"`
	MarketPrice float64   `gorm:"type:decimal(10,4)"`
	OurEstimate float64   `gorm:"type:decimal(10,4)"`
	Reasoning   string    `gorm:"type:text"`
	AIModel     string    `gorm:"size:50"`
	ActedOn     bool      `gorm:"default:false"`
	CreatedAt   time.Time `gorm:"not null;index:idx_signals_market,sort:desc,priority:2;index:idx_signals_type,sort:desc,priority:2"`
}

type Trade struct {
	ID         uint64 `gorm:"primaryKey;autoIncrement"`
	MarketID   string `gorm:"type:uuid;not null;index:idx_trades_market,priority:1"`
	SignalID   *uint64
	OrderID    string  `gorm:"size:255;uniqueIndex"`
	Side       string  `gorm:"size:10;not null"`
	Outcome    string  `gorm:"size:10;not null"`
	Price      float64 `gorm:"type:decimal(10,4);not null"`
	Size       float64 `gorm:"type:decimal(18,6);not null"`
	CostUSDC   float64 `gorm:"column:cost_usdc;type:decimal(18,6);not null"`
	FeeUSDC    float64 `gorm:"column:fee_usdc;type:decimal(18,6)"`
	Status     string  `gorm:"size:20;not null;index:idx_trades_status"`
	FillPrice  float64 `gorm:"type:decimal(10,4)"`
	FillSize   float64 `gorm:"type:decimal(18,6)"`
	PnLUSDC    float64 `gorm:"column:pnl_usdc;type:decimal(18,6)"`
	TxHash     string  `gorm:"size:66"`
	ExecutedAt *time.Time
	CreatedAt  time.Time `gorm:"not null;index:idx_trades_market,sort:desc,priority:2"`
}

type Competitor struct {
	ID                  uint64  `gorm:"primaryKey;autoIncrement"`
	Address             string  `gorm:"size:42;uniqueIndex;not null"`
	Label               string  `gorm:"size:100"`
	IsBot               bool    `gorm:"default:false;index:idx_competitors_bot,priority:1"`
	BotConfidence       float64 `gorm:"type:decimal(5,2)"`
	TotalTrades         int     `gorm:"default:0"`
	TotalVolume         float64 `gorm:"type:decimal(18,2);default:0"`
	AvgTradeIntervalSec float64 `gorm:"type:decimal(10,2)"`
	FirstSeenAt         *time.Time
	LastSeenAt          *time.Time `gorm:"index:idx_competitors_bot,sort:desc,priority:2"`
	Notes               string     `gorm:"type:text"`
	CreatedAt           time.Time  `gorm:"not null"`
	UpdatedAt           time.Time
}

type CompetitorTrade struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	CompetitorID uint64    `gorm:"not null;index:idx_competitor_trades_comp,priority:1"`
	MarketID     string    `gorm:"type:uuid;index:idx_competitor_trades_market,priority:1"`
	TxHash       string    `gorm:"size:66;not null;uniqueIndex"`
	BlockNumber  uint64    `gorm:"not null"`
	Side         string    `gorm:"size:10"`
	Outcome      string    `gorm:"size:10"`
	AmountUSDC   float64   `gorm:"column:amount_usdc;type:decimal(18,6)"`
	GasPriceGwei float64   `gorm:"type:decimal(10,4)"`
	Timestamp    time.Time `gorm:"not null;index:idx_competitor_trades_comp,sort:desc,priority:2;index:idx_competitor_trades_market,sort:desc,priority:2"`
	CreatedAt    time.Time `gorm:"not null"`
}

type Player struct {
	ID             uint64  `gorm:"primaryKey;autoIncrement"`
	Address        string  `gorm:"size:42;uniqueIndex;not null"`
	Username       string  `gorm:"size:100"`
	TotalPnL       float64 `gorm:"column:total_pnl;type:decimal(18,2)"`
	WinRate        float64 `gorm:"type:decimal(5,2)"`
	TotalMarkets   int     `gorm:"default:0"`
	WeatherMarkets int     `gorm:"default:0"`
	RankOverall    int
	RankWeather    int `gorm:"index:idx_players_rank"`
	LastActiveAt   *time.Time
	CreatedAt      time.Time `gorm:"not null"`
	UpdatedAt      time.Time
}

type PlayerPosition struct {
	ID           uint64  `gorm:"primaryKey;autoIncrement"`
	PlayerID     uint64  `gorm:"not null;index:idx_player_positions_player"`
	MarketID     string  `gorm:"type:uuid;not null;index:idx_player_positions_market"`
	Outcome      string  `gorm:"size:10;not null"`
	Size         float64 `gorm:"type:decimal(18,6);not null"`
	AvgPrice     float64 `gorm:"type:decimal(10,4)"`
	CurrentValue float64 `gorm:"type:decimal(18,6)"`
	PnL          float64 `gorm:"type:decimal(18,6)"`
	FirstEntryAt *time.Time
	LastUpdateAt time.Time
}

type WeatherStation struct {
	ID         string  `gorm:"size:20;primaryKey"`
	Name       string  `gorm:"size:255;not null"`
	City       string  `gorm:"size:100;not null"`
	State      string  `gorm:"size:50"`
	Latitude   float64 `gorm:"type:decimal(9,6);not null"`
	Longitude  float64 `gorm:"type:decimal(9,6);not null"`
	ElevationM float64 `gorm:"type:decimal(7,1)"`
	NWSGridWFO string  `gorm:"size:10"`
	NWSGridX   int
	NWSGridY   int
	IsActive   bool `gorm:"default:true"`
}

type AgentLog struct {
	ID               uint64 `gorm:"primaryKey;autoIncrement"`
	SessionID        string `gorm:"size:100;index:idx_agent_logs_session,priority:1"`
	MarketID         string `gorm:"type:uuid;index:idx_agent_logs_market,priority:1"`
	Action           string `gorm:"size:50;not null"`
	Model            string `gorm:"size:50"`
	PromptTokens     int
	CompletionTokens int
	ToolCalls        datatypes.JSON `gorm:"type:jsonb"`
	Reasoning        string         `gorm:"type:text"`
	Result           datatypes.JSON `gorm:"type:jsonb"`
	DurationMS       int
	CreatedAt        time.Time `gorm:"not null;index:idx_agent_logs_session,priority:2;index:idx_agent_logs_market,sort:desc,priority:2"`
}
