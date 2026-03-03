package model

import (
	"time"

	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
)

type Market struct {
	ID                string              `gorm:"column:id;type:uuid;default:gen_random_uuid();primaryKey"`
	PolymarketID      string              `gorm:"column:polymarket_id;size:255;uniqueIndex;not null"`
	ConditionID       string              `gorm:"column:condition_id;size:66"`
	Slug              string              `gorm:"column:slug;size:255"`
	Question          string              `gorm:"column:question;type:text;not null"`
	Description       string              `gorm:"column:description;type:text"`
	City              string              `gorm:"column:city;size:100;index:idx_markets_city"`
	MarketType        string              `gorm:"column:market_type;size:50;not null;index:idx_markets_type"`
	ResolutionSource  string              `gorm:"column:resolution_source;size:255"`
	TokenIDYes        string              `gorm:"column:token_id_yes;size:255"`
	TokenIDNo         string              `gorm:"column:token_id_no;size:255"`
	OutcomeYes        string              `gorm:"column:outcome_yes;size:255"`
	OutcomeNo         string              `gorm:"column:outcome_no;size:255"`
	EndDate           time.Time           `gorm:"column:end_date;index:idx_markets_active"`
	IsActive          bool                `gorm:"column:is_active;default:true;index:idx_markets_active"`
	IsResolved        bool                `gorm:"column:is_resolved;default:false"`
	Resolution        string              `gorm:"column:resolution;size:10"`
	VolumeTotal       decimal.NullDecimal `gorm:"column:volume_total;type:decimal(18,2)"`
	Liquidity         decimal.NullDecimal `gorm:"column:liquidity;type:decimal(18,2)"`
	ThresholdF        decimal.NullDecimal `gorm:"column:threshold_f;type:decimal(6,2)"`
	Comparator        string              `gorm:"column:comparator;size:8"`
	WeatherTargetDate *time.Time          `gorm:"column:weather_target_date;type:date;index:idx_markets_weather_target_date"`
	SpecStatus        string              `gorm:"column:spec_status;size:20;default:incomplete;index:idx_markets_spec_status"`
	CreatedAt         time.Time           `gorm:"column:created_at"`
	UpdatedAt         time.Time           `gorm:"column:updated_at"`
}

type MarketPrice struct {
	ID         uint64              `gorm:"column:id;primaryKey;autoIncrement"`
	MarketID   string              `gorm:"column:market_id;type:uuid;not null;index:idx_market_prices_market_time,priority:1"`
	PriceYes   decimal.Decimal     `gorm:"column:price_yes;type:decimal(10,4);not null"`
	PriceNo    decimal.Decimal     `gorm:"column:price_no;type:decimal(10,4);not null"`
	BidYes     decimal.NullDecimal `gorm:"column:bid_yes;type:decimal(10,4)"`
	AskYes     decimal.NullDecimal `gorm:"column:ask_yes;type:decimal(10,4)"`
	Spread     decimal.NullDecimal `gorm:"column:spread;type:decimal(10,4)"`
	Volume24h  decimal.NullDecimal `gorm:"column:volume_24h;type:decimal(18,2)"`
	Source     string              `gorm:"column:source;size:20;default:clob"`
	CapturedAt time.Time           `gorm:"column:captured_at;not null;index:idx_market_prices_market_time,sort:desc,priority:2"`
}

type Forecast struct {
	ID            uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	MarketID      *string        `gorm:"column:market_id;type:uuid;index:idx_forecasts_market,priority:1"`
	StationID     string         `gorm:"column:station_id;size:20;index:idx_forecasts_station_date,priority:1"`
	Location      string         `gorm:"column:location;size:255;not null;uniqueIndex:idx_forecasts_unique,priority:1"`
	City          string         `gorm:"column:city;size:100;index:idx_forecasts_city_date_source,priority:1"`
	ForecastDate  time.Time      `gorm:"column:forecast_date;type:date;not null;index:idx_forecasts_station_date,priority:2;index:idx_forecasts_city_date_source,priority:2;uniqueIndex:idx_forecasts_unique,priority:2"`
	Source        string         `gorm:"column:source;size:30;not null;index:idx_forecasts_market,priority:2;index:idx_forecasts_city_date_source,priority:3;uniqueIndex:idx_forecasts_unique,priority:3"`
	TempHighF     float64        `gorm:"column:temp_high_f;type:decimal(5,1)"`
	TempLowF      float64        `gorm:"column:temp_low_f;type:decimal(5,1)"`
	PrecipIn      float64        `gorm:"column:precip_in;type:decimal(6,2)"`
	SnowIn        float64        `gorm:"column:snow_in;type:decimal(6,2)"`
	WindSpeedMPH  float64        `gorm:"column:wind_speed_mph;type:decimal(5,1)"`
	WindGustMPH   float64        `gorm:"column:wind_gust_mph;type:decimal(5,1)"`
	CloudCoverPct int            `gorm:"column:cloud_cover_pct"`
	HumidityPct   int            `gorm:"column:humidity_pct"`
	RawData       datatypes.JSON `gorm:"column:raw_data;type:jsonb"`
	LeadDays      int            `gorm:"column:lead_days"`
	FetchedAt     time.Time      `gorm:"column:fetched_at;not null;index:idx_forecasts_market,sort:desc,priority:3"`
}

type Observation struct {
	ID           uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	StationID    string         `gorm:"column:station_id;size:20;not null;index:idx_observations_station_time,priority:1"`
	ObservedAt   time.Time      `gorm:"column:observed_at;not null;index:idx_observations_station_time,sort:desc,priority:2"`
	TempF        float64        `gorm:"column:temp_f;type:decimal(5,1)"`
	TempHighF    float64        `gorm:"column:temp_high_f;type:decimal(5,1)"`
	TempLowF     float64        `gorm:"column:temp_low_f;type:decimal(5,1)"`
	HumidityPct  int            `gorm:"column:humidity_pct"`
	WindSpeedMPH float64        `gorm:"column:wind_speed_mph;type:decimal(5,1)"`
	WindDir      string         `gorm:"column:wind_dir;size:10"`
	PressureMB   float64        `gorm:"column:pressure_mb;type:decimal(7,1)"`
	Precip1hIn   float64        `gorm:"column:precip_1h_in;type:decimal(6,2)"`
	VisibilityMi float64        `gorm:"column:visibility_mi;type:decimal(5,1)"`
	Description  string         `gorm:"column:description;size:255"`
	RawData      datatypes.JSON `gorm:"column:raw_data;type:jsonb"`
	FetchedAt    time.Time      `gorm:"column:fetched_at;not null"`
}

type Signal struct {
	ID                    uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	MarketID              string    `gorm:"column:market_id;type:uuid;not null;index:idx_signals_market,priority:1"`
	SignalDate            time.Time `gorm:"column:signal_date;type:date;index:idx_signals_market_date,priority:2"`
	SignalType            string    `gorm:"column:signal_type;size:30;not null;index:idx_signals_type,priority:1"`
	Direction             string    `gorm:"column:direction;size:10;not null"`
	EdgePct               float64   `gorm:"column:edge_pct;type:decimal(8,4);not null"`
	Confidence            float64   `gorm:"column:confidence;type:decimal(5,2)"`
	MarketPrice           float64   `gorm:"column:market_price;type:decimal(10,4)"`
	MarketPriceExecutable float64   `gorm:"column:market_price_executable;type:decimal(10,4)"`
	OurEstimate           float64   `gorm:"column:our_estimate;type:decimal(10,4)"`
	FrictionPct           float64   `gorm:"column:friction_pct;type:decimal(8,4)"`
	EdgeExecPct           float64   `gorm:"column:edge_exec_pct;type:decimal(8,4)"`
	Reasoning             string    `gorm:"column:reasoning;type:text"`
	AIModel               string    `gorm:"column:ai_model;size:50"`
	ActedOn               bool      `gorm:"column:acted_on;default:false"`
	CreatedAt             time.Time `gorm:"column:created_at;not null;index:idx_signals_market,sort:desc,priority:2;index:idx_signals_type,sort:desc,priority:2;index:idx_signals_market_date,sort:desc,priority:3"`
}

type SignalRun struct {
	ID               uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	StartedAt        time.Time      `gorm:"column:started_at;not null;index:idx_signal_runs_time,sort:desc,priority:1"`
	FinishedAt       time.Time      `gorm:"column:finished_at;not null"`
	DurationMS       int            `gorm:"column:duration_ms;not null"`
	MarketsTotal     int            `gorm:"column:markets_total;default:0"`
	SpecReady        int            `gorm:"column:spec_ready;default:0"`
	ForecastReady    int            `gorm:"column:forecast_ready;default:0"`
	RawEdgePass      int            `gorm:"column:raw_edge_pass;default:0"`
	ExecEdgePass     int            `gorm:"column:exec_edge_pass;default:0"`
	SignalsGenerated int            `gorm:"column:signals_generated;default:0"`
	Skipped          int            `gorm:"column:skipped;default:0"`
	SkipReasonsJSON  datatypes.JSON `gorm:"column:skip_reasons_json;type:jsonb"`
	CreatedAt        time.Time      `gorm:"column:created_at;not null;index:idx_signal_runs_time,sort:desc,priority:2"`
}

type CityResolutionCache struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	CacheKey  string    `gorm:"column:cache_key;size:64;uniqueIndex;not null"`
	Question  string    `gorm:"column:question;type:text;not null"`
	Slug      string    `gorm:"column:slug;size:255"`
	City      string    `gorm:"column:city;size:100;not null;index:idx_city_resolution_city"`
	Source    string    `gorm:"column:source;size:20;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

type Trade struct {
	ID         uint64              `gorm:"column:id;primaryKey;autoIncrement"`
	MarketID   string              `gorm:"column:market_id;type:uuid;not null;index:idx_trades_market,priority:1"`
	SignalID   *uint64             `gorm:"column:signal_id"`
	OrderID    string              `gorm:"column:order_id;size:255;uniqueIndex"`
	Side       string              `gorm:"column:side;size:10;not null"`
	Outcome    string              `gorm:"column:outcome;size:10;not null"`
	Price      decimal.Decimal     `gorm:"column:price;type:decimal(10,4);not null"`
	Size       decimal.Decimal     `gorm:"column:size;type:decimal(18,6);not null"`
	CostUSDC   decimal.Decimal     `gorm:"column:cost_usdc;type:decimal(18,6);not null"`
	FeeUSDC    decimal.NullDecimal `gorm:"column:fee_usdc;type:decimal(18,6)"`
	Status     string              `gorm:"column:status;size:20;not null;index:idx_trades_status"`
	FillPrice  decimal.NullDecimal `gorm:"column:fill_price;type:decimal(10,4)"`
	FillSize   decimal.NullDecimal `gorm:"column:fill_size;type:decimal(18,6)"`
	PnLUSDC    decimal.NullDecimal `gorm:"column:pnl_usdc;type:decimal(18,6)"`
	IsPaper    bool                `gorm:"column:is_paper;default:false;index:idx_trades_paper"`
	TxHash     string              `gorm:"column:tx_hash;size:66"`
	ExecutedAt *time.Time          `gorm:"column:executed_at"`
	CreatedAt  time.Time           `gorm:"column:created_at;not null;index:idx_trades_market,sort:desc,priority:2"`
}

type Competitor struct {
	ID                  uint64              `gorm:"column:id;primaryKey;autoIncrement"`
	Address             string              `gorm:"column:address;size:42;uniqueIndex;not null"`
	Label               string              `gorm:"column:label;size:100"`
	IsBot               bool                `gorm:"column:is_bot;default:false;index:idx_competitors_bot,priority:1"`
	BotConfidence       float64             `gorm:"column:bot_confidence;type:decimal(5,2)"`
	TotalTrades         int                 `gorm:"column:total_trades;default:0"`
	TotalVolume         decimal.NullDecimal `gorm:"column:total_volume;type:decimal(18,2)"`
	AvgTradeIntervalSec float64             `gorm:"column:avg_trade_interval_sec;type:decimal(10,2)"`
	FirstSeenAt         *time.Time          `gorm:"column:first_seen_at"`
	LastSeenAt          *time.Time          `gorm:"column:last_seen_at;index:idx_competitors_bot,sort:desc,priority:2"`
	Notes               string              `gorm:"column:notes;type:text"`
	CreatedAt           time.Time           `gorm:"column:created_at;not null"`
	UpdatedAt           time.Time           `gorm:"column:updated_at"`
}

type CompetitorTrade struct {
	ID           uint64              `gorm:"column:id;primaryKey;autoIncrement"`
	CompetitorID uint64              `gorm:"column:competitor_id;not null;index:idx_competitor_trades_comp,priority:1"`
	MarketID     *string             `gorm:"column:market_id;type:uuid;index:idx_competitor_trades_market,priority:1"`
	TxHash       string              `gorm:"column:tx_hash;size:66;not null;uniqueIndex"`
	BlockNumber  uint64              `gorm:"column:block_number;not null"`
	Side         string              `gorm:"column:side;size:10"`
	Outcome      string              `gorm:"column:outcome;size:10"`
	AmountUSDC   decimal.NullDecimal `gorm:"column:amount_usdc;type:decimal(18,6)"`
	GasPriceGwei decimal.NullDecimal `gorm:"column:gas_price_gwei;type:decimal(10,4)"`
	Timestamp    time.Time           `gorm:"column:timestamp;not null;index:idx_competitor_trades_comp,sort:desc,priority:2;index:idx_competitor_trades_market,sort:desc,priority:2"`
	CreatedAt    time.Time           `gorm:"column:created_at;not null"`
}

type Player struct {
	ID             uint64              `gorm:"column:id;primaryKey;autoIncrement"`
	Address        string              `gorm:"column:address;size:42;uniqueIndex;not null"`
	Username       string              `gorm:"column:username;size:100"`
	TotalPnL       decimal.NullDecimal `gorm:"column:total_pnl;type:decimal(18,2)"`
	TotalVolume    decimal.NullDecimal `gorm:"column:total_volume;type:decimal(18,2)"`
	WinRate        float64             `gorm:"column:win_rate;type:decimal(5,2)"`
	TotalMarkets   int                 `gorm:"column:total_markets;default:0"`
	WeatherMarkets int                 `gorm:"column:weather_markets;default:0"`
	RankOverall    int                 `gorm:"column:rank_overall"`
	RankWeather    int                 `gorm:"column:rank_weather;index:idx_players_rank"`
	LastActiveAt   *time.Time          `gorm:"column:last_active_at"`
	CreatedAt      time.Time           `gorm:"column:created_at;not null"`
	UpdatedAt      time.Time           `gorm:"column:updated_at"`
}

type PlayerPosition struct {
	ID           uint64              `gorm:"column:id;primaryKey;autoIncrement"`
	PlayerID     uint64              `gorm:"column:player_id;not null;index:idx_player_positions_player;uniqueIndex:idx_player_positions_unique,priority:1"`
	MarketID     string              `gorm:"column:market_id;type:uuid;not null;index:idx_player_positions_market;uniqueIndex:idx_player_positions_unique,priority:2"`
	Outcome      string              `gorm:"column:outcome;size:10;not null;uniqueIndex:idx_player_positions_unique,priority:3"`
	Size         decimal.Decimal     `gorm:"column:size;type:decimal(18,6);not null"`
	AvgPrice     decimal.NullDecimal `gorm:"column:avg_price;type:decimal(10,4)"`
	CurrentValue decimal.NullDecimal `gorm:"column:current_value;type:decimal(18,6)"`
	PnL          decimal.NullDecimal `gorm:"column:pnl;type:decimal(18,6)"`
	FirstEntryAt *time.Time          `gorm:"column:first_entry_at"`
	LastUpdateAt time.Time           `gorm:"column:last_update_at"`
}

type WeatherStation struct {
	ID         string  `gorm:"column:id;size:20;primaryKey"`
	Name       string  `gorm:"column:name;size:255;not null"`
	City       string  `gorm:"column:city;size:100;not null"`
	State      string  `gorm:"column:state;size:50"`
	Latitude   float64 `gorm:"column:latitude;type:decimal(9,6);not null"`
	Longitude  float64 `gorm:"column:longitude;type:decimal(9,6);not null"`
	ElevationM float64 `gorm:"column:elevation_m;type:decimal(7,1)"`
	NWSGridWFO string  `gorm:"column:nws_grid_wfo;size:10"`
	NWSGridX   int     `gorm:"column:nws_grid_x"`
	NWSGridY   int     `gorm:"column:nws_grid_y"`
	IsActive   bool    `gorm:"column:is_active;default:true"`
}

type AgentLog struct {
	ID        uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	SessionID string `gorm:"column:session_id;size:100;index:idx_agent_logs_session,priority:1"`
	MarketID  string `gorm:"column:market_id;type:uuid;index:idx_agent_logs_market,priority:1"`
	Action    string `gorm:"column:action;size:50;not null"`
	Model     string `gorm:"column:model;size:50"`
	// Rule-based agent mode does not consume LLM tokens; kept for future Gemini integration.
	PromptTokens int `gorm:"column:prompt_tokens"`
	// Rule-based agent mode does not consume LLM tokens; kept for future Gemini integration.
	CompletionTokens int            `gorm:"column:completion_tokens"`
	ToolCalls        datatypes.JSON `gorm:"column:tool_calls;type:jsonb"`
	Reasoning        string         `gorm:"column:reasoning;type:text"`
	Result           datatypes.JSON `gorm:"column:result;type:jsonb"`
	DurationMS       int            `gorm:"column:duration_ms"`
	CreatedAt        time.Time      `gorm:"column:created_at;not null;index:idx_agent_logs_session,priority:2;index:idx_agent_logs_market,sort:desc,priority:2"`
}

type PromptVersion struct {
	ID              uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	Name            string         `gorm:"column:name;size:80;not null;index:idx_prompt_versions_name_ver,priority:1"`
	Version         string         `gorm:"column:version;size:40;not null;index:idx_prompt_versions_name_ver,priority:2"`
	SystemPrompt    string         `gorm:"column:system_prompt;type:text;not null"`
	RuntimeTemplate string         `gorm:"column:runtime_template;type:text;not null"`
	ContextTemplate string         `gorm:"column:context_template;type:text;not null"`
	SchemaJSON      datatypes.JSON `gorm:"column:schema_json;type:jsonb"`
	IsActive        bool           `gorm:"column:is_active;default:false;index:idx_prompt_versions_active"`
	CreatedAt       time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt       time.Time      `gorm:"column:updated_at"`
}

type AgentSession struct {
	ID               string         `gorm:"column:id;type:uuid;primaryKey"`
	CycleID          string         `gorm:"column:cycle_id;size:64;index:idx_agent_sessions_cycle"`
	PromptVersion    string         `gorm:"column:prompt_version;size:40;not null"`
	RunMode          string         `gorm:"column:run_mode;size:20;not null"`
	Model            string         `gorm:"column:model;size:80"`
	Status           string         `gorm:"column:status;size:20;not null;index:idx_agent_sessions_status"`
	Decision         string         `gorm:"column:decision;size:20"`
	LLMCalls         int            `gorm:"column:llm_calls;default:0"`
	ToolCalls        int            `gorm:"column:tool_calls;default:0"`
	RecordsSuccess   int            `gorm:"column:records_success;default:0"`
	RecordsError     int            `gorm:"column:records_error;default:0"`
	RecordsSkipped   int            `gorm:"column:records_skipped;default:0"`
	ErrorCode        string         `gorm:"column:error_code;size:80"`
	ErrorMessage     string         `gorm:"column:error_message;type:text"`
	InputContextJSON datatypes.JSON `gorm:"column:input_context_json;type:jsonb"`
	OutputPlanJSON   datatypes.JSON `gorm:"column:output_plan_json;type:jsonb"`
	SummaryJSON      datatypes.JSON `gorm:"column:summary_json;type:jsonb"`
	StartedAt        time.Time      `gorm:"column:started_at;not null;index:idx_agent_sessions_started_at,sort:desc"`
	FinishedAt       time.Time      `gorm:"column:finished_at;not null"`
	DurationMS       int            `gorm:"column:duration_ms;not null"`
	CreatedAt        time.Time      `gorm:"column:created_at;not null"`
	UpdatedAt        time.Time      `gorm:"column:updated_at"`
}

type AgentStep struct {
	ID          uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	SessionID   string         `gorm:"column:session_id;type:uuid;not null;index:idx_agent_steps_session_step,priority:1"`
	StepNo      int            `gorm:"column:step_no;not null;index:idx_agent_steps_session_step,priority:2"`
	Tool        string         `gorm:"column:tool;size:80;not null"`
	Status      string         `gorm:"column:status;size:20;not null;index:idx_agent_steps_status"`
	OnFail      string         `gorm:"column:on_fail;size:20"`
	ErrorCode   string         `gorm:"column:error_code;size:80"`
	ErrorDetail string         `gorm:"column:error_detail;type:text"`
	ArgsJSON    datatypes.JSON `gorm:"column:args_json;type:jsonb"`
	ResultJSON  datatypes.JSON `gorm:"column:result_json;type:jsonb"`
	StartedAt   time.Time      `gorm:"column:started_at;not null"`
	FinishedAt  time.Time      `gorm:"column:finished_at;not null"`
	DurationMS  int            `gorm:"column:duration_ms;not null"`
	CreatedAt   time.Time      `gorm:"column:created_at;not null"`
}

type AgentMemory struct {
	ID             uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	SessionID      string         `gorm:"column:session_id;type:uuid;not null;index:idx_agent_memories_session"`
	CycleID        string         `gorm:"column:cycle_id;size:64;index:idx_agent_memories_cycle"`
	Outcome        string         `gorm:"column:outcome;size:20;not null"`
	KeyFailures    datatypes.JSON `gorm:"column:key_failures_json;type:jsonb"`
	FunnelSummary  datatypes.JSON `gorm:"column:funnel_summary_json;type:jsonb"`
	ActionSuggests datatypes.JSON `gorm:"column:action_suggestions_json;type:jsonb"`
	ExecutionJSON  datatypes.JSON `gorm:"column:execution_outcome_json;type:jsonb"`
	CreatedAt      time.Time      `gorm:"column:created_at;not null;index:idx_agent_memories_created,sort:desc"`
}

type AgentReport struct {
	ID          uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	SessionID   string         `gorm:"column:session_id;type:uuid;not null;index:idx_agent_reports_session,priority:1"`
	CycleID     string         `gorm:"column:cycle_id;size:64;index:idx_agent_reports_cycle"`
	RunMode     string         `gorm:"column:run_mode;size:20;not null"`
	Decision    string         `gorm:"column:decision;size:20"`
	Status      string         `gorm:"column:status;size:20;not null;index:idx_agent_reports_status"`
	SummaryJSON datatypes.JSON `gorm:"column:summary_json;type:jsonb"`
	FunnelJSON  datatypes.JSON `gorm:"column:funnel_json;type:jsonb"`
	CreatedAt   time.Time      `gorm:"column:created_at;not null;index:idx_agent_reports_session,sort:desc,priority:2"`
	UpdatedAt   time.Time      `gorm:"column:updated_at"`
}

type SchedulerRun struct {
	ID             uint64         `gorm:"column:id;primaryKey;autoIncrement"`
	JobName        string         `gorm:"column:job_name;size:80;not null;index:idx_scheduler_runs_job_time,priority:1"`
	StartedAt      time.Time      `gorm:"column:started_at;not null;index:idx_scheduler_runs_job_time,sort:desc,priority:2"`
	FinishedAt     time.Time      `gorm:"column:finished_at;not null"`
	DurationMS     int            `gorm:"column:duration_ms;not null"`
	Status         string         `gorm:"column:status;size:30;not null;index:idx_scheduler_runs_status_time,priority:1"`
	RecordsSuccess int            `gorm:"column:records_success;default:0"`
	RecordsError   int            `gorm:"column:records_error;default:0"`
	RecordsSkipped int            `gorm:"column:records_skipped;default:0"`
	ErrorCode      string         `gorm:"column:error_code;size:80"`
	ErrorMessage   string         `gorm:"column:error_message;type:text"`
	MetaJSON       datatypes.JSON `gorm:"column:meta_json;type:jsonb"`
	CreatedAt      time.Time      `gorm:"column:created_at;not null;index:idx_scheduler_runs_status_time,sort:desc,priority:2"`
}
