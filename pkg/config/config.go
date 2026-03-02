package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	App       AppConfig       `mapstructure:"app"`
	Server    ServerConfig    `mapstructure:"server"`
	Market    MarketConfig    `mapstructure:"market"`
	Weather   WeatherConfig   `mapstructure:"weather"`
	Signal    SignalConfig    `mapstructure:"signal"`
	Agent     AgentConfig     `mapstructure:"agent"`
	Trade     TradeConfig     `mapstructure:"trade"`
	Chain     ChainConfig     `mapstructure:"chain"`
	Sim       SimConfig       `mapstructure:"sim"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	Database  DatabaseConfig  `mapstructure:"database"`
	Log       LogConfig       `mapstructure:"log"`
}

type AppConfig struct {
	Name    string `mapstructure:"name"`
	Env     string `mapstructure:"env"`
	Version string `mapstructure:"version"`
}

type ServerConfig struct {
	Host              string        `mapstructure:"host"`
	Port              int           `mapstructure:"port"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`
	ShutdownTimeout   time.Duration `mapstructure:"shutdown_timeout"`
}

type DatabaseConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	Name         string `mapstructure:"name"`
	SSLMode      string `mapstructure:"sslmode"`
	TimeZone     string `mapstructure:"timezone"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

type MarketConfig struct {
	GammaBaseURL   string        `mapstructure:"gamma_base_url"`
	CLOBBaseURL    string        `mapstructure:"clob_base_url"`
	WeatherTag     string        `mapstructure:"weather_tag"`
	SyncLimit      int           `mapstructure:"sync_limit"`
	RequestTimeout time.Duration `mapstructure:"request_timeout"`
}

type WeatherConfig struct {
	NWSBaseURL                string        `mapstructure:"nws_base_url"`
	OpenMeteoBaseURL          string        `mapstructure:"openmeteo_base_url"`
	OpenMeteoGeocodingBaseURL string        `mapstructure:"openmeteo_geocoding_base_url"`
	OpenMeteoModels           string        `mapstructure:"openmeteo_models"`
	VisualCrossingBaseURL     string        `mapstructure:"visualcrossing_base_url"`
	VisualCrossingAPIKey      string        `mapstructure:"visualcrossing_api_key"`
	RequestTimeout            time.Duration `mapstructure:"request_timeout"`
	UserAgent                 string        `mapstructure:"user_agent"`
}

type SignalConfig struct {
	MinEdgePct          float64 `mapstructure:"min_edge_pct"`
	MinEdgeExecPct      float64 `mapstructure:"min_edge_exec_pct"`
	ExecFeePct          float64 `mapstructure:"exec_fee_pct"`
	ExecSlippagePct     float64 `mapstructure:"exec_slippage_pct"`
	MaxMarkets          int     `mapstructure:"max_markets"`
	DefaultLimit        int     `mapstructure:"default_limit"`
	Concurrency         int     `mapstructure:"concurrency"`
	ForecastMaxAgeHours int     `mapstructure:"forecast_max_age_hours"`
	MinSigma            float64 `mapstructure:"min_sigma"`
}

type AgentConfig struct {
	AnalyzeLimit          int           `mapstructure:"analyze_limit"`
	Concurrency           int           `mapstructure:"concurrency"`
	MarketTimeout         time.Duration `mapstructure:"market_timeout"`
	MaxForecastDays       int           `mapstructure:"max_forecast_days"`
	VertexProject         string        `mapstructure:"vertex_project"`
	VertexLocation        string        `mapstructure:"vertex_location"`
	VertexModel           string        `mapstructure:"vertex_model"`
	VertexTemperature     float32       `mapstructure:"vertex_temperature"`
	VertexMaxOutputTokens int           `mapstructure:"vertex_max_output_tokens"`
	VertexTimeout         time.Duration `mapstructure:"vertex_timeout"`
}

type TradeConfig struct {
	PrivateKeyHex        string  `mapstructure:"private_key"`
	ChainID              int64   `mapstructure:"chain_id"`
	MaxOrderSize         float64 `mapstructure:"max_order_size"`
	MaxPositionSize      float64 `mapstructure:"max_position_size"`
	MaxDailyLoss         float64 `mapstructure:"max_daily_loss"`
	MinEdgePct           float64 `mapstructure:"min_edge_pct"`
	MaxOpenPositions     int     `mapstructure:"max_open_positions"`
	MinLiquidity         float64 `mapstructure:"min_liquidity"`
	ConfirmationRequired bool    `mapstructure:"confirmation_required"`
	PaperMode            bool    `mapstructure:"paper_mode"`
}

type SimConfig struct {
	Interval     time.Duration `mapstructure:"interval"`
	OrderSize    float64       `mapstructure:"order_size"`
	MinEdgePct   float64       `mapstructure:"min_edge_pct"`
	MaxPositions int           `mapstructure:"max_positions"`
	AutoSettle   bool          `mapstructure:"auto_settle"`
}

type ChainConfig struct {
	RPCURL                 string        `mapstructure:"rpc_url"`
	ChainID                int64         `mapstructure:"chain_id"`
	CTFExchangeAddress     string        `mapstructure:"ctf_exchange_address"`
	NegRiskExchangeAddress string        `mapstructure:"negrisk_exchange_address"`
	ScanLookbackBlocks     uint64        `mapstructure:"scan_lookback_blocks"`
	ScanMaxTx              int           `mapstructure:"scan_max_tx"`
	BotMinTrades           int           `mapstructure:"bot_min_trades"`
	BotMaxAvgIntervalSec   float64       `mapstructure:"bot_max_avg_interval_sec"`
	WatchInterval          time.Duration `mapstructure:"watch_interval"`
}

type SchedulerConfig struct {
	Enabled            bool                `mapstructure:"enabled"`
	RunOnStart         bool                `mapstructure:"run_on_start"`
	LockMode           string              `mapstructure:"lock_mode"`
	LockKeyPrefix      int64               `mapstructure:"lock_key_prefix"`
	DefaultTimeout     time.Duration       `mapstructure:"default_timeout"`
	DefaultJitterRatio float64             `mapstructure:"default_jitter_ratio"`
	Jobs               SchedulerJobsConfig `mapstructure:"jobs"`
}

type SchedulerJobsConfig struct {
	MarketSync      SchedulerJobConfig        `mapstructure:"market_sync"`
	WeatherForecast SchedulerWeatherJobConfig `mapstructure:"weather_forecast"`
	ChainScan       SchedulerChainJobConfig   `mapstructure:"chain_scan"`
	SimCycle        SchedulerJobConfig        `mapstructure:"sim_cycle"`
}

type SchedulerJobConfig struct {
	Enabled   bool          `mapstructure:"enabled"`
	Interval  time.Duration `mapstructure:"interval"`
	Timeout   time.Duration `mapstructure:"timeout"`
	Immediate bool          `mapstructure:"immediate"`
}

type SchedulerWeatherJobConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	Interval        time.Duration `mapstructure:"interval"`
	Timeout         time.Duration `mapstructure:"timeout"`
	Immediate       bool          `mapstructure:"immediate"`
	CityConcurrency int           `mapstructure:"city_concurrency"`
	Days            int           `mapstructure:"days"`
	Source          string        `mapstructure:"source"`
}

type SchedulerChainJobConfig struct {
	Enabled        bool          `mapstructure:"enabled"`
	Interval       time.Duration `mapstructure:"interval"`
	Timeout        time.Duration `mapstructure:"timeout"`
	Immediate      bool          `mapstructure:"immediate"`
	LookbackBlocks uint64        `mapstructure:"lookback_blocks"`
	MaxTx          int           `mapstructure:"max_tx"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

type LogConfig struct {
	Level    string `mapstructure:"level"`
	Encoding string `mapstructure:"encoding"`
}

func Load(configFile string) (*Config, error) {
	v := viper.New()
	setDefaults(v)

	v.SetEnvPrefix("SKY_ALPHA")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./configs")
		v.AddConfigPath("$HOME")
	}

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if configFile != "" || !errors.As(err, &notFound) {
			return nil, fmt.Errorf("load config: %w", err)
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "sky-alpha-pro")
	v.SetDefault("app.env", "dev")
	v.SetDefault("app.version", "0.1.0")

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "10s")
	v.SetDefault("server.read_header_timeout", "5s")
	v.SetDefault("server.write_timeout", "15s")
	v.SetDefault("server.shutdown_timeout", "10s")

	v.SetDefault("market.gamma_base_url", "https://gamma-api.polymarket.com")
	v.SetDefault("market.clob_base_url", "https://clob.polymarket.com")
	v.SetDefault("market.weather_tag", "weather")
	v.SetDefault("market.sync_limit", 100)
	v.SetDefault("market.request_timeout", "12s")

	v.SetDefault("weather.nws_base_url", "https://api.weather.gov")
	v.SetDefault("weather.openmeteo_base_url", "https://api.open-meteo.com")
	v.SetDefault("weather.openmeteo_geocoding_base_url", "https://geocoding-api.open-meteo.com")
	v.SetDefault("weather.openmeteo_models", "")
	v.SetDefault("weather.visualcrossing_base_url", "https://weather.visualcrossing.com")
	v.SetDefault("weather.visualcrossing_api_key", "")
	v.SetDefault("weather.request_timeout", "12s")
	v.SetDefault("weather.user_agent", "sky-alpha-pro/0.1.0")

	v.SetDefault("signal.min_edge_pct", 5.0)
	v.SetDefault("signal.min_edge_exec_pct", 2.0)
	v.SetDefault("signal.exec_fee_pct", 0.2)
	v.SetDefault("signal.exec_slippage_pct", 0.3)
	v.SetDefault("signal.max_markets", 500)
	v.SetDefault("signal.default_limit", 50)
	v.SetDefault("signal.concurrency", 10)
	v.SetDefault("signal.forecast_max_age_hours", 24)
	v.SetDefault("signal.min_sigma", 0.5)

	v.SetDefault("agent.analyze_limit", 20)
	v.SetDefault("agent.concurrency", 8)
	v.SetDefault("agent.market_timeout", "20s")
	v.SetDefault("agent.max_forecast_days", 10)
	v.SetDefault("agent.vertex_project", "")
	v.SetDefault("agent.vertex_location", "us-central1")
	v.SetDefault("agent.vertex_model", "gemini-2.5-flash")
	v.SetDefault("agent.vertex_temperature", 0.2)
	v.SetDefault("agent.vertex_max_output_tokens", 600)
	v.SetDefault("agent.vertex_timeout", "15s")

	v.SetDefault("trade.private_key", "")
	v.SetDefault("trade.chain_id", 137)
	v.SetDefault("trade.max_order_size", 0.0)
	v.SetDefault("trade.max_position_size", 100.0)
	v.SetDefault("trade.max_daily_loss", 50.0)
	v.SetDefault("trade.min_edge_pct", 5.0)
	v.SetDefault("trade.max_open_positions", 10)
	v.SetDefault("trade.min_liquidity", 1000.0)
	v.SetDefault("trade.confirmation_required", true)
	v.SetDefault("trade.paper_mode", false)

	v.SetDefault("sim.interval", "5m")
	v.SetDefault("sim.order_size", 1.0)
	v.SetDefault("sim.min_edge_pct", 5.0)
	v.SetDefault("sim.max_positions", 20)
	v.SetDefault("sim.auto_settle", true)

	v.SetDefault("chain.rpc_url", "")
	v.SetDefault("chain.chain_id", 137)
	v.SetDefault("chain.ctf_exchange_address", "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
	v.SetDefault("chain.negrisk_exchange_address", "0xC5d563A36AE78145C45a50134d48A1215220f80a")
	v.SetDefault("chain.scan_lookback_blocks", 2000)
	v.SetDefault("chain.scan_max_tx", 2000)
	v.SetDefault("chain.bot_min_trades", 8)
	v.SetDefault("chain.bot_max_avg_interval_sec", 8.0)
	v.SetDefault("chain.watch_interval", "30s")

	v.SetDefault("scheduler.enabled", true)
	v.SetDefault("scheduler.run_on_start", true)
	v.SetDefault("scheduler.lock_mode", "local")
	v.SetDefault("scheduler.lock_key_prefix", 7300)
	v.SetDefault("scheduler.default_timeout", "60s")
	v.SetDefault("scheduler.default_jitter_ratio", 0.1)
	v.SetDefault("scheduler.jobs.market_sync.enabled", true)
	v.SetDefault("scheduler.jobs.market_sync.interval", "5m")
	v.SetDefault("scheduler.jobs.market_sync.timeout", "90s")
	v.SetDefault("scheduler.jobs.market_sync.immediate", true)
	v.SetDefault("scheduler.jobs.weather_forecast.enabled", true)
	v.SetDefault("scheduler.jobs.weather_forecast.interval", "15m")
	v.SetDefault("scheduler.jobs.weather_forecast.timeout", "120s")
	v.SetDefault("scheduler.jobs.weather_forecast.immediate", true)
	v.SetDefault("scheduler.jobs.weather_forecast.city_concurrency", 6)
	v.SetDefault("scheduler.jobs.weather_forecast.days", 7)
	v.SetDefault("scheduler.jobs.weather_forecast.source", "all")
	v.SetDefault("scheduler.jobs.chain_scan.enabled", true)
	v.SetDefault("scheduler.jobs.chain_scan.interval", "5m")
	v.SetDefault("scheduler.jobs.chain_scan.timeout", "90s")
	v.SetDefault("scheduler.jobs.chain_scan.immediate", false)
	v.SetDefault("scheduler.jobs.chain_scan.lookback_blocks", uint64(0))
	v.SetDefault("scheduler.jobs.chain_scan.max_tx", 0)

	v.SetDefault("scheduler.jobs.sim_cycle.enabled", false)
	v.SetDefault("scheduler.jobs.sim_cycle.interval", "5m")
	v.SetDefault("scheduler.jobs.sim_cycle.timeout", "120s")
	v.SetDefault("scheduler.jobs.sim_cycle.immediate", false)

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("database.host", "127.0.0.1")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.password", "postgres")
	v.SetDefault("database.name", "sky_alpha_pro")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("database.timezone", "UTC")
	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 5)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.encoding", "console")
}
