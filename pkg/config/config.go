package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	App      AppConfig      `mapstructure:"app"`
	Server   ServerConfig   `mapstructure:"server"`
	Market   MarketConfig   `mapstructure:"market"`
	Weather  WeatherConfig  `mapstructure:"weather"`
	Signal   SignalConfig   `mapstructure:"signal"`
	Agent    AgentConfig    `mapstructure:"agent"`
	Database DatabaseConfig `mapstructure:"database"`
	Log      LogConfig      `mapstructure:"log"`
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
	MaxMarkets          int     `mapstructure:"max_markets"`
	DefaultLimit        int     `mapstructure:"default_limit"`
	Concurrency         int     `mapstructure:"concurrency"`
	ForecastMaxAgeHours int     `mapstructure:"forecast_max_age_hours"`
	MinSigma            float64 `mapstructure:"min_sigma"`
}

type AgentConfig struct {
	Model        string `mapstructure:"model"`
	AnalyzeLimit int    `mapstructure:"analyze_limit"`
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
	v.SetDefault("signal.max_markets", 500)
	v.SetDefault("signal.default_limit", 50)
	v.SetDefault("signal.concurrency", 10)
	v.SetDefault("signal.forecast_max_age_hours", 24)
	v.SetDefault("signal.min_sigma", 0.5)

	v.SetDefault("agent.model", "rule-based-agent-v1")
	v.SetDefault("agent.analyze_limit", 20)

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
