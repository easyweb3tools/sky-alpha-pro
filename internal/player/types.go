package player

import "time"

type ListOptions struct {
	Limit            int
	MinWeatherMarket int
}

type LeaderboardOptions struct {
	Limit int
	Type  string
}

type PositionOptions struct {
	Limit int
}

type RefreshOptions struct {
	Limit int
}

type RefreshResult struct {
	PlayersUpserted   int       `json:"players_upserted"`
	PositionsUpserted int       `json:"positions_upserted"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
}

type PlayerView struct {
	Address        string     `json:"address"`
	Username       string     `json:"username,omitempty"`
	TotalPnL       *float64   `json:"total_pnl,omitempty"`
	TotalVolume    *float64   `json:"total_volume,omitempty"`
	WinRate        float64    `json:"win_rate"`
	WinRateReady   bool       `json:"win_rate_ready"`
	TotalMarkets   int        `json:"total_markets"`
	WeatherMarkets int        `json:"weather_markets"`
	RankOverall    int        `json:"rank_overall"`
	RankWeather    int        `json:"rank_weather"`
	LastActiveAt   *time.Time `json:"last_active_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

type PlayerPositionView struct {
	MarketID     string     `json:"market_id"`
	Outcome      string     `json:"outcome"`
	Size         float64    `json:"size"`
	AvgPrice     *float64   `json:"avg_price,omitempty"`
	CurrentValue *float64   `json:"current_value,omitempty"`
	PnL          *float64   `json:"pnl,omitempty"`
	FirstEntryAt *time.Time `json:"first_entry_at,omitempty"`
	LastUpdateAt time.Time  `json:"last_update_at"`
}

type CompareView struct {
	PlayerAddress         string   `json:"player_address"`
	PlayerWinRate         float64  `json:"player_win_rate"`
	PlayerWinRateReady    bool     `json:"player_win_rate_ready"`
	PlayerTotalPnL        *float64 `json:"player_total_pnl,omitempty"`
	PlayerTotalVolume     float64  `json:"player_total_volume"`
	PlayerMarkets         int      `json:"player_markets"`
	MyWinRate             float64  `json:"my_win_rate"`
	MyRealizedPnL         float64  `json:"my_realized_pnl"`
	MyFilledTrades        int      `json:"my_filled_trades"`
	WinRateDiff           *float64 `json:"win_rate_diff,omitempty"`
	RealizedPnLDiff       *float64 `json:"realized_pnl_diff,omitempty"`
	RealizedPnLDiffStatus string   `json:"realized_pnl_diff_status"`
}
