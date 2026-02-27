package weather

import "time"

type ForecastRequest struct {
	Location string
	Source   string
	Days     int
}

type ForecastEntry struct {
	Source       string    `json:"source"`
	Location     string    `json:"location"`
	ForecastDate time.Time `json:"forecast_date"`
	TempHighF    float64   `json:"temp_high_f,omitempty"`
	TempLowF     float64   `json:"temp_low_f,omitempty"`
	PrecipIn     float64   `json:"precip_in,omitempty"`
	WindSpeedMPH float64   `json:"wind_speed_mph,omitempty"`
	WindGustMPH  float64   `json:"wind_gust_mph,omitempty"`
	HumidityPct  int       `json:"humidity_pct,omitempty"`
	CloudCover   int       `json:"cloud_cover_pct,omitempty"`
}

type ForecastResponse struct {
	Location  string          `json:"location"`
	Latitude  float64         `json:"latitude,omitempty"`
	Longitude float64         `json:"longitude,omitempty"`
	Forecasts []ForecastEntry `json:"forecasts"`
	Errors    []string        `json:"errors,omitempty"`
}

type Observation struct {
	Source       string    `json:"source"`
	StationID    string    `json:"station_id"`
	ObservedAt   time.Time `json:"observed_at"`
	TempF        float64   `json:"temp_f,omitempty"`
	HumidityPct  int       `json:"humidity_pct,omitempty"`
	WindSpeedMPH float64   `json:"wind_speed_mph,omitempty"`
	WindDir      string    `json:"wind_dir,omitempty"`
	Description  string    `json:"description,omitempty"`
}
