package weather

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type NWSClient struct {
	baseURL   string
	userAgent string
	client    *http.Client
}

func NewNWSClient(baseURL string, userAgent string, client *http.Client) *NWSClient {
	return &NWSClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		userAgent: userAgent,
		client:    client,
	}
}

func (c *NWSClient) GetDailyForecast(ctx context.Context, lat, lon float64, days int) ([]ForecastEntry, error) {
	pointURL := fmt.Sprintf("%s/points/%.4f,%.4f", c.baseURL, lat, lon)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pointURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/geo+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("nws points status: %d", resp.StatusCode)
	}

	var points struct {
		Properties struct {
			ForecastHourly string `json:"forecastHourly"`
		} `json:"properties"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&points); err != nil {
		return nil, err
	}
	if points.Properties.ForecastHourly == "" {
		return nil, fmt.Errorf("nws forecast url missing")
	}

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, points.Properties.ForecastHourly, nil)
	if err != nil {
		return nil, err
	}
	req2.Header.Set("User-Agent", c.userAgent)
	req2.Header.Set("Accept", "application/geo+json")

	resp2, err := c.client.Do(req2)
	if err != nil {
		return nil, err
	}
	defer resp2.Body.Close()
	if resp2.StatusCode < 200 || resp2.StatusCode >= 300 {
		return nil, fmt.Errorf("nws hourly status: %d", resp2.StatusCode)
	}

	var hourly struct {
		Properties struct {
			Periods []struct {
				StartTime string  `json:"startTime"`
				Temp      float64 `json:"temperature"`
				TempUnit  string  `json:"temperatureUnit"`
				WindSpeed string  `json:"windSpeed"`
			} `json:"periods"`
		} `json:"properties"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&hourly); err != nil {
		return nil, err
	}
	if len(hourly.Properties.Periods) == 0 {
		return nil, nil
	}

	type agg struct {
		high float64
		low  float64
		wind float64
		cnt  int
	}
	byDay := map[string]*agg{}
	for _, p := range hourly.Properties.Periods {
		t, err := time.Parse(time.RFC3339, p.StartTime)
		if err != nil {
			continue
		}
		key := t.UTC().Format("2006-01-02")
		tempF := p.Temp
		if strings.EqualFold(p.TempUnit, "C") {
			tempF = celsiusToFahrenheit(p.Temp)
		}
		if _, ok := byDay[key]; !ok {
			byDay[key] = &agg{high: tempF, low: tempF}
		}
		dayAgg := byDay[key]
		if tempF > dayAgg.high {
			dayAgg.high = tempF
		}
		if tempF < dayAgg.low {
			dayAgg.low = tempF
		}
		if ws := parseWindSpeedMPH(p.WindSpeed); ws > 0 {
			dayAgg.wind += ws
			dayAgg.cnt++
		}
	}

	out := make([]ForecastEntry, 0, len(byDay))
	for i := 0; i < days; i++ {
		day := time.Now().UTC().Add(time.Duration(i) * 24 * time.Hour).Format("2006-01-02")
		dayAgg, ok := byDay[day]
		if !ok {
			continue
		}
		t, _ := time.Parse("2006-01-02", day)
		item := ForecastEntry{
			Source:       "nws",
			ForecastDate: t.UTC(),
			TempHighF:    dayAgg.high,
			TempLowF:     dayAgg.low,
		}
		if dayAgg.cnt > 0 {
			item.WindSpeedMPH = dayAgg.wind / float64(dayAgg.cnt)
		}
		out = append(out, item)
	}
	return out, nil
}

func (c *NWSClient) GetLatestObservation(ctx context.Context, stationID string) (*Observation, error) {
	u := fmt.Sprintf("%s/stations/%s/observations/latest", c.baseURL, stationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/geo+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("nws observation status: %d", resp.StatusCode)
	}

	var payload struct {
		Properties struct {
			Timestamp       string `json:"timestamp"`
			TextDescription string `json:"textDescription"`
			Temperature     struct {
				Value *float64 `json:"value"`
			} `json:"temperature"`
			RelativeHumidity struct {
				Value *float64 `json:"value"`
			} `json:"relativeHumidity"`
			WindSpeed struct {
				Value *float64 `json:"value"`
			} `json:"windSpeed"`
			WindDirection struct {
				Value *float64 `json:"value"`
			} `json:"windDirection"`
		} `json:"properties"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	observedAt, _ := time.Parse(time.RFC3339, payload.Properties.Timestamp)
	item := &Observation{
		Source:      "nws",
		StationID:   stationID,
		ObservedAt:  observedAt.UTC(),
		Description: payload.Properties.TextDescription,
	}
	if payload.Properties.Temperature.Value != nil {
		item.TempF = celsiusToFahrenheit(*payload.Properties.Temperature.Value)
	}
	if payload.Properties.RelativeHumidity.Value != nil {
		item.HumidityPct = int(*payload.Properties.RelativeHumidity.Value)
	}
	if payload.Properties.WindSpeed.Value != nil {
		item.WindSpeedMPH = metersPerSecondToMPH(*payload.Properties.WindSpeed.Value)
	}
	if payload.Properties.WindDirection.Value != nil {
		item.WindDir = degreeToDirection(*payload.Properties.WindDirection.Value)
	}
	return item, nil
}

func celsiusToFahrenheit(v float64) float64 {
	return (v * 9.0 / 5.0) + 32.0
}

func metersPerSecondToMPH(v float64) float64 {
	return v * 2.236936
}

func degreeToDirection(deg float64) string {
	directions := []string{"N", "NE", "E", "SE", "S", "SW", "W", "NW"}
	idx := int((deg+22.5)/45.0) % 8
	return directions[idx]
}

func parseWindSpeedMPH(v string) float64 {
	var n float64
	_, err := fmt.Sscanf(strings.TrimSpace(v), "%f mph", &n)
	if err == nil {
		return n
	}
	_, err = fmt.Sscanf(strings.TrimSpace(v), "%f", &n)
	if err == nil {
		return n
	}
	return 0
}
