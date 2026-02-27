package weather

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type OpenMeteoClient struct {
	baseURL      string
	geocodingURL string
	client       *http.Client
}

func NewOpenMeteoClient(baseURL string, geocodingURL string, client *http.Client) *OpenMeteoClient {
	return &OpenMeteoClient{
		baseURL:      strings.TrimRight(baseURL, "/"),
		geocodingURL: strings.TrimRight(geocodingURL, "/"),
		client:       client,
	}
}

func (c *OpenMeteoClient) ResolveCoordinates(ctx context.Context, location string) (float64, float64, string, error) {
	if lat, lon, ok := parseLatLon(location); ok {
		return lat, lon, location, nil
	}

	u, err := url.Parse(c.geocodingURL + "/v1/search")
	if err != nil {
		return 0, 0, "", err
	}
	q := u.Query()
	q.Set("name", location)
	q.Set("count", "1")
	q.Set("language", "en")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, 0, "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, 0, "", fmt.Errorf("open-meteo geocoding status: %d", resp.StatusCode)
	}

	var payload struct {
		Results []struct {
			Name      string  `json:"name"`
			Country   string  `json:"country"`
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, 0, "", err
	}
	if len(payload.Results) == 0 {
		return 0, 0, "", fmt.Errorf("location not found: %s", location)
	}

	name := payload.Results[0].Name
	if payload.Results[0].Country != "" {
		name += ", " + payload.Results[0].Country
	}
	return payload.Results[0].Latitude, payload.Results[0].Longitude, name, nil
}

func (c *OpenMeteoClient) GetDailyForecast(ctx context.Context, lat, lon float64, days int) ([]ForecastEntry, error) {
	if days < 1 {
		days = 1
	}
	if days > 16 {
		days = 16
	}

	u, err := url.Parse(c.baseURL + "/v1/forecast")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("latitude", strconv.FormatFloat(lat, 'f', 4, 64))
	q.Set("longitude", strconv.FormatFloat(lon, 'f', 4, 64))
	q.Set("daily", "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max")
	q.Set("temperature_unit", "fahrenheit")
	q.Set("wind_speed_unit", "mph")
	q.Set("precipitation_unit", "inch")
	q.Set("timezone", "UTC")
	q.Set("forecast_days", strconv.Itoa(days))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("open-meteo forecast status: %d", resp.StatusCode)
	}

	var payload struct {
		Daily struct {
			Time         []string  `json:"time"`
			TempMax      []float64 `json:"temperature_2m_max"`
			TempMin      []float64 `json:"temperature_2m_min"`
			PrecipSum    []float64 `json:"precipitation_sum"`
			WindSpeedMax []float64 `json:"wind_speed_10m_max"`
		} `json:"daily"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	items := make([]ForecastEntry, 0, len(payload.Daily.Time))
	for i, day := range payload.Daily.Time {
		t, err := time.Parse("2006-01-02", day)
		if err != nil {
			continue
		}
		item := ForecastEntry{
			Source:       "openmeteo",
			ForecastDate: t.UTC(),
		}
		if i < len(payload.Daily.TempMax) {
			item.TempHighF = payload.Daily.TempMax[i]
		}
		if i < len(payload.Daily.TempMin) {
			item.TempLowF = payload.Daily.TempMin[i]
		}
		if i < len(payload.Daily.PrecipSum) {
			item.PrecipIn = payload.Daily.PrecipSum[i]
		}
		if i < len(payload.Daily.WindSpeedMax) {
			item.WindSpeedMPH = payload.Daily.WindSpeedMax[i]
		}
		items = append(items, item)
	}
	return items, nil
}

func parseLatLon(location string) (float64, float64, bool) {
	parts := strings.Split(strings.TrimSpace(location), ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return lat, lon, true
}
