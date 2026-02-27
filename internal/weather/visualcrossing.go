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

	"sky-alpha-pro/pkg/httpretry"
)

type VisualCrossingClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewVisualCrossingClient(baseURL string, apiKey string, client *http.Client) *VisualCrossingClient {
	return &VisualCrossingClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  strings.TrimSpace(apiKey),
		client:  client,
	}
}

func (c *VisualCrossingClient) Enabled() bool {
	return c.apiKey != ""
}

func (c *VisualCrossingClient) GetDailyForecast(ctx context.Context, location string, days int) ([]ForecastEntry, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("visual crossing api key is empty")
	}
	if days < 1 {
		days = 1
	}

	pathLocation := url.PathEscape(strings.TrimSpace(location))
	u, err := url.Parse(fmt.Sprintf("%s/VisualCrossingWebServices/rest/services/timeline/%s", c.baseURL, pathLocation))
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("unitGroup", "us")
	q.Set("include", "days")
	q.Set("elements", "datetime,tempmax,tempmin,precip,windspeed,windgust,humidity,cloudcover")
	q.Set("key", c.apiKey)
	q.Set("contentType", "json")
	q.Set("forecastDays", strconv.Itoa(days))
	u.RawQuery = q.Encode()

	resp, err := httpretry.DoRequestWithRetry(ctx, c.client, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	}, 3)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("visual crossing status: %d", resp.StatusCode)
	}

	var payload struct {
		Days []struct {
			Date      string  `json:"datetime"`
			TempMax   float64 `json:"tempmax"`
			TempMin   float64 `json:"tempmin"`
			Precip    float64 `json:"precip"`
			WindSpeed float64 `json:"windspeed"`
			WindGust  float64 `json:"windgust"`
			Humidity  float64 `json:"humidity"`
			Cloud     float64 `json:"cloudcover"`
		} `json:"days"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	items := make([]ForecastEntry, 0, len(payload.Days))
	for i, day := range payload.Days {
		if i >= days {
			break
		}
		t, err := time.Parse("2006-01-02", day.Date)
		if err != nil {
			continue
		}
		items = append(items, ForecastEntry{
			Source:       "visualcrossing",
			ForecastDate: t.UTC(),
			TempHighF:    day.TempMax,
			TempLowF:     day.TempMin,
			PrecipIn:     day.Precip,
			WindSpeedMPH: day.WindSpeed,
			WindGustMPH:  day.WindGust,
			HumidityPct:  int(day.Humidity),
			CloudCover:   int(day.Cloud),
		})
	}
	return items, nil
}
