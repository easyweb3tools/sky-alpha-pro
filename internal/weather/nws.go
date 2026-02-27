package weather

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"sky-alpha-pro/pkg/httpretry"
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
	resp, err := c.requestGeoJSON(ctx, pointURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var points struct {
		Properties struct {
			ForecastHourly string `json:"forecastHourly"`
			TimeZone       string `json:"timeZone"`
		} `json:"properties"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&points); err != nil {
		return nil, err
	}
	if points.Properties.ForecastHourly == "" {
		return nil, fmt.Errorf("nws forecast url missing")
	}
	if !isSafeNWSURL(c.baseURL, points.Properties.ForecastHourly) {
		return nil, fmt.Errorf("nws forecast url is unsafe")
	}

	resp2, err := c.requestGeoJSON(ctx, points.Properties.ForecastHourly)
	if err != nil {
		return nil, err
	}
	defer resp2.Body.Close()

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

	localStandard := mustStandardLocation(points.Properties.TimeZone)
	nowLocal := time.Now().In(localStandard)
	todayKey := nowLocal.Format("2006-01-02")

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
		local := t.In(localStandard)
		key := local.Format("2006-01-02")
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
		dayTime, _ := time.ParseInLocation("2006-01-02", todayKey, localStandard)
		day := dayTime.AddDate(0, 0, i).Format("2006-01-02")
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
	resp, err := c.requestGeoJSON(ctx, u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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
			BarometricPressure struct {
				Value *float64 `json:"value"`
			} `json:"barometricPressure"`
			PrecipLastHour struct {
				Value *float64 `json:"value"`
			} `json:"precipitationLastHour"`
			Visibility struct {
				Value *float64 `json:"value"`
			} `json:"visibility"`
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
		item.WindSpeedMPH = kilometersPerHourToMPH(*payload.Properties.WindSpeed.Value)
	}
	if payload.Properties.WindDirection.Value != nil {
		item.WindDir = degreeToDirection(*payload.Properties.WindDirection.Value)
	}
	if payload.Properties.BarometricPressure.Value != nil {
		item.PressureMB = pascalToMillibar(*payload.Properties.BarometricPressure.Value)
	}
	if payload.Properties.PrecipLastHour.Value != nil {
		item.Precip1hIn = millimeterToInch(*payload.Properties.PrecipLastHour.Value)
	}
	if payload.Properties.Visibility.Value != nil {
		item.VisibilityMi = metersToMile(*payload.Properties.Visibility.Value)
	}
	return item, nil
}

func (c *NWSClient) requestGeoJSON(ctx context.Context, endpoint string) (*http.Response, error) {
	resp, err := httpretry.DoRequestWithRetry(ctx, c.client, func() (*http.Request, error) {
		req, buildErr := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if buildErr != nil {
			return nil, buildErr
		}
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Accept", "application/geo+json")
		return req, nil
	}, 3)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("nws status: %d", resp.StatusCode)
	}
	return resp, nil
}

func mustStandardLocation(zone string) *time.Location {
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return time.UTC
	}
	year := time.Now().Year()
	for m := 1; m <= 12; m++ {
		t := time.Date(year, time.Month(m), 1, 12, 0, 0, 0, loc)
		if !t.IsDST() {
			_, offset := t.Zone()
			return time.FixedZone(zone+"_standard", offset)
		}
	}
	_, offset := time.Now().In(loc).Zone()
	return time.FixedZone(zone+"_standard", offset)
}

func isSafeNWSURL(baseURL string, raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if !strings.EqualFold(u.Scheme, "https") && !strings.EqualFold(u.Scheme, "http") {
		return false
	}
	if strings.EqualFold(u.Host, "api.weather.gov") {
		return true
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, base.Host)
}

func celsiusToFahrenheit(v float64) float64 {
	return (v * 9.0 / 5.0) + 32.0
}

func kilometersPerHourToMPH(v float64) float64 {
	return v * 0.621371
}

func pascalToMillibar(v float64) float64 {
	return v / 100.0
}

func millimeterToInch(v float64) float64 {
	return v / 25.4
}

func metersToMile(v float64) float64 {
	return v / 1609.344
}

func degreeToDirection(deg float64) string {
	directions := []string{"N", "NE", "E", "SE", "S", "SW", "W", "NW"}
	idx := int((deg+22.5)/45.0) % 8
	return directions[idx]
}

func parseWindSpeedMPH(v string) float64 {
	var a float64
	var b float64
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%f to %f mph", &a, &b); err == nil {
		return (a + b) / 2.0
	}
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%f mph", &a); err == nil {
		return a
	}
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%f", &a); err == nil {
		return a
	}
	return 0
}
