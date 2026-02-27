package weather

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNWSGetLatestObservation(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"properties":{
				"timestamp":"2026-03-01T10:00:00Z",
				"textDescription":"Clear",
				"temperature":{"value":20},
				"relativeHumidity":{"value":60},
				"windSpeed":{"value":36},
				"windDirection":{"value":90},
				"barometricPressure":{"value":101325},
				"precipitationLastHour":{"value":12.7},
				"visibility":{"value":8046.72}
			}
		}`))
	}))
	defer srv.Close()

	client := NewNWSClient(srv.URL, "test-agent", &http.Client{Timeout: 2 * time.Second})
	obs, err := client.GetLatestObservation(context.Background(), "KNYC")
	if err != nil {
		t.Fatalf("GetLatestObservation error: %v", err)
	}
	if obs.TempF < 67 || obs.TempF > 69 {
		t.Fatalf("unexpected temp conversion: %v", obs.TempF)
	}
	if obs.WindSpeedMPH < 22 || obs.WindSpeedMPH > 23 {
		t.Fatalf("unexpected wind conversion: %v", obs.WindSpeedMPH)
	}
	if obs.PressureMB < 1013 || obs.PressureMB > 1014 {
		t.Fatalf("unexpected pressure conversion: %v", obs.PressureMB)
	}
	if obs.Precip1hIn < 0.49 || obs.Precip1hIn > 0.51 {
		t.Fatalf("unexpected precip conversion: %v", obs.Precip1hIn)
	}
	if obs.VisibilityMi < 4.9 || obs.VisibilityMi > 5.1 {
		t.Fatalf("unexpected visibility conversion: %v", obs.VisibilityMi)
	}
}

func TestNWSGetDailyForecast(t *testing.T) {
	now := time.Now().UTC()
	day0 := now.Format("2006-01-02")
	day1 := now.Add(24 * time.Hour).Format("2006-01-02")

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/points/40.0000,-73.0000":
			_, _ = w.Write([]byte(fmt.Sprintf(`{
				"properties":{"forecastHourly":"%s/hourly","timeZone":"UTC"}
			}`, srv.URL)))
		case "/hourly":
			_, _ = w.Write([]byte(fmt.Sprintf(`{
				"properties":{
					"periods":[
						{"startTime":"%sT01:00:00Z","temperature":10,"temperatureUnit":"C","windSpeed":"10 mph"},
						{"startTime":"%sT15:00:00Z","temperature":20,"temperatureUnit":"C","windSpeed":"20 mph"},
						{"startTime":"%sT12:00:00Z","temperature":15,"temperatureUnit":"C","windSpeed":"15 mph"}
					]
				}
			}`, day0, day0, day1)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := NewNWSClient(srv.URL, "test-agent", &http.Client{Timeout: 2 * time.Second})
	items, err := client.GetDailyForecast(context.Background(), 40.0, -73.0, 2)
	if err != nil {
		t.Fatalf("GetDailyForecast error: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("expected at least 2 day entries, got %d", len(items))
	}
}

func TestMustStandardLocation(t *testing.T) {
	loc := mustStandardLocation("America/New_York")
	summer := time.Date(2026, 7, 1, 12, 0, 0, 0, loc)
	_, off := summer.Zone()
	if off != -5*3600 {
		t.Fatalf("expected -5h offset for standard time, got %d", off)
	}
}
