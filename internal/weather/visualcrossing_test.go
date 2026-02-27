package weather

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestVisualCrossingGetDailyForecast(t *testing.T) {
	var gotEscapedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEscapedPath = r.URL.EscapedPath()
		_, _ = w.Write([]byte(`{
			"days":[
				{"datetime":"2026-03-01","tempmax":66,"tempmin":50,"precip":0.2,"windspeed":10,"windgust":16,"humidity":70,"cloudcover":40}
			]
		}`))
	}))
	defer srv.Close()

	client := NewVisualCrossingClient(srv.URL, "test-key", &http.Client{Timeout: 2 * time.Second})
	items, err := client.GetDailyForecast(context.Background(), "New York/Manhattan #1", 1)
	if err != nil {
		t.Fatalf("GetDailyForecast error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("unexpected items: %d", len(items))
	}
	if gotEscapedPath == "" {
		t.Fatal("missing request path")
	}
}
