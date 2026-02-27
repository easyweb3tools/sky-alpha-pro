package weather

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOpenMeteoResolveCoordinates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results":[{"name":"New York","country":"United States","latitude":40.71,"longitude":-74.0}]}`))
	}))
	defer srv.Close()

	client := NewOpenMeteoClient(srv.URL, srv.URL, &http.Client{Timeout: 2 * time.Second})
	lat, lon, name, err := client.ResolveCoordinates(context.Background(), "New York")
	if err != nil {
		t.Fatalf("ResolveCoordinates error: %v", err)
	}
	if lat == 0 || lon == 0 {
		t.Fatalf("invalid coordinates: %v %v", lat, lon)
	}
	if name == "" {
		t.Fatal("empty normalized name")
	}
}

func TestNormalizeSources(t *testing.T) {
	got := normalizeSources("all")
	if len(got) != 3 {
		t.Fatalf("unexpected sources length: %d", len(got))
	}
	got = normalizeSources("nws")
	if len(got) != 1 || got[0] != "nws" {
		t.Fatalf("unexpected nws sources: %#v", got)
	}
}
