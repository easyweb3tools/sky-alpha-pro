package market

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCLOBClientNumbers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/midpoint/yes-token":
			_, _ = w.Write([]byte(`{"mid":"0.55"}`))
		case "/spread/yes-token":
			_, _ = w.Write([]byte(`{"spread":"0.02"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := NewCLOBClient(srv.URL, &http.Client{Timeout: 2 * time.Second})

	mid, err := client.GetMidpoint(context.Background(), "yes-token")
	if err != nil {
		t.Fatalf("GetMidpoint error: %v", err)
	}
	if mid != 0.55 {
		t.Fatalf("unexpected midpoint: %v", mid)
	}

	spread, err := client.GetSpread(context.Background(), "yes-token")
	if err != nil {
		t.Fatalf("GetSpread error: %v", err)
	}
	if spread != 0.02 {
		t.Fatalf("unexpected spread: %v", spread)
	}
}
