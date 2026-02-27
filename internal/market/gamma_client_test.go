package market

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGammaClientListMarkets(t *testing.T) {
	var gotPath string
	var gotQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{
				"id":"mkt-1",
				"conditionId":"cond-1",
				"slug":"nyc-temp",
				"question":"Will NYC high temperature exceed 45F?",
				"description":"test market",
				"active":true,
				"outcomes":"[\"Yes\",\"No\"]",
				"clobTokenIds":"[\"yes-token\",\"no-token\"]",
				"outcomePrices":"[\"0.63\",\"0.37\"]",
				"endDate":"2026-03-01T00:00:00Z",
				"volume":"1234.56",
				"liquidity":"789.01"
			}
		]`))
	}))
	defer srv.Close()

	client := NewGammaClient(srv.URL, &http.Client{Timeout: 2 * time.Second})
	items, err := client.ListMarkets(context.Background(), "weather", true, 50)
	if err != nil {
		t.Fatalf("ListMarkets error: %v", err)
	}
	if gotPath != "/markets" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotQuery == "" {
		t.Fatal("expected query string")
	}
	if len(items) != 1 {
		t.Fatalf("unexpected market count: %d", len(items))
	}

	m := items[0]
	if m.PolymarketID != "mkt-1" {
		t.Fatalf("unexpected market id: %s", m.PolymarketID)
	}
	if m.TokenIDYes != "yes-token" || m.TokenIDNo != "no-token" {
		t.Fatalf("unexpected token ids: yes=%s no=%s", m.TokenIDYes, m.TokenIDNo)
	}
	if m.PriceYes != 0.63 || m.PriceNo != 0.37 {
		t.Fatalf("unexpected prices: yes=%v no=%v", m.PriceYes, m.PriceNo)
	}
}
