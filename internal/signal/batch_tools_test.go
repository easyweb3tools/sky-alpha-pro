package signal

import "testing"

func TestCityResolutionConfidence(t *testing.T) {
	tests := []struct {
		source string
		city   string
		want   float64
	}{
		{source: "market_field", city: "new york", want: 1.0},
		{source: "rule", city: "new york", want: 0.85},
		{source: "vertex_ai", city: "new york", want: 0.75},
		{source: "vertex_failed", city: "new york", want: 0},
		{source: "unknown", city: "new york", want: 0},
		{source: "rule", city: "", want: 0},
	}
	for _, tt := range tests {
		got := cityResolutionConfidence(tt.source, tt.city)
		if got != tt.want {
			t.Fatalf("source=%s city=%s got=%v want=%v", tt.source, tt.city, got, tt.want)
		}
	}
}
