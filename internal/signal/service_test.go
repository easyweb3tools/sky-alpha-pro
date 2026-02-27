package signal

import "testing"

func TestParseThresholdFahrenheit(t *testing.T) {
	tests := []struct {
		q    string
		want float64
		ok   bool
	}{
		{"Will NYC high exceed 45F?", 45, true},
		{"Will temp be above 10C tomorrow?", 50, true},
		{"No threshold here", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseThresholdFahrenheit(tt.q)
		if ok != tt.ok {
			t.Fatalf("unexpected ok for %q: got=%v want=%v", tt.q, ok, tt.ok)
		}
		if ok && (got < tt.want-0.1 || got > tt.want+0.1) {
			t.Fatalf("unexpected value for %q: got=%v want=%v", tt.q, got, tt.want)
		}
	}
}

func TestEstimateProbability(t *testing.T) {
	highValues := []float64{46, 47, 48}
	pHigh := estimateProbability(highValues, 45, "temperature_high")
	if pHigh <= 0.5 {
		t.Fatalf("expected high probability > 0.5, got=%v", pHigh)
	}

	lowValues := []float64{28, 30, 31}
	pLow := estimateProbability(lowValues, 32, "temperature_low")
	if pLow <= 0.5 {
		t.Fatalf("expected low probability > 0.5, got=%v", pLow)
	}
}
