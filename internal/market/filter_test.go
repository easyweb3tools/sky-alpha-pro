package market

import "testing"

func TestLooksLikeWeatherMarket(t *testing.T) {
	t.Run("positive", func(t *testing.T) {
		m := GammaMarket{
			Question: "Will NYC high temperature exceed 45°F tomorrow?",
		}
		if !looksLikeWeatherMarket(m) {
			t.Fatal("expected weather market match")
		}
	})

	t.Run("negative", func(t *testing.T) {
		m := GammaMarket{
			Question: "Will Trump resign before 2027?",
		}
		if looksLikeWeatherMarket(m) {
			t.Fatal("unexpected weather market match")
		}
	})
}
