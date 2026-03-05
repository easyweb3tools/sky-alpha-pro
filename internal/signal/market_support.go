package signal

import (
	"strings"

	"sky-alpha-pro/internal/model"
)

const (
	unsupportedReasonEmptyQuestion   = "empty_question"
	unsupportedReasonKnownNonWeather = "known_non_weather_market"
	unsupportedReasonNoTempMarker    = "no_temperature_marker"
	unsupportedReasonNoThreshold     = "no_threshold"
	unsupportedReasonNoComparator    = "no_comparator"
)

var unsupportedMarketMarkers = []string{
	"global temperature increase",
	"hottest year on record",
	"hottest on record",
	"world cup",
	"most sixes",
	"government shutdown",
	"democratic party",
	"republican party",
	"china blockade",
	"blockade taiwan",
}

var temperatureMarketMarkers = []string{
	"temperature",
	"temp",
	"fahrenheit",
	"celsius",
	"°f",
	"ºf",
	"°c",
	"ºc",
}

// IsSupportedMarketForSignal returns true when a market can be handled by the
// current city+threshold weather signal model.
func IsSupportedMarketForSignal(m model.Market) bool {
	return UnsupportedReasonForSignal(m) == ""
}

// UnsupportedReasonForSignal returns empty string if supported, otherwise a stable reason code.
func UnsupportedReasonForSignal(m model.Market) string {
	mt := strings.ToLower(strings.TrimSpace(m.MarketType))
	if mt == "temperature_high" || mt == "temperature_low" {
		return ""
	}

	q := strings.ToLower(strings.TrimSpace(m.Question))
	if q == "" {
		return unsupportedReasonEmptyQuestion
	}
	combined := q + " " + strings.ToLower(strings.ReplaceAll(strings.TrimSpace(m.Slug), "-", " "))
	for _, marker := range unsupportedMarketMarkers {
		if strings.Contains(combined, marker) {
			return unsupportedReasonKnownNonWeather
		}
	}

	hasTempMarker := false
	for _, marker := range temperatureMarketMarkers {
		if strings.Contains(combined, marker) {
			hasTempMarker = true
			break
		}
	}
	if !hasTempMarker {
		return unsupportedReasonNoTempMarker
	}

	if _, ok := parseThresholdFahrenheit(m.Question); !ok {
		return unsupportedReasonNoThreshold
	}
	if normalizeComparator(m.Comparator, m.MarketType) == "" && inferComparatorFromQuestion(m.Question) == "" {
		return unsupportedReasonNoComparator
	}
	return ""
}
