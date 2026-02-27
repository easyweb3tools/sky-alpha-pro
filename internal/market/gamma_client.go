package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type GammaClient struct {
	baseURL string
	client  *http.Client
}

var weatherMarketPattern = regexp.MustCompile(`(?i)\b(weather|temperature|high temperature|low temperature|fahrenheit|celsius|degrees?)\b|°f|°c`)

type httpStatusError struct {
	status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("http status: %d", e.status)
}

func NewGammaClient(baseURL string, client *http.Client) *GammaClient {
	return &GammaClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  client,
	}
}

func (c *GammaClient) ListMarkets(ctx context.Context, weatherTag string, activeOnly bool, limit int) ([]GammaMarket, error) {
	if limit <= 0 {
		limit = 100
	}

	markets, err := c.listMarkets(ctx, weatherTag, activeOnly, limit)
	usedFallback := false
	if err != nil {
		var statusErr *httpStatusError
		if weatherTag != "" && errors.As(err, &statusErr) && (statusErr.status == http.StatusUnprocessableEntity || statusErr.status == http.StatusBadRequest) {
			markets, err = c.listMarkets(ctx, "", activeOnly, limit)
			usedFallback = true
		}
	}
	if err != nil {
		return nil, err
	}

	if usedFallback {
		filtered := make([]GammaMarket, 0, len(markets))
		for _, m := range markets {
			if looksLikeWeatherMarket(m) {
				filtered = append(filtered, m)
			}
		}
		return filtered, nil
	}

	return markets, nil
}

func (c *GammaClient) listMarkets(ctx context.Context, weatherTag string, activeOnly bool, limit int) ([]GammaMarket, error) {
	u, err := url.Parse(c.baseURL + "/markets")
	if err != nil {
		return nil, fmt.Errorf("parse gamma url: %w", err)
	}
	q := u.Query()
	if weatherTag != "" {
		q.Set("tag_id", weatherTag)
	}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("closed", "false")
	if activeOnly {
		q.Set("active", "true")
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build gamma request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "sky-alpha-pro/0.1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request gamma markets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &httpStatusError{status: resp.StatusCode}
	}

	var payload any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode gamma markets: %w", err)
	}

	rows, err := extractMarketRows(payload)
	if err != nil {
		return nil, err
	}

	markets := make([]GammaMarket, 0, len(rows))
	for _, row := range rows {
		parsed := parseGammaMarket(row)
		if parsed.PolymarketID == "" || parsed.Question == "" {
			continue
		}
		markets = append(markets, parsed)
	}
	return markets, nil
}

func looksLikeWeatherMarket(m GammaMarket) bool {
	text := strings.ToLower(strings.Join([]string{
		m.Question,
		m.Description,
		m.Slug,
		m.MarketType,
	}, " "))
	return weatherMarketPattern.MatchString(text)
}

func extractMarketRows(payload any) ([]map[string]any, error) {
	switch p := payload.(type) {
	case []any:
		rows := make([]map[string]any, 0, len(p))
		for _, item := range p {
			if m, ok := item.(map[string]any); ok {
				rows = append(rows, m)
			}
		}
		return rows, nil
	case map[string]any:
		for _, key := range []string{"data", "markets", "results"} {
			if nested, ok := p[key]; ok {
				return extractMarketRows(nested)
			}
		}
		// Some APIs return a single object.
		return []map[string]any{p}, nil
	default:
		return nil, fmt.Errorf("unexpected gamma payload type %T", payload)
	}
}

func parseGammaMarket(row map[string]any) GammaMarket {
	m := GammaMarket{
		PolymarketID: firstString(row, "id", "market_id"),
		ConditionID:  firstString(row, "conditionId", "condition_id"),
		Slug:         firstString(row, "slug"),
		Question:     firstString(row, "question", "title"),
		Description:  firstString(row, "description"),
		City:         firstString(row, "city"),
		MarketType:   firstString(row, "market_type", "marketType"),
		IsActive:     firstBool(row, true, "active", "isActive"),
		IsResolved:   firstBool(row, false, "resolved", "isResolved", "closed"),
		Resolution:   strings.ToUpper(firstString(row, "resolution")),
		VolumeTotal:  firstFloat(row, "volume", "volumeTotal"),
		Liquidity:    firstFloat(row, "liquidity"),
	}

	m.EndDate = firstTime(row, "endDate", "end_date", "resolveDate", "resolve_date", "closedTime")

	m.RawOutcomes = firstStringSlice(row, "outcomes")
	m.RawTokenIDs = firstStringSlice(row, "clobTokenIds", "clob_token_ids", "tokenIds", "token_ids")
	m.RawPricePairs = firstFloatSlice(row, "outcomePrices", "outcome_prices")

	if len(m.RawOutcomes) >= 1 {
		m.OutcomeYes = m.RawOutcomes[0]
	}
	if len(m.RawOutcomes) >= 2 {
		m.OutcomeNo = m.RawOutcomes[1]
	}
	if len(m.RawTokenIDs) >= 1 {
		m.TokenIDYes = m.RawTokenIDs[0]
	}
	if len(m.RawTokenIDs) >= 2 {
		m.TokenIDNo = m.RawTokenIDs[1]
	}
	if len(m.RawPricePairs) >= 1 {
		m.PriceYes = m.RawPricePairs[0]
	}
	if len(m.RawPricePairs) >= 2 {
		m.PriceNo = m.RawPricePairs[1]
	}

	// If outcomes explicitly define YES/NO ordering, remap token IDs and prices.
	for i := range m.RawOutcomes {
		outcome := strings.TrimSpace(strings.ToUpper(m.RawOutcomes[i]))
		switch outcome {
		case "YES":
			m.OutcomeYes = m.RawOutcomes[i]
			if i < len(m.RawTokenIDs) {
				m.TokenIDYes = m.RawTokenIDs[i]
			}
			if i < len(m.RawPricePairs) {
				m.PriceYes = m.RawPricePairs[i]
			}
		case "NO":
			m.OutcomeNo = m.RawOutcomes[i]
			if i < len(m.RawTokenIDs) {
				m.TokenIDNo = m.RawTokenIDs[i]
			}
			if i < len(m.RawPricePairs) {
				m.PriceNo = m.RawPricePairs[i]
			}
		}
	}

	return m
}

func firstString(row map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case string:
			return strings.TrimSpace(v)
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case json.Number:
			return v.String()
		}
	}
	return ""
}

func firstBool(row map[string]any, fallback bool, keys ...string) bool {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case bool:
			return v
		case string:
			parsed, err := strconv.ParseBool(v)
			if err == nil {
				return parsed
			}
		}
	}
	return fallback
}

func firstFloat(row map[string]any, keys ...string) float64 {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}
		if num, ok := parseFloatValue(value); ok {
			return num
		}
	}
	return 0
}

func firstStringSlice(row map[string]any, keys ...string) []string {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case []any:
			out := make([]string, 0, len(v))
			for _, item := range v {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			if len(out) > 0 {
				return out
			}
		case []string:
			return v
		case string:
			var out []string
			if err := json.Unmarshal([]byte(v), &out); err == nil && len(out) > 0 {
				return out
			}
		}
	}
	return nil
}

func firstFloatSlice(row map[string]any, keys ...string) []float64 {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case []any:
			out := make([]float64, 0, len(v))
			for _, item := range v {
				if num, ok := parseFloatValue(item); ok {
					out = append(out, num)
				}
			}
			if len(out) > 0 {
				return out
			}
		case string:
			var out []float64
			if err := json.Unmarshal([]byte(v), &out); err == nil && len(out) > 0 {
				return out
			}
			var outStr []string
			if err := json.Unmarshal([]byte(v), &outStr); err == nil && len(outStr) > 0 {
				conv := make([]float64, 0, len(outStr))
				for _, s := range outStr {
					if num, ok := parseFloatValue(s); ok {
						conv = append(conv, num)
					}
				}
				if len(conv) > 0 {
					return conv
				}
			}
		}
	}
	return nil
}

func firstTime(row map[string]any, keys ...string) time.Time {
	for _, key := range keys {
		v := firstString(row, key)
		if v == "" {
			continue
		}
		if t, err := parseTime(v); err == nil {
			return t
		}
	}
	return time.Time{}
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %q", s)
}

func parseFloatValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
