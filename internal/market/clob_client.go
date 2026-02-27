package market

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type CLOBClient struct {
	baseURL string
	client  *http.Client
}

func NewCLOBClient(baseURL string, client *http.Client) *CLOBClient {
	return &CLOBClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  client,
	}
}

func (c *CLOBClient) GetMidpoint(ctx context.Context, tokenID string) (float64, error) {
	if tokenID == "" {
		return 0, fmt.Errorf("token id is empty")
	}
	return c.getNumber(ctx, "/midpoint/"+url.PathEscape(tokenID), []string{"mid", "midpoint", "price", "result"})
}

func (c *CLOBClient) GetSpread(ctx context.Context, tokenID string) (float64, error) {
	if tokenID == "" {
		return 0, fmt.Errorf("token id is empty")
	}
	return c.getNumber(ctx, "/spread/"+url.PathEscape(tokenID), []string{"spread", "result"})
}

func (c *CLOBClient) getNumber(ctx context.Context, path string, keys []string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return 0, fmt.Errorf("build clob request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "sky-alpha-pro/0.1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request clob %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("clob %s status: %d", path, resp.StatusCode)
	}

	var payload any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, fmt.Errorf("decode clob %s: %w", path, err)
	}

	if num, ok := parseFloatValue(payload); ok {
		return num, nil
	}
	if m, ok := payload.(map[string]any); ok {
		for _, key := range keys {
			if num, ok := parseFloatValue(m[key]); ok {
				return num, nil
			}
		}
	}
	return 0, fmt.Errorf("clob %s number not found", path)
}
