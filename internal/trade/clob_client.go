package trade

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"sky-alpha-pro/pkg/httpretry"
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

type placeOrderPayload struct {
	Order     SignedClobOrder `json:"order"`
	OrderType string          `json:"orderType"`
	Owner     string          `json:"owner,omitempty"`
}

func (c *CLOBClient) PlaceOrder(ctx context.Context, payload placeOrderPayload) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	endpoint := c.baseURL + "/order"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "sky-alpha-pro/0.1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return "", fmt.Errorf("clob place order status: %d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var decoded any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	return extractOrderID(decoded)
}

func (c *CLOBClient) CancelOrder(ctx context.Context, orderID string) error {
	if strings.TrimSpace(orderID) == "" {
		return fmt.Errorf("order id is empty")
	}
	endpoint := c.baseURL + "/order/" + url.PathEscape(orderID)

	resp, err := httpretry.DoRequestWithRetry(ctx, c.client, func() (*http.Request, error) {
		req, buildErr := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
		if buildErr != nil {
			return nil, buildErr
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "sky-alpha-pro/0.1.0")
		return req, nil
	}, 3)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("clob cancel order status: %d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func extractOrderID(v any) (string, error) {
	if m, ok := v.(map[string]any); ok {
		keys := []string{"orderID", "orderId", "order_id", "id", "result"}
		for _, k := range keys {
			if raw, exists := m[k]; exists {
				if s, ok := raw.(string); ok && strings.TrimSpace(s) != "" {
					return s, nil
				}
				if nested, ok := raw.(map[string]any); ok {
					if id, err := extractOrderID(nested); err == nil {
						return id, nil
					}
				}
			}
		}
	}
	return "", fmt.Errorf("order id not found in clob response")
}
