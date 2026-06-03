package dhan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"real_engine/common"
	"real_engine/internal/engine"
)

type Client struct {
	cfg        common.DhanConfig
	logger     *zap.Logger
	httpClient *http.Client
	symbolMap  map[string]string
}

type placeOrderResponse struct {
	OrderID      string `json:"orderId"`
	OrderStatus  string `json:"orderStatus"`
	ErrorType    string `json:"errorType"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

type orderDetailsResponse struct {
	OrderID      string  `json:"orderId"`
	OrderStatus  string  `json:"orderStatus"`
	AveragePrice float64 `json:"averageTradedPrice"`
	TradedPrice  float64 `json:"tradedPrice"`
	Quantity     int64   `json:"quantity"`
	TradedQty    int64   `json:"tradedQty"`
}

func NewClient(cfg common.DhanConfig, logger *zap.Logger) (*Client, error) {
	symbolMap, err := loadSymbolSecurityMap(cfg.InstrumentCSVPath)
	if err != nil {
		return nil, err
	}
	return &Client{
		cfg:        cfg,
		logger:     logger,
		httpClient: &http.Client{Timeout: time.Duration(cfg.RequestTimeoutSec) * time.Second},
		symbolMap:  symbolMap,
	}, nil
}

func (c *Client) PlaceMarketOrder(ctx context.Context, event engine.TradeEvent) (engine.TradeEvent, error) {
	securityID, ok := c.symbolMap[strings.ToUpper(event.Symbol)]
	if !ok {
		return event, fmt.Errorf("security id not found for symbol=%s", event.Symbol)
	}

	body := map[string]any{
		"dhanClientId":     c.cfg.ClientID,
		"correlationId":    truncateCorrelationID(event.EventKey),
		"transactionType":  strings.ToUpper(sideToTxn(event.Side)),
		"exchangeSegment":  c.cfg.ExchangeSegment,
		"productType":      c.cfg.ProductType,
		"orderType":        c.cfg.OrderType,
		"validity":         c.cfg.Validity,
		"securityId":       securityID,
		"quantity":         fmt.Sprintf("%d", event.Quantity),
		"price":            "",
		"triggerPrice":     "",
		"afterMarketOrder": false,
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.resolveURL("/v2/orders"), bytes.NewReader(raw))
	if err != nil {
		return event, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access-token", c.cfg.AccessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return event, err
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	var out placeOrderResponse
	_ = json.Unmarshal(payload, &out)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || isOrderRejected(out) {
		return event, fmt.Errorf("status=%d code=%s message=%s", resp.StatusCode, out.ErrorCode, firstNonEmpty(out.ErrorMessage, out.OrderStatus))
	}

	filled := event
	if details, err := c.fetchOrderDetails(ctx, out.OrderID); err == nil {
		filled = c.populateFillDetails(filled, details)
	}
	return filled, nil
}

func (c *Client) populateFillDetails(event engine.TradeEvent, details orderDetailsResponse) engine.TradeEvent {
	if details.AveragePrice > 0 {
		event.Price = details.AveragePrice
	} else if details.TradedPrice > 0 {
		event.Price = details.TradedPrice
	}
	if details.TradedQty > 0 {
		event.Quantity = details.TradedQty
	}
	return event
}

func isOrderRejected(resp placeOrderResponse) bool {
	status := strings.ToUpper(strings.TrimSpace(resp.OrderStatus))
	return strings.Contains(status, "REJECT") || strings.TrimSpace(resp.ErrorCode) != ""
}

func (c *Client) fetchOrderDetails(ctx context.Context, orderID string) (orderDetailsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.resolveURL("/v2/orders/"+orderID), nil)
	if err != nil {
		return orderDetailsResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access-token", c.cfg.AccessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return orderDetailsResponse{}, err
	}
	defer resp.Body.Close()
	var out orderDetailsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return orderDetailsResponse{}, err
	}
	return out, nil
}

func (c *Client) resolveURL(path string) string {
	return strings.TrimRight(c.cfg.BaseURL, "/") + path
}

func sideToTxn(side string) string {
	if strings.EqualFold(side, string(engine.SideShort)) || strings.EqualFold(side, string(engine.SideSell)) {
		return "SELL"
	}
	return "BUY"
}

func truncateCorrelationID(value string) string {
	value = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == ' ' {
			return r
		}
		return '_'
	}, value)
	if len(value) > 30 {
		return value[:30]
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
