package brokersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"real_engine/common"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS real_broker_order_updates (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    message_type TEXT NOT NULL,
    order_no TEXT,
    exchange_order_no TEXT,
    correlation_id TEXT,
    client_id TEXT,
    security_id TEXT,
    symbol TEXT,
    status TEXT,
    txn_type TEXT,
    order_type TEXT,
    product TEXT,
    quantity BIGINT,
    traded_qty BIGINT,
    remaining_quantity BIGINT,
    price DOUBLE PRECISION,
    traded_price DOUBLE PRECISION,
    avg_traded_price DOUBLE PRECISION,
    order_time TIMESTAMPTZ,
    exchange_time TIMESTAMPTZ,
    last_updated_time TIMESTAMPTZ,
    reason_description TEXT,
    raw_payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_real_broker_order_updates_order_no
    ON real_broker_order_updates(order_no, last_updated_time DESC);
CREATE INDEX IF NOT EXISTS idx_real_broker_order_updates_correlation_id
    ON real_broker_order_updates(correlation_id);

CREATE TABLE IF NOT EXISTS real_broker_positions (
    id BIGSERIAL PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL,
    trading_symbol TEXT NOT NULL,
    security_id TEXT,
    exchange_segment TEXT,
    product_type TEXT,
    position_type TEXT,
    net_qty BIGINT,
    buy_qty BIGINT,
    sell_qty BIGINT,
    buy_avg DOUBLE PRECISION,
    sell_avg DOUBLE PRECISION,
    cost_price DOUBLE PRECISION,
    realized_profit DOUBLE PRECISION,
    unrealized_profit DOUBLE PRECISION,
    raw_payload JSONB NOT NULL
);`

type Sync struct {
	cfg        *common.AppConfig
	logger     *zap.Logger
	nc         *nats.Conn
	db         *pgxpool.Pool
	httpClient *http.Client
	mu         sync.Mutex
	seenFills  map[string]time.Time
}

func Start(ctx context.Context, cfg *common.AppConfig, logger *zap.Logger) (*Sync, error) {
	nc, err := nats.Connect(cfg.Kafka.BootstrapServers, nats.Name("real_engine-broker-sync"), nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		return nil, err
	}

	var pool *pgxpool.Pool
	if cfg.Postgres.Enabled {
		pool, err = pgxpool.New(ctx, buildPostgresDSN(cfg.Postgres))
		if err != nil {
			nc.Close()
			return nil, err
		}
		if _, err := pool.Exec(ctx, schemaSQL); err != nil {
			pool.Close()
			nc.Close()
			return nil, err
		}
	}

	s := &Sync{
		cfg:    cfg,
		logger: logger,
		nc:     nc,
		db:     pool,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.Dhan.RequestTimeoutSec) * time.Second,
		},
		seenFills: make(map[string]time.Time),
	}

	go s.runOrderUpdateLoop(ctx)
	go s.runPositionsLoop(ctx)
	return s, nil
}

func (s *Sync) Close() {
	if s.db != nil {
		s.db.Close()
	}
	if s.nc != nil {
		s.nc.Drain()
		s.nc.Close()
	}
}

func (s *Sync) runOrderUpdateLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, s.cfg.Dhan.OrderUpdateURL, nil)
		if err != nil {
			s.logger.Warn("broker sync websocket connect failed", zap.Error(err))
			time.Sleep(5 * time.Second)
			continue
		}

		auth := map[string]any{
			"LoginReq": map[string]any{
				"MsgCode":  42,
				"ClientId": s.cfg.Dhan.ClientID,
				"Token":    s.cfg.Dhan.AccessToken,
			},
			"UserType": "SELF",
		}
		if err := conn.WriteJSON(auth); err != nil {
			_ = conn.Close()
			s.logger.Warn("broker sync websocket auth failed", zap.Error(err))
			time.Sleep(5 * time.Second)
			continue
		}

		s.logger.Info("broker sync websocket connected")
		for {
			select {
			case <-ctx.Done():
				_ = conn.Close()
				return
			default:
			}
			_, raw, err := conn.ReadMessage()
			if err != nil {
				s.logger.Warn("broker sync websocket disconnected", zap.Error(err))
				_ = conn.Close()
				time.Sleep(2 * time.Second)
				break
			}
			if err := s.handleOrderUpdate(ctx, raw); err != nil {
				s.logger.Warn("broker sync order update failed", zap.Error(err))
			}
		}
	}
}

func (s *Sync) runPositionsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Dhan.PositionsPollSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.syncPositions(ctx); err != nil {
				s.logger.Warn("broker positions sync failed", zap.Error(err))
			}
		}
	}
}

func (s *Sync) handleOrderUpdate(ctx context.Context, raw []byte) error {
	raw = bytes.Trim(raw, "\x00\r\n\t ")
	if len(raw) == 0 {
		return nil
	}
	if raw[0] != '{' && raw[0] != '[' {
		return nil
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	data, _ := envelope["Data"].(map[string]any)
	if len(data) == 0 {
		return nil
	}

	update := map[string]any{
		"source_service":   "real_engine",
		"received_at":      time.Now().Format(time.RFC3339Nano),
		"message_type":     stringValue(envelope["Type"]),
		"order_no":         stringValue(data["OrderNo"]),
		"correlation_id":   stringValue(data["CorrelationId"]),
		"symbol":           stringValue(data["Symbol"]),
		"security_id":      stringValue(data["SecurityId"]),
		"status":           stringValue(data["Status"]),
		"transaction_type": stringValue(data["TxnType"]),
		"traded_qty":       intValue(data["TradedQty"]),
		"remaining_qty":    intValue(data["RemainingQuantity"]),
		"avg_traded_price": floatValue(data["AvgTradedPrice"]),
		"raw_payload":      envelope,
	}
	if err := s.publishJSON(s.cfg.Kafka.TradesTopic+".orders", update); err != nil {
		return err
	}
	if s.db != nil {
		rawJSON, _ := json.Marshal(envelope)
		_, err := s.db.Exec(ctx, `
			INSERT INTO real_broker_order_updates (
				message_type, order_no, correlation_id, client_id, security_id, symbol, status,
				txn_type, quantity, traded_qty, remaining_quantity, avg_traded_price, raw_payload
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			stringValue(envelope["Type"]),
			stringValue(data["OrderNo"]),
			stringValue(data["CorrelationId"]),
			stringValue(data["ClientId"]),
			stringValue(data["SecurityId"]),
			stringValue(data["Symbol"]),
			stringValue(data["Status"]),
			stringValue(data["TxnType"]),
			intValue(data["Quantity"]),
			intValue(data["TradedQty"]),
			intValue(data["RemainingQuantity"]),
			floatValue(data["AvgTradedPrice"]),
			rawJSON,
		)
		if err != nil {
			return err
		}
	}

	fillKey := fmt.Sprintf("%s|%d|%s", stringValue(data["OrderNo"]), intValue(data["TradedQty"]), stringValue(data["LastUpdatedTime"]))
	if !s.shouldEmitFill(fillKey, intValue(data["TradedQty"])) {
		return nil
	}
	return s.publishJSON("fills.real", update)
}

func (s *Sync) syncPositions(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.cfg.Dhan.BaseURL, "/")+s.cfg.Dhan.PositionsPath, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access-token", s.cfg.Dhan.AccessToken)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("positions status=%s body=%s", resp.Status, string(body))
	}

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	if err := s.publishJSON("positions.real", payload); err != nil {
		return err
	}
	if err := s.publishPnL(payload); err != nil {
		return err
	}
	return s.storePositions(ctx, payload)
}

func (s *Sync) publishPnL(payload any) error {
	items, ok := payload.([]any)
	if !ok {
		return nil
	}
	var realized, unrealized float64
	for _, item := range items {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		realized += floatValue(row["realizedProfit"])
		unrealized += floatValue(row["unrealizedProfit"])
	}
	return s.publishJSON("broker_pnl.real", map[string]any{
		"time":           time.Now().Format(time.RFC3339Nano),
		"realized_pnl":   realized,
		"unrealized_pnl": unrealized,
	})
}

func (s *Sync) storePositions(ctx context.Context, payload any) error {
	if s.db == nil {
		return nil
	}
	items, ok := payload.([]any)
	if !ok {
		return nil
	}
	now := time.Now()
	for _, item := range items {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		rawJSON, _ := json.Marshal(row)
		if _, err := s.db.Exec(ctx, `
			INSERT INTO real_broker_positions (
				snapshot_time, trading_symbol, security_id, exchange_segment, product_type,
				position_type, net_qty, buy_qty, sell_qty, buy_avg, sell_avg, cost_price,
				realized_profit, unrealized_profit, raw_payload
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			now,
			stringValue(row["tradingSymbol"]),
			stringValue(row["securityId"]),
			stringValue(row["exchangeSegment"]),
			stringValue(row["productType"]),
			stringValue(row["positionType"]),
			intValue(row["netQty"]),
			intValue(row["buyQty"]),
			intValue(row["sellQty"]),
			floatValue(row["buyAvg"]),
			floatValue(row["sellAvg"]),
			floatValue(row["costPrice"]),
			floatValue(row["realizedProfit"]),
			floatValue(row["unrealizedProfit"]),
			rawJSON,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sync) publishJSON(subject string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.nc.Publish(subject, raw)
}

func (s *Sync) shouldEmitFill(key string, tradedQty int64) bool {
	if tradedQty <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, at := range s.seenFills {
		if now.Sub(at) > 6*time.Hour {
			delete(s.seenFills, k)
		}
	}
	if _, exists := s.seenFills[key]; exists {
		return false
	}
	s.seenFills[key] = now
	return true
}

func buildPostgresDSN(cfg common.PostgresConfig) string {
	passwordPart := ""
	if cfg.Password != "" {
		passwordPart = ":" + cfg.Password
	}
	return fmt.Sprintf("postgres://%s%s@%s:%d/%s?sslmode=%s", cfg.User, passwordPart, cfg.Host, cfg.Port, cfg.DBName, cfg.SSLMode)
}

func stringValue(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func intValue(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case json.Number:
		i, _ := x.Int64()
		return i
	default:
		return 0
	}
}

func floatValue(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case json.Number:
		f, _ := x.Float64()
		return f
	default:
		return 0
	}
}

var _ = log.Printf
