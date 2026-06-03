package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

const serviceName = "real_engine_broker_sync"

const createBrokerSyncSchemaSQL = `
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
);
CREATE INDEX IF NOT EXISTS idx_real_broker_positions_snapshot_time
    ON real_broker_positions(snapshot_time DESC);
`

type appConfig struct {
	Env  string `json:"env"`
	NATS struct {
		URL              string `json:"url"`
		OrdersSubject    string `json:"orders_subject"`
		FillsSubject     string `json:"fills_subject"`
		PositionsSubject string `json:"positions_subject"`
		BrokerPnLSubject string `json:"broker_pnl_subject"`
	} `json:"nats"`
	Postgres struct {
		Enabled  bool   `json:"enabled"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		DBName   string `json:"db_name"`
		SSLMode  string `json:"sslmode"`
	} `json:"postgres"`
	Dhan struct {
		RealEngineConfigPath  string `json:"real_engine_config_path"`
		OrderUpdateURL        string `json:"order_update_url"`
		PositionsPath         string `json:"positions_path"`
		PositionsPollInterval int    `json:"positions_poll_interval_sec"`
		RequestTimeoutSec     int    `json:"request_timeout_sec"`
	} `json:"dhan"`
}

type engineConfig struct {
	Dhan struct {
		ClientID    string `json:"client_id"`
		AccessToken string `json:"access_token"`
		BaseURL     string `json:"base_url"`
	} `json:"dhan"`
}

type brokerSync struct {
	cfg          *appConfig
	nc           *nats.Conn
	db           *pgxpool.Pool
	httpClient   *http.Client
	mu           sync.Mutex
	seenFillKeys map[string]time.Time
}

func main() {
	configPath := flag.String("config", "config/real_engine_broker_sync_config.json", "path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(cfg.NATS.URL, nats.Name(serviceName), nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Fatalf("nats connect error: %v", err)
	}
	defer nc.Close()

	var pool *pgxpool.Pool
	if cfg.Postgres.Enabled {
		pool, err = pgxpool.New(ctx, buildPostgresDSN(cfg))
		if err != nil {
			log.Fatalf("postgres connect error: %v", err)
		}
		defer pool.Close()
		if _, err := pool.Exec(ctx, createBrokerSyncSchemaSQL); err != nil {
			log.Fatalf("postgres schema ensure error: %v", err)
		}
	}

	sync := &brokerSync{
		cfg: cfg,
		nc:  nc,
		db:  pool,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.Dhan.RequestTimeoutSec) * time.Second,
		},
		seenFillKeys: make(map[string]time.Time),
	}

	log.Printf("%s started env=%s ws=%s positions=%s", serviceName, cfg.Env, cfg.Dhan.OrderUpdateURL, cfg.Dhan.PositionsPath)

	go sync.runOrderUpdateLoop(ctx)
	go sync.runPositionsLoop(ctx)

	<-ctx.Done()
	log.Printf("%s shutting down", serviceName)
}

func loadConfig(path string) (*appConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg appConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		cfg.NATS.URL = "nats://127.0.0.1:4222"
	}
	if strings.TrimSpace(cfg.NATS.OrdersSubject) == "" {
		cfg.NATS.OrdersSubject = "orders.real"
	}
	if strings.TrimSpace(cfg.NATS.FillsSubject) == "" {
		cfg.NATS.FillsSubject = "fills.real"
	}
	if strings.TrimSpace(cfg.NATS.PositionsSubject) == "" {
		cfg.NATS.PositionsSubject = "positions.real"
	}
	if strings.TrimSpace(cfg.NATS.BrokerPnLSubject) == "" {
		cfg.NATS.BrokerPnLSubject = "broker_pnl.real"
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
	}
	if strings.TrimSpace(cfg.Postgres.SSLMode) == "" {
		cfg.Postgres.SSLMode = "disable"
	}
	if strings.TrimSpace(cfg.Dhan.RealEngineConfigPath) == "" {
		return nil, fmt.Errorf("dhan.real_engine_config_path is required")
	}
	if strings.TrimSpace(cfg.Dhan.OrderUpdateURL) == "" {
		cfg.Dhan.OrderUpdateURL = "wss://api-order-update.dhan.co"
	}
	if strings.TrimSpace(cfg.Dhan.PositionsPath) == "" {
		cfg.Dhan.PositionsPath = "/v2/positions"
	}
	if cfg.Dhan.PositionsPollInterval <= 0 {
		cfg.Dhan.PositionsPollInterval = 15
	}
	if cfg.Dhan.RequestTimeoutSec <= 0 {
		cfg.Dhan.RequestTimeoutSec = 10
	}
	return &cfg, nil
}

func buildPostgresDSN(cfg *appConfig) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DBName,
		cfg.Postgres.SSLMode,
	)
}

func (s *brokerSync) runOrderUpdateLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		engineCfg, err := s.loadEngineConfig()
		if err != nil {
			log.Printf("load real_engine config failed: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, s.cfg.Dhan.OrderUpdateURL, nil)
		if err != nil {
			log.Printf("order update websocket connect failed: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		auth := map[string]any{
			"LoginReq": map[string]any{
				"MsgCode":  42,
				"ClientId": engineCfg.Dhan.ClientID,
				"Token":    engineCfg.Dhan.AccessToken,
			},
			"UserType": "SELF",
		}
		if err := conn.WriteJSON(auth); err != nil {
			_ = conn.Close()
			log.Printf("order update websocket auth failed: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("order update websocket connected")

		if err := s.consumeOrderUpdates(ctx, conn); err != nil && ctx.Err() == nil {
			log.Printf("order update websocket disconnected: %v", err)
		}
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

func (s *brokerSync) consumeOrderUpdates(ctx context.Context, conn *websocket.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := s.handleOrderUpdate(ctx, raw); err != nil {
			log.Printf("handle order update failed: %v", err)
		}
	}
}

func (s *brokerSync) handleOrderUpdate(ctx context.Context, raw []byte) error {
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("unmarshal order update: %w", err)
	}

	data, _ := envelope["Data"].(map[string]any)
	if len(data) == 0 {
		return nil
	}

	update := map[string]any{
		"source_service":   serviceName,
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

	encoded, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal order event: %w", err)
	}
	if err := s.nc.Publish(s.cfg.NATS.OrdersSubject, encoded); err != nil {
		return fmt.Errorf("publish order event: %w", err)
	}

	if shouldPublishFill(data) && s.markFillSeen(fillKey(data)) {
		if err := s.nc.Publish(s.cfg.NATS.FillsSubject, encoded); err != nil {
			return fmt.Errorf("publish fill event: %w", err)
		}
	}

	if s.db != nil {
		if err := s.insertOrderUpdate(ctx, envelope, data); err != nil {
			return fmt.Errorf("insert order update: %w", err)
		}
	}

	return nil
}

func shouldPublishFill(data map[string]any) bool {
	status := strings.ToUpper(strings.TrimSpace(stringValue(data["Status"])))
	return intValue(data["TradedQty"]) > 0 || status == "TRADED"
}

func fillKey(data map[string]any) string {
	return strings.Join([]string{
		stringValue(data["OrderNo"]),
		fmt.Sprintf("%d", intValue(data["TradedQty"])),
		stringValue(data["LastUpdatedTime"]),
	}, "|")
}

func (s *brokerSync) markFillSeen(key string) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for existing, seenAt := range s.seenFillKeys {
		if now.Sub(seenAt) > 12*time.Hour {
			delete(s.seenFillKeys, existing)
		}
	}
	if _, exists := s.seenFillKeys[key]; exists {
		return false
	}
	s.seenFillKeys[key] = now
	return true
}

func (s *brokerSync) insertOrderUpdate(ctx context.Context, envelope, data map[string]any) error {
	rawJSON, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(ctx, `
		INSERT INTO real_broker_order_updates (
			message_type, order_no, exchange_order_no, correlation_id, client_id, security_id,
			symbol, status, txn_type, order_type, product, quantity, traded_qty,
			remaining_quantity, price, traded_price, avg_traded_price, order_time,
			exchange_time, last_updated_time, reason_description, raw_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18,
			$19, $20, $21, $22::jsonb
		)
	`,
		stringValue(envelope["Type"]),
		stringValue(data["OrderNo"]),
		stringValue(data["ExchOrderNo"]),
		stringValue(data["CorrelationId"]),
		stringValue(data["ClientId"]),
		stringValue(data["SecurityId"]),
		stringValue(data["Symbol"]),
		stringValue(data["Status"]),
		stringValue(data["TxnType"]),
		stringValue(data["OrderType"]),
		stringValue(data["Product"]),
		intValue(data["Quantity"]),
		intValue(data["TradedQty"]),
		intValue(data["RemainingQuantity"]),
		floatValue(data["Price"]),
		floatValue(data["TradedPrice"]),
		floatValue(data["AvgTradedPrice"]),
		parseDhanTimestamp(stringValue(data["OrderDateTime"])),
		parseDhanTimestamp(stringValue(data["ExchOrderTime"])),
		parseDhanTimestamp(stringValue(data["LastUpdatedTime"])),
		stringValue(data["ReasonDescription"]),
		string(rawJSON),
	)
	return err
}

func (s *brokerSync) runPositionsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Dhan.PositionsPollInterval) * time.Second)
	defer ticker.Stop()

	for {
		if err := s.syncPositions(ctx); err != nil && ctx.Err() == nil {
			log.Printf("positions sync failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *brokerSync) syncPositions(ctx context.Context) error {
	engineCfg, err := s.loadEngineConfig()
	if err != nil {
		return err
	}

	url := strings.TrimRight(engineCfg.Dhan.BaseURL, "/") + s.cfg.Dhan.PositionsPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access-token", engineCfg.Dhan.AccessToken)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("positions api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var positions []map[string]any
	if err := json.Unmarshal(body, &positions); err != nil {
		return fmt.Errorf("unmarshal positions response: %w", err)
	}

	snapshotTime := time.Now().UTC()
	var totalRealized float64
	var totalUnrealized float64
	var openPositions int

	for _, position := range positions {
		totalRealized += floatValue(position["realizedProfit"])
		totalUnrealized += floatValue(position["unrealizedProfit"])
		if intValue(position["netQty"]) != 0 {
			openPositions++
		}

		payload := map[string]any{
			"source_service": serviceName,
			"snapshot_time":  snapshotTime.Format(time.RFC3339Nano),
			"position":       position,
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if err := s.nc.Publish(s.cfg.NATS.PositionsSubject, encoded); err != nil {
			return err
		}
		if s.db != nil {
			if err := s.insertPositionSnapshot(ctx, snapshotTime, position); err != nil {
				return err
			}
		}
	}

	pnlPayload := map[string]any{
		"source_service":    serviceName,
		"snapshot_time":     snapshotTime.Format(time.RFC3339Nano),
		"open_positions":    openPositions,
		"realized_profit":   totalRealized,
		"unrealized_profit": totalUnrealized,
	}
	encoded, err := json.Marshal(pnlPayload)
	if err != nil {
		return err
	}
	if err := s.nc.Publish(s.cfg.NATS.BrokerPnLSubject, encoded); err != nil {
		return err
	}
	log.Printf("positions sync success open_positions=%d realized=%.2f unrealized=%.2f snapshots=%d",
		openPositions, totalRealized, totalUnrealized, len(positions))
	return nil
}

func (s *brokerSync) insertPositionSnapshot(ctx context.Context, snapshotTime time.Time, position map[string]any) error {
	rawJSON, err := json.Marshal(position)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `
		INSERT INTO real_broker_positions (
			snapshot_time, trading_symbol, security_id, exchange_segment, product_type, position_type,
			net_qty, buy_qty, sell_qty, buy_avg, sell_avg, cost_price, realized_profit,
			unrealized_profit, raw_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12, $13,
			$14, $15::jsonb
		)
	`,
		snapshotTime,
		stringValue(position["tradingSymbol"]),
		stringValue(position["securityId"]),
		stringValue(position["exchangeSegment"]),
		stringValue(position["productType"]),
		stringValue(position["positionType"]),
		intValue(position["netQty"]),
		intValue(position["buyQty"]),
		intValue(position["sellQty"]),
		floatValue(position["buyAvg"]),
		floatValue(position["sellAvg"]),
		floatValue(position["costPrice"]),
		floatValue(position["realizedProfit"]),
		floatValue(position["unrealizedProfit"]),
		string(rawJSON),
	)
	return err
}

func (s *brokerSync) loadEngineConfig() (*engineConfig, error) {
	raw, err := os.ReadFile(s.cfg.Dhan.RealEngineConfigPath)
	if err != nil {
		return nil, err
	}
	var cfg engineConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Dhan.ClientID) == "" {
		return nil, fmt.Errorf("real_engine config missing dhan.client_id")
	}
	if strings.TrimSpace(cfg.Dhan.AccessToken) == "" {
		return nil, fmt.Errorf("real_engine config missing dhan.access_token")
	}
	if strings.TrimSpace(cfg.Dhan.BaseURL) == "" {
		cfg.Dhan.BaseURL = "https://api.dhan.co"
	}
	return &cfg, nil
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func intValue(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case json.Number:
		n, _ := v.Int64()
		return n
	default:
		return 0
	}
}

func floatValue(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		n, _ := v.Float64()
		return n
	default:
		return 0
	}
}

func parseDhanTimestamp(value string) any {
	value = strings.TrimSpace(value)
	if value == "" || value == "0001-01-01 00:00:00" {
		return nil
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UTC()
		}
	}
	return nil
}
