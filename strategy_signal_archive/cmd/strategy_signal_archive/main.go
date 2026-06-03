package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

type appConfig struct {
	Env  string `json:"env"`
	NATS struct {
		URL           string `json:"url"`
		SignalSubject string `json:"signal_subject"`
	} `json:"nats"`
	Postgres struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		DBName   string `json:"db_name"`
		SSLMode  string `json:"sslmode"`
	} `json:"postgres"`
}

const createTradeSignalHistoryTableSQL = `
CREATE TABLE IF NOT EXISTS trade_signal_history (
    id BIGSERIAL PRIMARY KEY,
    signal_id TEXT NOT NULL UNIQUE,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    signal_time TIMESTAMPTZ NOT NULL,
    service_name TEXT NOT NULL,
    subject TEXT NOT NULL,
    strategy TEXT NOT NULL,
    strategy_version TEXT NOT NULL DEFAULT 'unknown',
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    timeframe TEXT,
    reason TEXT,
    setup_type TEXT,
    trigger_type TEXT,
    decision_score DOUBLE PRECISION,
    entry_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    market_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    filter_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trade_signal_history_strategy_symbol_time
    ON trade_signal_history(strategy, symbol, signal_time DESC);
`

func main() {
	configPath := flag.String("config", "config/strategy_signal_archive_config.json", "path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, buildPostgresDSN(cfg))
	if err != nil {
		log.Fatalf("postgres connect error: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, createTradeSignalHistoryTableSQL); err != nil {
		log.Fatalf("postgres schema ensure error: %v", err)
	}

	nc, err := nats.Connect(cfg.NATS.URL, nats.Name("strategy_signal_archive"))
	if err != nil {
		log.Fatalf("nats connect error: %v", err)
	}
	defer nc.Close()

	log.Printf("strategy_signal_archive started env=%s nats=%s subject=%s", cfg.Env, cfg.NATS.URL, cfg.NATS.SignalSubject)

	_, err = nc.Subscribe(cfg.NATS.SignalSubject, func(msg *nats.Msg) {
		if err := archiveSignal(ctx, pool, msg); err != nil {
			log.Printf("archive signal failed: %v", err)
		}
	})
	if err != nil {
		log.Fatalf("nats subscribe error: %v", err)
	}

	<-ctx.Done()
	log.Printf("strategy_signal_archive shutting down")
}

func loadConfig(path string) (*appConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg appConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		cfg.NATS.URL = "nats://127.0.0.1:4222"
	}
	if strings.TrimSpace(cfg.NATS.SignalSubject) == "" {
		cfg.NATS.SignalSubject = "signals.strategy"
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
	}
	if strings.TrimSpace(cfg.Postgres.SSLMode) == "" {
		cfg.Postgres.SSLMode = "disable"
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

func archiveSignal(ctx context.Context, pool *pgxpool.Pool, msg *nats.Msg) error {
	var payload map[string]any
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		return fmt.Errorf("unmarshal signal payload: %w", err)
	}

	rawJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal raw payload: %w", err)
	}

	signalTime, err := parseSignalTime(stringValue(payload["time"]))
	if err != nil {
		return fmt.Errorf("parse signal time: %w", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO trade_signal_history (
			signal_id, signal_time, service_name, subject, strategy, strategy_version,
			symbol, side, timeframe, reason, setup_type, trigger_type, decision_score,
			entry_context, market_context, risk_context, filter_context, raw_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12, $13,
			$14::jsonb, $15::jsonb, $16::jsonb, $17::jsonb, $18::jsonb
		)
		ON CONFLICT (signal_id) DO UPDATE SET
			signal_time = EXCLUDED.signal_time,
			service_name = EXCLUDED.service_name,
			subject = EXCLUDED.subject,
			strategy = EXCLUDED.strategy,
			strategy_version = EXCLUDED.strategy_version,
			symbol = EXCLUDED.symbol,
			side = EXCLUDED.side,
			timeframe = EXCLUDED.timeframe,
			reason = EXCLUDED.reason,
			setup_type = EXCLUDED.setup_type,
			trigger_type = EXCLUDED.trigger_type,
			decision_score = EXCLUDED.decision_score,
			entry_context = EXCLUDED.entry_context,
			market_context = EXCLUDED.market_context,
			risk_context = EXCLUDED.risk_context,
			filter_context = EXCLUDED.filter_context,
			raw_payload = EXCLUDED.raw_payload
	`,
		stringValue(payload["signal_id"]),
		signalTime,
		stringValue(payload["signal_source_service"]),
		msg.Subject,
		stringValue(payload["strategy"]),
		defaultString(stringValue(payload["strategy_version"]), "unknown"),
		stringValue(payload["symbol"]),
		stringValue(payload["side"]),
		stringValue(payload["timeframe"]),
		stringValue(payload["reason"]),
		stringValue(payload["setup_type"]),
		stringValue(payload["trigger_type"]),
		floatValue(payload["score"]),
		jsonMap(payload["entry_context"]),
		jsonMap(payload["market_context"]),
		jsonMap(payload["risk_context"]),
		jsonMap(payload["filter_context"]),
		string(rawJSON),
	)
	return err
}

func parseSignalTime(value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, fmt.Errorf("empty signal time")
	}
	return time.Parse(time.RFC3339Nano, value)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func floatValue(value any) any {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return nil
	}
}

func jsonMap(value any) string {
	if value == nil {
		return "{}"
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
