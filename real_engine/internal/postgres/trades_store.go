package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"real_engine/common"
	"real_engine/internal/engine"
)

type TradeStore struct {
	enabled bool
	table   string
	pool    *pgxpool.Pool
	logger  *zap.Logger
}

const createTradeEventsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    id BIGSERIAL PRIMARY KEY,
    event_key TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity BIGINT NOT NULL,
    trade_type TEXT NOT NULL,
    side TEXT NOT NULL,
    strategy TEXT NOT NULL,
    signal_time TIMESTAMPTZ NOT NULL,
    reason TEXT,
    stop_loss_pct DOUBLE PRECISION,
    target_pct DOUBLE PRECISION,
    trailing_stop_pct DOUBLE PRECISION,
    trailing_freeze_profit_pct DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS %s_event_time_idx ON %s (event_time);
CREATE INDEX IF NOT EXISTS %s_symbol_time_idx ON %s (symbol, event_time DESC);
`

func NewTradeStore(ctx context.Context, cfg common.PostgresConfig, logger *zap.Logger) (*TradeStore, error) {
	if !cfg.Enabled {
		return &TradeStore{enabled: false, table: cfg.TableName, logger: logger}, nil
	}
	dsn := buildConnectionString(cfg)
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres connection: %w", err)
	}
	poolCfg.MaxConns = int32(cfg.MaxConns)
	poolCfg.MinConns = int32(cfg.MinConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	store := &TradeStore{enabled: true, table: cfg.TableName, pool: pool, logger: logger}
	if err := store.ensureTable(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure trade events table: %w", err)
	}
	logger.Info("postgres trade store initialized", zap.String("table", cfg.TableName), zap.String("host", cfg.Host), zap.Int("port", cfg.Port), zap.String("db_name", cfg.DBName))
	return store, nil
}

func (s *TradeStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *TradeStore) Enabled() bool { return s != nil && s.enabled && s.pool != nil }

func (s *TradeStore) SaveTradeEvent(ctx context.Context, ev engine.TradeEvent) error {
	if !s.Enabled() {
		return nil
	}
	if strings.TrimSpace(ev.EventKey) == "" {
		ev.EventKey = eventKey(ev)
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			event_key, symbol, event_time, price, quantity, trade_type, side, strategy,
			signal_time, reason, stop_loss_pct, target_pct, trailing_stop_pct,
			trailing_freeze_profit_pct, realized_pnl
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
		)
		ON CONFLICT (event_key) DO NOTHING
	`, s.table),
		ev.EventKey, ev.Symbol, ev.Time, ev.Price, ev.Quantity, ev.TradeType, ev.Side, ev.Strategy,
		ev.SignalTime, ev.Reason, ev.StopLossPct, ev.TargetPct, ev.TrailingStopPct,
		ev.TrailingFreezeProfitPct, ev.RealizedPnl,
	)
	return err
}

func (s *TradeStore) LoadTradeHistory(ctx context.Context, since string) ([]engine.TradeEvent, error) {
	if !s.Enabled() {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT event_key, symbol, event_time, price, quantity, trade_type, side, strategy,
		       signal_time, reason, stop_loss_pct, target_pct, trailing_stop_pct,
		       trailing_freeze_profit_pct, realized_pnl
		FROM %s
		WHERE ($1 = '' OR event_time::date >= $1::date)
		ORDER BY event_time ASC, id ASC
	`, s.table)
	rows, err := s.pool.Query(ctx, query, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]engine.TradeEvent, 0)
	for rows.Next() {
		var ev engine.TradeEvent
		if err := rows.Scan(
			&ev.EventKey, &ev.Symbol, &ev.Time, &ev.Price, &ev.Quantity, &ev.TradeType, &ev.Side, &ev.Strategy,
			&ev.SignalTime, &ev.Reason, &ev.StopLossPct, &ev.TargetPct, &ev.TrailingStopPct,
			&ev.TrailingFreezeProfitPct, &ev.RealizedPnl,
		); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *TradeStore) ensureTable(ctx context.Context) error {
	sql := fmt.Sprintf(createTradeEventsTableSQL,
		s.table,
		s.table, s.table,
		s.table, s.table,
	)
	_, err := s.pool.Exec(ctx, sql)
	return err
}

func buildConnectionString(cfg common.PostgresConfig) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName, cfg.SSLMode,
	)
}

func eventKey(ev engine.TradeEvent) string {
	return fmt.Sprintf("%s|%s|%s|%s", ev.Symbol, ev.TradeType, ev.Strategy, ev.Time.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
}
