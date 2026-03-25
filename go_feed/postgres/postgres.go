package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go_feed/common"
	appConfig "go_feed/config"
	appLogger "go_feed/logger"
	"go_feed/monitor"
)

var (
	GlobalPool  *pgxpool.Pool
	insertQueue chan common.DhanTick
	workerGroup sync.WaitGroup
	closeOnce   sync.Once
)

// Connection creates the PostgreSQL pool, ensures the target table exists, and starts workers.
func Connection(ctx context.Context) error {
	if appConfig.GlobalConfig == nil {
		return fmt.Errorf("application config is not initialized")
	}

	pgCfg := appConfig.GlobalConfig.Postgres
	if pgCfg.Host == "" {
		return fmt.Errorf("postgres host is required")
	}
	if pgCfg.Port == 0 {
		return fmt.Errorf("postgres port is required")
	}
	if pgCfg.User == "" {
		return fmt.Errorf("postgres user is required")
	}
	if pgCfg.DBName == "" {
		return fmt.Errorf("postgres db_name is required")
	}

	poolConfig, err := pgxpool.ParseConfig(buildConnectionString(pgCfg))
	if err != nil {
		return err
	}
	poolConfig.MaxConns = pgCfg.MaxConns
	poolConfig.MinConns = pgCfg.MinConns

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		monitor.SetPostgresConnected(false)
		monitor.RecordError("postgres")
		return err
	}
	GlobalPool = pool
	monitor.SetPostgresConnected(true)

	if err := ensureTable(ctx, pool, pgCfg.TableName); err != nil {
		monitor.SetPostgresConnected(false)
		monitor.RecordError("postgres")
		return err
	}

	insertQueue = make(chan common.DhanTick, appConfig.GlobalConfig.Pipeline.PostgresQueueSize)
	monitor.SetPostgresQueueDepth(0)

	for workerIndex := 0; workerIndex < appConfig.GlobalConfig.Pipeline.PostgresWorkers; workerIndex++ {
		workerGroup.Add(1)
		go startWorker()
	}

	appLogger.Infof("postgres connection initialized successfully using table=%s", pgCfg.TableName)
	return nil
}

// EnqueueTick submits a full tick for PostgreSQL insertion.
func EnqueueTick(ctx context.Context, tick common.DhanTick) error {
	if insertQueue == nil {
		return fmt.Errorf("postgres queue is not initialized")
	}

	select {
	case insertQueue <- tick:
		monitor.SetPostgresQueueDepth(len(insertQueue))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close drains pending inserts and closes the PostgreSQL pool.
func Close(ctx context.Context) error {
	var closeErr error

	closeOnce.Do(func() {
		if insertQueue != nil {
			close(insertQueue)
		}

		done := make(chan struct{})
		go func() {
			workerGroup.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			closeErr = ctx.Err()
		}

		if GlobalPool != nil {
			GlobalPool.Close()
			GlobalPool = nil
		}
		monitor.SetPostgresConnected(false)
		monitor.SetPostgresQueueDepth(0)
	})

	return closeErr
}

func startWorker() {
	defer workerGroup.Done()

	for tick := range insertQueue {
		monitor.SetPostgresQueueDepth(len(insertQueue))
		for {
			writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := insertTick(writeCtx, tick)
			cancel()
			if err != nil {
				appLogger.Warnf("postgres insert failed for security_id=%s symbol=%s: %v", tick.SecurityID, tick.Symbol, err)
				monitor.RecordError("postgres")
				monitor.SetPostgresConnected(false)
				reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
				reconnectErr := reconnect(reconnectCtx)
				reconnectCancel()
				if reconnectErr != nil {
					time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
					continue
				}
				time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
				continue
			}
			monitor.RecordPostgresInserted()
			break
		}
	}
}

func ensureTable(ctx context.Context, pool *pgxpool.Pool, tableName string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			security_id TEXT NOT NULL,
			symbol TEXT,
			exchange_segment SMALLINT NOT NULL,
			response_code SMALLINT NOT NULL,
			feed_timestamp TIMESTAMPTZ NOT NULL,
			last_traded_price REAL,
			last_trade_quantity INTEGER,
			last_trade_time INTEGER,
			average_trade_price REAL,
			volume BIGINT,
			total_sell_quantity BIGINT,
			total_buy_quantity BIGINT,
			open_interest BIGINT,
			highest_open_interest BIGINT,
			lowest_open_interest BIGINT,
			day_open REAL,
			day_close REAL,
			day_high REAL,
			day_low REAL,
			depth JSONB,
			raw_payload BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, tableName)
	_, err := pool.Exec(ctx, query)
	return err
}

func insertTick(ctx context.Context, tick common.DhanTick) error {
	if GlobalPool == nil {
		return fmt.Errorf("postgres pool is not initialized")
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			security_id, symbol, exchange_segment, response_code, feed_timestamp,
			last_traded_price, last_trade_quantity, last_trade_time, average_trade_price,
			volume, total_sell_quantity, total_buy_quantity, open_interest,
			highest_open_interest, lowest_open_interest, day_open, day_close, day_high,
			day_low, depth, raw_payload
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
		)
	`, appConfig.GlobalConfig.Postgres.TableName)

	_, err := GlobalPool.Exec(
		ctx,
		query,
		tick.SecurityID,
		tick.Symbol,
		tick.ExchangeSegment,
		tick.ResponseCode,
		tick.FeedTimestamp,
		tick.LastTradedPrice,
		tick.LastTradeQuantity,
		tick.LastTradeTime,
		tick.AverageTradePrice,
		tick.Volume,
		tick.TotalSellQuantity,
		tick.TotalBuyQuantity,
		tick.OpenInterest,
		tick.HighestOI,
		tick.LowestOI,
		tick.DayOpen,
		tick.DayClose,
		tick.DayHigh,
		tick.DayLow,
		tick.Depth,
		tick.RawPayload,
	)
	return err
}

func reconnect(ctx context.Context) error {
	monitor.RecordPostgresReconnect()
	appLogger.Warnf("attempting postgres reconnect")
	if GlobalPool != nil {
		GlobalPool.Close()
		GlobalPool = nil
	}

	poolConfig, err := pgxpool.ParseConfig(buildConnectionString(appConfig.GlobalConfig.Postgres))
	if err != nil {
		return err
	}
	poolConfig.MaxConns = appConfig.GlobalConfig.Postgres.MaxConns
	poolConfig.MinConns = appConfig.GlobalConfig.Postgres.MinConns

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	GlobalPool = pool
	monitor.SetPostgresConnected(true)
	return ensureTable(ctx, pool, appConfig.GlobalConfig.Postgres.TableName)
}

func buildConnectionString(cfg common.PostgresConfig) string {
	if cfg.Password == "" {
		return fmt.Sprintf(
			"postgres://%s@%s:%d/%s?sslmode=%s",
			cfg.User,
			cfg.Host,
			cfg.Port,
			cfg.DBName,
			cfg.SSLMode,
		)
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DBName,
		cfg.SSLMode,
	)
}
