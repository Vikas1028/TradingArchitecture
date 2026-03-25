package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"marketdata_go_feed/common"
	"marketdata_go_feed/internal/candle"
	"marketdata_go_feed/internal/config"
	"marketdata_go_feed/internal/kafka"
	"marketdata_go_feed/internal/logging"
	"marketdata_go_feed/internal/metrics"
)

// main is the entrypoint for the market data candle + VWAP builder service.
// Flow: load config, init logger, ensure topics, wire Kafka consumer/producer, build candles, publish, handle shutdown.
func main() {
	configPath := flag.String("config", "", "path to marketdata_go_feed config")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("service exited with error", zap.Error(err))
	}
}

// run encapsulates service lifecycle for easier error handling in main.
// Inputs: none; Outputs: error if startup or loop fails fatally.
// Flow: setup components, process loop with commits, flush on shutdown.
func run(configPath string) error {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}

	logger, err := logging.NewLogger()
	if err != nil {
		return err
	}
	defer logger.Sync() //nolint:errcheck
	zap.ReplaceGlobals(logger)
	_ = metrics.InitAndServeMetrics("9105")
	metrics.KafkaInConnected.Set(0)
	metrics.KafkaOutConnected.Set(0)
	logger.Info("starting marketdata_go_feed service",
		zap.String("app", common.AppName),
		zap.String("version", common.AppVersion),
		zap.String("env", cfg.Env),
		zap.String("bootstrap", cfg.Kafka.BootstrapServers),
		zap.String("group", cfg.Kafka.GroupID),
		zap.String("ticks_topic", cfg.Kafka.TicksTopic),
		zap.String("stock_topic", cfg.Kafka.StockCandlesTopic),
		zap.String("index_topic", cfg.Kafka.IndexCandlesTopic),
		zap.String("timezone", cfg.Aggregation.Timezone),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		logger.Info("received signal; shutting down", zap.String("signal", s.String()))
		cancel()
	}()

	// Ensure required topics exist.
	if err := kafka.EnsureTopics(ctx, cfg.Kafka.BootstrapServers, []string{
		cfg.Kafka.TicksTopic,
		cfg.Kafka.StockCandlesTopic,
		cfg.Kafka.IndexCandlesTopic,
	}); err != nil {
		logger.Warn("failed to ensure topics", zap.Error(err))
	} else {
		logger.Info("topics ensured/available")
	}

	consumer, err := kafka.NewTickConsumer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	defer consumer.Close()
	logger.Info("kafka consumer ready")

	producer, err := kafka.NewCandleProducer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	logger.Info("kafka producer ready")

	builder, err := candle.NewCandleBuilder(cfg.Aggregation.Timezone, logger)
	if err != nil {
		return err
	}
	logger.Info("candle builder initialized")

	indexSet := make(map[string]bool)
	for _, sym := range cfg.Symbols.IndexSymbols {
		indexSet[strings.ToUpper(sym)] = true
	}

	commitTicker := time.NewTicker(time.Duration(cfg.Kafka.CommitIntervalMs) * time.Millisecond)
	defer commitTicker.Stop()
	tickGapTicker := time.NewTicker(1 * time.Second)
	defer tickGapTicker.Stop()
	var lastTickTime time.Time

	// Main processing loop.
	for {
		select {
		case <-ctx.Done():
			logger.Info("context cancelled; exiting main loop")
			goto shutdown
		case <-commitTicker.C:
			if err := consumer.Commit(); err != nil {
				logger.Warn("commit failed", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			}
		case <-tickGapTicker.C:
			if lastTickTime.IsZero() {
				metrics.TickGapSeconds.Set(0)
			} else {
				metrics.TickGapSeconds.Set(time.Since(lastTickTime).Seconds())
			}
		default:
			tick, err := consumer.Poll(ctx)
			if err != nil {
				logger.Error("consumer poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				return err
			}
			if tick == nil {
				continue
			}
			lastTickTime = tick.Time
			metrics.TicksConsumedTotal.Inc()
			metrics.LastTickUnix.Set(float64(tick.Time.Unix()))
			metrics.TickGapSeconds.Set(time.Since(tick.Time).Seconds())
			closed := builder.OnTick(*tick)
			for _, c := range closed {
				if indexSet[c.Symbol] {
					if err := producer.PublishIndexCandle(ctx, c); err != nil {
						logger.Error("publish index candle failed", zap.Error(err), zap.String("symbol", c.Symbol))
						metrics.ErrorsTotal.Inc()
					}
				} else {
					if err := producer.PublishStockCandle(ctx, c); err != nil {
						logger.Error("publish stock candle failed", zap.Error(err), zap.String("symbol", c.Symbol))
						metrics.ErrorsTotal.Inc()
					}
				}
			}
		}
	}

shutdown:
	if cfg.Aggregation.FlushOnShutdown {
		flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
		candles := builder.FlushAll()
		for _, c := range candles {
			if indexSet[c.Symbol] {
				if err := producer.PublishIndexCandle(flushCtx, c); err != nil {
					logger.Warn("shutdown publish index candle failed", zap.Error(err), zap.String("symbol", c.Symbol))
				}
			} else {
				if err := producer.PublishStockCandle(flushCtx, c); err != nil {
					logger.Warn("shutdown publish stock candle failed", zap.Error(err), zap.String("symbol", c.Symbol))
				}
			}
		}
		cancelFlush()
	}

	// Final commit and flush.
	if err := consumer.Commit(); err != nil {
		logger.Warn("final commit failed", zap.Error(err))
	}
	if err := producer.Flush(5 * time.Second); err != nil {
		logger.Warn("producer flush failed", zap.Error(err))
	}

	logger.Info("shutdown complete")
	return nil
}
