package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"first_candle_strategy/common"
	"first_candle_strategy/internal/config"
	"first_candle_strategy/internal/kafka"
	"first_candle_strategy/internal/logging"
	"first_candle_strategy/internal/metrics"
	"first_candle_strategy/internal/strategy"
)

// main wires configuration, logging, Kafka IO, and the first-candle strategy loop.
// Flow: load config, init logger, create consumer/producer, process 1m candles into signals until shutdown.
func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("service exited with error", zap.Error(err))
	}
}

// run encapsulates service lifecycle and the main processing loop.
// Inputs: config file path.
// Outputs: error on fatal startup or runtime failure.
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
	_ = metrics.InitAndServeMetrics(common.DefaultMetricsPort)
	metrics.ServiceUp.Set(1)

	logger.Info("starting first candle strategy service",
		zap.String("app", common.AppName),
		zap.String("version", common.AppVersion),
		zap.String("env", cfg.Env),
		zap.String("bootstrap", cfg.Kafka.BootstrapServers),
		zap.String("group", cfg.Kafka.GroupID),
		zap.String("candles_topic", cfg.Kafka.CandlesTopic),
		zap.String("signal_topic", cfg.Kafka.SignalTopic),
		zap.String("timezone", cfg.Strategy.Timezone),
		zap.Int("opening_candle_slot", cfg.Strategy.OpeningCandleSlot),
		zap.Float64("move_threshold_pct", cfg.Strategy.MoveThresholdPct),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		logger.Info("received signal; shutting down", zap.String("signal", s.String()))
		cancel()
	}()

	consumer, err := kafka.NewCandleConsumer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	defer consumer.Close()

	producer, err := kafka.NewSignalProducer(cfg.Kafka, logger)
	if err != nil {
		return err
	}

	loc, err := time.LoadLocation(cfg.Strategy.Timezone)
	if err != nil {
		return err
	}

	engine, err := strategy.NewFirstCandleStrategy(strategy.EngineConfig{
		SessionStart:      cfg.Strategy.SessionStart,
		OpeningCandleSlot: cfg.Strategy.OpeningCandleSlot,
		MoveThresholdPct:  cfg.Strategy.MoveThresholdPct,
	}, loc, logger)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("context cancelled; exiting loop")
			goto shutdown
		default:
			candle, err := consumer.Poll(ctx)
			if err != nil {
				logger.Error("poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				continue
			}
			if candle == nil {
				continue
			}
			if err := consumer.Commit(); err != nil {
				logger.Warn("commit failed", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			}
			metrics.CandlesConsumedTotal.Inc()
			sig := engine.OnCandle(*candle)
			if sig == nil {
				continue
			}
			if err := producer.PublishSignal(ctx, *sig); err != nil {
				logger.Error("failed to publish signal", zap.Error(err), zap.String("symbol", sig.Symbol))
				metrics.ErrorsTotal.Inc()
				continue
			}
			metrics.SignalsEmittedTotal.Inc()
			logger.Info("signal emitted",
				zap.String("symbol", sig.Symbol),
				zap.String("side", string(sig.Side)),
				zap.String("reason", sig.Reason),
			)
		}
	}

shutdown:
	if err := producer.Flush(5 * time.Second); err != nil {
		logger.Warn("producer flush failed", zap.Error(err))
	}
	logger.Info("shutdown complete")
	return nil
}
