package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"vwap_strategy/internal/config"
	"vwap_strategy/internal/kafka"
	"vwap_strategy/internal/logging"
	"vwap_strategy/internal/metrics"
	"vwap_strategy/internal/strategy"
)

// main wires configuration, logging, Kafka IO, bias engine, and VWAP pullback strategy.
// Flow: load config, init logger, create consumer/producer, process candles into signals until shutdown.
func main() {
	configPath := flag.String("config", "config/vwap_strategy_config.json", "path to config file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("service exited with error", zap.Error(err))
	}
}

// run encapsulates the service lifecycle and main processing loop.
// Inputs: config file path; Outputs: error on fatal failure.
func run(configPath string) error {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}

	logger, err := logging.NewLogger(cfg.Log)
	if err != nil {
		return err
	}
	defer logger.Sync() //nolint:errcheck
	zap.ReplaceGlobals(logger)
	_ = metrics.InitAndServeMetrics("9101")
	metrics.ServiceUp.Set(1)

	logger.Info("starting vwap strategy service",
		zap.String("env", cfg.Env),
		zap.String("bootstrap", cfg.Kafka.BootstrapServers),
		zap.String("group", cfg.Kafka.GroupID),
		zap.String("stock_topic", cfg.Kafka.StockCandlesTopic),
		zap.String("index_topic", cfg.Kafka.IndexCandlesTopic),
		zap.String("signal_topic", cfg.Kafka.SignalTopic),
		zap.String("timezone", cfg.Strategy.Timezone),
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

	biasCfg := strategy.StrategyConfig{
		IndexBiasLookback:   cfg.Strategy.IndexBiasLookback,
		IndexBiasVWAPThresh: cfg.Strategy.IndexBiasVWAPThresh,
	}
	biasEngine := strategy.NewBiasEngine(biasCfg, loc, logger)

	pullbackCfg := strategy.PullbackConfig{
		Timezone:        cfg.Strategy.Timezone,
		EntryStart:      cfg.Strategy.EntryStart,
		EntryEnd:        cfg.Strategy.EntryEnd,
		TrendLookback:   cfg.Strategy.TrendLookback,
		PullbackWindow:  cfg.Strategy.PullbackWindow,
		MinBodyPct:      cfg.Strategy.MinBodyPct,
		MaxPullbackPct:  cfg.Strategy.MaxPullbackPct,
		EnableIndexBias: cfg.Strategy.EnableIndexBias,
	}
	vwapStrategy, err := strategy.NewVwapPullbackStrategy(pullbackCfg, loc, logger)
	if err != nil {
		return err
	}

	commitTicker := time.NewTicker(time.Duration(cfg.Kafka.CommitIntervalMs) * time.Millisecond)
	defer commitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("context cancelled; exiting loop")
			goto shutdown
		case <-commitTicker.C:
			if err := consumer.Commit(); err != nil {
				logger.Warn("commit failed", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			}
		default:
			polled, err := consumer.Poll(ctx)
			if err != nil {
				logger.Error("poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				continue
			}
			if polled == nil {
				continue
			}
			if polled.Topic == cfg.Kafka.IndexCandlesTopic {
				metrics.CandlesConsumedTotal.Inc()
				bias := biasEngine.OnIndexCandle(polled.Candle)
				logger.Debug("bias updated", zap.String("symbol", polled.Candle.Symbol), zap.String("bias", string(bias)))
			} else if polled.Topic == cfg.Kafka.StockCandlesTopic {
				bias := strategy.BiasNone
				if cfg.Strategy.EnableIndexBias {
					bias = biasEngine.CurrentBias()
				}
				metrics.CandlesConsumedTotal.Inc()
				sig := vwapStrategy.OnStockCandle(polled.Candle, bias)
				if sig != nil {
					if err := producer.PublishSignal(ctx, *sig); err != nil {
						logger.Error("failed to publish signal", zap.Error(err), zap.String("symbol", sig.Symbol))
						metrics.ErrorsTotal.Inc()
					} else {
						metrics.SignalsEmittedTotal.Inc()
						logger.Info("signal emitted", zap.String("symbol", sig.Symbol), zap.String("reason", sig.Reason))
					}
				}
			} else {
				logger.Debug("ignored topic", zap.String("topic", polled.Topic))
			}
		}
	}

shutdown:
	if err := producer.Flush(5 * time.Second); err != nil {
		logger.Warn("producer flush failed", zap.Error(err))
	}
	logger.Info("shutdown complete")
	return nil
}
