package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"paper_engine/internal/config"
	"paper_engine/internal/engine"
	"paper_engine/internal/kafka"
	"paper_engine/internal/logging"
	"paper_engine/internal/metrics"
)

// main wires configuration, logging, Kafka IO, and the paper engine, then runs the main loop.
// Flow: load config, init logger, build consumers/producers, process signals and candles until shutdown.
func main() {
	configPath := flag.String("config", "config/paper_engine_config.json", "path to config file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("service exited with error", zap.Error(err))
	}
}

// run encapsulates service lifecycle and main loop.
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
	_ = metrics.InitAndServeMetrics("9102")
	metrics.TradingHalted.Set(0)

	logger.Info("starting paper engine",
		zap.String("env", cfg.Env),
		zap.String("bootstrap", cfg.Kafka.BootstrapServers),
		zap.String("group", cfg.Kafka.GroupID),
		zap.String("signals_group", cfg.Kafka.SignalsGroupID),
		zap.String("candles_group", cfg.Kafka.CandlesGroupID),
		zap.String("signals_topic", cfg.Kafka.SignalsTopic),
		zap.String("candles_topic", cfg.Kafka.CandlesTopic),
		zap.String("trades_topic", cfg.Kafka.TradesTopic),
		zap.String("pnl_topic", cfg.Kafka.PnlTopic),
		zap.String("timezone", cfg.Trading.Timezone),
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

	signalsConsumer, err := kafka.NewSignalsConsumer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	defer signalsConsumer.Close()

	candlesConsumer, err := kafka.NewCandlesConsumer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	defer candlesConsumer.Close()

	producer, err := kafka.NewEngineProducer(cfg.Kafka, logger)
	if err != nil {
		return err
	}

	loc, err := time.LoadLocation(cfg.Trading.Timezone)
	if err != nil {
		return err
	}
	candleMinuteForSignal := func(sigTime time.Time) time.Time {
		return sigTime.In(loc).Truncate(time.Minute)
	}
	state := engine.NewEngineState(cfg.Risk, cfg.Trading, loc, logger)
	core, err := engine.NewEngine(state, logger)
	if err != nil {
		return err
	}

	commitTicker := time.NewTicker(time.Duration(cfg.Kafka.CommitIntervalMs) * time.Millisecond)
	defer commitTicker.Stop()
	pnlTicker := time.NewTicker(time.Duration(cfg.Trading.MtmSnapshotIntervalSec) * time.Second)
	defer pnlTicker.Stop()
	pendingExpiryCheckTicker := time.NewTicker(1 * time.Second)
	defer pendingExpiryCheckTicker.Stop()

	const pollTimeout = 200 * time.Millisecond
	pendingSignalMaxAge := time.Duration(cfg.Trading.PendingSignalMaxAgeSec) * time.Second

	for {
		select {
		case <-ctx.Done():
			logger.Info("context cancelled; exiting loop")
			goto shutdown
		case <-commitTicker.C:
			_ = signalsConsumer.Commit()
			_ = candlesConsumer.Commit()
		case <-pnlTicker.C:
			snap := core.BuildPnlSnapshot(time.Now().In(loc))
			if err := producer.PublishPnlSnapshot(ctx, snap); err != nil {
				logger.Warn("failed to publish pnl snapshot", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			} else {
				metrics.PnlSnapshotsTotal.Inc()
			}
		case <-pendingExpiryCheckTicker.C:
			expired := core.DrainExpiredPendingSignals(time.Now().In(loc), pendingSignalMaxAge)
			for _, pending := range expired {
				logger.Warn("expired pending signal without pricing candle",
					zap.String("symbol", pending.Signal.Symbol),
					zap.Time("signal_time", pending.Signal.Time.In(loc)),
					zap.Time("queued_at", pending.QueuedAt.In(loc)),
					zap.Int("max_age_sec", cfg.Trading.PendingSignalMaxAgeSec),
				)
			}
		default:
			sigPollCtx, sigPollCancel := context.WithTimeout(ctx, pollTimeout)
			sig, err := signalsConsumer.Poll(sigPollCtx)
			sigPollCancel()
			if err != nil {
				logger.Error("signals poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			} else if sig != nil {
				metrics.SignalsConsumedTotal.Inc()
				latest := core.GetLatestCandleForSymbol(sig.Symbol)
				if latest == nil || latest.Time.In(loc).Before(candleMinuteForSignal(sig.Time)) {
					core.QueueSignal(*sig, time.Now().In(loc))
					logger.Debug("queued signal until pricing candle is available",
						zap.String("symbol", sig.Symbol),
						zap.Time("signal_time", sig.Time.In(loc)),
					)
				} else if trade, err := core.OnSignal(*sig, latest); err != nil {
					logger.Error("OnSignal error", zap.Error(err), zap.String("symbol", sig.Symbol))
					metrics.ErrorsTotal.Inc()
				} else if trade != nil {
					if err := producer.PublishTrade(ctx, *trade); err != nil {
						metrics.ErrorsTotal.Inc()
					} else {
						metrics.TradesEmittedTotal.Inc()
					}
				}
			}

			candlePollCtx, candlePollCancel := context.WithTimeout(ctx, pollTimeout)
			candle, err := candlesConsumer.Poll(candlePollCtx)
			candlePollCancel()
			if err != nil {
				logger.Error("candles poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			} else if candle != nil {
				core.UpdateLatestCandle(*candle)
				trades, err := core.OnCandle(*candle)
				if err != nil {
					logger.Error("OnCandle error", zap.Error(err), zap.String("symbol", candle.Symbol))
					metrics.ErrorsTotal.Inc()
				}
				for _, tr := range trades {
					if err := producer.PublishTrade(ctx, tr); err != nil {
						metrics.ErrorsTotal.Inc()
					} else {
						metrics.TradesEmittedTotal.Inc()
					}
				}
				if pending := core.ConsumePendingSignal(candle.Symbol); pending != nil {
					latest := core.GetLatestCandleForSymbol(pending.Symbol)
					if trade, err := core.OnSignal(*pending, latest); err != nil {
						logger.Error("pending OnSignal error", zap.Error(err), zap.String("symbol", pending.Symbol))
						metrics.ErrorsTotal.Inc()
					} else if trade != nil {
						if err := producer.PublishTrade(ctx, *trade); err != nil {
							metrics.ErrorsTotal.Inc()
						} else {
							metrics.TradesEmittedTotal.Inc()
							logger.Info("executed queued signal", zap.String("symbol", trade.Symbol))
						}
					}
				}
			}
		}
	}

shutdown:
	snap := core.BuildPnlSnapshot(time.Now().In(loc))
	publishCtx, cancelPublish := context.WithTimeout(context.Background(), 5*time.Second)
	if err := producer.PublishPnlSnapshot(publishCtx, snap); err != nil {
		logger.Warn("failed to publish final pnl snapshot", zap.Error(err))
		metrics.ErrorsTotal.Inc()
	} else {
		metrics.PnlSnapshotsTotal.Inc()
	}
	cancelPublish()
	if err := producer.Flush(5 * time.Second); err != nil {
		logger.Warn("producer flush failed", zap.Error(err))
	}
	logger.Info("shutdown complete")
	return nil
}
