package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"ldrb_strategy/common"
	"ldrb_strategy/internal/config"
	"ldrb_strategy/internal/kafka"
	"ldrb_strategy/internal/logging"
	"ldrb_strategy/internal/metrics"
	"ldrb_strategy/internal/strategy"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("service exited with error", zap.Error(err))
	}
}

// run loads dependencies, consumes stock/index candles, and publishes LDRB signals.
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

	logger.Info("starting ldrb strategy service",
		zap.String("app", common.AppName),
		zap.String("version", common.AppVersion),
		zap.String("env", cfg.Env),
		zap.String("bootstrap", cfg.Kafka.BootstrapServers),
		zap.String("group", cfg.Kafka.GroupID),
		zap.String("stock_topic", cfg.Kafka.StockCandlesTopic),
		zap.String("index_topic", cfg.Kafka.IndexCandlesTopic),
		zap.String("signal_topic", cfg.Kafka.SignalTopic),
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
	engine, err := strategy.NewLDRBStrategy(strategy.EngineConfig{
		Timezone:                 cfg.Strategy.Timezone,
		SessionStart:             cfg.Strategy.SessionStart,
		EntryStart:               cfg.Strategy.EntryStart,
		EntryEnd:                 cfg.Strategy.EntryEnd,
		SameDayExitCutoff:        cfg.Strategy.SameDayExitCutoff,
		ExitMonitoringEnd:        cfg.Strategy.ExitMonitoringEnd,
		Capital:                  cfg.Strategy.Capital,
		RiskPerTradePct:          cfg.Strategy.RiskPerTradePct,
		MaxTradesPerDay:          cfg.Strategy.MaxTradesPerDay,
		MaxOpenOvernight:         cfg.Strategy.MaxOpenOvernight,
		MaxOvernightRiskPct:      cfg.Strategy.MaxOvernightRiskPct,
		CapitalUsageCapPct:       cfg.Strategy.CapitalUsageCapPct,
		MinPrice:                 cfg.Strategy.MinPrice,
		MaxIntradayMovePct:       cfg.Strategy.MaxIntradayMovePct,
		BodyRatioMin:             cfg.Strategy.BodyRatioMin,
		VolumeRatioMin:           cfg.Strategy.VolumeRatioMin,
		RVOLMin:                  cfg.Strategy.RVOLMin,
		BreakoutCushionPct:       cfg.Strategy.BreakoutCushionPct,
		SecondTryExtraPct:        cfg.Strategy.SecondTryExtraPct,
		SlippageBufferPct:        cfg.Strategy.SlippageBufferPct,
		MinStopDistancePct:       cfg.Strategy.MinStopDistancePct,
		MaxStopDistancePct:       cfg.Strategy.MaxStopDistancePct,
		GapUpPct:                 cfg.Strategy.GapUpPct,
		GapDownPct:               cfg.Strategy.GapDownPct,
		MarketDrawdownLimitPct:   cfg.Strategy.MarketDrawdownLimitPct,
		LiquidityTopN:            cfg.Strategy.LiquidityTopN,
		MinPartialFillPct:        cfg.Strategy.MinPartialFillPct,
		RequireSpreadCheck:       cfg.Strategy.RequireSpreadCheck,
		MaxSpreadPct:             cfg.Strategy.MaxSpreadPct,
		SwingStart:               cfg.Strategy.SwingStart,
		IndexSymbol:              cfg.Strategy.IndexSymbol,
		EnableMarketFilter:       cfg.Strategy.EnableMarketFilter,
		EnableEventFilter:        cfg.Strategy.EnableEventFilter,
		EnableSameDayFailureExit: cfg.Strategy.EnableSameDayFailureExit,
		EnableSafeMode:           cfg.Dependencies.EnableSafeMode,
		AllowRVOLFallback:        cfg.Dependencies.AllowRVOLFallback,
		SkipAllIfEventsMissing:   cfg.Dependencies.SkipAllIfEventsMissing,
		EventsPath:               cfg.Dependencies.EventsPath,
		VolumeProfilePath:        cfg.Dependencies.VolumeProfilePath,
		TurnoverRankingPath:      cfg.Dependencies.TurnoverRankingPath,
	}, loc, logger)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			goto shutdown
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
			if err := consumer.Commit(); err != nil {
				logger.Warn("commit failed", zap.Error(err))
				metrics.ErrorsTotal.Inc()
			}

			metrics.CandlesConsumedTotal.Inc()
			var sig *strategy.StrategySignal
			if polled.Topic == "index" {
				engine.OnIndexCandle(polled.Candle)
			} else if polled.Topic == "stock" {
				sig = engine.OnStockCandle(polled.Candle)
			}
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
