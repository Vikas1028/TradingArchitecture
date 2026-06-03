package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"real_engine/common"
	"real_engine/internal/brokersync"
	"real_engine/internal/config"
	"real_engine/internal/dashboard"
	"real_engine/internal/engine"
	"real_engine/internal/kafka"
	"real_engine/internal/logging"
	"real_engine/internal/metrics"
)

// main wires configuration, logging, broker IO, and the real engine, then runs the main loop.
// Flow: load config, init logger, build consumers/producers, process signals and candles until shutdown.
func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("service exited with error", zap.Error(err))
	}
}

// run encapsulates service lifecycle and main loop.
// Inputs: config file path; Outputs: error on fatal failure.
func run(configPath string) error {
	startedAt := time.Now()
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
	dashboardStore := dashboard.NewStore(startedAt)
	http.HandleFunc("/dashboard", dashboardStore.Handler)
	_ = metrics.InitAndServeMetrics(common.DefaultMetricsPort)
	metrics.TradingHalted.Set(0)
	metrics.RealizedPnl.Set(0)
	metrics.UnrealizedPnl.Set(0)
	metrics.OpenPositions.Set(0)
	metrics.PendingSignals.Set(0)
	metrics.TradesToday.Set(0)
	metrics.MaxDrawdown.Set(0)

	logger.Info("starting real engine",
		zap.String("app", common.AppName),
		zap.String("version", common.AppVersion),
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

	var brokerSync *brokersync.Sync
	if cfg.BrokerSync.Enabled {
		brokerSync, err = brokersync.Start(ctx, cfg, logger)
		if err != nil {
			return err
		}
		defer brokerSync.Close()
	}

	candlesConsumer, err := kafka.NewCandlesConsumer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	defer candlesConsumer.Close()

	ticksConsumer, err := kafka.NewTicksConsumer(cfg.Kafka, logger)
	if err != nil {
		return err
	}
	defer ticksConsumer.Close()

	producer, err := kafka.NewEngineProducer(cfg.Kafka, logger)
	if err != nil {
		return err
	}

	loc, err := time.LoadLocation(cfg.Trading.Timezone)
	if err != nil {
		return err
	}
	state := engine.NewEngineState(cfg.Risk, cfg.Trading, loc, logger)
	core, err := engine.NewEngine(state, logger)
	if err != nil {
		return err
	}
	dayStart := time.Now().In(loc)
	dayStart = time.Date(dayStart.Year(), dayStart.Month(), dayStart.Day(), 0, 0, 0, 0, loc)
	if history, err := kafka.LoadTradeHistory(cfg.Kafka, logger, dayStart); err != nil {
		logger.Warn("failed to load trade history for recovery", zap.Error(err))
	} else {
		core.RestoreTradeHistory(history, time.Now().In(loc))
	}
	for _, symbol := range core.OpenSymbols() {
		if tick, ok := ticksConsumer.LookupLatestTick(symbol); ok && tick != nil {
			if _, err := core.OnTick(*tick); err != nil {
				logger.Warn("failed to restore latest tick for open position", zap.Error(err), zap.String("symbol", symbol))
			}
		}
	}
	counters := dashboardCounters{}
	connections := map[string]bool{
		"signals_kafka": true,
		"candles_kafka": true,
		"ticks_kafka":   true,
		"trades_kafka":  true,
	}
	refreshDashboard(dashboardStore, core, counters, connections)

	pnlTicker := time.NewTicker(time.Duration(cfg.Trading.MtmSnapshotIntervalSec) * time.Second)
	defer pnlTicker.Stop()

	const pollTimeout = 200 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			logger.Info("context cancelled; exiting loop")
			goto shutdown
		case <-pnlTicker.C:
			snap := core.BuildPnlSnapshot(time.Now().In(loc))
			if err := producer.PublishPnlSnapshot(ctx, snap); err != nil {
				logger.Warn("failed to publish pnl snapshot", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				counters.ErrorsTotal++
			} else {
				metrics.PnlSnapshotsTotal.Inc()
				counters.PnlSnapshotsTotal++
			}
			refreshDashboard(dashboardStore, core, counters, connections)
		default:
			tickPollCtx, tickPollCancel := context.WithTimeout(ctx, pollTimeout)
			tick, err := ticksConsumer.Poll(tickPollCtx)
			tickPollCancel()
			if err != nil {
				logger.Error("ticks poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				counters.ErrorsTotal++
			} else if tick != nil {
				trade, err := core.OnTick(*tick)
				if err != nil {
					logger.Error("OnTick error", zap.Error(err), zap.String("symbol", tick.Symbol))
					metrics.ErrorsTotal.Inc()
					counters.ErrorsTotal++
				} else if trade != nil {
					if err := producer.PublishTrade(ctx, *trade); err != nil {
						metrics.ErrorsTotal.Inc()
						counters.ErrorsTotal++
					} else {
						metrics.TradesEmittedTotal.Inc()
						counters.TradesEmittedTotal++
					}
				}
				counters.TicksConsumedTotal++
				_ = ticksConsumer.Commit()
			}

			sigPollCtx, sigPollCancel := context.WithTimeout(ctx, pollTimeout)
			sig, err := signalsConsumer.Poll(sigPollCtx)
			sigPollCancel()
			if err != nil {
				logger.Error("signals poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				counters.ErrorsTotal++
			} else if sig != nil {
				normalized := normalizeStrategySignal(cfg, sig, time.Now().In(loc), loc)
				if normalized == nil {
					_ = signalsConsumer.Commit()
					continue
				}
				metrics.SignalsConsumedTotal.Inc()
				counters.SignalsConsumedTotal++
				entryPrice, ok := core.GetLatestPriceForSymbol(normalized.Symbol)
				if !ok {
					if latest := core.GetLatestCandleForSymbol(normalized.Symbol); latest != nil {
						entryPrice = latest.Close
						ok = entryPrice > 0
					}
				}
				if !ok {
					if lookedUp, found := ticksConsumer.LookupLatestPrice(normalized.Symbol); found {
						entryPrice = lookedUp
						ok = true
					}
				}
				if trade, err := core.OnSignal(*normalized, entryPrice); err != nil {
					if err.Error() == "invalid entry price" {
						core.QueueSignal(*normalized, time.Now().In(loc))
						logger.Info("queued signal until first live tick is available", zap.String("symbol", normalized.Symbol))
					} else {
						logger.Error("OnSignal error", zap.Error(err), zap.String("symbol", normalized.Symbol))
						metrics.ErrorsTotal.Inc()
						counters.ErrorsTotal++
					}
				} else if trade != nil {
					if err := producer.PublishTrade(ctx, *trade); err != nil {
						metrics.ErrorsTotal.Inc()
						counters.ErrorsTotal++
					} else {
						metrics.TradesEmittedTotal.Inc()
						counters.TradesEmittedTotal++
					}
				}
				_ = signalsConsumer.Commit()
			}

			candlePollCtx, candlePollCancel := context.WithTimeout(ctx, pollTimeout)
			candle, err := candlesConsumer.Poll(candlePollCtx)
			candlePollCancel()
			if err != nil {
				logger.Error("candles poll error", zap.Error(err))
				metrics.ErrorsTotal.Inc()
				counters.ErrorsTotal++
			} else if candle != nil {
				core.UpdateLatestCandle(*candle)
				trades, err := core.OnCandle(*candle)
				if err != nil {
					logger.Error("OnCandle error", zap.Error(err), zap.String("symbol", candle.Symbol))
					metrics.ErrorsTotal.Inc()
					counters.ErrorsTotal++
				}
				for _, tr := range trades {
					if err := producer.PublishTrade(ctx, tr); err != nil {
						metrics.ErrorsTotal.Inc()
						counters.ErrorsTotal++
					} else {
						metrics.TradesEmittedTotal.Inc()
						counters.TradesEmittedTotal++
					}
				}
				_ = candlesConsumer.Commit()
			}
			refreshDashboard(dashboardStore, core, counters, connections)
		}
	}

shutdown:
	snap := core.BuildPnlSnapshot(time.Now().In(loc))
	publishCtx, cancelPublish := context.WithTimeout(context.Background(), 5*time.Second)
	if err := producer.PublishPnlSnapshot(publishCtx, snap); err != nil {
		logger.Warn("failed to publish final pnl snapshot", zap.Error(err))
		metrics.ErrorsTotal.Inc()
		counters.ErrorsTotal++
	} else {
		metrics.PnlSnapshotsTotal.Inc()
		counters.PnlSnapshotsTotal++
	}
	cancelPublish()
	if err := producer.Flush(5 * time.Second); err != nil {
		logger.Warn("producer flush failed", zap.Error(err))
	}
	refreshDashboard(dashboardStore, core, counters, connections)
	logger.Info("shutdown complete")
	return nil
}

type dashboardCounters struct {
	SignalsConsumedTotal uint64
	TradesEmittedTotal   uint64
	PnlSnapshotsTotal    uint64
	ErrorsTotal          uint64
	TicksConsumedTotal   uint64
}

// refreshDashboard snapshots the current engine state into the HTTP dashboard model.
func refreshDashboard(store *dashboard.Store, core *engine.Engine, counters dashboardCounters, connections map[string]bool) {
	pnl := core.BuildPnlSnapshot(time.Now())
	store.Update(common.DashboardSnapshot{
		ConnectionStatus: cloneConnections(connections),
		QueueDepth:       map[string]int{},
		Reconnects:       map[string]uint64{},
		Metrics: map[string]float64{
			"real_engine_signals_consumed_total": float64(counters.SignalsConsumedTotal),
			"real_engine_trades_emitted_total":   float64(counters.TradesEmittedTotal),
			"real_engine_pnl_snapshots_total":    float64(counters.PnlSnapshotsTotal),
			"real_engine_errors_total":           float64(counters.ErrorsTotal),
			"real_engine_open_positions":         float64(pnl.OpenPositions),
			"real_engine_pending_signals":        0,
			"real_engine_trades_today":           float64(pnl.TradesToday),
			"real_engine_realized_pnl":           pnl.RealizedPnl,
			"real_engine_unrealized_pnl":         pnl.UnrealizedPnl,
			"real_engine_max_drawdown":           pnl.MaxDrawdown,
			"real_engine_trading_halted":         boolFloat(pnl.TradingHalted),
			"real_engine_ticks_consumed_total":   float64(counters.TicksConsumedTotal),
		},
		RunningTrades: buildRunningTradeSnapshots(core.BuildRunningTrades()),
	})
}

// buildRunningTradeSnapshots converts internal engine rows to the dashboard schema.
func buildRunningTradeSnapshots(rows []engine.RunningTrade) []common.RunningTradeSnapshot {
	out := make([]common.RunningTradeSnapshot, 0, len(rows))
	for _, row := range rows {
		lastPrice := row.LastPrice
		unrealizedPnl := row.UnrealizedPnl
		if lastPrice <= 0 {
			lastPrice = row.EntryPrice
			unrealizedPnl = 0
		}
		investedAmount := row.EntryPrice * float64(row.Quantity)
		unrealizedPnlPct := 0.0
		if investedAmount > 0 {
			unrealizedPnlPct = (unrealizedPnl / investedAmount) * 100
		}
		out = append(out, common.RunningTradeSnapshot{
			Symbol:           row.Symbol,
			Strategy:         row.Strategy,
			Side:             row.Side,
			Quantity:         row.Quantity,
			EntryPrice:       row.EntryPrice,
			LastPrice:        lastPrice,
			InvestedAmount:   investedAmount,
			UnrealizedPnl:    unrealizedPnl,
			UnrealizedPnlPct: unrealizedPnlPct,
			EntryTime:        row.EntryTime,
			LastTickTime:     row.LastTickTime,
		})
	}
	return out
}

func cloneConnections(values map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
