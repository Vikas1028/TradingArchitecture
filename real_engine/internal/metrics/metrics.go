package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Counters
var (
	// SignalsConsumedTotal counts signals consumed from the configured strategy topic.
	SignalsConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "real_engine_signals_consumed_total",
		Help: "Total strategy signals consumed",
	})
	// TradesEmittedTotal counts trades emitted to trades.paper.
	TradesEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "real_engine_trades_emitted_total",
		Help: "Total trade events emitted",
	})
	// PnlSnapshotsTotal counts PnL snapshots emitted.
	PnlSnapshotsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "real_engine_pnl_snapshots_total",
		Help: "Total PnL snapshots emitted",
	})
	// ErrorsTotal counts errors encountered.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "real_engine_errors_total",
		Help: "Total errors in real_engine",
	})
)

// Gauges
var (
	// TradingHalted is 1 when daily loss limit hit and trading halted.
	TradingHalted = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_trading_halted",
		Help: "Trading halted flag (1=halted,0=active)",
	})
	// RealizedPnl tracks the current realized PnL for the trading day.
	RealizedPnl = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_realized_pnl",
		Help: "Current realized PnL for the trading day",
	})
	// UnrealizedPnl tracks the current mark-to-market unrealized PnL.
	UnrealizedPnl = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_unrealized_pnl",
		Help: "Current unrealized PnL for open positions",
	})
	// OpenPositions tracks how many paper positions are currently open.
	OpenPositions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_open_positions",
		Help: "Number of currently open paper positions",
	})
	// PendingSignals tracks strategy signals waiting for pricing candles.
	PendingSignals = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_pending_signals",
		Help: "Number of queued strategy signals waiting for pricing candles",
	})
	// TradesToday tracks how many trades were opened today.
	TradesToday = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_trades_today",
		Help: "Number of trades opened during the current trading day",
	})
	// MaxDrawdown tracks the maximum realized drawdown reached during the day.
	MaxDrawdown = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "real_engine_max_drawdown",
		Help: "Maximum realized drawdown reached during the current trading day",
	})
)

// InitAndServeMetrics registers metrics and starts /metrics HTTP server on given port.
func InitAndServeMetrics(port string) error {
	prometheus.MustRegister(
		SignalsConsumedTotal,
		TradesEmittedTotal,
		PnlSnapshotsTotal,
		ErrorsTotal,
		TradingHalted,
		RealizedPnl,
		UnrealizedPnl,
		OpenPositions,
		PendingSignals,
		TradesToday,
		MaxDrawdown,
	)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(":"+port, nil)
	}()
	return nil
}
