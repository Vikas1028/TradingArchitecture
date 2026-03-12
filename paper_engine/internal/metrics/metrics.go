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
		Name: "paper_engine_signals_consumed_total",
		Help: "Total strategy signals consumed",
	})
	// TradesEmittedTotal counts trades emitted to trades.paper.
	TradesEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "paper_engine_trades_emitted_total",
		Help: "Total trade events emitted",
	})
	// PnlSnapshotsTotal counts PnL snapshots emitted.
	PnlSnapshotsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "paper_engine_pnl_snapshots_total",
		Help: "Total PnL snapshots emitted",
	})
	// ErrorsTotal counts errors encountered.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "paper_engine_errors_total",
		Help: "Total errors in paper_engine",
	})
)

// Gauges
var (
	// TradingHalted is 1 when daily loss limit hit and trading halted.
	TradingHalted = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "paper_engine_trading_halted",
		Help: "Trading halted flag (1=halted,0=active)",
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
	)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(":"+port, nil)
	}()
	return nil
}
