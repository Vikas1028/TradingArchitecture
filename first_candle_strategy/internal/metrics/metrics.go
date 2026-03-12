package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Counters track throughput and failures for the first-candle strategy service.
var (
	// CandlesConsumedTotal counts stock candles consumed from Kafka.
	CandlesConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "first_candle_strategy_candles_consumed_total",
		Help: "Total stock candles consumed",
	})
	// SignalsEmittedTotal counts signals published to Kafka.
	SignalsEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "first_candle_strategy_signals_emitted_total",
		Help: "Total first-candle signals emitted",
	})
	// ErrorsTotal counts errors in the strategy service.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "first_candle_strategy_errors_total",
		Help: "Total errors in first_candle_strategy",
	})
)

// Gauges expose liveness for the service.
var (
	// ServiceUp is 1 when the service is running.
	ServiceUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "first_candle_strategy_up",
		Help: "Service up gauge (1=running)",
	})
)

// InitAndServeMetrics registers metrics and starts a /metrics HTTP endpoint on the given port.
// Inputs: port string without protocol.
// Outputs: error is always nil in the current implementation.
func InitAndServeMetrics(port string) error {
	prometheus.MustRegister(
		CandlesConsumedTotal,
		SignalsEmittedTotal,
		ErrorsTotal,
		ServiceUp,
	)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(":"+port, nil)
	}()
	return nil
}
