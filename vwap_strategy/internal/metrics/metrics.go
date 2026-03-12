package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Counters
var (
	// CandlesConsumedTotal counts candles consumed from Kafka.
	CandlesConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vwap_strategy_candles_consumed_total",
		Help: "Total candles consumed",
	})
	// SignalsEmittedTotal counts signals published to Kafka.
	SignalsEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vwap_strategy_signals_emitted_total",
		Help: "Total signals emitted to the configured strategy topic",
	})
	// ErrorsTotal counts errors in the strategy service.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vwap_strategy_errors_total",
		Help: "Total errors in vwap_strategy",
	})
)

// Gauges
var (
	// ServiceUp is 1 when the service is running.
	ServiceUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vwap_strategy_up",
		Help: "Service up gauge (1=running)",
	})
)

// InitAndServeMetrics registers metrics and starts /metrics HTTP server on given port.
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
