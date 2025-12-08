package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Gauges
var (
	// KafkaInConnected is 1 when ticks.raw consumer is healthy.
	KafkaInConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "marketdata_kafka_in_connected",
		Help: "Kafka consumer connection status for ticks.raw (1=connected,0=disconnected)",
	})
	// KafkaOutConnected is 1 when candle producer is healthy.
	KafkaOutConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "marketdata_kafka_out_connected",
		Help: "Kafka producer connection status for candles (1=connected,0=disconnected)",
	})
)

// Counters
var (
	// TicksConsumedTotal counts ticks consumed from ticks.raw.
	TicksConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "marketdata_ticks_consumed_total",
		Help: "Total ticks consumed from Kafka topic ticks.raw",
	})
	// CandlesEmittedTotal counts candles emitted to Kafka.
	CandlesEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "marketdata_candles_emitted_total",
		Help: "Total candles emitted to Kafka",
	})
	// ErrorsTotal counts errors encountered.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "marketdata_errors_total",
		Help: "Total errors in marketdata service",
	})
)

// InitAndServeMetrics registers metrics and starts /metrics HTTP server on given port.
func InitAndServeMetrics(port string) error {
	prometheus.MustRegister(
		KafkaInConnected,
		KafkaOutConnected,
		TicksConsumedTotal,
		CandlesEmittedTotal,
		ErrorsTotal,
	)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(":"+port, nil)
	}()
	return nil
}
