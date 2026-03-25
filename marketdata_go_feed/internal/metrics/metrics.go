package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Gauges
var (
	// KafkaInConnected is 1 when go_feed.raw consumer is healthy.
	KafkaInConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "marketdata_kafka_in_connected",
		Help: "Kafka consumer connection status for go_feed.raw (1=connected,0=disconnected)",
	})
	// KafkaOutConnected is 1 when candle producer is healthy.
	KafkaOutConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "marketdata_kafka_out_connected",
		Help: "Kafka producer connection status for candles (1=connected,0=disconnected)",
	})
	// LastTickUnix records the exchange/event time of the most recent consumed tick.
	LastTickUnix = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "marketdata_last_tick_unixtime",
		Help: "Unix timestamp of the most recent tick consumed from go_feed.raw",
	})
	// TickGapSeconds tracks how long it has been since the last consumed tick.
	TickGapSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "marketdata_tick_gap_seconds",
		Help: "Seconds since the most recent tick consumed from go_feed.raw",
	})
)

// Counters
var (
	// TicksConsumedTotal counts ticks consumed from go_feed.raw.
	TicksConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "marketdata_ticks_consumed_total",
		Help: "Total ticks consumed from Kafka topic go_feed.raw",
	})
	// CandlesEmittedTotal counts candles emitted to Kafka.
	CandlesEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "marketdata_candles_emitted_total",
		Help: "Total candles emitted to Kafka",
	})
	// ErrorsTotal counts errors encountered.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "marketdata_errors_total",
		Help: "Total errors in marketdata_go_feed service",
	})
)

// InitAndServeMetrics registers metrics and starts /metrics HTTP server on given port.
func InitAndServeMetrics(port string) error {
	prometheus.MustRegister(
		KafkaInConnected,
		KafkaOutConnected,
		LastTickUnix,
		TickGapSeconds,
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
