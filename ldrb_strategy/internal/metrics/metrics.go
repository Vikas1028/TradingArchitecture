package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	CandlesConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ldrb_strategy_candles_consumed_total",
		Help: "Total candles consumed by ldrb_strategy",
	})
	SignalsEmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ldrb_strategy_signals_emitted_total",
		Help: "Total LDRB signals emitted",
	})
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ldrb_strategy_errors_total",
		Help: "Total errors in ldrb_strategy",
	})
)

var (
	ServiceUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ldrb_strategy_up",
		Help: "Service up gauge (1=running)",
	})
)

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
