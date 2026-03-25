package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ServiceUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "market_price_up",
		Help: "market_price service status (1=running,0=stopped)",
	})
	KafkaMessagesConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "market_price_messages_consumed_total",
		Help: "Total broker messages consumed by source feed",
	}, []string{"source"})
	LastTopicTickUnix = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "market_price_last_topic_tick_unixtime",
		Help: "Unix timestamp of the latest price update per source feed",
	}, []string{"source"})
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "market_price_errors_total",
		Help: "Total market_price errors",
	})
)

func InitAndServe(bindAddress, metricsPath string) error {
	prometheus.MustRegister(ServiceUp, KafkaMessagesConsumed, LastTopicTickUnix, ErrorsTotal)
	mux := http.NewServeMux()
	mux.Handle(metricsPath, promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(bindAddress, mux)
	}()
	ServiceUp.Set(1)
	return nil
}
