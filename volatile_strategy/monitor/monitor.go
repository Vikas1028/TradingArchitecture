package monitor

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"volatile_strategy/common"
)

var (
	registerOnce sync.Once
	stateMu      sync.RWMutex
	startedAt    = time.Now()
	lastTickAt   *time.Time
	lastSignals  []common.Signal
	lastLeaders  map[string][]common.Rank
	signalsTotal uint64
	windowsTotal uint64
	watchers     int

	serviceUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "volatile_strategy_service_up",
		Help: "volatile_strategy service status (1=running,0=stopped)",
	})
	kafkaMessagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "volatile_strategy_kafka_messages_total",
		Help: "Total go_ltp messages consumed",
	})
	signalsPublishedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "volatile_strategy_signals_published_total",
		Help: "Total strategy signals published",
	})
	lastTickUnix = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "volatile_strategy_last_tick_unixtime",
		Help: "Unix timestamp of the latest processed go_ltp tick",
	})
)

func Initialize() {
	registerOnce.Do(func() {
		prometheus.MustRegister(serviceUp, kafkaMessagesTotal, signalsPublishedTotal, lastTickUnix)
	})
	serviceUp.Set(1)
}

func Shutdown() { serviceUp.Set(0) }

func RecordTick(at time.Time) {
	stateMu.Lock()
	defer stateMu.Unlock()
	copyAt := at
	lastTickAt = &copyAt
	kafkaMessagesTotal.Inc()
	lastTickUnix.Set(float64(at.Unix()))
}

func RecordSignal(sig common.Signal) {
	stateMu.Lock()
	defer stateMu.Unlock()
	signalsTotal++
	lastSignals = append([]common.Signal{sig}, lastSignals...)
	if len(lastSignals) > 20 {
		lastSignals = lastSignals[:20]
	}
	signalsPublishedTotal.Inc()
}

func SetLeaders(leaders map[string][]common.Rank) {
	stateMu.Lock()
	defer stateMu.Unlock()
	cloned := make(map[string][]common.Rank, len(leaders))
	for k, v := range leaders {
		items := make([]common.Rank, len(v))
		copy(items, v)
		cloned[k] = items
	}
	lastLeaders = cloned
}

func SetWatchers(count int) {
	stateMu.Lock()
	defer stateMu.Unlock()
	watchers = count
}

func RecordWindowClosed() {
	stateMu.Lock()
	defer stateMu.Unlock()
	windowsTotal++
}

func DashboardHandler(w http.ResponseWriter, _ *http.Request) {
	stateMu.RLock()
	snapshot := common.DashboardSnapshot{
		StartedAt:      startedAt,
		UptimeSeconds:  int64(time.Since(startedAt).Seconds()),
		LastTickAt:     lastTickAt,
		SignalsTotal:   signalsTotal,
		WindowsClosed:  windowsTotal,
		ActiveWatchers: watchers,
		LastSignals:    append([]common.Signal(nil), lastSignals...),
		CurrentLeaders: lastLeaders,
	}
	stateMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshot)
}
