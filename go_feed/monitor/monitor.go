package monitor

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/process"
	"go_feed/common"
)

var (
	registerOnce sync.Once
	state        = runtimeState{
		startedAt:      time.Now(),
		perSymbolTicks: make(map[string]uint64),
		lastMinute:     make(map[int64]uint64),
	}
	processInfo, _ = process.NewProcess(int32(os.Getpid()))

	serviceUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_service_up",
		Help: "go_feed service status (1=running,0=stopped)",
	})
	websocketConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_websocket_connected",
		Help: "Websocket connection status (1=connected,0=disconnected)",
	})
	kafkaConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_kafka_connected",
		Help: "Kafka producer connection status (1=connected,0=disconnected)",
	})
	postgresConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_postgres_connected",
		Help: "PostgreSQL connection status (1=connected,0=disconnected)",
	})
	clientLoggedIn = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_client_logged_in",
		Help: "Client login status (1=logged in,0=logged out)",
	})
	lastTickUnix = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_last_tick_unixtime",
		Help: "Unix timestamp of the most recent parsed tick",
	})
	kafkaQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_kafka_queue_depth",
		Help: "Current number of pending Kafka messages",
	})
	postgresQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "go_feed_postgres_queue_depth",
		Help: "Current number of pending PostgreSQL inserts",
	})
	ticksReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "go_feed_ticks_received_total",
		Help: "Total ticks received from the websocket",
	})
	kafkaInsertedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "go_feed_kafka_inserted_total",
		Help: "Total tick messages inserted into Kafka",
	})
	postgresInsertedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "go_feed_postgres_inserted_total",
		Help: "Total full packets inserted into PostgreSQL",
	})
	reconnectsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "go_feed_reconnects_total",
		Help: "Total reconnect attempts by component",
	}, []string{"component"})
	errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "go_feed_errors_total",
		Help: "Total errors by component",
	}, []string{"component"})
	ticksBySymbol = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "go_feed_ticks_by_symbol_total",
		Help: "Total ticks processed per symbol",
	}, []string{"symbol"})
)

type runtimeState struct {
	mu                    sync.RWMutex
	startedAt             time.Time
	websocketUp           bool
	kafkaUp               bool
	postgresUp            bool
	clientUp              bool
	subscriptions         []common.TokenInfo
	lastTickAt            time.Time
	totalTicks            uint64
	totalKafkaInserted    uint64
	totalPostgresInserted uint64
	kafkaReconnects       uint64
	postgresReconnects    uint64
	websocketReconnects   uint64
	clientRelogins        uint64
	kafkaQueue            int
	postgresQueue         int
	perSymbolTicks        map[string]uint64
	lastMinute            map[int64]uint64
}

type symbolCount struct {
	Symbol string `json:"symbol"`
	Count  uint64 `json:"count"`
}

type dashboardSnapshot struct {
	StartedAt        time.Time          `json:"started_at"`
	UptimeSeconds    int64              `json:"uptime_seconds"`
	ConnectionStatus map[string]bool    `json:"connection_status"`
	Subscriptions    []common.TokenInfo `json:"subscriptions"`
	QueueDepth       map[string]int     `json:"queue_depth"`
	Ticks            map[string]any     `json:"ticks"`
	Reconnects       map[string]uint64  `json:"reconnects"`
	System           map[string]any     `json:"system"`
}

// Initialize registers Prometheus metrics.
func Initialize() {
	registerOnce.Do(func() {
		prometheus.MustRegister(
			serviceUp,
			websocketConnected,
			kafkaConnected,
			postgresConnected,
			clientLoggedIn,
			lastTickUnix,
			kafkaQueueDepth,
			postgresQueueDepth,
			ticksReceivedTotal,
			kafkaInsertedTotal,
			postgresInsertedTotal,
			reconnectsTotal,
			errorsTotal,
			ticksBySymbol,
		)
	})
	serviceUp.Set(1)
}

// Shutdown marks the service as down for metrics and dashboard consumers.
func Shutdown() {
	serviceUp.Set(0)
}

func SetWebsocketConnected(up bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.websocketUp = up
	if up {
		websocketConnected.Set(1)
		return
	}
	websocketConnected.Set(0)
}

func SetKafkaConnected(up bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.kafkaUp = up
	if up {
		kafkaConnected.Set(1)
		return
	}
	kafkaConnected.Set(0)
}

func SetPostgresConnected(up bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.postgresUp = up
	if up {
		postgresConnected.Set(1)
		return
	}
	postgresConnected.Set(0)
}

func SetClientLoggedIn(up bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.clientUp = up
	if up {
		clientLoggedIn.Set(1)
		return
	}
	clientLoggedIn.Set(0)
}

func SetSubscriptions(values []common.TokenInfo) {
	state.mu.Lock()
	defer state.mu.Unlock()
	cloned := make([]common.TokenInfo, len(values))
	copy(cloned, values)
	state.subscriptions = cloned
}

func SetKafkaQueueDepth(depth int) {
	state.mu.Lock()
	state.kafkaQueue = depth
	state.mu.Unlock()
	kafkaQueueDepth.Set(float64(depth))
}

func SetPostgresQueueDepth(depth int) {
	state.mu.Lock()
	state.postgresQueue = depth
	state.mu.Unlock()
	postgresQueueDepth.Set(float64(depth))
}

func RecordTick(symbol string, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	state.totalTicks++
	state.lastTickAt = at
	state.lastMinute[at.Unix()]++
	if symbol != "" {
		state.perSymbolTicks[symbol]++
		ticksBySymbol.WithLabelValues(symbol).Inc()
	}
	ticksReceivedTotal.Inc()
	lastTickUnix.Set(float64(at.Unix()))

	cutoff := at.Add(-59 * time.Second).Unix()
	for second := range state.lastMinute {
		if second < cutoff {
			delete(state.lastMinute, second)
		}
	}
}

func RecordKafkaInserted() {
	state.mu.Lock()
	state.totalKafkaInserted++
	state.mu.Unlock()
	kafkaInsertedTotal.Inc()
}

func RecordPostgresInserted() {
	state.mu.Lock()
	state.totalPostgresInserted++
	state.mu.Unlock()
	postgresInsertedTotal.Inc()
}

func RecordKafkaReconnect() {
	state.mu.Lock()
	state.kafkaReconnects++
	state.mu.Unlock()
	reconnectsTotal.WithLabelValues("kafka").Inc()
}

func RecordPostgresReconnect() {
	state.mu.Lock()
	state.postgresReconnects++
	state.mu.Unlock()
	reconnectsTotal.WithLabelValues("postgres").Inc()
}

func RecordWebsocketReconnect() {
	state.mu.Lock()
	state.websocketReconnects++
	state.mu.Unlock()
	reconnectsTotal.WithLabelValues("websocket").Inc()
}

func RecordClientRelogin() {
	state.mu.Lock()
	state.clientRelogins++
	state.mu.Unlock()
	reconnectsTotal.WithLabelValues("client_login").Inc()
}

func RecordError(component string) {
	if component == "" {
		component = "unknown"
	}
	errorsTotal.WithLabelValues(component).Inc()
}

// DashboardHandler serves runtime status as JSON.
func DashboardHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Snapshot())
}

// Snapshot returns a JSON-friendly summary of current runtime state.
func Snapshot() dashboardSnapshot {
	state.mu.RLock()
	defer state.mu.RUnlock()

	topSymbols := make([]symbolCount, 0, len(state.perSymbolTicks))
	for symbol, count := range state.perSymbolTicks {
		topSymbols = append(topSymbols, symbolCount{Symbol: symbol, Count: count})
	}
	sort.Slice(topSymbols, func(i, j int) bool {
		if topSymbols[i].Count == topSymbols[j].Count {
			return topSymbols[i].Symbol < topSymbols[j].Symbol
		}
		return topSymbols[i].Count > topSymbols[j].Count
	})
	if len(topSymbols) > 20 {
		topSymbols = topSymbols[:20]
	}

	nowUnix := time.Now().Unix()
	lastSecondCount := recentSecondCount(state.lastMinute, nowUnix)
	lastMinuteCount := uint64(0)
	cutoff := nowUnix - 59
	for second, count := range state.lastMinute {
		if second < cutoff {
			continue
		}
		lastMinuteCount += count
	}

	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)

	cpuPercent := float64(0)
	if processInfo != nil {
		if value, err := processInfo.CPUPercent(); err == nil {
			cpuPercent = value
		}
	}

	subscriptions := make([]common.TokenInfo, len(state.subscriptions))
	copy(subscriptions, state.subscriptions)

	return dashboardSnapshot{
		StartedAt:     state.startedAt,
		UptimeSeconds: int64(time.Since(state.startedAt).Seconds()),
		ConnectionStatus: map[string]bool{
			"websocket": state.websocketUp,
			"kafka":     state.kafkaUp,
			"postgres":  state.postgresUp,
			"client":    state.clientUp,
		},
		Subscriptions: subscriptions,
		QueueDepth: map[string]int{
			"kafka":    state.kafkaQueue,
			"postgres": state.postgresQueue,
		},
		Ticks: map[string]any{
			"total_received":          state.totalTicks,
			"per_second_received":     lastSecondCount,
			"per_minute_received":     lastMinuteCount,
			"last_tick_at":            state.lastTickAt,
			"kafka_inserted_total":    state.totalKafkaInserted,
			"postgres_inserted_total": state.totalPostgresInserted,
			"top_symbols":             topSymbols,
		},
		Reconnects: map[string]uint64{
			"kafka":     state.kafkaReconnects,
			"postgres":  state.postgresReconnects,
			"websocket": state.websocketReconnects,
			"client":    state.clientRelogins,
		},
		System: map[string]any{
			"cpu_percent":      cpuPercent,
			"goroutines":       runtime.NumGoroutine(),
			"heap_alloc_bytes": memStats.Alloc,
			"sys_bytes":        memStats.Sys,
		},
	}
}

func recentSecondCount(values map[int64]uint64, nowUnix int64) uint64 {
	if len(values) == 0 {
		return 0
	}

	if count := values[nowUnix]; count > 0 {
		return count
	}

	latestSecond := int64(0)
	latestCount := uint64(0)
	for second, count := range values {
		if second > latestSecond {
			latestSecond = second
			latestCount = count
		}
	}

	if latestSecond > 0 && nowUnix-latestSecond <= 1 {
		return latestCount
	}

	return 0
}
