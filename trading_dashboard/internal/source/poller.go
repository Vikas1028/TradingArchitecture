package source

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"trading_dashboard/internal/config"
)

type Poller struct {
	client   *http.Client
	interval time.Duration
	services []config.ServiceConfig
	mu       sync.RWMutex
	current  map[string]ServiceState
	history  map[string][]ChartPoint
}

func NewPoller(services []config.ServiceConfig, interval time.Duration) *Poller {
	return &Poller{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		interval: interval,
		services: services,
		current:  make(map[string]ServiceState, len(services)),
		history:  make(map[string][]ChartPoint, len(services)),
	}
}

func (p *Poller) Start(ctx context.Context) {
	p.refreshAll()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.refreshAll()
		}
	}
}

func (p *Poller) RefreshNow() {
	p.refreshAll()
}

func (p *Poller) SnapshotAll() AllServicesState {
	p.mu.RLock()
	defer p.mu.RUnlock()

	services := make([]ServiceState, 0, len(p.services))
	for _, service := range p.services {
		if value, ok := p.current[service.ID]; ok {
			value.ChartPoints = p.cloneHistoryLocked(service.ID)
			services = append(services, value)
			continue
		}
		services = append(services, ServiceState{
			ID:           service.ID,
			Name:         service.Name,
			SourceType:   service.SourceType,
			DashboardURL: service.DashboardURL,
			StartAllowed: strings.TrimSpace(service.StartScript) != "",
			StopAllowed:  strings.TrimSpace(service.StopScript) != "",
			LastError:    "waiting for first refresh",
			Healthy:      false,
		})
	}

	return AllServicesState{Services: services}
}

func (p *Poller) SnapshotByID(id string) (ServiceState, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, ok := p.current[id]
	if ok {
		value.ChartPoints = p.cloneHistoryLocked(id)
		return value, true
	}

	for _, service := range p.services {
		if service.ID == id {
			return ServiceState{
				ID:           service.ID,
				Name:         service.Name,
				SourceType:   service.SourceType,
				DashboardURL: service.DashboardURL,
				StartAllowed: strings.TrimSpace(service.StartScript) != "",
				StopAllowed:  strings.TrimSpace(service.StopScript) != "",
				LastError:    "waiting for first refresh",
				Healthy:      false,
			}, true
		}
	}

	return ServiceState{}, false
}

func (p *Poller) refreshAll() {
	var waitGroup sync.WaitGroup
	for _, service := range p.services {
		waitGroup.Add(1)
		go func(service config.ServiceConfig) {
			defer waitGroup.Done()
			p.refreshService(service)
		}(service)
	}
	waitGroup.Wait()
}

func (p *Poller) refreshService(service config.ServiceConfig) {
	if normalizedSourceType(service.SourceType) == "control_only" {
		running, err := isLaunchdServiceRunning(service.LaunchdLabel)
		lastError := ""
		if err != nil {
			lastError = fmt.Sprintf("launchctl check: %v", err)
		}
		snapshot := Snapshot{
			ConnectionStatus: map[string]bool{
				"launchd": running,
			},
		}
		p.setServiceState(service, snapshot, lastError, running)
		return
	}

	req, err := http.NewRequest(http.MethodGet, service.DashboardURL, nil)
	if err != nil {
		p.setServiceState(service, Snapshot{}, fmt.Sprintf("build request: %v", err), false)
		return
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.setServiceState(service, Snapshot{}, fmt.Sprintf("fetch source: %v", err), false)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.setServiceState(service, Snapshot{}, fmt.Sprintf("fetch source: unexpected status %d", resp.StatusCode), false)
		return
	}

	switch normalizedSourceType(service.SourceType) {
	case "prometheus_metrics":
		snapshot, err := parsePrometheusSnapshot(resp)
		if err != nil {
			p.setServiceState(service, Snapshot{}, fmt.Sprintf("decode metrics response: %v", err), false)
			return
		}
		snapshot = p.enrichPrometheusSnapshot(service.ID, snapshot)
		p.setServiceState(service, snapshot, "", computeHealthy(service.ID, snapshot))
	default:
		var snapshot Snapshot
		if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
			p.setServiceState(service, Snapshot{}, fmt.Sprintf("decode source response: %v", err), false)
			return
		}
		p.setServiceState(service, snapshot, "", computeHealthy(service.ID, snapshot))
	}
}

func (p *Poller) setServiceState(service config.ServiceConfig, snapshot Snapshot, lastError string, healthy bool) {
	summaryFields, detailSections := buildServicePresentation(service.ID, snapshot)
	chartPoints := p.nextHistory(service.ID, snapshot)
	p.mu.Lock()
	defer p.mu.Unlock()

	p.current[service.ID] = ServiceState{
		ID:             service.ID,
		Name:           service.Name,
		SourceType:     service.SourceType,
		DashboardURL:   service.DashboardURL,
		StartAllowed:   strings.TrimSpace(service.StartScript) != "",
		StopAllowed:    strings.TrimSpace(service.StopScript) != "",
		Running:        healthy || snapshot.ConnectionStatus["launchd"],
		Snapshot:       snapshot,
		LastUpdatedAt:  time.Now(),
		LastError:      lastError,
		Healthy:        healthy,
		SummaryFields:  summaryFields,
		DetailSections: detailSections,
		ChartPoints:    chartPoints,
	}
}

func (p *Poller) nextHistory(serviceID string, snapshot Snapshot) []ChartPoint {
	if serviceID != "go_feed" {
		return nil
	}

	now := time.Now()
	point := ChartPoint{
		At:    now,
		Value: float64(snapshot.Ticks.PerSecondReceived),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	history := append(p.history[serviceID], point)
	const maxAge = 6 * time.Hour
	trimIndex := 0
	for trimIndex < len(history) && now.Sub(history[trimIndex].At) > maxAge {
		trimIndex++
	}
	if trimIndex > 0 {
		history = history[trimIndex:]
	}

	const maxPoints = 10800
	if len(history) > maxPoints {
		history = history[len(history)-maxPoints:]
	}

	cloned := make([]ChartPoint, len(history))
	copy(cloned, history)
	p.history[serviceID] = cloned
	return cloned
}

func (p *Poller) cloneHistoryLocked(serviceID string) []ChartPoint {
	history := p.history[serviceID]
	if len(history) == 0 {
		return nil
	}

	cloned := make([]ChartPoint, len(history))
	copy(cloned, history)
	return cloned
}

func (p *Poller) enrichPrometheusSnapshot(serviceID string, snapshot Snapshot) Snapshot {
	p.mu.RLock()
	previous, ok := p.current[serviceID]
	p.mu.RUnlock()

	primaryTotal := choosePrimaryTotal(snapshot.Metrics)
	snapshot.Ticks.TotalReceived = uint64(primaryTotal)
	snapshot.Ticks.KafkaInsertedTotal = uint64(choosePrimaryOutput(snapshot.Metrics))
	snapshot.System.CPUPercent = computeCPUPercent(previous, snapshot, ok)
	snapshot.System.Goroutines = int(snapshot.Metrics["go_goroutines"])
	snapshot.System.HeapAllocBytes = uint64(snapshot.Metrics["go_memstats_heap_alloc_bytes"])
	snapshot.System.ResidentMemoryBytes = uint64(firstMetric(snapshot.Metrics, "process_resident_memory_bytes", "python_feed_process_resident_memory_bytes"))
	if snapshot.System.ResidentMemoryBytes > 0 {
		snapshot.System.SysBytes = snapshot.System.ResidentMemoryBytes
	} else {
		snapshot.System.SysBytes = uint64(snapshot.Metrics["go_memstats_sys_bytes"])
	}
	if lastTickUnix, ok := snapshot.Metrics["marketdata_last_tick_unixtime"]; ok && lastTickUnix > 0 {
		snapshot.Ticks.LastTickAt = time.Unix(int64(lastTickUnix), 0)
	}
	if startTime, ok := snapshot.Metrics["process_start_time_seconds"]; ok && startTime > 0 {
		snapshot.StartedAt = time.Unix(int64(startTime), 0)
		snapshot.UptimeSeconds = int64(time.Since(snapshot.StartedAt).Seconds())
	}

	if ok && !previous.LastUpdatedAt.IsZero() {
		elapsed := time.Since(previous.LastUpdatedAt).Seconds()
		if elapsed > 0 && primaryTotal >= float64(previous.Snapshot.Ticks.TotalReceived) {
			snapshot.Ticks.PerSecondReceived = uint64((primaryTotal - float64(previous.Snapshot.Ticks.TotalReceived)) / elapsed)
			snapshot.Ticks.PerMinuteReceived = uint64((primaryTotal - float64(previous.Snapshot.Ticks.TotalReceived)) * 60 / elapsed)
		}
	}

	return snapshot
}

func normalizedSourceType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "go_feed_dashboard"
	}
	return strings.TrimSpace(strings.ToLower(value))
}

func parsePrometheusSnapshot(resp *http.Response) (Snapshot, error) {
	snapshot := Snapshot{
		ConnectionStatus: make(map[string]bool),
		QueueDepth:       make(map[string]int),
		Reconnects:       make(map[string]uint64),
		Metrics:          make(map[string]float64),
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		metricName := sanitizeMetricName(fields[0])
		valueText := fields[len(fields)-1]
		value, err := strconv.ParseFloat(valueText, 64)
		if err != nil {
			continue
		}
		if strings.HasSuffix(metricName, "_created") {
			continue
		}
		snapshot.Metrics[metricName] += value

		switch {
		case strings.Contains(metricName, "_connected"), strings.HasSuffix(metricName, "service_up"):
			snapshot.ConnectionStatus[metricName] = value >= 1
		case strings.Contains(metricName, "queue"):
			snapshot.QueueDepth[metricName] = int(value)
		case strings.Contains(metricName, "reconnect"):
			snapshot.Reconnects[metricName] = uint64(value)
		}
	}

	if err := scanner.Err(); err != nil {
		return Snapshot{}, err
	}

	if len(snapshot.ConnectionStatus) == 0 {
		snapshot.ConnectionStatus["scrape_reachable"] = true
	}

	return snapshot, nil
}

func choosePrimaryTotal(metrics map[string]float64) float64 {
	priority := []string{
		"ticks_consumed_total",
		"ticks_published_total",
		"ticks_received_total",
		"candles_consumed_total",
		"signals_consumed_total",
		"signals_emitted_total",
		"trades_emitted_total",
		"pnl_snapshots_total",
	}
	for _, token := range priority {
		for name, value := range metrics {
			if strings.Contains(name, token) {
				return value
			}
		}
	}
	return 0
}

func choosePrimaryOutput(metrics map[string]float64) float64 {
	priority := []string{
		"kafka_inserted_total",
		"signals_emitted_total",
		"candles_emitted_total",
		"trades_emitted_total",
	}
	for _, token := range priority {
		for name, value := range metrics {
			if strings.Contains(name, token) {
				return value
			}
		}
	}
	return 0
}

func computeCPUPercent(previous ServiceState, current Snapshot, hasPrevious bool) float64 {
	currentCPU := firstMetric(current.Metrics, "process_cpu_seconds_total", "python_feed_process_cpu_seconds_total")
	if currentCPU == 0 || !hasPrevious || previous.LastUpdatedAt.IsZero() {
		return 0
	}

	previousCPU := firstMetric(previous.Snapshot.Metrics, "process_cpu_seconds_total", "python_feed_process_cpu_seconds_total")
	if previousCPU == 0 {
		return 0
	}

	elapsed := time.Since(previous.LastUpdatedAt).Seconds()
	if elapsed <= 0 || currentCPU < previousCPU {
		return 0
	}

	return ((currentCPU - previousCPU) / elapsed) * 100
}

func firstMetric(metrics map[string]float64, keys ...string) float64 {
	for _, key := range keys {
		value, ok := metrics[key]
		if ok {
			return value
		}
	}
	return 0
}

func sanitizeMetricName(raw string) string {
	name := raw
	if index := strings.Index(name, "{"); index >= 0 {
		name = name[:index]
	}
	return name
}

func computeHealthy(serviceID string, snapshot Snapshot) bool {
	if serviceID == "go_ltp" {
		return snapshot.ConnectionStatus["go_feed_service_up"] &&
			snapshot.ConnectionStatus["go_feed_kafka_connected"]
	}
	if serviceID == "market_caffeinate" {
		return true
	}
	if len(snapshot.ConnectionStatus) == 0 {
		return !snapshot.StartedAt.IsZero() || snapshot.UptimeSeconds > 0
	}
	for _, up := range snapshot.ConnectionStatus {
		if !up {
			return false
		}
	}
	return true
}

func isLaunchdServiceRunning(label string) (bool, error) {
	if strings.TrimSpace(label) == "" {
		return false, nil
	}
	output, err := exec.Command("launchctl", "list").Output()
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[2] == label {
			return fields[0] != "-" && fields[0] != "0", nil
		}
	}
	return false, nil
}
