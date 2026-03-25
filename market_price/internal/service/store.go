package service

import (
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"market_price/common"
)

type Store struct {
	startedAt time.Time
	sources   []string
	mu        sync.RWMutex
	data      map[string]map[string]common.TopicPrice
	connected map[string]bool
	proc      *process.Process
}

func NewStore(sources []string) *Store {
	connected := make(map[string]bool, len(sources))
	for _, source := range sources {
		connected[source] = false
	}
	proc, _ := process.NewProcess(int32(os.Getpid()))
	return &Store{
		startedAt: time.Now(),
		sources:   append([]string(nil), sources...),
		data:      make(map[string]map[string]common.TopicPrice),
		connected: connected,
		proc:      proc,
	}
}

// SetConnected records whether the source consumer loop is still live.
func (s *Store) SetConnected(source string, up bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected[source] = up
}

// Apply stores the newest price seen for one symbol/source pair.
func (s *Store) Apply(update common.TopicPrice, source, symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[symbol] == nil {
		s.data[symbol] = make(map[string]common.TopicPrice, len(s.sources))
	}
	s.data[symbol][source] = update
	s.connected[source] = true
}

// Snapshot builds the HTTP payload consumed by the trading dashboard.
func (s *Store) Snapshot() common.DashboardSnapshot {
	s.mu.RLock()
	rows := make([]common.SymbolPriceRow, 0, len(s.data))
	for symbol, prices := range s.data {
		// Keep the dashboard aligned to the active feed universe by
		// excluding symbols seen only on the stale python topic history.
		if _, ok := prices["go_feed"]; !ok {
			if _, ok := prices["go_ltp"]; !ok {
				continue
			}
		}
		cloned := make(map[string]common.TopicPrice, len(prices))
		for source, price := range prices {
			cloned[source] = price
		}
		rows = append(rows, common.SymbolPriceRow{Symbol: symbol, Prices: cloned})
	}
	connections := make(map[string]bool, len(s.connected))
	for source, up := range s.connected {
		connections[source] = up
	}
	sources := append([]string(nil), s.sources...)
	s.mu.RUnlock()

	sort.Slice(rows, func(i, j int) bool { return rows[i].Symbol < rows[j].Symbol })

	system := common.SystemSummary{
		Goroutines: runtime.NumGoroutine(),
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	system.HeapAllocBytes = mem.HeapAlloc
	system.SysBytes = mem.Sys
	if s.proc != nil {
		if percent, err := s.proc.CPUPercent(); err == nil {
			system.CPUPercent = percent
		}
		if info, err := s.proc.MemoryInfo(); err == nil && info != nil {
			system.ResidentMemoryBytes = info.RSS
		}
	}

	return common.DashboardSnapshot{
		StartedAt:          s.startedAt,
		UptimeSeconds:      int64(time.Since(s.startedAt).Seconds()),
		ConnectionStatus:   connections,
		QueueDepth:         map[string]int{},
		Reconnects:         map[string]uint64{},
		System:             system,
		MarketPriceSources: sources,
		MarketPrices:       rows,
	}
}
