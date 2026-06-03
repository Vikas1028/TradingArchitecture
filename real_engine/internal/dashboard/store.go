package dashboard

import (
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"

	"real_engine/common"
)

type Store struct {
	startedAt time.Time
	mu        sync.RWMutex
	snapshot  common.DashboardSnapshot
}

func NewStore(startedAt time.Time) *Store {
	return &Store{
		startedAt: startedAt,
		snapshot: common.DashboardSnapshot{
			StartedAt:        startedAt,
			ConnectionStatus: map[string]bool{},
			QueueDepth:       map[string]int{},
			Reconnects:       map[string]uint64{},
			Metrics:          map[string]float64{},
		},
	}
}

func (s *Store) Update(snapshot common.DashboardSnapshot) {
	system := common.SystemSummary{
		Goroutines: runtime.NumGoroutine(),
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	system.HeapAllocBytes = mem.HeapAlloc
	system.SysBytes = mem.Sys
	snapshot.StartedAt = s.startedAt
	snapshot.UptimeSeconds = int64(time.Since(s.startedAt).Seconds())
	snapshot.System = system

	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
}

func (s *Store) Snapshot() common.DashboardSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *Store) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Snapshot())
}
