package dashboard

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type Snapshot struct {
	StartedAt      time.Time `json:"started_at"`
	ActiveSymbols  int       `json:"active_symbols"`
	LastTickTime   time.Time `json:"last_tick_time"`
	SignalsTotal   uint64    `json:"signals_total"`
	Strategy1Total uint64    `json:"strategy1_total"`
	Strategy2Total uint64    `json:"strategy2_total"`
	Strategy3Total uint64    `json:"strategy3_total"`
	Strategy4Total uint64    `json:"strategy4_total"`
}

type Store struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func NewStore() *Store {
	return &Store{snapshot: Snapshot{StartedAt: time.Now()}}
}

func (s *Store) Update(snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
}

func (s *Store) Handler(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.snapshot)
}
