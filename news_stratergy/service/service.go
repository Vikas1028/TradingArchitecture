package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"news_strategy/broker"
	"news_strategy/classifier"
	"news_strategy/common"
	"news_strategy/mapper"
	"news_strategy/reaction"
	"news_strategy/scoring"
	"news_strategy/signal"
)

type Service struct {
	cfg        *common.AppConfig
	logger     *zap.Logger
	broker     *broker.Client
	classifier *classifier.Engine
	mapper     *mapper.Engine
	reaction   *reaction.Engine
	scoring    *scoring.Engine
	signal     *signal.Engine
	startedAt  time.Time

	mu           sync.RWMutex
	rawCount     int
	normCount    int
	classCount   int
	mappedCount  int
	signalCount  int
	events       []common.ClassifiedEvent
	signals      []common.EventSignal
	reactions    map[string]common.MarketReaction
	lastAlert    *common.Alert
	lastReaction *common.MarketReaction
}

// New wires the news ingestion pipeline and optional JetStream publisher.
func New(cfg *common.AppConfig, logger *zap.Logger) (*Service, error) {
	var brokerClient *broker.Client
	client, err := broker.NewClient(cfg.Broker)
	if err != nil {
		logger.Warn("broker unavailable; service will run in local-only mode", zap.Error(err))
	} else {
		brokerClient = client
	}
	reactionEngine, err := reaction.New(cfg.Broker.URL, cfg.Broker.TicksSubject, cfg.Broker.CandlesSubject)
	if err != nil {
		logger.Warn("reaction engine unavailable; market confirmation disabled", zap.Error(err))
	}
	return &Service{
		cfg:        cfg,
		logger:     logger,
		broker:     brokerClient,
		classifier: classifier.New(),
		mapper:     mapper.New(cfg.Symbols),
		reaction:   reactionEngine,
		scoring:    scoring.New(),
		signal:     signal.New(cfg.Rules),
		startedAt:  time.Now(),
		events:     make([]common.ClassifiedEvent, 0, common.MaxRecentItems),
		signals:    make([]common.EventSignal, 0, common.MaxRecentItems),
		reactions:  make(map[string]common.MarketReaction),
	}, nil
}

func (s *Service) Close() {
	if s.broker != nil {
		s.broker.Close()
	}
	if s.reaction != nil {
		s.reaction.Close()
	}
}

func (s *Service) Start(ctx context.Context) {
	if s.reaction != nil {
		s.reaction.Start(ctx)
	}
}

// Handler exposes the HTTP API used for health, dashboard inspection, and manual ingest.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.Service.HealthPath, s.handleHealth)
	mux.HandleFunc(s.cfg.Service.DashboardPath, s.handleDashboard)
	mux.HandleFunc(s.cfg.Service.EventsPath, s.handleEvents)
	mux.HandleFunc(s.cfg.Service.SignalsPath, s.handleSignals)
	mux.HandleFunc(s.cfg.Service.ReactionPath, s.handleReaction)
	mux.HandleFunc(s.cfg.Service.AdminIngest, s.handleIngest)
	return mux
}

func (s *Service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, common.HealthResponse{
		Status:    "ok",
		StartedAt: s.startedAt,
		UptimeSec: int64(time.Since(s.startedAt).Seconds()),
	})
}

func (s *Service) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	snapshot := common.DashboardSnapshot{
		StartedAt:       s.startedAt,
		UptimeSeconds:   int64(time.Since(s.startedAt).Seconds()),
		BrokerConnected: s.broker != nil,
		RawCount:        s.rawCount,
		NormalizedCount: s.normCount,
		ClassifiedCount: s.classCount,
		MappedCount:     s.mappedCount,
		SignalsCount:    s.signalCount,
		LastEvents:      append([]common.ClassifiedEvent(nil), s.events...),
		LastSignals:     append([]common.EventSignal(nil), s.signals...),
		LastAlert:       s.lastAlert,
		LastReaction:    s.lastReaction,
	}
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Service) handleEvents(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	events := append([]common.ClassifiedEvent(nil), s.events...)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Service) handleSignals(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	signals := append([]common.EventSignal(nil), s.signals...)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"signals": signals})
}

func (s *Service) handleReaction(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, s.cfg.Service.ReactionPath))
	if eventID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "event_id required"})
		return
	}

	s.mu.RLock()
	reaction, ok := s.reactions[eventID]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "reaction not found"})
		return
	}
	writeJSON(w, http.StatusOK, reaction)
}

func (s *Service) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	defer r.Body.Close()
	var item common.RawNewsItem
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	signal, err := s.Process(r.Context(), item)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, signal)
}

// Process runs one raw news item through normalization, classification, mapping,
// scoring, signal generation, and broker publication.
func (s *Service) Process(ctx context.Context, item common.RawNewsItem) (common.EventSignal, error) {
	normalized := s.classifier.Normalize(item)
	classified := s.classifier.Classify(normalized)
	classified = s.mapper.Map(classified)
	var reactionSnapshot *common.MarketReaction
	if s.reaction != nil {
		reactionSnapshot = s.reaction.Analyze(classified)
	}
	score := s.scoring.Score(classified, reactionSnapshot)
	sig := s.signal.Evaluate(classified, score)
	alert := common.Alert{
		EventID:   sig.EventID,
		Symbol:    sig.Symbol,
		Signal:    sig.SignalType,
		Message:   fmt.Sprintf("[%s] %s %s score=%.2f", classified.EventType, sig.Symbol, sig.SignalType, sig.Score),
		CreatedAt: time.Now(),
	}

	if s.broker != nil {
		_ = s.broker.PublishJSON(ctx, s.cfg.Broker.RawSubject, item)
		_ = s.broker.PublishJSON(ctx, s.cfg.Broker.NormalizedSubject, normalized)
		_ = s.broker.PublishJSON(ctx, s.cfg.Broker.ClassifiedSubject, classified)
		_ = s.broker.PublishJSON(ctx, s.cfg.Broker.EventsSubject, score)
		_ = s.broker.PublishJSON(ctx, s.cfg.Broker.SignalsSubject, sig)
		_ = s.broker.PublishJSON(ctx, s.cfg.Broker.AlertsSubject, alert)
	}

	s.mu.Lock()
	s.rawCount++
	s.normCount++
	s.classCount++
	if classified.Symbol != "" {
		s.mappedCount++
	}
	if sig.SignalType != "IGNORE" {
		s.signalCount++
	}
	s.events = appendRecent(s.events, classified)
	s.signals = appendRecent(s.signals, sig)
	s.lastAlert = &alert
	s.lastReaction = reactionSnapshot
	if reactionSnapshot != nil {
		s.reactions[classified.EventID] = *reactionSnapshot
	}
	s.mu.Unlock()

	s.logger.Info("news item processed",
		zap.String("event_id", classified.EventID),
		zap.String("event_type", classified.EventType),
		zap.String("symbol", classified.Symbol),
		zap.Float64("score", score.FinalScore),
		zap.String("signal", sig.SignalType),
	)
	return sig, nil
}

func appendRecent[T any](items []T, next T) []T {
	items = append(items, next)
	if len(items) > common.MaxRecentItems {
		items = items[len(items)-common.MaxRecentItems:]
	}
	return items
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// SeedSampleEvents pushes a few high-signal aliases into the mapper for quick local testing.
func SeedSampleEvents(cfg *common.AppConfig) {
	if len(cfg.Symbols.Aliases) != 0 {
		return
	}
	cfg.Symbols.Aliases = map[string][]string{
		"HDFCBANK": {"HDFC Bank", "HDFC Bank Ltd"},
		"RELIANCE": {"Reliance Industries", "Reliance Industries Ltd"},
		"TCS":      {"Tata Consultancy Services", "TCS"},
		"INFY":     {"Infosys", "Infosys Ltd"},
	}
}

func Reverse(items []common.EventSignal) []common.EventSignal {
	cloned := append([]common.EventSignal(nil), items...)
	slices.Reverse(cloned)
	return cloned
}
