package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

const serviceName = "real_signal_router"

type appConfig struct {
	Env  string `json:"env"`
	NATS struct {
		URL           string `json:"url"`
		Stream        string `json:"stream"`
		Consumer      string `json:"consumer"`
		InputSubject  string `json:"input_subject"`
		OutputSubject string `json:"output_subject"`
	} `json:"nats"`
	Postgres struct {
		Enabled         bool   `json:"enabled"`
		Host            string `json:"host"`
		Port            int    `json:"port"`
		User            string `json:"user"`
		Password        string `json:"password"`
		DBName          string `json:"db_name"`
		SSLMode         string `json:"sslmode"`
		TableName       string `json:"table_name"`
		MaxOpenSignals  int    `json:"max_open_signals"`
		QueryTimeoutSec int    `json:"query_timeout_sec"`
	} `json:"postgres"`
	Routing struct {
		AllowedStrategies []string `json:"allowed_strategies"`
		EntryStart        string   `json:"entry_start"`
		EntryEnd          string   `json:"entry_end"`
		EntryWindows      []struct {
			Start string `json:"start"`
			End   string `json:"end"`
		} `json:"entry_windows"`
		Timezone            string  `json:"timezone"`
		MaxSignalAgeSec     int     `json:"max_signal_age_sec"`
		DedupeTTLSec        int     `json:"dedupe_ttl_sec"`
		RouteReservationSec int     `json:"route_reservation_sec"`
		FixedQuantity       int     `json:"fixed_quantity"`
		FixedStopLossPct    float64 `json:"fixed_stop_loss_pct"`
		FixedTargetPct      float64 `json:"fixed_target_pct"`
		DryRun              bool    `json:"dry_run"`
	} `json:"routing"`
}

type router struct {
	cfg            *appConfig
	nc             *nats.Conn
	db             *pgxpool.Pool
	loc            *time.Location
	allowed        map[string]struct{}
	dedupeTTL      time.Duration
	maxSignalAge   time.Duration
	entryWindows   []timeWindow
	mu             sync.Mutex
	seenSignalKeys map[string]time.Time
	reservedRoutes map[string]time.Time
}

type timeWindow struct {
	start time.Duration
	end   time.Duration
}

func main() {
	configPath := flag.String("config", "config/real_signal_router_config.json", "path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	loc, err := time.LoadLocation(cfg.Routing.Timezone)
	if err != nil {
		log.Fatalf("timezone error: %v", err)
	}

	nc, err := nats.Connect(cfg.NATS.URL, nats.Name(serviceName), nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Fatalf("nats connect error: %v", err)
	}
	defer nc.Close()

	entryWindows, err := parseEntryWindows(cfg)
	if err != nil {
		log.Fatalf("entry window parse error: %v", err)
	}

	r := &router{
		cfg:            cfg,
		nc:             nc,
		loc:            loc,
		allowed:        makeAllowedSet(cfg.Routing.AllowedStrategies),
		dedupeTTL:      time.Duration(cfg.Routing.DedupeTTLSec) * time.Second,
		maxSignalAge:   time.Duration(cfg.Routing.MaxSignalAgeSec) * time.Second,
		entryWindows:   entryWindows,
		seenSignalKeys: make(map[string]time.Time),
		reservedRoutes: make(map[string]time.Time),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Postgres.Enabled {
		db, err := pgxpool.New(ctx, buildPostgresDSN(cfg))
		if err != nil {
			log.Fatalf("postgres connect error: %v", err)
		}
		if err := db.Ping(ctx); err != nil {
			db.Close()
			log.Fatalf("postgres ping error: %v", err)
		}
		defer db.Close()
		r.db = db
	}

	log.Printf("%s started env=%s input=%s output=%s dry_run=%t strategies=%v",
		serviceName,
		cfg.Env,
		cfg.NATS.InputSubject,
		cfg.NATS.OutputSubject,
		cfg.Routing.DryRun,
		cfg.Routing.AllowedStrategies,
	)

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream init error: %v", err)
	}

	_, err = js.AddConsumer(cfg.NATS.Stream, &nats.ConsumerConfig{
		Durable:       cfg.NATS.Consumer,
		Description:   "Routes selected strategy signals to signals.real for real_engine",
		DeliverPolicy: nats.DeliverNewPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		FilterSubject: cfg.NATS.InputSubject,
		ReplayPolicy:  nats.ReplayInstantPolicy,
		MaxAckPending: 1024,
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "in use") {
		log.Fatalf("consumer setup error: %v", err)
	}

	sub, err := js.PullSubscribe(
		cfg.NATS.InputSubject,
		cfg.NATS.Consumer,
		nats.BindStream(cfg.NATS.Stream),
		nats.ManualAck(),
	)
	if err != nil {
		log.Fatalf("pull subscribe error: %v", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s shutting down", serviceName)
			return
		default:
		}

		msgs, err := sub.Fetch(10, nats.MaxWait(2*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, msg := range msgs {
			if err := r.handleSignal(msg); err != nil {
				log.Printf("handle signal failed: %v", err)
				continue
			}
			if err := msg.Ack(); err != nil {
				log.Printf("ack failed: %v", err)
			}
		}
	}
}

func loadConfig(path string) (*appConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg appConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		cfg.NATS.URL = "nats://127.0.0.1:4222"
	}
	if strings.TrimSpace(cfg.NATS.Stream) == "" {
		cfg.NATS.Stream = "SIGNALS_STRATEGY"
	}
	if strings.TrimSpace(cfg.NATS.Consumer) == "" {
		cfg.NATS.Consumer = "real-signal-router-v1"
	}
	if strings.TrimSpace(cfg.NATS.InputSubject) == "" {
		cfg.NATS.InputSubject = "signals.strategy"
	}
	if strings.TrimSpace(cfg.NATS.OutputSubject) == "" {
		cfg.NATS.OutputSubject = "signals.real"
	}
	if strings.TrimSpace(cfg.Routing.Timezone) == "" {
		cfg.Routing.Timezone = "Asia/Kolkata"
	}
	if cfg.Routing.MaxSignalAgeSec <= 0 {
		cfg.Routing.MaxSignalAgeSec = 30
	}
	if cfg.Routing.DedupeTTLSec <= 0 {
		cfg.Routing.DedupeTTLSec = 21600
	}
	if cfg.Routing.RouteReservationSec <= 0 {
		cfg.Routing.RouteReservationSec = 45
	}
	if cfg.Routing.FixedQuantity <= 0 {
		cfg.Routing.FixedQuantity = 1
	}
	if cfg.Routing.FixedStopLossPct <= 0 {
		cfg.Routing.FixedStopLossPct = 0.30
	}
	if cfg.Routing.FixedTargetPct < 0 {
		cfg.Routing.FixedTargetPct = 0
	}
	if len(cfg.Routing.EntryWindows) == 0 {
		if strings.TrimSpace(cfg.Routing.EntryStart) == "" {
			cfg.Routing.EntryStart = "09:15:00"
		}
		if strings.TrimSpace(cfg.Routing.EntryEnd) == "" {
			cfg.Routing.EntryEnd = "09:30:00"
		}
	}
	if len(cfg.Routing.AllowedStrategies) == 0 {
		cfg.Routing.AllowedStrategies = []string{"openmarketvolatility_s2_30sec_burst"}
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
	}
	if strings.TrimSpace(cfg.Postgres.SSLMode) == "" {
		cfg.Postgres.SSLMode = "disable"
	}
	if strings.TrimSpace(cfg.Postgres.TableName) == "" {
		cfg.Postgres.TableName = "real_engine_trades"
	}
	if cfg.Postgres.MaxOpenSignals <= 0 {
		cfg.Postgres.MaxOpenSignals = 3
	}
	if cfg.Postgres.QueryTimeoutSec <= 0 {
		cfg.Postgres.QueryTimeoutSec = 2
	}
	return &cfg, nil
}

func buildPostgresDSN(cfg *appConfig) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DBName,
		cfg.Postgres.SSLMode,
	)
}

func (r *router) handleSignal(msg *nats.Msg) error {
	var payload map[string]any
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	strategy := strings.TrimSpace(stringValue(payload["strategy"]))
	if _, ok := r.allowed[strategy]; !ok {
		return nil
	}

	signalTime, err := parseSignalTime(stringValue(payload["time"]), r.loc)
	if err != nil {
		return fmt.Errorf("parse signal time for %s: %w", strategy, err)
	}

	if !r.isWithinWindow(signalTime) {
		log.Printf("skip signal outside router window strategy=%s symbol=%s time=%s", strategy, stringValue(payload["symbol"]), signalTime.Format(time.RFC3339))
		return nil
	}

	if age := time.Since(signalTime); age > r.maxSignalAge {
		log.Printf("skip stale signal strategy=%s symbol=%s age=%s", strategy, stringValue(payload["symbol"]), age.Truncate(time.Second))
		return nil
	}

	signalKey := routeKey(payload, signalTime)

	if r.db != nil {
		queryCtx, cancel := context.WithTimeout(context.Background(), time.Duration(r.cfg.Postgres.QueryTimeoutSec)*time.Second)
		defer cancel()

		symbol := stringValue(payload["symbol"])
		symbolOpen, err := r.isSymbolOpen(queryCtx, symbol)
		if err != nil {
			return fmt.Errorf("check symbol open state: %w", err)
		}
		if symbolOpen {
			log.Printf("skip signal already-open-symbol strategy=%s symbol=%s", strategy, symbol)
			return nil
		}

		openCount, err := r.currentOpenPositionCount(queryCtx)
		if err != nil {
			return fmt.Errorf("check open positions: %w", err)
		}
		reservedCount := r.activeReservedRouteCount()
		if openCount+reservedCount >= r.cfg.Postgres.MaxOpenSignals {
			log.Printf("skip signal max-open-positions strategy=%s symbol=%s open_positions=%d reserved=%d limit=%d",
				strategy,
				stringValue(payload["symbol"]),
				openCount,
				reservedCount,
				r.cfg.Postgres.MaxOpenSignals,
			)
			return nil
		}
	}

	if !r.markSeen(signalKey) {
		log.Printf("skip duplicate signal strategy=%s symbol=%s key=%s", strategy, stringValue(payload["symbol"]), signalKey)
		return nil
	}

	payload["router_service"] = serviceName
	payload["router_subject_in"] = r.cfg.NATS.InputSubject
	payload["router_subject_out"] = r.cfg.NATS.OutputSubject
	payload["routed_to_real_at"] = time.Now().In(r.loc).Format(time.RFC3339Nano)
	payload["real_execution_requested"] = true
	payload["quantity"] = r.cfg.Routing.FixedQuantity
	payload["stop_loss_pct"] = r.cfg.Routing.FixedStopLossPct
	payload["target_pct"] = r.cfg.Routing.FixedTargetPct
	delete(payload, "trailing_stop_pct")
	delete(payload, "trailing_freeze_profit_pct")

	out, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal routed payload: %w", err)
	}

	if r.cfg.Routing.DryRun {
		log.Printf("dry-run route strategy=%s symbol=%s side=%s signal_time=%s", strategy, stringValue(payload["symbol"]), stringValue(payload["side"]), signalTime.Format(time.RFC3339))
		return nil
	}

	if err := r.nc.Publish(r.cfg.NATS.OutputSubject, out); err != nil {
		return fmt.Errorf("publish to %s: %w", r.cfg.NATS.OutputSubject, err)
	}
	r.reserveRoute(signalKey)
	log.Printf("routed signal to real_engine strategy=%s symbol=%s side=%s signal_time=%s", strategy, stringValue(payload["symbol"]), stringValue(payload["side"]), signalTime.Format(time.RFC3339))
	return nil
}

func (r *router) currentOpenPositionCount(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
		WITH latest AS (
			SELECT DISTINCT ON (symbol) symbol, trade_type
			FROM %s
			ORDER BY symbol, event_time DESC, id DESC
		)
		SELECT count(*)
		FROM latest
		WHERE trade_type = 'ENTRY'
	`, r.cfg.Postgres.TableName)

	var count int
	if err := r.db.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *router) isSymbolOpen(ctx context.Context, symbol string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT trade_type
		FROM %s
		WHERE symbol = $1
		ORDER BY event_time DESC, id DESC
		LIMIT 1
	`, r.cfg.Postgres.TableName)

	var tradeType string
	err := r.db.QueryRow(ctx, query, symbol).Scan(&tradeType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return tradeType == "ENTRY", nil
}

func (r *router) isWithinWindow(ts time.Time) bool {
	local := ts.In(r.loc)
	clock := time.Duration(local.Hour())*time.Hour + time.Duration(local.Minute())*time.Minute + time.Duration(local.Second())*time.Second
	for _, window := range r.entryWindows {
		if clock >= window.start && clock <= window.end {
			return true
		}
	}
	return false
}

func (r *router) markSeen(key string) bool {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, seenAt := range r.seenSignalKeys {
		if now.Sub(seenAt) > r.dedupeTTL {
			delete(r.seenSignalKeys, k)
		}
	}
	if _, exists := r.seenSignalKeys[key]; exists {
		return false
	}
	r.seenSignalKeys[key] = now
	return true
}

func (r *router) reserveRoute(key string) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupReservedRoutesLocked(now)
	r.reservedRoutes[key] = now
}

func (r *router) activeReservedRouteCount() int {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupReservedRoutesLocked(now)
	return len(r.reservedRoutes)
}

func (r *router) cleanupReservedRoutesLocked(now time.Time) {
	ttl := time.Duration(r.cfg.Routing.RouteReservationSec) * time.Second
	for key, routedAt := range r.reservedRoutes {
		if now.Sub(routedAt) > ttl {
			delete(r.reservedRoutes, key)
		}
	}
}

func makeAllowedSet(items []string) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out[item] = struct{}{}
	}
	return out
}

func parseClock(value string) (time.Duration, error) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("expected HH:MM:SS")
	}
	var hh, mm, ss int
	if _, err := fmt.Sscanf(value, "%d:%d:%d", &hh, &mm, &ss); err != nil {
		return 0, err
	}
	return time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second, nil
}

func parseEntryWindows(cfg *appConfig) ([]timeWindow, error) {
	if len(cfg.Routing.EntryWindows) == 0 {
		start, err := parseClock(cfg.Routing.EntryStart)
		if err != nil {
			return nil, fmt.Errorf("entry_start: %w", err)
		}
		end, err := parseClock(cfg.Routing.EntryEnd)
		if err != nil {
			return nil, fmt.Errorf("entry_end: %w", err)
		}
		if end < start {
			return nil, fmt.Errorf("entry_end must be >= entry_start")
		}
		return []timeWindow{{start: start, end: end}}, nil
	}

	windows := make([]timeWindow, 0, len(cfg.Routing.EntryWindows))
	for idx, raw := range cfg.Routing.EntryWindows {
		start, err := parseClock(raw.Start)
		if err != nil {
			return nil, fmt.Errorf("entry_windows[%d].start: %w", idx, err)
		}
		end, err := parseClock(raw.End)
		if err != nil {
			return nil, fmt.Errorf("entry_windows[%d].end: %w", idx, err)
		}
		if end < start {
			return nil, fmt.Errorf("entry_windows[%d]: end must be >= start", idx)
		}
		windows = append(windows, timeWindow{start: start, end: end})
	}
	return windows, nil
}

func parseSignalTime(value string, loc *time.Location) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return ts.In(loc), nil
	}
	return time.Time{}, fmt.Errorf("unsupported time format %q", value)
}

func routeKey(payload map[string]any, ts time.Time) string {
	if signalID := strings.TrimSpace(stringValue(payload["signal_id"])); signalID != "" {
		return signalID
	}
	return strings.Join([]string{
		stringValue(payload["strategy"]),
		stringValue(payload["symbol"]),
		stringValue(payload["side"]),
		ts.Format(time.RFC3339Nano),
	}, "|")
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
