package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	baseDir              = "/Users/vikasbhandekar/live_services/trading_dashboard"
	mainConfigPath       = baseDir + "/cmd/config.json"
	coreConfigPath       = baseDir + "/cmd/config.core.json"
	coreBinaryPath       = baseDir + "/cmd/trading_dashboard_core"
	templateTradeHistory = baseDir + "/internal/web/templates/trade_history.html"
	templateStockFilter  = baseDir + "/internal/web/templates/stock_filter.html"
	templateChartReading = baseDir + "/internal/web/templates/chart_reading.html"
	coreBaseURL          = "http://127.0.0.1:9201"
	istOffsetSeconds     = 5*3600 + 30*60
	natsURL              = "nats://127.0.0.1:4222"
)

var chartReadingTimeframes = []string{"5s", "15s", "30s", "1m", "3m", "5m", "10m", "15m", "30m", "1h", "3h", "1d"}
var chartReadingIndexes = []string{"NIFTY", "BANKNIFTY"}
var stockFilters = newStockFilterCache()
var marketIST = time.FixedZone("Asia/Kolkata", istOffsetSeconds)

type dashboardConfig struct {
	BindAddress string `json:"bind_address"`
	PageTitle   string `json:"page_title"`
}

type childRunner struct {
	mu  sync.Mutex
	cmd *exec.Cmd
}

func main() {
	cfg, err := loadConfig(mainConfigPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if err := ensureCoreConfig(); err != nil {
		log.Fatalf("prepare core config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := &childRunner{}
	if err := runner.startCore(ctx); err != nil {
		log.Fatalf("start core dashboard: %v", err)
	}
	defer runner.stopCore()

	pageTitle := strings.TrimSpace(cfg.PageTitle)
	if pageTitle == "" {
		pageTitle = "Trading Dashboard"
	}

	tradeHistoryTmpl, err := template.ParseFiles(templateTradeHistory)
	if err != nil {
		log.Fatalf("parse trade_history template: %v", err)
	}
	stockFilterTmpl, err := template.ParseFiles(templateStockFilter)
	if err != nil {
		log.Fatalf("parse stock_filter template: %v", err)
	}
	chartReadingTmpl, err := template.ParseFiles(templateChartReading)
	if err != nil {
		log.Fatalf("parse chart_reading template: %v", err)
	}

	coreURL, err := url.Parse(coreBaseURL)
	if err != nil {
		log.Fatalf("parse core url: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(coreURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		http.Error(w, fmt.Sprintf("dashboard upstream error: %v", e), http.StatusBadGateway)
	}
	proxy.ModifyResponse = injectChartReadingNav
	stockFilters.ensureStarted()

	mux := http.NewServeMux()
	mux.HandleFunc("/trade_history", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tradeHistoryTmpl.Execute(w, map[string]any{"PageTitle": pageTitle}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/stock_filler", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := stockFilterTmpl.Execute(w, map[string]any{"PageTitle": pageTitle}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/chart_reading", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := chartReadingTmpl.Execute(w, map[string]any{"PageTitle": pageTitle}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/api/chart_reading/symbols", chartReadingSymbolsHandler)
	mux.HandleFunc("/api/chart_reading/candles", chartReadingCandlesHandler)
	mux.HandleFunc("/api/trades/chart", func(w http.ResponseWriter, r *http.Request) {
		if err := proxyChartWithIST(r.Context(), w, r); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
	})
	mux.HandleFunc("/api/tokens/dhan", handleDhanTokenUpdate)
	mux.HandleFunc("/api/stock_filter/", func(w http.ResponseWriter, r *http.Request) {
		stockFilterHandler(w, r)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:    cfg.BindAddress,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("trading_dashboard wrapper listening on %s", cfg.BindAddress)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}

func loadConfig(path string) (dashboardConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return dashboardConfig{}, err
	}
	var cfg dashboardConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return dashboardConfig{}, err
	}
	if strings.TrimSpace(cfg.BindAddress) == "" {
		cfg.BindAddress = ":9200"
	}
	return cfg, nil
}

func ensureCoreConfig() error {
	raw, err := os.ReadFile(mainConfigPath)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	doc["bind_address"] = ":9201"
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(coreConfigPath, encoded, 0o644)
}

func (r *childRunner) startCore(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if coreReachable() {
		return nil
	}
	if _, err := os.Stat(coreBinaryPath); err != nil {
		return fmt.Errorf("core binary missing at %s", coreBinaryPath)
	}

	logDir := filepath.Join(baseDir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return err
	}
	stdoutFile, err := os.OpenFile(filepath.Join(logDir, "core.stdout.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	stderrFile, err := os.OpenFile(filepath.Join(logDir, "core.stderr.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		_ = stdoutFile.Close()
		return err
	}

	cmd := exec.CommandContext(ctx, coreBinaryPath)
	cmd.Env = append(os.Environ(), "TRADING_DASHBOARD_CONFIG="+coreConfigPath)
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile
	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return err
	}
	r.cmd = cmd

	go func(c *exec.Cmd, out, errf *os.File) {
		_ = c.Wait()
		_ = out.Close()
		_ = errf.Close()
	}(cmd, stdoutFile, stderrFile)

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if coreReachable() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("core did not start on %s in time", coreBaseURL)
}

func (r *childRunner) stopCore() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cmd == nil || r.cmd.Process == nil {
		return
	}
	_ = r.cmd.Process.Signal(syscall.SIGTERM)
}

func coreReachable() bool {
	client := &http.Client{Timeout: 1200 * time.Millisecond}
	req, err := http.NewRequest(http.MethodGet, coreBaseURL+"/services", nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 500
}

func injectChartReadingNav(resp *http.Response) error {
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(contentType, "text/html") {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	html := string(body)
	if !strings.Contains(html, `href="/chart_reading"`) {
		html = strings.ReplaceAll(html,
			`<a class="link" href="/chart_view">Chart View</a>`,
			`<a class="link" href="/chart_view">Chart View</a>`+"\n"+`      <a class="link" href="/chart_reading">Chart Reading</a>`)
		html = strings.ReplaceAll(html,
			`<a class="nav-link" href="/chart_view">Chart View</a>`,
			`<a class="nav-link" href="/chart_view">Chart View</a>`+"\n"+`      <a class="nav-link" href="/chart_reading">Chart Reading</a>`)
	}
	html = strings.ReplaceAll(
		html,
		`Updates Dhan access token in <strong>go_feed</strong>, <strong>go_ltp</strong>, and <strong>go_option_feed</strong>, then restarts those three services.`,
		`Updates Dhan access token in <strong>go_feed</strong>, <strong>go_ltp</strong>, <strong>go_option_feed</strong>, <strong>real_engine</strong>, and <strong>real_engine_broker_sync</strong>, then restarts all five services.`,
	)
	html = strings.ReplaceAll(
		html,
		`Updates Dhan access token in <strong>go_feed</strong>, <strong>go_ltp</strong>, <strong>go_option_feed</strong>, and <strong>real_engine</strong>, then restarts all four services.`,
		`Updates Dhan access token in <strong>go_feed</strong>, <strong>go_ltp</strong>, <strong>go_option_feed</strong>, <strong>real_engine</strong>, and <strong>real_engine_broker_sync</strong>, then restarts all five services.`,
	)
	next := []byte(html)
	resp.Body = io.NopCloser(bytes.NewReader(next))
	resp.ContentLength = int64(len(next))
	resp.Header.Set("Content-Length", strconv.Itoa(len(next)))
	return nil
}

type dhanTokenRequest struct {
	AccessToken string `json:"access_token"`
}

type dhanTokenServiceResult struct {
	ServiceID string `json:"service_id"`
	Updated   bool   `json:"updated"`
	Restarted bool   `json:"restarted"`
	Error     string `json:"error,omitempty"`
}

type dhanTokenResponse struct {
	OK       bool                     `json:"ok"`
	Message  string                   `json:"message"`
	Error    string                   `json:"error,omitempty"`
	Services []dhanTokenServiceResult `json:"services"`
}

type dhanManagedService struct {
	ID          string
	ConfigPath  string
	StartScript string
	Format      string
}

var dhanManagedServices = []dhanManagedService{
	{
		ID:          "go_feed",
		ConfigPath:  "/Users/vikasbhandekar/live_services/go_feed/cmd/config.cfg",
		StartScript: "/Users/vikasbhandekar/live_services/go_feed/scripts/start_mac.sh",
		Format:      "ini",
	},
	{
		ID:          "go_ltp",
		ConfigPath:  "/Users/vikasbhandekar/live_services/go_ltp/cmd/config.cfg",
		StartScript: "/Users/vikasbhandekar/live_services/go_ltp/scripts/start_mac.sh",
		Format:      "ini",
	},
	{
		ID:          "go_option_feed",
		ConfigPath:  "/Users/vikasbhandekar/live_services/go_option_feed/cmd/config.cfg",
		StartScript: "/Users/vikasbhandekar/live_services/go_option_feed/scripts/start_mac.sh",
		Format:      "ini",
	},
	{
		ID:          "real_engine",
		ConfigPath:  "/Users/vikasbhandekar/live_services/real_engine/config/real_engine_config.json",
		StartScript: "/Users/vikasbhandekar/live_services/real_engine/internal/scripts/start_mac.sh",
		Format:      "json",
	},
	{
		ID:          "real_engine_broker_sync",
		StartScript: "/Users/vikasbhandekar/live_services/real_engine_broker_sync/scripts/start_mac.sh",
		Format:      "noop",
	},
}

func handleDhanTokenUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONStatus(w, http.StatusMethodNotAllowed, dhanTokenResponse{
			OK:      false,
			Message: "method not allowed",
			Error:   "method not allowed",
		})
		return
	}

	var req dhanTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, dhanTokenResponse{
			OK:      false,
			Message: "invalid request body",
			Error:   err.Error(),
		})
		return
	}
	accessToken := strings.TrimSpace(req.AccessToken)
	if accessToken == "" {
		writeJSONStatus(w, http.StatusBadRequest, dhanTokenResponse{
			OK:      false,
			Message: "access token is required",
			Error:   "access token is required",
		})
		return
	}

	results := make([]dhanTokenServiceResult, 0, len(dhanManagedServices))
	allOK := true
	for _, svc := range dhanManagedServices {
		result := dhanTokenServiceResult{ServiceID: svc.ID}
		if err := updateDhanTokenInConfig(svc, accessToken); err != nil {
			result.Error = err.Error()
			results = append(results, result)
			allOK = false
			continue
		}
		result.Updated = true
		if err := restartManagedService(svc); err != nil {
			result.Error = err.Error()
			results = append(results, result)
			allOK = false
			continue
		}
		result.Restarted = true
		results = append(results, result)
	}

	status := http.StatusOK
	resp := dhanTokenResponse{
		OK:       allOK,
		Message:  "Token updated and services restarted.",
		Services: results,
	}
	if !allOK {
		status = http.StatusInternalServerError
		resp.Message = "Token update completed with errors."
		resp.Error = "one or more services failed"
	}
	writeJSONStatus(w, status, resp)
}

func updateDhanTokenInConfig(svc dhanManagedService, accessToken string) error {
	switch svc.Format {
	case "ini":
		return updateINIConfigToken(svc.ConfigPath, accessToken)
	case "json":
		return updateJSONConfigToken(svc.ConfigPath, accessToken)
	case "noop":
		return nil
	default:
		return fmt.Errorf("unsupported config format %q", svc.Format)
	}
}

func updateINIConfigToken(path, accessToken string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	updated := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "access_token=") {
			lines[i] = "access_token=" + accessToken
			updated = true
			break
		}
	}
	if !updated {
		return fmt.Errorf("access_token key not found in %s", path)
	}
	output := strings.Join(lines, "\n")
	if !strings.HasSuffix(output, "\n") {
		output += "\n"
	}
	return os.WriteFile(path, []byte(output), 0o644)
}

func updateJSONConfigToken(path, accessToken string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	dhan, ok := doc["dhan"].(map[string]any)
	if !ok {
		return fmt.Errorf("dhan object not found in %s", path)
	}
	dhan["access_token"] = accessToken
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(path, encoded, 0o644)
}

func restartManagedService(svc dhanManagedService) error {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, svc.StartScript)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("restart %s: start script timed out", svc.ID)
		}
		msg := strings.TrimSpace(string(output))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("restart %s: %s", svc.ID, msg)
	}
	return nil
}

func writeJSONStatus(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func proxyChartWithIST(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	upstreamURL := coreBaseURL + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL, nil)
	if err != nil {
		return err
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		_, _ = w.Write(body)
		return nil
	}

	shifted, changed, err := shiftChartPayloadIST(body)
	if err != nil {
		_, _ = w.Write(body)
		return nil
	}
	if !changed {
		_, _ = w.Write(body)
		return nil
	}
	_, _ = w.Write(shifted)
	return nil
}

func proxyStockFilterNormalized(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	upstreamURL := coreBaseURL + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL, nil)
	if err != nil {
		return err
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.Header().Del("Content-Length")

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
		return nil
	}

	fixed, changed, err := normalizeStockFilterPayload(r.URL.Path, body)
	if err != nil || !changed {
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
		return nil
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(fixed)
	return nil
}

func normalizeStockFilterPayload(path string, body []byte) ([]byte, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, false, err
	}

	rowsAny, ok := payload["rows"].([]any)
	if !ok {
		return nil, false, nil
	}
	action := strings.TrimPrefix(path, "/api/stock_filter/")

	changed := false
	for _, rowAny := range rowsAny {
		row, ok := rowAny.(map[string]any)
		if !ok {
			continue
		}

		ltp, hasLTP := valueAsFloat(row["ltp"])
		dayOpen, hasDayOpen := valueAsFloat(row["day_open"])

		if hasLTP && hasDayOpen && dayOpen > 0 {
			if dayOpen >= 100000 && ltp >= 100000 {
				dayOpen = dayOpen / 100.0
				ltp = ltp / 100.0
				row["day_open"] = round2(dayOpen)
				row["ltp"] = round2(ltp)
				changed = true
			} else if ltp/dayOpen > 20 || ltp/dayOpen < 0.02 {
				ltp = ltp / 100.0
				row["ltp"] = round2(ltp)
				changed = true
			}
			movePct := ((ltp - dayOpen) / dayOpen) * 100.0
			row["move_pct"] = round2(movePct)
			changed = true
		}

		if candlePct, ok := valueAsFloat(row["candle_pct"]); ok {
			normalized := candlePct
			if candlePct > 500 {
				normalized = (candlePct - 9900.0) / 100.0
			} else if candlePct <= -95 && candlePct > -100.5 {
				normalized = candlePct + 99.0
			}
			if normalized != candlePct {
				row["candle_pct"] = round2(normalized)
				changed = true
			}
		}

		if hasLTP && !hasDayOpen && ltp >= 10000 {
			if almostInt(ltp) && ltp/100.0 < 20000 {
				row["ltp"] = round2(ltp / 100.0)
				changed = true
			}
		}
	}

	rowsAny = filterStockFilterRows(action, rowsAny)
	sortStockFilterRows(action, rowsAny)
	payload["rows"] = rowsAny
	payload["count"] = len(rowsAny)

	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	return encoded, changed, nil
}

func sortStockFilterRows(action string, rows []any) {
	switch action {
	case "high_gainers":
		sort.SliceStable(rows, func(i, j int) bool {
			left := rowMetric(rows[i], "move_pct")
			right := rowMetric(rows[j], "move_pct")
			return left > right
		})
	case "high_losers":
		sort.SliceStable(rows, func(i, j int) bool {
			left := rowMetric(rows[i], "move_pct")
			right := rowMetric(rows[j], "move_pct")
			return left < right
		})
	case "high_gainers_candle", "three_green_1m":
		sort.SliceStable(rows, func(i, j int) bool {
			left := rowMetric(rows[i], "candle_pct")
			right := rowMetric(rows[j], "candle_pct")
			return left > right
		})
	case "high_losers_candle":
		sort.SliceStable(rows, func(i, j int) bool {
			left := rowMetric(rows[i], "candle_pct")
			right := rowMetric(rows[j], "candle_pct")
			return left < right
		})
	}
}

func filterStockFilterRows(action string, rows []any) []any {
	filtered := make([]any, 0, len(rows))
	for _, row := range rows {
		switch action {
		case "high_gainers", "high_gainers_candle", "three_green_1m":
			if rowMetric(row, "move_pct") > 0 || rowMetric(row, "candle_pct") > 0 {
				filtered = append(filtered, row)
			}
		case "high_losers", "high_losers_candle":
			if rowMetric(row, "move_pct") < 0 || rowMetric(row, "candle_pct") < 0 {
				filtered = append(filtered, row)
			}
		default:
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func rowMetric(v any, key string) float64 {
	row, ok := v.(map[string]any)
	if !ok {
		return 0
	}
	out, _ := valueAsFloat(row[key])
	return out
}

func valueAsFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func round2(v float64) float64 {
	out := math.Round(v*100) / 100
	if math.Abs(out) < 0.005 {
		return 0
	}
	return out
}

func almostInt(v float64) bool {
	return math.Abs(v-math.Round(v)) < 1e-9
}

type chartReadingCandle struct {
	Symbol    string  `json:"symbol"`
	Time      string  `json:"time"`
	Timeframe string  `json:"timeframe"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int64   `json:"volume"`
	VWAP      float64 `json:"vwap"`
}

type stockFilterRow struct {
	Symbol        string  `json:"symbol"`
	LTP           float64 `json:"ltp"`
	PreviousClose float64 `json:"previous_close,omitempty"`
	DayOpen       float64 `json:"day_open,omitempty"`
	MovePct       float64 `json:"move_pct,omitempty"`
	CandlePct     float64 `json:"candle_pct,omitempty"`
	Minute        string  `json:"minute,omitempty"`
	GreenStreak   int     `json:"green_streak,omitempty"`
}

type stockFilterRecord struct {
	Symbol       string
	Candles1m    map[int64]chartReadingCandle
	CurrentPrice float64
	CurrentTime  time.Time
}

type stockFilterRecordView struct {
	Symbol       string
	Candles1m    []chartReadingCandle
	CurrentPrice float64
	CurrentTime  time.Time
}

type stockFilterCache struct {
	once        sync.Once
	mu          sync.RWMutex
	records     map[string]*stockFilterRecord
	symbols     map[string]bool
	startedAt   time.Time
	lastUpdated time.Time
	lastError   string
}

func newStockFilterCache() *stockFilterCache {
	return &stockFilterCache{records: make(map[string]*stockFilterRecord)}
}

func (c *stockFilterCache) ensureStarted() {
	c.once.Do(func() {
		c.mu.Lock()
		c.startedAt = time.Now()
		c.symbols = make(map[string]bool)
		for _, symbol := range loadChartReadingStockSymbols() {
			c.symbols[symbol] = true
		}
		c.mu.Unlock()

		go c.consumeCandleStream("1m", true)
		go c.consumeCandleStream("5s", false)
	})
}

func stockFilterHandler(w http.ResponseWriter, r *http.Request) {
	action := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/stock_filter/"), "/")
	if action == "" {
		http.Error(w, "stock filter action is required", http.StatusBadRequest)
		return
	}
	limit := parsePositiveInt(r.URL.Query().Get("limit"), 30)
	if limit > 100 {
		limit = 100
	}
	stockFilters.ensureStarted()
	rows := stockFilters.rows(action, limit)
	writeJSON(w, map[string]any{
		"action":     action,
		"count":      len(rows),
		"rows":       rows,
		"source":     "candle.raw.stock.1m + candle.raw.stock.5s",
		"updated_at": time.Now().In(istLocation()).Format(time.RFC3339),
	})
}

func (c *stockFilterCache) consumeCandleStream(timeframe string, historical bool) {
	subject := fmt.Sprintf("candle.raw.stock.%s.*", timeframe)
	for {
		if err := c.consumeCandleSubject(subject, timeframe, historical); err != nil {
			c.setStockFilterError(err)
			time.Sleep(2 * time.Second)
		}
	}
}

func (c *stockFilterCache) consumeCandleSubject(subject, timeframe string, historical bool) error {
	nc, err := nats.Connect(natsURL, nats.Name("trading_dashboard_stock_filter"), nats.Timeout(1200*time.Millisecond))
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	opts := []nats.SubOpt{nats.BindStream("CANDLE_RAW"), nats.AckNone()}
	if historical {
		opts = append(opts, nats.StartTime(stockFilterWarmupStart()))
	} else {
		opts = append(opts, nats.DeliverLastPerSubject())
	}
	sub, err := js.SubscribeSync(subject, opts...)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	_ = sub.SetPendingLimits(-1, -1)

	for {
		msg, err := sub.NextMsg(750 * time.Millisecond)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return err
		}
		c.applyStockFilterCandle(timeframe, msg.Subject, msg.Data)
	}
}

func (c *stockFilterCache) applyStockFilterCandle(timeframe, subject string, data []byte) {
	var candle chartReadingCandle
	if err := json.Unmarshal(data, &candle); err != nil {
		c.setStockFilterError(fmt.Errorf("decode %s: %w", timeframe, err))
		return
	}
	if candle.Open <= 0 || candle.High <= 0 || candle.Low <= 0 || candle.Close <= 0 {
		return
	}
	symbol := stockFilterSymbol(candle, subject)
	if symbol == "" || !c.tracksSymbol(symbol) {
		return
	}
	ts, err := parseMarketTime(candle.Time)
	if err != nil {
		c.setStockFilterError(fmt.Errorf("parse %s time %q: %w", symbol, candle.Time, err))
		return
	}
	if candle.Symbol == "" {
		candle.Symbol = symbol
	}
	if candle.Timeframe == "" {
		candle.Timeframe = timeframe
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	rec := c.records[symbol]
	if rec == nil {
		rec = &stockFilterRecord{Symbol: symbol, Candles1m: make(map[int64]chartReadingCandle)}
		c.records[symbol] = rec
	}
	if timeframe == "1m" {
		rec.Candles1m[ts.Unix()] = candle
		if rec.CurrentTime.IsZero() || !ts.Before(rec.CurrentTime) {
			rec.CurrentPrice = candle.Close
			rec.CurrentTime = ts
		}
		pruneStockFilterCandles(rec.Candles1m)
	} else if timeframe == "5s" && (rec.CurrentTime.IsZero() || !ts.Before(rec.CurrentTime)) {
		rec.CurrentPrice = candle.Close
		rec.CurrentTime = ts
	}
	c.lastUpdated = time.Now()
	c.lastError = ""
}

func (c *stockFilterCache) rows(action string, limit int) []stockFilterRow {
	views := c.snapshotStockFilterRecords()
	rows := make([]stockFilterRow, 0, len(views))
	for _, view := range views {
		row, ok := buildStockFilterRow(action, view)
		if ok {
			rows = append(rows, row)
		}
	}
	sortStockFilterResultRows(action, rows)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func (c *stockFilterCache) snapshotStockFilterRecords() []stockFilterRecordView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	views := make([]stockFilterRecordView, 0, len(c.records))
	for symbol, rec := range c.records {
		candles := make([]chartReadingCandle, 0, len(rec.Candles1m))
		for _, candle := range rec.Candles1m {
			candles = append(candles, candle)
		}
		views = append(views, stockFilterRecordView{
			Symbol:       symbol,
			Candles1m:    candles,
			CurrentPrice: rec.CurrentPrice,
			CurrentTime:  rec.CurrentTime,
		})
	}
	return views
}

func buildStockFilterRow(action string, view stockFilterRecordView) (stockFilterRow, bool) {
	points := sortedMarketCandles(view.Candles1m)
	if len(points) == 0 {
		return stockFilterRow{}, false
	}
	tradeDate := stockFilterTradeDate(view, points)
	if tradeDate == "" {
		return stockFilterRow{}, false
	}
	if tradeDate != time.Now().In(istLocation()).Format("2006-01-02") {
		return stockFilterRow{}, false
	}

	var previousClose float64
	today := make([]marketCandlePoint, 0, len(points))
	for _, point := range points {
		date := point.Time.Format("2006-01-02")
		if date < tradeDate {
			previousClose = point.Candle.Close
			continue
		}
		if date == tradeDate {
			today = append(today, point)
		}
	}
	if len(today) == 0 {
		return stockFilterRow{}, false
	}

	dayOpen := stockFilterDayOpen(today)
	latestPrice := view.CurrentPrice
	latestTime := view.CurrentTime
	if latestPrice <= 0 || latestTime.Format("2006-01-02") != tradeDate {
		latest := today[len(today)-1]
		latestPrice = latest.Candle.Close
		latestTime = latest.Time
	}
	row := stockFilterRow{
		Symbol:        view.Symbol,
		LTP:           round2(latestPrice),
		PreviousClose: round2(previousClose),
		DayOpen:       round2(dayOpen),
		Minute:        latestTime.In(istLocation()).Format("15:04:05"),
	}
	if previousClose > 0 {
		row.MovePct = round2(((latestPrice - previousClose) / previousClose) * 100)
	}
	if dayOpen > 0 {
		row.CandlePct = round2(((latestPrice - dayOpen) / dayOpen) * 100)
	}
	row.GreenStreak = trailingGreenCandles(today)

	switch action {
	case "high_gainers":
		return row, previousClose > 0 && row.MovePct > 0
	case "high_losers":
		return row, previousClose > 0 && row.MovePct < 0
	case "high_gainers_candle":
		return row, dayOpen > 0 && row.CandlePct > 0
	case "high_losers_candle":
		return row, dayOpen > 0 && row.CandlePct < 0
	case "three_green_1m":
		return row, row.GreenStreak >= 3
	default:
		return stockFilterRow{}, false
	}
}

type marketCandlePoint struct {
	Time   time.Time
	Candle chartReadingCandle
}

func sortedMarketCandles(candles []chartReadingCandle) []marketCandlePoint {
	points := make([]marketCandlePoint, 0, len(candles))
	for _, candle := range candles {
		ts, err := parseMarketTime(candle.Time)
		if err != nil || candle.Open <= 0 || candle.Close <= 0 {
			continue
		}
		points = append(points, marketCandlePoint{Time: ts, Candle: candle})
	}
	sort.SliceStable(points, func(i, j int) bool {
		return points[i].Time.Before(points[j].Time)
	})
	return points
}

func stockFilterTradeDate(view stockFilterRecordView, points []marketCandlePoint) string {
	if !view.CurrentTime.IsZero() && (len(points) == 0 || view.CurrentTime.After(points[len(points)-1].Time)) {
		return view.CurrentTime.In(istLocation()).Format("2006-01-02")
	}
	if len(points) == 0 {
		return ""
	}
	return points[len(points)-1].Time.In(istLocation()).Format("2006-01-02")
}

func stockFilterDayOpen(today []marketCandlePoint) float64 {
	for _, point := range today {
		hour, minute, second := point.Time.Clock()
		if hour > 9 || (hour == 9 && (minute > 15 || (minute == 15 && second >= 0))) {
			return point.Candle.Open
		}
	}
	return today[0].Candle.Open
}

func trailingGreenCandles(today []marketCandlePoint) int {
	streak := 0
	for i := len(today) - 1; i >= 0; i-- {
		candle := today[i].Candle
		if candle.Close <= candle.Open {
			break
		}
		streak++
	}
	return streak
}

func sortStockFilterResultRows(action string, rows []stockFilterRow) {
	switch action {
	case "high_losers":
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].MovePct < rows[j].MovePct })
	case "high_losers_candle":
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].CandlePct < rows[j].CandlePct })
	case "high_gainers_candle", "three_green_1m":
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].CandlePct > rows[j].CandlePct })
	default:
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].MovePct > rows[j].MovePct })
	}
}

func stockFilterSymbol(candle chartReadingCandle, subject string) string {
	if candle.Symbol != "" {
		return strings.ToUpper(strings.TrimSpace(candle.Symbol))
	}
	parts := strings.Split(subject, ".")
	if len(parts) == 0 {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(parts[len(parts)-1]))
}

func (c *stockFilterCache) tracksSymbol(symbol string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.symbols) == 0 || c.symbols[symbol]
}

func (c *stockFilterCache) setStockFilterError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastError = err.Error()
}

func pruneStockFilterCandles(candles map[int64]chartReadingCandle) {
	if len(candles) <= 2500 {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -7).Unix()
	for ts := range candles {
		if ts < cutoff {
			delete(candles, ts)
		}
	}
}

func stockFilterWarmupStart() time.Time {
	now := time.Now().In(istLocation())
	prev := now.AddDate(0, 0, -1)
	for prev.Weekday() == time.Saturday || prev.Weekday() == time.Sunday {
		prev = prev.AddDate(0, 0, -1)
	}
	return time.Date(prev.Year(), prev.Month(), prev.Day(), 15, 0, 0, 0, istLocation())
}

func parseMarketTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("empty time")
	}
	zonedLayouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range zonedLayouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.In(istLocation()), nil
		}
	}
	localLayouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05",
		"02/01/2006, 15:04:05",
		"02/01/2006 15:04:05",
	}
	for _, layout := range localLayouts {
		if ts, err := time.ParseInLocation(layout, value, istLocation()); err == nil {
			return ts.In(istLocation()), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %s", value)
}

func istLocation() *time.Location {
	return marketIST
}

func chartReadingSymbolsHandler(w http.ResponseWriter, r *http.Request) {
	mode := chartReadingMode(r.URL.Query().Get("mode"))
	var symbols []string
	if mode == "index" {
		symbols = append(symbols, chartReadingIndexes...)
	} else {
		symbols = loadChartReadingStockSymbols()
	}
	writeJSON(w, map[string]any{
		"mode":       mode,
		"symbols":    symbols,
		"timeframes": chartReadingTimeframes,
	})
}

func chartReadingCandlesHandler(w http.ResponseWriter, r *http.Request) {
	mode := chartReadingMode(r.URL.Query().Get("mode"))
	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
	timeframe := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("timeframe")))
	limit := parsePositiveInt(r.URL.Query().Get("limit"), 240)
	if limit > 800 {
		limit = 800
	}
	if symbol == "" {
		http.Error(w, "symbol is required", http.StatusBadRequest)
		return
	}
	if !validChartReadingTimeframe(timeframe) {
		http.Error(w, "invalid timeframe", http.StatusBadRequest)
		return
	}
	stream, subject := chartReadingStreamSubject(mode, timeframe, symbol)
	candles, err := fetchChartReadingCandles(r.Context(), stream, subject, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]any{
		"mode":      mode,
		"symbol":    symbol,
		"timeframe": timeframe,
		"subject":   subject,
		"candles":   candles,
		"count":     len(candles),
	})
}

func chartReadingMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "index", "indices":
		return "index"
	default:
		return "stock"
	}
}

func validChartReadingTimeframe(value string) bool {
	for _, tf := range chartReadingTimeframes {
		if value == tf {
			return true
		}
	}
	return false
}

func chartReadingStreamSubject(mode, timeframe, symbol string) (string, string) {
	if mode == "index" {
		return "INDICES_RAW", fmt.Sprintf("indices.raw.index.%s.%s", timeframe, symbol)
	}
	return "CANDLE_RAW", fmt.Sprintf("candle.raw.stock.%s.%s", timeframe, symbol)
}

func loadChartReadingStockSymbols() []string {
	paths := []string{
		"/Users/vikasbhandekar/live_services/go_feed/stocks/nifty500.csv",
		"/Users/vikasbhandekar/live_services/go_ltp/stocks/nifty500.csv",
		"/Users/vikasbhandekar/Desktop/TradingArchitecture/stocks/nifty500.csv",
		"/Users/vikasbhandekar/live_services/go_feed/stocks/nifty50.csv",
		"/Users/vikasbhandekar/Desktop/TradingArchitecture/stocks/nifty50.csv",
	}
	seen := make(map[string]bool)
	for _, path := range paths {
		for _, symbol := range readSymbolsCSV(path) {
			seen[symbol] = true
		}
		if len(seen) >= 500 {
			break
		}
	}
	out := make([]string, 0, len(seen))
	for symbol := range seen {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

func readSymbolsCSV(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(rows))
	symbolColumn := 0
	if len(rows) > 0 {
		for idx, header := range rows[0] {
			if strings.EqualFold(strings.TrimSpace(header), "symbol") {
				symbolColumn = idx
				break
			}
		}
	}
	for i, row := range rows {
		if len(row) <= symbolColumn {
			continue
		}
		candidate := strings.ToUpper(strings.TrimSpace(row[symbolColumn]))
		if candidate == "" || (i == 0 && strings.Contains(strings.ToLower(candidate), "symbol")) {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func fetchChartReadingCandles(ctx context.Context, stream, subject string, limit int) ([]chartReadingCandle, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 3500*time.Millisecond)
	defer cancel()

	nc, err := nats.Connect(natsURL, nats.Name("trading_dashboard_chart_reading"), nats.Timeout(1200*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.Context(requestCtx))
	if err != nil {
		return nil, err
	}
	sub, err := js.SubscribeSync(subject, nats.BindStream(stream), nats.DeliverAll(), nats.AckNone())
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	candles := make([]chartReadingCandle, 0, limit)
	for {
		msg, err := sub.NextMsg(120 * time.Millisecond)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
				break
			}
			return nil, err
		}
		var candle chartReadingCandle
		if err := json.Unmarshal(msg.Data, &candle); err != nil {
			continue
		}
		if candle.Open <= 0 || candle.High <= 0 || candle.Low <= 0 || candle.Close <= 0 {
			continue
		}
		candles = append(candles, candle)
		if len(candles) > limit {
			copy(candles, candles[len(candles)-limit:])
			candles = candles[:limit]
		}
	}
	sort.SliceStable(candles, func(i, j int) bool {
		return candles[i].Time < candles[j].Time
	})
	candles = dedupeChartReadingCandles(candles)
	return candles, nil
}

func dedupeChartReadingCandles(candles []chartReadingCandle) []chartReadingCandle {
	if len(candles) < 2 {
		return candles
	}
	out := candles[:0]
	var lastTime string
	for _, candle := range candles {
		if candle.Time == "" {
			continue
		}
		if candle.Time == lastTime && len(out) > 0 {
			out[len(out)-1] = candle
			continue
		}
		out = append(out, candle)
		lastTime = candle.Time
	}
	return out
}

func parsePositiveInt(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}

func shiftChartPayloadIST(body []byte) ([]byte, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, false, err
	}

	changed := false
	for _, key := range []string{"candles", "markers"} {
		rows, ok := payload[key].([]any)
		if !ok {
			continue
		}
		for _, row := range rows {
			obj, ok := row.(map[string]any)
			if !ok {
				continue
			}
			rawTime, ok := obj["time"]
			if !ok {
				continue
			}
			next, did := shiftTimeValue(rawTime)
			if did {
				obj["time"] = next
				changed = true
			}
		}
	}

	if !changed {
		return nil, false, nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	return encoded, true, nil
}

func shiftTimeValue(v any) (any, bool) {
	switch t := v.(type) {
	case string:
		ts, err := parseTimeFlexible(t)
		if err != nil {
			return nil, false
		}
		return ts.Add(time.Duration(istOffsetSeconds) * time.Second).UTC().Format(time.RFC3339), true
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return nil, false
		}
		return f + float64(istOffsetSeconds), true
	case float64:
		return t + float64(istOffsetSeconds), true
	case int64:
		return t + istOffsetSeconds, true
	case int:
		return t + istOffsetSeconds, true
	default:
		return nil, false
	}
}

func parseTimeFlexible(v string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05",
	}
	value := strings.TrimSpace(v)
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
}
