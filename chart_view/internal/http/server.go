package http

import (
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"chart_view/internal/config"
)

type Server struct {
	cfg    *config.Config
	client *nethttp.Client
}

type candle struct {
	Time   string  `json:"time"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume,omitempty"`
}

type indicatorPoint struct {
	Time  string  `json:"time"`
	Value float64 `json:"value"`
}

func New(cfg *config.Config) *Server {
	timeout := time.Duration(cfg.RequestTimeoutSec) * time.Second
	return &Server{
		cfg: cfg,
		client: &nethttp.Client{
			Timeout: timeout,
		},
	}
}

func (s *Server) Routes() nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/dashboard", s.handleDashboard)
	mux.HandleFunc("/chart/candles", s.handleChartCandles)
	mux.HandleFunc("/option/candles", s.handleOptionCandles)
	return mux
}

func (s *Server) handleHealth(w nethttp.ResponseWriter, _ *nethttp.Request) {
	writeJSON(w, nethttp.StatusOK, map[string]any{
		"status":  "ok",
		"service": s.cfg.ServiceName,
	})
}

func (s *Server) handleDashboard(w nethttp.ResponseWriter, _ *nethttp.Request) {
	status := map[string]any{
		"service": s.cfg.ServiceName,
		"engines": map[string]any{},
	}
	for name, base := range s.cfg.Engines {
		engineURL := buildTargetURL(base, "/health", nil)
		ok, body := s.pingEngine(engineURL)
		status["engines"].(map[string]any)[name] = map[string]any{
			"url":    base,
			"ok":     ok,
			"health": body,
		}
	}
	writeJSON(w, nethttp.StatusOK, status)
}

func (s *Server) handleChartCandles(w nethttp.ResponseWriter, r *nethttp.Request) {
	engineID := strings.TrimSpace(r.URL.Query().Get("engine"))
	if engineID == "" {
		engineID = "paper_engine"
	}
	base, ok := s.cfg.Engines[engineID]
	if !ok {
		writeJSONError(w, nethttp.StatusBadRequest, "unknown engine")
		return
	}
	target := buildTargetURL(base, "/candles", r.URL.Query())
	var payload any
	if err := s.fetchJSON(target, &payload); err != nil {
		writeJSONError(w, nethttp.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, nethttp.StatusOK, payload)
}

func (s *Server) handleOptionCandles(w nethttp.ResponseWriter, r *nethttp.Request) {
	target := buildTargetURL(s.cfg.OptionFeedBaseURL, "/candles", r.URL.Query())
	var candles []candle
	if err := s.fetchJSON(target, &candles); err != nil {
		writeJSONError(w, nethttp.StatusBadGateway, err.Error())
		return
	}

	period := parseEMAQuery(r.URL.Query().Get("ema"))
	if period <= 1 {
		writeJSON(w, nethttp.StatusOK, map[string]any{
			"candles": candles,
		})
		return
	}

	writeJSON(w, nethttp.StatusOK, map[string]any{
		"candles": candles,
		"ema":     emaSeries(candles, period),
	})
}

func (s *Server) pingEngine(target string) (bool, string) {
	req, err := nethttp.NewRequest(nethttp.MethodGet, target, nil)
	if err != nil {
		return false, err.Error()
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	var body any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return resp.StatusCode < 500, resp.Status
	}
	encoded, _ := json.Marshal(body)
	return resp.StatusCode < 500, string(encoded)
}

func (s *Server) fetchJSON(target string, dest any) error {
	req, err := nethttp.NewRequest(nethttp.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("upstream status %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func buildTargetURL(base, path string, query url.Values) string {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil {
		return base + path
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	if query != nil {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func parseEMAQuery(raw string) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func emaSeries(candles []candle, period int) []indicatorPoint {
	if period <= 1 || len(candles) == 0 {
		return nil
	}
	multiplier := 2.0 / float64(period+1)
	result := make([]indicatorPoint, 0, len(candles))
	ema := candles[0].Close
	for _, c := range candles {
		ema = ((c.Close - ema) * multiplier) + ema
		result = append(result, indicatorPoint{
			Time:  c.Time,
			Value: round2(ema),
		})
	}
	return result
}

func round2(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}

func writeJSON(w nethttp.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w nethttp.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"ok":    false,
		"error": message,
	})
}

