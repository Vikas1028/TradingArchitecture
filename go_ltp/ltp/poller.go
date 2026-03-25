package ltp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go_ltp/common"
	appConfig "go_ltp/config"
	appKafka "go_ltp/kafka"
	appLogger "go_ltp/logger"
	"go_ltp/monitor"
)

const (
	dhanLTPURL       = "https://api.dhan.co/v2/marketfeed/ltp"
	pollInterval     = 1 * time.Second
	rateLimitBackoff = 5 * time.Second
	requestTimeout   = 5 * time.Second
	nseEquitySegment = "NSE_EQ"
)

var (
	httpClient      = &http.Client{Timeout: requestTimeout}
	subscriptionSet []common.TokenInfo
	requestBody     []byte
	marketSchedule  marketWindow
)

// ltpResponse matches the broker snapshot response envelope.
type ltpResponse struct {
	Data map[string]map[string]ltpValue `json:"data"`
}

// ltpValue holds the only field currently consumed from one LTP snapshot entry.
type ltpValue struct {
	LastPrice float64 `json:"last_price"`
}

// marketWindow stores the configured market-hour gate for the poller.
type marketWindow struct {
	enabled     bool
	startMinute int
	endMinute   int
	startLabel  string
	endLabel    string
}

// pollLoopCounters stores running totals emitted by the periodic poller reporter.
type pollLoopCounters struct {
	pollAttempts  atomic.Uint64
	pollErrors    atomic.Uint64
	snapshotsSeen atomic.Uint64
	ticksReceived atomic.Uint64
}

// Initialize prepares the static LTP request payload for the configured token group.
func Initialize() error {
	if appConfig.GlobalConfig == nil {
		return fmt.Errorf("application config is not initialized")
	}

	tokenSet, err := common.LoadSubscriptionTokens()
	if err != nil {
		return err
	}
	tokens := common.GetTokensForGroup(appConfig.GlobalConfig.Subscription.Group, tokenSet)
	if len(tokens) == 0 {
		return fmt.Errorf("no tokens resolved for group %s", appConfig.GlobalConfig.Subscription.Group)
	}

	securityIDs := make([]int64, 0, len(tokens))
	for _, token := range tokens {
		if token.Token == "" {
			continue
		}
		securityID, err := strconv.ParseInt(token.Token, 10, 64)
		if err != nil {
			appLogger.Warnf("skipping token=%s symbol=%s because security id is not numeric", token.Token, token.Symbol)
			continue
		}
		securityIDs = append(securityIDs, securityID)
	}
	if len(securityIDs) == 0 {
		return fmt.Errorf("no security ids resolved for group %s", appConfig.GlobalConfig.Subscription.Group)
	}

	body, err := json.Marshal(map[string][]int64{
		nseEquitySegment: securityIDs,
	})
	if err != nil {
		return fmt.Errorf("marshal LTP request body: %w", err)
	}

	schedule, err := buildMarketWindow(appConfig.GlobalConfig.LTP)
	if err != nil {
		return err
	}

	subscriptionSet = append([]common.TokenInfo(nil), tokens...)
	requestBody = body
	marketSchedule = schedule
	monitor.SetSubscriptions(tokens)
	monitor.SetClientLoggedIn(true)
	appLogger.Infof(
		"prepared LTP polling request for group=%s tokens=%d market_hours_only=%t market_start=%s market_end=%s",
		appConfig.GlobalConfig.Subscription.Group,
		len(tokens),
		marketSchedule.enabled,
		marketSchedule.startLabel,
		marketSchedule.endLabel,
	)
	return nil
}

// StartPollLoop calls the Dhan LTP snapshot endpoint once per second and publishes the result to Kafka.
func StartPollLoop(ctx context.Context) {
	if len(subscriptionSet) == 0 || len(requestBody) == 0 {
		appLogger.Errorf("LTP polling loop cannot start because initialization is incomplete")
		return
	}

	tokenSymbolMap := make(map[string]string, len(subscriptionSet))
	for _, token := range subscriptionSet {
		tokenSymbolMap[token.Token] = token.Symbol
	}

	var counters pollLoopCounters
	reporterStop := make(chan struct{})
	go startPollCountReporter(&counters, reporterStop)
	defer close(reporterStop)

	appLogger.Infof("LTP polling loop started tokens=%d market_hours_only=%t", len(tokenSymbolMap), marketSchedule.enabled)
	outsideMarketLogged := false
	for {
		if ctx.Err() != nil {
			appLogger.Infof("LTP polling loop stopped because context was cancelled")
			return
		}

		now := time.Now()
		if !shouldPollAt(now) {
			delay := nextPollDelay(now)
			if !outsideMarketLogged {
				nextOpen := now.Add(delay)
				appLogger.Infof(
					"LTP polling paused because current time %s is outside market window %s-%s; next poll attempt at %s",
					now.Format(time.RFC3339),
					marketSchedule.startLabel,
					marketSchedule.endLabel,
					nextOpen.Format(time.RFC3339),
				)
				outsideMarketLogged = true
			}
			if !waitForDelay(ctx, delay) {
				appLogger.Infof("LTP polling loop stopped because context was cancelled while waiting for market hours")
				return
			}
			continue
		}
		if outsideMarketLogged {
			appLogger.Infof("LTP polling resumed because current time is inside the configured market window")
			outsideMarketLogged = false
		}

		delay := pollInterval
		counters.pollAttempts.Add(1)
		if err := pollAndPublish(ctx, tokenSymbolMap, &counters); err != nil {
			counters.pollErrors.Add(1)
			appLogger.Errorf("LTP poll failed: %v", err)
			monitor.RecordError("ltp_poll")
			if strings.Contains(err.Error(), "429 Too Many Requests") {
				delay = rateLimitBackoff
				appLogger.Warnf("LTP poll hit rate limit; applying backoff=%s", delay)
			}
		}

		if !waitForDelay(ctx, delay) {
			appLogger.Infof("LTP polling loop stopped because context was cancelled")
			return
		}
	}
}

// pollAndPublish fetches one LTP snapshot and forwards each quote to Kafka and runtime metrics.
func pollAndPublish(ctx context.Context, tokenSymbolMap map[string]string, counters *pollLoopCounters) error {
	if appConfig.GlobalConfig == nil {
		return fmt.Errorf("application config is not initialized")
	}
	appLogger.Debugf("starting LTP snapshot request for %d configured tokens", len(tokenSymbolMap))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dhanLTPURL, bytes.NewReader(requestBody))
	if err != nil {
		appLogger.Errorf("failed to create LTP request: %v", err)
		return fmt.Errorf("create LTP request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access-token", appConfig.GlobalConfig.DhanClient.AccessToken)
	req.Header.Set("client-id", appConfig.GlobalConfig.DhanClient.ClientID)

	resp, err := httpClient.Do(req)
	if err != nil {
		appLogger.Errorf("LTP HTTP request execution failed: %v", err)
		return fmt.Errorf("execute LTP request: %w", err)
	}
	defer resp.Body.Close()
	appLogger.Debugf("received LTP HTTP response status=%s", resp.Status)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		appLogger.Errorf("LTP HTTP response returned non-OK status=%s body=%s", resp.Status, string(body))
		return fmt.Errorf("unexpected LTP response status=%s body=%s", resp.Status, string(body))
	}

	var payload ltpResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		appLogger.Errorf("failed to decode LTP response payload: %v", err)
		return fmt.Errorf("decode LTP response: %w", err)
	}

	segmentData := payload.Data[nseEquitySegment]
	if len(segmentData) == 0 {
		appLogger.Errorf("LTP response did not contain any %s data", nseEquitySegment)
		return fmt.Errorf("LTP response contained no %s data", nseEquitySegment)
	}

	now := time.Now()
	publishedCount := 0
	for securityID, quote := range segmentData {
		symbol := tokenSymbolMap[securityID]
		if symbol == "" {
			appLogger.Warnf("symbol lookup missing for security_id=%s; using security id as symbol in downstream payload", securityID)
			symbol = securityID
		}
		message := common.KafkaTickMessage{
			SecurityID:      securityID,
			Symbol:          symbol,
			ExchangeSegment: 0,
			Timestamp:       now,
			LastTradedPrice: float32(quote.LastPrice),
		}
		if err := appKafka.PublishMessage(ctx, securityID, message); err != nil {
			appLogger.Errorf("failed to publish LTP tick security_id=%s symbol=%s price=%f: %v", securityID, symbol, quote.LastPrice, err)
			return fmt.Errorf("publish LTP tick for %s: %w", securityID, err)
		}
		appLogger.Debugf("published LTP tick security_id=%s symbol=%s price=%f", securityID, symbol, quote.LastPrice)
		monitor.RecordTick(symbol, now)
		publishedCount++
	}
	counters.snapshotsSeen.Add(1)
	counters.ticksReceived.Add(uint64(publishedCount))
	appLogger.Debugf("completed LTP snapshot publish snapshot_symbols=%d published_ticks=%d", len(segmentData), publishedCount)

	return nil
}

// buildMarketWindow validates and normalizes the configured market-hour gate.
func buildMarketWindow(cfg common.LTPConfig) (marketWindow, error) {
	startMinute, err := parseClockMinute(cfg.MarketStartTime)
	if err != nil {
		return marketWindow{}, fmt.Errorf("parse ltp market_start_time: %w", err)
	}
	endMinute, err := parseClockMinute(cfg.MarketEndTime)
	if err != nil {
		return marketWindow{}, fmt.Errorf("parse ltp market_end_time: %w", err)
	}
	if startMinute >= endMinute {
		return marketWindow{}, fmt.Errorf("ltp market window is invalid start=%s end=%s", cfg.MarketStartTime, cfg.MarketEndTime)
	}

	return marketWindow{
		enabled:     cfg.FetchOnlyDuringMarketHours,
		startMinute: startMinute,
		endMinute:   endMinute,
		startLabel:  cfg.MarketStartTime,
		endLabel:    cfg.MarketEndTime,
	}, nil
}

// parseClockMinute converts an HH:MM string into minutes since midnight.
func parseClockMinute(value string) (int, error) {
	parsed, err := time.Parse("15:04", strings.TrimSpace(value))
	if err != nil {
		return 0, err
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

// shouldPollAt reports whether the current local time is inside the configured trading window.
func shouldPollAt(now time.Time) bool {
	if !marketSchedule.enabled {
		return true
	}
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	currentMinute := now.Hour()*60 + now.Minute()
	return currentMinute >= marketSchedule.startMinute && currentMinute <= marketSchedule.endMinute
}

// nextPollDelay returns the sleep duration until the next allowed market-hour polling time.
func nextPollDelay(now time.Time) time.Duration {
	if !marketSchedule.enabled {
		return pollInterval
	}

	nextOpen := nextMarketOpen(now)
	delay := time.Until(nextOpen)
	if delay <= 0 {
		return pollInterval
	}
	return delay
}

// nextMarketOpen computes the next local market-open timestamp for the current configuration.
func nextMarketOpen(now time.Time) time.Time {
	candidate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	candidate = candidate.Add(time.Duration(marketSchedule.startMinute) * time.Minute)

	switch now.Weekday() {
	case time.Saturday:
		return candidate.AddDate(0, 0, 2)
	case time.Sunday:
		return candidate.AddDate(0, 0, 1)
	}

	currentMinute := now.Hour()*60 + now.Minute()
	if currentMinute < marketSchedule.startMinute {
		return candidate
	}
	return nextWeekday(candidate.AddDate(0, 0, 1))
}

// nextWeekday advances a timestamp to the next non-weekend date while keeping its clock time.
func nextWeekday(candidate time.Time) time.Time {
	for candidate.Weekday() == time.Saturday || candidate.Weekday() == time.Sunday {
		candidate = candidate.AddDate(0, 0, 1)
	}
	return candidate
}

// waitForDelay blocks for one poll interval or until cancellation, whichever happens first.
func waitForDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// startPollCountReporter writes periodic progress logs with cumulative tick and error totals.
func startPollCountReporter(counters *pollLoopCounters, stop <-chan struct{}) {
	interval := 10 * time.Second
	if appConfig.GlobalConfig != nil && appConfig.GlobalConfig.LTP.CountLogIntervalSec > 0 {
		interval = time.Duration(appConfig.GlobalConfig.LTP.CountLogIntervalSec) * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastTickTotal := uint64(0)
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			totalTicks := counters.ticksReceived.Load()
			appLogger.Infof(
				"LTP count report total_received=%d received_since_last_report=%d total_polls=%d total_poll_errors=%d total_snapshots=%d",
				totalTicks,
				totalTicks-lastTickTotal,
				counters.pollAttempts.Load(),
				counters.pollErrors.Load(),
				counters.snapshotsSeen.Load(),
			)
			lastTickTotal = totalTicks
		}
	}
}
