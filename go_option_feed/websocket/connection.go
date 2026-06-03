package websocket

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	gorillawebsocket "github.com/gorilla/websocket"
	"go_option_feed/common"
	appConfig "go_option_feed/config"
	"go_option_feed/feed"
	appKafka "go_option_feed/kafka"
	appLogger "go_option_feed/logger"
	"go_option_feed/monitor"
	appPostgres "go_option_feed/postgres"
)

var (
	// GlobalConnection stores the live Dhan websocket connection for reuse across the application.
	GlobalConnection   *gorillawebsocket.Conn
	subscriptionTokens []common.TokenInfo
	tokenSymbolMap     map[string]string
	stateMu            sync.RWMutex
	reconnectMu        sync.Mutex
	writeMu            sync.Mutex
	lastReconnectAt    time.Time
	heartbeatStop      chan struct{}
)

type inboundTickMessage struct {
	payload []byte
}

type readLoopCounters struct {
	readCalls      atomic.Uint64
	readErrors     atomic.Uint64
	channelEnqueue atomic.Uint64
}

// Connection opens the authenticated Dhan websocket connection after login succeeds.
func Connection() error {
	appLogger.Debugf("starting websocket connection flow")
	return connectWithRetry()
}

// Reconnect closes any stale websocket and creates a new authenticated websocket connection.
func Reconnect() error {
	reconnectMu.Lock()
	defer reconnectMu.Unlock()

	monitor.RecordWebsocketReconnect()
	appLogger.Warnf("websocket reconnect triggered")
	throttleReconnect()

	if GlobalConnection != nil {
		stopHeartbeatLoop()
		_ = GlobalConnection.Close()
		GlobalConnection = nil
	}
	monitor.SetWebsocketConnected(false)

	if err := connectWithRetry(); err != nil {
		appLogger.Warnf("websocket reconnect failed with existing session, retrying client login: %v", err)
		monitor.SetClientLoggedIn(false)
		if loginErr := ClientLogin(); loginErr != nil {
			appLogger.Errorf("client re-login failed during websocket reconnect: %v", loginErr)
			return loginErr
		}
		if err := connectWithRetry(); err != nil {
			appLogger.Errorf("websocket reconnect failed after client re-login: %v", err)
			return err
		}
	}

	if err := Subscribe(); err != nil {
		appLogger.Errorf("websocket resubscribe failed after reconnect: %v", err)
		return err
	}
	appLogger.Infof("websocket reconnect completed successfully")
	return nil
}

// Subscribe sends Dhan subscription messages for the configured startup token group.
func Subscribe() error {
	if GlobalConnection == nil {
		err := fmt.Errorf("websocket connection is not initialized")
		appLogger.Errorf("subscription failed: %v", err)
		monitor.RecordError("websocket")
		return err
	}
	if appConfig.GlobalConfig == nil {
		err := fmt.Errorf("application config is not initialized")
		appLogger.Errorf("subscription failed: %v", err)
		monitor.RecordError("websocket")
		return err
	}

	tokenSet, err := common.LoadSubscriptionTokens()
	if err != nil {
		appLogger.Errorf("failed to load subscription tokens: %v", err)
		monitor.RecordError("websocket")
		return err
	}
	tokens := common.GetTokensForGroup(appConfig.GlobalConfig.Subscription.Group, tokenSet)
	if len(tokens) == 0 {
		err := fmt.Errorf("no tokens resolved for group %s", appConfig.GlobalConfig.Subscription.Group)
		appLogger.Warnf("subscription skipped because no tokens were resolved: %v", err)
		monitor.RecordError("websocket")
		return err
	}

	stateMu.Lock()
	subscriptionTokens = cloneTokens(tokens)
	tokenSymbolMap = common.BuildTokenSymbolMap(tokenSet)
	stateMu.Unlock()
	monitor.SetSubscriptions(tokens)
	appLogger.Infof("resolved %d subscription tokens for group=%s", len(tokens), appConfig.GlobalConfig.Subscription.Group)

	batchSize := appConfig.GlobalConfig.DhanClient.BatchSize
	if batchSize <= 0 {
		batchSize = 100
		appLogger.Debugf("subscription batch size was invalid, using default=%d", batchSize)
	}

	for start := 0; start < len(tokens); start += batchSize {
		end := start + batchSize
		if end > len(tokens) {
			end = len(tokens)
		}

		request := map[string]any{
			"RequestCode":     appConfig.GlobalConfig.Subscription.RequestCode,
			"InstrumentCount": end - start,
			"InstrumentList":  buildInstrumentList(tokens[start:end], appConfig.GlobalConfig.DhanClient.ExchangeSegment),
		}
		appLogger.Debugf("sending websocket subscription batch start=%d end=%d count=%d", start, end, end-start)
		if err := writeJSON(request); err != nil {
			appLogger.Errorf("failed to send websocket subscription batch start=%d end=%d: %v", start, end, err)
			monitor.RecordError("websocket")
			return err
		}
	}

	appLogger.Infof("websocket subscription completed successfully")
	return nil
}

// StartReadLoop continuously reads websocket messages and republishes them to Kafka and PostgreSQL.
func StartReadLoop(ctx context.Context) {
	appLogger.Infof("websocket read loop started")

	inboundQueue := make(chan inboundTickMessage, inboundQueueSize())
	var processorGroup sync.WaitGroup
	var counters readLoopCounters
	reporterStop := make(chan struct{})
	for workerIndex := 0; workerIndex < inboundProcessorCount(); workerIndex++ {
		processorGroup.Add(1)
		go startInboundProcessor(ctx, inboundQueue, &processorGroup)
	}
	go startReadLoopReporter(&counters, reporterStop)
	defer func() {
		close(reporterStop)
		close(inboundQueue)
		processorGroup.Wait()
	}()

	for {
		if GlobalConnection == nil {
			appLogger.Warnf("websocket connection is nil inside read loop, attempting reconnect")
			if err := Reconnect(); err != nil {
				appLogger.Errorf("websocket reconnect from read loop failed: %v", err)
				monitor.RecordError("websocket")
				time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
				continue
			}
		}

		counters.readCalls.Add(1)
		messageType, payload, err := GlobalConnection.ReadMessage()
		if err != nil {
			counters.readErrors.Add(1)
			logReadFailure(err)
			monitor.RecordError("websocket")
			if ctx.Err() != nil {
				appLogger.Infof("websocket read loop stopping after read failure because context was cancelled")
				return
			}
			if reconnectErr := Reconnect(); reconnectErr != nil {
				time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
			}
			continue
		}
		refreshReadDeadline(GlobalConnection)
		if messageType != common.WebsocketMessageTypeBinary {
			appLogger.Debugf("ignored websocket message with unsupported type=%d", messageType)
			continue
		}

		select {
		case inboundQueue <- inboundTickMessage{payload: append([]byte(nil), payload...)}:
			counters.channelEnqueue.Add(1)
		case <-ctx.Done():
			appLogger.Infof("websocket read loop stopped while queuing inbound tick because context was cancelled")
			return
		}
	}
}

// Close releases the active websocket connection during shutdown.
func Close() error {
	if GlobalConnection == nil {
		appLogger.Debugf("websocket close skipped because connection is not initialized")
		return nil
	}
	stopHeartbeatLoop()
	err := GlobalConnection.Close()
	GlobalConnection = nil
	monitor.SetWebsocketConnected(false)
	if err != nil {
		appLogger.Errorf("failed to close websocket connection: %v", err)
		monitor.RecordError("websocket")
		return err
	}
	appLogger.Infof("websocket connection closed successfully")
	return nil
}

func connectWithRetry() error {
	if GlobalSession == nil {
		err := fmt.Errorf("websocket session is not initialized")
		appLogger.Errorf("websocket connection failed: %v", err)
		monitor.RecordError("websocket")
		return err
	}

	maxAttempts := common.DefaultRetryAttempts
	retryIntervalSec := common.DefaultRetryIntervalSec

	if appConfig.GlobalConfig != nil {
		if appConfig.GlobalConfig.DhanWebsocket.MaxReconnectAttempts > 0 {
			maxAttempts = appConfig.GlobalConfig.DhanWebsocket.MaxReconnectAttempts
		}
		if appConfig.GlobalConfig.DhanWebsocket.ReconnectIntervalSec > 0 {
			retryIntervalSec = appConfig.GlobalConfig.DhanWebsocket.ReconnectIntervalSec
		}
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		appLogger.Debugf("attempting websocket dial attempt=%d/%d", attempt, maxAttempts)
		dialer := *gorillawebsocket.DefaultDialer
		dialer.HandshakeTimeout = 15 * time.Second
		conn, resp, err := dialer.Dial(GlobalSession.FeedURL, http.Header{})
		if err == nil {
			refreshReadDeadline(conn)
			conn.SetPingHandler(func(appData string) error {
				refreshReadDeadline(conn)
				appLogger.Debugf("received websocket ping frame payload_size=%d", len(appData))
				return writeControl(conn, gorillawebsocket.PongMessage, []byte(appData))
			})
			conn.SetPongHandler(func(appData string) error {
				refreshReadDeadline(conn)
				appLogger.Debugf("received websocket pong frame payload_size=%d", len(appData))
				return nil
			})
			conn.SetCloseHandler(func(code int, text string) error {
				appLogger.Warnf("websocket close frame received from broker code=%d text=%s", code, text)
				return nil
			})
			GlobalConnection = conn
			startHeartbeatLoop(conn)
			monitor.SetWebsocketConnected(true)
			appLogger.Infof("websocket connection established successfully")
			return nil
		}
		lastErr = err
		logDialFailure(attempt, maxAttempts, err, resp)
		monitor.RecordError("websocket")
		time.Sleep(backoffDuration(retryIntervalSec, attempt))
	}
	appLogger.Fatalf("websocket connection failed after %d attempts: %v", maxAttempts, lastErr)
	return lastErr
}

func buildInstrumentList(tokens []common.TokenInfo, exchangeSegment string) []map[string]string {
	instruments := make([]map[string]string, 0, len(tokens))
	for _, tokenInfo := range tokens {
		appLogger.Debugf("building websocket instrument entry for symbol=%s token=%s", tokenInfo.Symbol, tokenInfo.Token)
		instruments = append(instruments, map[string]string{
			"ExchangeSegment": exchangeSegment,
			"SecurityId":      tokenInfo.Token,
		})
	}
	return instruments
}

func extractMessageKey(payload []byte) string {
	if len(payload) < 8 {
		appLogger.Debugf("websocket payload too small to extract security id payload_size=%d", len(payload))
		return "unknown"
	}
	securityID := binary.LittleEndian.Uint32(payload[4:8])
	return fmt.Sprintf("%d", securityID)
}

func currentTokenSymbolMap() map[string]string {
	stateMu.RLock()
	defer stateMu.RUnlock()
	cloned := make(map[string]string, len(tokenSymbolMap))
	for key, value := range tokenSymbolMap {
		cloned[key] = value
	}
	return cloned
}

func cloneTokens(values []common.TokenInfo) []common.TokenInfo {
	cloned := make([]common.TokenInfo, len(values))
	copy(cloned, values)
	return cloned
}

func throttleReconnect() {
	retryIntervalSec := common.DefaultRetryIntervalSec
	if appConfig.GlobalConfig != nil && appConfig.GlobalConfig.DhanWebsocket.ReconnectIntervalSec > 0 {
		retryIntervalSec = appConfig.GlobalConfig.DhanWebsocket.ReconnectIntervalSec
	}

	stateMu.Lock()
	defer stateMu.Unlock()

	if !lastReconnectAt.IsZero() {
		elapsed := time.Since(lastReconnectAt)
		minGap := time.Duration(retryIntervalSec) * time.Second
		if elapsed < minGap {
			waitFor := minGap - elapsed
			appLogger.Warnf("throttling websocket reconnect for %s to avoid broker handshake rejection", waitFor.Round(time.Millisecond))
			time.Sleep(waitFor)
		}
	}
	lastReconnectAt = time.Now()
}

func refreshReadDeadline(conn *gorillawebsocket.Conn) {
	if conn == nil || appConfig.GlobalConfig == nil || appConfig.GlobalConfig.DhanWebsocket.ReadTimeoutSec <= 0 {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Duration(appConfig.GlobalConfig.DhanWebsocket.ReadTimeoutSec) * time.Second))
}

func startHeartbeatLoop(conn *gorillawebsocket.Conn) {
	if conn == nil || appConfig.GlobalConfig == nil || appConfig.GlobalConfig.DhanWebsocket.PingIntervalSec <= 0 {
		return
	}

	stopHeartbeatLoop()

	stopCh := make(chan struct{})
	stateMu.Lock()
	heartbeatStop = stopCh
	stateMu.Unlock()

	interval := time.Duration(appConfig.GlobalConfig.DhanWebsocket.PingIntervalSec) * time.Second
	go func(localConn *gorillawebsocket.Conn, localStop chan struct{}) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stateMu.RLock()
				currentConn := GlobalConnection
				stateMu.RUnlock()
				if currentConn != localConn {
					return
				}
				if err := writeControl(localConn, gorillawebsocket.PingMessage, nil); err != nil {
					appLogger.Warnf("websocket heartbeat ping failed: %v", err)
					return
				}
			case <-localStop:
				return
			}
		}
	}(conn, stopCh)
}

func stopHeartbeatLoop() {
	stateMu.Lock()
	stopCh := heartbeatStop
	heartbeatStop = nil
	stateMu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
}

func writeJSON(payload any) error {
	if GlobalConnection == nil {
		return fmt.Errorf("websocket connection is not initialized")
	}

	writeMu.Lock()
	defer writeMu.Unlock()

	if timeout := writeTimeout(); timeout > 0 {
		_ = GlobalConnection.SetWriteDeadline(time.Now().Add(timeout))
		defer func() { _ = GlobalConnection.SetWriteDeadline(time.Time{}) }()
	}

	return GlobalConnection.WriteJSON(payload)
}

func writeControl(conn *gorillawebsocket.Conn, messageType int, data []byte) error {
	if conn == nil {
		return fmt.Errorf("websocket connection is not initialized")
	}

	writeMu.Lock()
	defer writeMu.Unlock()

	deadline := time.Now().Add(5 * time.Second)
	if timeout := writeTimeout(); timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	return conn.WriteControl(messageType, data, deadline)
}

func writeTimeout() time.Duration {
	if appConfig.GlobalConfig == nil || appConfig.GlobalConfig.DhanWebsocket.WriteTimeoutSec <= 0 {
		return 0
	}
	return time.Duration(appConfig.GlobalConfig.DhanWebsocket.WriteTimeoutSec) * time.Second
}

func backoffDuration(baseSec, attempt int) time.Duration {
	if baseSec <= 0 {
		baseSec = common.DefaultRetryIntervalSec
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Duration(baseSec*attempt) * time.Second
	maxDelay := 30 * time.Second
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func logDialFailure(attempt, maxAttempts int, err error, resp *http.Response) {
	if resp == nil {
		appLogger.Errorf("websocket dial failed attempt=%d/%d: %v", attempt, maxAttempts, err)
		return
	}

	bodyPreview := ""
	if resp.Body != nil {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr != nil {
			bodyPreview = fmt.Sprintf("unable to read handshake body: %v", readErr)
		} else {
			bodyPreview = string(body)
		}
	}

	appLogger.Errorf(
		"websocket dial failed attempt=%d/%d status=%s body=%q err=%v",
		attempt,
		maxAttempts,
		resp.Status,
		bodyPreview,
		err,
	)
}

func logReadFailure(err error) {
	if closeErr, ok := err.(*gorillawebsocket.CloseError); ok {
		appLogger.Errorf(
			"websocket connection broke while reading message code=%d text=%s: %v",
			closeErr.Code,
			closeErr.Text,
			err,
		)
		return
	}
	appLogger.Errorf("websocket connection broke while reading message: %v", err)
}

func startInboundProcessor(ctx context.Context, inboundQueue <-chan inboundTickMessage, processorGroup *sync.WaitGroup) {
	defer processorGroup.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-inboundQueue:
			if !ok {
				return
			}
			processInboundMessage(ctx, message.payload)
		}
	}
}

func processInboundMessage(ctx context.Context, payload []byte) {
	if len(payload) == 0 {
		appLogger.Debugf("ignored websocket packet with empty payload")
		return
	}

	responseCode := payload[0]
	if responseCode == common.DhanFeedCodeDisconnect {
		appLogger.Warnf("received websocket disconnect packet, forcing client re-login")
		monitor.SetClientLoggedIn(false)
		if err := ClientLogin(); err != nil {
			appLogger.Errorf("client re-login failed after disconnect packet: %v", err)
			monitor.RecordError("client_login")
		}
		if err := Reconnect(); err != nil {
			appLogger.Errorf("websocket reconnect failed after disconnect packet: %v", err)
		}
		return
	}

	if responseCode != common.DhanFeedCodeQuote && responseCode != common.DhanFeedCodeFull {
		appLogger.Debugf("ignored websocket packet with response_code=%d", responseCode)
		return
	}

	tick, err := feed.ParseTick(payload, currentTokenSymbolMap())
	if err != nil {
		appLogger.Warnf("failed to parse websocket payload for security_id=%s: %v", extractMessageKey(payload), err)
		monitor.RecordError("parser")
		return
	}

	monitor.RecordTick(tick.Symbol, tick.FeedTimestamp)

	smallPacket := feed.ToKafkaMessage(tick)
	if err := appKafka.PublishMessage(ctx, tick.SecurityID, smallPacket); err != nil {
		appLogger.Warnf("websocket message for key=%s was not inserted into kafka immediately: %v", tick.SecurityID, err)
	}
	if err := appPostgres.EnqueueTick(ctx, *tick); err != nil {
		appLogger.Warnf("websocket message for key=%s was not inserted into postgres immediately: %v", tick.SecurityID, err)
		monitor.RecordError("postgres")
	}

	appLogger.Debugf("websocket binary message processed successfully for key=%s payload_size=%d", tick.SecurityID, len(payload))
}

func inboundProcessorCount() int {
	if appConfig.GlobalConfig == nil {
		return 2
	}

	processorCount := appConfig.GlobalConfig.Pipeline.KafkaWorkers + appConfig.GlobalConfig.Pipeline.PostgresWorkers
	if processorCount < 2 {
		return 2
	}
	if processorCount > 8 {
		return 8
	}
	return processorCount
}

func inboundQueueSize() int {
	const (
		defaultQueueSize = 16384
		maxQueueSize     = 65536
	)

	if appConfig.GlobalConfig == nil {
		return defaultQueueSize
	}

	queueSize := appConfig.GlobalConfig.Pipeline.KafkaQueueSize + appConfig.GlobalConfig.Pipeline.PostgresQueueSize
	if queueSize < defaultQueueSize {
		return defaultQueueSize
	}
	if queueSize > maxQueueSize {
		return maxQueueSize
	}
	return queueSize
}

func startReadLoopReporter(counters *readLoopCounters, stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastReadCalls uint64
	var lastReadErrors uint64
	var lastChannelEnqueue uint64

	for {
		select {
		case <-ticker.C:
			readCallsTotal := counters.readCalls.Load()
			readErrorsTotal := counters.readErrors.Load()
			channelEnqueueTotal := counters.channelEnqueue.Load()

			appLogger.Infof(
				"read loop stats window=10s readmessage_calls_delta=%d readmessage_errors_delta=%d channel_enqueues_delta=%d readmessage_calls_total=%d readmessage_errors_total=%d channel_enqueues_total=%d",
				readCallsTotal-lastReadCalls,
				readErrorsTotal-lastReadErrors,
				channelEnqueueTotal-lastChannelEnqueue,
				readCallsTotal,
				readErrorsTotal,
				channelEnqueueTotal,
			)

			lastReadCalls = readCallsTotal
			lastReadErrors = readErrorsTotal
			lastChannelEnqueue = channelEnqueueTotal
		case <-stop:
			return
		}
	}
}
