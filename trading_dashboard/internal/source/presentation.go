package source

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func buildServicePresentation(serviceID string, snapshot Snapshot) ([]DisplayField, []DetailSection) {
	runtimeFields := []DisplayField{
		{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
		{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
		{Label: "Goroutines", Value: formatInt(int64(snapshot.System.Goroutines))},
	}

	switch serviceID {
	case "go_feed":
		return feedPresentation(snapshot, runtimeFields)
	case "market_price":
		return marketPricePresentation(snapshot, runtimeFields)
	case "python_feed_triplex":
		return []DisplayField{
				{Label: "Ticks In", Value: formatMetric(snapshot.Metrics, "python_feed_ticks_received_total")},
				{Label: "Ticks Out", Value: formatMetric(snapshot.Metrics, "python_feed_ticks_published_total")},
				{Label: "WS Clients", Value: formatMetric(snapshot.Metrics, "python_feed_ws_clients_active")},
				{Label: "Reconnects", Value: formatMetric(snapshot.Metrics, "python_feed_ws_reconnects_total")},
				{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
				{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
			},
			[]DetailSection{
				{Title: "Feed Flow", Fields: []DisplayField{
					{Label: "Ticks Received", Value: formatMetric(snapshot.Metrics, "python_feed_ticks_received_total")},
					{Label: "Ticks Published", Value: formatMetric(snapshot.Metrics, "python_feed_ticks_published_total")},
					{Label: "Duplicates Dropped", Value: formatMetric(snapshot.Metrics, "python_feed_ticks_duplicates_dropped_total")},
					{Label: "Reconnects", Value: formatMetric(snapshot.Metrics, "python_feed_ws_reconnects_total")},
					{Label: "Stall Reconnects", Value: formatMetric(snapshot.Metrics, "python_feed_ws_stall_reconnects_total")},
					{Label: "Last Tick Age", Value: formatSeconds(snapshot.Metrics["python_feed_last_tick_age_seconds"])},
				}},
				{Title: "Connections", Fields: []DisplayField{
					{Label: "Websocket", Value: formatConnectedMetric(snapshot.Metrics, "python_feed_ws_connected")},
					{Label: "Kafka", Value: formatConnectedMetric(snapshot.Metrics, "python_feed_kafka_connected")},
					{Label: "Active WS Clients", Value: formatMetric(snapshot.Metrics, "python_feed_ws_clients_active")},
					{Label: "Errors", Value: formatMetric(snapshot.Metrics, "python_feed_errors_total")},
				}},
				{Title: "Runtime", Fields: runtimeFields},
			}
	case "volatile_strategy":
		return []DisplayField{
				{Label: "Signals", Value: formatUint(snapshot.SignalsTotal)},
				{Label: "Windows Closed", Value: formatUint(snapshot.WindowsClosed)},
				{Label: "Watchers", Value: formatUint(snapshot.ActiveWatchers)},
				{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
				{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
			},
			[]DetailSection{
				{Title: "Strategy Flow", Fields: []DisplayField{
					{Label: "Flow", Value: "consumes go_ltp stream and emits volatile breakout signals"},
					{Label: "Signals Total", Value: formatUint(snapshot.SignalsTotal)},
					{Label: "Windows Closed", Value: formatUint(snapshot.WindowsClosed)},
					{Label: "Active Watchers", Value: formatUint(snapshot.ActiveWatchers)},
				}},
				{Title: "Runtime", Fields: runtimeFields},
			}
	case "marketdata":
		inputTopic := "ticks.raw"
		return []DisplayField{
				{Label: "Source Topic", Value: inputTopic},
				{Label: "Ticks In", Value: formatMetric(snapshot.Metrics, "marketdata_ticks_consumed_total")},
				{Label: "Candles Out", Value: formatMetric(snapshot.Metrics, "marketdata_candles_emitted_total")},
				{Label: "Tick Gap", Value: formatSeconds(snapshot.Metrics["marketdata_tick_gap_seconds"])},
				{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
				{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
			},
			[]DetailSection{
				{Title: "Candle Engine", Fields: []DisplayField{
					{Label: "Input Topic", Value: inputTopic},
					{Label: "Ticks Consumed", Value: formatMetric(snapshot.Metrics, "marketdata_ticks_consumed_total")},
					{Label: "Candles Emitted", Value: formatMetric(snapshot.Metrics, "marketdata_candles_emitted_total")},
					{Label: "Last Tick", Value: formatTime(snapshot.Ticks.LastTickAt)},
					{Label: "Tick Gap", Value: formatSeconds(snapshot.Metrics["marketdata_tick_gap_seconds"])},
					{Label: "Errors", Value: formatMetric(snapshot.Metrics, "marketdata_errors_total")},
				}},
				{Title: "Kafka", Fields: []DisplayField{
					{Label: "Kafka In", Value: formatConnectedMetric(snapshot.Metrics, "marketdata_kafka_in_connected")},
					{Label: "Kafka Out", Value: formatConnectedMetric(snapshot.Metrics, "marketdata_kafka_out_connected")},
				}},
				{Title: "Runtime", Fields: runtimeFields},
			}
	case "first_candle_strategy":
		return strategyPresentation("First Candle Strategy", snapshot, "opening-candle reversal")
	case "vwap_strategy":
		return strategyPresentation("VWAP Strategy", snapshot, "bias + VWAP pullback")
	case "ldrb_strategy":
		return []DisplayField{
				{Label: "Stock+Index Candles", Value: formatMetric(snapshot.Metrics, "ldrb_strategy_candles_consumed_total")},
				{Label: "Signals Out", Value: formatMetric(snapshot.Metrics, "ldrb_strategy_signals_emitted_total")},
				{Label: "Errors", Value: formatMetric(snapshot.Metrics, "ldrb_strategy_errors_total")},
				{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
				{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
			},
			[]DetailSection{
				{Title: "Strategy Flow", Fields: []DisplayField{
					{Label: "Flow", Value: "consumes stock + index candles and emits LDRB signals"},
					{Label: "Candles Consumed", Value: formatMetric(snapshot.Metrics, "ldrb_strategy_candles_consumed_total")},
					{Label: "Signals Emitted", Value: formatMetric(snapshot.Metrics, "ldrb_strategy_signals_emitted_total")},
					{Label: "Errors", Value: formatMetric(snapshot.Metrics, "ldrb_strategy_errors_total")},
					{Label: "Service Up", Value: formatConnectedMetric(snapshot.Metrics, "ldrb_strategy_up")},
				}},
				{Title: "Runtime", Fields: runtimeFields},
			}
	case "paper_engine":
		return []DisplayField{
				{Label: "Signals In", Value: formatMetric(snapshot.Metrics, "paper_engine_signals_consumed_total")},
				{Label: "Trades Out", Value: formatMetric(snapshot.Metrics, "paper_engine_trades_emitted_total")},
				{Label: "Live Trades", Value: formatInt(int64(len(snapshot.RunningTrades)))},
				{Label: "Realized PnL", Value: formatSigned(snapshot.Metrics["paper_engine_realized_pnl"])},
				{Label: "Unrealized PnL", Value: formatSigned(snapshot.Metrics["paper_engine_unrealized_pnl"])},
				{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
				{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
			},
			[]DetailSection{
				{Title: "Trading State", Fields: []DisplayField{
					{Label: "Signals Consumed", Value: formatMetric(snapshot.Metrics, "paper_engine_signals_consumed_total")},
					{Label: "Trades Emitted", Value: formatMetric(snapshot.Metrics, "paper_engine_trades_emitted_total")},
					{Label: "PnL Snapshots", Value: formatMetric(snapshot.Metrics, "paper_engine_pnl_snapshots_total")},
					{Label: "Trading Halted", Value: formatConnectedMetric(snapshot.Metrics, "paper_engine_trading_halted")},
					{Label: "Open Positions", Value: formatMetric(snapshot.Metrics, "paper_engine_open_positions")},
					{Label: "Pending Signals", Value: formatMetric(snapshot.Metrics, "paper_engine_pending_signals")},
					{Label: "Trades Today", Value: formatMetric(snapshot.Metrics, "paper_engine_trades_today")},
				}},
				{Title: "Profit And Risk", Fields: []DisplayField{
					{Label: "Realized PnL", Value: formatSigned(snapshot.Metrics["paper_engine_realized_pnl"])},
					{Label: "Unrealized PnL", Value: formatSigned(snapshot.Metrics["paper_engine_unrealized_pnl"])},
					{Label: "Max Drawdown", Value: formatSigned(snapshot.Metrics["paper_engine_max_drawdown"])},
					{Label: "Errors", Value: formatMetric(snapshot.Metrics, "paper_engine_errors_total")},
				}},
				{Title: "Runtime", Fields: runtimeFields},
			}
	default:
		return runtimeFields, []DetailSection{{Title: "Runtime", Fields: runtimeFields}}
	}
}

func marketPricePresentation(snapshot Snapshot, runtimeFields []DisplayField) ([]DisplayField, []DetailSection) {
	var latest time.Time
	rowCount := len(snapshot.MarketPrices)
	sourceCount := len(snapshot.MarketPriceSources)
	for _, row := range snapshot.MarketPrices {
		for _, price := range row.Prices {
			if !price.Timestamp.IsZero() && price.Timestamp.After(latest) {
				latest = price.Timestamp
			}
		}
	}
	lastUpdate := formatTime(latest)
	return []DisplayField{
			{Label: "Stocks", Value: formatInt(int64(rowCount))},
			{Label: "Sources", Value: formatInt(int64(sourceCount))},
			{Label: "Last Price", Value: lastUpdate},
			{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
			{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
		},
		[]DetailSection{
			{Title: "Price Fan-In", Fields: []DisplayField{
				{Label: "Source Topics", Value: strings.Join(snapshot.MarketPriceSources, ", ")},
				{Label: "Tracked Stocks", Value: formatInt(int64(rowCount))},
				{Label: "Topic Count", Value: formatInt(int64(sourceCount))},
				{Label: "Last Price Time", Value: lastUpdate},
			}},
			{Title: "Connections", Fields: boolFields(snapshot.ConnectionStatus)},
			{Title: "Runtime", Fields: runtimeFields},
		}
}

func feedPresentation(snapshot Snapshot, runtimeFields []DisplayField) ([]DisplayField, []DetailSection) {
	return []DisplayField{
			{Label: "Ticks", Value: formatUint(snapshot.Ticks.TotalReceived)},
			{Label: "Ticks/Sec", Value: formatUint(snapshot.Ticks.PerSecondReceived)},
			{Label: "Kafka Writes", Value: formatUint(snapshot.Ticks.KafkaInsertedTotal)},
			{Label: "Postgres Writes", Value: formatUint(snapshot.Ticks.PostgresInsertedTotal)},
			{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
			{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
		},
		[]DetailSection{
			{Title: "Feed Pipeline", Fields: []DisplayField{
				{Label: "Ticks Received", Value: formatUint(snapshot.Ticks.TotalReceived)},
				{Label: "Ticks / Sec", Value: formatUint(snapshot.Ticks.PerSecondReceived)},
				{Label: "Ticks / Min", Value: formatUint(snapshot.Ticks.PerMinuteReceived)},
				{Label: "Kafka Writes", Value: formatUint(snapshot.Ticks.KafkaInsertedTotal)},
				{Label: "Postgres Writes", Value: formatUint(snapshot.Ticks.PostgresInsertedTotal)},
				{Label: "Last Tick", Value: formatTime(snapshot.Ticks.LastTickAt)},
			}},
			{Title: "Connections", Fields: boolFields(snapshot.ConnectionStatus)},
			{Title: "Queues And Reconnects", Fields: append([]DisplayField{
				{Label: "Kafka Queue", Value: formatInt(int64(snapshot.QueueDepth["kafka"]))},
				{Label: "Postgres Queue", Value: formatInt(int64(snapshot.QueueDepth["postgres"]))},
				{Label: "Kafka Reconnects", Value: formatUint(snapshot.Reconnects["kafka"])},
				{Label: "Postgres Reconnects", Value: formatUint(snapshot.Reconnects["postgres"])},
				{Label: "Websocket Reconnects", Value: formatUint(snapshot.Reconnects["websocket"])},
				{Label: "Client Relogins", Value: formatUint(snapshot.Reconnects["client"])},
			}, runtimeFields...)},
		}
}

func strategyPresentation(name string, snapshot Snapshot, flow string) ([]DisplayField, []DetailSection) {
	prefix := strings.ToLower(strings.ReplaceAll(name, " ", "_"))
	if prefix == "first_candle_strategy" || strings.Contains(name, "First Candle") {
		prefix = "first_candle_strategy"
	}
	if prefix == "vwap_strategy" || strings.Contains(name, "VWAP") {
		prefix = "vwap_strategy"
	}
	runtimeFields := []DisplayField{
		{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
		{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
		{Label: "Goroutines", Value: formatInt(int64(snapshot.System.Goroutines))},
	}
	summary := []DisplayField{
		{Label: "Candles In", Value: formatMetric(snapshot.Metrics, prefix+"_candles_consumed_total")},
		{Label: "Signals Out", Value: formatMetric(snapshot.Metrics, prefix+"_signals_emitted_total")},
		{Label: "Errors", Value: formatMetric(snapshot.Metrics, prefix+"_errors_total")},
		{Label: "CPU", Value: formatPercent(snapshot.System.CPUPercent)},
		{Label: "RAM", Value: formatBytes(preferredRAM(snapshot.System))},
	}
	sections := []DetailSection{
		{Title: "Strategy Flow", Fields: []DisplayField{
			{Label: "Flow", Value: "consumes 1m candles and emits " + flow + " signals"},
			{Label: "Candles Consumed", Value: formatMetric(snapshot.Metrics, prefix+"_candles_consumed_total")},
			{Label: "Signals Emitted", Value: formatMetric(snapshot.Metrics, prefix+"_signals_emitted_total")},
			{Label: "Errors", Value: formatMetric(snapshot.Metrics, prefix+"_errors_total")},
			{Label: "Service Up", Value: formatConnectedMetric(snapshot.Metrics, prefix+"_up")},
		}},
		{Title: "Runtime", Fields: runtimeFields},
	}
	return summary, sections
}

func boolFields(values map[string]bool) []DisplayField {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]DisplayField, 0, len(keys))
	for _, key := range keys {
		fields = append(fields, DisplayField{
			Label: strings.ReplaceAll(strings.Title(strings.ReplaceAll(key, "_", " ")), " ", " "),
			Value: map[bool]string{true: "green", false: "red"}[values[key]],
		})
	}
	return fields
}

func preferredRAM(system SystemSummary) uint64 {
	if system.ResidentMemoryBytes > 0 {
		return system.ResidentMemoryBytes
	}
	if system.SysBytes > 0 {
		return system.SysBytes
	}
	return system.HeapAllocBytes
}

func formatMetric(metrics map[string]float64, key string) string {
	return formatNumber(metrics[key])
}

func formatConnectedMetric(metrics map[string]float64, key string) string {
	if metrics[key] >= 1 {
		return "green"
	}
	return "red"
}

func formatUint(value uint64) string {
	return fmt.Sprintf("%d", value)
}

func formatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}

func formatNumber(value float64) string {
	if value == float64(int64(value)) {
		return fmt.Sprintf("%d", int64(value))
	}
	return fmt.Sprintf("%.2f", value)
}

func formatSigned(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func formatPercent(value float64) string {
	return fmt.Sprintf("%.1f%%", value)
}

func formatBytes(value uint64) string {
	if value == 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	size := float64(value)
	index := 0
	for size >= 1024 && index < len(units)-1 {
		size /= 1024
		index++
	}
	return fmt.Sprintf("%.1f %s", size, units[index])
}

func formatSeconds(value float64) string {
	return fmt.Sprintf("%.1fs", value)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "N/A"
	}
	return value.Format("15:04:05")
}
