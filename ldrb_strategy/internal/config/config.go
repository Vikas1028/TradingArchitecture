package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	StockCandlesTopic     string `json:"stock_candles_topic"`
	IndexCandlesTopic     string `json:"index_candles_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

type LogConfig struct {
	Level string `json:"level"`
	File  string `json:"file"`
}

type DependenciesConfig struct {
	EventsPath             string `json:"events_path"`
	VolumeProfilePath      string `json:"volume_profile_path"`
	TurnoverRankingPath    string `json:"turnover_ranking_path"`
	EnableSafeMode         bool   `json:"enable_safe_mode"`
	AllowRVOLFallback      bool   `json:"allow_rvol_fallback"`
	SkipAllIfEventsMissing bool   `json:"skip_all_if_events_missing"`
}

type StrategyConfig struct {
	Timezone                 string  `json:"timezone"`
	SessionStart             string  `json:"session_start"`
	EntryStart               string  `json:"entry_start"`
	EntryEnd                 string  `json:"entry_end"`
	SameDayExitCutoff        string  `json:"same_day_exit_cutoff"`
	ExitMonitoringEnd        string  `json:"exit_monitoring_end"`
	Capital                  float64 `json:"capital"`
	RiskPerTradePct          float64 `json:"risk_per_trade_pct"`
	MaxTradesPerDay          int     `json:"max_trades_per_day"`
	MaxOpenOvernight         int     `json:"max_open_overnight"`
	MaxOvernightRiskPct      float64 `json:"max_overnight_risk_pct"`
	CapitalUsageCapPct       float64 `json:"capital_usage_cap_pct"`
	MinPrice                 float64 `json:"min_price"`
	MaxIntradayMovePct       float64 `json:"max_intraday_move_pct"`
	BodyRatioMin             float64 `json:"body_ratio_min"`
	VolumeRatioMin           float64 `json:"volume_ratio_min"`
	RVOLMin                  float64 `json:"rvol_min"`
	BreakoutCushionPct       float64 `json:"breakout_cushion_pct"`
	SecondTryExtraPct        float64 `json:"second_try_extra_pct"`
	SlippageBufferPct        float64 `json:"slippage_buffer_pct"`
	MinStopDistancePct       float64 `json:"min_stop_distance_pct"`
	MaxStopDistancePct       float64 `json:"max_stop_distance_pct"`
	GapUpPct                 float64 `json:"gap_up_pct"`
	GapDownPct               float64 `json:"gap_down_pct"`
	MarketDrawdownLimitPct   float64 `json:"market_drawdown_limit_pct"`
	LiquidityTopN            int     `json:"liquidity_top_n"`
	MinPartialFillPct        float64 `json:"min_partial_fill_pct"`
	RequireSpreadCheck       bool    `json:"require_spread_check"`
	MaxSpreadPct             float64 `json:"max_spread_pct"`
	SwingStart               string  `json:"swing_start"`
	IndexSymbol              string  `json:"index_symbol"`
	EnableMarketFilter       bool    `json:"enable_market_filter"`
	EnableEventFilter        bool    `json:"enable_event_filter"`
	EnableSameDayFailureExit bool    `json:"enable_same_day_failure_exit"`
}

type AppConfig struct {
	Env          string             `json:"env"`
	Kafka        KafkaConfig        `json:"kafka"`
	Log          LogConfig          `json:"log"`
	Strategy     StrategyConfig     `json:"strategy"`
	Dependencies DependenciesConfig `json:"dependencies"`
}

func LoadConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	if err := ensureLogDir(cfg.Log.File); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *AppConfig) {
	if strings.TrimSpace(cfg.Kafka.StockCandlesTopic) == "" {
		cfg.Kafka.StockCandlesTopic = "candles.1m"
	}
	if strings.TrimSpace(cfg.Kafka.IndexCandlesTopic) == "" {
		cfg.Kafka.IndexCandlesTopic = "indices.1m"
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		cfg.Kafka.SignalTopic = "signals.strategy"
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = 120
	}

	if strings.TrimSpace(cfg.Log.Level) == "" {
		cfg.Log.Level = "INFO"
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		cfg.Log.File = "logs/ldrb_strategy.log"
	}

	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = "Asia/Kolkata"
	}
	if strings.TrimSpace(cfg.Strategy.SessionStart) == "" {
		cfg.Strategy.SessionStart = "09:15"
	}
	if strings.TrimSpace(cfg.Strategy.EntryStart) == "" {
		cfg.Strategy.EntryStart = "14:30"
	}
	if strings.TrimSpace(cfg.Strategy.EntryEnd) == "" {
		cfg.Strategy.EntryEnd = "15:15"
	}
	if strings.TrimSpace(cfg.Strategy.SameDayExitCutoff) == "" {
		cfg.Strategy.SameDayExitCutoff = "15:25"
	}
	if strings.TrimSpace(cfg.Strategy.ExitMonitoringEnd) == "" {
		cfg.Strategy.ExitMonitoringEnd = "10:30"
	}
	if cfg.Strategy.Capital == 0 {
		cfg.Strategy.Capital = 100000
	}
	if cfg.Strategy.RiskPerTradePct == 0 {
		cfg.Strategy.RiskPerTradePct = 0.005
	}
	if cfg.Strategy.MaxTradesPerDay == 0 {
		cfg.Strategy.MaxTradesPerDay = 2
	}
	if cfg.Strategy.MaxOpenOvernight == 0 {
		cfg.Strategy.MaxOpenOvernight = 1
	}
	if cfg.Strategy.MaxOvernightRiskPct == 0 {
		cfg.Strategy.MaxOvernightRiskPct = 0.008
	}
	if cfg.Strategy.CapitalUsageCapPct == 0 {
		cfg.Strategy.CapitalUsageCapPct = 0.70
	}
	if cfg.Strategy.MinPrice == 0 {
		cfg.Strategy.MinPrice = 100
	}
	if cfg.Strategy.MaxIntradayMovePct == 0 {
		cfg.Strategy.MaxIntradayMovePct = 0.04
	}
	if cfg.Strategy.BodyRatioMin == 0 {
		cfg.Strategy.BodyRatioMin = 0.60
	}
	if cfg.Strategy.VolumeRatioMin == 0 {
		cfg.Strategy.VolumeRatioMin = 2.0
	}
	if cfg.Strategy.RVOLMin == 0 {
		cfg.Strategy.RVOLMin = 1.5
	}
	if cfg.Strategy.BreakoutCushionPct == 0 {
		cfg.Strategy.BreakoutCushionPct = 0.0015
	}
	if cfg.Strategy.SecondTryExtraPct == 0 {
		cfg.Strategy.SecondTryExtraPct = 0.0010
	}
	if cfg.Strategy.SlippageBufferPct == 0 {
		cfg.Strategy.SlippageBufferPct = 0.001
	}
	if cfg.Strategy.MinStopDistancePct == 0 {
		cfg.Strategy.MinStopDistancePct = 0.003
	}
	if cfg.Strategy.MaxStopDistancePct == 0 {
		cfg.Strategy.MaxStopDistancePct = 0.012
	}
	if cfg.Strategy.GapUpPct == 0 {
		cfg.Strategy.GapUpPct = 0.008
	}
	if cfg.Strategy.GapDownPct == 0 {
		cfg.Strategy.GapDownPct = -0.003
	}
	if cfg.Strategy.MarketDrawdownLimitPct == 0 {
		cfg.Strategy.MarketDrawdownLimitPct = -0.004
	}
	if cfg.Strategy.LiquidityTopN == 0 {
		cfg.Strategy.LiquidityTopN = 200
	}
	if cfg.Strategy.MinPartialFillPct == 0 {
		cfg.Strategy.MinPartialFillPct = 0.60
	}
	if cfg.Strategy.MaxSpreadPct == 0 {
		cfg.Strategy.MaxSpreadPct = 0.0015
	}
	if strings.TrimSpace(cfg.Strategy.SwingStart) == "" {
		cfg.Strategy.SwingStart = "11:00"
	}
	if strings.TrimSpace(cfg.Strategy.IndexSymbol) == "" {
		cfg.Strategy.IndexSymbol = "NIFTY 50"
	}
}

func validate(cfg *AppConfig) error {
	missing := make([]string, 0)
	if strings.TrimSpace(cfg.Kafka.BootstrapServers) == "" {
		missing = append(missing, "kafka.bootstrap_servers")
	}
	if strings.TrimSpace(cfg.Kafka.GroupID) == "" {
		missing = append(missing, "kafka.group_id")
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		missing = append(missing, "log.file")
	}
	if cfg.Strategy.RiskPerTradePct <= 0 {
		missing = append(missing, "strategy.risk_per_trade_pct(>0)")
	}
	if cfg.Strategy.MaxTradesPerDay < 1 {
		missing = append(missing, "strategy.max_trades_per_day(>=1)")
	}
	if cfg.Strategy.Capital <= 0 {
		missing = append(missing, "strategy.capital(>0)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid required config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func ensureLogDir(path string) error {
	if path == "" {
		return nil
	}
	return os.MkdirAll(filepath.Dir(path), 0o755)
}
