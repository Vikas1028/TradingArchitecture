package common

type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	StockCandlesTopic     string `json:"stock_candles_topic"`
	IndexCandlesTopic     string `json:"index_candles_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
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
	Strategy     StrategyConfig     `json:"strategy"`
	Dependencies DependenciesConfig `json:"dependencies"`
}

type LoggerConfig struct {
	Enable   bool   `json:"enable"`
	Level    string `json:"level"`
	Format   string `json:"format"`
	LogDir   string `json:"log_dir"`
	FileName string `json:"file_name"`
	Console  bool   `json:"console"`
}

type LoggerPaths struct {
	BaseDir  string
	DateDir  string
	FileName string
	FilePath string
}
