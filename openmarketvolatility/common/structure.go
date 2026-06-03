package common

import "time"

type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	TicksTopic            string `json:"ticks_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

type StrategyConfig struct {
	Timezone                string  `json:"timezone"`
	OpenWindowStart         string  `json:"open_window_start"`
	OpenWindowEnd           string  `json:"open_window_end"`
	RetraceThresholdPct     float64 `json:"retrace_threshold_pct"`
	ReturnTolerancePct      float64 `json:"return_tolerance_pct"`
	ReversalTriggerPct      float64 `json:"reversal_trigger_pct"`
	BurstWindowStart        string  `json:"burst_window_start"`
	BurstWindowEnd          string  `json:"burst_window_end"`
	BurstTriggerPct         float64 `json:"burst_trigger_pct"`
	BurstConfirmationSec    int     `json:"burst_confirmation_sec"`
	TwoCandleMinMovePct     float64 `json:"two_candle_min_move_pct"`
	DefaultStopLossPct      float64 `json:"default_stop_loss_pct"`
	DefaultTargetPct        float64 `json:"default_target_pct"`
	SourcePriorityStaleSec  int     `json:"source_priority_stale_sec"`
	S1MaxTradesPerDay       int     `json:"s1_max_trades_per_day"`
	S2MaxTradesPerMinute    int     `json:"s2_max_trades_per_minute"`
	S3MaxTradesPerDay       int     `json:"s3_max_trades_per_day"`
	S4MaxTradesPerDay       int     `json:"s4_max_trades_per_day"`
	S4WindowStart           string  `json:"s4_window_start"`
	S4WindowEnd             string  `json:"s4_window_end"`
	S4BreakoutPct           float64 `json:"s4_breakout_pct"`
	S4RetestPct             float64 `json:"s4_retest_pct"`
	S4ConfirmPct            float64 `json:"s4_confirm_pct"`
	S4StopLossPct           float64 `json:"s4_stop_loss_pct"`
	S4TargetPct             float64 `json:"s4_target_pct"`
	TrailingStopStepPct     float64 `json:"trailing_stop_step_pct"`
	TrailingFreezeProfitPct float64 `json:"trailing_freeze_profit_pct"`
}

type ServiceConfig struct {
	MetricsAddress string `json:"metrics_address"`
	DashboardPath  string `json:"dashboard_path"`
}

type AppConfig struct {
	Env      string         `json:"env"`
	Kafka    KafkaConfig    `json:"kafka"`
	Strategy StrategyConfig `json:"strategy"`
	Service  ServiceConfig  `json:"service"`
}

type Tick struct {
	Symbol     string    `json:"symbol"`
	Source     string    `json:"source"`
	Time       time.Time `json:"time"`
	LTP        float64   `json:"ltp"`
	DayOpen    float64   `json:"day_open"`
	Volume     int64     `json:"volume"`
	RawSubject string    `json:"-"`
}

type Signal struct {
	SignalID                string    `json:"signal_id,omitempty"`
	Strategy                string    `json:"strategy"`
	Symbol                  string    `json:"symbol"`
	Side                    string    `json:"side"`
	Time                    time.Time `json:"time"`
	Reason                  string    `json:"reason"`
	Quantity                int       `json:"quantity,omitempty"`
	StopLossPct             float64   `json:"stop_loss_pct,omitempty"`
	TargetPct               float64   `json:"target_pct,omitempty"`
	TrailingStopPct         float64   `json:"trailing_stop_pct,omitempty"`
	TrailingFreezeProfitPct float64   `json:"trailing_freeze_profit_pct,omitempty"`
}
