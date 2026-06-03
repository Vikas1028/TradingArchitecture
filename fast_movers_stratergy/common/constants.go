package common

const (
	AppName                    = "fast_movers_stratergy"
	ConfigDirName              = "config"
	ConfigFileName             = "fast_movers_stratergy_config.json"
	LoggerConfigDirName        = "cmd"
	LoggerConfigFileName       = "logger.json"
	DefaultEnv                 = "prod"
	DefaultTicksTopic          = "ticks.raw"
	DefaultSignalTopic         = "signals.strategy"
	DefaultTradesTopic         = "trades.paper"
	DefaultTradesGroupID       = "fast-movers-trades-v1"
	DefaultCommitIntervalMs    = 1000
	DefaultReplayGraceSec      = 0
	DefaultTimezone            = "Asia/Kolkata"
	DefaultOpenWindowStart     = "09:15:00"
	DefaultOpenWindowEnd       = "15:15:00"
	DefaultRetracePct          = 0.20
	DefaultReturnTolerancePct  = 0.05
	DefaultReversalTriggerPct  = 0.50
	DefaultBurstWindowStart    = "09:15:00"
	DefaultBurstWindowEnd      = "15:15:00"
	DefaultBurstTriggerPct     = 0.50
	DefaultBurstConfirmSec     = 30
	DefaultTwoCandleMinMovePct = 0.50
	DefaultStopLossPct         = 0.30
	DefaultTargetPct           = 0.00
	DefaultSourceStaleSeconds  = 2
	DefaultMaxOpenTrades       = 3
	DefaultS1MaxTradesPerDay   = 3
	DefaultS2MaxTradesPerMin   = 2
	DefaultS3MaxTradesPerDay   = 5
	DefaultS4MaxTradesPerDay   = 4
	DefaultS4WindowStart       = "09:20:00"
	DefaultS4WindowEnd         = "11:00:00"
	DefaultS4BreakoutPct       = 0.35
	DefaultS4RetestPct         = 0.10
	DefaultS4ConfirmPct        = 0.08
	DefaultS4StopLossPct       = 0.30
	DefaultS4TargetPct         = 0.90
	DefaultTrailingStopStepPct = 0.30
	DefaultTrailingFreezePct   = 1.00
	DefaultMetricsPort         = "9116"
	DefaultDashboardPath       = "/dashboard"
)
