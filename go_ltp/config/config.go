package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"go_ltp/common"
	appLogger "go_ltp/logger"
)

var (
	// GlobalConfig exposes the initialized application config to the full application.
	GlobalConfig *common.AppConfig
	once         sync.Once
	initErr      error
)

// InitializeConfig loads config.cfg and logger.json into the global config object.
func InitializeConfig() error {
	once.Do(func() {
		appLogger.Debugf("starting application config initialization")
		GlobalConfig, initErr = loadConfig()
		if initErr == nil {
			initErr = loadLoggerConfig(&GlobalConfig.Logging)
		}
		if initErr == nil {
			applyDefaults(GlobalConfig)
			appLogger.Infof("application config initialized successfully")
		} else {
			appLogger.Errorf("application config initialization failed: %v", initErr)
		}
	})
	return initErr
}

// loadConfig reads config.cfg and maps section values into the shared config structure.
func loadConfig() (*common.AppConfig, error) {
	filePath, err := common.ResolveConfigPath()
	if err != nil {
		appLogger.Errorf("failed to resolve config path: %v", err)
		return nil, err
	}
	appLogger.Debugf("resolved config path: %s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		appLogger.Errorf("failed to open config file %s: %v", filePath, err)
		return nil, err
	}
	defer file.Close()

	cfg := &common.AppConfig{}
	scanner := bufio.NewScanner(file)
	currentSection := ""

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			appLogger.Errorf("invalid config line encountered: %s", line)
			return nil, fmt.Errorf("invalid config line: %s", line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if err := assignConfigValue(cfg, currentSection, key, value); err != nil {
			appLogger.Errorf("failed to assign config value for section=%s key=%s: %v", currentSection, key, err)
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		appLogger.Errorf("error while scanning config file %s: %v", filePath, err)
		return nil, err
	}

	appLogger.Infof("config file loaded from %s", filePath)
	return cfg, nil
}

// assignConfigValue routes one config.cfg entry to the correct section-specific structure.
func assignConfigValue(cfg *common.AppConfig, section, key, value string) error {
	switch section {
	case common.ConfigSectionDhan:
		return assignDhanClientValue(&cfg.DhanClient, key, value)
	case common.ConfigSectionWebsocket:
		return assignDhanWebsocketValue(&cfg.DhanWebsocket, key, value)
	case common.ConfigSectionKafka:
		return assignKafkaValue(&cfg.Kafka, key, value)
	case common.ConfigSectionPostgres:
		return assignPostgresValue(&cfg.Postgres, key, value)
	case common.ConfigSectionSubscription:
		return assignSubscriptionValue(&cfg.Subscription, key, value)
	case common.ConfigSectionPipeline:
		return assignPipelineValue(&cfg.Pipeline, key, value)
	case common.ConfigSectionLTP:
		return assignLTPValue(&cfg.LTP, key, value)
	case common.ConfigSectionService:
		return assignServiceValue(&cfg.Service, key, value)
	default:
		return fmt.Errorf("unknown config section: %s", section)
	}
}

// loadLoggerConfig reads logger.json and decodes it into the shared logging structure.
func loadLoggerConfig(cfg *common.LoggingConfig) error {
	filePath, err := common.ResolveLoggerConfigPath()
	if err != nil {
		appLogger.Errorf("failed to resolve logger config path: %v", err)
		return err
	}
	appLogger.Debugf("resolved logger config path: %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		appLogger.Errorf("failed to open logger config file %s: %v", filePath, err)
		return err
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(cfg); err != nil {
		appLogger.Errorf("failed to decode logger config file %s: %v", filePath, err)
		return err
	}

	appLogger.Debugf("logger config merged into global application config")
	return nil
}

// assignDhanClientValue fills broker client settings from the dhan_client section.
func assignDhanClientValue(cfg *common.DhanClientConfig, key, value string) error {
	switch key {
	case "client_id":
		cfg.ClientID = value
	case "access_token":
		cfg.AccessToken = value
	case "api_env":
		cfg.APIEnv = value
	case "batch_size":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.BatchSize = parsedValue
	case "exchange_segment":
		cfg.ExchangeSegment = value
	default:
		return fmt.Errorf("unknown dhan_client key: %s", key)
	}
	return nil
}

// assignDhanWebsocketValue fills websocket settings from the dhan_websocket section.
func assignDhanWebsocketValue(cfg *common.DhanWebsocketConfig, key, value string) error {
	switch key {
	case "url":
		cfg.URL = value
	case "version":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.Version = parsedValue
	case "auth_type":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.AuthType = parsedValue
	case "feed_mode":
		cfg.FeedMode = value
	case "reconnect":
		parsedValue, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		cfg.Reconnect = parsedValue
	case "reconnect_interval_sec":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.ReconnectIntervalSec = parsedValue
	case "max_reconnect_attempts":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.MaxReconnectAttempts = parsedValue
	case "ping_interval_sec":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.PingIntervalSec = parsedValue
	case "read_timeout_sec":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.ReadTimeoutSec = parsedValue
	case "write_timeout_sec":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.WriteTimeoutSec = parsedValue
	default:
		return fmt.Errorf("unknown dhan_websocket key: %s", key)
	}
	return nil
}

// assignKafkaValue fills Kafka settings from the kafka section.
func assignKafkaValue(cfg *common.KafkaConfig, key, value string) error {
	switch key {
	case "brokers":
		cfg.Brokers = value
	case "topic":
		cfg.Topic = value
	case "group_id":
		cfg.GroupID = value
	default:
		return fmt.Errorf("unknown kafka key: %s", key)
	}
	return nil
}

// assignPostgresValue fills PostgreSQL settings from the postgres section.
func assignPostgresValue(cfg *common.PostgresConfig, key, value string) error {
	switch key {
	case "host":
		cfg.Host = value
	case "port":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.Port = parsedValue
	case "user":
		cfg.User = value
	case "password":
		cfg.Password = value
	case "db_name":
		cfg.DBName = value
	case "sslmode":
		cfg.SSLMode = value
	case "table_name":
		cfg.TableName = value
	case "max_conns":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.MaxConns = int32(parsedValue)
	case "min_conns":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.MinConns = int32(parsedValue)
	default:
		return fmt.Errorf("unknown postgres key: %s", key)
	}
	return nil
}

// assignSubscriptionValue fills startup subscription settings from the subscription section.
func assignSubscriptionValue(cfg *common.SubscriptionConfig, key, value string) error {
	switch key {
	case "group":
		cfg.Group = value
	case "request_code":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.RequestCode = parsedValue
	default:
		return fmt.Errorf("unknown subscription key: %s", key)
	}
	return nil
}

// assignPipelineValue fills downstream worker and queue settings from the pipeline section.
func assignPipelineValue(cfg *common.PipelineConfig, key, value string) error {
	switch key {
	case "kafka_workers":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.KafkaWorkers = parsedValue
	case "postgres_workers":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.PostgresWorkers = parsedValue
	case "kafka_queue_size":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.KafkaQueueSize = parsedValue
	case "postgres_queue_size":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.PostgresQueueSize = parsedValue
	case "shutdown_drain_timeout_sec":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.ShutdownDrainTimeoutSec = parsedValue
	default:
		return fmt.Errorf("unknown pipeline key: %s", key)
	}
	return nil
}

// assignLTPValue fills snapshot polling controls from the ltp section.
func assignLTPValue(cfg *common.LTPConfig, key, value string) error {
	switch key {
	case "fetch_only_during_market_hours":
		parsedValue, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		cfg.FetchOnlyDuringMarketHours = parsedValue
	case "market_start_time":
		cfg.MarketStartTime = value
	case "market_end_time":
		cfg.MarketEndTime = value
	case "count_log_interval_sec":
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.CountLogIntervalSec = parsedValue
	default:
		return fmt.Errorf("unknown ltp key: %s", key)
	}
	return nil
}

// assignServiceValue fills service runtime settings from the service section.
func assignServiceValue(cfg *common.ServiceConfig, key, value string) error {
	switch key {
	case "metrics_address":
		cfg.MetricsAddress = value
	case "dashboard_path":
		cfg.DashboardPath = value
	default:
		return fmt.Errorf("unknown service key: %s", key)
	}
	return nil
}

// applyDefaults fills any omitted config values with safe runtime defaults.
func applyDefaults(cfg *common.AppConfig) {
	if cfg.DhanClient.BatchSize == 0 {
		cfg.DhanClient.BatchSize = 100
		appLogger.Debugf("applied default dhan client batch size: %d", cfg.DhanClient.BatchSize)
	}
	if cfg.DhanWebsocket.Version == 0 {
		cfg.DhanWebsocket.Version = 2
		appLogger.Debugf("applied default websocket version: %d", cfg.DhanWebsocket.Version)
	}
	if cfg.DhanWebsocket.AuthType == 0 {
		cfg.DhanWebsocket.AuthType = 2
		appLogger.Debugf("applied default websocket auth type: %d", cfg.DhanWebsocket.AuthType)
	}
	if cfg.DhanWebsocket.ReconnectIntervalSec == 0 {
		cfg.DhanWebsocket.ReconnectIntervalSec = common.DefaultRetryIntervalSec
		appLogger.Debugf("applied default websocket reconnect interval: %d", cfg.DhanWebsocket.ReconnectIntervalSec)
	}
	if cfg.DhanWebsocket.ReadTimeoutSec == 0 {
		cfg.DhanWebsocket.ReadTimeoutSec = 40
		appLogger.Debugf("applied default websocket read timeout: %d", cfg.DhanWebsocket.ReadTimeoutSec)
	}
	if cfg.DhanClient.ExchangeSegment == "" {
		cfg.DhanClient.ExchangeSegment = "NSE_EQ"
		appLogger.Debugf("applied default exchange segment: %s", cfg.DhanClient.ExchangeSegment)
	}
	if cfg.Subscription.Group == "" {
		cfg.Subscription.Group = common.DefaultSubscriptionGroup
		appLogger.Debugf("applied default subscription group: %s", cfg.Subscription.Group)
	}
	if cfg.Subscription.RequestCode == 0 {
		switch strings.ToLower(strings.TrimSpace(cfg.DhanWebsocket.FeedMode)) {
		case "ticker":
			cfg.Subscription.RequestCode = common.FeedRequestCodeTicker
		case "quote":
			cfg.Subscription.RequestCode = common.FeedRequestCodeQuote
		default:
			cfg.Subscription.RequestCode = common.FeedRequestCodeFull
		}
		appLogger.Debugf("applied default subscription request code: %d", cfg.Subscription.RequestCode)
	}
	if cfg.Service.MetricsAddress == "" {
		cfg.Service.MetricsAddress = common.DefaultMetricsAddress
		appLogger.Debugf("applied default metrics address: %s", cfg.Service.MetricsAddress)
	}
	if cfg.Service.DashboardPath == "" {
		cfg.Service.DashboardPath = common.DefaultDashboardPath
		appLogger.Debugf("applied default dashboard path: %s", cfg.Service.DashboardPath)
	}
	if cfg.Postgres.TableName == "" {
		cfg.Postgres.TableName = common.DefaultPostgresTableName
		appLogger.Debugf("applied default postgres table name: %s", cfg.Postgres.TableName)
	}
	if cfg.Postgres.Host == "" {
		cfg.Postgres.Host = "localhost"
		appLogger.Debugf("applied default postgres host: %s", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
		appLogger.Debugf("applied default postgres port: %d", cfg.Postgres.Port)
	}
	if cfg.Postgres.SSLMode == "" {
		cfg.Postgres.SSLMode = "disable"
		appLogger.Debugf("applied default postgres sslmode: %s", cfg.Postgres.SSLMode)
	}
	if cfg.Postgres.MaxConns <= 0 {
		cfg.Postgres.MaxConns = int32(common.DefaultPostgresWorkers * 2)
		appLogger.Debugf("applied default postgres max conns: %d", cfg.Postgres.MaxConns)
	}
	if cfg.Postgres.MinConns < 0 {
		cfg.Postgres.MinConns = 0
		appLogger.Debugf("applied default postgres min conns: %d", cfg.Postgres.MinConns)
	}
	if cfg.Pipeline.KafkaWorkers <= 0 {
		cfg.Pipeline.KafkaWorkers = common.DefaultKafkaWorkers
		appLogger.Debugf("applied default kafka worker count: %d", cfg.Pipeline.KafkaWorkers)
	}
	if cfg.Pipeline.PostgresWorkers <= 0 {
		cfg.Pipeline.PostgresWorkers = common.DefaultPostgresWorkers
		appLogger.Debugf("applied default postgres worker count: %d", cfg.Pipeline.PostgresWorkers)
	}
	if cfg.Pipeline.KafkaQueueSize <= 0 {
		cfg.Pipeline.KafkaQueueSize = common.DefaultKafkaQueueSize
		appLogger.Debugf("applied default kafka queue size: %d", cfg.Pipeline.KafkaQueueSize)
	}
	if cfg.Pipeline.PostgresQueueSize <= 0 {
		cfg.Pipeline.PostgresQueueSize = common.DefaultPostgresQueueSize
		appLogger.Debugf("applied default postgres queue size: %d", cfg.Pipeline.PostgresQueueSize)
	}
	if cfg.Pipeline.ShutdownDrainTimeoutSec <= 0 {
		cfg.Pipeline.ShutdownDrainTimeoutSec = common.DefaultShutdownDrainSec
		appLogger.Debugf("applied default shutdown drain timeout: %d", cfg.Pipeline.ShutdownDrainTimeoutSec)
	}
	if strings.TrimSpace(cfg.LTP.MarketStartTime) == "" {
		cfg.LTP.MarketStartTime = "08:55"
		appLogger.Debugf("applied default LTP market start time: %s", cfg.LTP.MarketStartTime)
	}
	if strings.TrimSpace(cfg.LTP.MarketEndTime) == "" {
		cfg.LTP.MarketEndTime = "15:31"
		appLogger.Debugf("applied default LTP market end time: %s", cfg.LTP.MarketEndTime)
	}
	if cfg.LTP.CountLogIntervalSec <= 0 {
		cfg.LTP.CountLogIntervalSec = 10
		appLogger.Debugf("applied default LTP count log interval: %d", cfg.LTP.CountLogIntervalSec)
	}
	if cfg.Logging.LogDir == "" {
		cfg.Logging.LogDir = common.LogDirName
		appLogger.Debugf("applied default log directory: %s", cfg.Logging.LogDir)
	}
	if cfg.Logging.FileName == "" {
		cfg.Logging.FileName = common.AppName + ".log"
		appLogger.Debugf("applied default log file name: %s", cfg.Logging.FileName)
	}
}
