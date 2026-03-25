package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"marketdata_go_feed/common"
)

// NewLogger initializes and returns a structured logger for the market data service.
// Inputs: LogConfig with level and file path.
// Outputs: configured zap.Logger or error.
// Flow: build zap core with file + stdout, apply level, human-readable encoder.
func NewLogger() (*zap.Logger, error) {
	cfg, paths, err := loadLoggerConfig()
	if err != nil {
		return nil, err
	}
	level := zap.InfoLevel
	if err := level.UnmarshalText([]byte(strings.ToLower(cfg.Level))); err != nil {
		level = zap.InfoLevel
	}
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "",
		MessageKey:    "message",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(fmt.Sprintf("%s:%03d", t.Format("15:04:05"), t.Nanosecond()/int(time.Millisecond)))
		},
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	encoder := zapcore.NewJSONEncoder(encoderCfg)
	if strings.EqualFold(cfg.Format, "console") {
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	}
	file, err := os.OpenFile(paths.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, common.DefaultFilePermission)
	if err != nil {
		return nil, err
	}
	fileCore := zapcore.NewCore(encoder, zapcore.AddSync(file), level)
	core := fileCore
	if cfg.Console {
		stdoutCore := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)
		core = zapcore.NewTee(fileCore, stdoutCore)
	}
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel))
	logger.Info("logger initialized",
		zap.String("app", common.AppName),
		zap.String("path", paths.FilePath),
		zap.String("configured_level", strings.ToUpper(strings.TrimSpace(cfg.Level))),
		zap.String("configured_format", strings.ToLower(strings.TrimSpace(cfg.Format))),
	)
	return logger, nil
}

func loadLoggerConfig() (common.LoggerConfig, common.LoggerPaths, error) {
	cfg := common.LoggerConfig{
		Enable:   true,
		Level:    common.DefaultLogLevel,
		Format:   common.DefaultLogFormat,
		LogDir:   common.LogDirName,
		FileName: common.AppName + "_service.log",
		Console:  true,
	}
	configPath, err := common.ResolveLoggerConfigPath()
	if err != nil {
		return cfg, common.LoggerPaths{}, err
	}
	file, err := os.Open(configPath)
	if err != nil {
		return cfg, common.LoggerPaths{}, err
	}
	defer file.Close()
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return cfg, common.LoggerPaths{}, err
	}
	appRoot, err := common.ResolveAppRoot()
	if err != nil {
		return cfg, common.LoggerPaths{}, err
	}
	now := time.Now()
	baseDir := filepath.Join(appRoot, strings.TrimSpace(cfg.LogDir))
	if strings.TrimSpace(cfg.LogDir) == "" {
		baseDir = filepath.Join(appRoot, common.LogDirName)
	}
	dateDir := filepath.Join(baseDir, now.Format(common.DateFolderLayout))
	if err := os.MkdirAll(dateDir, common.DefaultDirPermission); err != nil {
		return cfg, common.LoggerPaths{}, err
	}
	baseFileName := strings.TrimSpace(cfg.FileName)
	if baseFileName == "" {
		baseFileName = common.AppName + "_service.log"
	}
	fileName := fmt.Sprintf("%s_%s", now.Format(common.LogFileTimeLayout), baseFileName)
	paths := common.LoggerPaths{BaseDir: baseDir, DateDir: dateDir, FileName: fileName, FilePath: filepath.Join(dateDir, fileName)}
	return cfg, paths, nil
}
