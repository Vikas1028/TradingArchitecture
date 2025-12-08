package logging

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"marketdata/internal/config"
)

// NewLogger initializes and returns a structured logger for the market data service.
// Inputs: LogConfig with level and file path.
// Outputs: configured zap.Logger or error.
// Flow: build zap core with file + stdout, apply level, human-readable encoder.
func NewLogger(cfg config.LogConfig) (*zap.Logger, error) {
	level := zap.InfoLevel
	if err := level.UnmarshalText([]byte(strings.ToLower(cfg.Level))); err != nil {
		level = zap.InfoLevel
	}

	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	encoder := zapcore.NewConsoleEncoder(encoderCfg)

	// File sink
	file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	fileCore := zapcore.NewCore(encoder, zapcore.AddSync(file), level)
	stdoutCore := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)

	core := zapcore.NewTee(fileCore, stdoutCore)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel))
	return logger, nil
}
