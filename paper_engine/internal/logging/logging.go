package logging

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"paper_engine/internal/config"
)

// NewLogger initializes and returns a logger for the paper engine.
// Inputs: LogConfig with level and file path.
// Outputs: zap.Logger or error.
// Flow: configure console encoder, attach file + stdout cores, apply level, add stacktraces.
func NewLogger(cfg config.LogConfig) (*zap.Logger, error) {
	level := zap.InfoLevel
	if err := level.UnmarshalText([]byte(strings.ToLower(cfg.Level))); err != nil {
		level = zap.InfoLevel
	}

	encCfg := zapcore.EncoderConfig{
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
	encoder := zapcore.NewConsoleEncoder(encCfg)

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
