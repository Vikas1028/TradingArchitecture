package logger

import (
	"go.uber.org/zap"
)

// NewLogger returns a production JSON logger aligned with the existing services.
func NewLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.DisableStacktrace = true
	return cfg.Build()
}
