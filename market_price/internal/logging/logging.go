package logging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type loggerConfig struct {
	Enable   bool   `json:"enable"`
	Level    string `json:"level"`
	Format   string `json:"format"`
	LogDir   string `json:"log_dir"`
	FileName string `json:"file_name"`
	Console  bool   `json:"console"`
}

func NewLogger() (*zap.Logger, error) {
	raw, err := os.ReadFile(filepath.Join("cmd", "logger.json"))
	if err != nil {
		return nil, err
	}
	var cfg loggerConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.LogDir == "" {
		cfg.LogDir = "logs"
	}
	if cfg.FileName == "" {
		cfg.FileName = "market_price.log"
	}

	now := time.Now()
	dateDir := filepath.Join(cfg.LogDir, now.Format("2006-01-02"))
	if err := os.MkdirAll(dateDir, 0o755); err != nil {
		return nil, err
	}
	filePath := filepath.Join(dateDir, now.Format("150405")+"_"+cfg.FileName)
	writer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    100,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   false,
	})
	if cfg.Console {
		writer = zapcore.NewMultiWriteSyncer(writer, zapcore.AddSync(os.Stdout))
	}

	level := zapcore.InfoLevel
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		level = zapcore.InfoLevel
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "time"
	encCfg.LevelKey = "level"
	encCfg.MessageKey = "message"
	encCfg.CallerKey = ""
	encCfg.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format("15:04:05.000"))
	}
	encCfg.EncodeLevel = zapcore.CapitalLevelEncoder

	encoder := zapcore.NewJSONEncoder(encCfg)
	if cfg.Format == "console" {
		encoder = zapcore.NewConsoleEncoder(encCfg)
	}

	return zap.New(zapcore.NewCore(encoder, writer, level)), nil
}
