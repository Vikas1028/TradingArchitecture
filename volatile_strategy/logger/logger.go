package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"volatile_strategy/common"
)

type LogLevel string

const (
	levelDebug LogLevel = "DEBUG"
	levelInfo  LogLevel = "INFO"
	levelWarn  LogLevel = "WARN"
	levelError LogLevel = "ERROR"
	levelFatal LogLevel = "FATAL"
)

var (
	currentLogConfig common.LoggingConfig
	baseLogger       *zap.Logger
	sugarLogger      *zap.SugaredLogger
	loggerOnce       sync.Once
	loggerErr        error
	loggerReady      bool
	loggerMu         sync.RWMutex
)

func InitializeLogger() error {
	loggerOnce.Do(func() {
		loggerErr = initializeLogger()
	})
	return loggerErr
}

func initializeLogger() error {
	if err := loadLoggerConfig(&currentLogConfig); err != nil {
		log.Printf("[ERROR] failed to load logger config: %v", err)
		return err
	}
	if !currentLogConfig.Enable {
		loggerMu.Lock()
		loggerReady = true
		loggerMu.Unlock()
		return nil
	}

	appRoot, err := common.ResolveAppRoot()
	if err != nil {
		return err
	}
	logDirName := strings.TrimSpace(currentLogConfig.LogDir)
	if logDirName == "" {
		logDirName = common.LogDirName
	}
	baseFileName := strings.TrimSpace(currentLogConfig.FileName)
	if baseFileName == "" {
		baseFileName = strings.TrimPrefix(common.LogFileNameSuffix, "_")
	}

	now := time.Now()
	baseDir := filepath.Join(appRoot, logDirName)
	if err := os.MkdirAll(filepath.Join(baseDir, now.Format(common.DateFolderLayout)), common.DefaultDirPermission); err != nil {
		return err
	}
	filePath := filepath.Join(baseDir, now.Format(common.DateFolderLayout), fmt.Sprintf("%s_%s", now.Format(common.LogFileTimeLayout), baseFileName))
	writer := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    100,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   false,
	}
	syncer := zapcore.AddSync(writer)
	if currentLogConfig.Console {
		syncer = zapcore.NewMultiWriteSyncer(syncer, zapcore.AddSync(os.Stdout))
	}
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.LevelKey = "level"
	encoderConfig.MessageKey = "message"
	encoderConfig.CallerKey = ""
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(fmt.Sprintf("%s:%03d", t.Format("15:04:05"), t.Nanosecond()/int(time.Millisecond)))
	}
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	encoder := zapcore.NewJSONEncoder(encoderConfig)
	if strings.EqualFold(strings.TrimSpace(currentLogConfig.Format), "console") {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}
	baseLogger = zap.New(zapcore.NewCore(encoder, syncer, zapcore.DebugLevel), zap.ErrorOutput(zapcore.AddSync(os.Stderr)))
	sugarLogger = baseLogger.Sugar()
	loggerMu.Lock()
	loggerReady = true
	loggerMu.Unlock()
	Infof("logger initialized")
	return nil
}

func loadLoggerConfig(cfg *common.LoggingConfig) error {
	filePath, err := common.ResolveLoggerConfigPath()
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(file).Decode(cfg)
}

func IsInitialized() bool {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return loggerReady
}

func Debugf(format string, args ...any) { writeLog(levelDebug, format, args...) }
func Infof(format string, args ...any)  { writeLog(levelInfo, format, args...) }
func Warnf(format string, args ...any)  { writeLog(levelWarn, format, args...) }
func Errorf(format string, args ...any) { writeLog(levelError, format, args...) }
func Fatalf(format string, args ...any) {
	writeLog(levelFatal, format, args...)
	os.Exit(1)
}

func writeLog(level LogLevel, format string, args ...any) {
	if !shouldLog(level) {
		return
	}
	switch level {
	case levelDebug:
		sugarLogger.Debugf(format, args...)
	case levelWarn:
		sugarLogger.Warnf(format, args...)
	case levelError, levelFatal:
		sugarLogger.Errorf(format, args...)
	default:
		sugarLogger.Infof(format, args...)
	}
}

func normalizeLevel(level string) LogLevel {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case string(levelDebug):
		return levelDebug
	case string(levelWarn), "WARNING":
		return levelWarn
	case string(levelError):
		return levelError
	case string(levelFatal):
		return levelFatal
	default:
		return levelInfo
	}
}

func shouldLog(level LogLevel) bool {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	if !loggerReady || !currentLogConfig.Enable || sugarLogger == nil {
		return false
	}
	switch normalizeLevel(currentLogConfig.Level) {
	case levelDebug:
		return true
	case levelInfo:
		return level != levelDebug
	case levelWarn:
		return level == levelWarn || level == levelError || level == levelFatal
	case levelError:
		return level == levelError || level == levelFatal
	case levelFatal:
		return level == levelFatal
	default:
		return level != levelDebug
	}
}

func WriteStackTrace(stackTrace string) {
	appRoot, err := common.ResolveAppRoot()
	if err != nil {
		return
	}
	logPath := filepath.Join(appRoot, strings.TrimSpace(currentLogConfig.LogDir), common.PanicLogFileName)
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, common.DefaultFilePermission)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = io.WriteString(file, stackTrace+"\n")
}

func CloseLogger() error {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	loggerReady = false
	if baseLogger == nil {
		return nil
	}
	err := baseLogger.Sync()
	baseLogger = nil
	sugarLogger = nil
	return err
}
