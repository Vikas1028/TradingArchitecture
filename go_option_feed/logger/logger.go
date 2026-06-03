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
	"go_option_feed/common"
	"gopkg.in/natefinch/lumberjack.v2"
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

// InitializeLogger prepares a single zap-backed log file for the current run.
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
		log.Printf("[ERROR] failed to resolve app root for logger: %v", err)
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
	if err := os.MkdirAll(baseDir, common.DefaultDirPermission); err != nil {
		log.Printf("[ERROR] failed to create log base directory %s: %v", baseDir, err)
		return err
	}

	dateDir := filepath.Join(baseDir, now.Format(common.DateFolderLayout))
	if err := os.MkdirAll(dateDir, common.DefaultDirPermission); err != nil {
		log.Printf("[ERROR] failed to create dated log directory %s: %v", dateDir, err)
		return err
	}

	fileName := fmt.Sprintf("%s_%s", now.Format(common.LogFileTimeLayout), baseFileName)
	filePath := filepath.Join(dateDir, fileName)
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

	baseLogger = zap.New(
		zapcore.NewCore(
			buildEncoder(strings.TrimSpace(currentLogConfig.Format)),
			syncer,
			zapcore.DebugLevel,
		),
		zap.ErrorOutput(zapcore.AddSync(os.Stderr)),
	)
	sugarLogger = baseLogger.Sugar()

	loggerMu.Lock()
	loggerReady = true
	loggerMu.Unlock()

	Infof("logger initialized successfully with level=%s", normalizeLevel(currentLogConfig.Level))
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

func buildEncoder(format string) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.LevelKey = "level"
	encoderConfig.MessageKey = "message"
	encoderConfig.CallerKey = ""
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(fmt.Sprintf("%s:%03d", t.Format("15:04:05"), t.Nanosecond()/int(time.Millisecond)))
	}
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	if strings.EqualFold(format, "console") {
		return zapcore.NewConsoleEncoder(encoderConfig)
	}
	return zapcore.NewJSONEncoder(encoderConfig)
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

// IsInitialized reports whether the logger is ready for file-backed logging.
func IsInitialized() bool {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return loggerReady
}

// Debugf writes a debug log when the configured log level includes debug messages.
func Debugf(format string, args ...any) {
	writeLog(levelDebug, format, args...)
}

// Infof writes an info log when the configured log level includes info messages.
func Infof(format string, args ...any) {
	writeLog(levelInfo, format, args...)
}

// Warnf writes a warning log when the configured log level includes warnings.
func Warnf(format string, args ...any) {
	writeLog(levelWarn, format, args...)
}

// Errorf writes an error log when the configured log level includes errors.
func Errorf(format string, args ...any) {
	writeLog(levelError, format, args...)
}

// Fatalf writes a fatal log and terminates the process.
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
	case levelError:
		sugarLogger.Errorf(format, args...)
	case levelFatal:
		// Use Errorf here so shutdown remains controlled by Fatalf.
		sugarLogger.Errorf(format, args...)
	default:
		sugarLogger.Infof(format, args...)
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
		return level == levelWarn || level == levelError || level == levelFatal
	case levelFatal:
		return level == levelFatal
	default:
		return level != levelDebug
	}
}

// WriteStackTrace persists recovered panic stack traces into a dedicated log file.
func WriteStackTrace(stackTrace string) {
	Errorf("panic stack trace captured")

	appRoot, err := common.ResolveAppRoot()
	if err != nil {
		return
	}

	logDirName := strings.TrimSpace(currentLogConfig.LogDir)
	if logDirName == "" {
		logDirName = common.LogDirName
	}

	logPath := filepath.Join(appRoot, logDirName, common.PanicLogFileName)
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, common.DefaultFilePermission)
	if err != nil {
		return
	}
	defer file.Close()

	if _, err := io.WriteString(file, stackTrace+common.StackTraceLineSuffix); err != nil {
		return
	}
}

// CloseLogger flushes and closes the active zap logger during shutdown.
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
