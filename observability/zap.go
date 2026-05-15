package observability

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ZapRollingFileConfig configures local rolling file logging.
type ZapRollingFileConfig struct {
	Filename   string
	Level      string
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
	Compress   bool
	Console    bool
}

// ZapLogger adapts zap to the SDK Logger interface.
type ZapLogger struct {
	logger *zap.Logger
	closer io.Closer
}

// NewZapLogger wraps an existing zap logger.
func NewZapLogger(logger *zap.Logger) *ZapLogger {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ZapLogger{logger: logger}
}

// NewZapRollingFileLogger creates a zap logger that rotates local log files.
func NewZapRollingFileLogger(config ZapRollingFileConfig) (*ZapLogger, error) {
	if config.Filename == "" {
		return nil, fmt.Errorf("log filename must not be blank")
	}
	if config.MaxSizeMB <= 0 {
		config.MaxSizeMB = 100
	}
	if config.MaxBackups <= 0 {
		config.MaxBackups = 7
	}
	if config.MaxAgeDays <= 0 {
		config.MaxAgeDays = 14
	}
	level := zapcore.InfoLevel
	if config.Level != "" {
		if err := level.UnmarshalText([]byte(config.Level)); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(config.Filename), 0o755); err != nil {
		return nil, err
	}
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	rollingFile := &lumberjack.Logger{
		Filename:   config.Filename,
		MaxSize:    config.MaxSizeMB,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAgeDays,
		Compress:   config.Compress,
	}
	fileSyncer := zapcore.AddSync(rollingFile)
	cores := []zapcore.Core{zapcore.NewCore(encoder, fileSyncer, level)}
	if config.Console {
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level))
	}
	logger := zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	return &ZapLogger{logger: logger, closer: rollingFile}, nil
}

func (l *ZapLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.logger.Debug(msg, l.zapFields(ctx, fields)...)
}

func (l *ZapLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.logger.Info(msg, l.zapFields(ctx, fields)...)
}

func (l *ZapLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.logger.Warn(msg, l.zapFields(ctx, fields)...)
}

func (l *ZapLogger) Error(ctx context.Context, msg string, fields ...Field) {
	l.logger.Error(msg, l.zapFields(ctx, fields)...)
}

// Sync flushes buffered log entries.
func (l *ZapLogger) Sync() error {
	return l.logger.Sync()
}

// Close flushes logs and closes owned local file resources.
func (l *ZapLogger) Close() error {
	var firstErr error
	if err := l.Sync(); err != nil {
		firstErr = err
	}
	if l.closer != nil {
		if err := l.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *ZapLogger) zapFields(ctx context.Context, fields []Field) []zap.Field {
	zapFields := make([]zap.Field, 0, len(fields)+2)
	for _, field := range fields {
		zapFields = append(zapFields, toZapField(field))
	}
	if spanContext := spanContextFromContext(ctx); spanContext.IsValid() {
		zapFields = append(zapFields,
			zap.String("trace_id", spanContext.TraceID().String()),
			zap.String("span_id", spanContext.SpanID().String()),
		)
	}
	return zapFields
}

func toZapField(field Field) zap.Field {
	switch value := field.Value.(type) {
	case string:
		return zap.String(field.Key, value)
	case int:
		return zap.Int(field.Key, value)
	case int16:
		return zap.Int16(field.Key, value)
	case int32:
		return zap.Int32(field.Key, value)
	case int64:
		return zap.Int64(field.Key, value)
	case bool:
		return zap.Bool(field.Key, value)
	case time.Duration:
		return zap.Duration(field.Key, value)
	case error:
		return zap.NamedError(field.Key, value)
	default:
		return zap.Any(field.Key, value)
	}
}
