package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/stellhub/stellflow-go-sdk"

// Field is one structured log field.
type Field struct {
	Key   string
	Value any
}

// Logger is the SDK logging contract used by framework adapters and local loggers.
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
}

// Options configures SDK observability.
type Options struct {
	TracerProvider trace.TracerProvider
	Logger         Logger
}

// Normalize fills unset observability dependencies with no-op implementations.
func Normalize(options Options) Options {
	if options.TracerProvider == nil {
		options.TracerProvider = trace.NewNoopTracerProvider()
	}
	if options.Logger == nil {
		options.Logger = NoopLogger{}
	}
	return options
}

// Merge applies override observability settings on top of base settings.
func Merge(base Options, override Options) Options {
	if override.TracerProvider != nil {
		base.TracerProvider = override.TracerProvider
	}
	if override.Logger != nil {
		base.Logger = override.Logger
	}
	return Normalize(base)
}

// Tracer returns the SDK tracer.
func Tracer(options Options) trace.Tracer {
	return Normalize(options).TracerProvider.Tracer(tracerName)
}

// NoopLogger drops all log entries.
type NoopLogger struct{}

func (NoopLogger) Debug(context.Context, string, ...Field) {}
func (NoopLogger) Info(context.Context, string, ...Field)  {}
func (NoopLogger) Warn(context.Context, string, ...Field)  {}
func (NoopLogger) Error(context.Context, string, ...Field) {}

// String creates a string field.
func String(key string, value string) Field {
	return Field{Key: key, Value: value}
}

// Int creates an int field.
func Int(key string, value int) Field {
	return Field{Key: key, Value: value}
}

// Int16 creates an int16 field.
func Int16(key string, value int16) Field {
	return Field{Key: key, Value: value}
}

// Int32 creates an int32 field.
func Int32(key string, value int32) Field {
	return Field{Key: key, Value: value}
}

// Int64 creates an int64 field.
func Int64(key string, value int64) Field {
	return Field{Key: key, Value: value}
}

// Bool creates a bool field.
func Bool(key string, value bool) Field {
	return Field{Key: key, Value: value}
}

// Duration creates a duration field.
func Duration(key string, value time.Duration) Field {
	return Field{Key: key, Value: value}
}

// Error creates an error field.
func Error(err error) Field {
	return Field{Key: "error", Value: err}
}
