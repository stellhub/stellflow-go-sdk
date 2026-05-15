package observability

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

func spanContextFromContext(ctx context.Context) trace.SpanContext {
	if ctx == nil {
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(ctx)
}
