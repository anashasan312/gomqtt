// Package logger provides the structured logging facade used by every layer.
//
// Layers depend on the Logger interface, never on a concrete sink, so the
// logging backend can be swapped in di without touching business code.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// ctxKey is the unexported key type used to attach fields to a context.
type ctxKey struct{}

// Field is a single structured key/value pair.
type Field struct {
	Key   string
	Value any
}

// F builds a Field.
func F(key string, value any) Field { return Field{Key: key, Value: value} }

// Logger is the logging port. Every method takes a context so that correlation
// fields attached upstream travel with the record automatically.
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, err error, fields ...Field)
}

// SlogLogger adapts log/slog to the Logger port.
type SlogLogger struct{ inner *slog.Logger }

// NewSlogLogger builds a JSON structured Logger at the supplied level. Unknown
// level strings fall back to info rather than failing startup.
func NewSlogLogger(level string) *SlogLogger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return &SlogLogger{inner: slog.New(handler)}
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Debug logs at debug level.
func (l *SlogLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelDebug, msg, nil, fields)
}

// Info logs at info level.
func (l *SlogLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelInfo, msg, nil, fields)
}

// Warn logs at warn level.
func (l *SlogLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelWarn, msg, nil, fields)
}

// Error logs at error level with the error attached under the "error" key.
func (l *SlogLogger) Error(ctx context.Context, msg string, err error, fields ...Field) {
	l.log(ctx, slog.LevelError, msg, err, fields)
}

func (l *SlogLogger) log(ctx context.Context, level slog.Level, msg string, err error, fields []Field) {
	attrs := make([]any, 0, (len(fields)+len(FromContext(ctx)))*2+2)
	for _, f := range FromContext(ctx) {
		attrs = append(attrs, f.Key, f.Value)
	}
	for _, f := range fields {
		attrs = append(attrs, f.Key, f.Value)
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	l.inner.Log(ctx, level, msg, attrs...)
}

// WithFields returns a context carrying fields that every subsequent log record
// made with that context will include.
func WithFields(ctx context.Context, fields ...Field) context.Context {
	return context.WithValue(ctx, ctxKey{}, append(FromContext(ctx), fields...))
}

// FromContext reads the fields attached to a context.
func FromContext(ctx context.Context) []Field {
	if ctx == nil {
		return nil
	}
	fields, _ := ctx.Value(ctxKey{}).([]Field)
	return fields
}

// Nop is a Logger that discards everything. Tests bind it to keep output clean.
type Nop struct{}

// NewNop builds a discarding Logger.
func NewNop() *Nop { return &Nop{} }

// Debug discards the record.
func (Nop) Debug(context.Context, string, ...Field) {}

// Info discards the record.
func (Nop) Info(context.Context, string, ...Field) {}

// Warn discards the record.
func (Nop) Warn(context.Context, string, ...Field) {}

// Error discards the record.
func (Nop) Error(context.Context, string, error, ...Field) {}
