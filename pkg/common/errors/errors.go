// Package errors defines the transport-agnostic error type that flows from the
// domain and application layers up to the API layer.
//
// Lower layers never import net/http. They return an *AppError carrying a stable
// machine-readable Code; the HTTP error middleware is the single place that maps
// a Kind onto a status code.
package errors

import (
	"errors"
	"fmt"
)

// Kind classifies an error so that outer layers can map it onto a protocol
// specific status without knowing anything about the originating domain.
type Kind uint8

const (
	// KindInternal is an unexpected failure. It maps to 5xx.
	KindInternal Kind = iota
	// KindInvalidArgument is a caller supplied value that violates an invariant.
	KindInvalidArgument
	// KindNotFound is a lookup that yielded nothing.
	KindNotFound
	// KindConflict is a violated uniqueness or state precondition.
	KindConflict
	// KindUnavailable is a downstream dependency that is temporarily unusable.
	KindUnavailable
)

// String renders the Kind for logs.
func (k Kind) String() string {
	switch k {
	case KindInvalidArgument:
		return "invalid_argument"
	case KindNotFound:
		return "not_found"
	case KindConflict:
		return "conflict"
	case KindUnavailable:
		return "unavailable"
	case KindInternal:
		return "internal"
	default:
		return "unknown"
	}
}

// AppError is the canonical error carried across layer boundaries.
type AppError struct {
	// Kind drives protocol mapping.
	Kind Kind
	// Code is a stable identifier defined by the owning aggregate, for example
	// "invalid_job_priority". Clients may branch on it; it never changes.
	Code string
	// Message is a human readable description safe to return to a caller.
	Message string
	// cause is the wrapped underlying error, never exposed to clients.
	cause error
}

// Error implements the error interface.
func (e *AppError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap exposes the cause to errors.Is and errors.As.
func (e *AppError) Unwrap() error { return e.cause }

// WithCause returns a copy of the error carrying an underlying cause.
func (e *AppError) WithCause(cause error) *AppError {
	return &AppError{Kind: e.Kind, Code: e.Code, Message: e.Message, cause: cause}
}

// New builds an AppError without a cause.
func New(kind Kind, code, message string) *AppError {
	return &AppError{Kind: kind, Code: code, Message: message}
}

// Wrap builds an AppError carrying an underlying cause.
func Wrap(kind Kind, code, message string, cause error) *AppError {
	return &AppError{Kind: kind, Code: code, Message: message, cause: cause}
}

// Invalid is a shorthand for a KindInvalidArgument error.
func Invalid(code, message string) *AppError { return New(KindInvalidArgument, code, message) }

// NotFound is a shorthand for a KindNotFound error.
func NotFound(code, message string) *AppError { return New(KindNotFound, code, message) }

// Conflict is a shorthand for a KindConflict error.
func Conflict(code, message string) *AppError { return New(KindConflict, code, message) }

// Internal is a shorthand for a KindInternal error carrying a cause.
func Internal(code, message string, cause error) *AppError {
	return Wrap(KindInternal, code, message, cause)
}

// As extracts an *AppError from an error chain. It reports false when the chain
// contains no AppError, in which case callers should treat it as internal.
func As(err error) (*AppError, bool) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr, true
	}
	return nil, false
}

// KindOf reports the Kind of an error, defaulting to KindInternal.
func KindOf(err error) Kind {
	if appErr, ok := As(err); ok {
		return appErr.Kind
	}
	return KindInternal
}
