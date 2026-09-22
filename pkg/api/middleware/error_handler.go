// Package middleware holds the cross-cutting HTTP concerns.
package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	"github.com/anashasan/gomqtt/pkg/common/logger"
)

// ErrorRes is the single error shape every failing endpoint returns.
type ErrorRes struct {
	// Code is the stable machine-readable identifier a client may branch on.
	Code string `json:"code"`
	// Message is the human-readable description.
	Message string `json:"message"`
}

// ErrorHandler converts errors collected with ctx.Error into HTTP responses.
//
// This is the only place in the codebase that knows about status codes. Handlers
// call `_ = ctx.Error(err); return` and nothing else, so no handler can invent
// its own mapping and no domain package ever has to import net/http to say that
// something was not found.
func ErrorHandler(log logger.Logger) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Next()

		if len(ctx.Errors) == 0 {
			return
		}

		// The last error is the one the handler chose to surface; anything
		// before it is context it attached on the way out.
		err := ctx.Errors.Last().Err
		appErr, ok := errors.As(err)
		if !ok {
			log.Error(ctx.Request.Context(), "unhandled error", err,
				logger.F("path", ctx.FullPath()))
			ctx.AbortWithStatusJSON(http.StatusInternalServerError, ErrorRes{
				Code:    "internal_error",
				Message: "an unexpected error occurred",
			})
			return
		}

		status := statusFor(appErr.Kind)
		if status >= http.StatusInternalServerError {
			log.Error(ctx.Request.Context(), "request failed", err,
				logger.F("path", ctx.FullPath()),
				logger.F("code", appErr.Code))
		}

		ctx.AbortWithStatusJSON(status, ErrorRes{
			Code:    appErr.Code,
			Message: appErr.Message,
		})
	}
}

// statusFor maps a domain error kind onto an HTTP status code.
func statusFor(kind errors.Kind) int {
	switch kind {
	case errors.KindInvalidArgument:
		return http.StatusBadRequest
	case errors.KindNotFound:
		return http.StatusNotFound
	case errors.KindConflict:
		return http.StatusConflict
	case errors.KindUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
