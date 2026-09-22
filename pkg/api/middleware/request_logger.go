package middleware

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/common/uid"
)

// RequestIDHeader is the header carrying a correlation id in and out.
const RequestIDHeader = "X-Request-ID"

// RequestID attaches a correlation id to every request.
//
// An inbound id is honoured so a trace started by a caller survives the hop;
// only a request without one gets a fresh id. The id is put on the request
// context, which means every log line written downstream carries it without any
// handler passing it around by hand.
func RequestID(ids uid.Generator) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		requestID := ctx.GetHeader(RequestIDHeader)
		if requestID == "" {
			requestID = ids.New()
		}

		ctx.Writer.Header().Set(RequestIDHeader, requestID)
		ctx.Request = ctx.Request.WithContext(
			logger.WithFields(ctx.Request.Context(), logger.F("request_id", requestID)),
		)
		ctx.Next()
	}
}

// RequestLogger writes one structured line per request.
func RequestLogger(log logger.Logger) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		start := time.Now()
		path := ctx.Request.URL.Path

		ctx.Next()

		// Skip the metrics endpoint: a Prometheus scrape every fifteen seconds
		// would otherwise be most of the log volume and none of its value.
		if path == "/metrics" {
			return
		}

		log.Info(ctx.Request.Context(), "http request",
			logger.F("method", ctx.Request.Method),
			logger.F("path", path),
			logger.F("status", ctx.Writer.Status()),
			logger.F("duration_ms", time.Since(start).Milliseconds()),
			logger.F("client_ip", ctx.ClientIP()),
		)
	}
}
