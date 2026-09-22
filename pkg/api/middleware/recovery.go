package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	"github.com/anashasan/gomqtt/pkg/common/logger"
)

// Recovery turns a panic in a handler into a 500 instead of a dead process.
//
// The stack trace goes to the log, never to the response: a stack tells an
// attacker the file layout and dependency versions of the service, and tells a
// legitimate client nothing it can act on.
func Recovery(log logger.Logger) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := errors.Internal(
					"panic_recovered",
					"an unexpected error occurred",
					fmt.Errorf("%v", recovered),
				)

				log.Error(ctx.Request.Context(), "handler panicked", err,
					logger.F("path", ctx.FullPath()),
					logger.F("stack", string(debug.Stack())),
				)

				ctx.AbortWithStatusJSON(http.StatusInternalServerError, ErrorRes{
					Code:    "internal_error",
					Message: "an unexpected error occurred",
				})
			}
		}()

		ctx.Next()
	}
}
