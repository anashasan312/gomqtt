package server

import (
	"github.com/gin-gonic/gin"

	h "github.com/anashasan/gomqtt/pkg/api/handlers"
)

// registerAdminRoutes mounts the broker inspection API.
//
//	GET /api/v1/stats          broker counters and uptime
//	GET /api/v1/clients        connected clients, with per-client session depth
//	GET /api/v1/subscriptions  every active subscription and its granted QoS
//	GET /api/v1/retained       every topic holding a retained message
//
// Read-only by design. Everything an operator needs to *change* about a running
// MQTT broker is already expressible in MQTT itself — publishing a zero-length
// retained message clears a topic, and disconnecting a client is the client's
// own affair — so an endpoint that mutated broker state would be a second,
// unaudited way to do what the protocol already does.
//
// Route registration is kept out of server.go and grouped by resource so the
// whole surface of the API is readable in one screen.
func registerAdminRoutes(engine *gin.Engine, handler *h.AdminHandler) {
	api := engine.Group(APIBasePath)

	api.GET("/stats", handler.Stats)
	api.GET("/clients", handler.Clients)
	api.GET("/subscriptions", handler.Subscriptions)
	api.GET("/retained", handler.Retained)
}
