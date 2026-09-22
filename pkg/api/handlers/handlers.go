// Package handlers holds the HTTP presentation layer for the admin API.
//
// A handler's job is narrow on purpose: bind the request, call one service
// method, render the result. There is no business logic here and no
// error-to-status mapping — that belongs to the error middleware, which is the
// single place in the codebase that knows about status codes.
package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/gomqtt/pkg/application/services"
	adminContr "github.com/anashasan/gomqtt/pkg/contracts/admin"
	"github.com/anashasan/gomqtt/pkg/hydrator"
)

// AdminHandler serves the broker inspection endpoints.
type AdminHandler struct {
	admin services.IAdminService
}

// NewAdminHandler builds an AdminHandler.
func NewAdminHandler(admin services.IAdminService) *AdminHandler {
	return &AdminHandler{admin: admin}
}

// Stats handles GET /api/v1/stats.
func (h *AdminHandler) Stats(ctx *gin.Context) {
	stats, err := h.admin.Stats(ctx.Request.Context())
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, hydrator.ToStatsRes(stats))
}

// Clients handles GET /api/v1/clients.
func (h *AdminHandler) Clients(ctx *gin.Context) {
	clients, err := h.admin.Clients(ctx.Request.Context())
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, hydrator.ToListClientsRes(clients))
}

// Subscriptions handles GET /api/v1/subscriptions.
func (h *AdminHandler) Subscriptions(ctx *gin.Context) {
	subs, err := h.admin.Subscriptions(ctx.Request.Context())
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, hydrator.ToListSubscriptionsRes(subs))
}

// Retained handles GET /api/v1/retained.
func (h *AdminHandler) Retained(ctx *gin.Context) {
	retained, err := h.admin.RetainedTopics(ctx.Request.Context())
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, hydrator.ToListRetainedRes(retained))
}

// SupportHandler serves health and service metadata.
type SupportHandler struct {
	version   string
	startedAt time.Time
}

// NewSupportHandler builds a SupportHandler.
func NewSupportHandler(version string) *SupportHandler {
	return &SupportHandler{version: version, startedAt: time.Now()}
}

// Health handles GET /health.
//
// Liveness answers "is this process alive", so it deliberately checks nothing
// external. The broker has no downstream dependency to check anyway — it is the
// thing other services depend on.
func (h *SupportHandler) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, adminContr.HealthRes{
		Status:  "ok",
		Service: "gomqtt",
		Version: h.version,
		Uptime:  time.Since(h.startedAt).Round(time.Second).String(),
	})
}

// Handlers aggregates every HTTP handler into one injectable struct.
//
// Wire builds this once and route registration reads from it. Without the
// aggregate, every new handler would mean a new parameter on the server
// constructor and a new argument in three places.
type Handlers struct {
	AdminHandler   *AdminHandler
	SupportHandler *SupportHandler
}

// NewHandlers builds the aggregate.
func NewHandlers(adminHandler *AdminHandler, supportHandler *SupportHandler) *Handlers {
	return &Handlers{AdminHandler: adminHandler, SupportHandler: supportHandler}
}
