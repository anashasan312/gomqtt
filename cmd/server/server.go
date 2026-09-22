// Package server owns the broker's runtime: the MQTT listener, the HTTP admin
// listener, the retransmission loop, and the ordered shutdown that stops them.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	prom "github.com/prometheus/client_golang/prometheus"

	h "github.com/anashasan/gomqtt/pkg/api/handlers"
	mw "github.com/anashasan/gomqtt/pkg/api/middleware"
	pubApp "github.com/anashasan/gomqtt/pkg/application/publishing"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/common/uid"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
	promInfra "github.com/anashasan/gomqtt/pkg/infrastructure/metrics/prometheus"
	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// APIBasePath is the versioned admin API prefix.
const APIBasePath = "/api/v1"

// Server runs the broker.
type Server struct {
	cfg        *config.AppConfig
	listener   *transport.Listener
	handlers   *h.Handlers
	publishing *pubApp.PublishingService
	sessions   persistence.ISessionStore
	index      persistence.ISubscriptionIndex
	registry   *prom.Registry
	metrics    metrics.Recorder
	ids        uid.Generator
	log        logger.Logger

	adminServer *http.Server
	cancel      context.CancelFunc
	background  chan struct{}
}

// Options bundles what NewServer needs.
//
// A struct rather than eleven positional parameters: the constructor has enough
// dependencies that a positional list would be unreadable and easy to transpose
// at the call site.
type Options struct {
	Config     *config.AppConfig
	Listener   *transport.Listener
	Handlers   *h.Handlers
	Publishing *pubApp.PublishingService
	Sessions   persistence.ISessionStore
	Index      persistence.ISubscriptionIndex
	Registry   *prom.Registry
	Metrics    metrics.Recorder
	IDs        uid.Generator
	Log        logger.Logger
}

// NewServer builds a Server.
func NewServer(opts Options) *Server {
	return &Server{
		cfg:        opts.Config,
		listener:   opts.Listener,
		handlers:   opts.Handlers,
		publishing: opts.Publishing,
		sessions:   opts.Sessions,
		index:      opts.Index,
		registry:   opts.Registry,
		metrics:    opts.Metrics,
		ids:        opts.IDs,
		log:        opts.Log,
		background: make(chan struct{}),
	}
}

// MQTTAddr returns the bound MQTT address, useful when the config asked for
// port 0 and the OS chose one — which is how the test suite runs many brokers
// in parallel.
func (s *Server) MQTTAddr() string {
	if addr := s.listener.Addr(); addr != nil {
		return addr.String()
	}
	return s.cfg.Broker.Address
}

// Start binds the listeners and launches the background loops.
//
// It returns once the MQTT socket is bound, so the caller knows the port is
// live before it reports readiness.
func (s *Server) Start(ctx context.Context) error {
	if err := s.listener.Start(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel

	go s.backgroundLoops(runCtx)

	if s.cfg.Admin.Enabled {
		s.startAdminServer(ctx)
	}

	s.log.Info(ctx, "gomqtt is ready",
		logger.F("mqtt", s.MQTTAddr()),
		logger.F("admin", s.cfg.Admin.Address),
	)
	return nil
}

// startAdminServer launches the HTTP listener for metrics and introspection.
func (s *Server) startAdminServer(ctx context.Context) {
	if s.cfg.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()

	// Middleware order is not arbitrary. Recovery is outermost so it catches a
	// panic raised anywhere inside; RequestID comes next so every log line
	// below it is correlated; the error handler runs last so it sees the errors
	// the handlers attach.
	engine.Use(
		mw.Recovery(s.log),
		mw.RequestID(s.ids),
		mw.RequestLogger(s.log),
		mw.ErrorHandler(s.log),
	)

	engine.GET("/health", s.handlers.SupportHandler.Health)
	engine.GET("/metrics", gin.WrapH(promInfra.NewHandler(s.registry)))
	registerAdminRoutes(engine, s.handlers.AdminHandler)

	s.adminServer = &http.Server{
		Addr:    s.cfg.Admin.Address,
		Handler: engine,
		// Explicit because http.Server's zero value has no timeouts at all, and
		// a server with no read timeout will hold a connection open forever for
		// a client that never finishes sending.
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		s.log.Info(ctx, "admin listener started", logger.F("address", s.cfg.Admin.Address))
		if err := s.adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error(ctx, "admin listener failed", err)
		}
	}()
}

// backgroundLoops runs the periodic work: retransmission and gauge sampling.
func (s *Server) backgroundLoops(ctx context.Context) {
	defer close(s.background)

	retransmit := time.NewTicker(s.cfg.Session.RetransmitInterval)
	defer retransmit.Stop()

	// Gauges are sampled on a timer rather than maintained incrementally,
	// because an incremental counter drifts the moment anything touches the
	// stores from outside the normal path, and a wrong gauge is worse than a
	// slightly stale one.
	sample := time.NewTicker(5 * time.Second)
	defer sample.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-retransmit.C:
			// §4.4: a QoS 1 message that is not acknowledged must be resent.
			// This covers the still-connected case; the reconnect case is
			// handled by ResumeSession.
			s.publishing.RetransmitExpired(ctx, s.cfg.Session.RetransmitTimeout)

		case <-sample.C:
			s.sampleGauges()
		}
	}
}

// sampleGauges refreshes the metrics that are counts of current state.
func (s *Server) sampleGauges() {
	inflight := 0
	for _, session := range s.sessions.All() {
		inflight += session.InflightCount()
	}

	s.metrics.SetInflightCount(inflight)
	s.metrics.SetSessionCount(s.sessions.Count())
	s.metrics.SetSubscriptionCount(s.index.SubscriptionCount())
}

// Stop shuts the broker down in order.
//
// The order is what makes the drain correct: the MQTT listener stops first so
// no new client is accepted into a broker that is going away and every existing
// client is told to finish, then the admin API goes, then the background loops.
// Reversing it would leave the admin API reporting on a broker that had already
// dropped its clients.
func (s *Server) Stop(ctx context.Context) error {
	var firstErr error

	if err := s.listener.Stop(ctx); err != nil {
		s.log.Error(ctx, "failed to stop the mqtt listener", err)
		firstErr = err
	}

	if s.adminServer != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := s.adminServer.Shutdown(shutdownCtx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if s.cancel != nil {
		s.cancel()
		// Wait for the loops to exit so nothing writes to a store after
		// shutdown reports it is done.
		select {
		case <-s.background:
		case <-time.After(5 * time.Second):
			s.log.Warn(ctx, "background loops did not stop in time")
		}
	}

	s.log.Info(ctx, "gomqtt stopped")
	return firstErr
}

// String renders the server for logs.
func (s *Server) String() string {
	return fmt.Sprintf("gomqtt %s on %s", s.cfg.Broker.Version, s.MQTTAddr())
}
