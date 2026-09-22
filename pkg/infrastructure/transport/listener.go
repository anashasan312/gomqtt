package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anashasan/gomqtt/pkg/common/clock"
	appErrors "github.com/anashasan/gomqtt/pkg/common/errors"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
)

// ListenerConfig tunes the TCP listener.
type ListenerConfig struct {
	// Address is the bind address, for example ":1883".
	Address string
	// MaxConnections caps concurrent connections. Zero means unlimited.
	//
	// The cap exists because every connection costs two goroutines, a read
	// buffer and an outbound queue; without it, the broker's failure mode under
	// a connection flood is the OOM killer rather than a refusal.
	MaxConnections int
	// ShutdownTimeout bounds how long Stop waits for clients to finish.
	ShutdownTimeout time.Duration
	// Connection configures each accepted connection.
	Connection ConnectionConfig
}

// withDefaults fills unset fields so a zero config still runs.
func (c ListenerConfig) withDefaults() ListenerConfig {
	if c.Address == "" {
		c.Address = ":1883"
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
	if c.Connection.OutboundQueueSize <= 0 {
		c.Connection.OutboundQueueSize = 256
	}
	if c.Connection.ReadBufferSize <= 0 {
		c.Connection.ReadBufferSize = 4096
	}
	if c.Connection.WriteTimeout <= 0 {
		c.Connection.WriteTimeout = 10 * time.Second
	}
	if c.Connection.ConnectTimeout <= 0 {
		c.Connection.ConnectTimeout = 10 * time.Second
	}
	return c
}

// Listener accepts MQTT connections over TCP.
type Listener struct {
	handler Handlers
	clock   clock.Clock
	metrics metrics.Recorder
	log     logger.Logger
	cfg     ListenerConfig

	listener net.Listener
	// connections tracks live connections so shutdown can close them all.
	connections sync.Map
	connCount   atomic.Int64

	wg      sync.WaitGroup
	cancel  context.CancelFunc
	started atomic.Bool
}

// NewListener builds a Listener.
func NewListener(
	handler Handlers,
	clk clock.Clock,
	recorder metrics.Recorder,
	log logger.Logger,
	cfg ListenerConfig,
) *Listener {
	return &Listener{
		handler: handler,
		clock:   clk,
		metrics: recorder,
		log:     log,
		cfg:     cfg.withDefaults(),
	}
}

// Addr returns the bound address, useful when the config asked for port 0 and
// the OS chose one — which is how the test suite runs many brokers in parallel.
func (l *Listener) Addr() net.Addr {
	if l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}

// ConnectionCount returns how many connections are live.
func (l *Listener) ConnectionCount() int64 { return l.connCount.Load() }

// Start binds the socket and begins accepting.
//
// It returns once the socket is bound, so the caller knows the port is live
// before it reports readiness; the accept loop continues in the background.
func (l *Listener) Start(ctx context.Context) error {
	if !l.started.CompareAndSwap(false, true) {
		return appErrors.Conflict("listener_already_started", "listener is already running")
	}

	listener, err := net.Listen("tcp", l.cfg.Address)
	if err != nil {
		l.started.Store(false)
		return appErrors.Wrap(
			appErrors.KindUnavailable,
			"listen_failed",
			"failed to bind "+l.cfg.Address,
			err,
		)
	}
	l.listener = listener

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l.cancel = cancel

	l.wg.Add(1)
	go l.acceptLoop(runCtx)

	l.log.Info(ctx, "mqtt listener started",
		logger.F("address", listener.Addr().String()),
		logger.F("max_connections", l.cfg.MaxConnections),
	)
	return nil
}

// acceptLoop accepts connections until the listener closes.
func (l *Listener) acceptLoop(ctx context.Context) {
	defer l.wg.Done()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Expected: Stop closed the listener.
				return
			default:
			}

			// A temporary accept error — file descriptors exhausted, say — must
			// not kill the whole broker. Back off briefly and carry on, so the
			// listener recovers when the pressure passes.
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}

			l.log.Error(ctx, "accept failed", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if l.cfg.MaxConnections > 0 && l.connCount.Load() >= int64(l.cfg.MaxConnections) {
			l.log.Warn(ctx, "refused a connection at the configured limit",
				logger.F("remote_addr", conn.RemoteAddr().String()),
				logger.F("limit", l.cfg.MaxConnections),
			)
			_ = conn.Close()
			l.metrics.RecordConnectionRejected("connection_limit")
			continue
		}

		l.serveConnection(ctx, conn)
	}
}

// serveConnection starts a goroutine for one accepted socket.
func (l *Listener) serveConnection(ctx context.Context, conn net.Conn) {
	// TCP_NODELAY. MQTT packets are small and latency-sensitive, and Nagle's
	// algorithm would hold a PUBACK back for up to 40ms waiting for more bytes
	// to coalesce — which is most of the latency budget for a QoS 1 round trip.
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	client := NewConnection(conn, l.handler, l.clock, l.metrics, l.log, l.cfg.Connection)

	l.connections.Store(client, struct{}{})
	l.connCount.Add(1)

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer func() {
			l.connections.Delete(client)
			l.connCount.Add(-1)
		}()

		client.Serve(ctx)
	}()
}

// Stop closes the listener and every live connection, then waits.
//
// The order is what makes the shutdown graceful: closing the listener first
// means no new client is accepted into a broker that is going away, and only
// then are the existing ones told to finish.
func (l *Listener) Stop(ctx context.Context) error {
	if !l.started.Load() {
		return nil
	}

	if l.cancel != nil {
		l.cancel()
	}
	if l.listener != nil {
		_ = l.listener.Close()
	}

	l.log.Info(ctx, "mqtt listener draining",
		logger.F("connections", l.connCount.Load()),
		logger.F("timeout", l.cfg.ShutdownTimeout.String()),
	)

	l.connections.Range(func(key, _ any) bool {
		if conn, ok := key.(*Connection); ok {
			// ReasonServerShutdown is not graceful by §3.1.2.5, so each client's
			// will is published — which is exactly right: subscribers watching
			// a device's status topic should learn it went away.
			conn.Close(clientAgg.ReasonServerShutdown)
		}
		return true
	})

	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		l.started.Store(false)
		l.log.Info(ctx, "mqtt listener stopped cleanly")
		return nil

	case <-time.After(l.cfg.ShutdownTimeout):
		l.started.Store(false)
		// Not an error: the sockets are closed and the goroutines are unblocked
		// or about to be. Reporting a failure here would make a routine restart
		// look broken.
		l.log.Warn(ctx, "mqtt listener shutdown timed out with connections still closing",
			logger.F("remaining", l.connCount.Load()))
		return nil
	}
}
