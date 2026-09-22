// Package transport owns the network: accepting TCP connections and running
// the per-client goroutines that read and write MQTT packets.
//
// It is the only package that touches a socket. Everything it learns from the
// wire it hands to the application services as decoded packets, and everything
// they want to send comes back as a packet — so the broker's protocol logic is
// testable with no network, and this package's job is narrow enough to reason
// about: bytes in, bytes out, and the lifetime of two goroutines.
package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anashasan/gomqtt/pkg/application/services"
	"github.com/anashasan/gomqtt/pkg/common/clock"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

// ErrClientQueueFull is returned by Send when a client's outbound queue is
// full.
//
// It is an error rather than a block, and that is the single most important
// decision in this file. If Send blocked, one subscriber that stopped reading
// its socket would stall the publisher goroutine that was delivering to it —
// and since a publisher fans out to many subscribers in sequence, one dead
// client would stop delivery to every other client too. Failing fast turns a
// broker-wide outage into one slow client losing messages.
var ErrClientQueueFull = errors.New("transport: client outbound queue is full")

// ErrConnectionClosed is returned when a packet is sent to a closed connection.
var ErrConnectionClosed = errors.New("transport: connection is closed")

// ConnectionConfig tunes one client connection.
type ConnectionConfig struct {
	// OutboundQueueSize is how many packets may await writing per client.
	//
	// The buffer absorbs a burst; it does not fix a client that is persistently
	// slower than its traffic. Larger means more tolerance and more memory per
	// client — at 10,000 clients, every slot is 10,000 slots.
	OutboundQueueSize int
	// ReadBufferSize sizes the bufio.Reader over the socket.
	ReadBufferSize int
	// WriteTimeout bounds a single socket write, so a client that has stopped
	// reading cannot pin its writer goroutine forever.
	WriteTimeout time.Duration
	// MaxPacketSize bounds one inbound packet.
	MaxPacketSize int
	// ConnectTimeout is how long a new connection has to send its CONNECT.
	//
	// Without it, opening a socket and sending nothing costs an attacker one
	// file descriptor and costs the broker one goroutine plus its buffers, for
	// as long as they care to hold it.
	ConnectTimeout time.Duration
	// MaxPayloadBytes caps a published payload.
	MaxPayloadBytes int
}

// Handlers are the application services a connection drives.
//
// Grouped into one struct because a connection needs all three and passing them
// individually would make the constructor's signature unreadable — and because
// it keeps di's binding in one place.
type Handlers struct {
	Connection   services.IConnectionService
	Subscription services.ISubscriptionService
	Publishing   services.IPublishingService
}

// Connection is one client's TCP connection.
//
// Two goroutines per client, and the split is deliberate: the reader blocks on
// the socket, and the writer blocks on a channel. If one goroutine did both, a
// client that sent nothing would block the reads and delay every message the
// broker wanted to push to it. Separating them is what lets a subscriber that
// never speaks still receive traffic promptly.
type Connection struct {
	conn    net.Conn
	reader  *packet.Reader
	writer  *packet.Writer
	handler Handlers
	clock   clock.Clock
	metrics metrics.Recorder
	log     logger.Logger
	cfg     ConnectionConfig

	// outbound carries packets from any goroutine to this client's writer.
	outbound chan packet.Packet

	// client is set once CONNECT is accepted. Read by the keep-alive loop from
	// another goroutine, so it is guarded.
	mu     sync.RWMutex
	client *clientAgg.Client

	closeOnce sync.Once
	// done is closed when the connection is shutting down, which is how the
	// writer and the keep-alive loop learn to stop.
	done chan struct{}
	// closeReason records why, so teardown reports the true cause rather than
	// the read error that followed it.
	closeReason atomic.Value
}

// NewConnection wraps a socket.
func NewConnection(
	conn net.Conn,
	handler Handlers,
	clk clock.Clock,
	recorder metrics.Recorder,
	log logger.Logger,
	cfg ConnectionConfig,
) *Connection {
	return &Connection{
		conn:     conn,
		reader:   packet.NewReader(conn, cfg.ReadBufferSize, cfg.MaxPacketSize),
		writer:   packet.NewWriter(conn),
		handler:  handler,
		clock:    clk,
		metrics:  recorder,
		log:      log,
		cfg:      cfg,
		outbound: make(chan packet.Packet, cfg.OutboundQueueSize),
		done:     make(chan struct{}),
	}
}

// ---------------------------------------------------------------------------
// PacketSink
// ---------------------------------------------------------------------------

var _ services.PacketSink = (*Connection)(nil)

// Send queues a packet for this client.
//
// Never blocks: the select's default arm turns a full queue into an error the
// publishing service counts as a drop.
func (c *Connection) Send(p packet.Packet) error {
	select {
	case <-c.done:
		return ErrConnectionClosed
	default:
	}

	select {
	case c.outbound <- p:
		return nil
	case <-c.done:
		return ErrConnectionClosed
	default:
		return ErrClientQueueFull
	}
}

// Close terminates the connection.
func (c *Connection) Close(reason clientAgg.DisconnectReason) {
	c.closeOnce.Do(func() {
		c.closeReason.Store(reason)
		close(c.done)
		// Closing the socket is what unblocks the reader goroutine, which is
		// otherwise parked in a blocking Read that no channel can interrupt.
		_ = c.conn.Close()
	})
}

// ClientID returns the connected client's identifier, empty before CONNECT.
func (c *Connection) ClientID() clientVO.ClientID {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client == nil {
		return ""
	}
	return c.client.ID()
}

// currentClient returns the connected client under the lock.
func (c *Connection) currentClient() *clientAgg.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client
}

// reason returns the recorded close reason.
func (c *Connection) reason() clientAgg.DisconnectReason {
	if v, ok := c.closeReason.Load().(clientAgg.DisconnectReason); ok {
		return v
	}
	return clientAgg.ReasonNetworkError
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Serve runs the connection until it closes. It blocks, and the caller runs it
// in its own goroutine per accepted socket.
func (c *Connection) Serve(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.writeLoop(ctx)
	}()

	// The read loop runs on this goroutine rather than a third one: Serve has
	// to block anyway, so spending a goroutine to make it block on a WaitGroup
	// instead would be one more stack per client for nothing.
	c.readLoop(ctx)

	c.Close(c.reason())
	wg.Wait()
	c.teardown(ctx)
}

// readLoop decodes packets until the connection ends.
func (c *Connection) readLoop(ctx context.Context) {
	// §3.1: the first packet must be CONNECT, and it must arrive promptly.
	if err := c.awaitConnect(ctx); err != nil {
		return
	}

	// The keep-alive loop starts only after CONNECT, because the interval it
	// enforces is negotiated by that packet.
	go c.keepAliveLoop(ctx)

	for {
		select {
		case <-c.done:
			return
		default:
		}

		c.applyReadDeadline()

		p, err := c.reader.ReadPacket()
		if err != nil {
			c.handleReadError(ctx, err)
			return
		}

		c.metrics.RecordPacketReceived(p.Type().String(), 0)

		// §3.1.2.10: *any* inbound packet resets the keep-alive timer, not only
		// PINGREQ. A broker that only counts pings will disconnect its busiest
		// clients.
		if client := c.currentClient(); client != nil {
			client.RecordActivity(c.clock.Now())
		}

		if err := c.dispatch(ctx, p); err != nil {
			c.log.Warn(ctx, "closing connection after a protocol error",
				logger.F("client_id", c.ClientID().String()),
				logger.F("packet_type", p.Type().String()),
				logger.F("error", err.Error()),
			)
			c.Close(clientAgg.ReasonProtocolViolation)
			return
		}
	}
}

// awaitConnect reads and processes the mandatory first CONNECT.
func (c *Connection) awaitConnect(ctx context.Context) error {
	if c.cfg.ConnectTimeout > 0 {
		_ = c.conn.SetReadDeadline(c.clock.Now().Add(c.cfg.ConnectTimeout))
	}

	p, err := c.reader.ReadPacket()
	if err != nil {
		c.Close(clientAgg.ReasonNetworkError)
		return err
	}

	connect, ok := p.(*packet.Connect)
	if !ok {
		// §3.1.0: any other first packet is a protocol violation, and the
		// server must close the connection without a CONNACK.
		c.log.Warn(ctx, "first packet was not CONNECT",
			logger.F("packet_type", p.Type().String()),
			logger.F("remote_addr", c.conn.RemoteAddr().String()),
		)
		c.Close(clientAgg.ReasonProtocolViolation)
		return errors.New("first packet was not CONNECT")
	}

	return c.handleConnect(ctx, connect)
}

// handleConnect accepts or refuses the client.
func (c *Connection) handleConnect(ctx context.Context, req *packet.Connect) error {
	result, err := c.handler.Connection.Connect(ctx, req, c.conn.RemoteAddr().String(), c)
	if err != nil {
		c.Close(clientAgg.ReasonNetworkError)
		return err
	}

	// The CONNACK is written directly rather than queued, because the writer
	// goroutine's ordering guarantee only matters once the client exists — and
	// a refused client must receive its CONNACK before the socket closes.
	connack := &packet.Connack{
		SessionPresent: result.SessionPresent,
		ReturnCode:     result.ReturnCode,
	}
	if err := c.writePacketNow(connack); err != nil {
		c.Close(clientAgg.ReasonNetworkError)
		return err
	}

	if !result.Accepted() {
		c.log.Info(ctx, "rejected a connection",
			logger.F("return_code", result.ReturnCode.String()),
			logger.F("remote_addr", c.conn.RemoteAddr().String()),
		)
		c.Close(clientAgg.ReasonProtocolViolation)
		return errors.New("connect rejected: " + result.ReturnCode.String())
	}

	c.mu.Lock()
	c.client = result.Client
	c.mu.Unlock()

	// §3.1.4: close the connection this one took over from. Done after the new
	// client is fully installed, so the old connection's teardown cannot
	// unregister the new one.
	if result.Displaced != nil {
		result.Displaced.Close(clientAgg.ReasonSessionTakenOver)
	}

	// §4.4: a resumed session redelivers its unacknowledged messages and its
	// offline queue. Only meaningful when the session was actually present.
	if result.SessionPresent {
		if err := c.handler.Publishing.ResumeSession(ctx, c); err != nil {
			c.log.Error(ctx, "failed to resume session backlog", err,
				logger.F("client_id", result.Client.ID().String()))
		}
	}
	return nil
}

// dispatch routes a decoded packet to its handler.
//
// Returning an error closes the connection: at this layer every error is a
// protocol violation, because anything recoverable was already handled inside
// the service.
func (c *Connection) dispatch(ctx context.Context, p packet.Packet) error {
	client := c.currentClient()
	if client == nil {
		return errors.New("packet received before CONNECT")
	}

	switch pkt := p.(type) {
	case *packet.Connect:
		// §3.1.0: a second CONNECT on one connection is a protocol violation.
		return errors.New("a second CONNECT is a protocol violation")

	case *packet.Publish:
		return c.handlePublish(ctx, pkt)

	case *packet.Subscribe:
		return c.handleSubscribe(ctx, client, pkt)

	case *packet.Unsubscribe:
		return c.handleUnsubscribe(ctx, client, pkt)

	case *packet.Ack:
		return c.handleAck(ctx, client, pkt)

	case *packet.Simple:
		return c.handleSimple(ctx, pkt)

	default:
		// A CONNACK, SUBACK or PINGRESP from a client is a server-to-client
		// packet arriving in the wrong direction.
		return errors.New("unexpected packet type from client: " + p.Type().String())
	}
}

// handlePublish accepts an inbound publication.
func (c *Connection) handlePublish(ctx context.Context, p *packet.Publish) error {
	qos, err := msgVO.NewQoS(p.QoS)
	if err != nil {
		return err
	}
	// QoS 2 is decoded and understood but the PUBREC/PUBREL/PUBCOMP flow is not
	// implemented. Closing the connection is the honest answer: silently
	// treating it as QoS 1 would tell the publisher its exactly-once guarantee
	// was honoured when it was not.
	if !qos.IsSupported() {
		return errors.New("QoS 2 publish is not supported by this broker")
	}

	topic, err := msgVO.NewTopicName(p.Topic)
	if err != nil {
		return err
	}

	message, err := msgAgg.NewMessage(msgAgg.NewMessageParams{
		Topic:           topic,
		Payload:         p.Payload,
		QoS:             qos,
		Retain:          p.Retain,
		DUP:             p.DUP,
		PacketID:        p.PacketID,
		Now:             c.clock.Now(),
		MaxPayloadBytes: c.cfg.MaxPayloadBytes,
	})
	if err != nil {
		return err
	}

	if err := c.handler.Publishing.Publish(ctx, message); err != nil {
		return err
	}

	// §3.3.4: QoS 1 is acknowledged *after* the broker has taken
	// responsibility for the message. Acknowledging first would mean a crash
	// between the two loses a message the publisher believes was accepted.
	if qos == msgVO.QoSAtLeastOnce {
		return c.send(packet.NewPuback(p.PacketID))
	}
	return nil
}

// handleSubscribe registers filters and answers with a SUBACK.
func (c *Connection) handleSubscribe(
	ctx context.Context,
	client *clientAgg.Client,
	p *packet.Subscribe,
) error {
	returnCodes, err := c.handler.Subscription.Subscribe(ctx, client, p, c)
	if err != nil {
		return err
	}
	return c.send(&packet.Suback{PacketID: p.PacketID, ReturnCodes: returnCodes})
}

// handleUnsubscribe removes filters and answers with an UNSUBACK.
func (c *Connection) handleUnsubscribe(
	ctx context.Context,
	client *clientAgg.Client,
	p *packet.Unsubscribe,
) error {
	if err := c.handler.Subscription.Unsubscribe(ctx, client, p); err != nil {
		return err
	}
	return c.send(packet.NewUnsuback(p.PacketID))
}

// handleAck processes an inbound acknowledgement.
func (c *Connection) handleAck(
	ctx context.Context,
	client *clientAgg.Client,
	p *packet.Ack,
) error {
	switch p.Type() {
	case packet.PUBACK:
		if err := c.handler.Publishing.Acknowledge(ctx, client.ID(), p.PacketID); err != nil {
			// An unsolicited PUBACK is a protocol violation (§4.4), but it is
			// logged and tolerated rather than fatal: some client libraries
			// retransmit an acknowledgement after a reconnect, and killing the
			// connection over it would be worse than ignoring it.
			c.log.Warn(ctx, "received an unexpected PUBACK",
				logger.F("client_id", client.ID().String()),
				logger.F("packet_id", p.PacketID),
			)
		}
		return nil

	case packet.PUBREC, packet.PUBREL, packet.PUBCOMP:
		return errors.New("QoS 2 is not supported by this broker")

	default:
		return errors.New("unexpected acknowledgement type: " + p.Type().String())
	}
}

// handleSimple processes PINGREQ and DISCONNECT.
func (c *Connection) handleSimple(ctx context.Context, p *packet.Simple) error {
	switch p.Type() {
	case packet.PINGREQ:
		return c.send(packet.NewPingresp())

	case packet.DISCONNECT:
		// §3.14.4: a DISCONNECT is a graceful close, and the will must *not*
		// be published. Recording the reason before closing is what carries
		// that distinction into teardown.
		c.Close(clientAgg.ReasonClientDisconnect)
		return nil

	default:
		return errors.New("unexpected packet type: " + p.Type().String())
	}
}

// send queues a packet, treating a full queue as fatal for this connection.
//
// Protocol responses — SUBACK, PUBACK, PINGRESP — are not droppable the way a
// published message is: a client that never receives its SUBACK is stuck
// forever. If the queue is too full even for these, the connection is not
// working and closing it is the honest outcome.
func (c *Connection) send(p packet.Packet) error {
	if err := c.Send(p); err != nil {
		return err
	}
	return nil
}

// writePacketNow writes straight to the socket, bypassing the queue. Used only
// for the CONNACK, before the writer goroutine owns the connection.
func (c *Connection) writePacketNow(p packet.Packet) error {
	if c.cfg.WriteTimeout > 0 {
		_ = c.conn.SetWriteDeadline(c.clock.Now().Add(c.cfg.WriteTimeout))
	}
	return c.writer.WritePacket(p)
}

// writeLoop drains the outbound queue onto the socket.
//
// One goroutine, and therefore one writer: two goroutines writing to the same
// TCP connection would interleave two packets' bytes and corrupt the stream
// irrecoverably. That is why Writer is documented as not safe for concurrent
// use — this loop is the reason it does not need to be.
func (c *Connection) writeLoop(ctx context.Context) {
	for {
		select {
		case <-c.done:
			// Drain what is already queued before going away, so a DISCONNECT
			// or a final CONNACK is not lost. Bounded by what is in the buffer,
			// so this cannot delay shutdown indefinitely.
			c.drainOutbound()
			return

		case p := <-c.outbound:
			if c.cfg.WriteTimeout > 0 {
				_ = c.conn.SetWriteDeadline(c.clock.Now().Add(c.cfg.WriteTimeout))
			}
			if err := c.writer.WritePacket(p); err != nil {
				c.log.Debug(ctx, "write failed",
					logger.F("client_id", c.ClientID().String()),
					logger.F("error", err.Error()),
				)
				c.Close(clientAgg.ReasonNetworkError)
				return
			}
			c.metrics.RecordPacketSent(p.Type().String(), 0)
		}
	}
}

// drainOutbound writes whatever is already queued, without blocking.
func (c *Connection) drainOutbound() {
	for {
		select {
		case p := <-c.outbound:
			if c.cfg.WriteTimeout > 0 {
				_ = c.conn.SetWriteDeadline(c.clock.Now().Add(c.cfg.WriteTimeout))
			}
			if err := c.writer.WritePacket(p); err != nil {
				return
			}
		default:
			return
		}
	}
}

// keepAliveLoop disconnects a client that has gone silent (§3.1.2.10).
func (c *Connection) keepAliveLoop(ctx context.Context) {
	client := c.currentClient()
	if client == nil || client.KeepAlive().IsDisabled() {
		return
	}

	// Checked several times per interval rather than once, so a client is
	// disconnected soon after its deadline rather than up to a full interval
	// later.
	interval := client.KeepAlive().Duration() / 2
	if interval < time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if client.IsKeepAliveExpired(c.clock.Now()) {
				c.log.Info(ctx, "disconnecting a client that missed its keep-alive window",
					logger.F("client_id", client.ID().String()),
					logger.F("keep_alive", client.KeepAlive().Seconds()),
				)
				c.Close(clientAgg.ReasonKeepAliveTimeout)
				return
			}
		}
	}
}

// applyReadDeadline bounds a blocking read by the keep-alive window.
//
// Belt and braces alongside keepAliveLoop: the loop notices a silent client,
// and this makes sure the read itself cannot block past the deadline even if
// the loop is not running.
func (c *Connection) applyReadDeadline() {
	client := c.currentClient()
	if client == nil {
		return
	}

	timeout := client.KeepAlive().Timeout()
	if timeout <= 0 {
		// Keep-alive disabled: clear any deadline left from the CONNECT wait,
		// or the connection would die at the connect timeout.
		_ = c.conn.SetReadDeadline(time.Time{})
		return
	}
	_ = c.conn.SetReadDeadline(c.clock.Now().Add(timeout))
}

// handleReadError classifies why the read loop ended.
func (c *Connection) handleReadError(ctx context.Context, err error) {
	switch {
	case errors.Is(err, io.EOF):
		// The peer closed between packets without DISCONNECT. Abnormal by
		// §3.1.2.5, so the will is published.
		c.Close(clientAgg.ReasonNetworkError)

	case errors.Is(err, packet.ErrMalformedPacket),
		errors.Is(err, packet.ErrInvalidQoS),
		errors.Is(err, packet.ErrInvalidUTF8),
		errors.Is(err, packet.ErrInvalidPacketID),
		errors.Is(err, packet.ErrPacketTooLarge),
		errors.Is(err, packet.ErrRemainingLengthOverflow):
		c.log.Warn(ctx, "closing connection after a malformed packet",
			logger.F("client_id", c.ClientID().String()),
			logger.F("remote_addr", c.conn.RemoteAddr().String()),
			logger.F("error", err.Error()),
		)
		c.Close(clientAgg.ReasonProtocolViolation)

	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			c.Close(clientAgg.ReasonKeepAliveTimeout)
			return
		}
		c.Close(clientAgg.ReasonNetworkError)
	}
}

// teardown releases everything the connection held.
func (c *Connection) teardown(ctx context.Context) {
	client := c.currentClient()
	if client == nil {
		// Never got past CONNECT: nothing was registered, so there is nothing
		// to unwind.
		return
	}

	c.handler.Publishing.UnregisterSink(client.ID(), c)

	if err := c.handler.Connection.Disconnect(ctx, client, c.reason()); err != nil {
		c.log.Error(ctx, "failed to clean up a disconnected client", err,
			logger.F("client_id", client.ID().String()))
	}
}
