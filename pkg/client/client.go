// Package client is a small MQTT 3.1.1 client built on the project's own codec.
//
// It exists for two jobs: the end-to-end tests, and the load generator. Sharing
// the codec with the broker is a deliberate trade — it keeps the project
// dependency-free, but it means a bug in the codec could make client and broker
// agree with each other while both disagree with the standard. That risk is
// covered from the outside: the interop suite runs this client against real
// Mosquitto, and real mosquitto_pub/mosquitto_sub against this broker, so the
// wire format is checked against an independent implementation in both
// directions.
package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

// Errors returned by the client.
var (
	// ErrNotConnected means an operation was attempted before Connect.
	ErrNotConnected = errors.New("mqtt client: not connected")
	// ErrConnectionRefused means the broker returned a non-zero CONNACK code.
	ErrConnectionRefused = errors.New("mqtt client: connection refused")
	// ErrTimeout means the broker did not answer in time.
	ErrTimeout = errors.New("mqtt client: timed out waiting for the broker")
)

// Message is a message received from the broker.
type Message struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
	DUP      bool
	PacketID uint16
	// ReceivedAt is stamped on arrival so the load generator can measure
	// end-to-end latency without a second clock read in the handler.
	ReceivedAt time.Time
}

// Options configures a Client.
type Options struct {
	// Address is the broker's host:port.
	Address string
	// ClientID is the MQTT client identifier. Empty asks the broker to assign
	// one, which requires CleanSession.
	ClientID string
	// CleanSession asks the broker to discard any stored session.
	CleanSession bool
	// KeepAlive is the keep-alive interval in seconds. Zero disables it.
	KeepAlive uint16
	Username  string
	Password  string
	// ConnectTimeout bounds the TCP dial and the CONNACK wait.
	ConnectTimeout time.Duration
	// AckTimeout bounds waiting for a PUBACK, SUBACK or UNSUBACK.
	AckTimeout time.Duration
	// OnMessage is called for every inbound PUBLISH, on the read goroutine.
	//
	// It must not block: doing so stalls the client's whole read loop, which
	// stops acknowledgements and eventually trips the broker's keep-alive.
	OnMessage func(Message)
	// Will is the last-will message, optional.
	Will *packet.Will
	// SuppressPing stops the client sending PINGREQ even when KeepAlive is set.
	//
	// It exists so a test can simulate a client that has gone silent and assert
	// that the broker enforces its keep-alive window — without it, the client's
	// own pinger keeps the connection alive and the enforcement is untested.
	SuppressPing bool
}

// withDefaults fills unset fields.
func (o Options) withDefaults() Options {
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 10 * time.Second
	}
	if o.AckTimeout <= 0 {
		o.AckTimeout = 10 * time.Second
	}
	return o
}

// Client is a connected MQTT client.
type Client struct {
	opts Options

	conn   net.Conn
	reader *packet.Reader
	writer *packet.Writer

	// writeMu serialises writes. Two goroutines writing to one TCP connection
	// would interleave their bytes and corrupt the stream, so every write goes
	// through this lock.
	writeMu sync.Mutex

	// pending maps a packet identifier to the channel its acknowledgement
	// wakes. One channel per in-flight request rather than a shared one, so a
	// slow PUBACK cannot deliver itself to the wrong waiter.
	pendingMu sync.Mutex
	pending   map[uint16]chan packet.Packet

	nextPacketID atomic.Uint32

	closeOnce sync.Once
	done      chan struct{}
	readErr   atomic.Value

	// SessionPresent records the CONNACK flag, which tells a reconnecting
	// client whether it must resubscribe.
	SessionPresent bool
}

// New builds an unconnected Client.
func New(opts Options) *Client {
	return &Client{
		opts:    opts.withDefaults(),
		pending: make(map[uint16]chan packet.Packet),
		done:    make(chan struct{}),
	}
}

// Connect dials the broker and completes the MQTT handshake.
func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.opts.Address, c.opts.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("mqtt client: dial %s: %w", c.opts.Address, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	c.conn = conn
	c.reader = packet.NewReader(conn, 4096, packet.DefaultMaxPacketSize)
	c.writer = packet.NewWriter(conn)
	c.nextPacketID.Store(0)

	connect := &packet.Connect{
		ProtocolName:  packet.ProtocolName,
		ProtocolLevel: packet.ProtocolLevel311,
		CleanSession:  c.opts.CleanSession,
		KeepAlive:     c.opts.KeepAlive,
		ClientID:      c.opts.ClientID,
		Will:          c.opts.Will,
	}
	if c.opts.Username != "" {
		connect.HasUsername = true
		connect.Username = c.opts.Username
	}
	if c.opts.Password != "" {
		connect.HasPassword = true
		connect.Password = []byte(c.opts.Password)
	}

	if err := c.write(connect); err != nil {
		_ = conn.Close()
		return err
	}

	// The CONNACK is read synchronously, before the read loop starts: until the
	// handshake completes there is nothing else the connection could carry, and
	// reading it here keeps the error path simple.
	_ = conn.SetReadDeadline(time.Now().Add(c.opts.ConnectTimeout))
	p, err := c.reader.ReadPacket()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("mqtt client: reading CONNACK: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	connack, ok := p.(*packet.Connack)
	if !ok {
		_ = conn.Close()
		return fmt.Errorf("mqtt client: expected CONNACK, got %s", p.Type())
	}
	if connack.ReturnCode != packet.ConnectAccepted {
		_ = conn.Close()
		return fmt.Errorf("%w: %s", ErrConnectionRefused, connack.ReturnCode)
	}

	c.SessionPresent = connack.SessionPresent

	go c.readLoop()
	if c.opts.KeepAlive > 0 && !c.opts.SuppressPing {
		go c.keepAliveLoop()
	}
	return nil
}

// Publish sends a message and, at QoS 1, waits for its PUBACK.
func (c *Client) Publish(topic string, payload []byte, qos byte, retain bool) error {
	if c.conn == nil {
		return ErrNotConnected
	}

	p := &packet.Publish{
		QoS:     qos,
		Retain:  retain,
		Topic:   topic,
		Payload: payload,
	}

	if qos == 0 {
		return c.write(p)
	}

	packetID := c.allocatePacketID()
	p.PacketID = packetID

	ack := c.expect(packetID)
	defer c.forget(packetID)

	if err := c.write(p); err != nil {
		return err
	}
	return c.await(ack)
}

// Subscribe registers a topic filter and returns the granted QoS.
func (c *Client) Subscribe(filter string, qos byte) (byte, error) {
	if c.conn == nil {
		return 0, ErrNotConnected
	}

	packetID := c.allocatePacketID()
	ack := c.expect(packetID)
	defer c.forget(packetID)

	err := c.write(&packet.Subscribe{
		PacketID:      packetID,
		Subscriptions: []packet.TopicSubscription{{Filter: filter, QoS: qos}},
	})
	if err != nil {
		return 0, err
	}

	select {
	case p := <-ack:
		suback, ok := p.(*packet.Suback)
		if !ok || len(suback.ReturnCodes) == 0 {
			return 0, errors.New("mqtt client: malformed SUBACK")
		}
		if suback.ReturnCodes[0] == packet.SubackFailure {
			return 0, fmt.Errorf("mqtt client: subscription to %q was refused", filter)
		}
		return suback.ReturnCodes[0], nil

	case <-time.After(c.opts.AckTimeout):
		return 0, ErrTimeout

	case <-c.done:
		return 0, c.closedError()
	}
}

// Unsubscribe removes a topic filter.
func (c *Client) Unsubscribe(filter string) error {
	if c.conn == nil {
		return ErrNotConnected
	}

	packetID := c.allocatePacketID()
	ack := c.expect(packetID)
	defer c.forget(packetID)

	if err := c.write(&packet.Unsubscribe{PacketID: packetID, Filters: []string{filter}}); err != nil {
		return err
	}
	return c.await(ack)
}

// Ping sends a PINGREQ. Exposed so a test can exercise keep-alive explicitly.
func (c *Client) Ping() error {
	if c.conn == nil {
		return ErrNotConnected
	}
	return c.write(packet.NewPingreq())
}

// Disconnect sends DISCONNECT and closes the connection.
//
// The distinction matters to the broker: a clean DISCONNECT suppresses the
// will, while dropping the socket publishes it (§3.1.2.5).
func (c *Client) Disconnect() error {
	if c.conn == nil {
		return nil
	}
	err := c.write(packet.NewDisconnect())
	c.close()
	return err
}

// Close drops the connection without sending DISCONNECT, which the broker sees
// as an abnormal disconnect and answers by publishing the will.
func (c *Client) Close() { c.close() }

// close shuts the client down exactly once.
func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

// closedError reports why the connection ended.
func (c *Client) closedError() error {
	if err, ok := c.readErr.Load().(error); ok {
		return err
	}
	return ErrNotConnected
}

// write serialises a packet onto the socket.
func (c *Client) write(p packet.Packet) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.writer.WritePacket(p)
}

// allocatePacketID hands out the next identifier, wrapping past zero, which is
// reserved (§2.3.1).
func (c *Client) allocatePacketID() uint16 {
	for {
		next := c.nextPacketID.Add(1)
		if id := uint16(next); id != 0 {
			return id
		}
	}
}

// expect registers a waiter for an acknowledgement.
func (c *Client) expect(packetID uint16) chan packet.Packet {
	// Buffered, so the read loop can deliver the acknowledgement and move on
	// even if the waiter has already timed out and stopped listening.
	ch := make(chan packet.Packet, 1)

	c.pendingMu.Lock()
	c.pending[packetID] = ch
	c.pendingMu.Unlock()

	return ch
}

// forget removes a waiter.
func (c *Client) forget(packetID uint16) {
	c.pendingMu.Lock()
	delete(c.pending, packetID)
	c.pendingMu.Unlock()
}

// await blocks for an acknowledgement.
func (c *Client) await(ack chan packet.Packet) error {
	select {
	case <-ack:
		return nil
	case <-time.After(c.opts.AckTimeout):
		return ErrTimeout
	case <-c.done:
		return c.closedError()
	}
}

// readLoop dispatches inbound packets.
func (c *Client) readLoop() {
	defer c.close()

	for {
		p, err := c.reader.ReadPacket()
		if err != nil {
			c.readErr.Store(err)
			return
		}

		switch pkt := p.(type) {
		case *packet.Publish:
			c.handlePublish(pkt)

		case *packet.Ack:
			c.deliver(pkt.PacketID, pkt)

		case *packet.Suback:
			c.deliver(pkt.PacketID, pkt)

		case *packet.Simple:
			// PINGRESP needs no handling beyond proving the broker is alive,
			// which the successful read already did.
		}
	}
}

// handlePublish delivers an inbound message and acknowledges it at QoS 1.
func (c *Client) handlePublish(p *packet.Publish) {
	if c.opts.OnMessage != nil {
		c.opts.OnMessage(Message{
			Topic:      p.Topic,
			Payload:    p.Payload,
			QoS:        p.QoS,
			Retained:   p.Retain,
			DUP:        p.DUP,
			PacketID:   p.PacketID,
			ReceivedAt: time.Now(),
		})
	}

	// Acknowledged after the handler has run, so the broker does not consider
	// the message delivered until this client has actually processed it.
	if p.QoS == 1 {
		_ = c.write(packet.NewPuback(p.PacketID))
	}
}

// deliver wakes the waiter for an acknowledgement.
func (c *Client) deliver(packetID uint16, p packet.Packet) {
	c.pendingMu.Lock()
	ch, ok := c.pending[packetID]
	c.pendingMu.Unlock()

	if !ok {
		return
	}

	select {
	case ch <- p:
	default:
		// The waiter gave up. The channel is buffered, so reaching here means a
		// duplicate acknowledgement, which is safe to discard.
	}
}

// keepAliveLoop sends PINGREQ at half the negotiated interval.
//
// Half rather than the full interval: pinging exactly on the deadline leaves no
// margin for network jitter, and a broker allowing 1.5x would still be within
// its rights to disconnect a client whose ping was delayed.
func (c *Client) keepAliveLoop() {
	interval := time.Duration(c.opts.KeepAlive) * time.Second / 2
	if interval < time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.Ping(); err != nil {
				return
			}
		}
	}
}
