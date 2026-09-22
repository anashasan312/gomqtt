// Package client_aggregate contains the Client aggregate: one connected
// client's identity, its negotiated connection parameters, and its liveness.
//
// A Client exists only while a TCP connection does. What survives a disconnect
// is the Session, keyed by the same client identifier — keeping them as separate
// aggregates is what makes "persistent session" expressible at all.
package client_aggregate

import (
	"sync"
	"time"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	clientErr "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/error"
	vo "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
)

// State is the connection's position in the MQTT handshake.
type State string

const (
	// StateConnecting means the TCP connection is up but no CONNECT has been
	// accepted yet. Only a CONNECT may arrive in this state (§3.1.0).
	StateConnecting State = "connecting"
	// StateConnected means CONNECT was accepted and CONNACK was sent.
	StateConnected State = "connected"
	// StateDisconnected means the connection is closed.
	StateDisconnected State = "disconnected"
)

// String renders the state.
func (s State) String() string { return string(s) }

// DisconnectReason records why a client's connection ended.
//
// It is a domain concept rather than a log string because it decides real
// behaviour: the will message is published on every abnormal path and
// suppressed on a clean DISCONNECT (§3.14.4).
type DisconnectReason string

const (
	// ReasonClientDisconnect is a clean DISCONNECT packet.
	ReasonClientDisconnect DisconnectReason = "client_disconnect"
	// ReasonNetworkError is a socket that failed or closed without DISCONNECT.
	ReasonNetworkError DisconnectReason = "network_error"
	// ReasonKeepAliveTimeout is a client that went silent past its window.
	ReasonKeepAliveTimeout DisconnectReason = "keep_alive_timeout"
	// ReasonProtocolViolation is a client that broke a protocol rule.
	ReasonProtocolViolation DisconnectReason = "protocol_violation"
	// ReasonSessionTakenOver is a second connection using the same client id,
	// which §3.1.4 requires the broker to resolve by closing the first.
	ReasonSessionTakenOver DisconnectReason = "session_taken_over"
	// ReasonServerShutdown is the broker stopping.
	ReasonServerShutdown DisconnectReason = "server_shutdown"
)

// String renders the reason.
func (r DisconnectReason) String() string { return string(r) }

// IsGraceful reports whether the client left cleanly.
//
// This single predicate decides whether the will is published, so the rule lives
// here rather than being re-derived at each call site.
func (r DisconnectReason) IsGraceful() bool {
	return r == ReasonClientDisconnect
}

// Client is the aggregate root for one connected client.
type Client struct {
	id           vo.ClientID
	protocol     vo.ProtocolVersion
	keepAlive    vo.KeepAlive
	cleanSession bool
	will         *msgAgg.Message
	username     string
	remoteAddr   string

	connectedAt time.Time
	// assignedID records that the broker generated the identifier because the
	// client sent an empty one.
	assignedID bool

	// mu guards the two fields that change after construction.
	//
	// A Client is genuinely shared: its read goroutine stamps activity on every
	// inbound packet, its keep-alive goroutine reads that stamp on a timer, its
	// teardown sets the state, and the admin API reads both from an HTTP
	// handler. Everything else on this aggregate is immutable for the life of
	// the connection, which is why the lock covers only these two.
	mu             sync.RWMutex
	state          State
	lastActivityAt time.Time
}

// NewClientParams is the input to NewClient.
type NewClientParams struct {
	ID           vo.ClientID
	Protocol     vo.ProtocolVersion
	KeepAlive    vo.KeepAlive
	CleanSession bool
	Will         *msgAgg.Message
	Username     string
	RemoteAddr   string
	AssignedID   bool
	Now          time.Time
}

// NewClient builds a connected Client.
//
// It is called only after the CONNECT has been validated and accepted, so the
// aggregate never exists in a half-connected state that other code has to
// defend against.
func NewClient(p NewClientParams) (*Client, error) {
	if p.ID.IsZero() {
		return nil, errors.Invalid(clientErr.EInvalidClientID, "client id is required")
	}

	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	return &Client{
		id:             p.ID,
		protocol:       p.Protocol,
		keepAlive:      p.KeepAlive,
		cleanSession:   p.CleanSession,
		will:           p.Will,
		username:       p.Username,
		remoteAddr:     p.RemoteAddr,
		assignedID:     p.AssignedID,
		state:          StateConnected,
		connectedAt:    now,
		lastActivityAt: now,
	}, nil
}

// ID returns the client identifier.
func (c *Client) ID() vo.ClientID { return c.id }

// Protocol returns the negotiated protocol version.
func (c *Client) Protocol() vo.ProtocolVersion { return c.protocol }

// KeepAlive returns the negotiated keep-alive.
func (c *Client) KeepAlive() vo.KeepAlive { return c.keepAlive }

// CleanSession reports whether the client asked for a fresh session.
func (c *Client) CleanSession() bool { return c.cleanSession }

// Will returns the last-will message, nil when none was supplied.
func (c *Client) Will() *msgAgg.Message { return c.will }

// Username returns the authenticated username, empty when none was supplied.
func (c *Client) Username() string { return c.username }

// RemoteAddr returns the peer address, for logs and the admin API.
func (c *Client) RemoteAddr() string { return c.remoteAddr }

// State returns the connection state.
func (c *Client) State() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// ConnectedAt returns when the CONNECT was accepted.
func (c *Client) ConnectedAt() time.Time { return c.connectedAt }

// LastActivityAt returns when the client last sent a packet.
func (c *Client) LastActivityAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastActivityAt
}

// HasAssignedID reports whether the broker generated the identifier.
func (c *Client) HasAssignedID() bool { return c.assignedID }

// IsConnected reports whether the client is in the connected state.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == StateConnected
}

// RecordActivity refreshes the keep-alive deadline.
//
// §3.1.2.10 is explicit that *any* inbound control packet resets the timer, not
// only PINGREQ. A broker that resets only on PINGREQ will disconnect a client
// that is busily publishing — which is exactly the client it should keep.
func (c *Client) RecordActivity(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivityAt = now.UTC()
}

// IsKeepAliveExpired reports whether the client has gone silent past its window.
func (c *Client) IsKeepAliveExpired(now time.Time) bool {
	if c.keepAlive.IsDisabled() {
		return false
	}

	c.mu.RLock()
	last := c.lastActivityAt
	c.mu.RUnlock()

	return now.UTC().Sub(last) > c.keepAlive.Timeout()
}

// Disconnect marks the client disconnected and reports whether its will must be
// published.
//
// The decision is returned rather than left to the caller because it is a
// domain rule with one correct answer: §3.1.2.5 says the will is published on
// any disconnect other than a clean DISCONNECT, and a caller that re-derives
// that will eventually get it wrong in one of the three paths that disconnect a
// client.
func (c *Client) Disconnect(reason DisconnectReason) (publishWill bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Idempotent: teardown can be reached from the read loop, the keep-alive
	// loop and shutdown, and publishing the will twice would tell subscribers
	// the device died twice.
	if c.state == StateDisconnected {
		return false
	}
	c.state = StateDisconnected
	return c.will != nil && !reason.IsGraceful()
}
