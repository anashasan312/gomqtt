// Package session_aggregate contains the Session aggregate: everything about a
// client that outlives one TCP connection.
//
// A Session owns the client's subscriptions, its in-flight QoS 1 window, and
// the messages queued while it was offline. It is the consistency boundary for
// all three, because they change together: acknowledging an in-flight message
// frees a packet identifier, which may let a queued message move into the
// window. Splitting them across aggregates would make that sequence racy.
package session_aggregate

import (
	"time"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	sessionErr "github.com/anashasan/gomqtt/pkg/domain/session_aggregate/error"
	vo "github.com/anashasan/gomqtt/pkg/domain/session_aggregate/value_objects"
	subAgg "github.com/anashasan/gomqtt/pkg/domain/subscription_aggregate"
)

// Session limits. Every one of these exists to bound per-client memory: a
// session is created by anyone who can open a TCP connection.
const (
	// DefaultMaxInflight is how many unacknowledged QoS 1 messages may be in
	// flight to one client at once.
	//
	// It is a flow-control window, exactly like TCP's. Without it, a subscriber
	// that stops acknowledging still has messages queued for it at the full
	// publish rate, and the broker's memory becomes the slow consumer's buffer.
	DefaultMaxInflight = 32

	// DefaultMaxQueued is how many messages are held for an offline client.
	DefaultMaxQueued = 1000

	// DefaultMaxSubscriptions caps the filters one session may hold.
	DefaultMaxSubscriptions = 512
)

// InflightMessage is a QoS 1 message sent and awaiting its PUBACK.
type InflightMessage struct {
	PacketID vo.PacketID
	Message  *msgAgg.Message
	SentAt   time.Time
	// Attempts counts transmissions, so an operator can distinguish a message
	// that was delivered once from one that has been retried eleven times.
	Attempts uint32
}

// Session is the aggregate root.
//
// Fields are unexported and every change goes through a method, because the
// invariants here are the ones that decide whether a message is lost or
// duplicated: a packet identifier must not be reused while in flight, a queue
// must not grow without bound, and an acknowledgement must free exactly one
// slot.
type Session struct {
	clientID clientVO.ClientID
	// clean records the clean-session flag of the connection that created this
	// session, which decides whether it survives a disconnect (§3.1.2.4).
	clean bool

	subscriptions map[msgVO.TopicFilter]*subAgg.Subscription

	inflight map[vo.PacketID]*InflightMessage
	// nextPacketID is the rotating allocation cursor. Identifiers are handed
	// out in ascending order and wrap, rather than always restarting at 1,
	// because reusing an identifier immediately after it is freed makes a late
	// PUBACK for the previous message acknowledge the wrong one.
	nextPacketID uint16

	queued []*msgAgg.Message

	maxInflight      int
	maxQueued        int
	maxSubscriptions int

	connected      bool
	createdAt      time.Time
	lastConnectAt  time.Time
	disconnectedAt *time.Time
	// droppedFromQueue counts messages dropped because the offline queue was
	// full, surfaced by the admin API so silent loss is visible.
	droppedFromQueue uint64
}

// Limits configures a session's bounds. Zero values take the defaults.
type Limits struct {
	MaxInflight      int
	MaxQueued        int
	MaxSubscriptions int
}

// withDefaults fills unset limits.
func (l Limits) withDefaults() Limits {
	if l.MaxInflight <= 0 {
		l.MaxInflight = DefaultMaxInflight
	}
	if l.MaxQueued <= 0 {
		l.MaxQueued = DefaultMaxQueued
	}
	if l.MaxSubscriptions <= 0 {
		l.MaxSubscriptions = DefaultMaxSubscriptions
	}
	return l
}

// NewSession builds a Session for a client.
func NewSession(clientID clientVO.ClientID, clean bool, limits Limits, now time.Time) *Session {
	limits = limits.withDefaults()
	now = now.UTC()

	return &Session{
		clientID:         clientID,
		clean:            clean,
		subscriptions:    make(map[msgVO.TopicFilter]*subAgg.Subscription),
		inflight:         make(map[vo.PacketID]*InflightMessage),
		nextPacketID:     vo.MinPacketID,
		maxInflight:      limits.MaxInflight,
		maxQueued:        limits.MaxQueued,
		maxSubscriptions: limits.MaxSubscriptions,
		connected:        true,
		createdAt:        now,
		lastConnectAt:    now,
	}
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

// ClientID returns the session's client identifier.
func (s *Session) ClientID() clientVO.ClientID { return s.clientID }

// IsClean reports whether the session was created with clean session set.
func (s *Session) IsClean() bool { return s.clean }

// IsConnected reports whether a client currently holds this session.
func (s *Session) IsConnected() bool { return s.connected }

// IsPersistent reports whether the session survives a disconnect (§3.1.2.4).
func (s *Session) IsPersistent() bool { return !s.clean }

// CreatedAt returns when the session was first created.
func (s *Session) CreatedAt() time.Time { return s.createdAt }

// LastConnectAt returns the most recent connection instant.
func (s *Session) LastConnectAt() time.Time { return s.lastConnectAt }

// DisconnectedAt returns when the session went offline, nil while connected.
func (s *Session) DisconnectedAt() *time.Time { return s.disconnectedAt }

// InflightCount returns how many messages await acknowledgement.
func (s *Session) InflightCount() int { return len(s.inflight) }

// QueuedCount returns how many messages are held for an offline client.
func (s *Session) QueuedCount() int { return len(s.queued) }

// SubscriptionCount returns how many filters this session holds.
func (s *Session) SubscriptionCount() int { return len(s.subscriptions) }

// DroppedFromQueue returns how many messages were dropped for lack of queue
// space.
func (s *Session) DroppedFromQueue() uint64 { return s.droppedFromQueue }

// HasInflightCapacity reports whether another message may be sent without
// exceeding the window.
func (s *Session) HasInflightCapacity() bool { return len(s.inflight) < s.maxInflight }

// Subscriptions returns a snapshot of the session's subscriptions.
func (s *Session) Subscriptions() []*subAgg.Subscription {
	out := make([]*subAgg.Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		out = append(out, sub)
	}
	return out
}

// Subscription returns one subscription by filter.
func (s *Session) Subscription(filter msgVO.TopicFilter) (*subAgg.Subscription, bool) {
	sub, ok := s.subscriptions[filter]
	return sub, ok
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

// Resume reattaches a reconnecting client to this session.
//
// §3.1.2.4: reconnecting with clean session 1 discards whatever was stored, so
// the caller deletes the session instead of resuming it. Reaching here means
// the client asked to resume, and what it gets back is its subscriptions, its
// queued messages, and its in-flight window.
func (s *Session) Resume(now time.Time) {
	s.connected = true
	s.lastConnectAt = now.UTC()
	s.disconnectedAt = nil
}

// Disconnect marks the session offline.
//
// The in-flight window is deliberately kept rather than cleared. §4.4 requires
// unacknowledged QoS 1 messages to be retransmitted when the client reconnects,
// so discarding them here would turn "at least once" into "at most once" for
// exactly the messages that were in flight when the network failed.
func (s *Session) Disconnect(now time.Time) {
	disconnectedAt := now.UTC()
	s.connected = false
	s.disconnectedAt = &disconnectedAt
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

// Subscribe adds or replaces a subscription and returns the granted QoS.
//
// §3.8.4: re-subscribing to an existing filter replaces the entry rather than
// adding a second one, and the new QoS takes effect. A broker that appended
// would deliver the message twice to a client that merely refreshed its
// subscription.
func (s *Session) Subscribe(
	filter msgVO.TopicFilter,
	requestedQoS msgVO.QoS,
	now time.Time,
) (msgVO.QoS, error) {
	if _, exists := s.subscriptions[filter]; !exists {
		if len(s.subscriptions) >= s.maxSubscriptions {
			return 0, errors.Invalid(
				sessionErr.ESubscriptionLimit,
				"session has reached its subscription limit",
			)
		}
	}

	sub := subAgg.NewSubscription(s.clientID, filter, requestedQoS, now)
	s.subscriptions[filter] = sub
	return sub.GrantedQoS(), nil
}

// Unsubscribe removes a subscription and reports whether one was removed.
//
// §3.10.4 requires the broker to answer UNSUBACK whether or not the filter was
// subscribed, so the boolean is informational — for metrics and logs — rather
// than an error condition.
func (s *Session) Unsubscribe(filter msgVO.TopicFilter) bool {
	if _, ok := s.subscriptions[filter]; !ok {
		return false
	}
	delete(s.subscriptions, filter)
	return true
}

// MatchingSubscription returns the subscription that best matches a topic.
//
// A client may hold several filters that all match one topic — "sport/#" and
// "sport/+/score", say. §3.3.5 leaves the behaviour to the implementation;
// this broker delivers the message once, at the highest granted QoS among the
// matching filters. Delivering once per matching filter is the alternative, and
// it surprises everyone: a client that adds a broader filter suddenly receives
// duplicates of messages it was already getting.
func (s *Session) MatchingSubscription(topic msgVO.TopicName) (*subAgg.Subscription, bool) {
	var best *subAgg.Subscription

	for _, sub := range s.subscriptions {
		if !sub.Matches(topic) {
			continue
		}
		if best == nil || sub.GrantedQoS() > best.GrantedQoS() {
			best = sub
		}
	}
	return best, best != nil
}

// ---------------------------------------------------------------------------
// Packet identifiers and the in-flight window
// ---------------------------------------------------------------------------

// NextPacketID allocates an unused packet identifier for this session.
//
// It scans forward from the rotating cursor and wraps, skipping identifiers
// still in flight. The scan is bounded by the identifier space, so a session
// whose window is genuinely full reports exhaustion rather than looping.
func (s *Session) NextPacketID() (vo.PacketID, error) {
	for i := 0; i < int(vo.MaxPacketID); i++ {
		candidate := vo.PacketID(s.nextPacketID)

		// Advance the cursor first, wrapping past zero, which is reserved.
		if s.nextPacketID == vo.MaxPacketID {
			s.nextPacketID = vo.MinPacketID
		} else {
			s.nextPacketID++
		}

		if _, inUse := s.inflight[candidate]; !inUse {
			return candidate, nil
		}
	}
	return 0, errors.Invalid(
		sessionErr.EPacketIDExhausted,
		"no packet identifier is available for this session",
	)
}

// TrackInflight records a QoS 1 message as sent and awaiting acknowledgement.
func (s *Session) TrackInflight(
	packetID vo.PacketID,
	message *msgAgg.Message,
	now time.Time,
) error {
	if packetID.IsZero() {
		return errors.Invalid(
			sessionErr.EUnknownPacketID,
			"packet identifier must not be zero",
		)
	}
	if len(s.inflight) >= s.maxInflight {
		return errors.Invalid(
			sessionErr.EInflightFull,
			"the in-flight window for this session is full",
		)
	}

	s.inflight[packetID] = &InflightMessage{
		PacketID: packetID,
		Message:  message,
		SentAt:   now.UTC(),
		Attempts: 1,
	}
	return nil
}

// Acknowledge clears an in-flight message on receipt of its PUBACK.
//
// An unknown identifier returns an error rather than being ignored. §4.4 makes
// an unsolicited acknowledgement a protocol violation, and silently ignoring it
// hides a genuinely broken client — or an attempt to make the broker free a
// window slot it should not.
func (s *Session) Acknowledge(packetID vo.PacketID) (*InflightMessage, error) {
	message, ok := s.inflight[packetID]
	if !ok {
		return nil, errors.Invalid(
			sessionErr.EUnknownPacketID,
			"no in-flight message has packet identifier "+packetID.String(),
		)
	}
	delete(s.inflight, packetID)
	return message, nil
}

// InflightMessages returns the unacknowledged messages, oldest first.
//
// The order is what makes retransmission correct: §4.6 requires a broker to
// resend in the order the messages were originally sent, so a subscriber that
// reconnects does not receive its backlog shuffled.
func (s *Session) InflightMessages() []*InflightMessage {
	out := make([]*InflightMessage, 0, len(s.inflight))
	for _, m := range s.inflight {
		out = append(out, m)
	}
	sortInflightBySentAt(out)
	return out
}

// MarkRetransmitted records another transmission attempt.
func (s *Session) MarkRetransmitted(packetID vo.PacketID, now time.Time) {
	if m, ok := s.inflight[packetID]; ok {
		m.Attempts++
		m.SentAt = now.UTC()
	}
}

// ExpiredInflight returns messages unacknowledged for longer than timeout.
func (s *Session) ExpiredInflight(now time.Time, timeout time.Duration) []*InflightMessage {
	var expired []*InflightMessage
	for _, m := range s.inflight {
		if now.UTC().Sub(m.SentAt) > timeout {
			expired = append(expired, m)
		}
	}
	sortInflightBySentAt(expired)
	return expired
}

// ---------------------------------------------------------------------------
// The offline queue
// ---------------------------------------------------------------------------

// Enqueue stores a message for a client that is offline or whose in-flight
// window is full.
//
// When the queue is full the *oldest* message is dropped, not the newest. For
// the telemetry traffic MQTT mostly carries, the newest reading is the one that
// matters; dropping it to preserve a stale one means a reconnecting client
// replays history and then learns nothing about the present. The drop is
// counted so the loss is visible rather than silent.
func (s *Session) Enqueue(message *msgAgg.Message) (dropped bool) {
	if len(s.queued) >= s.maxQueued {
		s.queued = s.queued[1:]
		s.droppedFromQueue++
		dropped = true
	}
	s.queued = append(s.queued, message)
	return dropped
}

// DrainQueue removes and returns up to limit queued messages, oldest first.
//
// It drains in batches rather than all at once so a client reconnecting to a
// thousand queued messages does not have all of them pushed into the in-flight
// window — which would immediately overflow it — or written to its socket in
// one burst it cannot read.
func (s *Session) DrainQueue(limit int) []*msgAgg.Message {
	if limit <= 0 || len(s.queued) == 0 {
		return nil
	}
	if limit > len(s.queued) {
		limit = len(s.queued)
	}

	batch := s.queued[:limit]
	// Re-slice into a fresh backing array so the drained messages are not kept
	// alive by the queue's capacity.
	remaining := make([]*msgAgg.Message, len(s.queued)-limit)
	copy(remaining, s.queued[limit:])
	s.queued = remaining

	return batch
}

// PeekQueue returns the queued messages without removing them, for the admin
// API.
func (s *Session) PeekQueue() []*msgAgg.Message {
	return append([]*msgAgg.Message(nil), s.queued...)
}

// Clear discards every piece of session state.
//
// Used when a client reconnects with clean session 1, which §3.1.2.4 requires
// to discard any previous session.
func (s *Session) Clear() {
	s.subscriptions = make(map[msgVO.TopicFilter]*subAgg.Subscription)
	s.inflight = make(map[vo.PacketID]*InflightMessage)
	s.queued = nil
	s.nextPacketID = vo.MinPacketID
}

// sortInflightBySentAt orders messages oldest first, breaking ties on packet
// identifier so the order is total and retransmission is deterministic.
func sortInflightBySentAt(messages []*InflightMessage) {
	// Insertion sort: the slice is bounded by the in-flight window, which is
	// dozens of entries, and this avoids the sort package's allocation on a
	// path that runs on every reconnect.
	for i := 1; i < len(messages); i++ {
		current := messages[i]
		j := i - 1
		for j >= 0 && inflightAfter(messages[j], current) {
			messages[j+1] = messages[j]
			j--
		}
		messages[j+1] = current
	}
}

// inflightAfter reports whether a sorts after b.
func inflightAfter(a, b *InflightMessage) bool {
	if a.SentAt.Equal(b.SentAt) {
		return a.PacketID > b.PacketID
	}
	return a.SentAt.After(b.SentAt)
}
