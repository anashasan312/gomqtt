// Package message_aggregate contains the Message aggregate: one published
// message and the rules that govern how it is delivered.
package message_aggregate

import (
	"time"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	msgErr "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/error"
	vo "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
)

// DefaultMaxPayloadBytes bounds a published payload.
//
// The protocol permits 256 MB. A broker that allows that on an unauthenticated
// connection lets one client exhaust memory, so the default is 1 MB and the
// ceiling is configurable for deployments that genuinely move large payloads.
const DefaultMaxPayloadBytes = 1 << 20

// Message is the aggregate root for a published message.
//
// Fields are unexported: a message's topic and QoS are fixed at publication and
// the only legal derivation is WithQoS, which produces a *new* message for one
// subscriber's effective level. Allowing a caller to assign to QoS directly is
// how a broker ends up mutating a retained message in place and serving the
// wrong level to the next subscriber that matches it.
type Message struct {
	topic     vo.TopicName
	payload   []byte
	qos       vo.QoS
	retain    bool
	dup       bool
	packetID  uint16
	createdAt time.Time
}

// NewMessageParams is the input to NewMessage.
type NewMessageParams struct {
	Topic    vo.TopicName
	Payload  []byte
	QoS      vo.QoS
	Retain   bool
	DUP      bool
	PacketID uint16
	Now      time.Time
	// MaxPayloadBytes caps the payload. Zero applies the default.
	MaxPayloadBytes int
}

// NewMessage validates the parameters and builds a Message.
func NewMessage(p NewMessageParams) (*Message, error) {
	if p.Topic == "" {
		return nil, errors.Invalid(msgErr.EInvalidTopicName, "topic is required")
	}
	if p.QoS > vo.QoSExactlyOnce {
		return nil, errors.Invalid(msgErr.EInvalidQoS, "QoS must be 0, 1 or 2")
	}

	limit := p.MaxPayloadBytes
	if limit <= 0 {
		limit = DefaultMaxPayloadBytes
	}
	if len(p.Payload) > limit {
		return nil, errors.Invalid(msgErr.EPayloadTooLarge, "payload exceeds the configured maximum size")
	}

	// §2.3.1: QoS 1 and 2 require a non-zero packet identifier.
	if p.QoS.RequiresAcknowledgement() && p.PacketID == 0 {
		return nil, errors.Invalid(
			msgErr.EInvalidPacketID,
			"a packet identifier is required for QoS 1 and 2",
		)
	}

	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}

	return &Message{
		topic: p.Topic,
		// Copied: the caller's slice aliases a connection read buffer that is
		// reused for the next packet, while this message may be queued for an
		// offline client or retained indefinitely.
		payload:   append([]byte(nil), p.Payload...),
		qos:       p.QoS,
		retain:    p.Retain,
		dup:       p.DUP,
		packetID:  p.PacketID,
		createdAt: now.UTC(),
	}, nil
}

// Topic returns the destination topic.
func (m *Message) Topic() vo.TopicName { return m.topic }

// Payload returns a defensive copy of the payload.
func (m *Message) Payload() []byte { return append([]byte(nil), m.payload...) }

// PayloadSize reports the payload length without copying it, for metrics.
func (m *Message) PayloadSize() int { return len(m.payload) }

// PayloadRef returns the payload without copying.
//
// Reserved for the delivery path, which encodes the bytes straight onto a
// socket and never retains or mutates them. Every other caller uses Payload.
func (m *Message) PayloadRef() []byte { return m.payload }

// QoS returns the delivery level.
func (m *Message) QoS() vo.QoS { return m.qos }

// IsRetained reports whether the broker should store this message as the
// retained message for its topic.
func (m *Message) IsRetained() bool { return m.retain }

// IsDuplicate reports whether the DUP flag is set.
func (m *Message) IsDuplicate() bool { return m.dup }

// PacketID returns the packet identifier, zero for QoS 0.
func (m *Message) PacketID() uint16 { return m.packetID }

// CreatedAt returns the publication instant.
func (m *Message) CreatedAt() time.Time { return m.createdAt }

// IsEmpty reports whether the payload has zero length.
//
// This is not a curiosity. §3.3.1.3: publishing a zero-length payload with the
// retain flag set *clears* the retained message for that topic rather than
// storing an empty one, so this predicate carries real protocol meaning.
func (m *Message) IsEmpty() bool { return len(m.payload) == 0 }

// ClearsRetained reports whether this publication deletes the topic's retained
// message.
func (m *Message) ClearsRetained() bool { return m.retain && m.IsEmpty() }

// WithQoS returns a copy of the message at the given effective level.
//
// Delivery to a subscriber uses the minimum of the publication's QoS and the
// subscription's (§4.3), so a single publication fans out as several messages
// that differ only in level. Returning a copy rather than mutating is what
// keeps two subscribers at different levels from corrupting each other's view.
func (m *Message) WithQoS(qos vo.QoS, packetID uint16) *Message {
	clone := *m
	clone.qos = qos
	clone.packetID = packetID
	if !qos.RequiresAcknowledgement() {
		clone.packetID = 0
	}
	return &clone
}

// AsRetainedDelivery returns a copy flagged as retained.
//
// §3.3.1.3 draws a distinction most implementations get wrong: a message
// delivered because it was *stored* carries RETAIN=1, while the same message
// delivered live to an already-subscribed client carries RETAIN=0. The flag
// tells the receiver "this is history, not news".
func (m *Message) AsRetainedDelivery() *Message {
	clone := *m
	clone.retain = true
	return &clone
}

// AsLiveDelivery returns a copy flagged as a live delivery.
func (m *Message) AsLiveDelivery() *Message {
	clone := *m
	clone.retain = false
	return &clone
}

// AsRedelivery returns a copy with the DUP flag set, for retransmission of an
// unacknowledged QoS 1 message (§3.3.1.1).
func (m *Message) AsRedelivery() *Message {
	clone := *m
	clone.dup = true
	return &clone
}
