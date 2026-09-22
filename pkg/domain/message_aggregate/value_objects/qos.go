package value_objects

import (
	"github.com/anashasan/gomqtt/pkg/common/errors"
	msgErr "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/error"
)

// QoS is the MQTT quality-of-service level (§4.3).
//
// A distinct type rather than a bare byte, so the compiler rejects passing a
// packet identifier or a return code where a delivery guarantee is expected.
type QoS byte

// Quality of service levels.
const (
	// QoSAtMostOnce is fire-and-forget: no acknowledgement, no retransmission.
	QoSAtMostOnce QoS = 0
	// QoSAtLeastOnce is acknowledged delivery: the sender retransmits until it
	// sees a PUBACK, so a receiver may see a message more than once.
	QoSAtLeastOnce QoS = 1
	// QoSExactlyOnce is the four-step handshake. This broker does not implement
	// it; see MaxSupportedQoS.
	QoSExactlyOnce QoS = 2
)

// MaxSupportedQoS is the highest level this broker implements.
//
// QoS 2 is decoded and understood — a QoS 2 SUBSCRIBE is answered honestly by
// granting QoS 1, which §3.8.4 explicitly permits — but the PUBREC/PUBREL/
// PUBCOMP flow is not implemented. Declaring the ceiling as a constant means
// every place that has to respect it reads the same value.
const MaxSupportedQoS = QoSAtLeastOnce

// NewQoS validates and constructs a QoS.
func NewQoS(level byte) (QoS, error) {
	if level > byte(QoSExactlyOnce) {
		return 0, errors.Invalid(msgErr.EInvalidQoS, "QoS must be 0, 1 or 2")
	}
	return QoS(level), nil
}

// Byte renders the level for the wire.
func (q QoS) Byte() byte { return byte(q) }

// String renders the level for logs and metric labels.
func (q QoS) String() string {
	switch q {
	case QoSAtMostOnce:
		return "0"
	case QoSAtLeastOnce:
		return "1"
	case QoSExactlyOnce:
		return "2"
	default:
		return "invalid"
	}
}

// IsSupported reports whether this broker implements the level.
func (q QoS) IsSupported() bool { return q <= MaxSupportedQoS }

// RequiresAcknowledgement reports whether delivery at this level must be
// tracked until the peer acknowledges it.
func (q QoS) RequiresAcknowledgement() bool { return q >= QoSAtLeastOnce }

// Downgrade returns the level actually granted for a requested one.
//
// The effective QoS of a delivery is the minimum of the publisher's QoS and the
// subscription's (§4.3): a QoS 1 subscriber to a QoS 0 publication receives it
// at QoS 0, because the publisher never offered a stronger guarantee.
func (q QoS) Downgrade(other QoS) QoS {
	effective := q
	if other < effective {
		effective = other
	}
	if effective > MaxSupportedQoS {
		effective = MaxSupportedQoS
	}
	return effective
}
