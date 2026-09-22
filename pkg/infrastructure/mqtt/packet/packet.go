// Package packet implements the MQTT 3.1.1 control packet wire format.
//
// This is the only package in the project that knows what a byte on the wire
// means. Everything above it works with decoded Go values, so the broker's
// business logic never touches an offset or a bitmask — and a spec fix lands in
// one place rather than being scattered through the connection handler.
//
// Reference: OASIS MQTT Version 3.1.1, chapters 2 and 3. Section numbers in the
// comments below point at the clause each rule comes from, because "why does
// this reject an empty client id with clean session 0" is a question the spec
// answers and a code comment should not have to re-derive.
package packet

import "fmt"

// Type is the MQTT control packet type, carried in the high nibble of byte 1.
type Type byte

// Control packet types (MQTT 3.1.1 §2.2.1, table 2.1). Value 0 and 15 are
// reserved and forbidden, which is why they are named here rather than left as
// gaps a reader has to notice.
const (
	typeReserved0  Type = 0
	CONNECT        Type = 1
	CONNACK        Type = 2
	PUBLISH        Type = 3
	PUBACK         Type = 4
	PUBREC         Type = 5
	PUBREL         Type = 6
	PUBCOMP        Type = 7
	SUBSCRIBE      Type = 8
	SUBACK         Type = 9
	UNSUBSCRIBE    Type = 10
	UNSUBACK       Type = 11
	PINGREQ        Type = 12
	PINGRESP       Type = 13
	DISCONNECT     Type = 14
	typeReserved15 Type = 15
)

// String renders the packet type for logs and errors.
func (t Type) String() string {
	switch t {
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	default:
		return fmt.Sprintf("RESERVED(%d)", byte(t))
	}
}

// IsValid reports whether the type is one the protocol defines.
func (t Type) IsValid() bool {
	return t > typeReserved0 && t < typeReserved15
}

// Packet is implemented by every decoded control packet.
//
// The interface is deliberately tiny. A decoded packet is data: it knows its own
// type and how to write itself back to the wire, and nothing else. Any behaviour
// that depends on broker state lives in the application layer, so the codec can
// be tested with no broker at all.
type Packet interface {
	// Type returns the control packet type.
	Type() Type
	// Encode appends the complete packet, fixed header included, to dst and
	// returns the extended slice.
	//
	// Appending to a caller-supplied buffer rather than allocating means the
	// writer can reuse one buffer for the life of a connection, which matters
	// when a fan-out publishes the same message to a thousand subscribers.
	Encode(dst []byte) ([]byte, error)
}

// requiredFlags maps a packet type to the low nibble the spec fixes for it
// (§2.2.2, table 2.2). PUBLISH is absent because its flags are variable.
//
// A table rather than a switch, because this is data: the spec states it as a
// table and the code should be checkable against it line by line.
var requiredFlags = map[Type]byte{
	CONNECT:     0x00,
	CONNACK:     0x00,
	PUBACK:      0x00,
	PUBREC:      0x00,
	PUBREL:      0x02,
	PUBCOMP:     0x00,
	SUBSCRIBE:   0x02,
	SUBACK:      0x00,
	UNSUBSCRIBE: 0x02,
	UNSUBACK:    0x00,
	PINGREQ:     0x00,
	PINGRESP:    0x00,
	DISCONNECT:  0x00,
}

// validateFlags rejects a packet whose reserved flag bits are wrong.
//
// This is not pedantry. The reserved bits are how a malformed or hostile client
// is caught early, before its bytes are interpreted as a topic length and turned
// into a multi-megabyte allocation.
func validateFlags(t Type, flags byte) error {
	if t == PUBLISH {
		// §3.3.1: QoS 3 is malformed, and DUP must be 0 for QoS 0.
		qos := (flags >> 1) & 0x03
		if qos == 3 {
			return ErrInvalidQoS
		}
		if qos == 0 && flags&0x08 != 0 {
			return newProtocolError("PUBLISH with QoS 0 must not set the DUP flag")
		}
		return nil
	}

	expected, known := requiredFlags[t]
	if !known {
		return newProtocolError("unknown packet type " + t.String())
	}
	if flags != expected {
		return newProtocolError(fmt.Sprintf(
			"%s must have flags 0x%02X, got 0x%02X", t, expected, flags,
		))
	}
	return nil
}
