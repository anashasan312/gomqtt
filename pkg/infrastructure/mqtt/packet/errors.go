package packet

import "errors"

// Codec errors.
//
// They are sentinel values rather than formatted strings so the transport layer
// can branch on them — a malformed packet closes the connection, whereas an
// unsupported-but-well-formed one gets a protocol-level refusal.
var (
	// ErrMalformedPacket means the bytes do not form a valid control packet.
	ErrMalformedPacket = errors.New("mqtt: malformed packet")

	// ErrRemainingLengthOverflow means the remaining-length varint exceeded the
	// four bytes the spec allows (§2.2.3).
	ErrRemainingLengthOverflow = errors.New("mqtt: remaining length exceeds 4 bytes")

	// ErrPacketTooLarge means the packet exceeds the configured maximum size.
	ErrPacketTooLarge = errors.New("mqtt: packet exceeds maximum size")

	// ErrInvalidQoS means a QoS value of 3, which the spec forbids (§3.3.1.2).
	ErrInvalidQoS = errors.New("mqtt: invalid QoS value")

	// ErrInvalidUTF8 means a string field was not valid UTF-8, or contained a
	// null character or a surrogate (§1.5.3).
	ErrInvalidUTF8 = errors.New("mqtt: invalid UTF-8 string")

	// ErrInvalidPacketID means a packet identifier of zero where the spec
	// requires a non-zero value (§2.3.1).
	ErrInvalidPacketID = errors.New("mqtt: packet identifier must not be zero")

	// ErrUnsupportedProtocol means the CONNECT named a protocol other than
	// MQTT 3.1.1.
	ErrUnsupportedProtocol = errors.New("mqtt: unsupported protocol version")
)

// ProtocolError describes a well-formed packet that breaks a protocol rule.
//
// It is distinct from ErrMalformedPacket because the two mean different things
// to an operator reading a log: malformed is "these bytes are not MQTT",
// protocol error is "this is MQTT, and it is wrong".
type ProtocolError struct{ Reason string }

// Error implements the error interface.
func (e *ProtocolError) Error() string { return "mqtt: protocol violation: " + e.Reason }

// Is makes every ProtocolError match ErrMalformedPacket for callers that only
// care that the connection must be closed.
func (e *ProtocolError) Is(target error) bool { return target == ErrMalformedPacket }

// newProtocolError builds a ProtocolError.
func newProtocolError(reason string) error { return &ProtocolError{Reason: reason} }
