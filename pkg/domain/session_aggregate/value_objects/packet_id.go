package value_objects

import (
	"strconv"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	sessionErr "github.com/anashasan/gomqtt/pkg/domain/session_aggregate/error"
)

// Packet identifier bounds (§2.3.1).
const (
	// MinPacketID is the lowest valid identifier. Zero is reserved.
	MinPacketID uint16 = 1
	// MaxPacketID is the highest identifier the two-byte field can hold.
	MaxPacketID uint16 = 65535
)

// PacketID identifies an in-flight QoS 1 or QoS 2 message within one session.
//
// The scope matters and is easy to get wrong: identifiers are unique per
// *session direction*, not globally. Two clients may both have packet 1 in
// flight at the same time, and the broker's outbound 1 to a subscriber is
// unrelated to that subscriber's inbound 1. Modelling it as a value object
// allocated by the Session makes that scoping structural rather than a comment.
type PacketID uint16

// NewPacketID validates and constructs a PacketID.
func NewPacketID(raw uint16) (PacketID, error) {
	if raw == 0 {
		return 0, errors.Invalid(
			sessionErr.EUnknownPacketID,
			"packet identifier must not be zero",
		)
	}
	return PacketID(raw), nil
}

// Uint16 renders the identifier for the wire.
func (p PacketID) Uint16() uint16 { return uint16(p) }

// IsZero reports whether the identifier is unset.
func (p PacketID) IsZero() bool { return p == 0 }

// String renders the identifier for error messages.
func (p PacketID) String() string {
	return strconv.FormatUint(uint64(p), 10)
}
