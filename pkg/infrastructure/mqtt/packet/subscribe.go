package packet

// TopicSubscription is one filter/QoS pair inside a SUBSCRIBE (§3.8.3).
type TopicSubscription struct {
	Filter string
	QoS    byte
}

// Subscribe is a decoded SUBSCRIBE packet (§3.8).
type Subscribe struct {
	PacketID      uint16
	Subscriptions []TopicSubscription
}

// Type returns SUBSCRIBE.
func (*Subscribe) Type() Type { return SUBSCRIBE }

// decodeSubscribe parses a SUBSCRIBE body.
func decodeSubscribe(body []byte) (*Subscribe, error) {
	r := newReader(body)

	packetID, err := r.uint16Value()
	if err != nil {
		return nil, err
	}
	if packetID == 0 {
		return nil, ErrInvalidPacketID
	}

	s := &Subscribe{PacketID: packetID}

	for !r.done() {
		filter, err := r.stringValue()
		if err != nil {
			return nil, err
		}
		requestedQoS, err := r.byteValue()
		if err != nil {
			return nil, err
		}
		// §3.8.3.1: the upper six bits are reserved and QoS 3 is invalid.
		if requestedQoS > 2 {
			return nil, newProtocolError("SUBSCRIBE requested QoS must be 0, 1 or 2")
		}
		s.Subscriptions = append(s.Subscriptions, TopicSubscription{
			Filter: filter,
			QoS:    requestedQoS,
		})
	}

	// §3.8.3: a SUBSCRIBE with no topic filters is a protocol violation.
	// Accepting it would mean answering with an empty SUBACK, which tells the
	// client its subscription succeeded when nothing was subscribed.
	if len(s.Subscriptions) == 0 {
		return nil, newProtocolError("SUBSCRIBE must contain at least one topic filter")
	}
	return s, nil
}

// Encode writes the SUBSCRIBE packet.
func (s *Subscribe) Encode(dst []byte) ([]byte, error) {
	if s.PacketID == 0 {
		return nil, ErrInvalidPacketID
	}
	if len(s.Subscriptions) == 0 {
		return nil, newProtocolError("SUBSCRIBE must contain at least one topic filter")
	}

	length := 2
	for _, sub := range s.Subscriptions {
		length += stringSize(sub.Filter) + 1
	}

	dst = append(dst, byte(SUBSCRIBE)<<4|requiredFlags[SUBSCRIBE])
	dst = encodeRemainingLength(dst, length)
	dst = appendUint16(dst, s.PacketID)
	for _, sub := range s.Subscriptions {
		dst = appendString(dst, sub.Filter)
		dst = append(dst, sub.QoS)
	}
	return dst, nil
}

// ---------------------------------------------------------------------------
// SUBACK
// ---------------------------------------------------------------------------

// SubackFailure is the return code for a rejected subscription (§3.9.3).
const SubackFailure byte = 0x80

// Suback is a decoded SUBACK packet (§3.9).
type Suback struct {
	PacketID uint16
	// ReturnCodes has one entry per requested filter, in the same order: the
	// granted QoS, or SubackFailure. The positional correspondence is the whole
	// contract, which is why a partial SUBACK is not representable here.
	ReturnCodes []byte
}

// Type returns SUBACK.
func (*Suback) Type() Type { return SUBACK }

// Encode writes the SUBACK packet.
func (s *Suback) Encode(dst []byte) ([]byte, error) {
	if s.PacketID == 0 {
		return nil, ErrInvalidPacketID
	}
	dst = append(dst, byte(SUBACK)<<4)
	dst = encodeRemainingLength(dst, 2+len(s.ReturnCodes))
	dst = appendUint16(dst, s.PacketID)
	return append(dst, s.ReturnCodes...), nil
}

// decodeSuback parses a SUBACK body.
func decodeSuback(body []byte) (*Suback, error) {
	if len(body) < 3 {
		return nil, ErrMalformedPacket
	}
	packetID := uint16(body[0])<<8 | uint16(body[1])
	if packetID == 0 {
		return nil, ErrInvalidPacketID
	}
	codes := append([]byte(nil), body[2:]...)
	for _, code := range codes {
		if code > 2 && code != SubackFailure {
			return nil, newProtocolError("SUBACK return code must be 0, 1, 2 or 0x80")
		}
	}
	return &Suback{PacketID: packetID, ReturnCodes: codes}, nil
}

// ---------------------------------------------------------------------------
// UNSUBSCRIBE
// ---------------------------------------------------------------------------

// Unsubscribe is a decoded UNSUBSCRIBE packet (§3.10).
type Unsubscribe struct {
	PacketID uint16
	Filters  []string
}

// Type returns UNSUBSCRIBE.
func (*Unsubscribe) Type() Type { return UNSUBSCRIBE }

// decodeUnsubscribe parses an UNSUBSCRIBE body.
func decodeUnsubscribe(body []byte) (*Unsubscribe, error) {
	r := newReader(body)

	packetID, err := r.uint16Value()
	if err != nil {
		return nil, err
	}
	if packetID == 0 {
		return nil, ErrInvalidPacketID
	}

	u := &Unsubscribe{PacketID: packetID}
	for !r.done() {
		filter, err := r.stringValue()
		if err != nil {
			return nil, err
		}
		u.Filters = append(u.Filters, filter)
	}

	// §3.10.3: at least one topic filter is required.
	if len(u.Filters) == 0 {
		return nil, newProtocolError("UNSUBSCRIBE must contain at least one topic filter")
	}
	return u, nil
}

// Encode writes the UNSUBSCRIBE packet.
func (u *Unsubscribe) Encode(dst []byte) ([]byte, error) {
	if u.PacketID == 0 {
		return nil, ErrInvalidPacketID
	}
	if len(u.Filters) == 0 {
		return nil, newProtocolError("UNSUBSCRIBE must contain at least one topic filter")
	}

	length := 2
	for _, filter := range u.Filters {
		length += stringSize(filter)
	}

	dst = append(dst, byte(UNSUBSCRIBE)<<4|requiredFlags[UNSUBSCRIBE])
	dst = encodeRemainingLength(dst, length)
	dst = appendUint16(dst, u.PacketID)
	for _, filter := range u.Filters {
		dst = appendString(dst, filter)
	}
	return dst, nil
}

// ---------------------------------------------------------------------------
// Zero-length packets
// ---------------------------------------------------------------------------

// Simple is a packet with a fixed header and an empty body: PINGREQ, PINGRESP
// and DISCONNECT (§3.12, §3.13, §3.14).
type Simple struct{ packetType Type }

// NewPingreq builds a PINGREQ.
func NewPingreq() *Simple { return &Simple{packetType: PINGREQ} }

// NewPingresp builds a PINGRESP.
func NewPingresp() *Simple { return &Simple{packetType: PINGRESP} }

// NewDisconnect builds a DISCONNECT.
func NewDisconnect() *Simple { return &Simple{packetType: DISCONNECT} }

// Type returns the packet type.
func (s *Simple) Type() Type { return s.packetType }

// Encode writes the two-byte packet.
func (s *Simple) Encode(dst []byte) ([]byte, error) {
	return append(dst, byte(s.packetType)<<4, 0), nil
}

// decodeSimple parses a zero-length-body packet.
func decodeSimple(t Type, body []byte) (*Simple, error) {
	if len(body) != 0 {
		return nil, newProtocolError(t.String() + " must have an empty body")
	}
	return &Simple{packetType: t}, nil
}
