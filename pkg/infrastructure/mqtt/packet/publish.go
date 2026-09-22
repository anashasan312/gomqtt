package packet

// Publish is a decoded PUBLISH packet (§3.3).
type Publish struct {
	// DUP marks a redelivery. It is informational only: the broker must not
	// treat a DUP PUBLISH differently from a first delivery, because the flag
	// says what the *sender* believes, not what the receiver has seen.
	DUP    bool
	QoS    byte
	Retain bool
	Topic  string
	// PacketID is meaningful only for QoS 1 and 2 (§3.3.2.2).
	PacketID uint16
	Payload  []byte
}

// Type returns PUBLISH.
func (*Publish) Type() Type { return PUBLISH }

// decodePublish parses a PUBLISH body, taking the flags from the fixed header
// because for this one packet type they carry meaning rather than a constant.
func decodePublish(flags byte, body []byte) (*Publish, error) {
	p := &Publish{
		DUP:    flags&0x08 != 0,
		QoS:    (flags >> 1) & 0x03,
		Retain: flags&0x01 != 0,
	}

	r := newReader(body)

	topic, err := r.stringValue()
	if err != nil {
		return nil, err
	}
	// §3.3.2.1: the topic name in a PUBLISH must not contain wildcards. They
	// are a subscription concept; a publisher using one is either confused or
	// probing, and either way the connection must close.
	if containsWildcard(topic) {
		return nil, newProtocolError("PUBLISH topic must not contain wildcard characters")
	}
	p.Topic = topic

	if p.QoS > 0 {
		packetID, err := r.uint16Value()
		if err != nil {
			return nil, err
		}
		// §2.3.1: a packet identifier of zero is invalid.
		if packetID == 0 {
			return nil, ErrInvalidPacketID
		}
		p.PacketID = packetID
	}

	// The payload is whatever remains, and it is copied because it outlives the
	// connection's read buffer: it is handed to every matching subscriber and
	// may sit in a retained store indefinitely.
	p.Payload = append([]byte(nil), r.rest()...)
	return p, nil
}

// Encode writes the PUBLISH packet.
func (p *Publish) Encode(dst []byte) ([]byte, error) {
	if p.QoS > 2 {
		return nil, ErrInvalidQoS
	}
	if p.QoS > 0 && p.PacketID == 0 {
		return nil, ErrInvalidPacketID
	}

	var flags byte
	if p.DUP {
		flags |= 0x08
	}
	flags |= (p.QoS << 1) & 0x06
	if p.Retain {
		flags |= 0x01
	}

	length := stringSize(p.Topic) + len(p.Payload)
	if p.QoS > 0 {
		length += 2
	}

	dst = append(dst, byte(PUBLISH)<<4|flags)
	dst = encodeRemainingLength(dst, length)
	dst = appendString(dst, p.Topic)
	if p.QoS > 0 {
		dst = appendUint16(dst, p.PacketID)
	}
	return append(dst, p.Payload...), nil
}

// EncodedSize reports the wire size of the packet.
//
// The fan-out path uses it to size a buffer once before encoding a message for
// many subscribers, rather than letting append grow a slice per delivery.
func (p *Publish) EncodedSize() int {
	length := stringSize(p.Topic) + len(p.Payload)
	if p.QoS > 0 {
		length += 2
	}
	return 1 + remainingLengthSize(length) + length
}

// containsWildcard reports whether a topic contains a wildcard character.
func containsWildcard(topic string) bool {
	for i := 0; i < len(topic); i++ {
		if topic[i] == '+' || topic[i] == '#' {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Acknowledgement packets
// ---------------------------------------------------------------------------

// Ack is the shared shape of PUBACK, PUBREC, PUBREL, PUBCOMP and UNSUBACK:
// a fixed header plus a packet identifier and nothing else (§3.4–§3.7, §3.11).
//
// One type covers all five rather than five near-identical structs, because the
// only thing that distinguishes them on the wire is the type nibble.
type Ack struct {
	packetType Type
	PacketID   uint16
}

// NewPuback builds a PUBACK for the given packet identifier.
func NewPuback(packetID uint16) *Ack { return &Ack{packetType: PUBACK, PacketID: packetID} }

// NewPubrec builds a PUBREC.
func NewPubrec(packetID uint16) *Ack { return &Ack{packetType: PUBREC, PacketID: packetID} }

// NewPubrel builds a PUBREL.
func NewPubrel(packetID uint16) *Ack { return &Ack{packetType: PUBREL, PacketID: packetID} }

// NewPubcomp builds a PUBCOMP.
func NewPubcomp(packetID uint16) *Ack { return &Ack{packetType: PUBCOMP, PacketID: packetID} }

// NewUnsuback builds an UNSUBACK.
func NewUnsuback(packetID uint16) *Ack { return &Ack{packetType: UNSUBACK, PacketID: packetID} }

// Type returns the acknowledgement's packet type.
func (a *Ack) Type() Type { return a.packetType }

// Encode writes the acknowledgement packet.
func (a *Ack) Encode(dst []byte) ([]byte, error) {
	if a.PacketID == 0 {
		return nil, ErrInvalidPacketID
	}
	// PUBREL carries flags 0x02; the rest carry 0x00 (§2.2.2).
	flags := requiredFlags[a.packetType]
	dst = append(dst, byte(a.packetType)<<4|flags, 2)
	return appendUint16(dst, a.PacketID), nil
}

// decodeAck parses any of the packet-identifier-only acknowledgements.
func decodeAck(t Type, body []byte) (*Ack, error) {
	if len(body) != 2 {
		return nil, ErrMalformedPacket
	}
	packetID := uint16(body[0])<<8 | uint16(body[1])
	if packetID == 0 {
		return nil, ErrInvalidPacketID
	}
	return &Ack{packetType: t, PacketID: packetID}, nil
}
