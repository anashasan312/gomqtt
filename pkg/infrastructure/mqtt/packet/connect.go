package packet

// Protocol identification (§3.1.2.1, §3.1.2.2).
const (
	// ProtocolName is the name every MQTT 3.1.1 CONNECT must carry.
	ProtocolName = "MQTT"
	// ProtocolLevel311 is the level byte for MQTT 3.1.1.
	ProtocolLevel311 byte = 4
)

// Connect flag bit positions in the connect flags byte (§3.1.2.3).
const (
	flagReserved     byte = 0x01
	flagCleanSession byte = 0x02
	flagWill         byte = 0x04
	flagWillQoSMask  byte = 0x18
	flagWillRetain   byte = 0x20
	flagPassword     byte = 0x40
	flagUsername     byte = 0x80
)

// Will carries the client's last-will message.
type Will struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// Connect is a decoded CONNECT packet (§3.1).
type Connect struct {
	ProtocolName  string
	ProtocolLevel byte
	CleanSession  bool
	KeepAlive     uint16
	ClientID      string
	Will          *Will
	Username      string
	Password      []byte
	// HasUsername and HasPassword record whether the flag was set, which is not
	// the same as the field being non-empty: a client may legitimately send an
	// empty password, and the broker's auth decision differs between "absent"
	// and "present but empty".
	HasUsername bool
	HasPassword bool
}

// Type returns CONNECT.
func (*Connect) Type() Type { return CONNECT }

// decodeConnect parses a CONNECT body.
//
// The ordering of the checks matters and follows the spec: protocol name and
// level first, so a client speaking a different protocol is rejected before any
// of its length fields are trusted.
func decodeConnect(body []byte) (*Connect, error) {
	r := newReader(body)

	protocolName, err := r.stringValue()
	if err != nil {
		return nil, err
	}
	if protocolName != ProtocolName {
		// §3.1.2.1: the server MAY disconnect without a CONNACK if the name is
		// wrong. Treating it as malformed does exactly that.
		return nil, newProtocolError("protocol name must be " + ProtocolName)
	}

	protocolLevel, err := r.byteValue()
	if err != nil {
		return nil, err
	}

	flags, err := r.byteValue()
	if err != nil {
		return nil, err
	}
	// §3.1.2.3: the reserved bit must be zero. This is the spec's own
	// tripwire for a client that is not really speaking MQTT 3.1.1.
	if flags&flagReserved != 0 {
		return nil, newProtocolError("CONNECT reserved flag must be zero")
	}

	keepAlive, err := r.uint16Value()
	if err != nil {
		return nil, err
	}

	clientID, err := r.stringValue()
	if err != nil {
		return nil, err
	}

	connect := &Connect{
		ProtocolName:  protocolName,
		ProtocolLevel: protocolLevel,
		CleanSession:  flags&flagCleanSession != 0,
		KeepAlive:     keepAlive,
		ClientID:      clientID,
		HasUsername:   flags&flagUsername != 0,
		HasPassword:   flags&flagPassword != 0,
	}

	willQoS := (flags & flagWillQoSMask) >> 3
	hasWill := flags&flagWill != 0

	switch {
	case hasWill:
		if willQoS > 2 {
			return nil, ErrInvalidQoS
		}
		willTopic, err := r.stringValue()
		if err != nil {
			return nil, err
		}
		willPayload, err := r.bytesValue()
		if err != nil {
			return nil, err
		}
		connect.Will = &Will{
			Topic: willTopic,
			// Copied: the payload is retained past the life of the read buffer.
			Payload: append([]byte(nil), willPayload...),
			QoS:     willQoS,
			Retain:  flags&flagWillRetain != 0,
		}

	default:
		// §3.1.2.6: with the will flag clear, will QoS must be 0 and will
		// retain must be 0.
		if willQoS != 0 {
			return nil, newProtocolError("will QoS must be zero when the will flag is clear")
		}
		if flags&flagWillRetain != 0 {
			return nil, newProtocolError("will retain must be zero when the will flag is clear")
		}
	}

	if connect.HasUsername {
		username, err := r.stringValue()
		if err != nil {
			return nil, err
		}
		connect.Username = username
	}

	// §3.1.2.9: a password without a username is a protocol violation.
	if connect.HasPassword {
		if !connect.HasUsername {
			return nil, newProtocolError("password flag set without username flag")
		}
		password, err := r.bytesValue()
		if err != nil {
			return nil, err
		}
		connect.Password = append([]byte(nil), password...)
	}

	if !r.done() {
		return nil, newProtocolError("CONNECT has trailing bytes")
	}
	return connect, nil
}

// Encode writes the CONNECT packet.
//
// A broker does not normally send CONNECT — this exists so the project's own
// client package and its tests can speak the protocol, which is what lets the
// codec be verified in both directions against a real broker.
func (c *Connect) Encode(dst []byte) ([]byte, error) {
	var flags byte
	if c.CleanSession {
		flags |= flagCleanSession
	}
	if c.Will != nil {
		flags |= flagWill
		flags |= (c.Will.QoS << 3) & flagWillQoSMask
		if c.Will.Retain {
			flags |= flagWillRetain
		}
	}
	if c.HasUsername {
		flags |= flagUsername
	}
	if c.HasPassword {
		flags |= flagPassword
	}

	length := stringSize(c.ProtocolName) + 1 + 1 + 2 + stringSize(c.ClientID)
	if c.Will != nil {
		length += stringSize(c.Will.Topic) + 2 + len(c.Will.Payload)
	}
	if c.HasUsername {
		length += stringSize(c.Username)
	}
	if c.HasPassword {
		length += 2 + len(c.Password)
	}

	dst = append(dst, byte(CONNECT)<<4)
	dst = encodeRemainingLength(dst, length)
	dst = appendString(dst, c.ProtocolName)
	dst = append(dst, c.ProtocolLevel, flags)
	dst = appendUint16(dst, c.KeepAlive)
	dst = appendString(dst, c.ClientID)

	if c.Will != nil {
		dst = appendString(dst, c.Will.Topic)
		dst = appendBytes(dst, c.Will.Payload)
	}
	if c.HasUsername {
		dst = appendString(dst, c.Username)
	}
	if c.HasPassword {
		dst = appendBytes(dst, c.Password)
	}
	return dst, nil
}

// ---------------------------------------------------------------------------
// CONNACK
// ---------------------------------------------------------------------------

// ConnectReturnCode is the CONNACK result (§3.2.2.3, table 3.1).
type ConnectReturnCode byte

// CONNACK return codes.
const (
	ConnectAccepted                ConnectReturnCode = 0
	ConnectUnacceptableProtocolVer ConnectReturnCode = 1
	ConnectIdentifierRejected      ConnectReturnCode = 2
	ConnectServerUnavailable       ConnectReturnCode = 3
	ConnectBadCredentials          ConnectReturnCode = 4
	ConnectNotAuthorized           ConnectReturnCode = 5
)

// String renders the return code for logs.
func (c ConnectReturnCode) String() string {
	switch c {
	case ConnectAccepted:
		return "accepted"
	case ConnectUnacceptableProtocolVer:
		return "unacceptable_protocol_version"
	case ConnectIdentifierRejected:
		return "identifier_rejected"
	case ConnectServerUnavailable:
		return "server_unavailable"
	case ConnectBadCredentials:
		return "bad_username_or_password"
	case ConnectNotAuthorized:
		return "not_authorized"
	default:
		return "unknown"
	}
}

// Connack is a decoded CONNACK packet (§3.2).
type Connack struct {
	// SessionPresent tells the client whether the broker resumed its stored
	// session. A client uses it to decide whether it must resubscribe.
	SessionPresent bool
	ReturnCode     ConnectReturnCode
}

// Type returns CONNACK.
func (*Connack) Type() Type { return CONNACK }

// Encode writes the CONNACK packet.
func (c *Connack) Encode(dst []byte) ([]byte, error) {
	var ackFlags byte
	// §3.2.2.2: session present must be 0 when the return code is non-zero.
	if c.SessionPresent && c.ReturnCode == ConnectAccepted {
		ackFlags = 0x01
	}
	dst = append(dst, byte(CONNACK)<<4, 2, ackFlags, byte(c.ReturnCode))
	return dst, nil
}

// decodeConnack parses a CONNACK body.
func decodeConnack(body []byte) (*Connack, error) {
	if len(body) != 2 {
		return nil, ErrMalformedPacket
	}
	if body[0]&0xFE != 0 {
		return nil, newProtocolError("CONNACK reserved acknowledge flags must be zero")
	}
	return &Connack{
		SessionPresent: body[0]&0x01 != 0,
		ReturnCode:     ConnectReturnCode(body[1]),
	}, nil
}
