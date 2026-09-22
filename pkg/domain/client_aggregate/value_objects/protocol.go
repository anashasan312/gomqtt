package value_objects

import (
	"github.com/anashasan/gomqtt/pkg/common/errors"
	clientErr "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/error"
)

// Protocol identification (§3.1.2.1, §3.1.2.2).
const (
	// ProtocolNameMQTT is the name MQTT 3.1.1 uses.
	ProtocolNameMQTT = "MQTT"
	// ProtocolNameMQIsdp is the name MQTT 3.1 used. Recognised so that an older
	// client gets an honest "unacceptable protocol version" CONNACK rather than
	// a bare socket close it cannot diagnose.
	ProtocolNameMQIsdp = "MQIsdp"

	// ProtocolLevel310 is MQTT 3.1.
	ProtocolLevel310 byte = 3
	// ProtocolLevel311 is MQTT 3.1.1, the version this broker implements.
	ProtocolLevel311 byte = 4
	// ProtocolLevel500 is MQTT 5.0.
	ProtocolLevel500 byte = 5
)

// ProtocolVersion is a validated protocol name and level pair.
type ProtocolVersion struct {
	name  string
	level byte
}

// NewProtocolVersion validates the CONNECT protocol fields.
//
// The returned error carries EUnsupportedProtocolVersion, which the connection
// service maps to CONNACK return code 1. That mapping matters: §3.1.4 requires
// the server to send that CONNACK *and then* close, so the client learns why.
// Closing silently is a much harder thing to debug from the other end.
func NewProtocolVersion(name string, level byte) (ProtocolVersion, error) {
	if name != ProtocolNameMQTT {
		return ProtocolVersion{}, errors.Invalid(
			clientErr.EUnsupportedProtocolVersion,
			"protocol name must be MQTT; this broker implements MQTT 3.1.1",
		)
	}
	if level != ProtocolLevel311 {
		return ProtocolVersion{}, errors.Invalid(
			clientErr.EUnsupportedProtocolVersion,
			"unsupported protocol level; this broker implements MQTT 3.1.1 (level 4)",
		)
	}
	return ProtocolVersion{name: name, level: level}, nil
}

// Name renders the protocol name.
func (p ProtocolVersion) Name() string { return p.name }

// Level renders the protocol level.
func (p ProtocolVersion) Level() byte { return p.level }

// String renders the version for logs.
func (p ProtocolVersion) String() string {
	if p.level == ProtocolLevel311 {
		return "MQTT 3.1.1"
	}
	return p.name
}
