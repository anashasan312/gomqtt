package packet_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

// These tests are the security surface of the broker. Every byte they feed in
// arrives, in production, from a peer that has done nothing but complete a TCP
// handshake — so "decoder rejects it" and "decoder does not panic or allocate
// wildly" are the two properties that matter.
func TestDecode_RejectsMalformedInput(t *testing.T) {
	cases := []struct {
		name  string
		bytes []byte
	}{
		{"empty input", nil},
		{"one byte", []byte{0x10}},
		{"reserved packet type 0", []byte{0x00, 0x00}},
		{"reserved packet type 15", []byte{0xF0, 0x00}},

		// Fixed-header flag violations (§2.2.2). Each of these is a well-formed
		// length with a flag nibble the spec pins to another value.
		{"CONNECT with non-zero flags", []byte{0x11, 0x00}},
		{"SUBSCRIBE without its mandatory 0x02", []byte{0x80, 0x00}},
		{"PUBREL without its mandatory 0x02", []byte{0x60, 0x02, 0x00, 0x01}},
		{"PUBLISH with QoS 3", []byte{0x36, 0x03, 0x00, 0x01, 0x61}},
		{"PUBLISH QoS 0 with DUP set", []byte{0x38, 0x03, 0x00, 0x01, 0x61}},

		// Length-field attacks: the declared length exceeds what is present.
		{"truncated body", []byte{0x30, 0x10, 0x00, 0x01, 0x61}},
		{"string length past end of packet", []byte{0x30, 0x04, 0x00, 0xFF, 0x61, 0x62}},
		{"remaining length over four bytes", []byte{0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},

		// Protocol-level violations in otherwise well-formed packets.
		{"PUBLISH QoS 1 with packet id zero", []byte{0x32, 0x05, 0x00, 0x01, 0x61, 0x00, 0x00}},
		{"PUBACK with packet id zero", []byte{0x40, 0x02, 0x00, 0x00}},
		{"SUBSCRIBE with no filters", []byte{0x82, 0x02, 0x00, 0x01}},
		{"UNSUBSCRIBE with no filters", []byte{0xA2, 0x02, 0x00, 0x01}},
		{"PINGREQ with a non-empty body", []byte{0xC0, 0x01, 0x00}},
		{"DISCONNECT with a non-empty body", []byte{0xE0, 0x01, 0x00}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The assertion is as much that this does not panic as that it errors.
			require.NotPanics(t, func() {
				_, err := packet.Decode(tc.bytes)
				assert.Error(t, err)
			})
		})
	}
}

func TestDecodeConnect_RejectsSpecViolations(t *testing.T) {
	// A valid CONNECT, which each case then corrupts in exactly one way — so a
	// failure points at the rule under test rather than at the fixture.
	valid := func() []byte {
		out, err := (&packet.Connect{
			ProtocolName:  packet.ProtocolName,
			ProtocolLevel: packet.ProtocolLevel311,
			CleanSession:  true,
			KeepAlive:     60,
			ClientID:      "c",
		}).Encode(nil)
		require.NoError(t, err)
		return out
	}

	t.Run("wrong protocol name", func(t *testing.T) {
		b := valid()
		b[4] = 'X' // "MQTT" -> "XQTT"
		_, err := packet.Decode(b)
		assert.Error(t, err)
	})

	t.Run("reserved connect flag set", func(t *testing.T) {
		b := valid()
		// Connect flags sit right after the protocol level byte.
		b[9] |= 0x01
		_, err := packet.Decode(b)
		assert.Error(t, err)
	})

	t.Run("password flag without username flag", func(t *testing.T) {
		b := valid()
		b[9] |= 0x40 // password flag, username flag left clear
		_, err := packet.Decode(b)
		assert.Error(t, err)
	})

	t.Run("will QoS set without the will flag", func(t *testing.T) {
		b := valid()
		b[9] |= 0x08 // will QoS 1, will flag clear
		_, err := packet.Decode(b)
		assert.Error(t, err)
	})

	t.Run("trailing bytes", func(t *testing.T) {
		b := valid()
		b[1]++ // claim one more byte of remaining length
		b = append(b, 0x00)
		_, err := packet.Decode(b)
		assert.Error(t, err)
	})
}

func TestDecode_RejectsInvalidUTF8Strings(t *testing.T) {
	// §1.5.3 forbids more than just invalid UTF-8: a null character and the
	// surrogate range are also illegal, and both would otherwise travel out to
	// every subscriber inside a topic name.
	cases := map[string][]byte{
		"invalid UTF-8 sequence": {0xFF, 0xFE},
		"embedded null":          {'a', 0x00, 'b'},
		"encoded surrogate":      {0xED, 0xA0, 0x80},
	}

	for name, topic := range cases {
		t.Run(name, func(t *testing.T) {
			body := append([]byte{byte(len(topic) >> 8), byte(len(topic))}, topic...)
			raw := append([]byte{0x30, byte(len(body))}, body...)

			_, err := packet.Decode(raw)
			assert.Error(t, err)
		})
	}
}

func TestDecodePublish_RejectsWildcardsInTheTopic(t *testing.T) {
	// §3.3.2.1. A publisher using a wildcard is confused or probing; either way
	// the connection must close rather than the broker guessing what it meant.
	for _, topic := range []string{"a/+/c", "a/#", "+", "#"} {
		body := append([]byte{0x00, byte(len(topic))}, topic...)
		raw := append([]byte{0x30, byte(len(body))}, body...)

		_, err := packet.Decode(raw)
		assert.Error(t, err, "topic %q must be rejected", topic)
	}
}

// FuzzDecode asserts the decoder never panics, whatever it is fed.
//
// A panic in this package is a remote crash: the input is attacker-controlled by
// definition. The seed corpus is every well-formed packet type plus the known
// awkward shapes, so the fuzzer starts from valid structure and mutates outward
// rather than spending its budget rediscovering the fixed header.
func FuzzDecode(f *testing.F) {
	seeds := [][]byte{
		{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00},
		{0x20, 0x02, 0x00, 0x00},
		{0x30, 0x05, 0x00, 0x01, 'a', 'b', 'c'},
		{0x32, 0x07, 0x00, 0x01, 'a', 0x00, 0x01, 'b', 'c'},
		{0x40, 0x02, 0x00, 0x01},
		{0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x01},
		{0x90, 0x03, 0x00, 0x01, 0x01},
		{0xA2, 0x05, 0x00, 0x01, 0x00, 0x01, 'a'},
		{0xC0, 0x00},
		{0xD0, 0x00},
		{0xE0, 0x00},
		{0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		{},
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Both entry points, because they parse the fixed header separately.
		if p, err := packet.Decode(data); err == nil {
			// Anything that decodes must also re-encode without panicking:
			// the broker echoes decoded packets back out on the fan-out path.
			_, _ = p.Encode(nil)
		}
		if p, err := packet.NewReader(bytes.NewReader(data), 0, 4096).ReadPacket(); err == nil {
			_, _ = p.Encode(nil)
		}
	})
}
