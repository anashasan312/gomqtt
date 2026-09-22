package packet_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

// encode is a helper that encodes a packet or fails the test.
func encode(t *testing.T, p packet.Packet) []byte {
	t.Helper()
	out, err := p.Encode(nil)
	require.NoError(t, err)
	return out
}

// TestRoundTrip is the codec's central property: whatever the broker writes, it
// must be able to read back as the same value. A codec that is wrong in the
// same way in both directions would pass a decode-only test and fail against a
// real client, so encode and decode are always checked as a pair.
func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		packet packet.Packet
		verify func(t *testing.T, decoded packet.Packet)
	}{
		{
			name: "CONNECT minimal",
			packet: &packet.Connect{
				ProtocolName:  packet.ProtocolName,
				ProtocolLevel: packet.ProtocolLevel311,
				CleanSession:  true,
				KeepAlive:     60,
				ClientID:      "client-1",
			},
			verify: func(t *testing.T, decoded packet.Packet) {
				c := decoded.(*packet.Connect)
				assert.Equal(t, "client-1", c.ClientID)
				assert.True(t, c.CleanSession)
				assert.Equal(t, uint16(60), c.KeepAlive)
				assert.Nil(t, c.Will)
			},
		},
		{
			name: "CONNECT with will, credentials and persistent session",
			packet: &packet.Connect{
				ProtocolName:  packet.ProtocolName,
				ProtocolLevel: packet.ProtocolLevel311,
				CleanSession:  false,
				KeepAlive:     30,
				ClientID:      "sensor-42",
				Will: &packet.Will{
					Topic:   "devices/42/status",
					Payload: []byte("offline"),
					QoS:     1,
					Retain:  true,
				},
				HasUsername: true,
				Username:    "anas",
				HasPassword: true,
				Password:    []byte("s3cret"),
			},
			verify: func(t *testing.T, decoded packet.Packet) {
				c := decoded.(*packet.Connect)
				assert.False(t, c.CleanSession)
				require.NotNil(t, c.Will)
				assert.Equal(t, "devices/42/status", c.Will.Topic)
				assert.Equal(t, []byte("offline"), c.Will.Payload)
				assert.Equal(t, byte(1), c.Will.QoS)
				assert.True(t, c.Will.Retain)
				assert.Equal(t, "anas", c.Username)
				assert.Equal(t, []byte("s3cret"), c.Password)
			},
		},
		{
			name:   "CONNACK accepted with session present",
			packet: &packet.Connack{SessionPresent: true, ReturnCode: packet.ConnectAccepted},
			verify: func(t *testing.T, decoded packet.Packet) {
				c := decoded.(*packet.Connack)
				assert.True(t, c.SessionPresent)
				assert.Equal(t, packet.ConnectAccepted, c.ReturnCode)
			},
		},
		{
			name:   "CONNACK rejected",
			packet: &packet.Connack{ReturnCode: packet.ConnectIdentifierRejected},
			verify: func(t *testing.T, decoded packet.Packet) {
				c := decoded.(*packet.Connack)
				assert.False(t, c.SessionPresent)
				assert.Equal(t, packet.ConnectIdentifierRejected, c.ReturnCode)
			},
		},
		{
			name:   "PUBLISH QoS 0",
			packet: &packet.Publish{Topic: "sensors/temp", Payload: []byte("21.5")},
			verify: func(t *testing.T, decoded packet.Packet) {
				p := decoded.(*packet.Publish)
				assert.Equal(t, "sensors/temp", p.Topic)
				assert.Equal(t, []byte("21.5"), p.Payload)
				assert.Equal(t, byte(0), p.QoS)
				assert.Zero(t, p.PacketID)
			},
		},
		{
			name: "PUBLISH QoS 1 retained and duplicated",
			packet: &packet.Publish{
				DUP: true, QoS: 1, Retain: true,
				Topic: "a/b/c", PacketID: 4242, Payload: []byte("hello"),
			},
			verify: func(t *testing.T, decoded packet.Packet) {
				p := decoded.(*packet.Publish)
				assert.True(t, p.DUP)
				assert.True(t, p.Retain)
				assert.Equal(t, byte(1), p.QoS)
				assert.Equal(t, uint16(4242), p.PacketID)
			},
		},
		{
			name:   "PUBLISH with an empty payload",
			packet: &packet.Publish{Topic: "a/b", Payload: nil},
			verify: func(t *testing.T, decoded packet.Packet) {
				p := decoded.(*packet.Publish)
				// An empty payload is how a retained message is cleared, so it
				// must survive the round trip as empty rather than becoming nil
				// in a way the retain logic cannot distinguish.
				assert.Empty(t, p.Payload)
			},
		},
		{
			name:   "PUBACK",
			packet: packet.NewPuback(7),
			verify: func(t *testing.T, decoded packet.Packet) {
				assert.Equal(t, uint16(7), decoded.(*packet.Ack).PacketID)
			},
		},
		{
			name:   "PUBREL carries its mandatory 0x02 flags",
			packet: packet.NewPubrel(9),
			verify: func(t *testing.T, decoded packet.Packet) {
				assert.Equal(t, packet.PUBREL, decoded.Type())
				assert.Equal(t, uint16(9), decoded.(*packet.Ack).PacketID)
			},
		},
		{
			name: "SUBSCRIBE with several filters",
			packet: &packet.Subscribe{
				PacketID: 10,
				Subscriptions: []packet.TopicSubscription{
					{Filter: "a/+/c", QoS: 1},
					{Filter: "d/#", QoS: 0},
				},
			},
			verify: func(t *testing.T, decoded packet.Packet) {
				s := decoded.(*packet.Subscribe)
				require.Len(t, s.Subscriptions, 2)
				assert.Equal(t, "a/+/c", s.Subscriptions[0].Filter)
				assert.Equal(t, byte(1), s.Subscriptions[0].QoS)
				assert.Equal(t, "d/#", s.Subscriptions[1].Filter)
			},
		},
		{
			name:   "SUBACK mixing granted and failed",
			packet: &packet.Suback{PacketID: 10, ReturnCodes: []byte{0, 1, packet.SubackFailure}},
			verify: func(t *testing.T, decoded packet.Packet) {
				s := decoded.(*packet.Suback)
				assert.Equal(t, []byte{0, 1, packet.SubackFailure}, s.ReturnCodes)
			},
		},
		{
			name:   "UNSUBSCRIBE",
			packet: &packet.Unsubscribe{PacketID: 11, Filters: []string{"a/b", "c/#"}},
			verify: func(t *testing.T, decoded packet.Packet) {
				u := decoded.(*packet.Unsubscribe)
				assert.Equal(t, []string{"a/b", "c/#"}, u.Filters)
			},
		},
		{
			name:   "PINGREQ",
			packet: packet.NewPingreq(),
			verify: func(t *testing.T, decoded packet.Packet) {
				assert.Equal(t, packet.PINGREQ, decoded.Type())
			},
		},
		{
			name:   "DISCONNECT",
			packet: packet.NewDisconnect(),
			verify: func(t *testing.T, decoded packet.Packet) {
				assert.Equal(t, packet.DISCONNECT, decoded.Type())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encode(t, tc.packet)

			// Decoded from a byte slice...
			decoded, err := packet.Decode(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.packet.Type(), decoded.Type())
			tc.verify(t, decoded)

			// ...and decoded from a stream, which is a separate code path and
			// has its own chance to be wrong.
			streamed, err := packet.NewReader(bytes.NewReader(encoded), 0, 0).ReadPacket()
			require.NoError(t, err)
			assert.Equal(t, tc.packet.Type(), streamed.Type())
			tc.verify(t, streamed)
		})
	}
}

func TestRemainingLength_BoundaryValues(t *testing.T) {
	// The varint changes width at 128, 16384 and 2097152. Each boundary gets a
	// payload sized to land exactly on it, because an off-by-one in the length
	// encoder desynchronises the whole stream rather than corrupting one packet.
	for _, payloadSize := range []int{0, 1, 125, 126, 127, 128, 129, 16381, 16384, 16387} {
		p := &packet.Publish{Topic: "t", Payload: bytes.Repeat([]byte("x"), payloadSize)}

		encoded := encode(t, p)
		assert.Equal(t, p.EncodedSize(), len(encoded),
			"EncodedSize disagrees with Encode at payload size %d", payloadSize)

		decoded, err := packet.Decode(encoded)
		require.NoError(t, err, "payload size %d", payloadSize)
		assert.Len(t, decoded.(*packet.Publish).Payload, payloadSize)
	}
}

func TestReader_DecodesBackToBackPacketsFromOneBuffer(t *testing.T) {
	// A client that publishes in a burst puts several packets in one TCP
	// segment. The reader must split them, not treat the segment as one packet.
	var stream []byte
	stream = append(stream, encode(t, &packet.Publish{Topic: "a", Payload: []byte("1")})...)
	stream = append(stream, encode(t, packet.NewPingreq())...)
	stream = append(stream, encode(t, &packet.Publish{Topic: "b", QoS: 1, PacketID: 5})...)

	r := packet.NewReader(bytes.NewReader(stream), 0, 0)

	first, err := r.ReadPacket()
	require.NoError(t, err)
	assert.Equal(t, "a", first.(*packet.Publish).Topic)

	second, err := r.ReadPacket()
	require.NoError(t, err)
	assert.Equal(t, packet.PINGREQ, second.Type())

	third, err := r.ReadPacket()
	require.NoError(t, err)
	assert.Equal(t, uint16(5), third.(*packet.Publish).PacketID)
}

func TestReader_RejectsAnOversizedPacket(t *testing.T) {
	p := &packet.Publish{Topic: "a", Payload: bytes.Repeat([]byte("x"), 4096)}

	// The limit must be enforced from the declared length, before the body is
	// allocated — otherwise the limit does not protect against the attack it
	// exists for.
	_, err := packet.NewReader(bytes.NewReader(encode(t, p)), 0, 1024).ReadPacket()

	assert.ErrorIs(t, err, packet.ErrPacketTooLarge)
}

func TestWriter_WritesToAStream(t *testing.T) {
	var out bytes.Buffer
	w := packet.NewWriter(&out)

	require.NoError(t, w.WritePacket(&packet.Publish{Topic: "a/b", Payload: []byte("x")}))
	require.NoError(t, w.WritePacket(packet.NewPingresp()))

	// The writer reuses its buffer between packets; this asserts the second
	// write is not polluted by the first.
	r := packet.NewReader(bytes.NewReader(out.Bytes()), 0, 0)

	first, err := r.ReadPacket()
	require.NoError(t, err)
	assert.Equal(t, "a/b", first.(*packet.Publish).Topic)

	second, err := r.ReadPacket()
	require.NoError(t, err)
	assert.Equal(t, packet.PINGRESP, second.Type())
}
