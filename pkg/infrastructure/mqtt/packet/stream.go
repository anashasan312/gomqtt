package packet

import (
	"bufio"
	"io"
)

// DefaultMaxPacketSize bounds a single inbound packet.
//
// The protocol allows up to 256 MB. A broker that honours that on an
// unauthenticated socket lets any client that can complete a TCP handshake
// demand a quarter-gigabyte allocation, so the default here is 1 MB and the
// limit is configurable upward for deployments that genuinely need it.
const DefaultMaxPacketSize = 1 << 20

// Reader decodes control packets from a stream.
//
// It owns a bufio.Reader so that reading a two-byte PINGREQ does not cost a
// syscall per byte, and so a burst of small packets arriving in one TCP segment
// is decoded without going back to the kernel.
type Reader struct {
	src           *bufio.Reader
	maxPacketSize int
	// header is reused across reads to decode the fixed header without
	// allocating on the hot path.
	header [5]byte
}

// NewReader builds a Reader. A non-positive maxPacketSize resolves to the
// default.
func NewReader(src io.Reader, bufferSize, maxPacketSize int) *Reader {
	if bufferSize <= 0 {
		bufferSize = 4096
	}
	if maxPacketSize <= 0 {
		maxPacketSize = DefaultMaxPacketSize
	}
	return &Reader{
		src:           bufio.NewReaderSize(src, bufferSize),
		maxPacketSize: maxPacketSize,
	}
}

// ReadPacket decodes the next control packet.
//
// It returns io.EOF when the peer closed cleanly between packets, which the
// connection loop treats as a normal disconnect rather than an error.
func (r *Reader) ReadPacket() (Packet, error) {
	firstByte, err := r.src.ReadByte()
	if err != nil {
		return nil, err
	}

	packetType := Type(firstByte >> 4)
	flags := firstByte & 0x0F

	if !packetType.IsValid() {
		return nil, newProtocolError("reserved packet type " + packetType.String())
	}
	if err := validateFlags(packetType, flags); err != nil {
		return nil, err
	}

	remainingLength, err := r.readRemainingLength()
	if err != nil {
		return nil, err
	}
	// Enforced before allocating, which is the entire point: the length is a
	// number an untrusted peer chose.
	if remainingLength > r.maxPacketSize {
		return nil, ErrPacketTooLarge
	}

	var body []byte
	if remainingLength > 0 {
		body = make([]byte, remainingLength)
		if _, err := io.ReadFull(r.src, body); err != nil {
			// A truncated body is a malformed packet, not a clean close, even
			// when the underlying error is io.EOF.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, ErrMalformedPacket
			}
			return nil, err
		}
	}

	return decodeBody(packetType, flags, body)
}

// readRemainingLength decodes the varint length field a byte at a time,
// straight from the buffered reader.
func (r *Reader) readRemainingLength() (int, error) {
	for i := 0; i < 4; i++ {
		b, err := r.src.ReadByte()
		if err != nil {
			return 0, err
		}
		r.header[i] = b
		if b&0x80 == 0 {
			value, _, err := decodeRemainingLength(r.header[:i+1])
			return value, err
		}
	}
	return 0, ErrRemainingLengthOverflow
}

// decodeBody dispatches to the per-type decoder.
//
// A map of decoders would be tidier to look at but slower and harder to read
// here: this switch is on the hot path of every packet the broker receives, and
// the compiler turns it into a jump table.
func decodeBody(t Type, flags byte, body []byte) (Packet, error) {
	switch t {
	case CONNECT:
		return decodeConnect(body)
	case CONNACK:
		return decodeConnack(body)
	case PUBLISH:
		return decodePublish(flags, body)
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:
		return decodeAck(t, body)
	case SUBSCRIBE:
		return decodeSubscribe(body)
	case SUBACK:
		return decodeSuback(body)
	case UNSUBSCRIBE:
		return decodeUnsubscribe(body)
	case PINGREQ, PINGRESP, DISCONNECT:
		return decodeSimple(t, body)
	default:
		return nil, newProtocolError("unhandled packet type " + t.String())
	}
}

// Decode parses a complete packet from a byte slice.
//
// Used by tests and by the fuzz target; the connection path uses Reader, which
// does not need the whole packet in memory before it starts.
func Decode(buf []byte) (Packet, error) {
	if len(buf) < 2 {
		return nil, ErrMalformedPacket
	}

	packetType := Type(buf[0] >> 4)
	flags := buf[0] & 0x0F

	if !packetType.IsValid() {
		return nil, newProtocolError("reserved packet type " + packetType.String())
	}
	if err := validateFlags(packetType, flags); err != nil {
		return nil, err
	}

	remainingLength, consumed, err := decodeRemainingLength(buf[1:])
	if err != nil {
		return nil, err
	}

	start := 1 + consumed
	if len(buf) < start+remainingLength {
		return nil, ErrMalformedPacket
	}
	return decodeBody(packetType, flags, buf[start:start+remainingLength])
}

// Writer encodes control packets onto a stream.
//
// It is not safe for concurrent use. Each connection has exactly one writer
// goroutine, which is what makes that acceptable — and is a deliberate design
// choice rather than an oversight, because interleaving two packets on one TCP
// connection corrupts the stream irrecoverably.
type Writer struct {
	dst io.Writer
	// buf is reused across writes so a steady publish rate does not allocate.
	buf []byte
}

// NewWriter builds a Writer.
func NewWriter(dst io.Writer) *Writer {
	return &Writer{dst: dst, buf: make([]byte, 0, 512)}
}

// WritePacket encodes and writes one packet.
func (w *Writer) WritePacket(p Packet) error {
	w.buf = w.buf[:0]

	encoded, err := p.Encode(w.buf)
	if err != nil {
		return err
	}
	// Keep the grown buffer for next time; that is the whole point of reusing it.
	w.buf = encoded

	_, err = w.dst.Write(encoded)
	return err
}
