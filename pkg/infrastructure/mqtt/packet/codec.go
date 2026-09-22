package packet

import (
	"encoding/binary"
	"unicode/utf16"
	"unicode/utf8"
)

// maxRemainingLength is the largest value the four-byte varint can express
// (§2.2.3): 256 MB minus one.
const maxRemainingLength = 268435455

// encodeRemainingLength appends the variable-length integer encoding of n.
//
// The scheme is seven bits of payload per byte with the top bit as a
// continuation flag, so a small packet — the overwhelming majority — spends one
// byte on its length rather than four.
func encodeRemainingLength(dst []byte, n int) []byte {
	for {
		digit := byte(n % 128)
		n /= 128
		if n > 0 {
			digit |= 0x80
		}
		dst = append(dst, digit)
		if n == 0 {
			return dst
		}
	}
}

// decodeRemainingLength reads the varint from buf, returning the value and the
// number of bytes consumed.
//
// It refuses a fifth byte rather than accepting it. A decoder that keeps reading
// continuation bytes can be walked off the end of a buffer by a hostile client,
// and the spec caps the field at four bytes precisely so it cannot be.
func decodeRemainingLength(buf []byte) (value int, consumed int, err error) {
	multiplier := 1

	for i := 0; i < 4; i++ {
		if i >= len(buf) {
			return 0, 0, ErrMalformedPacket
		}
		digit := buf[i]
		value += int(digit&0x7F) * multiplier
		if digit&0x80 == 0 {
			return value, i + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, ErrRemainingLengthOverflow
}

// remainingLengthSize reports how many bytes the varint encoding of n occupies,
// so an encoder can size its buffer in one pass.
func remainingLengthSize(n int) int {
	switch {
	case n < 128:
		return 1
	case n < 16384:
		return 2
	case n < 2097152:
		return 3
	default:
		return 4
	}
}

// ---------------------------------------------------------------------------
// Field encoding
// ---------------------------------------------------------------------------

// appendUint16 appends a big-endian two-byte integer. MQTT is big-endian
// throughout (§1.5.2).
func appendUint16(dst []byte, v uint16) []byte {
	return binary.BigEndian.AppendUint16(dst, v)
}

// appendString appends a length-prefixed UTF-8 string (§1.5.3).
func appendString(dst []byte, s string) []byte {
	dst = appendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

// appendBytes appends a length-prefixed binary blob.
func appendBytes(dst, b []byte) []byte {
	dst = appendUint16(dst, uint16(len(b)))
	return append(dst, b...)
}

// stringSize reports the encoded size of a string field.
func stringSize(s string) int { return 2 + len(s) }

// ---------------------------------------------------------------------------
// Field decoding
// ---------------------------------------------------------------------------

// reader walks a decoded packet body.
//
// Every read is bounds-checked and returns an error rather than panicking,
// because the bytes it walks come straight off a socket from an unauthenticated
// peer. A panic here would be a remote denial of service.
type reader struct {
	buf []byte
	pos int
}

// newReader builds a reader over a packet body.
func newReader(buf []byte) *reader { return &reader{buf: buf} }

// remaining reports how many unread bytes are left.
func (r *reader) remaining() int { return len(r.buf) - r.pos }

// done reports whether every byte has been consumed.
func (r *reader) done() bool { return r.pos >= len(r.buf) }

// byteValue reads one byte.
func (r *reader) byteValue() (byte, error) {
	if r.remaining() < 1 {
		return 0, ErrMalformedPacket
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// uint16Value reads a big-endian two-byte integer.
func (r *reader) uint16Value() (uint16, error) {
	if r.remaining() < 2 {
		return 0, ErrMalformedPacket
	}
	v := binary.BigEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

// stringValue reads a length-prefixed UTF-8 string and validates it.
func (r *reader) stringValue() (string, error) {
	raw, err := r.bytesValue()
	if err != nil {
		return "", err
	}
	if !validUTF8(raw) {
		return "", ErrInvalidUTF8
	}
	return string(raw), nil
}

// bytesValue reads a length-prefixed blob.
//
// The returned slice aliases the read buffer. Callers that keep the bytes past
// the lifetime of the packet must copy them; PUBLISH does exactly that, because
// a payload is handed to subscribers long after the read buffer is reused.
func (r *reader) bytesValue() ([]byte, error) {
	length, err := r.uint16Value()
	if err != nil {
		return nil, err
	}
	if r.remaining() < int(length) {
		return nil, ErrMalformedPacket
	}
	out := r.buf[r.pos : r.pos+int(length)]
	r.pos += int(length)
	return out, nil
}

// rest returns the unread remainder, which for PUBLISH is the payload.
func (r *reader) rest() []byte {
	out := r.buf[r.pos:]
	r.pos = len(r.buf)
	return out
}

// validUTF8 applies the string rules of §1.5.3.
//
// Well-formed UTF-8 is not enough. The spec additionally forbids U+0000 and the
// surrogate range, and either would otherwise travel happily through a topic
// name and out to every subscriber, where some client's string handling will
// eventually mishandle it.
func validUTF8(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		if r == 0 {
			return false
		}
		if utf16.IsSurrogate(r) {
			return false
		}
	}
	return true
}
