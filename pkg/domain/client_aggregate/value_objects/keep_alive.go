package value_objects

import "time"

// KeepAlive is the interval a client promises to communicate within (§3.1.2.10).
//
// A duration wrapper rather than a bare uint16, because the protocol's unit is
// seconds and the rest of the broker works in time.Duration — converting at the
// boundary once is what keeps a stray "* time.Second" from appearing in three
// different places and disagreeing in one of them.
type KeepAlive struct {
	seconds uint16
}

// KeepAliveDisabled is the keep-alive that never expires.
var KeepAliveDisabled = KeepAlive{seconds: 0}

// NewKeepAlive constructs a KeepAlive from the CONNECT field.
//
// Every uint16 is valid, including zero, which §3.1.2.10 defines as "the
// keep-alive mechanism is off". There is therefore no error return: a
// constructor that cannot fail should not pretend it can.
func NewKeepAlive(seconds uint16) KeepAlive { return KeepAlive{seconds: seconds} }

// Seconds renders the raw protocol value.
func (k KeepAlive) Seconds() uint16 { return k.seconds }

// IsDisabled reports whether the client opted out of keep-alive.
func (k KeepAlive) IsDisabled() bool { return k.seconds == 0 }

// Duration returns the keep-alive as a duration.
func (k KeepAlive) Duration() time.Duration {
	return time.Duration(k.seconds) * time.Second
}

// Timeout returns how long the broker waits before declaring the client dead.
//
// §3.1.2.10 requires the server to allow one and a half keep-alive intervals
// before disconnecting. The margin is not politeness: a client that sends its
// PINGREQ exactly on the interval would otherwise be killed by ordinary network
// jitter, and a broker that disconnects healthy clients is worse than one that
// is slow to notice dead ones.
func (k KeepAlive) Timeout() time.Duration {
	if k.IsDisabled() {
		return 0
	}
	return k.Duration() + k.Duration()/2
}
