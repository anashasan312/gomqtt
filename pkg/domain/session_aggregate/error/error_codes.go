// Package error holds the stable error codes owned by the session aggregate.
package error

// Session aggregate error codes.
const (
	// ESessionNotFound is returned when no session exists for a client id.
	ESessionNotFound = "session_not_found"
	// EPacketIDExhausted is returned when all 65535 packet identifiers are in
	// flight for one session.
	EPacketIDExhausted = "packet_id_exhausted"
	// EUnknownPacketID is returned for an acknowledgement whose identifier is
	// not in flight.
	EUnknownPacketID = "unknown_packet_id"
	// EInflightFull is returned when the in-flight window is at capacity.
	EInflightFull = "inflight_window_full"
	// EQueueFull is returned when an offline client's queue is at capacity.
	EQueueFull = "offline_queue_full"
	// ESubscriptionLimit is returned when a session has too many subscriptions.
	ESubscriptionLimit = "subscription_limit_reached"
)
