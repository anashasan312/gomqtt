// Package error holds the stable error codes owned by the message aggregate.
//
// Codes live next to the aggregate that enforces the invariant, so there is
// exactly one place to look when someone asks what a code means.
package error

// Message aggregate error codes.
const (
	// EInvalidTopicName is returned for a topic that is empty, too long,
	// contains a wildcard, or is not valid UTF-8.
	EInvalidTopicName = "invalid_topic_name"
	// EInvalidTopicFilter is returned for a filter whose wildcards are
	// misplaced.
	EInvalidTopicFilter = "invalid_topic_filter"
	// EInvalidQoS is returned for a QoS outside 0..2.
	EInvalidQoS = "invalid_qos"
	// EUnsupportedQoS is returned for a QoS this broker does not implement.
	EUnsupportedQoS = "unsupported_qos"
	// EPayloadTooLarge is returned when a payload exceeds the configured limit.
	EPayloadTooLarge = "payload_too_large"
	// EInvalidPacketID is returned for a packet identifier of zero.
	EInvalidPacketID = "invalid_packet_id"
)
