// Package error holds the stable error codes owned by the client aggregate.
package error

// Client aggregate error codes.
const (
	// EInvalidClientID is returned for a client identifier that is empty when
	// one is required, too long, or malformed.
	EInvalidClientID = "invalid_client_id"
	// EUnsupportedProtocolVersion is returned for a CONNECT that is not 3.1.1.
	EUnsupportedProtocolVersion = "unsupported_protocol_version"
	// EInvalidKeepAlive is returned for a keep-alive outside the allowed range.
	EInvalidKeepAlive = "invalid_keep_alive"
	// ENotAuthorized is returned when credentials are rejected.
	ENotAuthorized = "not_authorized"
	// EClientNotFound is returned when no connected client has an identifier.
	EClientNotFound = "client_not_found"
	// EKeepAliveExpired is returned when a client misses its keep-alive window.
	EKeepAliveExpired = "keep_alive_expired"
	// EProtocolViolation is returned when a client breaks a protocol rule.
	EProtocolViolation = "protocol_violation"
	// ESecondConnect is returned when a client sends CONNECT twice.
	ESecondConnect = "second_connect_packet"
	// ENotConnected is returned when a non-CONNECT packet arrives first.
	ENotConnected = "not_connected"
)
