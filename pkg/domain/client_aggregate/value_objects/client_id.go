package value_objects

import (
	"strings"
	"unicode/utf8"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	clientErr "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/error"
)

// Client identifier limits (§3.1.3.1).
const (
	// MaxClientIDLength is the limit this broker enforces.
	//
	// The spec requires a server to accept at least 23 bytes and permits it to
	// accept more. 128 is generous enough for the UUID-and-prefix schemes real
	// fleets use, and bounded enough that the identifier cannot be used to
	// inflate the broker's per-client memory.
	MaxClientIDLength = 128
	// SpecMinimumSupportedLength is the 23 bytes §3.1.3.1 requires every server
	// to accept. Kept as a named constant so the test that asserts compliance
	// reads as a spec citation rather than a magic number.
	SpecMinimumSupportedLength = 23
)

// ClientID identifies a client and, with it, the session the broker keeps for
// that client.
//
// This is the identity of both the Client and the Session aggregate: MQTT keys
// session state by client identifier, which is why a reconnecting client with
// the same id resumes its subscriptions and its queued messages.
type ClientID string

// NewClientID validates and constructs a ClientID.
//
// An empty identifier is rejected here. The protocol allows one only alongside
// clean session 1, in which case the server assigns it — that rule lives in the
// connection service, which is where the clean-session flag is known. This
// constructor's contract is simply "a real identifier", which keeps it usable
// for both the client-supplied and broker-assigned cases.
func NewClientID(raw string) (ClientID, error) {
	if raw == "" {
		return "", errors.Invalid(clientErr.EInvalidClientID, "client id must not be empty")
	}
	if len(raw) > MaxClientIDLength {
		return "", errors.Invalid(
			clientErr.EInvalidClientID,
			"client id must not exceed 128 bytes",
		)
	}
	if !utf8.ValidString(raw) {
		return "", errors.Invalid(clientErr.EInvalidClientID, "client id must be valid UTF-8")
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.Invalid(
			clientErr.EInvalidClientID,
			"client id must not contain a null character",
		)
	}
	return ClientID(raw), nil
}

// String renders the identifier.
func (c ClientID) String() string { return string(c) }

// IsZero reports whether the identifier is unset.
func (c ClientID) IsZero() bool { return c == "" }
