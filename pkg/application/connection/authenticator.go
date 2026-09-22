package connection

import (
	"context"
	"crypto/subtle"

	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

var _ Authenticator = (*StaticAuthenticator)(nil)

// StaticAuthenticator checks credentials against a fixed table.
//
// Suitable for a small fleet with credentials injected from the environment.
// It is not a production identity system, and the Authenticator port exists so
// a real one can replace it without touching the connection service.
type StaticAuthenticator struct {
	users          map[string]string
	allowAnonymous bool
}

// NewStaticAuthenticator builds a StaticAuthenticator.
func NewStaticAuthenticator(users map[string]string, allowAnonymous bool) *StaticAuthenticator {
	copied := make(map[string]string, len(users))
	for name, password := range users {
		copied[name] = password
	}
	return &StaticAuthenticator{users: copied, allowAnonymous: allowAnonymous}
}

// Authenticate checks a client's credentials.
func (a *StaticAuthenticator) Authenticate(
	_ context.Context,
	_ clientVO.ClientID,
	username string,
	password []byte,
) (bool, packet.ConnectReturnCode) {
	if username == "" {
		if a.allowAnonymous {
			return true, packet.ConnectAccepted
		}
		return false, packet.ConnectNotAuthorized
	}

	expected, known := a.users[username]
	if !known {
		// The same return code and the same amount of work as a wrong password,
		// so the response does not tell an attacker which usernames exist.
		// ConstantTimeCompare against the supplied password keeps the timing
		// comparable too.
		subtle.ConstantTimeCompare(password, password)
		return false, packet.ConnectBadCredentials
	}

	if subtle.ConstantTimeCompare([]byte(expected), password) != 1 {
		return false, packet.ConnectBadCredentials
	}
	return true, packet.ConnectAccepted
}
