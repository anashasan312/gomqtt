package memory

import (
	"sync"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
	sessionErr "github.com/anashasan/gomqtt/pkg/domain/session_aggregate/error"
)

var _ persistence.ISessionStore = (*SessionStore)(nil)

// sessionEntry pairs a session with its own mutex.
//
// A per-session lock rather than one lock over the map is the point of this
// type. A publish to a thousand subscribers touches a thousand sessions; under
// a single store-wide lock those thousand updates would serialise behind each
// other and the broker's fan-out would be single-threaded no matter how many
// cores it had.
type sessionEntry struct {
	mu      sync.Mutex
	session *sessionAgg.Session
}

// SessionStore holds Session aggregates in memory.
//
// Two levels of locking, with a deliberate discipline: the store's RWMutex
// guards the map's shape and is held only long enough to find an entry; the
// entry's own mutex guards the aggregate and is held across the caller's
// mutation. The store lock is always released before the entry lock is taken,
// so the two can never deadlock against each other.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[clientVO.ClientID]*sessionEntry
}

// NewSessionStore builds an empty store.
func NewSessionStore() *SessionStore {
	return &SessionStore{sessions: make(map[clientVO.ClientID]*sessionEntry)}
}

// Get returns a session by client identifier.
//
// The returned pointer must not be mutated; use Update for that. Read-only
// callers — the admin API, the delivery path checking whether a client is
// connected — are safe because the fields they read are only ever written under
// Update, and they tolerate a value that is one instant stale.
func (s *SessionStore) Get(clientID clientVO.ClientID) (*sessionAgg.Session, bool) {
	s.mu.RLock()
	entry, ok := s.sessions[clientID]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}
	return entry.session, true
}

// Put stores a session, replacing any existing one.
func (s *SessionStore) Put(session *sessionAgg.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sessions[session.ClientID()] = &sessionEntry{session: session}
}

// Delete removes a session and reports whether one existed.
func (s *SessionStore) Delete(clientID clientVO.ClientID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[clientID]; !ok {
		return false
	}
	delete(s.sessions, clientID)
	return true
}

// Update runs fn against a session while holding that session's lock.
//
// This is the only sanctioned way to mutate a session. fn must not call back
// into the store, and must not block on I/O: it runs with the session's lock
// held, so a blocking call inside it stalls every publisher trying to deliver
// to that client.
func (s *SessionStore) Update(
	clientID clientVO.ClientID,
	fn func(*sessionAgg.Session) error,
) error {
	s.mu.RLock()
	entry, ok := s.sessions[clientID]
	s.mu.RUnlock()

	if !ok {
		return errors.NotFound(
			sessionErr.ESessionNotFound,
			"no session found for client "+clientID.String(),
		)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	return fn(entry.session)
}

// Count returns how many sessions exist.
func (s *SessionStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.sessions)
}

// All returns a snapshot of every session.
func (s *SessionStore) All() []*sessionAgg.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*sessionAgg.Session, 0, len(s.sessions))
	for _, entry := range s.sessions {
		out = append(out, entry.session)
	}
	return out
}
