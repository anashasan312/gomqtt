package memory

import (
	"sync"

	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
)

var _ persistence.IClientRegistry = (*ClientRegistry)(nil)

// ClientRegistry tracks currently connected clients.
type ClientRegistry struct {
	mu      sync.RWMutex
	clients map[clientVO.ClientID]*clientAgg.Client
}

// NewClientRegistry builds an empty registry.
func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{clients: make(map[clientVO.ClientID]*clientAgg.Client)}
}

// Register adds a client and returns the one it displaced, if any.
//
// §3.1.4 requires that a second CONNECT using an existing client identifier
// causes the broker to disconnect the first. The swap happens under one lock so
// there is no instant at which both connections are registered — otherwise a
// message published in that window could be delivered twice, or to the
// connection that is about to be closed.
//
// The displaced client is returned rather than closed here: this type knows who
// is connected, not how to hang up on them. Keeping the socket out of the
// registry is what lets it be tested without a network.
func (r *ClientRegistry) Register(client *clientAgg.Client) (*clientAgg.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	displaced := r.clients[client.ID()]
	r.clients[client.ID()] = client
	return displaced, nil
}

// Unregister removes a client, but only if the registered client is the same
// instance.
//
// The identity check is load-bearing. When a client reconnects, the old
// connection's cleanup runs concurrently with the new connection's registration;
// without this check, the slow teardown of the old socket would evict the new
// client that had already taken over the identifier, and the reconnected client
// would silently stop receiving messages.
func (r *ClientRegistry) Unregister(
	clientID clientVO.ClientID,
	client *clientAgg.Client,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	current, ok := r.clients[clientID]
	if !ok || current != client {
		return false
	}
	delete(r.clients, clientID)
	return true
}

// Get returns a connected client by identifier.
func (r *ClientRegistry) Get(clientID clientVO.ClientID) (*clientAgg.Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	client, ok := r.clients[clientID]
	return client, ok
}

// Count returns how many clients are connected.
func (r *ClientRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.clients)
}

// All returns a snapshot of connected clients.
func (r *ClientRegistry) All() []*clientAgg.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*clientAgg.Client, 0, len(r.clients))
	for _, client := range r.clients {
		out = append(out, client)
	}
	return out
}
