// Package persistence declares the outbound ports owned by the domain.
//
// The interfaces live here, beside the aggregates they serve, and the concrete
// types live in pkg/infrastructure. Dependencies therefore point inwards:
// infrastructure imports domain, never the reverse. Today every implementation
// is in memory; adding a disk-backed session store is a new package plus one
// binding in di, and no service changes.
package persistence

import (
	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
)

// ISessionStore holds Session aggregates across connections.
//
// Sessions are mutable and shared: a publisher goroutine queues a message into
// a subscriber's session while that subscriber's own goroutine drains it. The
// store therefore hands out access under a lock rather than handing out
// pointers — see Update — so the aggregate's invariants survive concurrency.
type ISessionStore interface {
	// Get returns a session by client identifier.
	Get(clientID clientVO.ClientID) (*sessionAgg.Session, bool)

	// Put stores a session, replacing any existing one for that client.
	Put(session *sessionAgg.Session)

	// Delete removes a session and reports whether one existed.
	Delete(clientID clientVO.ClientID) bool

	// Update runs fn against a session while holding that session's lock.
	//
	// This is the only safe way to mutate a session. A caller that took the
	// pointer from Get and mutated it would race every other goroutine touching
	// the same client, and the races that produces — a lost packet identifier,
	// a double-freed window slot — are exactly the ones that are invisible
	// until production.
	Update(clientID clientVO.ClientID, fn func(*sessionAgg.Session) error) error

	// Count returns how many sessions exist.
	Count() int

	// All returns a snapshot of every session, for the admin API.
	All() []*sessionAgg.Session
}

// IClientRegistry tracks currently connected clients.
//
// Separate from ISessionStore because the lifetimes differ: a client exists for
// one TCP connection, a session may outlive many. Merging them would make
// "list connected clients" and "list stored sessions" the same query, and they
// are not.
type IClientRegistry interface {
	// Register adds a connected client and returns the client it displaced, if
	// any.
	//
	// §3.1.4 requires the broker to close an existing connection using the same
	// client identifier. Returning the displaced client rather than closing it
	// here keeps the registry free of transport concerns: it knows who is
	// connected, not how to hang up on them.
	Register(client *clientAgg.Client) (displaced *clientAgg.Client, err error)

	// Unregister removes a client, but only if the registered client is the
	// same instance.
	//
	// The identity check closes a real race: a slow disconnect of an old
	// connection must not evict the new connection that already took over the
	// same client identifier.
	Unregister(clientID clientVO.ClientID, client *clientAgg.Client) bool

	// Get returns a connected client by identifier.
	Get(clientID clientVO.ClientID) (*clientAgg.Client, bool)

	// Count returns how many clients are connected.
	Count() int

	// All returns a snapshot of connected clients, for the admin API.
	All() []*clientAgg.Client
}

// IRetainedStore holds the last retained message per topic (§3.3.1.3).
type IRetainedStore interface {
	// Store sets the retained message for a topic.
	Store(topic msgVO.TopicName, message *msgAgg.Message)

	// Remove clears the retained message for a topic, which is what a
	// zero-length retained publication means.
	Remove(topic msgVO.TopicName)

	// Matching returns every retained message whose topic matches a filter.
	//
	// This is the query a new subscriber triggers: §3.8.4 requires the broker
	// to deliver matching retained messages immediately on subscription, which
	// is how a client learns the current state of a topic without waiting for
	// the next publication.
	Matching(filter msgVO.TopicFilter) []*msgAgg.Message

	// Count returns how many topics have a retained message.
	Count() int

	// All returns every retained message, for the admin API.
	All() []*msgAgg.Message
}

// ISubscriptionIndex maps a topic to the clients that should receive it.
//
// This is the broker's hottest query: it runs once per published message, and
// its cost decides the broker's throughput. The interface deliberately says
// nothing about how — a trie today, something else tomorrow — and the
// implementation is verified against TopicFilter.Matches, which is the
// readable definition of the same question.
type ISubscriptionIndex interface {
	// Subscribe records that a client is interested in a filter.
	Subscribe(filter msgVO.TopicFilter, clientID clientVO.ClientID, grantedQoS msgVO.QoS)

	// Unsubscribe removes one client's interest in a filter.
	Unsubscribe(filter msgVO.TopicFilter, clientID clientVO.ClientID)

	// UnsubscribeAll removes every subscription a client holds, which is what a
	// clean-session disconnect does.
	UnsubscribeAll(clientID clientVO.ClientID)

	// Match returns every client subscribed to a topic, with the granted QoS.
	//
	// A client appears at most once even if several of its filters match, at
	// the highest QoS among them.
	Match(topic msgVO.TopicName) []Subscriber

	// FilterCount returns how many distinct filters are registered.
	FilterCount() int

	// SubscriptionCount returns the total number of client/filter pairs.
	SubscriptionCount() int
}

// Subscriber is one result of a subscription index lookup.
type Subscriber struct {
	ClientID   clientVO.ClientID
	GrantedQoS msgVO.QoS
}
