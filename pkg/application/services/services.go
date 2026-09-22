// Package services declares the application-layer interfaces.
//
// Every service interface lives here, separate from its implementation package,
// so the transport depends on this package alone and cannot reach into a
// service's internals. The interfaces are split by *caller need* rather than by
// implementation: the connection handler needs all three, but the publishing
// service needs only a way to send to a client, and gets exactly that.
package services

import (
	"context"

	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

// PacketSink is how a service hands a packet to one client's writer.
//
// This is the seam between the application layer and the network. A service
// never touches a socket; it calls Send, and the transport's writer goroutine
// turns that into bytes. That is what lets the publishing service — including
// the whole QoS 1 flow — be tested with an in-memory sink and no network at all.
//
// Send must not block indefinitely. The transport implements it as a bounded
// queue that reports ErrClientQueueFull rather than stalling, because a single
// slow subscriber blocking a publisher would spread one bad client's problem
// across the whole broker.
type PacketSink interface {
	// Send queues a packet for delivery to the client.
	Send(p packet.Packet) error
	// Close terminates the client's connection.
	Close(reason clientAgg.DisconnectReason)
	// ClientID returns the client this sink belongs to.
	ClientID() clientVO.ClientID
}

// ConnectResult is what the connection service decided about a CONNECT.
type ConnectResult struct {
	// Client is the accepted client, nil when the CONNECT was refused.
	Client *clientAgg.Client
	// ReturnCode goes into the CONNACK.
	ReturnCode packet.ConnectReturnCode
	// SessionPresent goes into the CONNACK, telling the client whether its
	// stored session was resumed (§3.2.2.2).
	SessionPresent bool
	// Displaced is the connection this one took over from, which the caller
	// must close (§3.1.4).
	Displaced PacketSink
}

// Accepted reports whether the CONNECT was accepted.
func (r ConnectResult) Accepted() bool {
	return r.ReturnCode == packet.ConnectAccepted && r.Client != nil
}

// IConnectionService owns the client lifecycle: CONNECT, disconnect, and the
// session decisions that go with each.
type IConnectionService interface {
	// Connect validates a CONNECT and either accepts the client or produces the
	// CONNACK return code that refuses it.
	Connect(ctx context.Context, req *packet.Connect, remoteAddr string, sink PacketSink) (ConnectResult, error)

	// Disconnect tears a client down: publishes its will if the disconnect was
	// not graceful, and either keeps or discards its session according to the
	// clean-session flag.
	Disconnect(ctx context.Context, client *clientAgg.Client, reason clientAgg.DisconnectReason) error
}

// ISubscriptionService owns SUBSCRIBE and UNSUBSCRIBE.
type ISubscriptionService interface {
	// Subscribe registers filters and returns the per-filter SUBACK codes, in
	// the same order as the request.
	Subscribe(ctx context.Context, client *clientAgg.Client, req *packet.Subscribe, sink PacketSink) ([]byte, error)

	// Unsubscribe removes filters.
	Unsubscribe(ctx context.Context, client *clientAgg.Client, req *packet.Unsubscribe) error
}

// IPublishingService owns message routing and the QoS flows.
type IPublishingService interface {
	// Publish accepts a message from a client and fans it out to subscribers.
	Publish(ctx context.Context, message *msgAgg.Message) error

	// Acknowledge handles an inbound PUBACK, clearing an in-flight message.
	Acknowledge(ctx context.Context, clientID clientVO.ClientID, packetID uint16) error

	// DeliverRetained sends the retained messages matching a filter to a client
	// that has just subscribed (§3.8.4).
	DeliverRetained(ctx context.Context, sink PacketSink, filter msgVO.TopicFilter, grantedQoS msgVO.QoS) error

	// ResumeSession redelivers a reconnecting client's unacknowledged messages
	// and drains its offline queue (§4.4).
	ResumeSession(ctx context.Context, sink PacketSink) error

	// RegisterSink records where to deliver a connected client's messages and
	// returns the sink it displaced, if any.
	//
	// The swap happens under one lock and reports what it replaced, so a
	// takeover (§3.1.4) has no instant in which two sinks are registered for
	// one client — which would otherwise deliver the same message twice, or
	// deliver it to the connection that is about to be closed.
	RegisterSink(clientID clientVO.ClientID, sink PacketSink) (displaced PacketSink)

	// UnregisterSink removes a client's delivery target, but only if the
	// registered sink is the same instance — so a slow teardown of an old
	// connection cannot unregister the new one that replaced it.
	UnregisterSink(clientID clientVO.ClientID, sink PacketSink)
}

// IAdminService serves the read-only inspection API.
type IAdminService interface {
	// Stats returns a snapshot of broker counters.
	Stats(ctx context.Context) (BrokerStats, error)
	// Clients returns the connected clients.
	Clients(ctx context.Context) ([]ClientInfo, error)
	// Subscriptions returns every active subscription.
	Subscriptions(ctx context.Context) ([]SubscriptionInfo, error)
	// RetainedTopics returns every topic holding a retained message.
	RetainedTopics(ctx context.Context) ([]RetainedInfo, error)
}

// BrokerStats is the broker's counter snapshot.
type BrokerStats struct {
	ConnectedClients  int
	Sessions          int
	Subscriptions     int
	DistinctFilters   int
	RetainedMessages  int
	InflightMessages  int
	QueuedMessages    int
	UptimeSeconds     int64
	MessagesPublished uint64
	MessagesDelivered uint64
	MessagesDropped   uint64
}

// ClientInfo describes one connected client.
type ClientInfo struct {
	ClientID       string
	RemoteAddr     string
	Username       string
	CleanSession   bool
	KeepAlive      uint16
	ConnectedAt    string
	LastActivityAt string
	Subscriptions  int
	Inflight       int
	Queued         int
}

// SubscriptionInfo describes one subscription.
type SubscriptionInfo struct {
	ClientID     string
	Filter       string
	RequestedQoS byte
	GrantedQoS   byte
	Downgraded   bool
	CreatedAt    string
}

// RetainedInfo describes one retained message.
type RetainedInfo struct {
	Topic       string
	QoS         byte
	PayloadSize int
	CreatedAt   string
}
