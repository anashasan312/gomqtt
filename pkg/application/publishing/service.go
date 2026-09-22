// Package publishing implements message routing and the QoS delivery flows.
//
// This is the broker's hot path: every published message passes through
// Publish, which is the function whose cost sets the broker's throughput.
package publishing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anashasan/gomqtt/pkg/application/services"
	"github.com/anashasan/gomqtt/pkg/common/clock"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
	sessionVO "github.com/anashasan/gomqtt/pkg/domain/session_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

var _ services.IPublishingService = (*PublishingService)(nil)

// Drop reasons, used as bounded metric label values.
const (
	dropReasonQueueFull    = "client_queue_full"
	dropReasonNoSession    = "no_session"
	dropReasonOfflineQueue = "offline_queue_overflow"
	dropReasonNoPacketID   = "packet_id_exhausted"
	dropReasonInflightFull = "inflight_window_full"
)

// Config tunes delivery.
type Config struct {
	// ResumeBatchSize caps how many queued messages are drained per reconnect
	// pass, so a client with a large backlog does not have it all pushed into
	// its socket at once.
	ResumeBatchSize int
}

// withDefaults fills unset fields.
func (c Config) withDefaults() Config {
	if c.ResumeBatchSize <= 0 {
		c.ResumeBatchSize = 64
	}
	return c
}

// PublishingService routes messages to subscribers.
type PublishingService struct {
	index    persistence.ISubscriptionIndex
	sessions persistence.ISessionStore
	retained persistence.IRetainedStore
	clock    clock.Clock
	metrics  metrics.Recorder
	log      logger.Logger
	cfg      Config

	// sinks maps a connected client to its writer.
	//
	// Held here rather than in the client registry because this is the only
	// component that sends to clients, and a registry that also held sockets
	// could not be tested without a network.
	sinksMu sync.RWMutex
	sinks   map[clientVO.ClientID]services.PacketSink

	published atomic.Uint64
	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// NewPublishingService builds a PublishingService.
func NewPublishingService(
	index persistence.ISubscriptionIndex,
	sessions persistence.ISessionStore,
	retained persistence.IRetainedStore,
	clk clock.Clock,
	recorder metrics.Recorder,
	log logger.Logger,
	cfg Config,
) *PublishingService {
	return &PublishingService{
		index:    index,
		sessions: sessions,
		retained: retained,
		clock:    clk,
		metrics:  recorder,
		log:      log,
		cfg:      cfg.withDefaults(),
		sinks:    make(map[clientVO.ClientID]services.PacketSink),
	}
}

// RegisterSink records a connected client's writer and returns the one it
// displaced.
func (s *PublishingService) RegisterSink(
	clientID clientVO.ClientID,
	sink services.PacketSink,
) services.PacketSink {
	s.sinksMu.Lock()
	defer s.sinksMu.Unlock()

	displaced := s.sinks[clientID]
	s.sinks[clientID] = sink
	if displaced == sink {
		return nil
	}
	return displaced
}

// UnregisterSink removes a client's writer, but only if it is the same
// instance.
//
// The identity check matters during a takeover: the old connection's teardown
// runs concurrently with the new one's registration, and without this check it
// would unregister the new sink and silently stop delivering to a client that
// is perfectly healthy.
func (s *PublishingService) UnregisterSink(
	clientID clientVO.ClientID,
	sink services.PacketSink,
) {
	s.sinksMu.Lock()
	defer s.sinksMu.Unlock()

	if current, ok := s.sinks[clientID]; ok && current == sink {
		delete(s.sinks, clientID)
	}
}

// sinkFor looks up a client's writer.
func (s *PublishingService) sinkFor(clientID clientVO.ClientID) (services.PacketSink, bool) {
	s.sinksMu.RLock()
	defer s.sinksMu.RUnlock()

	sink, ok := s.sinks[clientID]
	return sink, ok
}

// Publish accepts a message and fans it out to every matching subscriber.
func (s *PublishingService) Publish(ctx context.Context, message *msgAgg.Message) error {
	start := s.clock.Now()

	s.published.Add(1)
	s.metrics.RecordMessagePublished(message.QoS(), message.PayloadSize())

	s.applyRetention(message)

	// One index lookup decides the whole fan-out. This is the call the trie
	// exists for.
	subscribers := s.index.Match(message.Topic())
	if len(subscribers) == 0 {
		return nil
	}

	// A live delivery carries RETAIN=0 even when the publication set it: §3.3.1.3
	// reserves RETAIN=1 for messages delivered *because they were stored*, so a
	// subscriber can tell current state from history.
	live := message.AsLiveDelivery()

	for _, subscriber := range subscribers {
		effectiveQoS := message.QoS().Downgrade(subscriber.GrantedQoS)
		s.deliver(ctx, subscriber.ClientID, live, effectiveQoS)
	}

	s.metrics.RecordDeliveryLatency(s.clock.Now().Sub(start))
	return nil
}

// applyRetention stores or clears the topic's retained message (§3.3.1.3).
func (s *PublishingService) applyRetention(message *msgAgg.Message) {
	if !message.IsRetained() {
		return
	}

	// A retained publication with a zero-length payload *clears* the topic's
	// retained message rather than storing an empty one. This is the mechanism
	// clients use to delete retained state, and a broker that stores the empty
	// message instead leaves every future subscriber receiving a blank payload
	// forever.
	if message.ClearsRetained() {
		s.retained.Remove(message.Topic())
		s.metrics.SetRetainedCount(s.retained.Count())
		return
	}

	s.retained.Store(message.Topic(), message)
	s.metrics.SetRetainedCount(s.retained.Count())
}

// deliver sends one message to one subscriber at its effective QoS.
//
// The three outcomes are: send now (client online, window has room), queue
// (client offline, or window full), or drop (no session, or queue overflowed).
// Which one applies is decided inside the session's lock so the window
// accounting cannot race another publisher delivering to the same client.
func (s *PublishingService) deliver(
	ctx context.Context,
	clientID clientVO.ClientID,
	message *msgAgg.Message,
	qos msgVO.QoS,
) {
	sink, online := s.sinkFor(clientID)

	// QoS 0 needs no session bookkeeping at all: there is no acknowledgement to
	// track and, by definition, no promise to keep if the client is offline.
	// Taking the session lock for it would put the cheapest delivery path
	// behind the most contended lock in the broker.
	if qos == msgVO.QoSAtMostOnce {
		if !online {
			return
		}
		s.sendPublish(ctx, sink, message.WithQoS(msgVO.QoSAtMostOnce, 0))
		return
	}

	err := s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
		if !online || !session.HasInflightCapacity() {
			// Offline, or the client is not keeping up: hold the message.
			// §3.1.2.4 requires a persistent session to queue; a clean session
			// queues too, for the window-full case, and loses it on disconnect.
			if dropped := session.Enqueue(message.WithQoS(qos, 0)); dropped {
				s.dropped.Add(1)
				s.metrics.RecordMessageDropped(dropReasonOfflineQueue)
			}
			return nil
		}

		packetID, err := session.NextPacketID()
		if err != nil {
			s.dropped.Add(1)
			s.metrics.RecordMessageDropped(dropReasonNoPacketID)
			return nil
		}

		outbound := message.WithQoS(qos, packetID.Uint16())
		if err := session.TrackInflight(packetID, outbound, s.clock.Now()); err != nil {
			s.dropped.Add(1)
			s.metrics.RecordMessageDropped(dropReasonInflightFull)
			return nil
		}

		// Sent while holding the session lock, on purpose. The sink is a
		// non-blocking bounded queue, so this does not do I/O; and sending
		// inside the lock is what guarantees the in-flight record exists before
		// the PUBACK for it can possibly arrive.
		s.sendPublish(ctx, sink, outbound)
		return nil
	})

	if err != nil {
		// No session means the client vanished between the index lookup and
		// here. Normal under churn, worth counting, not worth an error log.
		s.dropped.Add(1)
		s.metrics.RecordMessageDropped(dropReasonNoSession)
	}
}

// sendPublish hands a PUBLISH to a client's writer.
func (s *PublishingService) sendPublish(
	ctx context.Context,
	sink services.PacketSink,
	message *msgAgg.Message,
) {
	if sink == nil {
		return
	}

	err := sink.Send(&packet.Publish{
		DUP:    message.IsDuplicate(),
		QoS:    message.QoS().Byte(),
		Retain: message.IsRetained(),
		Topic:  message.Topic().String(),
		// PayloadRef rather than Payload: the writer encodes these bytes
		// straight onto the socket and never keeps or mutates them, so the
		// copy Payload would make is pure cost on the hottest path.
		Payload:  message.PayloadRef(),
		PacketID: message.PacketID(),
	})

	if err != nil {
		s.dropped.Add(1)
		s.metrics.RecordMessageDropped(dropReasonQueueFull)
		s.log.Debug(ctx, "dropped a message for a client that is not keeping up",
			logger.F("client_id", sink.ClientID().String()),
			logger.F("topic", message.Topic().String()),
		)
		return
	}

	s.delivered.Add(1)
	s.metrics.RecordMessageDelivered(message.QoS())
}

// Acknowledge handles an inbound PUBACK, clearing the in-flight message and
// pulling the next queued one into the window.
func (s *PublishingService) Acknowledge(
	ctx context.Context,
	clientID clientVO.ClientID,
	rawPacketID uint16,
) error {
	packetID, err := sessionVO.NewPacketID(rawPacketID)
	if err != nil {
		return err
	}

	sink, online := s.sinkFor(clientID)

	return s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
		if _, err := session.Acknowledge(packetID); err != nil {
			// An unsolicited PUBACK is a protocol violation (§4.4). Reporting
			// it rather than ignoring it surfaces a broken client instead of
			// letting it quietly free window slots it does not own.
			return err
		}

		// A freed slot is the moment to promote a queued message: this is what
		// makes the in-flight window act as flow control rather than a cap that
		// silently drops everything past it.
		if online {
			s.promoteQueued(ctx, session, sink)
		}
		return nil
	})
}

// promoteQueued moves queued messages into the in-flight window while it has
// room. The caller holds the session lock.
func (s *PublishingService) promoteQueued(
	ctx context.Context,
	session *sessionAgg.Session,
	sink services.PacketSink,
) {
	for session.HasInflightCapacity() {
		batch := session.DrainQueue(1)
		if len(batch) == 0 {
			return
		}
		queued := batch[0]

		if queued.QoS() == msgVO.QoSAtMostOnce {
			s.sendPublish(ctx, sink, queued)
			continue
		}

		packetID, err := session.NextPacketID()
		if err != nil {
			// Put it back rather than losing it: the identifier space will free
			// up as acknowledgements arrive.
			session.Enqueue(queued)
			return
		}

		outbound := queued.WithQoS(queued.QoS(), packetID.Uint16())
		if err := session.TrackInflight(packetID, outbound, s.clock.Now()); err != nil {
			session.Enqueue(queued)
			return
		}
		s.sendPublish(ctx, sink, outbound)
	}
}

// DeliverRetained sends the retained messages matching a filter (§3.8.4).
//
// A newly subscribed client gets the current state of every matching topic
// immediately, rather than waiting for the next publication. These carry
// RETAIN=1, which is how the client knows they are stored state rather than
// live traffic.
func (s *PublishingService) DeliverRetained(
	ctx context.Context,
	sink services.PacketSink,
	filter msgVO.TopicFilter,
	grantedQoS msgVO.QoS,
) error {
	messages := s.retained.Matching(filter)
	if len(messages) == 0 {
		return nil
	}

	clientID := sink.ClientID()

	for _, message := range messages {
		effectiveQoS := message.QoS().Downgrade(grantedQoS)
		retained := message.AsRetainedDelivery()

		if effectiveQoS == msgVO.QoSAtMostOnce {
			s.sendPublish(ctx, sink, retained.WithQoS(msgVO.QoSAtMostOnce, 0))
			continue
		}

		_ = s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
			packetID, err := session.NextPacketID()
			if err != nil {
				return nil
			}
			outbound := retained.WithQoS(effectiveQoS, packetID.Uint16())
			if err := session.TrackInflight(packetID, outbound, s.clock.Now()); err != nil {
				session.Enqueue(retained.WithQoS(effectiveQoS, 0))
				return nil
			}
			s.sendPublish(ctx, sink, outbound)
			return nil
		})
	}

	s.log.Debug(ctx, "delivered retained messages",
		logger.F("client_id", clientID.String()),
		logger.F("filter", filter.String()),
		logger.F("count", len(messages)),
	)
	return nil
}

// ResumeSession redelivers a reconnecting client's backlog.
//
// §4.4 requires unacknowledged QoS 1 messages to be retransmitted with DUP set
// when the client reconnects. They go first, before the offline queue, so
// ordering is preserved: those messages were sent before the ones that were
// queued while the client was away.
func (s *PublishingService) ResumeSession(ctx context.Context, sink services.PacketSink) error {
	clientID := sink.ClientID()

	return s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
		inflight := session.InflightMessages()
		for _, m := range inflight {
			redelivery := m.Message.AsRedelivery()
			session.MarkRetransmitted(m.PacketID, s.clock.Now())
			s.metrics.RecordRetransmission()
			s.sendPublish(ctx, sink, redelivery)
		}

		drained := 0
		for session.HasInflightCapacity() && drained < s.cfg.ResumeBatchSize {
			before := session.QueuedCount()
			s.promoteQueued(ctx, session, sink)
			if session.QueuedCount() == before {
				break
			}
			drained += before - session.QueuedCount()
		}

		if len(inflight) > 0 || drained > 0 {
			s.log.Info(ctx, "resumed session backlog",
				logger.F("client_id", clientID.String()),
				logger.F("retransmitted", len(inflight)),
				logger.F("queued_delivered", drained),
			)
		}
		return nil
	})
}

// RetransmitExpired redelivers in-flight messages that have gone unacknowledged
// past the timeout.
//
// This is the retransmission loop the broker runs while a client stays
// connected but stops acknowledging — the reconnect path is ResumeSession. Both
// exist because the two failure modes are different: a dropped PUBACK on a live
// connection, and a connection that went away entirely.
func (s *PublishingService) RetransmitExpired(ctx context.Context, timeout time.Duration) int {
	now := s.clock.Now()
	total := 0

	for _, session := range s.sessions.All() {
		if !session.IsConnected() {
			continue
		}
		clientID := session.ClientID()
		sink, online := s.sinkFor(clientID)
		if !online {
			continue
		}

		_ = s.sessions.Update(clientID, func(sess *sessionAgg.Session) error {
			for _, m := range sess.ExpiredInflight(now, timeout) {
				sess.MarkRetransmitted(m.PacketID, now)
				s.metrics.RecordRetransmission()
				s.sendPublish(ctx, sink, m.Message.AsRedelivery())
				total++
			}
			return nil
		})
	}

	if total > 0 {
		s.log.Warn(ctx, "retransmitted unacknowledged messages",
			logger.F("count", total),
			logger.F("timeout", timeout.String()),
		)
	}
	return total
}

// Counters returns the service's totals for the admin API.
func (s *PublishingService) Counters() (published, delivered, dropped uint64) {
	return s.published.Load(), s.delivered.Load(), s.dropped.Load()
}
