// Package subscribing implements SUBSCRIBE and UNSUBSCRIBE.
package subscribing

import (
	"context"

	"github.com/anashasan/gomqtt/pkg/application/services"
	"github.com/anashasan/gomqtt/pkg/common/clock"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

var _ services.ISubscriptionService = (*SubscriptionService)(nil)

// SubscriptionService registers and removes subscriptions.
//
// A subscription is recorded in two places, and both are necessary: the session
// owns it so it survives a disconnect and can be listed per client, and the
// index holds it so the delivery path can answer "who wants this topic" in one
// lookup. Writing to one and not the other is the bug this service exists to
// make impossible.
type SubscriptionService struct {
	sessions   persistence.ISessionStore
	index      persistence.ISubscriptionIndex
	publishing services.IPublishingService
	clock      clock.Clock
	metrics    metrics.Recorder
	log        logger.Logger
}

// NewSubscriptionService builds a SubscriptionService.
func NewSubscriptionService(
	sessions persistence.ISessionStore,
	index persistence.ISubscriptionIndex,
	publishing services.IPublishingService,
	clk clock.Clock,
	recorder metrics.Recorder,
	log logger.Logger,
) *SubscriptionService {
	return &SubscriptionService{
		sessions:   sessions,
		index:      index,
		publishing: publishing,
		clock:      clk,
		metrics:    recorder,
		log:        log,
	}
}

// Subscribe registers filters and returns the SUBACK return codes.
//
// One code per requested filter, in the same order (§3.9.3). A filter that
// fails validation yields 0x80 in its slot and the rest still succeed —
// refusing the whole SUBSCRIBE because one filter was malformed would be a
// worse experience and is not what the spec asks for.
func (s *SubscriptionService) Subscribe(
	ctx context.Context,
	client *clientAgg.Client,
	req *packet.Subscribe,
	sink services.PacketSink,
) ([]byte, error) {
	clientID := client.ID()
	now := s.clock.Now()

	returnCodes := make([]byte, len(req.Subscriptions))

	// accepted records the filters that were registered, so retained messages
	// can be delivered after the session lock is released. Delivering inside
	// the lock would hold it across a fan-out to every matching retained topic.
	type accepted struct {
		filter     msgVO.TopicFilter
		grantedQoS msgVO.QoS
	}
	var registered []accepted

	err := s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
		for i, requested := range req.Subscriptions {
			filter, err := msgVO.NewTopicFilter(requested.Filter)
			if err != nil {
				returnCodes[i] = packet.SubackFailure
				s.log.Warn(ctx, "rejected an invalid topic filter",
					logger.F("client_id", clientID.String()),
					logger.F("filter", requested.Filter),
				)
				continue
			}

			qos, err := msgVO.NewQoS(requested.QoS)
			if err != nil {
				returnCodes[i] = packet.SubackFailure
				continue
			}

			granted, err := session.Subscribe(filter, qos, now)
			if err != nil {
				returnCodes[i] = packet.SubackFailure
				s.log.Warn(ctx, "rejected a subscription",
					logger.F("client_id", clientID.String()),
					logger.F("filter", filter.String()),
					logger.F("error", err.Error()),
				)
				continue
			}

			s.index.Subscribe(filter, clientID, granted)
			returnCodes[i] = granted.Byte()
			registered = append(registered, accepted{filter: filter, grantedQoS: granted})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// §3.8.4: retained messages matching a new subscription are delivered
	// immediately, after the SUBACK is on its way.
	for _, entry := range registered {
		if err := s.publishing.DeliverRetained(ctx, sink, entry.filter, entry.grantedQoS); err != nil {
			s.log.Error(ctx, "failed to deliver retained messages", err,
				logger.F("client_id", clientID.String()),
				logger.F("filter", entry.filter.String()),
			)
		}
	}

	s.metrics.SetSubscriptionCount(s.index.SubscriptionCount())

	s.log.Info(ctx, "client subscribed",
		logger.F("client_id", clientID.String()),
		logger.F("filters", len(req.Subscriptions)),
		logger.F("accepted", len(registered)),
	)
	return returnCodes, nil
}

// Unsubscribe removes filters.
//
// §3.10.4 requires an UNSUBACK whether or not the filters were subscribed, so
// an unknown filter is not an error here — the caller acknowledges regardless.
func (s *SubscriptionService) Unsubscribe(
	ctx context.Context,
	client *clientAgg.Client,
	req *packet.Unsubscribe,
) error {
	clientID := client.ID()
	removed := 0

	err := s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
		for _, raw := range req.Filters {
			filter, err := msgVO.NewTopicFilter(raw)
			if err != nil {
				continue
			}
			if session.Unsubscribe(filter) {
				removed++
			}
			// Removed from the index unconditionally: if the two ever
			// disagreed, the index is the one that decides delivery, so it is
			// the one that must not keep a stale entry.
			s.index.Unsubscribe(filter, clientID)
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.metrics.SetSubscriptionCount(s.index.SubscriptionCount())

	s.log.Info(ctx, "client unsubscribed",
		logger.F("client_id", clientID.String()),
		logger.F("requested", len(req.Filters)),
		logger.F("removed", removed),
	)
	return nil
}
