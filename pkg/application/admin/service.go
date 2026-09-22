// Package admin implements the read-only inspection API.
package admin

import (
	"context"
	"sort"
	"time"

	"github.com/anashasan/gomqtt/pkg/application/services"
	"github.com/anashasan/gomqtt/pkg/common/clock"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
)

var _ services.IAdminService = (*AdminService)(nil)

// CounterSource exposes the publishing service's totals.
//
// A narrow interface rather than a dependency on the whole publishing service:
// the admin API reads three numbers, and giving it the ability to publish
// messages as a side effect of wanting to count them would be exactly the kind
// of over-broad dependency interface segregation exists to prevent.
type CounterSource interface {
	Counters() (published, delivered, dropped uint64)
}

// AdminService serves broker introspection.
type AdminService struct {
	sessions persistence.ISessionStore
	clients  persistence.IClientRegistry
	index    persistence.ISubscriptionIndex
	retained persistence.IRetainedStore
	counters CounterSource
	clock    clock.Clock

	startedAt time.Time
}

// NewAdminService builds an AdminService.
func NewAdminService(
	sessions persistence.ISessionStore,
	clients persistence.IClientRegistry,
	index persistence.ISubscriptionIndex,
	retained persistence.IRetainedStore,
	counters CounterSource,
	clk clock.Clock,
) *AdminService {
	return &AdminService{
		sessions:  sessions,
		clients:   clients,
		index:     index,
		retained:  retained,
		counters:  counters,
		clock:     clk,
		startedAt: clk.Now(),
	}
}

// Stats returns a snapshot of broker counters.
//
// Each number is read independently, so the snapshot is not a consistent
// instant across all of them. That is the right trade for an inspection
// endpoint: locking the whole broker to produce a perfectly coherent count
// would mean the admin API could stall message delivery.
func (s *AdminService) Stats(_ context.Context) (services.BrokerStats, error) {
	published, delivered, dropped := s.counters.Counters()

	stats := services.BrokerStats{
		ConnectedClients:  s.clients.Count(),
		Sessions:          s.sessions.Count(),
		Subscriptions:     s.index.SubscriptionCount(),
		DistinctFilters:   s.index.FilterCount(),
		RetainedMessages:  s.retained.Count(),
		UptimeSeconds:     int64(s.clock.Now().Sub(s.startedAt).Seconds()),
		MessagesPublished: published,
		MessagesDelivered: delivered,
		MessagesDropped:   dropped,
	}

	for _, session := range s.sessions.All() {
		stats.InflightMessages += session.InflightCount()
		stats.QueuedMessages += session.QueuedCount()
	}
	return stats, nil
}

// Clients returns the connected clients.
func (s *AdminService) Clients(_ context.Context) ([]services.ClientInfo, error) {
	connected := s.clients.All()
	out := make([]services.ClientInfo, 0, len(connected))

	for _, client := range connected {
		info := services.ClientInfo{
			ClientID:       client.ID().String(),
			RemoteAddr:     client.RemoteAddr(),
			Username:       client.Username(),
			CleanSession:   client.CleanSession(),
			KeepAlive:      client.KeepAlive().Seconds(),
			ConnectedAt:    formatTime(client.ConnectedAt()),
			LastActivityAt: formatTime(client.LastActivityAt()),
		}

		if session, ok := s.sessions.Get(client.ID()); ok {
			info.Subscriptions = session.SubscriptionCount()
			info.Inflight = session.InflightCount()
			info.Queued = session.QueuedCount()
		}
		out = append(out, info)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out, nil
}

// Subscriptions returns every active subscription.
func (s *AdminService) Subscriptions(_ context.Context) ([]services.SubscriptionInfo, error) {
	var out []services.SubscriptionInfo

	for _, session := range s.sessions.All() {
		for _, sub := range session.Subscriptions() {
			out = append(out, services.SubscriptionInfo{
				ClientID:     sub.ClientID().String(),
				Filter:       sub.Filter().String(),
				RequestedQoS: sub.RequestedQoS().Byte(),
				GrantedQoS:   sub.GrantedQoS().Byte(),
				// Surfaced so an operator can see a client believing it has a
				// guarantee the broker did not grant.
				Downgraded: sub.WasDowngraded(),
				CreatedAt:  formatTime(sub.CreatedAt()),
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ClientID != out[j].ClientID {
			return out[i].ClientID < out[j].ClientID
		}
		return out[i].Filter < out[j].Filter
	})
	return out, nil
}

// RetainedTopics returns every topic holding a retained message.
func (s *AdminService) RetainedTopics(_ context.Context) ([]services.RetainedInfo, error) {
	messages := s.retained.All()
	out := make([]services.RetainedInfo, 0, len(messages))

	for _, message := range messages {
		out = append(out, services.RetainedInfo{
			Topic: message.Topic().String(),
			QoS:   message.QoS().Byte(),
			// The size rather than the payload: a retained listing is an
			// operator view, and dumping every stored payload through it would
			// turn an inspection endpoint into an accidental data export.
			PayloadSize: message.PayloadSize(),
			CreatedAt:   formatTime(message.CreatedAt()),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out, nil
}

// formatTime renders an instant as RFC3339, mapping the zero time to empty so a
// client can tell "not set" from "the epoch".
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
