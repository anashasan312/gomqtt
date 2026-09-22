// Package hydrator maps application-layer results onto API contracts.
//
// The mapping lives in its own package so neither side has to know about the
// other: the services stay free of json tags, and the contracts stay free of
// domain types.
package hydrator

import (
	"github.com/anashasan/gomqtt/pkg/application/services"
	adminContr "github.com/anashasan/gomqtt/pkg/contracts/admin"
)

// ToStatsRes maps the broker stats snapshot.
func ToStatsRes(stats services.BrokerStats) adminContr.StatsRes {
	return adminContr.StatsRes{
		ConnectedClients:  stats.ConnectedClients,
		Sessions:          stats.Sessions,
		Subscriptions:     stats.Subscriptions,
		DistinctFilters:   stats.DistinctFilters,
		RetainedMessages:  stats.RetainedMessages,
		InflightMessages:  stats.InflightMessages,
		QueuedMessages:    stats.QueuedMessages,
		UptimeSeconds:     stats.UptimeSeconds,
		MessagesPublished: stats.MessagesPublished,
		MessagesDelivered: stats.MessagesDelivered,
		MessagesDropped:   stats.MessagesDropped,
	}
}

// ToListClientsRes maps the client listing.
func ToListClientsRes(clients []services.ClientInfo) adminContr.ListClientsRes {
	out := make([]adminContr.ClientRes, 0, len(clients))
	for _, c := range clients {
		out = append(out, adminContr.ClientRes{
			ClientID:       c.ClientID,
			RemoteAddr:     c.RemoteAddr,
			Username:       c.Username,
			CleanSession:   c.CleanSession,
			KeepAlive:      c.KeepAlive,
			ConnectedAt:    c.ConnectedAt,
			LastActivityAt: c.LastActivityAt,
			Subscriptions:  c.Subscriptions,
			Inflight:       c.Inflight,
			Queued:         c.Queued,
		})
	}
	return adminContr.ListClientsRes{Clients: out, Total: len(out)}
}

// ToListSubscriptionsRes maps the subscription listing.
func ToListSubscriptionsRes(subs []services.SubscriptionInfo) adminContr.ListSubscriptionsRes {
	out := make([]adminContr.SubscriptionRes, 0, len(subs))
	for _, s := range subs {
		out = append(out, adminContr.SubscriptionRes{
			ClientID:     s.ClientID,
			Filter:       s.Filter,
			RequestedQoS: s.RequestedQoS,
			GrantedQoS:   s.GrantedQoS,
			Downgraded:   s.Downgraded,
			CreatedAt:    s.CreatedAt,
		})
	}
	return adminContr.ListSubscriptionsRes{Subscriptions: out, Total: len(out)}
}

// ToListRetainedRes maps the retained-message listing.
func ToListRetainedRes(retained []services.RetainedInfo) adminContr.ListRetainedRes {
	out := make([]adminContr.RetainedRes, 0, len(retained))
	for _, r := range retained {
		out = append(out, adminContr.RetainedRes{
			Topic:       r.Topic,
			QoS:         r.QoS,
			PayloadSize: r.PayloadSize,
			CreatedAt:   r.CreatedAt,
		})
	}
	return adminContr.ListRetainedRes{Retained: out, Total: len(out)}
}
