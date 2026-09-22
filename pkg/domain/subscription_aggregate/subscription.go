// Package subscription_aggregate contains the Subscription entity: one client's
// interest in a topic filter, at a requested quality of service.
package subscription_aggregate

import (
	"time"

	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
)

// Subscription binds a client to a topic filter.
//
// It is an entity within the Session aggregate rather than an aggregate root of
// its own: a subscription has no life outside the session that holds it, and
// deleting a session deletes its subscriptions atomically.
type Subscription struct {
	clientID     clientVO.ClientID
	filter       msgVO.TopicFilter
	requestedQoS msgVO.QoS
	grantedQoS   msgVO.QoS
	createdAt    time.Time
}

// NewSubscription builds a Subscription, granting the highest QoS the broker
// can honour.
//
// §3.8.4 permits a server to grant a lower QoS than requested, and the SUBACK
// reports what was actually granted. That is how this broker answers a QoS 2
// subscription honestly — granting 1 — rather than either failing the
// subscription or claiming a guarantee it does not implement.
func NewSubscription(
	clientID clientVO.ClientID,
	filter msgVO.TopicFilter,
	requestedQoS msgVO.QoS,
	now time.Time,
) *Subscription {
	granted := requestedQoS
	if granted > msgVO.MaxSupportedQoS {
		granted = msgVO.MaxSupportedQoS
	}

	return &Subscription{
		clientID:     clientID,
		filter:       filter,
		requestedQoS: requestedQoS,
		grantedQoS:   granted,
		createdAt:    now.UTC(),
	}
}

// ClientID returns the subscribing client.
func (s *Subscription) ClientID() clientVO.ClientID { return s.clientID }

// Filter returns the topic filter.
func (s *Subscription) Filter() msgVO.TopicFilter { return s.filter }

// RequestedQoS returns the level the client asked for.
func (s *Subscription) RequestedQoS() msgVO.QoS { return s.requestedQoS }

// GrantedQoS returns the level the broker granted, which is what the SUBACK
// reports and what every delivery on this subscription is capped at.
func (s *Subscription) GrantedQoS() msgVO.QoS { return s.grantedQoS }

// CreatedAt returns when the subscription was made.
func (s *Subscription) CreatedAt() time.Time { return s.createdAt }

// Matches reports whether the subscription selects a topic.
func (s *Subscription) Matches(topic msgVO.TopicName) bool {
	return s.filter.Matches(topic)
}

// EffectiveQoS returns the level a message published at publishQoS is delivered
// at on this subscription: the minimum of the two (§4.3).
func (s *Subscription) EffectiveQoS(publishQoS msgVO.QoS) msgVO.QoS {
	return publishQoS.Downgrade(s.grantedQoS)
}

// WasDowngraded reports whether the granted QoS is below the requested one,
// which the admin API surfaces so an operator can see that a client believes it
// has a stronger guarantee than it does.
func (s *Subscription) WasDowngraded() bool {
	return s.grantedQoS < s.requestedQoS
}
