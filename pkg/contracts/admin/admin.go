// Package admin holds the response DTOs for the broker's inspection API.
//
// Contracts are separate from aggregates on purpose. An aggregate is shaped by
// the invariants it protects; a contract is shaped by what a client is allowed
// to see. Letting one type do both makes every domain refactor a breaking API
// change.
package admin

// StatsRes is the broker's counter snapshot.
type StatsRes struct {
	ConnectedClients  int    `json:"connected_clients"`
	Sessions          int    `json:"sessions"`
	Subscriptions     int    `json:"subscriptions"`
	DistinctFilters   int    `json:"distinct_filters"`
	RetainedMessages  int    `json:"retained_messages"`
	InflightMessages  int    `json:"inflight_messages"`
	QueuedMessages    int    `json:"queued_messages"`
	UptimeSeconds     int64  `json:"uptime_seconds"`
	MessagesPublished uint64 `json:"messages_published"`
	MessagesDelivered uint64 `json:"messages_delivered"`
	MessagesDropped   uint64 `json:"messages_dropped"`
}

// ClientRes describes one connected client.
type ClientRes struct {
	ClientID       string `json:"client_id"`
	RemoteAddr     string `json:"remote_addr"`
	Username       string `json:"username,omitempty"`
	CleanSession   bool   `json:"clean_session"`
	KeepAlive      uint16 `json:"keep_alive"`
	ConnectedAt    string `json:"connected_at"`
	LastActivityAt string `json:"last_activity_at"`
	Subscriptions  int    `json:"subscriptions"`
	Inflight       int    `json:"inflight"`
	Queued         int    `json:"queued"`
}

// ListClientsRes is the client listing.
type ListClientsRes struct {
	Clients []ClientRes `json:"clients"`
	Total   int         `json:"total"`
}

// SubscriptionRes describes one subscription.
type SubscriptionRes struct {
	ClientID     string `json:"client_id"`
	Filter       string `json:"filter"`
	RequestedQoS byte   `json:"requested_qos"`
	GrantedQoS   byte   `json:"granted_qos"`
	// Downgraded flags a client that believes it has a stronger guarantee than
	// the broker granted, which is worth an operator seeing.
	Downgraded bool   `json:"downgraded"`
	CreatedAt  string `json:"created_at"`
}

// ListSubscriptionsRes is the subscription listing.
type ListSubscriptionsRes struct {
	Subscriptions []SubscriptionRes `json:"subscriptions"`
	Total         int               `json:"total"`
}

// RetainedRes describes one retained message.
//
// The payload size rather than the payload: an inspection endpoint should not
// double as a data export for everything the broker has ever stored.
type RetainedRes struct {
	Topic       string `json:"topic"`
	QoS         byte   `json:"qos"`
	PayloadSize int    `json:"payload_size"`
	CreatedAt   string `json:"created_at"`
}

// ListRetainedRes is the retained-message listing.
type ListRetainedRes struct {
	Retained []RetainedRes `json:"retained"`
	Total    int           `json:"total"`
}

// HealthRes is the liveness payload.
type HealthRes struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Version string `json:"version"`
	Uptime  string `json:"uptime"`
}
