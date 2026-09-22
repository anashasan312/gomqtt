// Package prometheus implements the metrics port on the Prometheus client.
//
// Every label here has bounded cardinality by construction: packet types come
// from a closed enum, QoS from 0..2, and drop and disconnect reasons from the
// constants in the packages that emit them. No label ever carries a client
// identifier or a topic — a broker with 50,000 clients and a topic per device
// would otherwise produce a metric cardinality that takes the Prometheus server
// down long before it tells anyone anything useful.
package prometheus

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
)

var _ metrics.Recorder = (*Recorder)(nil)

const namespace = "gomqtt"

// Label names.
const (
	labelPacketType = "packet_type"
	labelQoS        = "qos"
	labelReason     = "reason"
)

// Recorder implements the metrics port.
type Recorder struct {
	packetsReceived *prometheus.CounterVec
	packetsSent     *prometheus.CounterVec
	bytesReceived   prometheus.Counter
	bytesSent       prometheus.Counter

	messagesPublished *prometheus.CounterVec
	messagesDelivered *prometheus.CounterVec
	messagesDropped   *prometheus.CounterVec
	payloadBytes      prometheus.Histogram
	deliveryLatency   prometheus.Histogram
	retransmissions   prometheus.Counter

	connections         prometheus.Counter
	disconnections      *prometheus.CounterVec
	connectionsRejected *prometheus.CounterVec

	connectedClients prometheus.Gauge
	sessions         prometheus.Gauge
	subscriptions    prometheus.Gauge
	retainedMessages prometheus.Gauge
	inflightMessages prometheus.Gauge
}

// NewRecorder builds a Recorder and registers its collectors.
//
// The registry is injected rather than taken from the package global so a test
// can build a Recorder against a throwaway registry, and so two Recorders in
// one process cannot collide on a duplicate registration.
func NewRecorder(registry prometheus.Registerer) *Recorder {
	r := &Recorder{
		packetsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "packets_received_total",
			Help:      "Control packets received, by type.",
		}, []string{labelPacketType}),

		packetsSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "packets_sent_total",
			Help:      "Control packets sent, by type.",
		}, []string{labelPacketType}),

		bytesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_received_total",
			Help:      "Total bytes read from client connections.",
		}),

		bytesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_sent_total",
			Help:      "Total bytes written to client connections.",
		}),

		messagesPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_total",
			Help:      "Messages accepted from publishers, by QoS.",
		}, []string{labelQoS}),

		messagesDelivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_delivered_total",
			Help:      "Message deliveries to subscribers, by QoS. One publish to fifty subscribers counts fifty.",
		}, []string{labelQoS}),

		messagesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_dropped_total",
			Help:      "Messages that could not be delivered, by reason.",
		}, []string{labelReason}),

		payloadBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "message_payload_bytes",
			Help:      "Published payload size in bytes.",
			// Powers of four from 16 bytes to 1 MB: MQTT payloads are usually
			// tiny telemetry values, so the buckets are dense at the small end
			// where the distribution actually lives.
			Buckets: prometheus.ExponentialBuckets(16, 4, 9),
		}),

		deliveryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "delivery_latency_seconds",
			Help:      "Time from accepting a publish to handing it to every matching subscriber's writer.",
			// From 10µs: in-process fan-out is microseconds, and the default
			// client buckets start at 5ms, which would put every healthy
			// measurement in the first bucket and show nothing.
			Buckets: []float64{
				0.00001, 0.00005, 0.0001, 0.0005,
				0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1,
			},
		}),

		retransmissions: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retransmissions_total",
			Help:      "QoS 1 messages redelivered because they were not acknowledged.",
		}),

		connections: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_total",
			Help:      "Accepted CONNECT packets.",
		}),

		disconnections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "disconnections_total",
			Help:      "Closed connections, by reason.",
		}, []string{labelReason}),

		connectionsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_rejected_total",
			Help:      "Refused CONNECT packets, by reason.",
		}, []string{labelReason}),

		connectedClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connected_clients",
			Help:      "Clients currently connected.",
		}),

		sessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "sessions",
			Help:      "Sessions currently stored, including those of offline clients.",
		}),

		subscriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "subscriptions",
			Help:      "Active client/filter subscription pairs.",
		}),

		retainedMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "retained_messages",
			Help:      "Topics holding a retained message.",
		}),

		inflightMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "inflight_messages",
			Help:      "Unacknowledged QoS 1 messages across all sessions.",
		}),
	}

	registry.MustRegister(
		r.packetsReceived, r.packetsSent, r.bytesReceived, r.bytesSent,
		r.messagesPublished, r.messagesDelivered, r.messagesDropped,
		r.payloadBytes, r.deliveryLatency, r.retransmissions,
		r.connections, r.disconnections, r.connectionsRejected,
		r.connectedClients, r.sessions, r.subscriptions,
		r.retainedMessages, r.inflightMessages,
	)
	return r
}

// RecordPacketReceived counts an inbound packet.
func (r *Recorder) RecordPacketReceived(packetType string, bytes int) {
	r.packetsReceived.WithLabelValues(packetType).Inc()
	if bytes > 0 {
		r.bytesReceived.Add(float64(bytes))
	}
}

// RecordPacketSent counts an outbound packet.
func (r *Recorder) RecordPacketSent(packetType string, bytes int) {
	r.packetsSent.WithLabelValues(packetType).Inc()
	if bytes > 0 {
		r.bytesSent.Add(float64(bytes))
	}
}

// RecordMessagePublished counts an accepted publication.
func (r *Recorder) RecordMessagePublished(qos msgVO.QoS, payloadBytes int) {
	r.messagesPublished.WithLabelValues(qos.String()).Inc()
	r.payloadBytes.Observe(float64(payloadBytes))
}

// RecordMessageDelivered counts one delivery to one subscriber.
func (r *Recorder) RecordMessageDelivered(qos msgVO.QoS) {
	r.messagesDelivered.WithLabelValues(qos.String()).Inc()
}

// RecordMessageDropped counts an undeliverable message.
func (r *Recorder) RecordMessageDropped(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	r.messagesDropped.WithLabelValues(reason).Inc()
}

// RecordDeliveryLatency observes fan-out latency.
func (r *Recorder) RecordDeliveryLatency(d time.Duration) {
	r.deliveryLatency.Observe(d.Seconds())
}

// RecordRetransmission counts a redelivered message.
func (r *Recorder) RecordRetransmission() { r.retransmissions.Inc() }

// RecordConnection counts an accepted CONNECT.
func (r *Recorder) RecordConnection() { r.connections.Inc() }

// RecordDisconnection counts a closed connection.
func (r *Recorder) RecordDisconnection(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	r.disconnections.WithLabelValues(reason).Inc()
}

// RecordConnectionRejected counts a refused CONNECT.
func (r *Recorder) RecordConnectionRejected(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	r.connectionsRejected.WithLabelValues(reason).Inc()
}

// SetConnectedClients publishes the connected client count.
func (r *Recorder) SetConnectedClients(count int) { r.connectedClients.Set(float64(count)) }

// SetSessionCount publishes the stored session count.
func (r *Recorder) SetSessionCount(count int) { r.sessions.Set(float64(count)) }

// SetSubscriptionCount publishes the subscription count.
func (r *Recorder) SetSubscriptionCount(count int) { r.subscriptions.Set(float64(count)) }

// SetRetainedCount publishes the retained topic count.
func (r *Recorder) SetRetainedCount(count int) { r.retainedMessages.Set(float64(count)) }

// SetInflightCount publishes the in-flight message count.
func (r *Recorder) SetInflightCount(count int) { r.inflightMessages.Set(float64(count)) }

// NewRegistry builds a dedicated registry with the Go runtime and process
// collectors attached.
//
// A dedicated registry rather than the package global keeps this process's
// metrics self-contained: nothing a library imports can quietly add a
// collector, and tests can build one per case without cleanup.
func NewRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return registry
}

// NewHandler builds the HTTP handler that serves the metrics endpoint.
func NewHandler(registry *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		// A scrape must never be able to take the broker down, so a collector
		// error is reported in the response body rather than by panicking.
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      registry,
	})
}
