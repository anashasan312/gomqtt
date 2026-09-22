// Package metrics defines the observability port.
//
// The port lives in the domain so the services and the transport can emit
// measurements without importing Prometheus. Swapping in OpenTelemetry later is
// a di change rather than a rewrite.
package metrics

import (
	"time"

	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
)

// Recorder is the measurement port.
//
// Implementations must be safe for concurrent use by every connection goroutine
// and must never block or return an error: an observability failure may not
// take down message delivery.
type Recorder interface {
	// RecordPacketReceived counts an inbound control packet by type.
	RecordPacketReceived(packetType string, bytes int)

	// RecordPacketSent counts an outbound control packet by type.
	RecordPacketSent(packetType string, bytes int)

	// RecordMessagePublished counts a message accepted from a publisher.
	RecordMessagePublished(qos msgVO.QoS, payloadBytes int)

	// RecordMessageDelivered counts one delivery to one subscriber. A publish
	// that fans out to fifty subscribers records one publish and fifty
	// deliveries, which is what makes the fan-out ratio visible.
	RecordMessageDelivered(qos msgVO.QoS)

	// RecordMessageDropped counts a message that could not be delivered,
	// labelled by reason so a full queue is distinguishable from a dead socket.
	RecordMessageDropped(reason string)

	// RecordDeliveryLatency observes the time from accepting a publish to
	// handing it to a subscriber's writer.
	RecordDeliveryLatency(d time.Duration)

	// RecordRetransmission counts a redelivered QoS 1 message.
	RecordRetransmission()

	// RecordConnection counts an accepted CONNECT.
	RecordConnection()

	// RecordDisconnection counts a closed connection, labelled by reason.
	RecordDisconnection(reason string)

	// RecordConnectionRejected counts a refused CONNECT.
	RecordConnectionRejected(reason string)

	// SetConnectedClients publishes the current connected client count.
	SetConnectedClients(count int)

	// SetSessionCount publishes the number of stored sessions.
	SetSessionCount(count int)

	// SetSubscriptionCount publishes the number of active subscriptions.
	SetSubscriptionCount(count int)

	// SetRetainedCount publishes the number of retained topics.
	SetRetainedCount(count int)

	// SetInflightCount publishes the total in-flight QoS 1 messages.
	SetInflightCount(count int)
}

// NopRecorder discards every measurement. It is the default binding for tests
// and for a process started with metrics disabled.
type NopRecorder struct{}

// NewNopRecorder builds a discarding Recorder.
func NewNopRecorder() *NopRecorder { return &NopRecorder{} }

// RecordPacketReceived discards the measurement.
func (NopRecorder) RecordPacketReceived(string, int) {}

// RecordPacketSent discards the measurement.
func (NopRecorder) RecordPacketSent(string, int) {}

// RecordMessagePublished discards the measurement.
func (NopRecorder) RecordMessagePublished(msgVO.QoS, int) {}

// RecordMessageDelivered discards the measurement.
func (NopRecorder) RecordMessageDelivered(msgVO.QoS) {}

// RecordMessageDropped discards the measurement.
func (NopRecorder) RecordMessageDropped(string) {}

// RecordDeliveryLatency discards the measurement.
func (NopRecorder) RecordDeliveryLatency(time.Duration) {}

// RecordRetransmission discards the measurement.
func (NopRecorder) RecordRetransmission() {}

// RecordConnection discards the measurement.
func (NopRecorder) RecordConnection() {}

// RecordDisconnection discards the measurement.
func (NopRecorder) RecordDisconnection(string) {}

// RecordConnectionRejected discards the measurement.
func (NopRecorder) RecordConnectionRejected(string) {}

// SetConnectedClients discards the measurement.
func (NopRecorder) SetConnectedClients(int) {}

// SetSessionCount discards the measurement.
func (NopRecorder) SetSessionCount(int) {}

// SetSubscriptionCount discards the measurement.
func (NopRecorder) SetSubscriptionCount(int) {}

// SetRetainedCount discards the measurement.
func (NopRecorder) SetRetainedCount(int) {}

// SetInflightCount discards the measurement.
func (NopRecorder) SetInflightCount(int) {}
