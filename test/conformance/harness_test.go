// Package conformance runs the broker end to end over a real TCP socket.
//
// Nothing here is mocked. Each test starts a real broker on an OS-assigned
// port, connects real clients over loopback TCP, and asserts on what actually
// arrives — so a bug anywhere between the packet codec and the subscription
// index is caught here even if every unit test passes.
package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anashasan/gomqtt/pkg/client"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/di"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// broker is a running broker under test.
type broker struct {
	addr     string
	listener *transport.Listener
	stop     func()
}

// startBroker boots a broker on an OS-assigned port.
//
// Port 0 rather than a fixed port so the whole suite can run in parallel and on
// a developer's machine that already has something on 1883 — a test suite that
// needs a specific port free is a test suite people stop running.
func startBroker(t *testing.T, mutate ...func(*config.AppConfig)) *broker {
	t.Helper()

	cfg := config.Default()
	cfg.Broker.Address = "127.0.0.1:0"
	cfg.Admin.Enabled = false
	cfg.Logger.Level = "error"
	// Short so the keep-alive tests do not take a minute each.
	cfg.Broker.ConnectTimeout = 3 * time.Second
	for _, m := range mutate {
		m(cfg)
	}
	require.NoError(t, cfg.Validate())

	deps := di.InjectBroker(cfg, metrics.NewNopRecorder())
	_ = logger.NewNop()

	ctx := context.Background()
	require.NoError(t, deps.Listener.Start(ctx))

	b := &broker{
		addr:     deps.Listener.Addr().String(),
		listener: deps.Listener,
		stop: func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = deps.Listener.Stop(stopCtx)
		},
	}
	t.Cleanup(b.stop)
	return b
}

// collector accumulates messages a client receives.
//
// Guarded by a mutex because OnMessage runs on the client's read goroutine
// while the test asserts from its own — the race detector would catch an
// unguarded slice immediately, which is the point.
type collector struct {
	mu       sync.Mutex
	messages []client.Message
}

// newCollector builds an empty collector.
func newCollector() *collector { return &collector{} }

// handler returns the OnMessage callback.
func (c *collector) handler() func(client.Message) {
	return func(m client.Message) {
		c.mu.Lock()
		defer c.mu.Unlock()
		// The payload aliases the client's read buffer, so it is copied here;
		// without this the assertions would read whatever the next packet
		// overwrote it with.
		c.messages = append(c.messages, client.Message{
			Topic:      m.Topic,
			Payload:    append([]byte(nil), m.Payload...),
			QoS:        m.QoS,
			Retained:   m.Retained,
			DUP:        m.DUP,
			PacketID:   m.PacketID,
			ReceivedAt: m.ReceivedAt,
		})
	}
}

// all returns a snapshot of what has arrived.
func (c *collector) all() []client.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]client.Message(nil), c.messages...)
}

// count returns how many messages have arrived.
func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

// topics returns the topics received, in order.
func (c *collector) topics() []string {
	out := make([]string, 0, c.count())
	for _, m := range c.all() {
		out = append(out, m.Topic)
	}
	return out
}

// payloads returns the payloads received, in order.
func (c *collector) payloads() []string {
	out := make([]string, 0, c.count())
	for _, m := range c.all() {
		out = append(out, string(m.Payload))
	}
	return out
}

// waitFor blocks until n messages have arrived, or fails the test.
//
// Polling rather than a fixed sleep: a sleep long enough to be reliable on a
// loaded CI machine makes the suite slow, and a shorter one makes it flaky.
func (c *collector) waitFor(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return c.count() >= n },
		5*time.Second, 5*time.Millisecond,
		"expected %d messages, got %d", n, c.count())
}

// expectNoMore asserts that no further message arrives within a grace period.
//
// Necessarily a fixed wait: proving a negative has no event to poll for. Kept
// short, and used only where a delivery would be a genuine bug.
func (c *collector) expectNoMore(t *testing.T, within time.Duration, current int) {
	t.Helper()
	time.Sleep(within)
	require.Equal(t, current, c.count(), "an unexpected message arrived")
}

// connect dials the broker and completes the handshake.
func (b *broker) connect(t *testing.T, opts client.Options) *client.Client {
	t.Helper()

	opts.Address = b.addr
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = 5 * time.Second
	}
	if opts.AckTimeout == 0 {
		opts.AckTimeout = 5 * time.Second
	}

	c := client.New(opts)
	require.NoError(t, c.Connect())
	t.Cleanup(c.Close)
	return c
}

// subscriber connects a clean-session client subscribed to one filter.
func (b *broker) subscriber(t *testing.T, id, filter string, qos byte) (*client.Client, *collector) {
	t.Helper()

	col := newCollector()
	c := b.connect(t, client.Options{
		ClientID:     id,
		CleanSession: true,
		OnMessage:    col.handler(),
	})

	granted, err := c.Subscribe(filter, qos)
	require.NoError(t, err)
	require.LessOrEqual(t, granted, qos, "granted QoS must not exceed what was requested")

	return c, col
}

// publisher connects a clean-session client with no subscriptions.
func (b *broker) publisher(t *testing.T, id string) *client.Client {
	t.Helper()
	return b.connect(t, client.Options{ClientID: id, CleanSession: true})
}

// uniqueID builds a client identifier unique to a test.
func uniqueID(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%1000000)
}
