//go:build interop

// Package interop checks GoMQTT against Eclipse Mosquitto.
//
// This is the suite that makes the wire format trustworthy. Everything else in
// the project tests GoMQTT against itself — the same codec encodes and decodes,
// so a misreading of the specification would be applied consistently in both
// directions and every test would still pass.
//
// These tests break that circularity in both directions:
//
//	mosquitto_pub / mosquitto_sub  ->  GoMQTT   (an independent client talks to our broker)
//	our client                     ->  mosquitto (our client talks to an independent broker)
//
// Passing both means the bytes on the wire really are MQTT 3.1.1, not a private
// dialect that happens to be self-consistent.
//
// Build-tagged because it needs the mosquitto binaries installed. Run with
// `make test-interop`.
package interop

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/gomqtt/pkg/client"
	"github.com/anashasan/gomqtt/pkg/di"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// requireBinary skips the test when a mosquitto binary is missing, rather than
// failing: a developer without mosquitto installed should still be able to run
// `make test`, and this suite is explicitly opt-in.
func requireBinary(t *testing.T, name string) string {
	t.Helper()

	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s is not installed; skipping the interop suite", name)
	}
	return path
}

// freePort asks the OS for an unused port, so the suite can run in parallel and
// on a machine that already has something on 1883.
func freePort(t *testing.T) int {
	t.Helper()

	listener, err := netListen()
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	_, portStr, err := splitHostPort(listener.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return port
}

// startGoMQTT boots our broker on an OS-assigned port.
func startGoMQTT(t *testing.T) (addr string, listener *transport.Listener) {
	t.Helper()

	cfg := config.Default()
	cfg.Broker.Address = "127.0.0.1:0"
	cfg.Admin.Enabled = false
	cfg.Logger.Level = "error"

	deps := di.InjectBroker(cfg, metrics.NewNopRecorder())
	require.NoError(t, deps.Listener.Start(context.Background()))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = deps.Listener.Stop(ctx)
	})

	return deps.Listener.Addr().String(), deps.Listener
}

// startMosquitto boots a real Mosquitto on a free port.
func startMosquitto(t *testing.T) string {
	t.Helper()
	binary := requireBinary(t, "mosquitto")

	port := freePort(t)

	// Mosquitto 2.x refuses remote connections without an explicit listener and
	// allow_anonymous, so the config is written rather than relying on defaults.
	confPath := t.TempDir() + "/mosquitto.conf"
	conf := fmt.Sprintf("listener %d 127.0.0.1\nallow_anonymous true\npersistence false\n", port)
	require.NoError(t, os.WriteFile(confPath, []byte(conf), 0o600))

	cmd := exec.Command(binary, "-c", confPath)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitForPort(t, addr)
	return addr
}

// collector accumulates messages a client receives.
type collector struct {
	mu       sync.Mutex
	messages []client.Message
}

func newCollector() *collector { return &collector{} }

func (c *collector) handler() func(client.Message) {
	return func(m client.Message) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.messages = append(c.messages, client.Message{
			Topic:    m.Topic,
			Payload:  append([]byte(nil), m.Payload...),
			QoS:      m.QoS,
			Retained: m.Retained,
		})
	}
}

func (c *collector) all() []client.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]client.Message(nil), c.messages...)
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

func (c *collector) waitFor(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return c.count() >= n },
		10*time.Second, 10*time.Millisecond,
		"expected %d messages, got %d", n, c.count())
}

// ---------------------------------------------------------------------------
// Direction 1: Mosquitto's clients against our broker
// ---------------------------------------------------------------------------

func TestMosquittoPub_AgainstGoMQTT(t *testing.T) {
	addr := mustSplitPort(t, mustAddr(startGoMQTT(t)))
	pubBinary := requireBinary(t, "mosquitto_pub")

	received := newCollector()
	sub := client.New(client.Options{
		Address: "127.0.0.1:" + addr, ClientID: "our-sub",
		CleanSession: true, OnMessage: received.handler(),
	})
	require.NoError(t, sub.Connect())
	t.Cleanup(sub.Close)

	_, err := sub.Subscribe("interop/#", 1)
	require.NoError(t, err)

	for _, qos := range []string{"0", "1"} {
		// A real, independent MQTT client publishing into our broker. If our
		// CONNECT or PUBLISH decoding disagreed with the standard in any way
		// that mattered, mosquitto_pub would fail here.
		out, err := exec.Command(pubBinary,
			"-h", "127.0.0.1", "-p", addr,
			"-t", "interop/qos"+qos,
			"-m", "from-mosquitto-qos"+qos,
			"-q", qos,
		).CombinedOutput()
		require.NoError(t, err, "mosquitto_pub -q %s failed: %s", qos, out)
	}

	received.waitFor(t, 2)

	payloads := make([]string, 0, 2)
	for _, m := range received.all() {
		payloads = append(payloads, string(m.Payload))
	}
	assert.ElementsMatch(t,
		[]string{"from-mosquitto-qos0", "from-mosquitto-qos1"},
		payloads)
}

func TestMosquittoSub_AgainstGoMQTT(t *testing.T) {
	port := mustSplitPort(t, mustAddr(startGoMQTT(t)))
	subBinary := requireBinary(t, "mosquitto_sub")

	// mosquitto_sub subscribing to our broker with a wildcard, and reading
	// deliveries from it. This exercises our SUBSCRIBE decoding, our SUBACK
	// encoding and our PUBLISH encoding against an independent implementation.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, subBinary,
		"-h", "127.0.0.1", "-p", port,
		"-t", "interop/+/data",
		"-q", "1",
		"-C", "2", // exit after two messages
	)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	lines := make(chan string, 4)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	// mosquitto_sub needs a moment to complete its subscription before the
	// publications begin; without it the messages arrive first and are lost,
	// which is correct MQTT behaviour and a flaky test.
	time.Sleep(500 * time.Millisecond)

	pub := client.New(client.Options{
		Address: "127.0.0.1:" + port, ClientID: "our-pub", CleanSession: true,
	})
	require.NoError(t, pub.Connect())
	t.Cleanup(pub.Close)

	require.NoError(t, pub.Publish("interop/a/data", []byte("first"), 1, false))
	require.NoError(t, pub.Publish("interop/b/data", []byte("second"), 1, false))

	var got []string
	for len(got) < 2 {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("mosquitto_sub exited after receiving %v", got)
			}
			if trimmed := strings.TrimSpace(line); trimmed != "" {
				got = append(got, trimmed)
			}
		case <-ctx.Done():
			t.Fatalf("timed out; mosquitto_sub received only %v", got)
		}
	}

	assert.ElementsMatch(t, []string{"first", "second"}, got)
	_ = cmd.Wait()
}

func TestMosquittoSub_ReceivesRetainedFromGoMQTT(t *testing.T) {
	port := mustSplitPort(t, mustAddr(startGoMQTT(t)))
	subBinary := requireBinary(t, "mosquitto_sub")

	pub := client.New(client.Options{
		Address: "127.0.0.1:" + port, ClientID: "our-pub", CleanSession: true,
	})
	require.NoError(t, pub.Connect())
	t.Cleanup(pub.Close)

	require.NoError(t, pub.Publish("interop/retained", []byte("stored"), 0, true))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// -C 1 exits after one message. If our retained delivery did not set
	// RETAIN=1, or arrived at the wrong moment in the subscribe handshake,
	// mosquitto_sub would block here until the timeout.
	out, err := exec.CommandContext(ctx, subBinary,
		"-h", "127.0.0.1", "-p", port,
		"-t", "interop/retained",
		"-C", "1",
	).Output()

	require.NoError(t, err)
	assert.Equal(t, "stored", strings.TrimSpace(string(out)))
}

// ---------------------------------------------------------------------------
// Direction 2: our client against Mosquitto
// ---------------------------------------------------------------------------

func TestOurClient_AgainstMosquitto(t *testing.T) {
	addr := startMosquitto(t)

	received := newCollector()
	sub := client.New(client.Options{
		Address: addr, ClientID: "our-sub-on-mosquitto",
		CleanSession: true, OnMessage: received.handler(),
	})
	require.NoError(t, sub.Connect(), "our client could not connect to Mosquitto")
	t.Cleanup(sub.Close)

	granted, err := sub.Subscribe("interop/#", 1)
	require.NoError(t, err)
	assert.Equal(t, byte(1), granted)

	pub := client.New(client.Options{
		Address: addr, ClientID: "our-pub-on-mosquitto", CleanSession: true,
	})
	require.NoError(t, pub.Connect())
	t.Cleanup(pub.Close)

	// Both QoS levels, so our PUBLISH encoding and our PUBACK handling are both
	// checked against a real broker.
	require.NoError(t, pub.Publish("interop/x", []byte("qos0"), 0, false))
	require.NoError(t, pub.Publish("interop/y", []byte("qos1"), 1, false))

	received.waitFor(t, 2)

	payloads := make([]string, 0, 2)
	for _, m := range received.all() {
		payloads = append(payloads, string(m.Payload))
	}
	assert.ElementsMatch(t, []string{"qos0", "qos1"}, payloads)
}

func TestOurClient_ReceivesRetainedFromMosquitto(t *testing.T) {
	addr := startMosquitto(t)

	pub := client.New(client.Options{
		Address: addr, ClientID: "our-pub", CleanSession: true,
	})
	require.NoError(t, pub.Connect())
	t.Cleanup(pub.Close)
	require.NoError(t, pub.Publish("interop/state", []byte("retained-by-mosquitto"), 0, true))

	received := newCollector()
	sub := client.New(client.Options{
		Address: addr, ClientID: "our-late-sub",
		CleanSession: true, OnMessage: received.handler(),
	})
	require.NoError(t, sub.Connect())
	t.Cleanup(sub.Close)

	_, err := sub.Subscribe("interop/state", 0)
	require.NoError(t, err)

	received.waitFor(t, 1)
	msg := received.all()[0]
	assert.Equal(t, "retained-by-mosquitto", string(msg.Payload))
	// Mosquitto sets RETAIN=1 on a stored delivery, and so do we. Asserting it
	// here confirms we read the flag the same way an independent broker writes
	// it.
	assert.True(t, msg.Retained)
}

func TestOurClient_PersistentSessionOnMosquitto(t *testing.T) {
	addr := startMosquitto(t)
	clientID := "our-persistent-client"

	first := client.New(client.Options{
		Address: addr, ClientID: clientID, CleanSession: false,
	})
	require.NoError(t, first.Connect())
	_, err := first.Subscribe("interop/queued", 1)
	require.NoError(t, err)
	require.NoError(t, first.Disconnect())

	pub := client.New(client.Options{
		Address: addr, ClientID: "our-pub", CleanSession: true,
	})
	require.NoError(t, pub.Connect())
	t.Cleanup(pub.Close)
	require.NoError(t, pub.Publish("interop/queued", []byte("while-offline"), 1, false))

	received := newCollector()
	second := client.New(client.Options{
		Address: addr, ClientID: clientID,
		CleanSession: false, OnMessage: received.handler(),
	})
	require.NoError(t, second.Connect())
	t.Cleanup(second.Close)

	// Mosquitto's own answer to the same question our persistent-session test
	// asks of GoMQTT: the session was present, and the queued message arrives.
	assert.True(t, second.SessionPresent)
	received.waitFor(t, 1)
	assert.Equal(t, "while-offline", string(received.all()[0].Payload))
}
