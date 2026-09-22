package conformance

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/gomqtt/pkg/client"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

func TestPubSub_QoS0(t *testing.T) {
	b := startBroker(t)
	_, received := b.subscriber(t, "sub", "sensors/temp", 0)
	pub := b.publisher(t, "pub")

	require.NoError(t, pub.Publish("sensors/temp", []byte("21.5"), 0, false))

	received.waitFor(t, 1)
	msg := received.all()[0]
	assert.Equal(t, "sensors/temp", msg.Topic)
	assert.Equal(t, "21.5", string(msg.Payload))
	assert.Equal(t, byte(0), msg.QoS)
	assert.False(t, msg.Retained, "a live delivery must carry RETAIN=0")
}

func TestPubSub_QoS1IsAcknowledged(t *testing.T) {
	b := startBroker(t)
	_, received := b.subscriber(t, "sub", "sensors/temp", 1)
	pub := b.publisher(t, "pub")

	// Publish blocks until the PUBACK arrives, so returning at all proves the
	// broker acknowledged it.
	require.NoError(t, pub.Publish("sensors/temp", []byte("21.5"), 1, false))

	received.waitFor(t, 1)
	msg := received.all()[0]
	assert.Equal(t, byte(1), msg.QoS)
	assert.NotZero(t, msg.PacketID, "a QoS 1 delivery must carry a packet identifier")
}

func TestQoS_IsDowngradedToTheMinimumOfPublishAndSubscribe(t *testing.T) {
	b := startBroker(t)
	_, received := b.subscriber(t, "sub-qos0", "a/b", 0)
	pub := b.publisher(t, "pub")

	// §4.3: the effective QoS is the lower of the two. A QoS 0 subscriber must
	// receive a QoS 1 publication at QoS 0.
	require.NoError(t, pub.Publish("a/b", []byte("x"), 1, false))

	received.waitFor(t, 1)
	assert.Equal(t, byte(0), received.all()[0].QoS)
}

func TestSubscribe_GrantsQoS1ForAQoS2Request(t *testing.T) {
	b := startBroker(t)
	c := b.connect(t, client.Options{ClientID: "sub", CleanSession: true})

	// §3.8.4 permits a server to grant a lower QoS than requested. Granting 1
	// is how this broker answers honestly instead of claiming an
	// exactly-once guarantee it does not implement.
	granted, err := c.Subscribe("a/b", 2)

	require.NoError(t, err)
	assert.Equal(t, byte(1), granted)
}

func TestWildcards_DeliverToEveryMatchingSubscriber(t *testing.T) {
	b := startBroker(t)

	_, exact := b.subscriber(t, "exact", "sport/tennis/player1", 0)
	_, single := b.subscriber(t, "single", "sport/tennis/+", 0)
	_, multi := b.subscriber(t, "multi", "sport/#", 0)
	_, unrelated := b.subscriber(t, "unrelated", "weather/#", 0)

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("sport/tennis/player1", []byte("ace"), 0, false))

	exact.waitFor(t, 1)
	single.waitFor(t, 1)
	multi.waitFor(t, 1)

	assert.Equal(t, []string{"ace"}, exact.payloads())
	assert.Equal(t, []string{"ace"}, single.payloads())
	assert.Equal(t, []string{"ace"}, multi.payloads())
	unrelated.expectNoMore(t, 200*time.Millisecond, 0)
}

func TestWildcards_MultiLevelMatchesItsOwnParent(t *testing.T) {
	b := startBroker(t)
	_, received := b.subscriber(t, "sub", "sport/#", 0)
	pub := b.publisher(t, "pub")

	// §4.7.1.2: "sport/#" matches "sport" itself, not only its children.
	require.NoError(t, pub.Publish("sport", []byte("parent"), 0, false))

	received.waitFor(t, 1)
	assert.Equal(t, []string{"parent"}, received.payloads())
}

func TestSubscriber_ReceivesAMessageOnceDespiteSeveralMatchingFilters(t *testing.T) {
	b := startBroker(t)

	received := newCollector()
	c := b.connect(t, client.Options{
		ClientID: "sub", CleanSession: true, OnMessage: received.handler(),
	})

	for _, filter := range []string{"a/b", "a/+", "a/#", "#"} {
		_, err := c.Subscribe(filter, 0)
		require.NoError(t, err)
	}

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("a/b", []byte("once"), 0, false))

	received.waitFor(t, 1)
	// Delivering once per matching filter would surprise every client that
	// ever broadened a subscription: adding "#" would suddenly duplicate
	// everything it was already receiving.
	received.expectNoMore(t, 300*time.Millisecond, 1)
}

func TestRetained_IsDeliveredToALaterSubscriber(t *testing.T) {
	b := startBroker(t)
	pub := b.publisher(t, "pub")

	require.NoError(t, pub.Publish("devices/1/status", []byte("online"), 0, true))

	// Subscribing *after* the publication: without retention this client would
	// learn nothing until the next update, which for a status topic could be
	// hours.
	_, received := b.subscriber(t, "late", "devices/+/status", 0)

	received.waitFor(t, 1)
	msg := received.all()[0]
	assert.Equal(t, "online", string(msg.Payload))
	// §3.3.1.3: a message delivered *because it was stored* carries RETAIN=1,
	// which is how the client tells history from news.
	assert.True(t, msg.Retained, "a retained delivery must carry RETAIN=1")
}

func TestRetained_IsClearedByAnEmptyRetainedPublish(t *testing.T) {
	b := startBroker(t)
	pub := b.publisher(t, "pub")

	require.NoError(t, pub.Publish("devices/1/status", []byte("online"), 0, true))
	// §3.3.1.3: a zero-length retained publication deletes the retained
	// message rather than storing an empty one.
	require.NoError(t, pub.Publish("devices/1/status", nil, 0, true))

	_, received := b.subscriber(t, "late", "devices/1/status", 0)
	received.expectNoMore(t, 300*time.Millisecond, 0)
}

func TestRetained_KeepsOnlyTheMostRecentMessagePerTopic(t *testing.T) {
	b := startBroker(t)
	pub := b.publisher(t, "pub")

	for _, value := range []string{"v1", "v2", "v3"} {
		require.NoError(t, pub.Publish("config/version", []byte(value), 0, true))
	}

	_, received := b.subscriber(t, "late", "config/version", 0)
	received.waitFor(t, 1)

	// Current state, not history: a subscriber must not receive the topic's
	// entire past on subscribe.
	assert.Equal(t, []string{"v3"}, received.payloads())
	received.expectNoMore(t, 200*time.Millisecond, 1)
}

func TestPersistentSession_QueuesMessagesWhileTheClientIsOffline(t *testing.T) {
	b := startBroker(t)
	clientID := uniqueID(t, "persistent")

	// First connection: subscribe with clean session 0, then disconnect.
	first := b.connect(t, client.Options{ClientID: clientID, CleanSession: false})
	_, err := first.Subscribe("orders/+", 1)
	require.NoError(t, err)
	require.NoError(t, first.Disconnect())

	// Published while the client is away. A clean session would lose these.
	pub := b.publisher(t, "pub")
	for i := 0; i < 3; i++ {
		require.NoError(t, pub.Publish(
			fmt.Sprintf("orders/%d", i), []byte(fmt.Sprintf("order-%d", i)), 1, false))
	}

	// Reconnect with the same identifier and clean session 0.
	received := newCollector()
	second := b.connect(t, client.Options{
		ClientID: clientID, CleanSession: false, OnMessage: received.handler(),
	})

	assert.True(t, second.SessionPresent,
		"CONNACK must report the stored session as present")

	received.waitFor(t, 3)
	assert.ElementsMatch(t,
		[]string{"order-0", "order-1", "order-2"},
		received.payloads())
}

func TestPersistentSession_RestoresSubscriptionsWithoutResubscribing(t *testing.T) {
	b := startBroker(t)
	clientID := uniqueID(t, "resub")

	first := b.connect(t, client.Options{ClientID: clientID, CleanSession: false})
	_, err := first.Subscribe("a/b", 0)
	require.NoError(t, err)
	require.NoError(t, first.Disconnect())

	received := newCollector()
	_ = b.connect(t, client.Options{
		ClientID: clientID, CleanSession: false, OnMessage: received.handler(),
	})

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("a/b", []byte("still subscribed"), 0, false))

	// The whole point of a persistent session: the client did not resubscribe.
	received.waitFor(t, 1)
	assert.Equal(t, []string{"still subscribed"}, received.payloads())
}

func TestCleanSession_DiscardsTheStoredSession(t *testing.T) {
	b := startBroker(t)
	clientID := uniqueID(t, "clean")

	first := b.connect(t, client.Options{ClientID: clientID, CleanSession: false})
	_, err := first.Subscribe("a/b", 0)
	require.NoError(t, err)
	require.NoError(t, first.Disconnect())

	// §3.1.2.4: reconnecting with clean session 1 throws the stored session
	// away, subscriptions included.
	received := newCollector()
	second := b.connect(t, client.Options{
		ClientID: clientID, CleanSession: true, OnMessage: received.handler(),
	})
	assert.False(t, second.SessionPresent)

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("a/b", []byte("gone"), 0, false))

	received.expectNoMore(t, 300*time.Millisecond, 0)
}

func TestSessionTakeover_ClosesTheOlderConnection(t *testing.T) {
	b := startBroker(t)
	clientID := uniqueID(t, "takeover")

	firstReceived := newCollector()
	_ = b.connect(t, client.Options{
		ClientID: clientID, CleanSession: true, OnMessage: firstReceived.handler(),
	})

	// §3.1.4: a second CONNECT with the same identifier takes over, and the
	// broker disconnects the first.
	secondReceived := newCollector()
	second := b.connect(t, client.Options{
		ClientID: clientID, CleanSession: true, OnMessage: secondReceived.handler(),
	})

	_, err := second.Subscribe("a/b", 0)
	require.NoError(t, err)

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("a/b", []byte("to the survivor"), 0, false))

	secondReceived.waitFor(t, 1)
	firstReceived.expectNoMore(t, 200*time.Millisecond, 0)
}

func TestUnsubscribe_StopsDelivery(t *testing.T) {
	b := startBroker(t)
	received := newCollector()

	c := b.connect(t, client.Options{
		ClientID: "sub", CleanSession: true, OnMessage: received.handler(),
	})
	_, err := c.Subscribe("a/b", 0)
	require.NoError(t, err)

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("a/b", []byte("before"), 0, false))
	received.waitFor(t, 1)

	require.NoError(t, c.Unsubscribe("a/b"))
	require.NoError(t, pub.Publish("a/b", []byte("after"), 0, false))

	received.expectNoMore(t, 300*time.Millisecond, 1)
}

func TestWill_IsPublishedOnAnAbnormalDisconnect(t *testing.T) {
	b := startBroker(t)
	_, received := b.subscriber(t, "watcher", "devices/+/status", 0)

	willing := b.connect(t, client.Options{
		ClientID:     "device-1",
		CleanSession: true,
		Will: &packet.Will{
			Topic:   "devices/1/status",
			Payload: []byte("offline"),
			QoS:     0,
		},
	})

	// Close drops the socket without DISCONNECT, which §3.1.2.5 defines as the
	// abnormal case that publishes the will.
	willing.Close()

	received.waitFor(t, 1)
	assert.Equal(t, []string{"offline"}, received.payloads())
	assert.Equal(t, []string{"devices/1/status"}, received.topics())
}

func TestWill_IsSuppressedOnACleanDisconnect(t *testing.T) {
	b := startBroker(t)
	_, received := b.subscriber(t, "watcher", "devices/+/status", 0)

	willing := b.connect(t, client.Options{
		ClientID:     "device-1",
		CleanSession: true,
		Will: &packet.Will{
			Topic:   "devices/1/status",
			Payload: []byte("offline"),
			QoS:     0,
		},
	})

	// §3.14.4: a clean DISCONNECT means the client left on purpose, so the will
	// must not be published — otherwise every orderly shutdown would look like
	// a crash to whoever is watching.
	require.NoError(t, willing.Disconnect())

	received.expectNoMore(t, 400*time.Millisecond, 0)
}

func TestSystemTopics_AreNotMatchedByALeadingWildcard(t *testing.T) {
	b := startBroker(t)
	_, hash := b.subscriber(t, "hash", "#", 0)
	_, sys := b.subscriber(t, "sys", "$SYS/#", 0)

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("$SYS/broker/uptime", []byte("42"), 0, false))
	require.NoError(t, pub.Publish("normal/topic", []byte("visible"), 0, false))

	sys.waitFor(t, 1)
	hash.waitFor(t, 1)

	// §4.7.2. Without this rule, subscribing to "#" would hand any client the
	// broker's own internal telemetry.
	assert.Equal(t, []string{"42"}, sys.payloads())
	assert.Equal(t, []string{"visible"}, hash.payloads())
}

func TestKeepAlive_DisconnectsASilentClient(t *testing.T) {
	b := startBroker(t)

	// KeepAlive 1 with the client's pinger suppressed: the broker allows 1.5x
	// the interval (§3.1.2.10), so it should hang up at about 1.5 seconds.
	_ = b.connect(t, client.Options{
		ClientID: "silent", CleanSession: true, KeepAlive: 1,
		SuppressPing: true,
	})

	// The probe is the broker's own connection count, not a client call.
	// §3.1.2.10 resets the keep-alive timer on *any* inbound packet, so a test
	// that polled by sending something would keep the client alive and assert
	// nothing.
	require.Eventually(t, func() bool {
		return b.listener.ConnectionCount() == 0
	}, 6*time.Second, 50*time.Millisecond,
		"the broker did not disconnect a client that missed its keep-alive window")
}

func TestKeepAlive_TolerantOfAClientThatPings(t *testing.T) {
	b := startBroker(t)

	// The same interval, but the client's keep-alive loop pings at half of it.
	// This is the other half of the test above: a broker that disconnects a
	// client which *is* communicating is far worse than one that is slow to
	// notice a dead one.
	_, received := b.subscriber(t, "chatty", "a/b", 0)
	_ = b.connect(t, client.Options{
		ClientID: "pinger", CleanSession: true, KeepAlive: 1,
	})

	time.Sleep(2500 * time.Millisecond)

	pub := b.publisher(t, "pub")
	require.NoError(t, pub.Publish("a/b", []byte("still here"), 0, false))
	received.waitFor(t, 1)
}

func TestBroker_RefusesAConnectionOverTheConfiguredLimit(t *testing.T) {
	b := startBroker(t, func(cfg *config.AppConfig) {
		cfg.Broker.MaxConnections = 2
	})

	_ = b.connect(t, client.Options{ClientID: "c1", CleanSession: true})
	_ = b.connect(t, client.Options{ClientID: "c2", CleanSession: true})

	// The limit exists so the broker's failure mode under a connection flood is
	// a refusal rather than the OOM killer.
	third := client.New(client.Options{
		Address: b.addr, ClientID: "c3", CleanSession: true,
		ConnectTimeout: 2 * time.Second,
	})
	assert.Error(t, third.Connect())
}

func TestBroker_RejectsAnEmptyClientIDWithAPersistentSession(t *testing.T) {
	b := startBroker(t)

	// §3.1.3.1: an empty identifier is legal only with clean session 1, because
	// the identifier is the key the session is stored under.
	c := client.New(client.Options{
		Address: b.addr, ClientID: "", CleanSession: false,
		ConnectTimeout: 2 * time.Second,
	})

	err := c.Connect()
	require.Error(t, err)
	assert.ErrorIs(t, err, client.ErrConnectionRefused)
}

func TestBroker_AssignsAnIdentifierForAnEmptyCleanSessionConnect(t *testing.T) {
	b := startBroker(t)

	// The same field, the other flag: with clean session 1 the broker assigns
	// an identifier rather than refusing.
	c := client.New(client.Options{
		Address: b.addr, ClientID: "", CleanSession: true,
		ConnectTimeout: 2 * time.Second,
	})
	require.NoError(t, c.Connect())
	t.Cleanup(c.Close)

	_, err := c.Subscribe("a/b", 0)
	assert.NoError(t, err)
}

func TestBroker_HandlesManyConcurrentClients(t *testing.T) {
	b := startBroker(t)

	const (
		subscribers  = 25
		perPublisher = 20
	)

	collectors := make([]*collector, subscribers)
	for i := 0; i < subscribers; i++ {
		_, col := b.subscriber(t, fmt.Sprintf("sub-%d", i), "fanout/#", 0)
		collectors[i] = col
	}

	pub := b.publisher(t, "pub")
	for i := 0; i < perPublisher; i++ {
		require.NoError(t, pub.Publish(
			fmt.Sprintf("fanout/%d", i), []byte(fmt.Sprintf("m%d", i)), 0, false))
	}

	// Every subscriber must receive every message: a fan-out that drops under
	// concurrency is the failure this test exists to catch.
	for i, col := range collectors {
		col.waitFor(t, perPublisher)
		assert.Equal(t, perPublisher, col.count(), "subscriber %d lost messages", i)
	}
}

func TestBroker_StopsGracefullyWithClientsConnected(t *testing.T) {
	b := startBroker(t)

	for i := 0; i < 10; i++ {
		_, _ = b.subscriber(t, fmt.Sprintf("sub-%d", i), "a/#", 0)
	}
	require.Equal(t, int64(10), b.listener.ConnectionCount())

	start := time.Now()
	b.stop()

	// Shutdown must not hang waiting for clients that will never speak again.
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Zero(t, b.listener.ConnectionCount())
}
