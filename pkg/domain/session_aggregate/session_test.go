package session_aggregate_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
	vo "github.com/anashasan/gomqtt/pkg/domain/session_aggregate/value_objects"
)

// baseTime is a fixed instant so every assertion about timing is exact rather
// than "roughly now".
var baseTime = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

// newSession builds a session with small limits, so the boundary behaviour the
// tests care about is reachable without building a thousand messages.
func newSession(t *testing.T, limits sessionAgg.Limits) *sessionAgg.Session {
	t.Helper()
	return sessionAgg.NewSession("client-1", false, limits, baseTime)
}

// newMessage builds a message for a topic.
func newMessage(t *testing.T, topic, payload string, qos msgVO.QoS) *msgAgg.Message {
	t.Helper()

	packetID := uint16(0)
	if qos.RequiresAcknowledgement() {
		packetID = 1
	}

	m, err := msgAgg.NewMessage(msgAgg.NewMessageParams{
		Topic:    msgVO.MustNewTopicName(topic),
		Payload:  []byte(payload),
		QoS:      qos,
		PacketID: packetID,
		Now:      baseTime,
	})
	require.NoError(t, err)
	return m
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

func TestSubscribe_GrantsTheRequestedQoSUpToTheBrokerCeiling(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{})

	granted, err := s.Subscribe(msgVO.MustNewTopicFilter("a/b"), msgVO.QoSAtLeastOnce, baseTime)
	require.NoError(t, err)
	assert.Equal(t, msgVO.QoSAtLeastOnce, granted)

	// §3.8.4 permits granting lower than requested, which is how this broker
	// answers a QoS 2 request honestly rather than claiming a guarantee it does
	// not implement.
	granted, err = s.Subscribe(msgVO.MustNewTopicFilter("c/d"), msgVO.QoSExactlyOnce, baseTime)
	require.NoError(t, err)
	assert.Equal(t, msgVO.QoSAtLeastOnce, granted)
}

func TestSubscribe_ReplacesAnExistingFilterRatherThanAddingASecond(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{})
	filter := msgVO.MustNewTopicFilter("a/b")

	_, err := s.Subscribe(filter, msgVO.QoSAtMostOnce, baseTime)
	require.NoError(t, err)
	_, err = s.Subscribe(filter, msgVO.QoSAtLeastOnce, baseTime)
	require.NoError(t, err)

	// A broker that appended would deliver twice to a client that merely
	// refreshed its subscription.
	assert.Equal(t, 1, s.SubscriptionCount())

	sub, ok := s.Subscription(filter)
	require.True(t, ok)
	assert.Equal(t, msgVO.QoSAtLeastOnce, sub.GrantedQoS())
}

func TestSubscribe_RejectsPastTheSubscriptionLimit(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxSubscriptions: 2})

	for i := 0; i < 2; i++ {
		_, err := s.Subscribe(
			msgVO.MustNewTopicFilter(fmt.Sprintf("a/%d", i)), msgVO.QoSAtMostOnce, baseTime)
		require.NoError(t, err)
	}

	// The limit bounds per-session memory: a session is created by anyone who
	// can open a TCP connection.
	_, err := s.Subscribe(msgVO.MustNewTopicFilter("a/3"), msgVO.QoSAtMostOnce, baseTime)
	assert.Error(t, err)

	// Re-subscribing to an existing filter is still allowed at the limit,
	// because it replaces rather than adds.
	_, err = s.Subscribe(msgVO.MustNewTopicFilter("a/0"), msgVO.QoSAtLeastOnce, baseTime)
	assert.NoError(t, err)
}

func TestMatchingSubscription_PicksTheHighestGrantedQoS(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{})

	_, err := s.Subscribe(msgVO.MustNewTopicFilter("a/b"), msgVO.QoSAtMostOnce, baseTime)
	require.NoError(t, err)
	_, err = s.Subscribe(msgVO.MustNewTopicFilter("a/+"), msgVO.QoSAtLeastOnce, baseTime)
	require.NoError(t, err)
	_, err = s.Subscribe(msgVO.MustNewTopicFilter("a/#"), msgVO.QoSAtMostOnce, baseTime)
	require.NoError(t, err)

	// §3.3.5 leaves this to the implementation. Delivering once per matching
	// filter surprises every client that ever broadens a subscription, so this
	// broker delivers once at the best QoS among the matches.
	sub, ok := s.MatchingSubscription(msgVO.MustNewTopicName("a/b"))
	require.True(t, ok)
	assert.Equal(t, msgVO.QoSAtLeastOnce, sub.GrantedQoS())

	_, ok = s.MatchingSubscription(msgVO.MustNewTopicName("z/z"))
	assert.False(t, ok)
}

func TestUnsubscribe_ReportsWhetherAnythingWasRemoved(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{})
	filter := msgVO.MustNewTopicFilter("a/b")

	_, err := s.Subscribe(filter, msgVO.QoSAtMostOnce, baseTime)
	require.NoError(t, err)

	assert.True(t, s.Unsubscribe(filter))
	// §3.10.4 requires an UNSUBACK either way, so this is informational rather
	// than an error condition.
	assert.False(t, s.Unsubscribe(filter))
	assert.Zero(t, s.SubscriptionCount())
}

// ---------------------------------------------------------------------------
// Packet identifiers and the in-flight window
// ---------------------------------------------------------------------------

func TestNextPacketID_NeverReturnsZeroAndSkipsIdentifiersInFlight(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 4})

	first, err := s.NextPacketID()
	require.NoError(t, err)
	// §2.3.1: zero is reserved.
	assert.NotZero(t, first.Uint16())

	require.NoError(t, s.TrackInflight(first, newMessage(t, "a/b", "x", msgVO.QoSAtLeastOnce), baseTime))

	second, err := s.NextPacketID()
	require.NoError(t, err)
	assert.NotEqual(t, first, second, "an identifier in flight must not be reissued")
}

func TestNextPacketID_RotatesRatherThanRestartingAtOne(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 8})

	first, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(first, newMessage(t, "a", "x", msgVO.QoSAtLeastOnce), baseTime))

	_, err = s.Acknowledge(first)
	require.NoError(t, err)

	// Reusing an identifier the instant it is freed makes a late PUBACK for the
	// previous message acknowledge the wrong one, so the cursor moves on.
	next, err := s.NextPacketID()
	require.NoError(t, err)
	assert.NotEqual(t, first, next)
}

func TestTrackInflight_RefusesPastTheWindow(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 2})

	for i := 0; i < 2; i++ {
		id, err := s.NextPacketID()
		require.NoError(t, err)
		require.NoError(t, s.TrackInflight(id, newMessage(t, "a", "x", msgVO.QoSAtLeastOnce), baseTime))
	}

	assert.False(t, s.HasInflightCapacity())

	// The window is flow control: without it, a subscriber that stops
	// acknowledging still has messages queued at the full publish rate and the
	// broker's memory becomes that one client's buffer.
	id, err := s.NextPacketID()
	require.NoError(t, err)
	assert.Error(t, s.TrackInflight(id, newMessage(t, "a", "x", msgVO.QoSAtLeastOnce), baseTime))
}

func TestAcknowledge_ClearsTheSlotAndRejectsAnUnknownIdentifier(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 2})

	id, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(id, newMessage(t, "a", "payload", msgVO.QoSAtLeastOnce), baseTime))
	require.Equal(t, 1, s.InflightCount())

	acked, err := s.Acknowledge(id)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(acked.Message.Payload()))
	assert.Zero(t, s.InflightCount())

	// §4.4 makes an unsolicited acknowledgement a protocol violation. Silently
	// ignoring it would hide a broken client, or let one free a window slot it
	// does not own.
	_, err = s.Acknowledge(id)
	assert.Error(t, err)
}

func TestInflightMessages_AreReturnedOldestFirst(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 8})

	for i := 0; i < 4; i++ {
		id, err := s.NextPacketID()
		require.NoError(t, err)
		require.NoError(t, s.TrackInflight(
			id,
			newMessage(t, "a", fmt.Sprintf("m%d", i), msgVO.QoSAtLeastOnce),
			baseTime.Add(time.Duration(i)*time.Second),
		))
	}

	// §4.6 requires retransmission in the original order, so a reconnecting
	// subscriber does not receive its backlog shuffled.
	messages := s.InflightMessages()
	require.Len(t, messages, 4)
	for i := 0; i < 4; i++ {
		assert.Equal(t, fmt.Sprintf("m%d", i), string(messages[i].Message.Payload()))
	}
}

func TestExpiredInflight_ReturnsOnlyMessagesPastTheTimeout(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 8})

	oldID, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(oldID, newMessage(t, "a", "old", msgVO.QoSAtLeastOnce), baseTime))

	recentID, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(
		recentID, newMessage(t, "a", "recent", msgVO.QoSAtLeastOnce),
		baseTime.Add(50*time.Second)))

	expired := s.ExpiredInflight(baseTime.Add(60*time.Second), 30*time.Second)

	require.Len(t, expired, 1)
	assert.Equal(t, "old", string(expired[0].Message.Payload()))
}

func TestMarkRetransmitted_CountsAttemptsAndResetsTheClock(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 4})

	id, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(id, newMessage(t, "a", "x", msgVO.QoSAtLeastOnce), baseTime))

	s.MarkRetransmitted(id, baseTime.Add(time.Minute))

	messages := s.InflightMessages()
	require.Len(t, messages, 1)
	// The count is what lets an operator tell a message delivered once from one
	// that has been retried eleven times.
	assert.Equal(t, uint32(2), messages[0].Attempts)
	// The clock reset stops the same message expiring again immediately.
	assert.Empty(t, s.ExpiredInflight(baseTime.Add(70*time.Second), 30*time.Second))
}

// ---------------------------------------------------------------------------
// The offline queue
// ---------------------------------------------------------------------------

func TestEnqueue_DropsTheOldestWhenFull(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxQueued: 3})

	for i := 0; i < 3; i++ {
		assert.False(t, s.Enqueue(newMessage(t, "a", fmt.Sprintf("m%d", i), msgVO.QoSAtMostOnce)))
	}
	require.Equal(t, 3, s.QueuedCount())

	dropped := s.Enqueue(newMessage(t, "a", "m3", msgVO.QoSAtMostOnce))

	// For the telemetry MQTT mostly carries, the newest reading is the one that
	// matters: dropping it to preserve a stale one means a reconnecting client
	// replays history and learns nothing about the present.
	assert.True(t, dropped)
	assert.Equal(t, 3, s.QueuedCount())
	assert.Equal(t, uint64(1), s.DroppedFromQueue(), "the loss must be counted, not silent")

	payloads := make([]string, 0, 3)
	for _, m := range s.PeekQueue() {
		payloads = append(payloads, string(m.Payload()))
	}
	assert.Equal(t, []string{"m1", "m2", "m3"}, payloads)
}

func TestDrainQueue_ReturnsOldestFirstInBatches(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxQueued: 10})

	for i := 0; i < 5; i++ {
		s.Enqueue(newMessage(t, "a", fmt.Sprintf("m%d", i), msgVO.QoSAtMostOnce))
	}

	// Batched so a client reconnecting to a large backlog does not have it all
	// pushed into its in-flight window, or its socket, at once.
	first := s.DrainQueue(2)
	require.Len(t, first, 2)
	assert.Equal(t, "m0", string(first[0].Payload()))
	assert.Equal(t, "m1", string(first[1].Payload()))
	assert.Equal(t, 3, s.QueuedCount())

	rest := s.DrainQueue(100)
	assert.Len(t, rest, 3)
	assert.Zero(t, s.QueuedCount())

	assert.Nil(t, s.DrainQueue(10))
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

func TestDisconnect_KeepsTheInflightWindow(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 4})

	id, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(id, newMessage(t, "a", "x", msgVO.QoSAtLeastOnce), baseTime))

	s.Disconnect(baseTime.Add(time.Minute))

	assert.False(t, s.IsConnected())
	require.NotNil(t, s.DisconnectedAt())
	// §4.4 requires unacknowledged QoS 1 messages to be retransmitted on
	// reconnect. Clearing the window here would quietly turn "at least once"
	// into "at most once" for exactly the messages in flight when the network
	// failed.
	assert.Equal(t, 1, s.InflightCount())
}

func TestResume_ReattachesWithoutLosingState(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{})

	_, err := s.Subscribe(msgVO.MustNewTopicFilter("a/b"), msgVO.QoSAtLeastOnce, baseTime)
	require.NoError(t, err)
	s.Enqueue(newMessage(t, "a/b", "queued", msgVO.QoSAtLeastOnce))
	s.Disconnect(baseTime.Add(time.Minute))

	s.Resume(baseTime.Add(time.Hour))

	assert.True(t, s.IsConnected())
	assert.Nil(t, s.DisconnectedAt())
	assert.Equal(t, baseTime.Add(time.Hour), s.LastConnectAt())
	// The whole point of a persistent session.
	assert.Equal(t, 1, s.SubscriptionCount())
	assert.Equal(t, 1, s.QueuedCount())
}

func TestClear_DiscardsEverything(t *testing.T) {
	s := newSession(t, sessionAgg.Limits{MaxInflight: 4})

	_, err := s.Subscribe(msgVO.MustNewTopicFilter("a/b"), msgVO.QoSAtLeastOnce, baseTime)
	require.NoError(t, err)
	s.Enqueue(newMessage(t, "a/b", "queued", msgVO.QoSAtMostOnce))

	id, err := s.NextPacketID()
	require.NoError(t, err)
	require.NoError(t, s.TrackInflight(id, newMessage(t, "a/b", "x", msgVO.QoSAtLeastOnce), baseTime))

	// §3.1.2.4: reconnecting with clean session 1 discards any previous session.
	s.Clear()

	assert.Zero(t, s.SubscriptionCount())
	assert.Zero(t, s.QueuedCount())
	assert.Zero(t, s.InflightCount())
}

func TestIsPersistent_FollowsTheCleanSessionFlag(t *testing.T) {
	persistent := sessionAgg.NewSession("c", false, sessionAgg.Limits{}, baseTime)
	clean := sessionAgg.NewSession("c", true, sessionAgg.Limits{}, baseTime)

	assert.True(t, persistent.IsPersistent())
	assert.False(t, persistent.IsClean())
	assert.False(t, clean.IsPersistent())
	assert.True(t, clean.IsClean())
}

func TestNewPacketID_Validation(t *testing.T) {
	_, err := vo.NewPacketID(0)
	assert.Error(t, err, "§2.3.1 reserves zero")

	id, err := vo.NewPacketID(42)
	require.NoError(t, err)
	assert.Equal(t, uint16(42), id.Uint16())
	assert.Equal(t, "42", id.String())
	assert.False(t, id.IsZero())
}
