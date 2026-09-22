package memory_test

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/infrastructure/persistence/memory"
)

// matchedIDs returns the client identifiers the index matched, sorted so the
// comparison is order-independent — the trie walk has no defined result order
// and asserting on one would make the test fragile for no benefit.
func matchedIDs(index *memory.SubscriptionIndex, topic string) []string {
	subscribers := index.Match(msgVO.MustNewTopicName(topic))

	ids := make([]string, 0, len(subscribers))
	for _, s := range subscribers {
		ids = append(ids, s.ClientID.String())
	}
	sort.Strings(ids)
	return ids
}

func TestIndex_MatchesLiteralAndWildcardFilters(t *testing.T) {
	index := memory.NewSubscriptionIndex()

	index.Subscribe(msgVO.MustNewTopicFilter("sport/tennis/player1"), "exact", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("sport/tennis/+"), "single", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("sport/#"), "multi", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("+/tennis/#"), "mixed", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("other/topic"), "unrelated", msgVO.QoSAtMostOnce)

	assert.Equal(t,
		[]string{"exact", "mixed", "multi", "single"},
		matchedIDs(index, "sport/tennis/player1"))

	assert.Equal(t,
		[]string{"mixed", "multi"},
		matchedIDs(index, "sport/tennis/player1/ranking"))

	// "sport/#" matches its own parent level (§4.7.1.2).
	assert.Equal(t, []string{"multi"}, matchedIDs(index, "sport"))

	assert.Empty(t, matchedIDs(index, "nothing/here"))
}

func TestIndex_ReturnsAClientOnceAtItsHighestGrantedQoS(t *testing.T) {
	index := memory.NewSubscriptionIndex()

	// One client, three filters, all matching the same topic. Delivering once
	// per matching filter would surprise every client that ever broadened a
	// subscription, so the contract is once, at the best QoS.
	index.Subscribe(msgVO.MustNewTopicFilter("a/b"), "c1", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("a/+"), "c1", msgVO.QoSAtLeastOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("a/#"), "c1", msgVO.QoSAtMostOnce)

	subscribers := index.Match(msgVO.MustNewTopicName("a/b"))

	require.Len(t, subscribers, 1)
	assert.Equal(t, msgVO.QoSAtLeastOnce, subscribers[0].GrantedQoS)
}

func TestIndex_ResubscribingReplacesTheGrantedQoS(t *testing.T) {
	index := memory.NewSubscriptionIndex()
	filter := msgVO.MustNewTopicFilter("a/b")

	index.Subscribe(filter, "c1", msgVO.QoSAtMostOnce)
	index.Subscribe(filter, "c1", msgVO.QoSAtLeastOnce)

	// §3.8.4: a repeat SUBSCRIBE replaces the entry rather than adding one.
	assert.Equal(t, 1, index.SubscriptionCount())

	subscribers := index.Match(msgVO.MustNewTopicName("a/b"))
	require.Len(t, subscribers, 1)
	assert.Equal(t, msgVO.QoSAtLeastOnce, subscribers[0].GrantedQoS)
}

func TestIndex_DoesNotMatchSystemTopicsWithLeadingWildcards(t *testing.T) {
	index := memory.NewSubscriptionIndex()

	index.Subscribe(msgVO.MustNewTopicFilter("#"), "hash", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("+/broker/uptime"), "plus", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("$SYS/#"), "sys", msgVO.QoSAtMostOnce)

	// §4.7.2. Without this rule, any client subscribing to "#" would receive
	// the broker's own internal telemetry.
	assert.Equal(t, []string{"sys"}, matchedIDs(index, "$SYS/broker/uptime"))

	// '$' only matters at the very start of a topic.
	assert.Equal(t, []string{"hash"}, matchedIDs(index, "a$b/c"))
}

func TestIndex_UnsubscribeRemovesOnlyTheNamedPair(t *testing.T) {
	index := memory.NewSubscriptionIndex()
	filter := msgVO.MustNewTopicFilter("a/b")

	index.Subscribe(filter, "c1", msgVO.QoSAtMostOnce)
	index.Subscribe(filter, "c2", msgVO.QoSAtMostOnce)

	index.Unsubscribe(filter, "c1")

	assert.Equal(t, []string{"c2"}, matchedIDs(index, "a/b"))
	assert.Equal(t, 1, index.SubscriptionCount())
}

func TestIndex_UnsubscribeAllClearsOneClientEverywhere(t *testing.T) {
	index := memory.NewSubscriptionIndex()

	index.Subscribe(msgVO.MustNewTopicFilter("a/b"), "c1", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("x/+/z"), "c1", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("#"), "c1", msgVO.QoSAtMostOnce)
	index.Subscribe(msgVO.MustNewTopicFilter("a/b"), "c2", msgVO.QoSAtMostOnce)

	index.UnsubscribeAll("c1")

	assert.Equal(t, []string{"c2"}, matchedIDs(index, "a/b"))
	assert.Empty(t, matchedIDs(index, "x/y/z"))
	assert.Equal(t, 1, index.SubscriptionCount())
}

func TestIndex_PrunesEmptyNodes(t *testing.T) {
	index := memory.NewSubscriptionIndex()

	// A fleet that subscribes per device and reconnects under new identifiers
	// would, without pruning, leave one permanent node per device that ever
	// existed. Over months of uptime that is an unbounded leak.
	for i := 0; i < 500; i++ {
		filter := msgVO.MustNewTopicFilter(fmt.Sprintf("devices/%d/telemetry", i))
		index.Subscribe(filter, clientVO.ClientID(fmt.Sprintf("c%d", i)), msgVO.QoSAtMostOnce)
	}
	require.Equal(t, 500, index.FilterCount())

	for i := 0; i < 500; i++ {
		index.UnsubscribeAll(clientVO.ClientID(fmt.Sprintf("c%d", i)))
	}

	assert.Zero(t, index.FilterCount())
	assert.Zero(t, index.SubscriptionCount())
	assert.Empty(t, matchedIDs(index, "devices/1/telemetry"))
}

// TestIndex_AgreesWithTheSpecImplementation is the test that makes the trie
// trustworthy.
//
// TopicFilter.Matches is the readable, obviously-correct statement of §4.7; the
// trie is the fast index. This generates thousands of filter/topic pairs and
// asserts the two never disagree — so the trie can be optimised freely, and any
// optimisation that breaks a corner case is caught here rather than by a user
// whose messages stopped arriving.
func TestIndex_AgreesWithTheSpecImplementation(t *testing.T) {
	rng := rand.New(rand.NewSource(20260922))

	levelAlphabet := []string{"a", "b", "c", "sport", "tennis", "", "$SYS", "1"}

	randomTopic := func() string {
		depth := 1 + rng.Intn(4)
		levels := make([]string, depth)
		for i := range levels {
			levels[i] = levelAlphabet[rng.Intn(len(levelAlphabet))]
		}
		return joinLevels(levels)
	}

	randomFilter := func() string {
		depth := 1 + rng.Intn(4)
		levels := make([]string, 0, depth)
		for i := 0; i < depth; i++ {
			switch rng.Intn(6) {
			case 0:
				levels = append(levels, msgVO.SingleLevelWildcard)
			case 1:
				// '#' must be last, so stop here (§4.7.1.2).
				levels = append(levels, msgVO.MultiLevelWildcard)
				return joinLevels(levels)
			default:
				levels = append(levels, levelAlphabet[rng.Intn(len(levelAlphabet))])
			}
		}
		return joinLevels(levels)
	}

	const rounds = 3000
	checked := 0

	for round := 0; round < rounds; round++ {
		rawFilter := randomFilter()
		rawTopic := randomTopic()

		filter, err := msgVO.NewTopicFilter(rawFilter)
		if err != nil {
			continue
		}
		topic, err := msgVO.NewTopicName(rawTopic)
		if err != nil {
			continue
		}
		checked++

		// A fresh index per round, holding exactly one subscription, so the
		// index's answer is unambiguously about this one filter.
		index := memory.NewSubscriptionIndex()
		index.Subscribe(filter, "c1", msgVO.QoSAtMostOnce)

		wantMatch := filter.Matches(topic)
		gotMatch := len(index.Match(topic)) == 1

		require.Equal(t, wantMatch, gotMatch,
			"trie and spec disagree: filter %q vs topic %q", rawFilter, rawTopic)
	}

	// Guard against the generator silently producing nothing valid, which would
	// make this test pass while checking zero cases.
	require.Greater(t, checked, 1000, "the generator produced too few valid pairs")
	t.Logf("trie agreed with the spec implementation on %d generated pairs", checked)
}

func TestIndex_IsSafeForConcurrentUse(t *testing.T) {
	index := memory.NewSubscriptionIndex()

	// Run under -race. In production, many publisher goroutines call Match
	// while clients subscribe and disconnect, so an unguarded trie would be a
	// live data race on the delivery path.
	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			clientID := clientVO.ClientID(fmt.Sprintf("c%d", worker))

			for i := 0; i < 200; i++ {
				filter := msgVO.MustNewTopicFilter(fmt.Sprintf("a/%d/+", i%10))
				index.Subscribe(filter, clientID, msgVO.QoSAtLeastOnce)
				index.Match(msgVO.MustNewTopicName(fmt.Sprintf("a/%d/c", i%10)))
				index.Unsubscribe(filter, clientID)
			}
			index.UnsubscribeAll(clientID)
		}(w)
	}
	wg.Wait()

	assert.Zero(t, index.SubscriptionCount())
}

// joinLevels joins topic levels with the separator.
func joinLevels(levels []string) string {
	out := ""
	for i, level := range levels {
		if i > 0 {
			out += msgVO.TopicLevelSeparator
		}
		out += level
	}
	return out
}

// BenchmarkIndex_Match measures the hot path: one lookup per published message.
func BenchmarkIndex_Match(b *testing.B) {
	for _, filterCount := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("filters=%d", filterCount), func(b *testing.B) {
			index := memory.NewSubscriptionIndex()
			for i := 0; i < filterCount; i++ {
				index.Subscribe(
					msgVO.MustNewTopicFilter(fmt.Sprintf("devices/%d/telemetry/+", i)),
					clientVO.ClientID(fmt.Sprintf("c%d", i)),
					msgVO.QoSAtMostOnce,
				)
			}
			// Some wildcard subscriptions, because a trie with only literal
			// filters never branches and the benchmark would flatter itself.
			index.Subscribe(msgVO.MustNewTopicFilter("devices/#"), "watcher", msgVO.QoSAtMostOnce)
			index.Subscribe(msgVO.MustNewTopicFilter("+/+/telemetry/+"), "auditor", msgVO.QoSAtMostOnce)

			topic := msgVO.MustNewTopicName("devices/42/telemetry/temp")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = index.Match(topic)
			}
		})
	}
}
