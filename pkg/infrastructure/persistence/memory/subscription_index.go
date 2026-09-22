// Package memory implements the domain persistence ports in process memory.
package memory

import (
	"strings"
	"sync"

	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
)

var _ persistence.ISubscriptionIndex = (*SubscriptionIndex)(nil)

// trieNode is one topic level in the subscription trie.
//
// Wildcard children are held in their own fields rather than in the children
// map. That costs two words per node and buys the matching walk a direct
// pointer instead of two map lookups at every level — and matching runs once
// per published message, which is the broker's hottest path.
type trieNode struct {
	children map[string]*trieNode
	single   *trieNode // '+'
	multi    *trieNode // '#'

	// subscribers is the set of clients subscribed at exactly this node, with
	// the QoS granted to each. A map because subscribe/unsubscribe must be
	// O(1) and because re-subscribing replaces rather than appends (§3.8.4).
	subscribers map[clientVO.ClientID]msgVO.QoS
}

// newTrieNode builds an empty node.
func newTrieNode() *trieNode {
	return &trieNode{children: make(map[string]*trieNode)}
}

// isEmpty reports whether the node holds nothing and can be pruned.
func (n *trieNode) isEmpty() bool {
	return len(n.subscribers) == 0 &&
		len(n.children) == 0 &&
		n.single == nil &&
		n.multi == nil
}

// SubscriptionIndex is a topic trie: the routing table that turns a published
// topic into a set of subscribers.
//
// The shape follows the topic hierarchy, so a lookup costs one step per topic
// level rather than one comparison per registered filter. With 10,000 filters
// registered, a linear scan does 10,000 pattern matches per published message;
// this does as many steps as the topic has levels — typically three or four —
// plus a branch wherever a wildcard subscription exists.
//
// It is guarded by a single RWMutex. Matching takes the read lock and is the
// overwhelming majority of the traffic, so many publisher goroutines proceed in
// parallel; subscribe and unsubscribe are comparatively rare and take the write
// lock. A sharded lock would scale further and is not worth the complexity
// until the profile says the lock is the bottleneck.
type SubscriptionIndex struct {
	mu   sync.RWMutex
	root *trieNode

	// clientFilters lets UnsubscribeAll find a client's filters directly.
	// Without it, disconnecting one client would mean walking the whole trie,
	// and a broker restart with 50,000 sessions would walk it 50,000 times.
	clientFilters map[clientVO.ClientID]map[msgVO.TopicFilter]struct{}

	subscriptionCount int
}

// NewSubscriptionIndex builds an empty index.
func NewSubscriptionIndex() *SubscriptionIndex {
	return &SubscriptionIndex{
		root:          newTrieNode(),
		clientFilters: make(map[clientVO.ClientID]map[msgVO.TopicFilter]struct{}),
	}
}

// Subscribe records a client's interest in a filter.
func (i *SubscriptionIndex) Subscribe(
	filter msgVO.TopicFilter,
	clientID clientVO.ClientID,
	grantedQoS msgVO.QoS,
) {
	i.mu.Lock()
	defer i.mu.Unlock()

	node := i.root
	for _, level := range filter.Levels() {
		node = node.childFor(level)
	}

	if node.subscribers == nil {
		node.subscribers = make(map[clientVO.ClientID]msgVO.QoS)
	}
	// A repeat subscription replaces the granted QoS rather than adding a
	// second entry, so the count only grows on a genuinely new pair.
	if _, existed := node.subscribers[clientID]; !existed {
		i.subscriptionCount++
	}
	node.subscribers[clientID] = grantedQoS

	filters, ok := i.clientFilters[clientID]
	if !ok {
		filters = make(map[msgVO.TopicFilter]struct{})
		i.clientFilters[clientID] = filters
	}
	filters[filter] = struct{}{}
}

// childFor returns the child node for a filter level, creating it if needed.
func (n *trieNode) childFor(level string) *trieNode {
	switch level {
	case msgVO.SingleLevelWildcard:
		if n.single == nil {
			n.single = newTrieNode()
		}
		return n.single

	case msgVO.MultiLevelWildcard:
		if n.multi == nil {
			n.multi = newTrieNode()
		}
		return n.multi

	default:
		child, ok := n.children[level]
		if !ok {
			child = newTrieNode()
			n.children[level] = child
		}
		return child
	}
}

// Unsubscribe removes one client's interest in a filter.
func (i *SubscriptionIndex) Unsubscribe(filter msgVO.TopicFilter, clientID clientVO.ClientID) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.removeLocked(filter, clientID)

	if filters, ok := i.clientFilters[clientID]; ok {
		delete(filters, filter)
		if len(filters) == 0 {
			delete(i.clientFilters, clientID)
		}
	}
}

// UnsubscribeAll removes every subscription a client holds.
func (i *SubscriptionIndex) UnsubscribeAll(clientID clientVO.ClientID) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for filter := range i.clientFilters[clientID] {
		i.removeLocked(filter, clientID)
	}
	delete(i.clientFilters, clientID)
}

// removeLocked deletes a subscription and prunes any nodes it emptied. The
// caller holds the write lock.
//
// Pruning matters over a long uptime: a fleet that subscribes to per-device
// topics and reconnects with new identifiers would otherwise leave a permanent
// node per device that ever existed, and the trie would only ever grow.
func (i *SubscriptionIndex) removeLocked(filter msgVO.TopicFilter, clientID clientVO.ClientID) {
	levels := filter.Levels()

	// Record the path so empty nodes can be pruned back up it afterwards.
	path := make([]*trieNode, 0, len(levels)+1)
	node := i.root
	path = append(path, node)

	for _, level := range levels {
		next := node.lookupChild(level)
		if next == nil {
			return
		}
		node = next
		path = append(path, node)
	}

	if _, ok := node.subscribers[clientID]; !ok {
		return
	}
	delete(node.subscribers, clientID)
	i.subscriptionCount--

	for depth := len(path) - 1; depth > 0; depth-- {
		if !path[depth].isEmpty() {
			break
		}
		path[depth-1].detach(levels[depth-1])
	}
}

// lookupChild returns the child for a filter level without creating it.
func (n *trieNode) lookupChild(level string) *trieNode {
	switch level {
	case msgVO.SingleLevelWildcard:
		return n.single
	case msgVO.MultiLevelWildcard:
		return n.multi
	default:
		return n.children[level]
	}
}

// detach removes a child node.
func (n *trieNode) detach(level string) {
	switch level {
	case msgVO.SingleLevelWildcard:
		n.single = nil
	case msgVO.MultiLevelWildcard:
		n.multi = nil
	default:
		delete(n.children, level)
	}
}

// Match returns every client subscribed to a topic.
//
// A client that matches through several filters appears once, at the highest
// granted QoS among them — the same rule Session.MatchingSubscription applies,
// stated here because the index is what the delivery path actually consults.
func (i *SubscriptionIndex) Match(topic msgVO.TopicName) []persistence.Subscriber {
	i.mu.RLock()
	defer i.mu.RUnlock()

	levels := strings.Split(topic.String(), msgVO.TopicLevelSeparator)

	// Collected into a map first to deduplicate, then flattened. Allocating the
	// map lazily would save work for topics with no subscribers, but the common
	// case in a live broker is that a published topic has at least one.
	matched := make(map[clientVO.ClientID]msgVO.QoS)

	// §4.7.2: a wildcard at the start of a filter must not match a topic whose
	// first level begins with '$'. Skipping the root's wildcard branches is
	// exactly that rule, applied once at the top rather than checked at every
	// node.
	skipRootWildcards := topic.IsSystem()

	i.walk(i.root, levels, matched, skipRootWildcards)

	out := make([]persistence.Subscriber, 0, len(matched))
	for clientID, qos := range matched {
		out = append(out, persistence.Subscriber{ClientID: clientID, GrantedQoS: qos})
	}
	return out
}

// walk descends the trie, collecting subscribers.
//
// Recursive rather than iterative because a topic branches at every wildcard
// node and an explicit stack would be the same algorithm with more bookkeeping.
// Depth is bounded by MaxTopicLevels, so the stack is bounded too.
func (i *SubscriptionIndex) walk(
	node *trieNode,
	levels []string,
	matched map[clientVO.ClientID]msgVO.QoS,
	skipWildcards bool,
) {
	if node == nil {
		return
	}

	if !skipWildcards && node.multi != nil {
		// '#' matches the remainder of the topic, including zero levels, so its
		// subscribers match here regardless of what is left (§4.7.1.2).
		collect(node.multi, matched)
	}

	if len(levels) == 0 {
		collect(node, matched)
		return
	}

	level := levels[0]
	rest := levels[1:]

	if child, ok := node.children[level]; ok {
		i.walk(child, rest, matched, false)
	}
	if !skipWildcards && node.single != nil {
		i.walk(node.single, rest, matched, false)
	}
}

// collect merges a node's subscribers into the result, keeping the highest QoS
// per client.
func collect(node *trieNode, matched map[clientVO.ClientID]msgVO.QoS) {
	for clientID, qos := range node.subscribers {
		if existing, ok := matched[clientID]; !ok || qos > existing {
			matched[clientID] = qos
		}
	}
}

// FilterCount returns how many distinct filters are registered.
func (i *SubscriptionIndex) FilterCount() int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return countFilters(i.root)
}

// countFilters walks the trie counting nodes that hold subscribers.
func countFilters(node *trieNode) int {
	if node == nil {
		return 0
	}
	count := 0
	if len(node.subscribers) > 0 {
		count++
	}
	for _, child := range node.children {
		count += countFilters(child)
	}
	count += countFilters(node.single)
	count += countFilters(node.multi)
	return count
}

// SubscriptionCount returns the total number of client/filter pairs.
func (i *SubscriptionIndex) SubscriptionCount() int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.subscriptionCount
}
