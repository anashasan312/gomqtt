package memory

import (
	"sync"

	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
)

var _ persistence.IRetainedStore = (*RetainedStore)(nil)

// RetainedStore holds the last retained message per topic (§3.3.1.3).
//
// A flat map keyed by exact topic, not a trie. The asymmetry with the
// subscription index is deliberate and comes from the access pattern: retained
// messages are *written* by exact topic on every retained publish, and *read*
// by filter only when a client subscribes. Writes are frequent and reads are
// rare, so the structure is chosen to make writes O(1) and accepts a linear
// scan on the rare read.
//
// The scan is the known scaling limit and is called out here rather than
// discovered later: a deployment with a very large number of retained topics
// and a high subscribe rate would want this indexed too.
type RetainedStore struct {
	mu       sync.RWMutex
	messages map[msgVO.TopicName]*msgAgg.Message
}

// NewRetainedStore builds an empty store.
func NewRetainedStore() *RetainedStore {
	return &RetainedStore{messages: make(map[msgVO.TopicName]*msgAgg.Message)}
}

// Store sets the retained message for a topic.
//
// It always replaces: a topic has exactly one retained message, the most recent
// one (§3.3.1.3). Accumulating them would make a subscriber receive a topic's
// entire history on subscribe rather than its current state.
func (s *RetainedStore) Store(topic msgVO.TopicName, message *msgAgg.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[topic] = message
}

// Remove clears the retained message for a topic.
func (s *RetainedStore) Remove(topic msgVO.TopicName) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.messages, topic)
}

// Matching returns every retained message whose topic matches a filter.
//
// This runs when a client subscribes. It uses TopicFilter.Matches — the spec's
// own definition — rather than a second index, so there is no possibility of
// retained delivery and live delivery disagreeing about what a filter selects.
func (s *RetainedStore) Matching(filter msgVO.TopicFilter) []*msgAgg.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []*msgAgg.Message
	for topic, message := range s.messages {
		if filter.Matches(topic) {
			matched = append(matched, message)
		}
	}
	return matched
}

// Count returns how many topics have a retained message.
func (s *RetainedStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.messages)
}

// All returns every retained message, for the admin API.
func (s *RetainedStore) All() []*msgAgg.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*msgAgg.Message, 0, len(s.messages))
	for _, message := range s.messages {
		out = append(out, message)
	}
	return out
}
