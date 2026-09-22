package value_objects

import (
	"strings"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	msgErr "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/error"
)

// TopicFilter is the pattern a subscription registers.
//
// This type is the *specification* of topic matching. The trie in the
// infrastructure layer is an index that must agree with it, and a property test
// asserts the two never disagree over thousands of generated cases. Keeping the
// readable definition and the fast index as separate artefacts — one obviously
// correct, one obviously fast — is what makes it safe to optimise the index
// later.
type TopicFilter string

// NewTopicFilter validates and constructs a TopicFilter (§4.7.1).
func NewTopicFilter(raw string) (TopicFilter, error) {
	if raw == "" {
		return "", errors.Invalid(msgErr.EInvalidTopicFilter, "topic filter must not be empty")
	}
	if len(raw) > MaxTopicLength {
		return "", errors.Invalid(msgErr.EInvalidTopicFilter, "topic filter must not exceed 65535 bytes")
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.Invalid(msgErr.EInvalidTopicFilter, "topic filter must not contain a null character")
	}

	levels := strings.Split(raw, TopicLevelSeparator)
	if len(levels) > MaxTopicLevels {
		return "", errors.Invalid(msgErr.EInvalidTopicFilter, "topic filter has too many levels")
	}

	for i, level := range levels {
		switch {
		case level == SingleLevelWildcard, level == MultiLevelWildcard:
			// A wildcard occupying a whole level is the only legal form.
			// '#' additionally has to be last, checked below.

		case strings.Contains(level, SingleLevelWildcard),
			strings.Contains(level, MultiLevelWildcard):
			// §4.7.1.2 and §4.7.1.3: a wildcard must occupy an entire level.
			// "sport+" and "sport/tennis#" are both illegal. Accepting them
			// would mean inventing matching semantics the spec does not define,
			// and two brokers would then disagree about what a client asked for.
			return "", errors.Invalid(
				msgErr.EInvalidTopicFilter,
				"a wildcard must occupy an entire topic level",
			)
		}

		// §4.7.1.2: '#' must be the last level.
		if level == MultiLevelWildcard && i != len(levels)-1 {
			return "", errors.Invalid(
				msgErr.EInvalidTopicFilter,
				"the multi-level wildcard # must be the last level of a filter",
			)
		}
	}

	return TopicFilter(raw), nil
}

// MustNewTopicFilter constructs a TopicFilter and panics on failure. Intended
// for constants and tests only.
func MustNewTopicFilter(raw string) TopicFilter {
	filter, err := NewTopicFilter(raw)
	if err != nil {
		panic(err)
	}
	return filter
}

// String renders the filter.
func (f TopicFilter) String() string { return string(f) }

// Levels splits the filter into its levels.
func (f TopicFilter) Levels() []string {
	return strings.Split(string(f), TopicLevelSeparator)
}

// HasWildcard reports whether the filter contains any wildcard.
func (f TopicFilter) HasWildcard() bool {
	return strings.ContainsAny(string(f), SingleLevelWildcard+MultiLevelWildcard)
}

// Matches reports whether the filter selects the given topic name.
//
// This is the reference implementation of §4.7: direct, level by level, chosen
// for readability over speed because its job is to be *obviously* right. The
// broker does not call it on the delivery path — the trie does that — but every
// trie result is checked against it in tests.
func (f TopicFilter) Matches(topic TopicName) bool {
	filterLevels := f.Levels()
	topicLevels := topic.Levels()

	// §4.7.2: a wildcard at the start of a filter must not match a topic whose
	// first level begins with '$'. This is what stops a client subscribing to
	// "#" and receiving the broker's own internal traffic.
	if topic.IsSystem() {
		first := filterLevels[0]
		if first == SingleLevelWildcard || first == MultiLevelWildcard {
			return false
		}
	}

	for i, filterLevel := range filterLevels {
		if filterLevel == MultiLevelWildcard {
			// '#' matches the parent level and any number of child levels, so
			// "sport/#" matches "sport" as well as "sport/tennis" (§4.7.1.2).
			// It is the last level by construction, so this is the final word.
			return true
		}

		if i >= len(topicLevels) {
			// The filter is longer than the topic and did not end in '#'.
			return false
		}

		if filterLevel == SingleLevelWildcard {
			// '+' matches exactly one level, including an empty one.
			continue
		}

		if filterLevel != topicLevels[i] {
			return false
		}
	}

	// Every filter level matched; the topic must not have levels left over.
	return len(filterLevels) == len(topicLevels)
}
