package value_objects

import (
	"strings"

	"github.com/anashasan/gomqtt/pkg/common/errors"
	msgErr "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/error"
)

// Topic constraints (§4.7).
const (
	// TopicLevelSeparator divides a topic into levels.
	TopicLevelSeparator = "/"
	// SingleLevelWildcard matches exactly one level.
	SingleLevelWildcard = "+"
	// MultiLevelWildcard matches the remainder of a topic.
	MultiLevelWildcard = "#"
	// SystemTopicPrefix marks a broker-internal topic. Wildcards must not match
	// one (§4.7.2).
	SystemTopicPrefix = "$"
	// MaxTopicLength is the spec's limit: topics are length-prefixed by a
	// two-byte integer.
	MaxTopicLength = 65535
	// MaxTopicLevels bounds nesting. The spec sets no limit, but an unbounded
	// one lets a client grow the subscription trie without bound, so the broker
	// picks a number that is far beyond any sane hierarchy.
	MaxTopicLevels = 128
)

// TopicName is the topic a message is published to.
//
// Distinct from TopicFilter on purpose. A name identifies one destination and
// may never contain a wildcard; a filter selects a set and may. Conflating them
// into one string type is how a broker ends up letting a client publish to "#"
// and deliver to every subscriber at once.
type TopicName string

// NewTopicName validates and constructs a TopicName.
func NewTopicName(raw string) (TopicName, error) {
	if raw == "" {
		return "", errors.Invalid(msgErr.EInvalidTopicName, "topic name must not be empty")
	}
	if len(raw) > MaxTopicLength {
		return "", errors.Invalid(msgErr.EInvalidTopicName, "topic name must not exceed 65535 bytes")
	}
	// §4.7.0.1 and §4.7.0.2: wildcards belong to filters, never to names.
	if strings.ContainsAny(raw, SingleLevelWildcard+MultiLevelWildcard) {
		return "", errors.Invalid(
			msgErr.EInvalidTopicName,
			"topic name must not contain the wildcard characters + or #",
		)
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.Invalid(msgErr.EInvalidTopicName, "topic name must not contain a null character")
	}
	if strings.Count(raw, TopicLevelSeparator)+1 > MaxTopicLevels {
		return "", errors.Invalid(msgErr.EInvalidTopicName, "topic name has too many levels")
	}
	return TopicName(raw), nil
}

// MustNewTopicName constructs a TopicName and panics on failure. It is intended
// for package-level constants and tests, never for parsing client input.
func MustNewTopicName(raw string) TopicName {
	name, err := NewTopicName(raw)
	if err != nil {
		panic(err)
	}
	return name
}

// String renders the topic name.
func (t TopicName) String() string { return string(t) }

// Levels splits the topic into its levels.
//
// An empty level is legal: "a//b" has three levels, the middle one empty, and
// "+" matches it. Trimming empties here would silently change what a filter
// matches.
func (t TopicName) Levels() []string {
	return strings.Split(string(t), TopicLevelSeparator)
}

// IsSystem reports whether the topic is broker-internal, that is, whether its
// first level begins with '$'.
func (t TopicName) IsSystem() bool {
	return strings.HasPrefix(string(t), SystemTopicPrefix)
}
