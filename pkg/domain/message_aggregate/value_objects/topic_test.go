package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vo "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
)

func TestNewTopicName_RejectsInvalidNames(t *testing.T) {
	invalid := map[string]string{
		"empty":                 "",
		"single-level wildcard": "a/+/c",
		"multi-level wildcard":  "a/#",
		"bare plus":             "+",
		"bare hash":             "#",
		"embedded null":         "a\x00b",
	}

	for name, topic := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := vo.NewTopicName(topic)
			assert.Error(t, err)
		})
	}
}

func TestNewTopicName_AcceptsValidNames(t *testing.T) {
	// Empty levels are legal and meaningful: "a//b" has three levels, and "+"
	// matches the empty one. A validator that trimmed them would silently
	// change what subscribers receive.
	valid := []string{"a", "a/b/c", "a//b", "/", "/a", "a/", "$SYS/broker/uptime", " "}

	for _, topic := range valid {
		_, err := vo.NewTopicName(topic)
		assert.NoError(t, err, "topic %q must be accepted", topic)
	}
}

func TestNewTopicFilter_RejectsMisplacedWildcards(t *testing.T) {
	invalid := map[string]string{
		"empty":                    "",
		"plus inside a level":      "sport+",
		"plus glued to a level":    "sport/tennis+",
		"hash inside a level":      "sport#",
		"hash not last":            "sport/#/ranking",
		"hash glued to a level":    "sport/tennis#",
		"two wildcards in a level": "+#",
		"embedded null":            "a\x00b",
	}

	for name, filter := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := vo.NewTopicFilter(filter)
			assert.Error(t, err)
		})
	}
}

func TestNewTopicFilter_AcceptsValidFilters(t *testing.T) {
	valid := []string{
		"a", "a/b", "#", "+", "+/+", "sport/#", "sport/+/player1",
		"+/tennis/#", "/+", "a//b", "$SYS/#",
	}

	for _, filter := range valid {
		_, err := vo.NewTopicFilter(filter)
		assert.NoError(t, err, "filter %q must be accepted", filter)
	}
}

// TestTopicFilter_Matches is the spec table, transcribed.
//
// Most cases come straight from the non-normative examples in §4.7 of the
// MQTT 3.1.1 specification, so a disagreement here is a disagreement with the
// standard rather than with someone's opinion about it.
func TestTopicFilter_Matches(t *testing.T) {
	cases := []struct {
		filter string
		topic  string
		want   bool
		why    string
	}{
		// Exact matches.
		{"sport/tennis/player1", "sport/tennis/player1", true, "identical"},
		{"sport/tennis/player1", "sport/tennis/player2", false, "different leaf"},
		{"sport", "sport/tennis", false, "filter shorter than topic"},
		{"sport/tennis", "sport", false, "filter longer than topic"},

		// Multi-level wildcard (§4.7.1.2).
		{"sport/tennis/player1/#", "sport/tennis/player1", true, "# matches the parent level"},
		{"sport/tennis/player1/#", "sport/tennis/player1/ranking", true, "# matches one child"},
		{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true, "# matches many children"},
		{"sport/#", "sport", true, "# matches its own parent"},
		{"#", "a/b/c", true, "# alone matches everything"},
		{"#", "a", true, "# alone matches a single level"},
		{"sport/#", "sports/tennis", false, "parent level must still match exactly"},

		// Single-level wildcard (§4.7.1.3).
		{"sport/tennis/+", "sport/tennis/player1", true, "+ matches one level"},
		{"sport/tennis/+", "sport/tennis/player1/ranking", false, "+ matches exactly one level"},
		{"sport/+", "sport", false, "+ requires a level to be present"},
		{"sport/+", "sport/", true, "+ matches an empty level"},
		{"+/+", "/finance", true, "+ matches a leading empty level"},
		{"/+", "/finance", true, "leading separator is its own empty level"},
		{"+", "/finance", false, "a single + cannot match two levels"},
		{"+/tennis/#", "sport/tennis/player1", true, "+ and # combined"},

		// Empty levels.
		{"a/+/b", "a//b", true, "+ matches an empty middle level"},
		{"a/b", "a//b", false, "an empty level is a real level"},

		// System topics (§4.7.2): a wildcard at the start must not match '$'.
		{"#", "$SYS/broker/uptime", false, "# must not match a $ topic"},
		{"+/broker/uptime", "$SYS/broker/uptime", false, "+ must not match a $ first level"},
		{"$SYS/#", "$SYS/broker/uptime", true, "an explicit $SYS prefix does match"},
		{"$SYS/broker/+", "$SYS/broker/uptime", true, "wildcards below $SYS are fine"},
		{"#", "a$b/c", true, "$ only matters at the very start of the topic"},
	}

	for _, tc := range cases {
		t.Run(tc.filter+" vs "+tc.topic, func(t *testing.T) {
			filter, err := vo.NewTopicFilter(tc.filter)
			require.NoError(t, err)
			topic, err := vo.NewTopicName(tc.topic)
			require.NoError(t, err)

			assert.Equal(t, tc.want, filter.Matches(topic), tc.why)
		})
	}
}

func TestQoS_DowngradeTakesTheMinimumAndRespectsTheCeiling(t *testing.T) {
	// §4.3: the effective QoS of a delivery is the lower of the publication's
	// and the subscription's, and this broker additionally caps at 1.
	assert.Equal(t, vo.QoSAtMostOnce, vo.QoSAtLeastOnce.Downgrade(vo.QoSAtMostOnce))
	assert.Equal(t, vo.QoSAtMostOnce, vo.QoSAtMostOnce.Downgrade(vo.QoSAtLeastOnce))
	assert.Equal(t, vo.QoSAtLeastOnce, vo.QoSAtLeastOnce.Downgrade(vo.QoSAtLeastOnce))
	assert.Equal(t, vo.QoSAtLeastOnce, vo.QoSExactlyOnce.Downgrade(vo.QoSExactlyOnce))
}

func TestQoS_Validation(t *testing.T) {
	for _, level := range []byte{0, 1, 2} {
		_, err := vo.NewQoS(level)
		assert.NoError(t, err)
	}
	_, err := vo.NewQoS(3)
	assert.Error(t, err)

	assert.True(t, vo.QoSAtMostOnce.IsSupported())
	assert.True(t, vo.QoSAtLeastOnce.IsSupported())
	assert.False(t, vo.QoSExactlyOnce.IsSupported())

	assert.False(t, vo.QoSAtMostOnce.RequiresAcknowledgement())
	assert.True(t, vo.QoSAtLeastOnce.RequiresAcknowledgement())
}
