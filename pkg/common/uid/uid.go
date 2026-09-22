// Package uid generates the opaque identifiers used for job IDs.
package uid

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Generator is the port used by the application layer to mint identifiers. It is
// an interface so that tests can substitute a deterministic sequence.
type Generator interface {
	New() string
}

// RandomGenerator produces 128 bit random identifiers.
type RandomGenerator struct{}

// NewRandomGenerator builds the production Generator.
func NewRandomGenerator() *RandomGenerator { return &RandomGenerator{} }

// New returns a 32 character lowercase hex identifier. crypto/rand.Read never
// returns a short read, so an error here means the OS entropy source is broken
// and the process cannot continue meaningfully.
func (RandomGenerator) New() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("uid: entropy source unavailable: %v", err))
	}
	return hex.EncodeToString(buf)
}

// SequenceGenerator emits predictable identifiers for tests.
type SequenceGenerator struct {
	prefix string
	n      int
}

// NewSequenceGenerator builds a deterministic Generator.
func NewSequenceGenerator(prefix string) *SequenceGenerator {
	return &SequenceGenerator{prefix: prefix}
}

// New returns the next identifier in the sequence.
func (s *SequenceGenerator) New() string {
	s.n++
	return fmt.Sprintf("%s-%d", s.prefix, s.n)
}
