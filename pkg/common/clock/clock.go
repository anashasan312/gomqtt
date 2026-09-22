// Package clock abstracts wall-clock time so that scheduling logic stays
// deterministic under test.
package clock

import "time"

// Clock is the port through which every layer reads the current time. Production
// code binds SystemClock; tests bind a Fake and drive time forward by hand.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the host wall clock.
type SystemClock struct{}

// NewSystemClock builds the production Clock implementation.
func NewSystemClock() *SystemClock { return &SystemClock{} }

// Now returns the current UTC time. UTC is enforced here so that no other layer
// has to remember to normalise it before writing a Redis score.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// Fake is a Clock whose time is advanced explicitly. It lives in the production
// tree rather than a _test file so that any package may use it in its own tests.
type Fake struct{ current time.Time }

// NewFake builds a Fake pinned to the supplied instant.
func NewFake(at time.Time) *Fake { return &Fake{current: at.UTC()} }

// Now returns the pinned instant.
func (f *Fake) Now() time.Time { return f.current }

// Advance moves the pinned instant forward by d.
func (f *Fake) Advance(d time.Duration) { f.current = f.current.Add(d) }

// Set repins the instant.
func (f *Fake) Set(at time.Time) { f.current = at.UTC() }
