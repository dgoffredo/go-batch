package batch

import (
	"fmt"
	"time"

	"github.com/jmhodges/clock" // mockable wrapper around time functions
)

// synchronizedClock extends the FakeClock type from github.com/jmhodges/clock
// to additionally support lockstep time increments. I encountered a race
// condition in batch's test driver, TODO.
type synchronizedClock struct {
	clock.FakeClock
	// Steps TODO
	Steps chan time.Duration
	// Notify TODO
	Notify chan struct{}
}

func newSynchronizedClock() *synchronizedClock {
	c := &synchronizedClock{}
	c.FakeClock = clock.NewFake()
	// The buffering here is important. These channels are used as asynchonous
	// signaling mechanisms, not as rendezvous points.
	c.Steps = make(chan time.Duration, 1)
	c.Notify = make(chan struct{}, 1)
	return c
}

// Now behaves like clock.FakeClock.Now, except that Now will also apply a
// pending Step if one exists.
func (c *synchronizedClock) Now() time.Time {
	now := c.FakeClock.Now()
	fmt.Println("time is now ", now)

	select {
	case duration := <-c.Steps:
		c.FakeClock.Add(duration)
		c.Notify <- struct{}{}
	default:
	}

	return now
}
