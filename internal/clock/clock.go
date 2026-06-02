package clock

import "time"

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

type FakeClock struct {
	CurrentTime time.Time
}

func (c *FakeClock) Now() time.Time          { return c.CurrentTime }
func (c *FakeClock) Set(t time.Time)         { c.CurrentTime = t }
func (c *FakeClock) Advance(d time.Duration) { c.CurrentTime = c.CurrentTime.Add(d) }
