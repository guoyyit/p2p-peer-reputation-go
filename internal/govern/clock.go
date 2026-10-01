package govern

import "time"

// Clock supplies time as unix millis so scoring stays deterministic under test.
type Clock interface {
	Now() int64
}

// RealClock is the production wall clock.
type RealClock struct{}

func (RealClock) Now() int64 { return time.Now().UnixMilli() }

// ManualClock is a test clock advanced explicitly by the caller.
type ManualClock struct {
	now int64
}

func NewManualClock(start int64) *ManualClock { return &ManualClock{now: start} }

func (m *ManualClock) Now() int64 { return m.now }

func (m *ManualClock) Set(t int64) { m.now = t }

func (m *ManualClock) Advance(d time.Duration) { m.now += d.Milliseconds() }
