package gov

import "testing"

func TestConnQuota(t *testing.T) {
	g := NewGovernor(Config{MaxConnsPerNode: 2, RateTokens: 5, RateWindowMillis: 1000, FloodWindowMillis: 1000, FloodBadThreshold: 5})
	if !g.AcquireConn("A") {
		t.Fatal("first conn allowed")
	}
	if !g.AcquireConn("A") {
		t.Fatal("second conn allowed")
	}
	if g.AcquireConn("A") {
		t.Fatal("third conn must be rejected")
	}
	if !g.AcquireConn("B") {
		t.Fatal("other node unaffected by A quota")
	}
	g.ReleaseConn("A")
	if !g.AcquireConn("A") {
		t.Fatal("conn allowed after release")
	}
}

func TestRateLimitWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RateTokens = 3
	g := NewGovernor(cfg)
	for i := 0; i < 3; i++ {
		if !g.AllowRate("A", "tx", 0) {
			t.Fatalf("msg %d should be allowed", i)
		}
	}
	if g.AllowRate("A", "tx", 500) {
		t.Fatal("4th msg within window must be limited")
	}
	// Other topic unaffected.
	if !g.AllowRate("A", "blocks", 500) {
		t.Fatal("rate limit must be topic-scoped")
	}
	// Window refills.
	if !g.AllowRate("A", "tx", 1000) {
		t.Fatal("tokens must refill after window")
	}
	if got := g.Stats("A", "tx").RateLimited; got != 1 {
		t.Fatalf("rateLimited counter = %d, want 1", got)
	}
}

func TestFloodProtection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FloodBadThreshold = 3
	cfg.FloodWindowMillis = 1000
	g := NewGovernor(cfg)

	if g.RecordBad("A", "tx", 0) {
		t.Fatal("flood must not trip on first bad message")
	}
	g.RecordBad("A", "tx", 100)
	if !g.RecordBad("A", "tx", 200) {
		t.Fatal("flood must trip at threshold")
	}
	if !g.IsFlooding("A", "tx") {
		t.Fatal("flood engaged flag")
	}
	s := g.Stats("A", "tx")
	if s.FloodTripped != 1 || s.WindowBad != 3 {
		t.Fatalf("stats = %+v", s)
	}
	// Other topic not engaged.
	if g.IsFlooding("A", "blocks") {
		t.Fatal("flood must be topic-scoped")
	}
	// Old bad messages age out of the sliding window (but engagement stays
	// until explicitly cleared).
	g.ClearFlood("A", "tx")
	if g.IsFlooding("A", "tx") {
		t.Fatal("flood must disengage after clear")
	}
}

func TestFloodWindowExpiresBeforeThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FloodBadThreshold = 3
	cfg.FloodWindowMillis = 1000
	g := NewGovernor(cfg)
	g.RecordBad("A", "tx", 0)
	g.RecordBad("A", "tx", 100)
	// At 1500 the first two stamps (0,100) are outside the window.
	if g.RecordBad("A", "tx", 1500) {
		t.Fatal("aged-out bad messages must not count toward flood")
	}
	if got := g.Stats("A", "tx").WindowBad; got != 1 {
		t.Fatalf("window bad = %d, want 1", got)
	}
}
