// Package gov enforces connection quotas, rate limits and flood protection.
//
// All decisions are functions of (node, topic, injected time), so replaying
// the same event stream produces identical decisions.
package gov

import (
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

// Config holds governance limits.
type Config struct {
	// MaxConnsPerNode caps concurrent connections from one identity.
	MaxConnsPerNode int
	// RateTokens is the per-window message allowance per (node, topic).
	RateTokens int
	// RateWindowMillis is the token-bucket refill window.
	RateWindowMillis int64
	// FloodWindowMillis is the sliding window for flood detection.
	FloodWindowMillis int64
	// FloodBadThreshold triggers flood protection when the number of bad
	// (invalid/duplicate/excess) messages from one node on one topic within
	// FloodWindowMillis reaches this value.
	FloodBadThreshold int
}

// DefaultConfig returns default governance limits.
func DefaultConfig() Config {
	return Config{
		MaxConnsPerNode:   2,
		RateTokens:        20,
		RateWindowMillis:  1000,
		FloodWindowMillis: 2000,
		FloodBadThreshold: 5,
	}
}

// Counter is an observable governance counter for one (node, topic).
type Counter struct {
	Accepted     int64
	RateLimited  int64
	Bad          int64
	FloodTripped int64
	WindowBad    int
	FloodEngaged bool
}

// tokenBucket is a simple fixed-window integer token bucket.
type tokenBucket struct {
	windowStart int64
	tokens      int
}

// badStamp records one bad-message timestamp for flood-window accounting.
type pair struct {
	node  protocol.NodeID
	topic protocol.Topic
}

type rateState struct {
	bucket tokenBucket
}

// Governor enforces per-node quotas, rate limits and flood isolation.
type Governor struct {
	cfg   Config
	mu    sync.Mutex
	conns map[protocol.NodeID]int
	rate  map[pair]*rateState
	bad   map[pair][]int64
	stat  map[pair]*Counter
}

// NewGovernor constructs a governor.
func NewGovernor(cfg Config) *Governor {
	return &Governor{
		cfg:   cfg,
		conns: make(map[protocol.NodeID]int),
		rate:  make(map[pair]*rateState),
		bad:   make(map[pair][]int64),
		stat:  make(map[pair]*Counter),
	}
}

// AcquireConn applies the connection-quota policy for a node. It returns
// false when the node already holds MaxConnsPerNode connections.
func (g *Governor) AcquireConn(node protocol.NodeID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conns[node] >= g.cfg.MaxConnsPerNode {
		return false
	}
	g.conns[node]++
	return true
}

// ReleaseConn releases one connection slot.
func (g *Governor) ReleaseConn(node protocol.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conns[node] > 0 {
		g.conns[node]--
		if g.conns[node] == 0 {
			delete(g.conns, node)
		}
	}
}

// AllowRate applies the per-(node, topic) rate limiter. Rejected messages
// are counted and are not forwarded.
func (g *Governor) AllowRate(node protocol.NodeID, topic protocol.Topic, now int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := pair{node, topic}
	st := g.rate[p]
	if st == nil {
		st = &rateState{bucket: tokenBucket{windowStart: now, tokens: g.cfg.RateTokens}}
		g.rate[p] = st
	}
	b := &st.bucket
	if now-b.windowStart >= g.cfg.RateWindowMillis {
		b.windowStart = now
		b.tokens = g.cfg.RateTokens
	}
	c := g.statForLocked(p)
	if b.tokens <= 0 {
		c.RateLimited++
		return false
	}
	b.tokens--
	c.Accepted++
	return true
}

// RecordBad records an invalid/duplicate/excess message and evaluates flood
// protection over the configured sliding window. It returns true once flood
// protection becomes engaged for this (node, topic); subsequent bad messages
// keep returning true while engaged. Engagement is observable via Stats.
func (g *Governor) RecordBad(node protocol.NodeID, topic protocol.Topic, now int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := pair{node, topic}
	c := g.statForLocked(p)

	cutoff := now - g.cfg.FloodWindowMillis
	stamps := g.bad[p]
	kept := stamps[:0]
	for _, ts := range stamps {
		if ts >= cutoff {
			kept = append(kept, ts)
		}
	}
	kept = append(kept, now)
	g.bad[p] = kept

	c.Bad++
	c.WindowBad = len(kept)
	if c.FloodEngaged {
		return true
	}
	if len(kept) >= g.cfg.FloodBadThreshold {
		c.FloodEngaged = true
		c.FloodTripped++
		return true
	}
	return false
}

// IsFlooding reports whether flood protection is currently engaged.
func (g *Governor) IsFlooding(node protocol.NodeID, topic protocol.Topic) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.stat[pair{node, topic}]; c != nil {
		return c.FloodEngaged
	}
	return false
}

// ClearFlood disengages flood protection for a (node, topic), e.g. after a
// ban expires and the node enters probation.
func (g *Governor) ClearFlood(node protocol.NodeID, topic protocol.Topic) {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := pair{node, topic}
	if c := g.stat[p]; c != nil {
		c.FloodEngaged = false
		c.WindowBad = 0
	}
	g.bad[p] = nil
}

// Stats returns a point-in-time copy of observable counters for a pair.
func (g *Governor) Stats(node protocol.NodeID, topic protocol.Topic) Counter {
	g.mu.Lock()
	defer g.mu.Unlock()
	return *g.statForLocked(pair{node, topic})
}

func (g *Governor) statForLocked(p pair) *Counter {
	c := g.stat[p]
	if c == nil {
		c = &Counter{}
		g.stat[p] = c
	}
	return c
}
