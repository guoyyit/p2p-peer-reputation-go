// Package scoring implements per-node, per-topic reputation scoring.
package scoring

import (
	"sort"
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

// Outcome classifies how a received message was judged.
type Outcome int

const (
	OutcomeValidTimely Outcome = iota
	OutcomeValidLate
	OutcomeInvalid
	OutcomeDuplicate
	OutcomeExcess
)

// Config holds scoring model parameters.
type Config struct {
	// StartScore is assigned to a previously unseen (node, topic) pair.
	StartScore int64
	// Score bounds keep long-running scores finite.
	MinScore int64
	MaxScore int64
	// Score deltas applied per judged message. Penalties are positive
	// magnitudes and are subtracted.
	ValidReward       int64
	LateReward        int64
	InvalidPenalty    int64
	DuplicatePenalty  int64
	ExcessPenalty     int64
	LowScoreThreshold int64
	// Decay moves the score toward zero by DecayStep every
	// DecayIntervalMillis of elapsed time. Integer arithmetic is used so
	// decay is identical across machines and restarts.
	DecayIntervalMillis int64
	DecayStep           int64
}

// DefaultConfig returns the deterministic default scoring parameters.
func DefaultConfig() Config {
	return Config{
		StartScore:          50,
		MinScore:            -1000,
		MaxScore:            100,
		ValidReward:         10,
		LateReward:          2,
		InvalidPenalty:      25,
		DuplicatePenalty:    10,
		ExcessPenalty:       5,
		LowScoreThreshold:   -1,
		DecayIntervalMillis: 1000,
		DecayStep:           5,
	}
}

type key struct {
	node  protocol.NodeID
	topic protocol.Topic
}

type entry struct {
	Score     int64
	UpdatedAt int64
}

// Board maintains reputation scores indexed by node and topic.
type Board struct {
	cfg     Config
	mu      sync.Mutex
	entries map[key]*entry
}

// NewBoard constructs an empty score board.
func NewBoard(cfg Config) *Board {
	return &Board{cfg: cfg, entries: make(map[key]*entry)}
}

// Apply records one judged message and returns the resulting score.
func (b *Board) Apply(node protocol.NodeID, topic protocol.Topic, outcome Outcome, now int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{node, topic}
	e := b.entries[k]
	if e == nil {
		e = &entry{Score: b.cfg.StartScore, UpdatedAt: now}
		b.entries[k] = e
	}
	b.decayEntry(e, now)
	e.Score += delta(b.cfg, outcome)
	if e.Score < b.cfg.MinScore {
		e.Score = b.cfg.MinScore
	}
	if e.Score > b.cfg.MaxScore {
		e.Score = b.cfg.MaxScore
	}
	e.UpdatedAt = now
	return e.Score
}

// Score returns the current (decayed) score for a node on a topic.
func (b *Board) Score(node protocol.NodeID, topic protocol.Topic, now int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.entries[key{node, topic}]
	if e == nil {
		return b.cfg.StartScore
	}
	b.decayEntry(e, now)
	return e.Score
}

// Decay applies time-based score decay up to now.
func (b *Board) Decay(now int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range b.entries {
		b.decayEntry(e, now)
	}
}

// IsLow reports whether a score has fallen below the low-score threshold.
func (b *Board) IsLow(score int64) bool { return score < b.cfg.LowScoreThreshold }

// decayEntry moves the score toward zero in fixed integer steps. Caller
// holds b.mu.
func (b *Board) decayEntry(e *entry, now int64) {
	if b.cfg.DecayIntervalMillis <= 0 || b.cfg.DecayStep <= 0 || now <= e.UpdatedAt {
		if now > e.UpdatedAt {
			e.UpdatedAt = now
		}
		return
	}
	elapsed := now - e.UpdatedAt
	steps := elapsed / b.cfg.DecayIntervalMillis
	if steps <= 0 {
		return
	}
	amount := steps * b.cfg.DecayStep
	switch {
	case e.Score > 0:
		e.Score -= min64(amount, e.Score)
	case e.Score < 0:
		e.Score += min64(amount, -e.Score)
	}
	e.UpdatedAt += steps * b.cfg.DecayIntervalMillis
}

func delta(cfg Config, outcome Outcome) int64 {
	switch outcome {
	case OutcomeValidTimely:
		return cfg.ValidReward
	case OutcomeValidLate:
		return cfg.LateReward
	case OutcomeInvalid:
		return -cfg.InvalidPenalty
	case OutcomeDuplicate:
		return -cfg.DuplicatePenalty
	case OutcomeExcess:
		return -cfg.ExcessPenalty
	default:
		return 0
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// EntryState is the persisted form of one (node, topic) score.
type EntryState struct {
	Node      string `json:"node"`
	Topic     string `json:"topic"`
	Score     int64  `json:"score"`
	UpdatedAt int64  `json:"updated_at"`
}

// Snapshot is a deterministic, JSON-serializable copy of board state.
type Snapshot struct {
	Entries []EntryState `json:"entries"`
}

// SnapshotState returns all current scores sorted for stable serialization.
func (b *Board) SnapshotState() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	snap := Snapshot{Entries: make([]EntryState, 0, len(b.entries))}
	for k, e := range b.entries {
		snap.Entries = append(snap.Entries, EntryState{
			Node:      string(k.node),
			Topic:     string(k.topic),
			Score:     e.Score,
			UpdatedAt: e.UpdatedAt,
		})
	}
	sort.Slice(snap.Entries, func(i, j int) bool {
		if snap.Entries[i].Node != snap.Entries[j].Node {
			return snap.Entries[i].Node < snap.Entries[j].Node
		}
		return snap.Entries[i].Topic < snap.Entries[j].Topic
	})
	return snap
}

// RestoreState replaces board contents from a snapshot.
func (b *Board) RestoreState(snap Snapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = make(map[key]*entry, len(snap.Entries))
	for _, s := range snap.Entries {
		b.entries[key{protocol.NodeID(s.Node), protocol.Topic(s.Topic)}] = &entry{
			Score:     s.Score,
			UpdatedAt: s.UpdatedAt,
		}
	}
}
