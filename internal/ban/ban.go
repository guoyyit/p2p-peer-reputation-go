// Package ban manages connection bans, expiry, probation and backoff.
package ban

import (
	"sort"
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

// Status is the current governance status of a node on a topic.
type Status int

const (
	StatusClear Status = iota
	StatusBanned
	StatusProbation
)

// Config holds ban policy parameters.
type Config struct {
	// BaseBanMillis is the duration of a first offense ban.
	BaseBanMillis int64
	// MaxBanMillis caps exponential backoff.
	MaxBanMillis int64
	// BackoffFactor multiplies the previous ban duration on reoffense,
	// using fixed integer doubling (factor of 2).
	// ProbationMillis is the observation window after a ban expires.
	ProbationMillis int64
}

// DefaultConfig returns default ban policy parameters.
func DefaultConfig() Config {
	return Config{
		BaseBanMillis:   5_000,
		MaxBanMillis:    60 * 60 * 1000,
		ProbationMillis: 10_000,
	}
}

type key struct {
	node  protocol.NodeID
	topic protocol.Topic
}

type state struct {
	Status    Status `json:"status"`
	Until     int64  `json:"until"`
	Offense   int    `json:"offense"`
	BanMillis int64  `json:"ban_millis"`
}

// Manager tracks ban state per node and topic.
type Manager struct {
	cfg Config
	mu  sync.Mutex
	m   map[key]*state
}

// NewManager constructs an empty ban manager.
func NewManager(cfg Config) *Manager {
	return &Manager{cfg: cfg, m: make(map[key]*state)}
}

// Ban moves a node into banned state on one topic. Repeated bans while
// banned or in probation double the duration (exponential backoff).
func (m *Manager) Ban(node protocol.NodeID, topic protocol.Topic, now int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key{node, topic}
	s := m.m[k]
	if s == nil {
		s = &state{}
		m.m[k] = s
	}
	// Resolve any ban/probation expiry at "now" before classifying.
	m.advance(s, now)
	// Reoffense within an active ban or probation escalates; a node that
	// completed probation cleanly restarts from the base duration.
	escalate := s.Status == StatusBanned || s.Status == StatusProbation
	s.Offense++
	duration := m.cfg.BaseBanMillis
	if escalate && s.BanMillis > 0 {
		duration = s.BanMillis * 2
	}
	if duration > m.cfg.MaxBanMillis {
		duration = m.cfg.MaxBanMillis
	}
	s.BanMillis = duration
	s.Status = StatusBanned
	s.Until = now + duration
	return duration
}

// Status reports the current status at time now.
func (m *Manager) Status(node protocol.NodeID, topic protocol.Topic, now int64) Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.m[key{node, topic}]
	if s == nil {
		return StatusClear
	}
	m.advance(s, now)
	return s.Status
}

// Tick advances time-based state transitions (ban expiry, probation).
func (m *Manager) Tick(now int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.m {
		m.advance(s, now)
	}
}

// advance must be called with m.mu held.
func (m *Manager) advance(s *state, now int64) {
	for {
		switch s.Status {
		case StatusBanned:
			if now < s.Until {
				return
			}
			s.Status = StatusProbation
			s.Until += m.cfg.ProbationMillis
		case StatusProbation:
			if now < s.Until {
				return
			}
			s.Status = StatusClear
			s.Offense = 0
			s.BanMillis = 0
			s.Until = 0
			return
		default:
			return
		}
	}
}

// EntryState is the persisted form of one ban record.
type EntryState struct {
	Node      string `json:"node"`
	Topic     string `json:"topic"`
	Status    int    `json:"status"`
	Until     int64  `json:"until"`
	Offense   int    `json:"offense"`
	BanMillis int64  `json:"ban_millis"`
}

// Snapshot is a deterministic, JSON-serializable copy of ban state.
type Snapshot struct {
	Entries []EntryState `json:"entries"`
}

// SnapshotState returns all records sorted for stable serialization.
func (m *Manager) SnapshotState() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := Snapshot{Entries: make([]EntryState, 0, len(m.m))}
	for k, s := range m.m {
		snap.Entries = append(snap.Entries, EntryState{
			Node:      string(k.node),
			Topic:     string(k.topic),
			Status:    int(s.Status),
			Until:     s.Until,
			Offense:   s.Offense,
			BanMillis: s.BanMillis,
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

// RestoreState replaces manager contents from a snapshot.
func (m *Manager) RestoreState(snap Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m = make(map[key]*state, len(snap.Entries))
	for _, e := range snap.Entries {
		m.m[key{protocol.NodeID(e.Node), protocol.Topic(e.Topic)}] = &state{
			Status:    Status(e.Status),
			Until:     e.Until,
			Offense:   e.Offense,
			BanMillis: e.BanMillis,
		}
	}
}
