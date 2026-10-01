package govern

import "math"

// ScoreEntry is the persisted scoring state of one (peer, topic) pair.
type ScoreEntry struct {
	Score      float64
	LastUpdate int64 // unix millis of the last applied event
}

// ScoreTable keeps per-(peer, topic) scores with exponential time decay.
type ScoreTable struct {
	entries map[PeerID]map[Topic]*ScoreEntry
}

func NewScoreTable() *ScoreTable {
	return &ScoreTable{entries: make(map[PeerID]map[Topic]*ScoreEntry)}
}

// Apply decays the current score to `now`, adds weight, clamps, and stores.
func (t *ScoreTable) Apply(peer PeerID, topic Topic, weight float64, now int64, p Params) float64 {
	entry := t.entry(peer, topic)
	score := decay(entry.Score, entry.LastUpdate, now, p)
	score += weight
	score = clamp(score, p.MinScore, p.MaxScore)
	entry.Score = score
	entry.LastUpdate = now
	return score
}

// Get returns the decayed score at `now` without mutating state.
func (t *ScoreTable) Get(peer PeerID, topic Topic, now int64, p Params) float64 {
	topics, ok := t.entries[peer]
	if !ok {
		return 0
	}
	entry, ok := topics[topic]
	if !ok {
		return 0
	}
	return decay(entry.Score, entry.LastUpdate, now, p)
}

// Snapshot exports a deep copy of the table for persistence.
func (t *ScoreTable) Snapshot() map[PeerID]map[Topic]ScoreEntry {
	out := make(map[PeerID]map[Topic]ScoreEntry, len(t.entries))
	for peer, topics := range t.entries {
		cp := make(map[Topic]ScoreEntry, len(topics))
		for topic, entry := range topics {
			cp[topic] = *entry
		}
		out[peer] = cp
	}
	return out
}

// Restore loads a previously exported snapshot.
func (t *ScoreTable) Restore(data map[PeerID]map[Topic]ScoreEntry) {
	t.entries = make(map[PeerID]map[Topic]*ScoreEntry, len(data))
	for peer, topics := range data {
		cp := make(map[Topic]*ScoreEntry, len(topics))
		for topic, entry := range topics {
			e := entry
			cp[topic] = &e
		}
		t.entries[peer] = cp
	}
}

func (t *ScoreTable) entry(peer PeerID, topic Topic) *ScoreEntry {
	topics, ok := t.entries[peer]
	if !ok {
		topics = make(map[Topic]*ScoreEntry)
		t.entries[peer] = topics
	}
	entry, ok := topics[topic]
	if !ok {
		entry = &ScoreEntry{}
		topics[topic] = entry
	}
	return entry
}

// decay halves the score every HalfLife. Deterministic for integer-millis
// inputs; a non-positive half-life disables decay.
func decay(score float64, last, now int64, p Params) float64 {
	halfLife := p.HalfLife.Milliseconds()
	if halfLife <= 0 || now <= last || score == 0 {
		return score
	}
	return score * math.Pow(0.5, float64(now-last)/float64(halfLife))
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
