package govern

import (
	"errors"
	"fmt"
)

// ErrBanned is returned when a peer attempts to relay while banned.
var ErrBanned = errors.New("govern: peer is banned on this topic")

// Engine combines scoring, banning, dedup, and persistence into one
// deterministic event processor. It is not safe for concurrent use; callers
// must serialize access (the p2p service does so with a mutex).
type Engine struct {
	params Params
	clock  Clock
	scores *ScoreTable
	bans   *BanTracker
	store  *FileStore
	seen   map[string]int64 // msgID -> expiry unix millis
	seq    uint64
	dirty  int // events since last snapshot
}

// NewEngine creates an engine; call Recover before use.
func NewEngine(params Params, store *FileStore, clock Clock) *Engine {
	return &Engine{
		params: params,
		clock:  clock,
		scores: NewScoreTable(),
		bans:   NewBanTracker(),
		store:  store,
		seen:   make(map[string]int64),
	}
}

// Recover loads the latest snapshot and replays the WAL on top of it.
func (e *Engine) Recover() error {
	snap, err := e.store.LoadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil {
		e.scores.Restore(snap.Scores)
		e.bans.Restore(snap.Bans)
		if snap.Seen != nil {
			e.seen = snap.Seen
		}
		e.seq = snap.Seq
	}
	return e.store.Replay(e.seq, func(ev Event) error {
		if _, err := e.applyEvent(ev, false); err != nil {
			return err
		}
		if ev.Seq > e.seq {
			e.seq = ev.Seq
		}
		return nil
	})
}

// HandleMessage scores one inbound message. The caller supplies a verdict of
// VerdictValid or VerdictInvalid; the engine itself detects duplicates via its
// persisted dedup cache, so recovery replays never double-count a message.
func (e *Engine) HandleMessage(peer PeerID, msg Message, verdict Verdict) (Decision, error) {
	ev := Event{
		Kind:         KindMessage,
		Peer:         peer,
		Topic:        msg.Topic,
		MsgID:        msg.ID,
		Verdict:      verdict,
		Timestamp:    e.clock.Now(),
		MsgTimestamp: msg.Timestamp,
	}
	return e.applyEvent(ev, true)
}

// applyEvent is the single deterministic state-transition function. With
// persist=true the event is appended to the WAL before being applied, so a
// crash loses nothing that was acknowledged.
func (e *Engine) applyEvent(ev Event, persist bool) (Decision, error) {
	if persist {
		e.seq++
		ev.Seq = e.seq
		if err := e.store.Append(ev); err != nil {
			e.seq--
			return Decision{}, err
		}
		e.dirty++
	}
	var d Decision
	switch ev.Kind {
	case KindMessage:
		d = e.applyMessage(ev)
	default: // connect/disconnect are audit-only records
		d = Decision{Accepted: true}
	}
	// Snapshot only after the event is applied, so the snapshot at seq N
	// covers the effects of events 1..N and the WAL can be truncated safely.
	if persist && e.params.SnapshotEvery > 0 && e.dirty >= e.params.SnapshotEvery {
		if err := e.Snapshot(); err != nil {
			return Decision{}, err
		}
	}
	return d, nil
}

func (e *Engine) applyMessage(ev Event) Decision {
	now := ev.Timestamp
	status := e.bans.Status(ev.Peer, ev.Topic, now)
	if status == StatusBanned {
		return Decision{
			Accepted: false,
			Reason:   "banned",
			Score:    e.scores.Get(ev.Peer, ev.Topic, now, e.params),
			Action:   ActionBan,
		}
	}

	verdict := ev.Verdict
	if verdict == VerdictValid && ev.MsgID != "" {
		if e.isDuplicate(ev.MsgID, now) {
			verdict = VerdictDuplicate
		} else {
			e.remember(ev.MsgID, now)
		}
	}

	weight := e.weightFor(ev, verdict)
	if status == StatusObservation && weight < 0 {
		weight *= e.params.ObservationPenaltyFactor
	}
	score := e.scores.Apply(ev.Peer, ev.Topic, weight, now, e.params)

	d := Decision{
		Accepted: verdict == VerdictValid,
		Reason:   verdict.String(),
		Verdict:  verdict,
		Score:    score,
	}
	if score <= e.params.BanThreshold {
		rec := e.bans.Ban(ev.Peer, ev.Topic, now, e.params)
		d.Accepted = false
		d.Action = ActionBan
		d.BannedUntil = rec.Until
		d.Reason = "score below ban threshold"
	}
	return d
}

func (e *Engine) weightFor(ev Event, verdict Verdict) float64 {
	switch verdict {
	case VerdictValid:
		if ev.MsgTimestamp > 0 && ev.Timestamp-ev.MsgTimestamp > e.params.LateThreshold.Milliseconds() {
			return e.params.LatePenalty
		}
		return e.params.ValidWeight
	case VerdictInvalid:
		return e.params.InvalidWeight
	case VerdictDuplicate:
		return e.params.DuplicateWeight
	case VerdictExcess:
		return e.params.ExcessWeight
	default:
		return 0
	}
}

func (e *Engine) isDuplicate(id string, now int64) bool {
	exp, ok := e.seen[id]
	if !ok {
		return false
	}
	if now >= exp {
		delete(e.seen, id)
		return false
	}
	return true
}

func (e *Engine) remember(id string, now int64) {
	e.seen[id] = now + e.params.SeenTTL.Milliseconds()
}

func (e *Engine) pruneSeen(now int64) {
	for id, exp := range e.seen {
		if now >= exp {
			delete(e.seen, id)
		}
	}
}

// CanConnect reports whether a peer may open a connection. Bans are enforced
// per-topic at relay time, so any peer may connect subject to quotas.
func (e *Engine) CanConnect(peer PeerID) Decision {
	return Decision{Accepted: true}
}

// CanRelay reports whether the peer may currently relay on the topic.
func (e *Engine) CanRelay(peer PeerID, topic Topic) bool {
	return e.bans.Status(peer, topic, e.clock.Now()) != StatusBanned
}

// Score returns the decayed score for a (peer, topic) pair.
func (e *Engine) Score(peer PeerID, topic Topic) float64 {
	return e.scores.Get(peer, topic, e.clock.Now(), e.params)
}

// BanStatus returns the lifecycle status of a (peer, topic) pair.
func (e *Engine) BanStatus(peer PeerID, topic Topic) BanStatus {
	return e.bans.Status(peer, topic, e.clock.Now())
}

// OnConnect records a connection event for audit.
func (e *Engine) OnConnect(peer PeerID) error {
	_, err := e.applyEvent(Event{Kind: KindConnect, Peer: peer, Timestamp: e.clock.Now()}, true)
	return err
}

// OnDisconnect records a disconnection event for audit.
func (e *Engine) OnDisconnect(peer PeerID) error {
	_, err := e.applyEvent(Event{Kind: KindDisconnect, Peer: peer, Timestamp: e.clock.Now()}, true)
	return err
}

// Snapshot forces a persisted snapshot and truncates the WAL.
func (e *Engine) Snapshot() error {
	e.pruneSeen(e.clock.Now())
	seen := make(map[string]int64, len(e.seen))
	for id, exp := range e.seen {
		seen[id] = exp
	}
	snap := &Snapshot{
		Seq:    e.seq,
		Scores: e.scores.Snapshot(),
		Bans:   e.bans.Snapshot(),
		Seen:   seen,
	}
	if err := e.store.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("govern: snapshot: %w", err)
	}
	e.dirty = 0
	return nil
}

// Close snapshots state and closes the store.
func (e *Engine) Close() error {
	if err := e.Snapshot(); err != nil {
		return err
	}
	return e.store.Close()
}
