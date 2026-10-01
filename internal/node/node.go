// Package node wires transport, scoring, bans, persistence and governance
// into a runnable P2P node.
package node

import (
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/ban"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/gov"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/scoring"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/store"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

// Validator judges payload validity and timeliness. It is supplied by the
// caller so the same event input always yields the same verdicts.
type Validator func(msg protocol.Message, receivedAt int64) (valid bool, timely bool)

// Config configures a node.
type Config struct {
	Score scoring.Config
	Ban   ban.Config
	Gov   gov.Config
	// LateMillis: a valid message arriving more than this many ms after its
	// CreatedAt is treated as late and earns a reduced reward.
	LateMillis     int64
	DedupTTLMillis int64
	// PersistEveryMillis bounds how often state is flushed to disk.
	PersistEveryMillis int64
}

// DefaultConfig returns sensible deterministic defaults.
func DefaultConfig() Config {
	return Config{
		Score:              scoring.DefaultConfig(),
		Ban:                ban.DefaultConfig(),
		Gov:                gov.DefaultConfig(),
		LateMillis:         2000,
		DedupTTLMillis:     10 * 60 * 1000,
		PersistEveryMillis: 500,
	}
}

// Options binds a node to a network fabric, storage path and validator.
type Options struct {
	Config    Config
	Network   *transport.Network
	StorePath string
	Validate  Validator
	Topics    []protocol.Topic
	// Clock is injected for determinism; nil uses wall-clock milliseconds.
	Clock func() int64
}

// EventType classifies processing results for observability/replay.
type EventType int

const (
	EventAcceptedTimely EventType = iota
	EventAcceptedLate
	EventRejectedInvalid
	EventRejectedDuplicate
	EventRejectedExcess
	EventRejectedBanned
	EventFloodDropped
)

// Event is the deterministic result of handling one inbound message.
type Event struct {
	From   protocol.NodeID
	Topic  protocol.Topic
	MsgID  protocol.MsgID
	Type   EventType
	Score  int64
	Banned bool
}

// Node is a single P2P participant with full local governance.
type Node struct {
	id       protocol.NodeID
	cfg      Config
	network  *transport.Network
	store    *store.Store
	validate Validator
	topics   []protocol.Topic
	clock    func() int64

	scores *scoring.Board
	bans   *ban.Manager
	govn   *gov.Governor
	dedup  *dedupSet

	mu          sync.Mutex
	lastPersist int64
	dirty       bool
	closed      bool
}

// New constructs a node and restores persisted governance state when present.
func New(id protocol.NodeID, opts Options) (*Node, error) {
	cfg := opts.Config
	st, err := store.Open(opts.StorePath)
	if err != nil {
		return nil, err
	}
	n := &Node{
		id:       id,
		cfg:      cfg,
		network:  opts.Network,
		store:    st,
		validate: opts.Validate,
		topics:   opts.Topics,
		clock:    opts.Clock,
		scores:   scoring.NewBoard(cfg.Score),
		bans:     ban.NewManager(cfg.Ban),
		govn:     gov.NewGovernor(cfg.Gov),
		dedup:    newDedupSet(),
	}
	if n.clock == nil {
		n.clock = wallMillis
	}
	if err := n.restore(); err != nil {
		return nil, err
	}
	return n, nil
}

// ID returns the node identity.
func (n *Node) ID() protocol.NodeID { return n.id }

func (n *Node) restore() error {
	data, err := n.store.Load()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	st, err := unmarshalState(data)
	if err != nil {
		return err
	}
	n.scores.RestoreState(st.Scores)
	n.bans.RestoreState(st.Bans)
	n.dedup.restore(st.Seen)
	return nil
}

// persistLocked writes all governance state atomically. Caller coordinates
// locking; the underlying store is itself process-safe per call.
func (n *Node) persistLocked(now int64, force bool) error {
	if !force && !n.dirty {
		return nil
	}
	data, err := marshalState(n.scores, n.bans, n.dedup.snapshot())
	if err != nil {
		return err
	}
	if err := n.store.Save(data); err != nil {
		return err
	}
	n.dirty = false
	n.lastPersist = now
	return nil
}

// maybePersist throttles disk flushes.
func (n *Node) maybePersist(now int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dirty && now-n.lastPersist >= n.cfg.PersistEveryMillis {
		return n.persistLocked(now, true)
	}
	return nil
}

// ForcePersist immediately and atomically writes all governance state.
func (n *Node) ForcePersist() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.persistLocked(n.clock(), true)
}

// HandleEnvelope runs the full deterministic governance pipeline for one
// inbound message. It is the unit under test for replay determinism.
func (n *Node) HandleEnvelope(env protocol.Envelope) Event {
	now := n.receivedAt(env)
	msg := env.Msg
	ev := Event{From: msg.From, Topic: msg.Topic, MsgID: msg.ID}

	n.bans.Tick(now)

	// 1. Banned nodes on this topic are rejected outright; other topics are
	// unaffected.
	if status := n.bans.Status(msg.From, msg.Topic, now); status == ban.StatusBanned {
		ev.Type = EventRejectedBanned
		return ev
	}

	// 2. Flood isolation: once engaged, the node's traffic on this topic is
	// dropped before it can consume processing or queue bandwidth.
	if n.govn.IsFlooding(msg.From, msg.Topic) {
		ev.Type = EventFloodDropped
		return ev
	}

	// 3. Per-(node,topic) rate limiting.
	if !n.govn.AllowRate(msg.From, msg.Topic, now) {
		ev.Score = n.applyOutcome(msg, scoring.OutcomeExcess, now)
		ev.Type = EventRejectedExcess
		n.recordBadAndMaybeBan(msg.From, msg.Topic, now, &ev)
		n.markDirty()
		return ev
	}

	// 4. Duplicate detection (survives restarts).
	if !n.dedup.add(msg.From, msg.Topic, msg.ID, now) {
		ev.Score = n.applyOutcome(msg, scoring.OutcomeDuplicate, now)
		ev.Type = EventRejectedDuplicate
		n.recordBadAndMaybeBan(msg.From, msg.Topic, now, &ev)
		n.markDirty()
		return ev
	}

	// 5. Payload validation and timeliness.
	valid, timely := true, true
	if n.validate != nil {
		valid, timely = n.validate(msg, now)
	}
	if !valid {
		ev.Score = n.applyOutcome(msg, scoring.OutcomeInvalid, now)
		ev.Type = EventRejectedInvalid
		n.recordBadAndMaybeBan(msg.From, msg.Topic, now, &ev)
		n.markDirty()
		return ev
	}

	outcome := scoring.OutcomeValidTimely
	ev.Type = EventAcceptedTimely
	if !timely || (n.cfg.LateMillis > 0 && now-msg.CreatedAt > n.cfg.LateMillis) {
		outcome = scoring.OutcomeValidLate
		ev.Type = EventAcceptedLate
	}
	ev.Score = n.applyOutcome(msg, outcome, now)
	n.markDirty()
	return ev
}

func (n *Node) receivedAt(env protocol.Envelope) int64 {
	if env.ReceivedAt > 0 {
		return env.ReceivedAt
	}
	return n.clock()
}

func (n *Node) applyOutcome(msg protocol.Message, outcome scoring.Outcome, now int64) int64 {
	return n.scores.Apply(msg.From, msg.Topic, outcome, now)
}

// recordBadAndMaybeBan first enforces a low-score ban (the flood filter must
// not suppress scoring, otherwise a flooder could never reach the ban
// threshold), then records the bad message toward flood detection. The two
// mechanisms are independent: a ban disconnects on this topic, while flood
// engagement drops further traffic without resetting scores.
func (n *Node) recordBadAndMaybeBan(from protocol.NodeID, topic protocol.Topic, now int64, ev *Event) {
	if n.scores.IsLow(ev.Score) {
		n.bans.Ban(from, topic, now)
		ev.Banned = true
		n.network.Disconnect(from)
		n.govn.ClearFlood(from, topic)
		return
	}
	n.govn.RecordBad(from, topic, now)
}

func (n *Node) markDirty() {
	n.mu.Lock()
	n.dirty = true
	n.mu.Unlock()
}

// Score exposes the current score for tests and observability.
func (n *Node) Score(from protocol.NodeID, topic protocol.Topic) int64 {
	return n.scores.Score(from, topic, n.clock())
}

// Status exposes ban status for tests and observability.
func (n *Node) Status(from protocol.NodeID, topic protocol.Topic) ban.Status {
	return n.bans.Status(from, topic, n.clock())
}

// Stats exposes governance counters.
func (n *Node) Stats(from protocol.NodeID, topic protocol.Topic) gov.Counter {
	return n.govn.Stats(from, topic)
}
