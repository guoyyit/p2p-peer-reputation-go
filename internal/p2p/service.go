package p2p

import (
	"fmt"
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/govern"
)

// ServiceParams tunes connection quotas, rate limits, and flood protection.
type ServiceParams struct {
	MaxConnsPerPeer       int     // simultaneous connections per peer identity
	RatePerSecond         float64 // sustained per-peer message rate
	Burst                 int     // per-peer token-bucket burst allowance
	GlobalRatePerSecond   float64 // total processing bandwidth shared by all peers
	GlobalBurst           int     // global token-bucket burst allowance
	ObservationRateFactor float64 // rate multiplier during observation (0..1)
}

func DefaultServiceParams() ServiceParams {
	return ServiceParams{
		MaxConnsPerPeer:       1,
		RatePerSecond:         50,
		Burst:                 20,
		GlobalRatePerSecond:   500,
		GlobalBurst:           200,
		ObservationRateFactor: 0.5,
	}
}

// Metrics exposes flood-protection and governance counters for observability.
type Metrics struct {
	Accepted    uint64
	Invalid     uint64
	Duplicates  uint64
	Throttled   uint64 // rate-limited messages
	FloodDrops  uint64 // in-flight overflow drops
	Rejected    uint64 // banned / quota rejections
	Bans        uint64
	Disconnects uint64
}

// Service is the governance gateway: every inbound message and connection
// attempt passes through it before reaching a node.
type Service struct {
	engine *govern.Engine
	params ServiceParams
	clock  govern.Clock

	mu        sync.Mutex
	conns     map[govern.PeerID]int
	buckets   map[govern.PeerID]*bucket
	global    bucket
	metrics   Metrics
	validator Validator
}

// bucket is a deterministic token bucket driven by the injected clock.
type bucket struct {
	tokens float64
	last   int64
}

// NewService creates the gateway around a governance engine.
func NewService(engine *govern.Engine, params ServiceParams, clock govern.Clock, validator Validator) *Service {
	return &Service{
		engine:    engine,
		params:    params,
		clock:     clock,
		conns:     make(map[govern.PeerID]int),
		buckets:   make(map[govern.PeerID]*bucket),
		global:    bucket{tokens: float64(params.GlobalBurst)},
		validator: validator,
	}
}

// Connect registers a connection for a peer, enforcing bans and quotas.
func (s *Service) Connect(peer govern.PeerID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.params.MaxConnsPerPeer > 0 && s.conns[peer] >= s.params.MaxConnsPerPeer {
		s.metrics.Rejected++
		return fmt.Errorf("p2p: peer %q exceeds connection quota", peer)
	}
	if err := s.engine.OnConnect(peer); err != nil {
		return err
	}
	s.conns[peer]++
	return nil
}

// Disconnect releases a peer's connection slot.
func (s *Service) Disconnect(peer govern.PeerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnectLocked(peer)
}

func (s *Service) disconnectLocked(peer govern.PeerID) {
	if s.conns[peer] > 0 {
		s.conns[peer]--
	}
	if s.conns[peer] == 0 {
		delete(s.conns, peer)
		delete(s.buckets, peer)
	}
	_ = s.engine.OnDisconnect(peer)
}

// Deliver applies rate limits, flood protection, and scoring to one inbound
// message, returning the governance decision.
func (s *Service) Deliver(peer govern.PeerID, msg govern.Message) govern.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()

	if s.conns[peer] == 0 {
		s.metrics.Rejected++
		return govern.Decision{Accepted: false, Reason: "not connected"}
	}
	if !s.engine.CanRelay(peer, msg.Topic) {
		s.metrics.Rejected++
		return govern.Decision{Accepted: false, Reason: "banned", Action: govern.ActionBan}
	}

	// Flood protection: a peer must spend from its own bucket before touching
	// the shared processing budget, so floods are dropped at ingress.
	rate := s.params.RatePerSecond
	if s.engine.BanStatus(peer, msg.Topic) == govern.StatusObservation {
		rate *= s.params.ObservationRateFactor
	}
	if !s.allowPeer(peer, rate, now) {
		s.metrics.Throttled++
		dec, err := s.engine.HandleMessage(peer, msg, govern.VerdictExcess)
		if err != nil {
			return govern.Decision{Accepted: false, Reason: "store error"}
		}
		dec.Accepted = false
		s.afterScore(peer, dec)
		return dec
	}
	if !s.allowGlobal(now) {
		s.metrics.FloodDrops++
		return govern.Decision{Accepted: false, Reason: "global congestion"}
	}

	verdict := govern.VerdictValid
	if s.validator != nil && !s.validator(msg) {
		verdict = govern.VerdictInvalid
	}
	dec, err := s.engine.HandleMessage(peer, msg, verdict)
	if err != nil {
		return govern.Decision{Accepted: false, Reason: "store error"}
	}
	s.afterScore(peer, dec)
	return dec
}

// afterScore updates metrics and enforces ban-driven disconnects.
func (s *Service) afterScore(peer govern.PeerID, dec govern.Decision) {
	switch dec.Verdict {
	case govern.VerdictValid:
		s.metrics.Accepted++
	case govern.VerdictInvalid:
		s.metrics.Invalid++
	case govern.VerdictDuplicate:
		s.metrics.Duplicates++
	}
	if dec.Action == govern.ActionBan {
		s.metrics.Bans++
		s.metrics.Disconnects++
		s.disconnectLocked(peer)
	}
}

// allowPeer consumes one token from the peer's bucket, refilled at rate/sec.
func (s *Service) allowPeer(peer govern.PeerID, rate float64, now int64) bool {
	b, ok := s.buckets[peer]
	if !ok {
		b = &bucket{tokens: float64(s.params.Burst), last: now}
		s.buckets[peer] = b
	}
	return s.allow(b, rate, float64(s.params.Burst), now)
}

// allowGlobal consumes one token from the shared processing budget.
func (s *Service) allowGlobal(now int64) bool {
	return s.allow(&s.global, s.params.GlobalRatePerSecond, float64(s.params.GlobalBurst), now)
}

func (s *Service) allow(b *bucket, rate, burst float64, now int64) bool {
	if now > b.last {
		b.tokens += float64(now-b.last) / 1000 * rate
		if b.tokens > burst {
			b.tokens = burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// noteQueueDrop records a drop at a receiver's bounded inbox.
func (s *Service) noteQueueDrop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics.FloodDrops++
}

// Metrics returns a copy of the current counters.
func (s *Service) Metrics() Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metrics
}
