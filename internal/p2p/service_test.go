package p2p_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/govern"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/p2p"
)

func engineParams() govern.Params {
	p := govern.DefaultParams()
	p.HalfLife = time.Minute
	p.BaseBanDuration = time.Minute
	p.MaxBanDuration = 10 * time.Minute
	p.ObservationPeriod = time.Minute
	p.SnapshotEvery = 50
	return p
}

func svcParams() p2p.ServiceParams {
	return p2p.ServiceParams{
		MaxConnsPerPeer:       1,
		RatePerSecond:         1,
		Burst:                 2,
		GlobalRatePerSecond:   1000,
		GlobalBurst:           1000,
		ObservationRateFactor: 0.5,
	}
}

func newSvc(t *testing.T, sp p2p.ServiceParams, clock *govern.ManualClock, validator p2p.Validator) (*p2p.Service, *govern.Engine) {
	t.Helper()
	store, err := govern.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	engine := govern.NewEngine(engineParams(), store, clock)
	if err := engine.Recover(); err != nil {
		t.Fatal(err)
	}
	return p2p.NewService(engine, sp, clock, validator), engine
}

func msg(id string, topic govern.Topic, now int64) govern.Message {
	return govern.Message{ID: id, Topic: topic, Payload: []byte("x"), Timestamp: now}
}

func TestConnectionQuota(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	svc, _ := newSvc(t, svcParams(), clock, nil)

	if err := svc.Connect("peer-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Connect("peer-a"); err == nil {
		t.Fatal("second connection must be rejected by quota")
	}
	svc.Disconnect("peer-a")
	if err := svc.Connect("peer-a"); err != nil {
		t.Fatalf("reconnect after release: %v", err)
	}
}

func TestRateLimitThrottleAndRecover(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	svc, engine := newSvc(t, svcParams(), clock, nil)
	if err := svc.Connect("peer-a"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if dec := svc.Deliver("peer-a", msg(fmt.Sprint(i), "blocks", clock.Now())); !dec.Accepted {
			t.Fatalf("burst message %d: got %+v", i, dec)
		}
	}
	dec := svc.Deliver("peer-a", msg("x", "blocks", clock.Now()))
	if dec.Accepted || dec.Verdict != govern.VerdictExcess {
		t.Fatalf("expected throttle, got %+v", dec)
	}
	if got := svc.Metrics().Throttled; got != 1 {
		t.Fatalf("throttled metric: got %d", got)
	}
	if got := engine.Score("peer-a", "blocks"); got != 0 { // +1+1-2
		t.Fatalf("excess penalty: got %v", got)
	}

	clock.Advance(time.Second) // one token refills
	if dec := svc.Deliver("peer-a", msg("y", "blocks", clock.Now())); !dec.Accepted {
		t.Fatalf("after refill: got %+v", dec)
	}
}

func TestFloodDoesNotStarveHonestPeer(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	svc, engine := newSvc(t, svcParams(), clock, nil)
	for _, p := range []govern.PeerID{"flooder", "honest"} {
		if err := svc.Connect(p); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 40; i++ {
		svc.Deliver("flooder", msg(fmt.Sprint(i), "blocks", clock.Now()))
	}
	m := svc.Metrics()
	if m.Throttled == 0 {
		t.Fatal("flood must be throttled")
	}
	if m.Bans == 0 {
		t.Fatal("flood penalties must lead to a ban")
	}
	if engine.CanRelay("flooder", "blocks") {
		t.Fatal("flooder should be banned")
	}

	// The honest peer keeps its own budget throughout the flood.
	dec := svc.Deliver("honest", msg("h1", "blocks", clock.Now()))
	if !dec.Accepted {
		t.Fatalf("honest peer starved: %+v", dec)
	}
	if got := engine.Score("honest", "blocks"); got != 1 {
		t.Fatalf("honest score: got %v", got)
	}
}

func TestInvalidMessageViaValidator(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	validator := func(m govern.Message) bool { return len(m.Payload) > 0 }
	svc, engine := newSvc(t, svcParams(), clock, validator)
	if err := svc.Connect("peer-a"); err != nil {
		t.Fatal(err)
	}

	bad := govern.Message{ID: "bad", Topic: "blocks", Timestamp: clock.Now()}
	dec := svc.Deliver("peer-a", bad)
	if dec.Accepted || dec.Verdict != govern.VerdictInvalid {
		t.Fatalf("invalid: got %+v", dec)
	}
	if got := engine.Score("peer-a", "blocks"); got != -10 {
		t.Fatalf("invalid penalty: got %v", got)
	}
}

func TestReconnectCannotEvadeBan(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	sp := svcParams()
	sp.Burst = 100 // keep rate limiter out of the way
	validator := func(m govern.Message) bool { return len(m.Payload) > 0 }
	svc, engine := newSvc(t, sp, clock, validator)
	if err := svc.Connect("peer-a"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		bad := govern.Message{ID: fmt.Sprintf("bad-%d", i), Topic: "blocks", Timestamp: clock.Now()}
		if dec := svc.Deliver("peer-a", bad); dec.Accepted {
			t.Fatalf("invalid message accepted: %+v", dec)
		}
	}
	if engine.BanStatus("peer-a", "blocks") != govern.StatusBanned {
		t.Fatal("should be banned")
	}

	// Drop the connection and open a fresh one: identity keeps the ban.
	svc.Disconnect("peer-a")
	if err := svc.Connect("peer-a"); err != nil {
		t.Fatal(err)
	}
	dec := svc.Deliver("peer-a", msg("new", "blocks", clock.Now()))
	if dec.Accepted || dec.Reason != "banned" {
		t.Fatalf("reconnect must not evade ban: %+v", dec)
	}
	// Other topics remain usable on the fresh connection.
	if dec := svc.Deliver("peer-a", msg("ok", "txs", clock.Now())); !dec.Accepted {
		t.Fatalf("other topic must work: %+v", dec)
	}
}

func TestUnconnectedPeerRejected(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	svc, _ := newSvc(t, svcParams(), clock, nil)
	dec := svc.Deliver("ghost", msg("g", "blocks", clock.Now()))
	if dec.Accepted || dec.Reason != "not connected" {
		t.Fatalf("ghost: got %+v", dec)
	}
	if got := svc.Metrics().Rejected; got != 1 {
		t.Fatalf("rejected metric: got %d", got)
	}
}

func TestNodeTransportEndToEnd(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	svc, _ := newSvc(t, svcParams(), clock, nil)
	transport := p2p.NewTransport()

	alice, err := p2p.NewNode("alice", transport, svc)
	if err != nil {
		t.Fatal(err)
	}
	bob, err := p2p.NewNode("bob", transport, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	defer bob.Close()

	if _, err := alice.Send("bob", "blocks", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-bob.Inbox():
		if string(m.Payload) != "hello" || m.From != "alice" {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered")
	}
}

func TestDuplicateFloodPenalized(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	sp := svcParams()
	sp.Burst = 100
	sp.RatePerSecond = 100
	svc, engine := newSvc(t, sp, clock, nil)
	if err := svc.Connect("peer-a"); err != nil {
		t.Fatal(err)
	}

	dup := msg("same", "blocks", clock.Now())
	if dec := svc.Deliver("peer-a", dup); !dec.Accepted {
		t.Fatalf("first delivery: %+v", dec)
	}
	for i := 0; i < 10; i++ {
		dec := svc.Deliver("peer-a", dup)
		if dec.Accepted || dec.Verdict != govern.VerdictDuplicate {
			t.Fatalf("dup %d: got %+v", i, dec)
		}
	}
	if got := svc.Metrics().Duplicates; got != 10 {
		t.Fatalf("duplicates metric: got %d", got)
	}
	if got := engine.Score("peer-a", "blocks"); got != -9 { // +1 then 10x -1
		t.Fatalf("duplicate flood score: got %v", got)
	}
}
