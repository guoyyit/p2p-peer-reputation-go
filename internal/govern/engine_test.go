package govern_test

import (
	"math"
	"testing"
	"time"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/govern"
)

func testParams() govern.Params {
	p := govern.DefaultParams()
	p.HalfLife = time.Minute
	p.BaseBanDuration = time.Minute
	p.MaxBanDuration = 10 * time.Minute
	p.ObservationPeriod = time.Minute
	p.SeenTTL = 10 * time.Minute
	p.SnapshotEvery = 5
	return p
}

func newEngine(t *testing.T, dir string, clock *govern.ManualClock) *govern.Engine {
	t.Helper()
	store, err := govern.OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := govern.NewEngine(testParams(), store, clock)
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	return e
}

func msg(id string, topic govern.Topic, now int64) govern.Message {
	return govern.Message{ID: id, Topic: topic, Payload: []byte("x"), Timestamp: now}
}

func mustHandle(t *testing.T, e *govern.Engine, peer govern.PeerID, m govern.Message, v govern.Verdict) govern.Decision {
	t.Helper()
	dec, err := e.HandleMessage(peer, m, v)
	if err != nil {
		t.Fatal(err)
	}
	return dec
}

func TestValidAndInvalidScoring(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	dec := mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictValid)
	if !dec.Accepted || dec.Score != 1 {
		t.Fatalf("valid: got %+v", dec)
	}
	dec = mustHandle(t, e, "peer-a", msg("m2", "blocks", clock.Now()), govern.VerdictInvalid)
	if dec.Accepted || dec.Score != -9 {
		t.Fatalf("invalid: got %+v", dec)
	}
}

func TestLateMessageGetsPenalty(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	stale := msg("m1", "blocks", clock.Now()-10_000) // 10s old, threshold 5s
	dec := mustHandle(t, e, "peer-a", stale, govern.VerdictValid)
	if !dec.Accepted || dec.Score != -0.5 {
		t.Fatalf("late valid: got %+v", dec)
	}
}

func TestDuplicateMessage(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictValid)
	dec := mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictValid)
	if dec.Verdict != govern.VerdictDuplicate || dec.Accepted {
		t.Fatalf("expected duplicate rejection, got %+v", dec)
	}
	if dec.Score != 0 { // +1 then -1
		t.Fatalf("duplicate score: got %v", dec.Score)
	}
}

func TestScoreDecay(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	for i := 0; i < 10; i++ {
		mustHandle(t, e, "peer-a", msg(string(rune('a'+i)), "blocks", clock.Now()), govern.VerdictValid)
	}
	clock.Advance(time.Minute) // one half-life
	if got := e.Score("peer-a", "blocks"); got != 5 {
		t.Fatalf("decay: got %v want 5", got)
	}
	clock.Advance(time.Minute)
	if got := e.Score("peer-a", "blocks"); got != 2.5 {
		t.Fatalf("decay 2: got %v want 2.5", got)
	}
}

func TestBanTriggerAndLifecycle(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictInvalid)
	dec := mustHandle(t, e, "peer-a", msg("m2", "blocks", clock.Now()), govern.VerdictInvalid)
	if dec.Action != govern.ActionBan {
		t.Fatalf("expected ban, got %+v", dec)
	}
	if e.BanStatus("peer-a", "blocks") != govern.StatusBanned {
		t.Fatal("should be banned")
	}
	if e.CanRelay("peer-a", "blocks") {
		t.Fatal("banned peer must not relay")
	}
	// Messages arriving while banned are dropped without scoring.
	dec = mustHandle(t, e, "peer-a", msg("m3", "blocks", clock.Now()), govern.VerdictInvalid)
	if dec.Accepted || dec.Reason != "banned" {
		t.Fatalf("banned delivery: got %+v", dec)
	}

	clock.Advance(time.Minute + time.Millisecond)
	if e.BanStatus("peer-a", "blocks") != govern.StatusObservation {
		t.Fatal("should be in observation after ban expiry")
	}
	clock.Advance(time.Minute)
	if e.BanStatus("peer-a", "blocks") != govern.StatusNormal {
		t.Fatal("should return to normal after observation")
	}
}

func TestBanBackoffDoubles(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	var firstUntil int64
	mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictInvalid)
	dec := mustHandle(t, e, "peer-a", msg("m2", "blocks", clock.Now()), govern.VerdictInvalid)
	firstUntil = dec.BannedUntil
	if got := firstUntil - clock.Now(); got != 60_000 {
		t.Fatalf("first ban: got %dms want 60000ms", got)
	}

	// Let ban + observation lapse, then re-offend.
	clock.Set(firstUntil + 60_001)
	mustHandle(t, e, "peer-a", msg("m3", "blocks", clock.Now()), govern.VerdictInvalid)
	dec = mustHandle(t, e, "peer-a", msg("m4", "blocks", clock.Now()), govern.VerdictInvalid)
	if dec.Action != govern.ActionBan {
		t.Fatalf("expected second ban, got %+v", dec)
	}
	if got := dec.BannedUntil - clock.Now(); got != 120_000 {
		t.Fatalf("second ban: got %dms want 120000ms (backoff)", got)
	}
}

func TestObservationPenaltyDoubled(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictInvalid)
	dec := mustHandle(t, e, "peer-a", msg("m2", "blocks", clock.Now()), govern.VerdictInvalid)
	clock.Set(dec.BannedUntil + 1) // inside observation window

	before := e.Score("peer-a", "blocks")
	mustHandle(t, e, "peer-a", msg("m3", "blocks", clock.Now()), govern.VerdictInvalid)
	after := e.Score("peer-a", "blocks")
	if delta := after - before; delta != -20 {
		t.Fatalf("observation penalty: got delta %v want -20", delta)
	}
}

func TestPerTopicIsolation(t *testing.T) {
	clock := govern.NewManualClock(1_000_000)
	e := newEngine(t, t.TempDir(), clock)

	mustHandle(t, e, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictInvalid)
	mustHandle(t, e, "peer-a", msg("m2", "blocks", clock.Now()), govern.VerdictInvalid)
	if e.CanRelay("peer-a", "blocks") {
		t.Fatal("blocks should be banned")
	}
	if !e.CanRelay("peer-a", "txs") {
		t.Fatal("txs must stay relayable")
	}
	dec := mustHandle(t, e, "peer-a", msg("m3", "txs", clock.Now()), govern.VerdictValid)
	if !dec.Accepted || dec.Score != 1 {
		t.Fatalf("txs scoring unaffected: got %+v", dec)
	}
	if got := e.Score("peer-a", "txs"); got != 1 {
		t.Fatalf("txs score: got %v", got)
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	clock := govern.NewManualClock(1_000_000)

	e := newEngine(t, dir, clock)
	for _, id := range []string{"m1", "m2", "m3"} {
		mustHandle(t, e, "peer-a", msg(id, "blocks", clock.Now()), govern.VerdictValid)
	}
	mustHandle(t, e, "peer-b", msg("x1", "blocks", clock.Now()), govern.VerdictInvalid)
	mustHandle(t, e, "peer-b", msg("x2", "blocks", clock.Now()), govern.VerdictInvalid)
	scoreA := e.Score("peer-a", "blocks")
	// Simulate crash: no Close, no final snapshot.

	e2 := newEngine(t, dir, clock)
	if got := e2.Score("peer-a", "blocks"); got != scoreA {
		t.Fatalf("recovered score: got %v want %v", got, scoreA)
	}
	if e2.BanStatus("peer-b", "blocks") != govern.StatusBanned {
		t.Fatal("ban must survive crash")
	}
	// A message seen before the crash must not be scored as valid again.
	dec := mustHandle(t, e2, "peer-a", msg("m1", "blocks", clock.Now()), govern.VerdictValid)
	if dec.Verdict != govern.VerdictDuplicate {
		t.Fatalf("replay must not double-count: got %+v", dec)
	}
	if got := e2.Score("peer-a", "blocks"); got != scoreA-1 {
		t.Fatalf("duplicate penalty only: got %v want %v", got, scoreA-1)
	}
}

func TestSnapshotThenRecover(t *testing.T) {
	dir := t.TempDir()
	clock := govern.NewManualClock(1_000_000)

	e := newEngine(t, dir, clock)
	for i := 0; i < 12; i++ { // crosses SnapshotEvery=5 twice
		mustHandle(t, e, "peer-a", msg(string(rune('a'+i)), "blocks", clock.Now()), govern.VerdictValid)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := newEngine(t, dir, clock)
	if got := e2.Score("peer-a", "blocks"); got != 12 {
		t.Fatalf("snapshot recovery: got %v want 12", got)
	}
}

func TestDeterminism(t *testing.T) {
	script := func(dir string) []govern.Decision {
		clock := govern.NewManualClock(1_000_000)
		e := newEngine(t, dir, clock)
		var out []govern.Decision
		verdicts := []govern.Verdict{
			govern.VerdictValid, govern.VerdictValid, govern.VerdictInvalid,
			govern.VerdictValid, govern.VerdictExcess, govern.VerdictInvalid,
			govern.VerdictValid, govern.VerdictInvalid,
		}
		for i, v := range verdicts {
			clock.Advance(700 * time.Millisecond)
			out = append(out, mustHandle(t, e, "peer-a", msg(string(rune('a'+i)), "blocks", clock.Now()), v))
		}
		// Duplicate of the first message.
		out = append(out, mustHandle(t, e, "peer-a", msg("a", "blocks", clock.Now()), govern.VerdictValid))
		return out
	}
	run1 := script(t.TempDir())
	run2 := script(t.TempDir())
	if len(run1) != len(run2) {
		t.Fatal("run length mismatch")
	}
	for i := range run1 {
		a, b := run1[i], run2[i]
		if a.Accepted != b.Accepted || a.Verdict != b.Verdict || a.Action != b.Action ||
			a.BannedUntil != b.BannedUntil || math.Float64bits(a.Score) != math.Float64bits(b.Score) {
			t.Fatalf("step %d diverged: %+v vs %+v", i, a, b)
		}
	}
}
