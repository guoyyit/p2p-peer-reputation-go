package scoring

import (
	"testing"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

var (
	nodeA = protocol.NodeID("A")
	nodeB = protocol.NodeID("B")
	t1    = protocol.Topic("tx-main")
	t2    = protocol.Topic("blocks")
)

func TestRewardsAndPenalties(t *testing.T) {
	b := NewBoard(DefaultConfig())
	if got := b.Apply(nodeA, t1, OutcomeValidTimely, 0); got != 60 {
		t.Fatalf("timely valid = %d, want 60", got)
	}
	if got := b.Apply(nodeA, t1, OutcomeInvalid, 100); got != 35 {
		t.Fatalf("invalid = %d, want 35", got)
	}
	if got := b.Score(nodeA, t1, 100); got != 35 {
		t.Fatalf("score = %d, want 35", got)
	}
}

func TestClamping(t *testing.T) {
	b := NewBoard(DefaultConfig())
	for i := 0; i < 100; i++ {
		b.Apply(nodeA, t1, OutcomeValidTimely, int64(i))
	}
	if got := b.Score(nodeA, t1, 99); got != 100 {
		t.Fatalf("max clamp = %d, want 100", got)
	}
	for i := 0; i < 100; i++ {
		b.Apply(nodeB, t1, OutcomeInvalid, int64(i))
	}
	if got := b.Score(nodeB, t1, 99); got != -1000 {
		t.Fatalf("min clamp = %d, want -1000", got)
	}
}

func TestDecayIsIntegerAndTowardZero(t *testing.T) {
	cfg := DefaultConfig()
	b := NewBoard(cfg)
	b.Apply(nodeA, t1, OutcomeInvalid, 0) // 50-25 = 25
	// 1500ms => one full decay step of 5.
	if got := b.Score(nodeA, t1, 1500); got != 20 {
		t.Fatalf("positive decay = %d, want 20", got)
	}
	b.Apply(nodeB, t1, OutcomeInvalid, 0)
	b.Apply(nodeB, t1, OutcomeInvalid, 1) // 0
	b.Apply(nodeB, t1, OutcomeInvalid, 2) // -25
	if got := b.Score(nodeB, t1, 1502); got != -20 {
		t.Fatalf("negative decay = %d, want -20", got)
	}
}

func TestTopicIsolation(t *testing.T) {
	b := NewBoard(DefaultConfig())
	b.Apply(nodeA, t1, OutcomeInvalid, 0) // 25 on t1
	if got := b.Score(nodeA, t1, 0); got != 25 {
		t.Fatalf("t1 score = %d, want 25", got)
	}
	if got := b.Score(nodeA, t2, 0); got != 50 {
		t.Fatalf("t2 must be unaffected, got %d", got)
	}
}

func TestSnapshotRestore(t *testing.T) {
	b := NewBoard(DefaultConfig())
	b.Apply(nodeA, t1, OutcomeInvalid, 0)
	b.Apply(nodeA, t2, OutcomeValidTimely, 0)
	snap := b.SnapshotState()

	b2 := NewBoard(DefaultConfig())
	b2.RestoreState(snap)
	if got := b2.Score(nodeA, t1, 0); got != 25 {
		t.Fatalf("restored t1 = %d, want 25", got)
	}
	if got := b2.Score(nodeA, t2, 0); got != 60 {
		t.Fatalf("restored t2 = %d, want 60", got)
	}
	// Snapshot ordering is deterministic.
	if snap.Entries[0].Topic != "blocks" || snap.Entries[1].Topic != "tx-main" {
		t.Fatalf("snapshot not sorted deterministically: %+v", snap.Entries)
	}
}
