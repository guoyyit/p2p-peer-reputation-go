package ban

import (
	"testing"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

var (
	nodeA = protocol.NodeID("A")
	t1    = protocol.Topic("tx-main")
	t2    = protocol.Topic("blocks")
)

func TestBanExpiryAndProbation(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.Ban(nodeA, t1, 0)
	if got := m.Status(nodeA, t1, 100); got != StatusBanned {
		t.Fatalf("status = %v, want banned", got)
	}
	if got := m.Status(nodeA, t1, 5000); got != StatusProbation {
		t.Fatalf("status = %v, want probation", got)
	}
	if got := m.Status(nodeA, t1, 15000); got != StatusClear {
		t.Fatalf("status = %v, want clear", got)
	}
}

func TestBackoffDoubles(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	if d := m.Ban(nodeA, t1, 0); d != cfg.BaseBanMillis {
		t.Fatalf("first ban = %d, want %d", d, cfg.BaseBanMillis)
	}
	// Reoffend during probation: duration doubles.
	if d := m.Ban(nodeA, t1, 5000); d != 2*cfg.BaseBanMillis {
		t.Fatalf("second ban = %d, want %d", d, 2*cfg.BaseBanMillis)
	}
	if got := m.Status(nodeA, t1, 14999); got != StatusBanned {
		t.Fatalf("status = %v, want banned", got)
	}
}

func TestBackoffCaps(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	now := int64(0)
	for i := 0; i < 40; i++ {
		m.Ban(nodeA, t1, now)
		now++
	}
	if got := m.Ban(nodeA, t1, now); got != cfg.MaxBanMillis {
		t.Fatalf("ban = %d, want cap %d", got, cfg.MaxBanMillis)
	}
}

func TestTopicIsolation(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.Ban(nodeA, t1, 0)
	if got := m.Status(nodeA, t1, 0); got != StatusBanned {
		t.Fatalf("t1 = %v, want banned", got)
	}
	if got := m.Status(nodeA, t2, 0); got != StatusClear {
		t.Fatalf("t2 must be clear, got %v", got)
	}
}

func TestRestorePreservesBan(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.Ban(nodeA, t1, 0)
	snap := m.SnapshotState()

	m2 := NewManager(DefaultConfig())
	m2.RestoreState(snap)
	if got := m2.Status(nodeA, t1, 100); got != StatusBanned {
		t.Fatalf("restored status = %v, want banned", got)
	}
	// A re-ban after restore continues the backoff (not reset).
	if d := m2.Ban(nodeA, t1, 100); d != 2*DefaultConfig().BaseBanMillis {
		t.Fatalf("re-ban after restore = %d, want doubled", d)
	}
}

func TestBackoffResetsAfterCleanProbation(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(cfg)
	// First ban, then reoffend during probation => doubled.
	m.Ban(nodeA, t1, 0)
	if d := m.Ban(nodeA, t1, cfg.BaseBanMillis); d != 2*cfg.BaseBanMillis {
		t.Fatalf("probation reoffense = %d, want doubled %d", d, 2*cfg.BaseBanMillis)
	}
	// Complete ban + probation cleanly, then reoffend => base duration.
	if cfg.BaseBanMillis != 5000 {
		t.Fatalf("test assumes 5000ms base ban, got %d", cfg.BaseBanMillis)
	}
	// Second ban starts at 5000 with doubled duration 10000 (until 15000),
	// followed by a 10000ms probation (until 25000).
	clearAt := int64(25001)
	if got := m.Status(nodeA, t1, clearAt); got != StatusClear {
		t.Fatalf("status = %v, want clear", got)
	}
	if d := m.Ban(nodeA, t1, clearAt); d != cfg.BaseBanMillis {
		t.Fatalf("post-probation ban = %d, want base %d", d, cfg.BaseBanMillis)
	}
}
