package node

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/ban"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

func recoverNode(t *testing.T, path string, clock func() int64) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Gov.FloodBadThreshold = 100
	net := transport.NewNetwork(8)
	n, err := New("sink", Options{
		Config:    cfg,
		Network:   net,
		StorePath: path,
		Validate:  func(m protocol.Message, at int64) (bool, bool) { return !strings.HasPrefix(string(m.ID), "bad"), true },
		Topics:    []protocol.Topic{"tx"},
		Clock:     clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCrashRecoveryKeepsBanAndDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	now := int64(1000)
	clock := func() int64 { return now }

	// First process lifetime: enough invalid messages to trigger a ban.
	n1 := recoverNode(t, path, clock)
	n1.Conn()
	var last Event
	for i := 0; i < 6; i++ {
		last = n1.HandleEnvelope(env("evil", "tx", "bad"+string(rune('a'+i)), now))
		if last.Banned {
			break
		}
		now++
	}
	if !last.Banned {
		t.Fatalf("expected ban before crash, last=%+v", last)
	}
	n1.flush()

	// Simulate restart: new node, same store path, retained identity state.
	n2 := recoverNode(t, path, clock)
	if got := n2.Status("evil", "tx"); got != ban.StatusBanned {
		t.Fatalf("ban lost across restart: %v", got)
	}
	// A previously judged message must be treated as duplicate, never scored
	// again after recovery.
	before := n2.Score("evil", "tx")
	ev := n2.HandleEnvelope(env("evil", "tx", "bada", now))
	if ev.Type != EventRejectedBanned {
		// Still within ban window => rejected as banned first.
		t.Fatalf("expected banned rejection, got %+v", ev)
	}

	// Advance beyond ban but before full probation expiry: node is in
	// probation; an already-seen message must now be rejected as duplicate
	// and must not change the score.
	now += 6_000
	n2.bans.Tick(now)
	if got := n2.Status("evil", "tx"); got != ban.StatusProbation {
		t.Fatalf("expected probation, got %v", got)
	}
	scoreBefore := n2.scores.Score("evil", "tx", now)
	dupEv := n2.HandleEnvelope(env("evil", "tx", "bada", now))
	if dupEv.Type != EventRejectedDuplicate {
		t.Fatalf("replayed message must be duplicate, got %+v", dupEv)
	}
	// A duplicate carries the duplicate penalty exactly once at this time,
	// demonstrating post-restart dedup: the persisted message ID is still
	// recognized rather than treated as a brand-new valid message.
	want := scoreBefore - n2.cfg.Score.DuplicatePenalty
	if dupEv.Score != want {
		t.Fatalf("duplicate penalty wrong: got %d want %d", dupEv.Score, want)
	}
	_ = before
}

func TestScoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	now := int64(5000)
	clock := func() int64 { return now }

	n1 := recoverNode(t, path, clock)
	n1.Conn()
	n1.HandleEnvelope(env("good", "tx", "g1", now))
	n1.flush()

	n2 := recoverNode(t, path, clock)
	if got := n2.Score("good", "tx"); got != 60 {
		t.Fatalf("restored score = %d, want 60", got)
	}
}
