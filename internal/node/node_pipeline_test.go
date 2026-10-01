package node

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/ban"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

func testNode(t *testing.T, validator Validator) (*Node, *transport.Network) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.PersistEveryMillis = 0
	net := transport.NewNetwork(8)
	n, err := New("sink", Options{
		Config:    cfg,
		Network:   net,
		StorePath: filepath.Join(t.TempDir(), "state.json"),
		Validate:  validator,
		Topics:    []protocol.Topic{"tx"},
		Clock:     func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return n, net
}

func env(from, topic, id string, at int64) protocol.Envelope {
	return protocol.Envelope{
		Msg:        protocol.Message{ID: protocol.MsgID(id), From: protocol.NodeID(from), Topic: protocol.Topic(topic), CreatedAt: at},
		ReceivedAt: at,
	}
}

func TestInvalidLowersScoreAndBans(t *testing.T) {
	n, net := testNode(t, func(m protocol.Message, at int64) (bool, bool) {
		return false, true
	})
	n.Conn()
	// Start 50, invalid -25 => 25, 0, -25 (low) => ban + disconnect.
	ev1 := n.HandleEnvelope(env("bad", "tx", "m1", 1000))
	if ev1.Type != EventRejectedInvalid || ev1.Score != 25 || ev1.Banned {
		t.Fatalf("ev1 = %+v", ev1)
	}
	n.HandleEnvelope(env("bad", "tx", "m2", 1001))
	ev3 := n.HandleEnvelope(env("bad", "tx", "m3", 1002))
	if !ev3.Banned {
		t.Fatal("third invalid must trigger ban")
	}
	if got := n.Status("bad", "tx"); got != ban.StatusBanned {
		t.Fatalf("status = %v", got)
	}
	if peers := net.Peers(); containsID(peers, "bad") {
		t.Fatalf("banned node must be disconnected, peers=%v", peers)
	}
	// Banned messages are rejected without scoring.
	ev := n.HandleEnvelope(env("bad", "tx", "m4", 1003))
	if ev.Type != EventRejectedBanned {
		t.Fatalf("banned ev = %+v", ev)
	}
}

func containsID(ids []protocol.NodeID, want string) bool {
	for _, id := range ids {
		if string(id) == want {
			return true
		}
	}
	return false
}

func TestDuplicateNotScoredTwice(t *testing.T) {
	n, _ := testNode(t, func(m protocol.Message, at int64) (bool, bool) { return true, true })
	n.Conn()
	first := n.HandleEnvelope(env("A", "tx", "dup", 1000))
	if first.Type != EventAcceptedTimely {
		t.Fatalf("first = %+v", first)
	}
	second := n.HandleEnvelope(env("A", "tx", "dup", 1000))
	if second.Type != EventRejectedDuplicate {
		t.Fatalf("second = %+v", second)
	}
}

func TestExcessRateLimited(t *testing.T) {
	n, _ := testNode(t, func(m protocol.Message, at int64) (bool, bool) { return true, true })
	n.Conn()
	cfg := n.cfg.Gov
	for i := 0; i < cfg.RateTokens; i++ {
		ev := n.HandleEnvelope(env("A", "tx", fmt.Sprintf("m%d", i), int64(1000+i)))
		if ev.Type != EventAcceptedTimely {
			t.Fatalf("within-budget msg %d = %+v", i, ev)
		}
	}
	ev := n.HandleEnvelope(env("A", "tx", "overflow", 1000+int64(cfg.RateTokens)))
	if ev.Type != EventRejectedExcess {
		t.Fatalf("overflow = %+v", ev)
	}
}

func TestTopicIsolation(t *testing.T) {
	n, _ := testNode(t, func(m protocol.Message, at int64) (bool, bool) {
		return m.Topic != "tx", true
	})
	n.Conn()
	// Many invalid on tx triggers a ban.
	for i := 0; i < 5; i++ {
		n.HandleEnvelope(env("A", "tx", "m", 1000))
	}
	if n.Status("A", "tx") != ban.StatusBanned {
		t.Fatal("A should be banned on tx")
	}
	// blocks topic stays clear and a valid message still scores.
	if n.Status("A", "blocks") != ban.StatusClear {
		t.Fatal("A must remain clear on blocks")
	}
	ev := n.HandleEnvelope(env("A", "blocks", "b1", 1000))
	if ev.Type != EventAcceptedTimely {
		t.Fatalf("blocks message = %+v", ev)
	}
}

func TestDeterministicReplay(t *testing.T) {
	stream := []protocol.Envelope{
		env("A", "tx", "1", 1000),
		env("A", "tx", "2", 1000),
		env("A", "tx", "1", 1000), // duplicate
		env("A", "tx", "3", 1000),
	}
	run := func() []int64 {
		n, _ := testNode(t, func(m protocol.Message, at int64) (bool, bool) { return m.ID != "2", true })
		n.Conn()
		scores := make([]int64, 0, len(stream))
		for _, e := range stream {
			scores = append(scores, n.HandleEnvelope(e).Score)
		}
		return scores
	}
	got1 := run()
	got2 := run()
	for i := range got1 {
		if got1[i] != got2[i] {
			t.Fatalf("nondeterministic at %d: %v vs %v", i, got1, got2)
		}
	}
}
