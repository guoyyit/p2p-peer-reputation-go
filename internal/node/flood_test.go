package node

import (
	"path/filepath"
	"testing"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

func floodNode(t *testing.T) (*Node, *transport.Network) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Score.LowScoreThreshold = -100000 // never ban in this test
	cfg.Gov.FloodBadThreshold = 3
	cfg.Gov.FloodWindowMillis = 5000
	cfg.Gov.RateTokens = 1000
	net := transport.NewNetwork(8)
	n, err := New("sink", Options{
		Config:    cfg,
		Network:   net,
		StorePath: filepath.Join(t.TempDir(), "state.json"),
		Validate: func(m protocol.Message, at int64) (bool, bool) {
			if m.From == "flooder" && m.Topic == "tx" {
				return false, true
			}
			return true, true
		},
		Topics: []protocol.Topic{"tx", "blocks"},
		Clock:  func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Conn()
	return n, net
}

func TestFloodIsolationProtectsGoodNode(t *testing.T) {
	n, _ := floodNode(t)

	// Flooder emits invalid messages on tx; flood protection trips at 3.
	var tripped bool
	for i := 0; i < 10; i++ {
		ev := n.HandleEnvelope(env("flooder", "tx", "f"+string(rune('a'+i)), 1000))
		if ev.Type == EventFloodDropped {
			tripped = true
		}
	}
	if !tripped {
		t.Fatal("flood protection must drop excess flooder traffic")
	}
	s := n.Stats("flooder", "tx")
	if s.FloodTripped == 0 || !s.FloodEngaged {
		t.Fatalf("flood not observable: %+v", s)
	}

	// A well-behaved node on the same topic is fully unaffected.
	for i := 0; i < 10; i++ {
		ev := n.HandleEnvelope(env("good", "tx", "g"+string(rune('a'+i)), 1000))
		if ev.Type != EventAcceptedTimely {
			t.Fatalf("good node must keep processing, got %+v", ev)
		}
	}
	gs := n.Stats("good", "tx")
	if gs.RateLimited != 0 || gs.Bad != 0 {
		t.Fatalf("good node counters must stay clean: %+v", gs)
	}
	if got := n.Score("good", "tx"); got != 100 {
		t.Fatalf("good node score = %d, want capped 100", got)
	}

	// Flood on tx never affects the blocks topic for the same flooder.
	ev := n.HandleEnvelope(env("flooder", "blocks", "b1", 1000))
	if ev.Type != EventAcceptedTimely {
		t.Fatalf("blocks topic must be isolated from tx flood, got %+v", ev)
	}
}
