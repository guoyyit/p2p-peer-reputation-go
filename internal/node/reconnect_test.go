package node

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/ban"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

func reconnectNode(t *testing.T) (*Node, *transport.Network) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Gov.FloodBadThreshold = 100
	net := transport.NewNetwork(8)
	n, err := New("sink", Options{
		Config:    cfg,
		Network:   net,
		StorePath: filepath.Join(t.TempDir(), "state.json"),
		Validate:  func(m protocol.Message, at int64) (bool, bool) { return !strings.HasPrefix(string(m.ID), "bad"), true },
		Topics:    []protocol.Topic{"tx"},
		Clock:     func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return n, net
}

func TestReconnectKeepsIdentityBoundState(t *testing.T) {
	n, net := reconnectNode(t)
	conn1, ok := n.Conn()
	if !ok {
		t.Fatal("first conn must acquire quota")
	}
	// Drive a ban for peer "shifty".
	now := int64(1000)
	for i := 0; i < 3; i++ {
		ev := n.HandleEnvelope(env("shifty", "tx", "bad"+string(rune('a'+i)), now))
		if ev.Banned {
			break
		}
		now++
	}
	if n.Status("shifty", "tx") != ban.StatusBanned {
		t.Fatal("shifty should be banned")
	}
	// Peer leaves (offline) and later rejoins with a brand-new connection.
	net.Leave("shifty")
	if peers := net.Peers(); containsID(peers, "shifty") {
		t.Fatal("offline peer connection must be recycled")
	}
	newConn := net.Join("shifty")
	select {
	case <-conn1.Closed():
	default:
	}
	_ = newConn
	// Reputation and ban follow the identity, not the connection: rejoining
	// must not evade the ban.
	if n.Status("shifty", "tx") != ban.StatusBanned {
		t.Fatal("ban must survive reconnect with the same identity")
	}
	ev := n.HandleEnvelope(env("shifty", "tx", "newmsg", now))
	if ev.Type != EventRejectedBanned {
		t.Fatalf("reconnected banned peer must be rejected, got %+v", ev)
	}
}

func TestConnQuotaEnforcedPerIdentity(t *testing.T) {
	n, _ := reconnectNode(t)
	c1, ok := n.Conn()
	if !ok {
		t.Fatal("conn 1")
	}
	_, ok = n.Conn()
	if !ok {
		t.Fatal("conn 2 within default quota of 2")
	}
	_, ok = n.Conn()
	if ok {
		t.Fatal("conn 3 must exceed per-identity quota")
	}
	c1.Disconnect()
}
