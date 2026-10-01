package node

import (
	"context"
	"time"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

const maintenanceIntervalMillis = 250

// Conn attaches the node to its network, enforcing the connection quota.
// Identity is bound to reputation: reconnecting returns a new transport
// connection but keeps the same scores and bans.
func (n *Node) Conn() (*transport.Conn, bool) {
	if !n.govn.AcquireConn(n.id) {
		return nil, false
	}
	c := n.network.Join(n.id)
	c.Subscribe(n.topics...)
	return c, true
}

// Run processes inbound messages until Stop cancels the context.
func (n *Node) Run(ctx context.Context, conn *transport.Conn) error {
	ticker := time.NewTicker(time.Millisecond * time.Duration(maintenanceIntervalMillis))
	defer ticker.Stop()
	defer func() {
		conn.Disconnect()
		n.govn.ReleaseConn(n.id)
		n.flush()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-conn.Closed():
			return nil
		case env, ok := <-conn.Recv():
			if !ok {
				return nil
			}
			ev := n.HandleEnvelope(env)
			if ev.Type == EventAcceptedTimely || ev.Type == EventAcceptedLate {
				n.relay(conn, env.Msg)
			}
		case <-ticker.C:
			n.maintenance()
		}
	}
}

// relay forwards an accepted message to the rest of the topic mesh.
func (n *Node) relay(conn *transport.Conn, msg protocol.Message) {
	if msg.From == n.id {
		return
	}
	_ = conn.Send(msg)
}

func (n *Node) maintenance() {
	now := n.clock()
	n.scores.Decay(now)
	n.bans.Tick(now)
	if n.cfg.DedupTTLMillis > 0 {
		n.dedup.prune(now, n.cfg.DedupTTLMillis)
	}
	_ = n.maybePersist(now)
}

// flush forces an atomic state write (used on shutdown).
func (n *Node) flush() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	n.closed = true
	_ = n.persistLocked(n.clock(), true)
}
