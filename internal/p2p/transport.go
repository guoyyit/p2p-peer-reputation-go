package p2p

import (
	"fmt"
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/govern"
)

// Transport is an in-memory message bus simulating loopback P2P links.
type Transport struct {
	mu    sync.Mutex
	nodes map[govern.PeerID]*Node
}

func NewTransport() *Transport {
	return &Transport{nodes: make(map[govern.PeerID]*Node)}
}

// Register attaches a node to the bus.
func (t *Transport) Register(n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[n.ID] = n
}

// Unregister detaches a node from the bus.
func (t *Transport) Unregister(id govern.PeerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, id)
}

// Send routes a message from one node to another via the governance service.
// The message is scored first; only accepted messages reach the target inbox.
func (t *Transport) Send(from, to govern.PeerID, msg govern.Message) (govern.Decision, error) {
	t.mu.Lock()
	target, ok := t.nodes[to]
	t.mu.Unlock()
	if !ok {
		return govern.Decision{}, fmt.Errorf("p2p: unknown peer %q", to)
	}
	dec := target.svc.Deliver(from, msg)
	if !dec.Accepted {
		return dec, nil
	}
	select {
	case target.inbox <- msg:
	default:
		// Receiver queue full: drop and make it observable.
		target.svc.noteQueueDrop()
	}
	return dec, nil
}
