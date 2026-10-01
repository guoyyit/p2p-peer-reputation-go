package p2p

import (
	"fmt"
	"sync/atomic"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/govern"
)

// Validator decides whether a message payload is well-formed.
type Validator func(govern.Message) bool

// Node is a simulated peer: it has an identity, sends messages, and receives
// messages accepted by the governance service.
type Node struct {
	ID        govern.PeerID
	transport *Transport
	svc       *Service
	inbox     chan govern.Message
	done      chan struct{}
	seq       atomic.Uint64
}

// NewNode creates and connects a node, enforcing connection quotas.
func NewNode(id govern.PeerID, transport *Transport, svc *Service) (*Node, error) {
	n := &Node{
		ID:        id,
		transport: transport,
		svc:       svc,
		inbox:     make(chan govern.Message, 64),
		done:      make(chan struct{}),
	}
	if err := svc.Connect(id); err != nil {
		return nil, err
	}
	transport.Register(n)
	return n, nil
}

// Send transmits a message to another node through the bus.
func (n *Node) Send(to govern.PeerID, topic govern.Topic, payload []byte) (govern.Decision, error) {
	seq := n.seq.Add(1)
	msg := govern.Message{
		ID:        fmt.Sprintf("%s-%d", n.ID, seq),
		From:      n.ID,
		Topic:     topic,
		Payload:   payload,
		Timestamp: n.svc.clock.Now(),
	}
	return n.transport.Send(n.ID, to, msg)
}

// Inbox returns the channel of accepted inbound messages.
func (n *Node) Inbox() <-chan govern.Message {
	return n.inbox
}

// Close disconnects the node and releases its connection slot.
func (n *Node) Close() {
	n.transport.Unregister(n.ID)
	n.svc.Disconnect(n.ID)
	close(n.done)
}
