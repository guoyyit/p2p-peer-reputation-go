// Package transport simulates P2P connections over in-memory / loopback pipes.
package transport

import (
	"errors"
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

// ErrQueueFull is returned when at least one subscriber's receive queue is
// full. Governance layers use this as an observable flood/backpressure
// signal; other peers still receive the message.
var ErrQueueFull = errors.New("transport: receive queue full")

// ErrNotConnected is returned when the sender is not attached to the network.
var ErrNotConnected = errors.New("transport: node not connected")

// Conn is one node's attachment to the simulated network.
type Conn struct {
	id      protocol.NodeID
	network *Network
	recv    chan protocol.Envelope
	closed  chan struct{}
	once    sync.Once
}

// ID returns the bound peer identity.
func (c *Conn) ID() protocol.NodeID { return c.id }

// Recv returns the receive channel.
func (c *Conn) Recv() <-chan protocol.Envelope { return c.recv }

// Closed is closed when the connection is recycled (leave/disconnect).
func (c *Conn) Closed() <-chan struct{} { return c.closed }

// Send publishes a message to all currently connected subscribers of the
// message topic, excluding the sender. It never blocks: a full peer queue
// yields ErrQueueFull without affecting delivery to other peers.
func (c *Conn) Send(msg protocol.Message) error {
	return c.network.fanout(c, msg)
}

// Subscribe registers interest in topics. Without subscriptions a peer
// receives nothing.
func (c *Conn) Subscribe(topics ...protocol.Topic) {
	c.network.subscribe(c, topics)
}

// Disconnect recycles this connection. A node may later re-Join with the
// same identity; identity is independent of the connection instance.
func (c *Conn) Disconnect() {
	c.network.disconnect(c)
}

// Network is the shared in-memory network fabric.
type Network struct {
	mu         sync.Mutex
	conns      map[protocol.NodeID]*Conn
	subs       map[protocol.Topic]map[protocol.NodeID]*Conn
	queueDepth int
}

// NewNetwork creates an empty network with the given per-peer receive
// queue depth.
func NewNetwork(queueDepth int) *Network {
	if queueDepth <= 0 {
		queueDepth = 64
	}
	return &Network{
		conns:      make(map[protocol.NodeID]*Conn),
		subs:       make(map[protocol.Topic]map[protocol.NodeID]*Conn),
		queueDepth: queueDepth,
	}
}

// Join attaches a node identity and returns its connection. Re-joining the
// same identity recycles the previous connection.
func (n *Network) Join(id protocol.NodeID) *Conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	if old := n.conns[id]; old != nil {
		n.removeLocked(old)
	}
	c := &Conn{
		id:      id,
		network: n,
		recv:    make(chan protocol.Envelope, n.queueDepth),
		closed:  make(chan struct{}),
	}
	n.conns[id] = c
	return c
}

// Leave detaches a node and recycles its connection.
func (n *Network) Leave(id protocol.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if c := n.conns[id]; c != nil {
		n.removeLocked(c)
	}
}

// Disconnect forcibly detaches a node by identity (used for low-score bans).
func (n *Network) Disconnect(id protocol.NodeID) { n.Leave(id) }

// Peers returns a snapshot of currently connected node IDs.
func (n *Network) Peers() []protocol.NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]protocol.NodeID, 0, len(n.conns))
	for id := range n.conns {
		ids = append(ids, id)
	}
	return ids
}

func (n *Network) subscribe(c *Conn, topics []protocol.Topic) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, topic := range topics {
		set := n.subs[topic]
		if set == nil {
			set = make(map[protocol.NodeID]*Conn)
			n.subs[topic] = set
		}
		set[c.id] = c
	}
}

func (n *Network) fanout(sender *Conn, msg protocol.Message) error {
	n.mu.Lock()
	if n.conns[sender.id] != sender {
		n.mu.Unlock()
		return ErrNotConnected
	}
	targets := make([]*Conn, 0, len(n.subs[msg.Topic]))
	for id, c := range n.subs[msg.Topic] {
		if id != sender.id {
			targets = append(targets, c)
		}
	}
	n.mu.Unlock()

	env := protocol.Envelope{Msg: msg, ReceivedAt: 0}
	full := 0
	for _, c := range targets {
		select {
		case c.recv <- env:
		default:
			full++
		}
	}
	if full > 0 {
		return ErrQueueFull
	}
	return nil
}

func (n *Network) disconnect(c *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if cur := n.conns[c.id]; cur == c {
		n.removeLocked(c)
	}
}

// removeLocked closes the connection and clears its registrations. Caller
// holds n.mu.
func (n *Network) removeLocked(c *Conn) {
	delete(n.conns, c.id)
	for topic, set := range n.subs {
		if set[c.id] == c {
			delete(set, c.id)
			if len(set) == 0 {
				delete(n.subs, topic)
			}
		}
	}
	c.once.Do(func() { close(c.closed) })
}
