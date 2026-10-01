// Package protocol defines the domain types exchanged between P2P nodes.
package protocol

// NodeID permanently identifies a peer identity. Reconnecting with a new
// transport connection but the same NodeID keeps the same reputation.
type NodeID string

// Topic is an independent scoring and relay domain. Misbehavior on one topic
// never affects scoring or relay decisions on another topic.
type Topic string

// MsgID uniquely identifies a message within a topic. It is used for
// duplicate detection across runtime and process restarts.
type MsgID string

// Kind enumerates payload categories.
type Kind int

const (
	KindTx Kind = iota
	KindBlock
)

// Message is a single P2P message.
type Message struct {
	ID        MsgID
	From      NodeID
	Topic     Topic
	Kind      Kind
	CreatedAt int64
	Payload   []byte
}

// Envelope wraps a message with receiver-side arrival metadata.
type Envelope struct {
	Msg        Message
	ReceivedAt int64
}
