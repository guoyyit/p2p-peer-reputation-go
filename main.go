// Command p2p-reputation runs a local, in-memory simulated P2P network that
// demonstrates reputation-based connection governance: scoring, rate
// limiting, flood isolation, banning with exponential backoff, topic
// isolation and crash recovery from a local state file.
//
// Usage:
//
//	go run .                 # run the deterministic demo, persist state
//	go run . -restart        # resume from the persisted state file
//	go run . -state <path>   # choose a different state file
package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/ban"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/node"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

func main() {
	statePath := flag.String("state", filepath.Join("data", "gov-state.json"), "persistent state file")
	restart := flag.Bool("restart", false, "resume from the existing state file")
	flag.Parse()

	cfg := node.DefaultConfig()
	// Fast, deterministic demo timings.
	cfg.Ban.BaseBanMillis = 3000
	cfg.Ban.ProbationMillis = 4000
	cfg.Gov.FloodBadThreshold = 4
	net := transport.NewNetwork(32)

	clock := newFakeClock(1_000_000)
	validator := func(m protocol.Message, at int64) (bool, bool) {
		return !strings.HasPrefix(string(m.ID), "invalid"), true
	}

	n, err := node.New("sink", node.Options{
		Config:    cfg,
		Network:   net,
		StorePath: *statePath,
		Validate:  validator,
		Topics:    []protocol.Topic{"tx", "blocks"},
		Clock:     clock.now,
	})
	must(err)
	if _, ok := n.Conn(); !ok {
		panic("sink connection rejected by quota")
	}

	fmt.Printf("== P2P reputation demo (restart=%v, state=%s) ==\n", *restart, *statePath)

	// A good node on tx and an independent node on blocks.
	net.Join("good").Subscribe("tx")
	net.Join("builder").Subscribe("blocks")

	step := func(label string, ms int64, fn func()) {
		clock.advance(ms)
		fn()
		fmt.Printf("[t=%dms] %s\n", clock.now(), label)
	}

	// Good behavior earns score.
	step("good: valid tx message", 100, func() {
		submit(n, "good", "tx", "g1", clock.now(), clock.now())
	})
	// Timeliness: a late valid message earns only the small late reward.
	step("good: late tx message", 100, func() {
		submit(n, "good", "tx", "g2", clock.now()-cfg.LateMillis-1, clock.now())
	})

	// Misbehaving node: invalid messages drive score below threshold.
	net.Join("evil").Subscribe("tx")
	for i := 0; i < 4; i++ {
		idx := i
		step(fmt.Sprintf("evil: invalid message #%d", idx+1), 100, func() {
			submit(n, "evil", "tx", fmt.Sprintf("invalid%d", idx), clock.now(), clock.now())
		})
	}

	// Topic isolation: evil's ban on tx does not touch blocks.
	step("evil: valid message on blocks topic", 100, func() {
		submit(n, "evil", "blocks", "b1", clock.now(), clock.now())
	})

	printStatus(n)

	// Advance beyond the ban: node enters probation, then clears.
	step("advance past ban expiry -> probation", cfg.Ban.BaseBanMillis, func() {})
	printStatus(n)
	step("advance past probation -> clear", cfg.Ban.ProbationMillis+1, func() {})
	printStatus(n)

	// Flush state for the -restart demonstration.
	n.ForcePersist()
	fmt.Printf("state persisted to %s\n", *statePath)
}

func submit(n *node.Node, from, topic, id string, createdAt, receivedAt int64) {
	fmtEvent(n.HandleEnvelope(envelope(from, topic, id, createdAt, receivedAt)))
}

func envelope(from, topic, id string, createdAt, receivedAt int64) protocol.Envelope {
	return protocol.Envelope{
		Msg: protocol.Message{
			ID:        protocol.MsgID(id),
			From:      protocol.NodeID(from),
			Topic:     protocol.Topic(topic),
			CreatedAt: createdAt,
		},
		ReceivedAt: receivedAt,
	}
}

func fmtEvent(ev node.Event) {
	fmt.Printf("    -> %-22s from=%-7s topic=%-7s score=%4d banned=%v\n",
		eventName(ev.Type), ev.From, ev.Topic, ev.Score, ev.Banned)
}

func eventName(t node.EventType) string {
	switch t {
	case node.EventAcceptedTimely:
		return "accepted(timely)"
	case node.EventAcceptedLate:
		return "accepted(late)"
	case node.EventRejectedInvalid:
		return "rejected(invalid)"
	case node.EventRejectedDuplicate:
		return "rejected(duplicate)"
	case node.EventRejectedExcess:
		return "rejected(excess)"
	case node.EventRejectedBanned:
		return "rejected(banned)"
	case node.EventFloodDropped:
		return "dropped(flood)"
	default:
		return "unknown"
	}
}

func printStatus(n *node.Node) {
	for _, p := range []struct{ id, topic string }{
		{"good", "tx"}, {"evil", "tx"}, {"evil", "blocks"},
	} {
		fmt.Printf("    status node=%-7s topic=%-7s score=%4d ban=%s\n",
			p.id, p.topic, n.Score(protocol.NodeID(p.id), protocol.Topic(p.topic)),
			statusName(n.Status(protocol.NodeID(p.id), protocol.Topic(p.topic))))
	}
}

func statusName(s ban.Status) string {
	switch s {
	case ban.StatusBanned:
		return "BANNED"
	case ban.StatusProbation:
		return "PROBATION"
	default:
		return "clear"
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// fakeClock is a manually advanced millisecond clock for deterministic demos.
type fakeClock struct{ t int64 }

func newFakeClock(start int64) *fakeClock { return &fakeClock{t: start} }
func (c *fakeClock) now() int64           { return c.t }
func (c *fakeClock) advance(ms int64)     { c.t += ms }
