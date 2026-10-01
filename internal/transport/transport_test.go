package transport

import (
	"testing"
	"time"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

func TestFanoutAndTopicSubscription(t *testing.T) {
	n := NewNetwork(4)
	a := n.Join("A")
	b := n.Join("B")
	c := n.Join("C")
	b.Subscribe("tx")
	c.Subscribe("blocks")

	if err := a.Send(protocol.Message{ID: "m1", From: "A", Topic: "tx"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case env := <-b.Recv():
		if env.Msg.ID != "m1" {
			t.Fatalf("B got %s", env.Msg.ID)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("B did not receive topic message")
	}
	select {
	case env := <-c.Recv():
		t.Fatalf("C must not receive tx message, got %+v", env)
	default:
	}
}

func TestLeaveRecyclesConn(t *testing.T) {
	n := NewNetwork(4)
	a := n.Join("A")
	n.Join("B")
	if peers := n.Peers(); len(peers) != 2 {
		t.Fatalf("peers = %v", peers)
	}
	n.Leave("A")
	if peers := n.Peers(); len(peers) != 1 {
		t.Fatalf("after leave peers = %v", peers)
	}
	select {
	case <-a.Closed():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("old conn not closed on leave")
	}
	if err := a.Send(protocol.Message{From: "A", Topic: "tx"}); err != ErrNotConnected {
		t.Fatalf("send after leave err = %v, want ErrNotConnected", err)
	}
}

func TestRejoinReplacesOldConn(t *testing.T) {
	n := NewNetwork(4)
	old := n.Join("A")
	new := n.Join("A")
	if old == new {
		t.Fatal("rejoin must produce a new connection")
	}
	select {
	case <-old.Closed():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("old conn not closed on rejoin")
	}
}

func TestFullQueueDoesNotBlockOthers(t *testing.T) {
	n := NewNetwork(1)
	sender := n.Join("S")
	slow := n.Join("slow")
	fast := n.Join("fast")
	slow.Subscribe("tx")
	fast.Subscribe("tx")

	// First message fills slow's depth-1 queue.
	if err := sender.Send(protocol.Message{ID: "1", From: "S", Topic: "tx"}); err != nil {
		t.Fatalf("first send: %v", err)
	}
	// Drain fast so only slow's queue is saturated for the second send.
	<-fast.Recv()
	// Second send: slow drops, fast still receives; observable ErrQueueFull.
	if err := sender.Send(protocol.Message{ID: "2", From: "S", Topic: "tx"}); err != ErrQueueFull {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	gotFast := 0
loop:
	for {
		select {
		case env := <-fast.Recv():
			if env.Msg.ID != "2" {
				t.Fatalf("fast got unexpected message %s", env.Msg.ID)
			}
			gotFast++
		default:
			break loop
		}
	}
	if gotFast != 1 {
		t.Fatalf("fast received %d, want 1 (not starved)", gotFast)
	}
}
