package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/transport"
)

type e2e struct {
	net     *transport.Network
	sink    *Node
	sinkCon *transport.Conn
	good    *transport.Conn
	bad     *transport.Conn
}

func setupE2E(t *testing.T) *e2e {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Gov.FloodBadThreshold = 100
	net := transport.NewNetwork(16)

	sink, err := New("sink", Options{
		Config:    cfg,
		Network:   net,
		StorePath: filepath.Join(t.TempDir(), "sink.json"),
		Validate:  func(m protocol.Message, at int64) (bool, bool) { return m.From != "bad", true },
		Topics:    []protocol.Topic{"tx"},
		Clock:     func() int64 { return time.Now().UnixMilli() },
	})
	if err != nil {
		t.Fatal(err)
	}
	sinkCon, ok := sink.Conn()
	if !ok {
		t.Fatal("sink conn")
	}
	good := net.Join("good")
	good.Subscribe("tx")
	bad := net.Join("bad")
	bad.Subscribe("tx")
	return &e2e{net: net, sink: sink, sinkCon: sinkCon, good: good, bad: bad}
}

func (e *e2e) start(t *testing.T) (context.CancelFunc, chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = e.sink.Run(ctx, e.sinkCon)
		close(done)
	}()
	return cancel, done
}

func TestE2EGoodTrafficRelayedAndBadBanned(t *testing.T) {
	e := setupE2E(t)
	cancel, done := e.start(t)
	defer func() {
		cancel()
		<-done
	}()

	// Good node message must be relayed by sink back to bad subscriber.
	e.good.Send(protocol.Message{ID: "ok1", From: "good", Topic: "tx", CreatedAt: time.Now().UnixMilli()})
	select {
	case env := <-e.bad.Recv():
		if env.Msg.ID != "ok1" {
			t.Fatalf("relayed id = %s", env.Msg.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("good message was not relayed")
	}

	// Bad node floods invalid messages until banned.
	deadline := time.Now().Add(2 * time.Second)
	i := 0
	for time.Now().Before(deadline) {
		_ = e.bad.Send(protocol.Message{
			ID:        protocol.MsgID("bad" + string(rune('a'+i%26)) + string(rune('0'+i/26%10))),
			From:      "bad",
			Topic:     "tx",
			CreatedAt: time.Now().UnixMilli(),
		})
		i++
		if e.sink.Status("bad", "tx") == 1 { // StatusBanned
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if e.sink.Status("bad", "tx") != 1 {
		t.Fatal("bad node not banned")
	}

	// After the bad node is removed: sink stays up, and a fresh observer
	// still receives good traffic, proving good nodes are not starved.
	observer := e.net.Join("observer")
	observer.Subscribe("tx")
	e.good.Send(protocol.Message{ID: "ok2", From: "good", Topic: "tx", CreatedAt: time.Now().UnixMilli()})
	select {
	case env := <-observer.Recv():
		if env.Msg.ID != "ok2" {
			t.Fatalf("post-ban relay id = %s", env.Msg.ID)
		}
	case <-e.sinkCon.Closed():
		t.Fatal("sink must stay connected")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("good traffic stopped after banning bad node")
	}
}
