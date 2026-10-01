package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/govern"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/p2p"
)

// demoParams uses short durations so the whole lifecycle is visible live.
func demoParams() govern.Params {
	p := govern.DefaultParams()
	p.HalfLife = 10 * time.Second
	p.BaseBanDuration = 2 * time.Second
	p.MaxBanDuration = 10 * time.Second
	p.ObservationPeriod = 3 * time.Second
	p.SnapshotEvery = 10
	return p
}

func main() {
	dir := flag.String("dir", "./data", "governance state directory")
	flag.Parse()

	store, err := govern.OpenFileStore(*dir)
	if err != nil {
		log.Fatal(err)
	}
	engine := govern.NewEngine(demoParams(), store, govern.RealClock{})
	if err := engine.Recover(); err != nil {
		log.Fatal(err)
	}
	validator := func(m govern.Message) bool { return len(m.Payload) > 0 }
	svc := p2p.NewService(engine, p2p.DefaultServiceParams(), govern.RealClock{}, validator)
	transport := p2p.NewTransport()

	mustNode := func(id govern.PeerID) *p2p.Node {
		n, err := p2p.NewNode(id, transport, svc)
		if err != nil {
			log.Fatal(err)
		}
		return n
	}
	alice := mustNode("alice")
	bob := mustNode("bob")
	mallory := mustNode("mallory")
	topic := govern.Topic("blocks")

	fmt.Println("== phase 1: honest traffic ==")
	for i := 0; i < 5; i++ {
		if _, err := alice.Send("bob", topic, []byte("block")); err != nil {
			log.Fatal(err)
		}
		if _, err := bob.Send("alice", topic, []byte("block")); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("alice=%.1f bob=%.1f\n", engine.Score("alice", topic), engine.Score("bob", topic))

	fmt.Println("== phase 2: mallory sends invalid messages ==")
	for i := 0; i < 3; i++ {
		dec, _ := mallory.Send("alice", topic, nil) // empty payload fails validation
		fmt.Printf("invalid #%d: accepted=%v score=%.1f action=%s\n", i, dec.Accepted, dec.Score, dec.Action)
	}
	fmt.Printf("mallory status=%s score=%.1f\n", engine.BanStatus("mallory", topic), engine.Score("mallory", topic))

	fmt.Println("== phase 3: banned peer reconnects (identity-bound ban) ==")
	mallory.Close()
	mallory2, err := p2p.NewNode("mallory", transport, svc)
	if err != nil {
		log.Fatal(err)
	}
	dec, _ := mallory2.Send("alice", topic, []byte("block"))
	fmt.Printf("relay while banned: accepted=%v reason=%s\n", dec.Accepted, dec.Reason)

	fmt.Println("== phase 4: ban expires, observation begins ==")
	time.Sleep(2500 * time.Millisecond)
	fmt.Printf("mallory status=%s (penalties doubled, rate halved)\n", engine.BanStatus("mallory", topic))

	fmt.Println("== phase 5: restart, state recovered from disk ==")
	before := engine.Score("alice", topic)
	if err := engine.Close(); err != nil {
		log.Fatal(err)
	}
	store2, err := govern.OpenFileStore(*dir)
	if err != nil {
		log.Fatal(err)
	}
	engine2 := govern.NewEngine(demoParams(), store2, govern.RealClock{})
	if err := engine2.Recover(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice score before=%.1f after-restart=%.1f\n", before, engine2.Score("alice", topic))
	fmt.Printf("mallory offenses persist: status=%s\n", engine2.BanStatus("mallory", topic))
	m := svc.Metrics()
	fmt.Printf("metrics: accepted=%d invalid=%d throttled=%d rejected=%d bans=%d\n",
		m.Accepted, m.Invalid, m.Throttled, m.Rejected, m.Bans)
	_ = engine2.Close()
}
