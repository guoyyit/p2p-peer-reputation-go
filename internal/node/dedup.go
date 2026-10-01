package node

import (
	"sync"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/protocol"
)

// dedupKey identifies a judged message within a node/topic pair.
type dedupKey struct {
	node  string
	topic string
	msgID string
}

// dedupSet remembers message IDs already judged so duplicates are detected
// within the process and across restarts.
type dedupSet struct {
	mu   sync.Mutex
	seen map[dedupKey]int64
}

func newDedupSet() *dedupSet {
	return &dedupSet{seen: make(map[dedupKey]int64)}
}

// add marks msgID as seen at time now. It returns false if it was already
// present (a duplicate), without updating its timestamp.
func (d *dedupSet) add(node protocol.NodeID, topic protocol.Topic, msgID protocol.MsgID, now int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	k := dedupKey{string(node), string(topic), string(msgID)}
	if _, ok := d.seen[k]; ok {
		return false
	}
	d.seen[k] = now
	return true
}

// snapshot returns the underlying map under lock.
func (d *dedupSet) snapshot() map[dedupKey]int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[dedupKey]int64, len(d.seen))
	for k, v := range d.seen {
		out[k] = v
	}
	return out
}

// restore replaces contents.
func (d *dedupSet) restore(entries []dedupEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen = make(map[dedupKey]int64, len(entries))
	for _, e := range entries {
		d.seen[dedupKey{e.Node, e.Topic, e.MsgID}] = e.JudgedAt
	}
}

// prune drops entries older than now-ttlMillis to bound memory.
func (d *dedupSet) prune(now, ttlMillis int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cutoff := now - ttlMillis
	for k, at := range d.seen {
		if at < cutoff {
			delete(d.seen, k)
		}
	}
}
