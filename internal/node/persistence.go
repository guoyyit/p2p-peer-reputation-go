package node

import (
	"encoding/json"
	"sort"

	"github.com/guoyyit/p2p-peer-reputation-go/internal/ban"
	"github.com/guoyyit/p2p-peer-reputation-go/internal/scoring"
)

// dedupEntry identifies one message already judged for a (node, topic).
// Persisting these ensures a crash replay can never score the same message
// twice.
type dedupEntry struct {
	Node     string `json:"node"`
	Topic    string `json:"topic"`
	MsgID    string `json:"msg_id"`
	JudgedAt int64  `json:"judged_at"`
}

// stateV1 is the on-disk governance state. Fields are ordered and slices are
// sorted so persisted bytes are deterministic for the same logical state.
type stateV1 struct {
	Version int              `json:"version"`
	Scores  scoring.Snapshot `json:"scores"`
	Bans    ban.Snapshot     `json:"bans"`
	Seen    []dedupEntry     `json:"seen"`
}

func marshalState(scores *scoring.Board, bans *ban.Manager, seen map[dedupKey]int64) ([]byte, error) {
	entries := make([]dedupEntry, 0, len(seen))
	for k, at := range seen {
		entries = append(entries, dedupEntry{
			Node:     k.node,
			Topic:    k.topic,
			MsgID:    k.msgID,
			JudgedAt: at,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Node != b.Node {
			return a.Node < b.Node
		}
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.MsgID < b.MsgID
	})
	st := stateV1{
		Version: 1,
		Scores:  scores.SnapshotState(),
		Bans:    bans.SnapshotState(),
		Seen:    entries,
	}
	return json.MarshalIndent(st, "", "  ")
}

func unmarshalState(data []byte) (stateV1, error) {
	var st stateV1
	err := json.Unmarshal(data, &st)
	return st, err
}
