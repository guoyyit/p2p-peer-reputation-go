package govern

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	walFileName      = "wal.log"
	snapshotFileName = "snapshot.json"
)

// EventKind enumerates persisted governance events.
type EventKind string

const (
	KindMessage    EventKind = "message"
	KindConnect    EventKind = "connect"
	KindDisconnect EventKind = "disconnect"
)

// Event is one WAL record: the deterministic input to the scoring engine.
type Event struct {
	Seq          uint64
	Kind         EventKind
	Peer         PeerID
	Topic        Topic
	MsgID        string
	Verdict      Verdict
	Timestamp    int64 // governance processing time, unix millis
	MsgTimestamp int64 // sender-side timestamp, for timeliness checks
}

// Snapshot is the point-in-time persisted engine state.
type Snapshot struct {
	Seq    uint64
	Scores map[PeerID]map[Topic]ScoreEntry
	Bans   map[PeerID]map[Topic]BanRecord
	Seen   map[string]int64 // msgID -> expiry unix millis
}

// FileStore persists snapshots and a write-ahead log under a directory.
type FileStore struct {
	dir string
	wal *os.File
	enc *json.Encoder
}

// OpenFileStore creates the directory if needed and opens the store.
func OpenFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("govern store: mkdir: %w", err)
	}
	wal, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("govern store: open wal: %w", err)
	}
	return &FileStore{dir: dir, wal: wal, enc: json.NewEncoder(wal)}, nil
}

// Append writes one event to the WAL and fsyncs it.
func (s *FileStore) Append(e Event) error {
	if err := s.enc.Encode(e); err != nil {
		return fmt.Errorf("govern store: append wal: %w", err)
	}
	return s.wal.Sync()
}

// LoadSnapshot reads the latest snapshot, or nil if none exists.
func (s *FileStore) LoadSnapshot() (*Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, snapshotFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("govern store: read snapshot: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("govern store: decode snapshot: %w", err)
	}
	return &snap, nil
}

// SaveSnapshot atomically writes a snapshot and truncates the WAL.
func (s *FileStore) SaveSnapshot(snap *Snapshot) error {
	tmp := filepath.Join(s.dir, snapshotFileName+".tmp")
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("govern store: encode snapshot: %w", err)
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("govern store: write snapshot: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, snapshotFileName)); err != nil {
		return fmt.Errorf("govern store: rename snapshot: %w", err)
	}
	// Snapshot is durable; the WAL up to snap.Seq is redundant.
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("govern store: close wal: %w", err)
	}
	wal, err := os.OpenFile(filepath.Join(s.dir, walFileName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("govern store: truncate wal: %w", err)
	}
	s.wal = wal
	s.enc = json.NewEncoder(wal)
	return nil
}

// Replay invokes fn for every WAL event with Seq > fromSeq, in order.
func (s *FileStore) Replay(fromSeq uint64, fn func(Event) error) error {
	f, err := os.Open(filepath.Join(s.dir, walFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("govern store: open wal for replay: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var e Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return fmt.Errorf("govern store: decode wal entry: %w", err)
		}
		if e.Seq <= fromSeq {
			continue
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("govern store: scan wal: %w", err)
	}
	return nil
}

// Close flushes and closes the store.
func (s *FileStore) Close() error {
	if s.wal == nil {
		return nil
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return s.wal.Close()
}
