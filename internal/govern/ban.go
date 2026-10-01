package govern

// BanRecord is the persisted ban state of one (peer, topic) pair.
type BanRecord struct {
	Until            int64 // ban expiry, unix millis
	Offenses         int   // lifetime offense count, drives backoff
	ObservationUntil int64 // observation-period expiry, unix millis
}

// BanTracker keeps per-(peer, topic) ban and observation state.
type BanTracker struct {
	records map[PeerID]map[Topic]*BanRecord
}

func NewBanTracker() *BanTracker {
	return &BanTracker{records: make(map[PeerID]map[Topic]*BanRecord)}
}

// Status reports whether the pair is normal, banned, or under observation.
func (b *BanTracker) Status(peer PeerID, topic Topic, now int64) BanStatus {
	if rec, ok := b.get(peer, topic); ok {
		if now < rec.Until {
			return StatusBanned
		}
		if now < rec.ObservationUntil {
			return StatusObservation
		}
	}
	return StatusNormal
}

// Ban imposes a new ban with exponential backoff based on prior offenses.
func (b *BanTracker) Ban(peer PeerID, topic Topic, now int64, p Params) BanRecord {
	rec := b.record(peer, topic)
	rec.Offenses++
	duration := p.BaseBanDuration
	for i := 1; i < rec.Offenses && duration < p.MaxBanDuration; i++ {
		duration *= 2
	}
	if duration > p.MaxBanDuration {
		duration = p.MaxBanDuration
	}
	rec.Until = now + duration.Milliseconds()
	rec.ObservationUntil = rec.Until + p.ObservationPeriod.Milliseconds()
	return *rec
}

// Snapshot exports a deep copy of all records for persistence.
func (b *BanTracker) Snapshot() map[PeerID]map[Topic]BanRecord {
	out := make(map[PeerID]map[Topic]BanRecord, len(b.records))
	for peer, topics := range b.records {
		cp := make(map[Topic]BanRecord, len(topics))
		for topic, rec := range topics {
			cp[topic] = *rec
		}
		out[peer] = cp
	}
	return out
}

// Restore loads a previously exported snapshot.
func (b *BanTracker) Restore(data map[PeerID]map[Topic]BanRecord) {
	b.records = make(map[PeerID]map[Topic]*BanRecord, len(data))
	for peer, topics := range data {
		cp := make(map[Topic]*BanRecord, len(topics))
		for topic, rec := range topics {
			r := rec
			cp[topic] = &r
		}
		b.records[peer] = cp
	}
}

func (b *BanTracker) get(peer PeerID, topic Topic) (*BanRecord, bool) {
	topics, ok := b.records[peer]
	if !ok {
		return nil, false
	}
	rec, ok := topics[topic]
	return rec, ok
}

func (b *BanTracker) record(peer PeerID, topic Topic) *BanRecord {
	topics, ok := b.records[peer]
	if !ok {
		topics = make(map[Topic]*BanRecord)
		b.records[peer] = topics
	}
	rec, ok := topics[topic]
	if !ok {
		rec = &BanRecord{}
		topics[topic] = rec
	}
	return rec
}
