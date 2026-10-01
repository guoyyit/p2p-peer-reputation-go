package govern

import "time"

// PeerID identifies a node by identity (not by connection).
type PeerID string

// Topic is a message classification channel; scores and bans are per-topic.
type Topic string

// Verdict classifies an inbound message for scoring purposes.
type Verdict int

const (
	VerdictValid Verdict = iota
	VerdictInvalid
	VerdictDuplicate
	VerdictExcess
)

func (v Verdict) String() string {
	switch v {
	case VerdictValid:
		return "valid"
	case VerdictInvalid:
		return "invalid"
	case VerdictDuplicate:
		return "duplicate"
	case VerdictExcess:
		return "excess"
	default:
		return "unknown"
	}
}

// Message is the unit of gossip between peers.
type Message struct {
	ID        string
	From      PeerID
	Topic     Topic
	Payload   []byte
	Timestamp int64 // sender-side unix millis, used for timeliness checks
}

// Action is the governance decision attached to a scoring outcome.
type Action int

const (
	ActionNone Action = iota
	ActionDisconnect
	ActionBan
)

func (a Action) String() string {
	switch a {
	case ActionNone:
		return "none"
	case ActionDisconnect:
		return "disconnect"
	case ActionBan:
		return "ban"
	default:
		return "unknown"
	}
}

// Decision is the result of governing one message or connection attempt.
type Decision struct {
	Accepted    bool
	Reason      string
	Verdict     Verdict
	Score       float64
	Action      Action
	BannedUntil int64
}

// BanStatus describes where a (peer, topic) pair sits in the ban lifecycle.
type BanStatus int

const (
	StatusNormal BanStatus = iota
	StatusBanned
	StatusObservation
)

func (s BanStatus) String() string {
	switch s {
	case StatusNormal:
		return "normal"
	case StatusBanned:
		return "banned"
	case StatusObservation:
		return "observation"
	default:
		return "unknown"
	}
}

// Params tunes the scoring and governance model. All fields are part of the
// deterministic contract: identical Params + identical event sequence yield
// identical scores and decisions.
type Params struct {
	HalfLife                 time.Duration // score decay half-life
	LateThreshold            time.Duration // valid messages older than this are "late"
	ValidWeight              float64       // reward for a valid, timely message
	LatePenalty              float64       // penalty for a valid but late message
	InvalidWeight            float64       // penalty for an invalid message
	DuplicateWeight          float64       // penalty for a duplicate message
	ExcessWeight             float64       // penalty for a rate-limited message
	MaxScore                 float64
	MinScore                 float64
	BanThreshold             float64       // score <= threshold triggers ban
	BaseBanDuration          time.Duration // first-offense ban length
	MaxBanDuration           time.Duration // backoff cap
	ObservationPeriod        time.Duration // post-ban probation length
	ObservationPenaltyFactor float64       // penalty multiplier during observation
	SeenTTL                  time.Duration // dedup cache retention
	SnapshotEvery            int           // events between persisted snapshots
}

// DefaultParams returns a sane local default configuration.
func DefaultParams() Params {
	return Params{
		HalfLife:                 time.Hour,
		LateThreshold:            5 * time.Second,
		ValidWeight:              1,
		LatePenalty:              -0.5,
		InvalidWeight:            -10,
		DuplicateWeight:          -1,
		ExcessWeight:             -2,
		MaxScore:                 100,
		MinScore:                 -100,
		BanThreshold:             -20,
		BaseBanDuration:          time.Minute,
		MaxBanDuration:           30 * time.Minute,
		ObservationPeriod:        2 * time.Minute,
		ObservationPenaltyFactor: 2,
		SeenTTL:                  10 * time.Minute,
		SnapshotEvery:            100,
	}
}
