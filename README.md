# P2P Peer Reputation & Connection Governance

A local, runnable Go service that simulates a P2P mesh and governs
connections based on per-node behavior. Peers forward transaction/block
messages over an in-memory (loopback-style) transport; the service scores
reputation, rate-limits, flood-isolates, bans and quarantines misbehaving
nodes, and persists all governance state so a process restart resumes
seamlessly.

## Run

```sh
go test ./...          # full automated suite
go run .               # deterministic demo, writes data/gov-state.json
go run . -restart      # resume from the persisted state file
go run . -state <path> # use a different state file
```

The demo uses a manually advanced virtual clock, so its output is identical
on every run.

## Packages

| Package | Responsibility |
| --- | --- |
| `internal/protocol` | Node identity, topic, message/envelope types |
| `internal/scoring` | Per-(node, topic) integer reputation scores with time decay |
| `internal/ban` | Ban → probation → clear state machine with exponential backoff |
| `internal/store` | Atomic, file-backed state persistence |
| `internal/transport` | In-memory P2P fabric: join/leave, topic fan-out, bounded queues |
| `internal/gov` | Connection quota, token-bucket rate limit, flood protection + counters |
| `internal/node` | Runtime that wires the pipeline, relay loop, recovery and snapshots |

## Scoring model

Scores are kept independently for every `(node, topic)` pair and use integer
arithmetic only (no floating point), which makes evolution bit-for-bit
deterministic across machines and restarts.

Each inbound message is judged once and produces one delta:

| Verdict | Effect |
| --- | --- |
| valid + timely | `+ValidReward` |
| valid but late | `+LateReward` |
| invalid payload | `-InvalidPenalty` |
| duplicate id | `-DuplicatePenalty` |
| over rate quota | `-ExcessPenalty` |

Scores are clamped to `[MinScore, MaxScore]` and decay linearly toward zero:
every `DecayIntervalMillis` the score moves `DecayStep` closer to zero.
Timeliness uses the message `CreatedAt` vs arrival time; a message older than
`LateMillis` earns only the small late reward.

When a score falls below `LowScoreThreshold`, the node is **banned on that
topic**, its transport connection is dropped, and flood state is cleared.

Default parameters live in `scoring.DefaultConfig()` and are tunable via
`node.Config`.

## Ban, backoff and probation

Per `(node, topic)` the state machine is:

```
clear ──low score──▶ banned ──ban expires──▶ probation ──clean window──▶ clear
                          │                     │
                          └──re-offense doubles──┘  (exponential backoff)
```

- First ban lasts `BaseBanMillis`.
- Re-offending **while banned or in probation** doubles the previous ban
  duration, capped at `MaxBanMillis`.
- Completing probation cleanly resets the offense count; a later offense
  starts again from the base duration.
- Time transitions cascade in a single update, so jumping far forward lands
  in the correct terminal state without requiring intermediate ticks.

## Topic isolation

Scores, rate buckets, flood state and bans are all keyed by `(node, topic)`.
A node banned for flooding transaction messages still relays and accrues
reputation normally on the block topic. Subscriptions in the transport are
also topic-scoped.

## Quotas, rate limiting and flood protection

- **Connection quota** (`gov.Config.MaxConnsPerNode`): an identity may hold
  at most N concurrent connections; excess joins are rejected.
- **Rate limit**: a fixed-window token bucket per `(node, topic)`
  (`RateTokens` / `RateWindowMillis`). Over-quota messages are rejected,
  scored as excess, and never forwarded.
- **Flood protection**: bad (invalid/duplicate/excess) messages within
  `FloodWindowMillis` reaching `FloodBadThreshold` engage flood isolation for
  that `(node, topic)`. While engaged, the node's traffic is dropped at the
  head of the pipeline before it can consume processing or queue bandwidth.
  Engagement is observable via `gov.Counter` (`FloodTripped`,
  `FloodEngaged`, `WindowBad`, `RateLimited`, `Bad`, `Accepted`).

Scoring-driven bans and flood isolation are deliberately **independent**:
engaging flood protection does not suppress scoring, so a flooder still
reaches the ban threshold; conversely a low-score ban disconnects even before
the flood threshold is met.

The transport additionally protects healthy peers: a saturated receive
queue yields `transport.ErrQueueFull` for the slow peer only, while delivery
to every other subscriber still succeeds (head-of-line avoidance).

## Persistence and crash recovery

All governance state — scores, ban records and the judged-message set — is
serialized as sorted JSON (`node/stateV1`) and written atomically
(temp file + `fsync` + rename).

- State is flushed on shutdown and throttled during steady state
  (`PersistEveryMillis`); `Node.ForcePersist` flushes immediately.
- On startup a node restores scores and bans before processing traffic, so
  neither is reset or amplified by a restart.
- The persisted dedup set is keyed by `(node, topic, message id)`. Replaying
  a message after recovery is recognized as a duplicate and is never accepted
  (and never re-added) as a brand-new message.
- Entries older than `DedupTTLMillis` are pruned to bound memory.

## Dynamic membership and identity binding

Reputation is bound to the stable `NodeID`, not the transport connection.
- `Leave`/disconnect recycles the connection and subscriptions; an offline
  node releases its connection slot.
- Re-`Join`ing the same identity gets a fresh connection but retains score,
  ban and probation state — reconnecting cannot evade a ban.

## Determinism

Every decision is a pure function of the ordered event stream plus an
injected millisecond clock (`Options.Clock`):
- integer scores and integer decay steps (no float/non-deterministic map
  ordering affects output);
- snapshot slices are sorted before serialization, so identical logical state
  produces identical bytes;
- `Node.HandleEnvelope` processes one message into one reproducible `Event`
  (verdict, resulting score, ban flag), enabling replay and auditing.

`TestDeterministicReplay` runs the same stream through independent nodes and
asserts identical per-step scores; recovery, reconnect and flood tests
exercise the durability and governance guarantees.
