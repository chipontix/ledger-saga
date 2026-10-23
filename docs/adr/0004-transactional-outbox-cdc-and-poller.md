# ADR-0004: Transactional outbox, shipped by CDC or by a poller

Status: accepted

## Context

Every state change must produce its message exactly when it commits: a
saga that moved to `CAPTURE_PENDING` without its capture command is stuck,
and a capture command for a transition that rolled back is a double effect
waiting to happen. Writing to Postgres and to Kafka from the same handler
cannot be atomic.

## Decision

Services never produce to Kafka. The message is an `INSERT` into the
service's own `outbox` table in the same transaction as the state change
(`outbox.Put`). A separate relay process ships committed rows to Kafka. Two
relays implement the same contract (at-least-once, no ordering promise
across keys):

**Poller** (`relay/poll.go`). One transaction: `SELECT … ORDER BY id LIMIT
500 FOR UPDATE SKIP LOCKED`, `WriteMessages` with `acks=all`, `DELETE`,
commit. A crash after the Kafka write and before the commit re-sends the
batch. Sleeps `POLL_INTERVAL` (10 ms) when the table is empty.

**CDC** (`relay/cdc.go`) with [go-pq-cdc-kafka]. A logical replication slot
streams the outbox `INSERT`s from the WAL; the connector acknowledges an LSN
only after the Kafka write containing it succeeded, so a crash replays from
the last acknowledged position. The table is append-only and trimmed by age;
the publication carries inserts only, so the trim is not replicated, and
decoding reads rows from the WAL, so trimming a row the slot has not reached
is safe.

Both modes are crash-tested at the same two boundaries: before the Kafka
write and after it but before the commit/ack (ADR-0007).

## Trade-offs

| | poller | CDC |
|---|---|---|
| extra DB load | a query every interval per table, plus a `DELETE` per batch (dead tuples, vacuum) | one walsender; no queries on the hot path |
| latency floor | poll interval | WAL decode + producer flush tick |
| ordering | by `id`, which is not commit order (sequence values are taken before commit) | commit order |
| operational risk | none beyond the table | a stalled slot retains WAL until the disk fills; needs `max_slot_wal_keep_size` and a lag alert |
| needs | nothing | `wal_level=logical`, a replication-capable role |

Neither ordering guarantee is relied on: the protocol is order-insensitive
by construction (ADR-0002, ADR-0005), and the chaos harness reorders on
purpose.

## Measurements

See "CDC vs poller" in the README; numbers come from `make bench` runs on
the hardware listed there.

[go-pq-cdc-kafka]: https://github.com/Trendyol/go-pq-cdc-kafka
