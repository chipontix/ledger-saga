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

`scripts/bench.sh`, closed-loop clients (each creates a transfer and
long-polls it to a terminal state), 4 vCPU / 4 GiB colima VM on a shared
laptop, services at commit `66ee2b6`. Two passes: A (first) and B (with
Prometheus delivery quantiles and per-container CPU). Their raw files were
in `results/bench/` until 2026-11-05 and are in the git history from `b2c4400`
back.

A third pass on 2026-11-05 (commit `b2c4400`, 6 GiB VM, host load 5.3–7.1)
replaced them in `results/bench/`: CDC 825 / poller 899 sagas/s at 100 clients,
903 / 901 at 1000. The relays are still a tie and the ledger consumer is
still the queue at 1000 clients (README, Numbers).

| | CDC | poller |
|---|---|---|
| 100 clients, sagas/s (A / B) | 768 / 624 | 624† / 615 |
| 100 clients, saga p99 (A / B) | 200 / 439 ms | 490† / 357 ms |
| outbox → ledger consumer, p50 / p99 (B, 100 clients) | 21 / 85 ms | 23 / 73 ms |
| outbox → orchestrator consumer, p50 / p99 (B, 100 clients) | 16 / 68 ms | 16 / 62 ms |
| Postgres CPU at 100 clients (B, 1.0 = one core) | 1.09 | 1.11 |
| 1000 clients, sagas/s (A / B) | 824 / 742 | 825 / 447† |
| added DB transactions per second at idle | ~1 | ~178 |

† the run contains multi-second gaps with (almost) no completions. In pass
B, where `vmstat` was recorded inside the VM, 11 of the 13 gap seconds show
46–98% iowait with block writes near zero: the VM's disk stalled while the
host was swapping. Pass A had no `vmstat`; its gaps look the same (4 s, no
error in any service log). They are in the numbers, not filtered out.

Reading: under load the relay is not the bottleneck and the two are within
run-to-run noise here; at 1000 clients both saturate at the same ~800
sagas/s because commands queue in front of the ledger consumer (B, CDC:
command delivery p50 364 ms against 21 ms for replies), with Postgres at
about 1.2 of 4 cores. The difference that does show is cost at rest: the
poller issues ~178 empty transactions a second for two tables at a 10 ms
interval, forever, per instance; CDC costs a walsender and the slot-retention
risk instead.

Default is CDC (`RELAY_MODE=cdc` in compose). The poller stays for databases
where logical replication is not available.

[go-pq-cdc-kafka]: https://github.com/Trendyol/go-pq-cdc-kafka
