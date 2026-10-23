# ADR-0007: Crash harness: real processes, SIGKILL at named points

Status: accepted

## Context

Unit and property tests (`internal/sim`) prove the protocol against an
adversarial network, but they model each handler as atomic. What they
cannot show is that the real code keeps that atomicity: that the outbox row
really is in the same transaction, that an offset is really committed after
the effect, that a relay really re-sends after a crash. Those are properties
of processes, Postgres and Kafka together, so they are tested with processes,
Postgres and Kafka.

## Decision

`ledgersaga chaos` (`cmd/ledgersaga/chaos.go`) runs, for every crash point in
`failpoint.Points` plus a no-crash baseline, and for every seed:

1. a fresh database and fresh topics (names unique per invocation, so a
   failed run's leftovers cannot leak into the next one);
2. orchestrator, ledger and relay as separate OS processes, with
   `FAILPOINT=<point>:<n>` in the one that owns the point. `failpoint.Hit`
   sends `SIGKILL` to its own process on the n-th pass: no deferred
   functions, no graceful shutdown, no flushed buffers;
3. a seeded workload through the HTTP API, paced so the crash lands in the
   middle of it. A POST that gets no answer is retried with its key, the way
   a real client would; one in ten is re-sent after success and must return
   the same transfer;
4. a supervisor that expects exactly one SIGKILL from the armed process,
   restarts it after a seeded delay without the failpoint, and reports any
   other exit;
5. on both consumers, in every run: 10% of saga steps have every first
   delivery deferred through the retry topic (chosen by a hash of seed and
   saga+step, so all attempts of a step are late together and its
   compensation is not), 5% duplicated, 2% held back inside the batch, 5%
   redelivered up to 1.5 s later;
6. wait until every saga is terminal, then the invariant audit
   (`internal/check`) plus the harness's own idempotency checks.

The seed fixes the workload, the chaos decisions, the hit index and the
restart delay. The interleaving of concurrent goroutines is not fixed, so a
seed is a reproducible scenario, not a reproducible schedule; the matrix
compensates with breadth. The exhaustive, shrinkable schedule search is the
property test's job.

## The harness is tested too

A checker that never fails proves nothing. Removing the release fence in
`ledger.Decide` (one `if`) must make runs fail. It did not at first: hold
and release of one saga share a partition (keyed by account), so in
practice they were never reordered, and the property test was the only
thing that exercised the fence. Three changes made the harness see it:

- deterministic per-step deferral through the retry topic (above);
- kafka-konsumer's `TransactionalRetry` (default on) re-ran a failed
  message immediately, which hid most deferrals; it is off now, since
  rung 1 of the retry ladder is ours (ADR-0006);
- the retry consumer works 1 s of every 5 s in chaos runs, so a deferred
  message really is late.

With the fence removed, 6 of 8 runs (baseline and ledger-crash, 4 seeds each)
now fail with `ledger effects map[hold:ok release:ok/tombstone] not allowed
for CANCELLED`, i.e. funds held for a cancelled transfer.

## Findings the harness produced

- **Timeouts right after a restart.** A restarted orchestrator ran its
  timeout sweeper while its reply consumer was still waiting for the group
  rebalance (session timeout, 6 s). It timed out holds whose successful
  replies were sitting unread in Kafka and cancelled almost every transfer:
  safe (all invariants held) but wrong. The sweeper now waits
  `TIMEOUT_GRACE` (default 10 s, ≥ session timeout) after start.
- **Retry producer defaults.** kafka-konsumer's retry producer defaults to
  `acks=none` and a 1 s batch timeout. The first can lose a message on its
  way to the retry topic; the second blocked a worker for a second per
  failed message and, through the batch barrier, the whole consumer. Both
  are set explicitly now (ADR-0006).
- **Head-of-line blocking.** kafka-konsumer processes `Concurrency`
  messages and commits after all of them. One slow message stalls the
  batch; at 10% delayed messages throughput collapsed and the retry storm
  cancelled most transfers. The production consequence: handler latency
  outliers cost throughput, not just latency.
