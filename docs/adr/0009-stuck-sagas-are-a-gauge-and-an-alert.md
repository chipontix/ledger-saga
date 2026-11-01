# ADR-0009: Stuck sagas are a gauge and an alert

Status: accepted

## Context

Two earlier decisions trade a wrong answer for a saga that never finishes:

- after hold, every step retries forever (ADR-0002), because giving up there
  is unsafe;
- `OnReply` ignores a refused capture, refund or release, because those
  refusals can only come from a bug. It "keeps waiting so it surfaces as a
  stuck saga rather than a silent wrong terminal".

Nothing surfaced it, though. A saga stuck in `CREDIT_PENDING` was visible
only to someone who already knew its id, or to the chaos harness, which
fails a run that does not reach quiescence.

## Decision

- The orchestrator runs `Store.Stuck` every `STUCK_EVERY` (5 s). It is one
  query over the open transfers (`finished_at IS NULL`, the same rows the
  sweeper's partial index covers), and it exports:
  - `saga_stuck{state}`: open transfers accepted more than `STUCK_AFTER`
    (60 s) ago. It is set for every waiting state, so a cleared state drops
    to 0 instead of keeping its last value;
  - `saga_oldest_open_seconds`: the age of the oldest open transfer.
- It is a periodic query and not a counter derived from transitions,
  because "older than X" is a question about the current set of open rows.
  A counter cannot answer it after a restart, and neither can a gauge that
  is incremented and decremented in memory.
- The age is counted from acceptance, not from the last transition. A saga
  that keeps retrying the same step has fresh transitions and is still
  stuck.
- Alerts (`deploy/prometheus/alerts.yml`):
  - `SagaStuck`: `max by (state) (saga_stuck) > 0` for 1 m. It uses max
    because every orchestrator replica reads the same table and reports the
    same number; sum would multiply it.
  - `SagaStuckDetectorAbsent`: the gauge is absent for 2 m. Otherwise a
    dead orchestrator, or a query that keeps failing, would silence the
    alert it exists to raise.
- Thresholds: a healthy saga finishes in well under a second (p99 below
  0.5 s at 100 clients), and the 1000-client run, which is saturated, has
  p99 under 3 s. Hold gives up after three attempts, about 15 s. 60 s is far
  above both, so the alert means an outage or a bug, not load.

## Tests

- `promtool test rules deploy/prometheus/alerts.test.yml` (in CI): pending
  at 45 s, firing at 60 s with the state label, one alert for two replicas,
  and the absence alert.
- Integration (`TestStuckSagasAreCounted`): with no relay nothing is
  delivered, and a transfer accepted two minutes ago is counted and
  exported. A fresh one and a finished one are not.
- Chaos: at the end of every run, the restarted orchestrator must export
  `saga_oldest_open_seconds` and it must read 0. This tells "the detector
  works after a crash" apart from "the detector is not running".

## Consequences

- It reads, it does not change state: no new crash point, and the saga
  protocol is unchanged.
- Cost: one query every 5 s over the open transfers only (a few hundred
  rows at 1000 clients).
- It says that sagas are stuck, not why. The attempt counter on
  `GET /transfers/{id}` and the DLQ topics answer that.
