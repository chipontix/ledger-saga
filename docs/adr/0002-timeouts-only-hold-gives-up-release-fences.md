# ADR-0002: Timeouts: only hold gives up; release fences the late hold

Status: accepted

## Context

A command or its reply can be delayed arbitrarily (ledger down, consumer
lag, retry topic) or lost (a reply dead-lettered). The orchestrator must not
wait forever, and it must not compensate a step whose outcome it does not
know: "hold timed out" does not mean "hold did not happen".

## Decision

Every pending state has a deadline. The sweeper
(`saga.Store.SweepTimeouts`) picks expired sagas with
`FOR UPDATE SKIP LOCKED` and calls the pure `OnTimeout`:

- **hold**: resend with the next attempt number, up to `HoldAttempts`
  (default 3). Then move to `RELEASE_PENDING` and send `release`.
- **capture, credit, refund, release**: resend forever. These steps are past
  the point where giving up is safe; they cannot be refused by business
  rules, so the only failure is unavailability, which retrying fixes.

Deadline of attempt *n*: `StepTimeout · 2^(n-1)`, capped at `MaxBackoff`,
plus up to 25% jitter. The jitter is a hash of `(saga, attempt)`, not a
random draw, so a replayed saga computes the same schedule.

**Fencing.** `release` is processed under the same per-saga lock as `hold`
and leaves a record whether or not a hold exists:

- hold already applied → undo it (`reason=released`);
- hold refused or not arrived yet → record a tombstone (`reason=tombstone`).

A hold that arrives after a release (it was stuck in a partition, or it is
attempt 2 overtaking attempt 1) sees the release and answers `fenced`
without touching money. Without the tombstone a late hold would reserve
funds for a saga that is already `CANCELLED`, and they would stay held
forever. The property test finds exactly that within a few hundred schedules
if the fence is removed (`money not conserved: ... held 1`).

## Consequences

- A saga only ends `CANCELLED` with zero net effect, whatever order hold and
  release arrive in. Four allowed effect sets, listed in `saga/effects.go`.
- A long ledger outage cancels the sagas still in hold and stalls the rest;
  they complete after recovery. Measured recovery time is in the README.
- Retried commands are distinct messages (`saga:step:attempt`); the ledger
  dedups them on `(saga, step)`, not on the transport id (ADR-0005).
