# ADR-0005: Three layers of idempotency

Status: accepted

## Context

Duplicates come from everywhere: a client retrying a POST whose response it
never saw, a relay re-sending a batch after a crash, a consumer killed after
its transaction committed but before its offset did, the orchestrator
re-sending a command on timeout, the chaos harness on purpose. Each layer
below exists because the layer above cannot see that kind of duplicate.

## Decision

**1. API: `Idempotency-Key`.** Required on `POST /transfers`, stored as a
unique column on the transfer together with a SHA-256 of the request. The
insert is `ON CONFLICT (idem_key) DO NOTHING`; a concurrent request with the
same key waits on the uncommitted row and then reads the winner. Same key and
same body returns the original transfer (200); same key, different body is
409. Crash-tested at both sides of the commit (`api.before_commit`,
`api.after_commit`): the harness retries every failed POST with its key and
asserts one transfer per key.

**2. Transport: inbox table.** Every consumer inserts the message id into
its service's `inbox` in the same transaction as the effect
(`outbox.Seen`). A redelivery after a crash before the Kafka offset commit
finds the id and does nothing. A crash before the database commit forgets
the id along with the effect, so the redelivery is processed for real.

**3. Business: natural keys.** Transport ids are per attempt
(`saga:step:attempt`), so the inbox does not dedup a retry the orchestrator
sent on purpose. The ledger keys every operation by `(saga, step)` (unique
constraint) and records refusals and fences too. A second command for a
recorded step replays the recorded answer instead of deciding again: a
duplicate hold returns the original "insufficient funds" even if the
account has been topped up since. The orchestrator ignores any reply that is
not for the step its current state waits on.

Replies use a deterministic id (`saga:step:reply`), so a replayed reply is
dropped by layer 2 if the original was already applied, and recovers the
saga if the original was lost (e.g. dead-lettered).

## Consequences

- Layer 3 alone would be correct; layer 2 saves a lock and a lookup on the
  common duplicate and gives a cheap `duplicate` metric.
- The inbox grows by one row per message. It only has to remember ids for
  as long as a redelivery is possible (uncommitted offsets, the retry
  topic); layer 3 makes forgetting older ids safe. Retention is not
  implemented: a daily `DELETE … WHERE at < now() - interval '1 day'` would do.
