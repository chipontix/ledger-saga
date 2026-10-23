# ADR-0006: Retry ladder and dead-letter topic

Status: accepted

## Context

A handler can fail for reasons that fix themselves (Postgres restarting, a
deadlock victim, a connection reset) and reasons that never will (an
undecodable payload, a reply for a saga that does not exist). Blocking the
partition on a poison message stops every account on it; dropping a message
on a transient error loses money movement.

## Decision

Consumers use [kafka-konsumer] with a three-rung ladder
(`internal/consumer`):

1. **In process**: up to 4 attempts with exponential backoff and full
   jitter (25 ms, 100 ms, 400 ms). Covers the common blips without leaving
   the partition.
2. **Retry topic** (`<topic>.retry`): still failing → the message is
   produced to the retry topic and the main offset moves on. kafka-konsumer's
   cron consumer re-reads it on a schedule (`RETRY_EVERY`, default 10 s) with
   exponential backoff, up to 3 times.
3. **Dead-letter topic** (`<topic>.dlq`): `consumer.Permanent` errors go
   there directly; so do messages that exhausted the retry topic. The error
   text travels in the `x-error-message` header.

Offsets are committed after the whole in-flight batch has been handled, so
a crash anywhere on this ladder redelivers.

Library settings that are not defaults, each found by the crash harness
(ADR-0007):

- `TransactionalRetry: false`. kafka-konsumer's default re-runs a failed
  message once immediately; rung 1 already did that, with backoff.
- `ProducerRequiredAcks: RequireAll`. The retry producer defaults to
  `acks=none`: a message moved to the retry topic could be lost in flight.
- `ProducerBatchTimeout: 1ms`. The default 1 s made every failed message
  block its worker for a second, and with it the whole in-flight batch.
- The dead-letter producer's batch timeout is not configurable in
  kafka-konsumer v2.6.19, so each dead-lettered message still costs ~1 s of
  stall. Acceptable for a rung that only poison messages reach.

## Why this is safe for a saga

Moving a message to the retry topic reorders it behind later messages for
the same key. That is only acceptable because no handler depends on order:
release fences a late hold (ADR-0002), the ledger replays recorded steps and
the orchestrator ignores replies its state does not wait for (ADR-0005).

A dead-lettered **command** is not lost money: the orchestrator times out
and re-sends it with a new attempt id. A dead-lettered **reply** is recovered
the same way: the re-sent command hits a recorded step and the ledger
replays the reply. The DLQ is for humans to inspect, not a place where
transfers silently die.

[kafka-konsumer]: https://github.com/Trendyol/kafka-konsumer
