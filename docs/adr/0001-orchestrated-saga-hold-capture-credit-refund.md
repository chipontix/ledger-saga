# ADR-0001: Orchestrated saga: hold → capture → credit, refund as compensation

Status: accepted

## Context

A transfer moves money between two accounts that may live on different
ledger shards. A shard only commits transactions that touch its own rows, so
a cross-shard transfer cannot be one ACID transaction, and two-phase commit
across shards would couple their availability (a coordinator crash holds
locks on both sides).

## Decision

A transfer is an **orchestrated** saga. The orchestrator owns the saga row
and its state machine (`internal/saga/machine.go`). The ledger is a passive
participant that executes one command at a time in one local transaction
(`internal/ledger`). They only communicate through Kafka.

Steps:

| step    | shard       | effect                                   | if refused                  |
|---------|-------------|------------------------------------------|-----------------------------|
| hold    | source      | available → held                         | saga `REJECTED`, nothing to undo |
| capture | source      | held → shard clearing account            | cannot be refused (bug)     |
| credit  | destination | destination clearing → destination       | compensate with refund      |
| refund  | source      | source clearing → available              | cannot be refused (bug)     |
| release | source      | held → available, fences a late hold     | n/a                         |

States: `HOLD_PENDING → CAPTURE_PENDING → CREDIT_PENDING → COMPLETED`, with
`HOLD_PENDING → REJECTED`, `HOLD_PENDING → RELEASE_PENDING → CANCELLED` (hold
timeout, ADR-0002) and `CREDIT_PENDING → REFUND_PENDING → REFUNDED`. Every
legal edge is listed in `saga.Edges`; the chaos checker replays each saga's
transition log against it.

Why hold before capture instead of debiting immediately: the hold is the
only step that is cheap to undo (`release` never shows up as a debit on a
statement) and it is where the funds check happens. Capture is the point of
no return on the source side; after it, the only way back is a refund, which
is a new entry, never a deletion. This mirrors card authorisation/capture/
refund and gives a natural place for the only timeout that is allowed to
give up (hold).

In saga terms, hold is compensatable (by release), capture is compensatable
(by refund), credit is the pivot: once it succeeds the saga can only go
forward. Refund and capture are retriable: they cannot be refused by business
rules, so they are retried until they succeed.

Why orchestration over choreography: the transfer has a compensation branch
and timeouts. With choreography the "what happens next" logic is spread over
event handlers in two services and nobody owns the timeout. With an
orchestrator the whole machine is one pure function, testable without I/O
(`internal/sim` runs it against an adversarial network with rapid).

## Consequences

- The orchestrator is the single owner of "where is this transfer"; GET
  `/transfers/{id}` reads one row.
- The ledger never needs to know about sagas beyond the `(saga, step)`
  natural key it records (ADR-0005).
- A transfer to a frozen or missing account costs four ledger operations
  (hold, capture, credit refused, refund) instead of failing fast. Accepted:
  the destination shard is the only one that knows the destination's state.
