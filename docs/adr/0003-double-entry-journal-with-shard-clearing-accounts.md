# ADR-0003: Double-entry journal with per-shard clearing accounts

Status: accepted

## Context

"Money is conserved" must be checkable at any moment, including in the
middle of a crash, not only after everything settles. A transfer in flight
has left the source but not reached the destination; that money has to be
somewhere.

## Decision

Every ledger operation writes a balanced set of journal entries
(`ledger.entries`, legs sum to zero). Customer accounts additionally keep a
materialised `available` / `held` row with `CHECK (>= 0)` constraints, updated
in the same transaction.

System accounts are negative ids and have entries but no balance row:

- `-1` equity: the counter-leg of opening balances;
- `-10 - shard`: the shard's clearing account. Capture credits the source
  shard's clearing account, credit debits the destination shard's, refund
  debits the source shard's.

Every command touches exactly one customer account and the clearing account
of that account's shard, so no transaction crosses a shard boundary
(checked by the chaos checker per operation).

Not keeping a balance row for clearing accounts is deliberate: every capture
and credit on a shard would otherwise update the same row and serialise the
whole shard on one lock.

## Invariants (checked after every chaos run, `internal/check`)

1. The journal sums to zero, and so does every operation.
2. Materialised balances equal the journal per account and bucket.
3. No customer balance is negative (also a DB constraint).
4. At quiescence: nothing is held, the clearing accounts sum to zero, and
   customer money equals the opening total.
5. Each terminal saga left exactly the ledger operations its state allows
   (ADR-0002) and moved exactly `-amount/+amount` (completed) or nothing.

## Consequences

- In-flight money is visible: `sum(clearing)` is the amount captured but
  not yet credited or refunded.
- Balances are a cache of the journal; a bug in the cache is caught by
  invariant 2 rather than silently trusted.
- Amounts are per currency since [ADR-0008](0008-currencies-checked-by-the-ledger-no-conversion.md):
  an operation has one currency, its legs inherit it, and invariants 1 and
  4 hold for each currency separately.
