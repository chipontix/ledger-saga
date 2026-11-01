# ADR-0008: Currencies are checked by the ledger; no conversion

Status: accepted

## Context

Until now every amount was a bare integer. "Money is conserved" was checked
as one sum over all accounts, so if 100 EUR was taken from one account and
100 USD was paid into another, the audit would still pass. Accounts on
different shards can hold different currencies, and the orchestrator never
reads the ledger schema, so it cannot tell which currency an account holds.

## Decision

- Every account has a currency (`ledger.accounts.currency`, ISO 4217
  shape enforced by a `CHECK`). Every ledger operation has exactly one
  currency (`ledger.operations.currency`). Journal entries carry no currency
  of their own; they inherit it from their operation. That is why the
  clearing and equity accounts, which have no balance row, can hold several
  currencies without mixing them.
- A transfer is denominated by the client: `POST /transfers` requires
  `"currency"`, and there is no default. An implicit currency in a money API
  is a bug waiting to happen. The currency is part of the idempotency hash,
  so the same key with another currency is a 409.
- The orchestrator only copies the currency into every command
  (`wire.Command.Currency`). The ledger, which owns the accounts, checks it
  inside `Decide`:
  - **hold**: if the source is in another currency, the hold is rejected
    with `currency mismatch`, so nothing moves and the saga ends `REJECTED`.
  - **credit**: if the destination is in another currency, the credit is
    rejected with `currency mismatch`, so the saga refunds and ends
    `REFUNDED`. This is the same path as a frozen or missing destination.
  - capture, refund and release only touch the source, whose currency the
    hold already checked.
- No conversion. Conversion needs a rate, a rate needs a source and a
  timestamp, and both need an FX position account per currency pair. That is
  a different project. Refusing the transfer is the safe answer.

## Invariants added to the audit (`internal/check`)

5. Every customer-account leg is in that account's currency.
6. Money is conserved per currency: customer balances plus clearing equal
   what was opened against equity, and at rest clearing is zero, for each
   currency separately.
7. (Extended.) Every ledger operation of a saga is in the saga's currency.

The property test draws account currencies and sometimes lets the client
name the wrong one. Removing the check from either the hold or the credit
branch of `Decide` fails on the first generated schedule ("credit USD posted
to account 3 in EUR"). An integration test writes a forged EUR operation that
balances but pays a USD account, and the audit reports both the leg and the
USD/EUR imbalance.

## Crash matrix

The steps and their commit boundaries did not change. The new check happens
inside the same ledger transaction as the existing ones, so there is no new
crash point. What changed is the workload: chaos runs seed accounts in USD
and EUR, about one in five transfers crosses currencies and must refund,
and one in twenty names the wrong currency and must be rejected. So every
run of the matrix exercises both new refusals under crashes.

## Consequences

- Rolling upgrade: a command from an older orchestrator has no currency.
  The ledger rejects its hold, which is safe because nothing moves, or
  rejects its credit, which leads to a refund. Money is never posted in an
  unknown currency. Drain in-flight sagas before upgrading if those refunds
  matter.
- The benchmarks seed a single currency (`seed -currencies` defaults to
  `USD`), so their numbers stay comparable with earlier runs. Currency adds
  one column read under a row lock that is already held.
- The schema changed (new `NOT NULL` columns). Migrations are
  `CREATE ... IF NOT EXISTS`, so an existing database needs to be recreated
  (`make down`).
