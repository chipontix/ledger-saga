# ADR-0011: Daily hold limit, checked under the account lock and by the audit

Status: accepted

## Context

An account can carry a cap on how much may leave it per day, whatever its
balance. In this saga money leaves an account at hold: capture only moves
what hold reserved, and nothing after hold can be refused (ADR-0002). So the
limit is a hold rule, and it has to survive what every other hold rule
survives: concurrent holds on one account, duplicates, retries, release
arriving before or after hold, and crashes of every process.

## Decision

- `ledger.accounts.daily_limit` (minor units, `NULL` = no limit). A hold is
  refused with `daily limit exceeded` if what the account's holds already
  took since 00:00 UTC, plus this hold, is over the limit. The saga ends
  `REJECTED` with that reason, like insufficient funds.
- What counts: every hold with outcome `ok` made today, unless its saga was
  released (`release` with reason `released`). A captured hold stays
  counted, also after a refund: the money did leave the account, and a
  refund is not a way to reset the limit. The day is the UTC date of the
  hold's transaction (`operations.at`).
- The sum is computed from `ledger.operations` in `Store.Handle`, not kept
  in a counter. It is read only for holds on accounts with a limit, through
  the partial index `operations (account_id, at) WHERE step = 'hold' AND
  outcome = 'ok'`.
- **It is a statement of its own, after `SELECT … FOR UPDATE` on the
  account.** In READ COMMITTED each statement takes a new snapshot, so once
  the row lock is ours the sum includes every hold committed by whoever held
  the lock before. Written as a subquery of the locking `SELECT`, the sum
  would come from the snapshot taken before the lock wait, and concurrent
  holds would each see room for themselves. That mutation was tried: ten
  concurrent holds of 100 against a limit of 300 gave 4 completed instead of
  3, in 3 runs out of 3 (`TestDailyLimit`).
- `Decide` stays pure: `Account` gets `DailyLimit` and `SpentToday`, filled
  by the store.

**Audit invariant** (`internal/check`, no. 9): for every account with a
limit and every UTC day, unreleased `ok` holds of that day add up to no more
than the limit. It reads only operations and the account row, so it holds at
any instant, not only at rest, and it does not trust `Decide`.

**Crash point `orch.timeout.after_commit`.** The sweeper had a point before
its commit only. The new one kills the orchestrator after a batch of
timeouts (a retried hold or a move to `RELEASE_PENDING` with its `release`
command) is committed, before the sweep returns. Nothing after the commit
is needed for correctness, and the matrix now shows it.

## Tests

- Unit: hold up to the limit passes, one unit over is refused.
- Integration (`TestDailyLimit`): ten concurrent transfers of 100 from an
  account with limit 300 and balance 1000 end 3 `COMPLETED` and 7
  `REJECTED/daily limit exceeded`; one more of 1 is refused, an account
  without a limit is not; the audit is clean. A forged `ok` hold of 1 is
  then reported as `holds took 301 on <day>, daily limit 300`.
- Chaos: the seed gives every third account a limit of 600 against a
  balance of 1000. After every run the audit checks the invariant. The
  matrix fails if the limit refused nothing in any run, so the check is not
  vacuous; each run's count is in its row. A per-run requirement was tried
  first and failed 6 runs out of 65 on an overloaded host (load average
  ~110): nearly every hold timed out and was cancelled before the limit could
  refuse one, while every invariant held. The matrix grows from 60 to 65
  runs with the new point.

## Consequences

- One indexed query per hold on a limited account, under a lock the hold
  already takes. Accounts without a limit pay nothing but a `NULL` column.
- Changing a limit applies to the next hold. The audit checks history
  against the current limit, so lowering a limit below what was already
  taken today shows up as a violation for that day. ponytail: limits are not
  versioned; keep a history table if limits change during the day.
- The day is UTC for everyone. Per-account time zones would need the zone
  on the account and the same expression in the store and the audit.
- The protocol is unchanged: a refusal at hold already existed.
