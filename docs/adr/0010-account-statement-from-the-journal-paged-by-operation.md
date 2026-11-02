# ADR-0010: Account statement from the journal, paged by operation id

Status: accepted

## Context

The journal answered "is the book balanced?" (the audit) but not the
question an account holder asks: which operations made up my balance? A
statement is also an independent check on the books. It is built from the
entries alone, so if it ends anywhere other than the balance row, one of the
two is wrong.

## Decision

- The ledger service serves `GET /accounts/{id}/statement?after=&limit=` on
  `HTTP_ADDR` (`:8081`). It is the ledger's own read: the orchestrator does
  not read the ledger schema, and the statement does not go through it.
- A page lists up to `limit` (1–1000, default 100) operations that posted to
  the account, oldest first. Each line has the saga, the step, the change
  to `available` and `held`, and both after the change. Refusals and fences
  post nothing and do not appear. The page also carries the balance row,
  currency and frozen flag, read in the same REPEATABLE READ snapshot, so
  on the last page the last line equals the balance.
- Paging is by operation id (`next` → `?after=`). Ids come from a sequence
  and normally do not follow commit order, so in general keyset paging on
  them can skip a row that commits late with a lower id. Here it cannot.
  Every operation that posts to a customer account first takes that
  account's row lock (`SELECT … FOR UPDATE` in `Store.Handle`), gets its id
  while holding the lock, and keeps it until commit. Per account, ids are
  therefore handed out in commit order, and once a page has shown op `n`,
  no op below `n` can still appear.
- Index `entries (account_id, op_id) WHERE account_id > 0`. Clearing and
  equity accounts take a leg in almost every operation, and nobody reads
  their statement, so they are left out of the index rather than becoming
  its hot page.

## Tests

- Integration (`TestStatementAgreesWithAudit`):
  - concatenated pages of 1, 3 and 1000 lines are the same list;
  - every statement ends at the balance row;
  - a rejected transfer adds no line;
  - a refunded one shows hold, capture, refund;
  - per currency the closing balances add up to the opening total, and the
    audit reports the same customer sum;
  - 404 for an unknown account, 400 for bad parameters.
- Chaos, after every run: every account's statement is read in pages of 7
  through the possibly restarted ledger. Each line must continue the running
  totals of the one before, the last must equal the balance row, and the
  closing balances must add up per currency.

## Consequences

- Read-only, so the saga protocol and the crash points are unchanged.
- One more index insert per customer leg on the write path. The load
  numbers in the README were measured before it and are re-measured with
  the next benchmark run.
- ponytail: the running totals are a window over the account's whole
  history on every page. Fine for thousands of lines. With millions, store
  a per-page opening balance or checkpoint balances.
