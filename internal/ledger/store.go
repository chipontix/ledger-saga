package ledger

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chipontix/ledger-saga/internal/failpoint"
	"github.com/chipontix/ledger-saga/internal/outbox"
	"github.com/chipontix/ledger-saga/internal/wire"
)

type Store struct {
	DB      *pgxpool.Pool
	Replies string // topic
}

// Handle executes one command in one transaction and stages the reply. It
// returns the consumer result label.
//
// Lock order: per-saga advisory lock, then the account row. The advisory
// lock serialises hold/release/capture/refund of one saga even when the
// account does not exist (no row to lock); the row lock serialises sagas
// touching the same account. Both are taken in the same order by every
// command, so they cannot deadlock.
func (s *Store) Handle(ctx context.Context, c wire.Command) (string, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if dup, err := outbox.Seen(ctx, tx, outbox.Ledger, c.MsgID); err != nil || dup {
		return "duplicate", err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, c.SagaID.String()); err != nil {
		return "", err
	}
	rows, err := tx.Query(ctx, `SELECT step, outcome, reason FROM ledger.operations WHERE saga_id = $1`, c.SagaID)
	if err != nil {
		return "", err
	}
	prior := Prior{}
	var replay *Decision
	for rows.Next() {
		var step wire.Step
		var d Decision
		if err := rows.Scan(&step, &d.Outcome, &d.Reason); err != nil {
			return "", err
		}
		prior[step] = d.Outcome
		if step == c.Step {
			replay = &d
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	result := "replayed"
	d := replay
	if d == nil {
		result = "applied"
		a := Account{ID: c.Account}
		var limit *int64
		err := tx.QueryRow(ctx, `SELECT available, held, frozen, currency, daily_limit FROM ledger.accounts WHERE id = $1 FOR UPDATE`,
			c.Account).Scan(&a.Available, &a.Held, &a.Frozen, &a.Currency, &limit)
		switch {
		case err == nil:
			a.Exists = true
		case !errors.Is(err, pgx.ErrNoRows):
			return "", err
		}
		if limit != nil && c.Step == wire.Hold {
			// A statement of its own, after the row lock: it then sees every
			// hold committed by whoever held the lock before us. As a subquery
			// of the locking SELECT it would read the snapshot taken before
			// the wait, and two holds could both fit under the limit.
			a.DailyLimit = *limit
			if err := tx.QueryRow(ctx, spentToday, c.Account).Scan(&a.SpentToday); err != nil {
				return "", err
			}
		}
		dec := Decide(c, prior, a)
		d = &dec
		if err := s.record(ctx, tx, c, dec); err != nil {
			return "", err
		}
	}

	r := wire.Reply{MsgID: wire.ReplyID(c.SagaID, c.Step), SagaID: c.SagaID, Step: c.Step,
		Outcome: d.Outcome, Reason: d.Reason, SentAt: time.Now()}
	if err := outbox.Put(ctx, tx, outbox.Ledger, s.Replies, r.Key(), r.MsgID, r); err != nil {
		return "", err
	}
	failpoint.Hit("ledger.before_commit")
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	failpoint.Hit("ledger.after_commit")
	return result, nil
}

// spentToday is what holds took from account $1 since the start of the
// current UTC day, minus those released. Captured holds stay counted: the
// money left the account, and a later refund does not make room.
const spentToday = `SELECT coalesce(sum(h.amount), 0) FROM ledger.operations h
	WHERE h.account_id = $1 AND h.step = 'hold' AND h.outcome = 'ok'
	  AND h.at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
	  AND NOT EXISTS (SELECT 1 FROM ledger.operations r
	                  WHERE r.saga_id = h.saga_id AND r.step = 'release' AND r.reason = 'released')`

func (s *Store) record(ctx context.Context, tx pgx.Tx, c wire.Command, d Decision) error {
	var opID int64
	if err := tx.QueryRow(ctx, `INSERT INTO ledger.operations (saga_id, step, account_id, amount, currency, outcome, reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		c.SagaID, c.Step, c.Account, c.Amount, c.Currency, d.Outcome, d.Reason).Scan(&opID); err != nil {
		return err
	}
	return post(ctx, tx, opID, d.Entries)
}

// post writes the legs and applies the customer-account legs to the
// materialised balance. System accounts (negative ids) have no balance row.
func post(ctx context.Context, tx pgx.Tx, opID int64, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	accts := make([]int64, len(entries))
	buckets := make([]string, len(entries))
	amounts := make([]int64, len(entries))
	for i, e := range entries {
		accts[i], buckets[i], amounts[i] = e.Account, string(e.Bucket), e.Amount
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ledger.entries (op_id, account_id, bucket, amount)
		SELECT $1::bigint, * FROM unnest($2::bigint[], $3::text[], $4::bigint[])`, opID, accts, buckets, amounts); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE ledger.accounts a SET
			available = a.available + d.available,
			held      = a.held + d.held
		FROM (SELECT account_id,
		             coalesce(sum(amount) FILTER (WHERE bucket = 'available'), 0) AS available,
		             coalesce(sum(amount) FILTER (WHERE bucket = 'held'), 0) AS held
		      FROM unnest($1::bigint[], $2::text[], $3::bigint[]) AS l(account_id, bucket, amount)
		      WHERE account_id > 0 GROUP BY account_id) d
		WHERE a.id = d.account_id`, accts, buckets, amounts)
	return err
}

// SeedCurrency is the currency Seed gives account id: blocks of Shards
// consecutive ids share one, so every shard holds every currency.
func SeedCurrency(id int64, currencies []string) string {
	return currencies[int((id-1)/Shards)%len(currencies)]
}

// Seed opens n accounts with the same balance, one balanced operation
// against equity per currency. Every frozenEvery-th account is frozen
// (0 = none), so transfers to it exercise the refund path.
func Seed(ctx context.Context, db *pgxpool.Pool, n int, balance int64, frozenEvery int, currencies []string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	byCur := map[string][]Entry{}
	ids := make([]int64, n)
	curs := make([]string, n)
	for i := range n {
		id := int64(i + 1)
		ids[i], curs[i] = id, SeedCurrency(id, currencies)
		byCur[curs[i]] = append(byCur[curs[i]], Entry{id, Available, balance})
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ledger.accounts (id, available, held, frozen, currency)
		SELECT id, 0, 0, $3 > 0 AND id % $3 = 0, cur FROM unnest($1::bigint[], $2::text[]) AS a(id, cur)`,
		ids, curs, frozenEvery); err != nil {
		return err
	}
	for cur, entries := range byCur {
		total := int64(len(entries)) * balance
		var opID int64
		if err := tx.QueryRow(ctx, `INSERT INTO ledger.operations (saga_id, step, account_id, amount, currency, outcome)
			VALUES ($1, 'open', $2, $3, $4, 'ok') RETURNING id`, uuid.New(), Equity, total, cur).Scan(&opID); err != nil {
			return err
		}
		if err := post(ctx, tx, opID, append(entries, Entry{Equity, Available, -total})); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
