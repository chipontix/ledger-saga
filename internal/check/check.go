// Package check audits a database against the invariants of ADR-0003. It
// reads both schemas, which no service ever does: it is the auditor, not a
// participant.
package check

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chipontix/ledger-saga/internal/ledger"
	"github.com/chipontix/ledger-saga/internal/saga"
	"github.com/chipontix/ledger-saga/internal/wire"
)

type Report struct {
	States      map[saga.State]int `json:"states"`
	Operations  int                `json:"operations"`
	InFlight    int64              `json:"in_flight"` // clearing balance: captured, not yet credited/refunded
	Violations  []string           `json:"violations"`
	Quiescent   bool               `json:"quiescent"`
	CustomerSum int64              `json:"customer_sum"`
}

func (r *Report) fail(format string, args ...any) {
	if len(r.Violations) < 50 {
		r.Violations = append(r.Violations, fmt.Sprintf(format, args...))
	}
}

// Run checks everything that must hold at any instant, and, if quiescent,
// what must hold once every saga is terminal. It reads in one REPEATABLE
// READ snapshot so a running system is audited consistently.
func Run(ctx context.Context, db *pgxpool.Pool, quiescent bool) (*Report, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	r := &Report{States: map[saga.State]int{}, Quiescent: quiescent}

	// 1. The journal balances, globally and per operation.
	var total int64
	if err := tx.QueryRow(ctx, `SELECT coalesce(sum(amount), 0) FROM ledger.entries`).Scan(&total); err != nil {
		return nil, err
	}
	if total != 0 {
		r.fail("journal sums to %d, not 0", total)
	}
	if err := each(ctx, tx, `SELECT op_id, sum(amount) FROM ledger.entries GROUP BY op_id HAVING sum(amount) <> 0`,
		func(op, s int64) { r.fail("operation %d legs sum to %d", op, s) }); err != nil {
		return nil, err
	}

	// 2. Materialised balances equal the journal; 3. none negative.
	if err := each(ctx, tx, `
		SELECT coalesce(a.id, j.account_id), coalesce(a.available, -1) - coalesce(j.av, 0) + coalesce(a.held, -1) - coalesce(j.he, 0)
		FROM ledger.accounts a
		FULL JOIN (SELECT account_id,
		                  sum(amount) FILTER (WHERE bucket = 'available') av,
		                  sum(amount) FILTER (WHERE bucket = 'held') he
		           FROM ledger.entries WHERE account_id > 0 GROUP BY account_id) j ON j.account_id = a.id
		WHERE a.id IS NULL OR a.available <> coalesce(j.av, 0) OR a.held <> coalesce(j.he, 0)
		   OR a.available < 0 OR a.held < 0`,
		func(id, diff int64) {
			r.fail("account %d: balance row disagrees with journal (diff %d) or is negative", id, diff)
		}); err != nil {
		return nil, err
	}

	// 4. No operation crosses a shard.
	if err := each(ctx, tx, `
		SELECT e.op_id, count(DISTINCT CASE WHEN e.account_id > 0 THEN e.account_id % $1 ELSE -10 - e.account_id END)
		FROM ledger.entries e JOIN ledger.operations o ON o.id = e.op_id
		WHERE o.step <> 'open' GROUP BY e.op_id HAVING count(DISTINCT CASE WHEN e.account_id > 0 THEN e.account_id % $1 ELSE -10 - e.account_id END) > 1`,
		func(op, n int64) { r.fail("operation %d touches %d shards", op, n) }, ledger.Shards); err != nil {
		return nil, err
	}

	var held, opened int64
	if err := tx.QueryRow(ctx, `SELECT
			coalesce((SELECT sum(available + held) FROM ledger.accounts), 0),
			coalesce((SELECT sum(held) FROM ledger.accounts), 0),
			coalesce((SELECT sum(amount) FROM ledger.entries WHERE account_id < $1), 0),
			coalesce((SELECT -sum(amount) FROM ledger.entries WHERE account_id = $2), 0)`,
		ledger.Equity, ledger.Equity).Scan(&r.CustomerSum, &held, &r.InFlight, &opened); err != nil {
		return nil, err
	}
	if r.CustomerSum+r.InFlight != opened {
		r.fail("customer money %d + in flight %d != opened %d", r.CustomerSum, r.InFlight, opened)
	}

	// 5. Per saga: ledger effects exactly as the terminal state allows, and
	// the right amount moved between the right accounts.
	type sagaRow struct {
		saga.Transfer
		src, dst int64
		ops      map[wire.Step]string
	}
	sagas := map[uuid.UUID]*sagaRow{}
	rows, err := tx.Query(ctx, `
		SELECT t.id, t.state, t.amount,
		       coalesce(sum(e.amount) FILTER (WHERE e.account_id = t.from_account), 0),
		       coalesce(sum(e.amount) FILTER (WHERE e.account_id = t.to_account), 0)
		FROM saga.transfers t
		LEFT JOIN ledger.operations o ON o.saga_id = t.id
		LEFT JOIN ledger.entries e ON e.op_id = o.id
		GROUP BY t.id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		s := &sagaRow{ops: map[wire.Step]string{}}
		if err := rows.Scan(&s.ID, &s.State, &s.Amount, &s.src, &s.dst); err != nil {
			return nil, err
		}
		sagas[s.ID] = s
		r.States[s.State]++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, `SELECT saga_id, step, outcome, reason FROM ledger.operations WHERE step <> 'open'`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id uuid.UUID
		var step wire.Step
		var outcome, reason string
		if err := rows.Scan(&id, &step, &outcome, &reason); err != nil {
			return nil, err
		}
		r.Operations++
		s, ok := sagas[id]
		if !ok {
			r.fail("operation %s/%s for unknown saga", id, step)
			continue
		}
		if step == wire.Release {
			outcome += "/" + reason
		}
		s.ops[step] = outcome
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for id, s := range sagas {
		if !s.State.Terminal() {
			if quiescent {
				r.fail("saga %s still %s", id, s.State)
			}
			continue
		}
		if err := saga.CheckEffects(s.State, s.ops); err != nil {
			r.fail("saga %s: %v", id, err)
		}
		want := int64(0)
		if s.State == saga.Completed {
			want = s.Amount
		}
		if s.src != -want || s.dst != want {
			r.fail("saga %s %s moved src %d dst %d, want %d/%d", id, s.State, s.src, s.dst, -want, want)
		}
	}

	// 6. The transition log is a legal path ending in the current state.
	rows, err = tx.Query(ctx, `SELECT saga_id, seq, from_state, to_state FROM saga.transitions ORDER BY saga_id, seq`)
	if err != nil {
		return nil, err
	}
	var prev uuid.UUID
	var last saga.State
	var seq int
	end := func() {
		if s := sagas[prev]; prev != uuid.Nil && (s == nil || s.State != last) {
			r.fail("saga %s: log ends in %s, row says %v", prev, last, s)
		}
	}
	for rows.Next() {
		var id uuid.UUID
		var n int
		var from, to saga.State
		if err := rows.Scan(&id, &n, &from, &to); err != nil {
			return nil, err
		}
		if id != prev {
			end()
			prev, last, seq = id, "", 0
		}
		seq++
		if n != seq || from != last || !saga.Legal(from, to) {
			r.fail("saga %s: transition #%d %s -> %s after %s", id, n, from, to, last)
		}
		last = to
	}
	end()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if quiescent {
		if held != 0 || r.InFlight != 0 {
			r.fail("at rest but %d held and %d in clearing", held, r.InFlight)
		}
		if r.CustomerSum != opened {
			r.fail("at rest customer money %d != opened %d", r.CustomerSum, opened)
		}
	}
	return r, nil
}

func each(ctx context.Context, tx pgx.Tx, q string, f func(a, b int64), args ...any) error {
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var a, b int64
		if err := rows.Scan(&a, &b); err != nil {
			return err
		}
		f(a, b)
	}
	return rows.Err()
}
