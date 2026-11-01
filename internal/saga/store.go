package saga

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chipontix/ledger-saga/internal/failpoint"
	"github.com/chipontix/ledger-saga/internal/metrics"
	"github.com/chipontix/ledger-saga/internal/outbox"
	"github.com/chipontix/ledger-saga/internal/wire"
)

var (
	ErrKeyReuse = errors.New("idempotency key reused with a different request")
	ErrNotFound = errors.New("transfer not found")
)

// Store persists transfers. Every state change, its transition log row and
// the command it emits are written in one transaction.
type Store struct {
	DB       *pgxpool.Pool
	Policy   Policy
	Commands string // topic
	Now      func() time.Time
	OnFinish func(uuid.UUID) // called after a terminal state commits
}

type Request struct {
	From, To, Amount int64
	Currency         string
}

func (r Request) hash() []byte {
	h := sha256.Sum256(fmt.Appendf(nil, "%d|%d|%d|%s", r.From, r.To, r.Amount, r.Currency))
	return h[:]
}

// View is a transfer as the API returns it.
type View struct {
	Transfer
	CreatedAt  time.Time
	FinishedAt *time.Time
	version    int
}

const cols = `id, from_account, to_account, amount, currency, state, attempt, deadline, reason, version, created_at, finished_at`

func scan(row pgx.Row) (View, error) {
	var v View
	err := row.Scan(&v.ID, &v.From, &v.To, &v.Amount, &v.Currency, &v.State, &v.Attempt, &v.Deadline, &v.Reason,
		&v.version, &v.CreatedAt, &v.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

// Create starts a transfer under an idempotency key. A repeated key with the
// same request returns the original transfer (created=false); with a
// different request it is ErrKeyReuse.
func (s *Store) Create(ctx context.Context, key string, req Request) (v View, created bool, err error) {
	now := s.Now()
	t, cmd := Start(Transfer{ID: uuid.Must(uuid.NewV7()), From: req.From, To: req.To, Amount: req.Amount,
		Currency: req.Currency}, now, s.Policy)

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return v, false, err
	}
	defer tx.Rollback(ctx)
	// ON CONFLICT waits for a concurrent insert of the same key to finish, so
	// two racing requests cannot both create a transfer.
	tag, err := tx.Exec(ctx, `INSERT INTO saga.transfers
		(id, idem_key, request_hash, from_account, to_account, amount, currency, state, attempt, deadline, version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11) ON CONFLICT (idem_key) DO NOTHING`,
		t.ID, key, req.hash(), t.From, t.To, t.Amount, t.Currency, t.State, t.Attempt, t.Deadline, now)
	if err != nil {
		return v, false, err
	}
	if tag.RowsAffected() == 0 {
		tx.Rollback(ctx)
		var hash []byte
		row := s.DB.QueryRow(ctx, `SELECT request_hash, `+cols+` FROM saga.transfers WHERE idem_key = $1`, key)
		err := row.Scan(&hash, &v.ID, &v.From, &v.To, &v.Amount, &v.Currency, &v.State, &v.Attempt, &v.Deadline, &v.Reason,
			&v.version, &v.CreatedAt, &v.FinishedAt)
		if err == nil && !bytes.Equal(hash, req.hash()) {
			err = ErrKeyReuse
		}
		return v, false, err
	}
	if err := logTransition(ctx, tx, "", t, 1, now); err != nil {
		return v, false, err
	}
	if err := outbox.Put(ctx, tx, outbox.Saga, s.Commands, cmd.Key(), cmd.MsgID, cmd); err != nil {
		return v, false, err
	}
	failpoint.Hit("api.before_commit")
	if err := tx.Commit(ctx); err != nil {
		return v, false, err
	}
	failpoint.Hit("api.after_commit")
	metrics.SagaStarted.Inc()
	return View{Transfer: t, CreatedAt: now, version: 1}, true, nil
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (View, error) {
	return scan(s.DB.QueryRow(ctx, `SELECT `+cols+` FROM saga.transfers WHERE id = $1`, id))
}

// HandleReply applies one ledger reply. It returns the consumer result label.
func (s *Store) HandleReply(ctx context.Context, r wire.Reply) (string, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if dup, err := outbox.Seen(ctx, tx, outbox.Saga, r.MsgID); err != nil || dup {
		return "duplicate", err
	}
	cur, err := scan(tx.QueryRow(ctx, `SELECT `+cols+` FROM saga.transfers WHERE id = $1 FOR UPDATE`, r.SagaID))
	if err != nil {
		return "", err
	}
	now := s.Now()
	next, cmd, ok := OnReply(cur.Transfer, r, now, s.Policy)
	result := "stale"
	if ok {
		result = "applied"
		if err := s.save(ctx, tx, cur, next, now); err != nil {
			return "", err
		}
		if cmd != nil {
			if err := outbox.Put(ctx, tx, outbox.Saga, s.Commands, cmd.Key(), cmd.MsgID, cmd); err != nil {
				return "", err
			}
		}
	}
	failpoint.Hit("orch.reply.before_commit")
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	failpoint.Hit("orch.reply.after_commit")
	if ok && next.State.Terminal() {
		metrics.SagaFinished.WithLabelValues(string(next.State)).Observe(now.Sub(cur.CreatedAt).Seconds())
		if s.OnFinish != nil {
			s.OnFinish(next.ID)
		}
	}
	return result, nil
}

// SweepTimeouts advances up to limit transfers whose deadline has passed.
// SKIP LOCKED lets several sweepers (or a sweeper racing a reply) coexist.
func (s *Store) SweepTimeouts(ctx context.Context, limit int) (int, error) {
	now := s.Now()
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT `+cols+` FROM saga.transfers
		WHERE finished_at IS NULL AND deadline <= $1 ORDER BY deadline LIMIT $2 FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return 0, err
	}
	due, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (View, error) { return scan(r) })
	if err != nil {
		return 0, err
	}
	for _, cur := range due {
		next, cmd := OnTimeout(cur.Transfer, now, s.Policy)
		if err := s.save(ctx, tx, cur, next, now); err != nil {
			return 0, err
		}
		if err := outbox.Put(ctx, tx, outbox.Saga, s.Commands, cmd.Key(), cmd.MsgID, cmd); err != nil {
			return 0, err
		}
	}
	if len(due) > 0 {
		failpoint.Hit("orch.timeout.before_commit")
	}
	return len(due), tx.Commit(ctx)
}

// Stuck counts open transfers accepted more than after ago, by state, and
// exports the counts and the age of the oldest open transfer as gauges.
// Nothing gives up after hold (ADR-0002), and a refused capture, refund or
// release is ignored on purpose (OnReply), so an outage or a bug shows up
// here and nowhere else.
func (s *Store) Stuck(ctx context.Context, after time.Duration) (map[State]int, time.Duration, error) {
	now := s.Now()
	rows, err := s.DB.Query(ctx, `SELECT state, count(*) FILTER (WHERE created_at < $1), min(created_at)
		FROM saga.transfers WHERE finished_at IS NULL GROUP BY state`, now.Add(-after))
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	stuck := map[State]int{}
	var oldest time.Duration
	for rows.Next() {
		var st State
		var n int
		var first time.Time
		if err := rows.Scan(&st, &n, &first); err != nil {
			return nil, 0, err
		}
		if n > 0 {
			stuck[st] = n
		}
		oldest = max(oldest, now.Sub(first))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	for st := range pending { // every waiting state, so a cleared one drops to 0
		metrics.SagaStuck.WithLabelValues(string(st)).Set(float64(stuck[st]))
	}
	metrics.SagaOldestOpen.Set(oldest.Seconds())
	return stuck, oldest, nil
}

func (s *Store) save(ctx context.Context, tx pgx.Tx, cur View, next Transfer, now time.Time) error {
	var finished *time.Time
	if next.State.Terminal() {
		finished = &now
	}
	tag, err := tx.Exec(ctx, `UPDATE saga.transfers
		SET state = $2, attempt = $3, deadline = $4, reason = $5, version = version + 1, finished_at = $6
		WHERE id = $1 AND version = $7`,
		next.ID, next.State, next.Attempt, next.Deadline, next.Reason, finished, cur.version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("transfer %s changed under its row lock", next.ID)
	}
	return logTransition(ctx, tx, cur.State, next, cur.version+1, now)
}

func logTransition(ctx context.Context, tx pgx.Tx, from State, to Transfer, seq int, now time.Time) error {
	metrics.Transitions.WithLabelValues(string(from), string(to.State)).Inc()
	_, err := tx.Exec(ctx, `INSERT INTO saga.transitions (saga_id, seq, from_state, to_state, attempt, at)
		VALUES ($1, $2, $3, $4, $5, $6)`, to.ID, seq, from, to.State, to.Attempt, now)
	return err
}
