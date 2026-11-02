package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chipontix/ledger-saga/internal/metrics"
)

var ErrNoAccount = errors.New("no such account")

// Line is one operation that moved money on the account: the change to
// each bucket and both buckets after it. Refusals and fences post nothing
// and do not appear.
type Line struct {
	Op             int64     `json:"op"`
	SagaID         uuid.UUID `json:"saga_id"`
	Step           string    `json:"step"`
	At             time.Time `json:"at"`
	Available      int64     `json:"available"`
	Held           int64     `json:"held"`
	AvailableAfter int64     `json:"available_after"`
	HeldAfter      int64     `json:"held_after"`
}

// Statement is one page of an account's journal, read in the same snapshot
// as its balance row. On the last page (Next == 0) the last line's *After
// equal Available and Held.
type Statement struct {
	Account   int64  `json:"account"`
	Currency  string `json:"currency"`
	Frozen    bool   `json:"frozen"`
	Available int64  `json:"available"`
	Held      int64  `json:"held"`
	Lines     []Line `json:"lines"`
	Next      int64  `json:"next,omitempty"` // ?after= for the next page
}

// ReadStatement returns up to limit lines with op > after, oldest first.
//
// Paging by op id is safe across snapshots because every operation that
// posts to a customer account takes that account's row lock before it gets
// its id and holds it until commit (Handle). So for one account, ids are
// handed out in commit order: once a page has seen op n, no op < n can
// still appear.
// ponytail: the running totals are a window over the account's whole
// history on every page; keep a per-page opening balance if accounts grow
// to millions of lines.
func ReadStatement(ctx context.Context, db *pgxpool.Pool, account, after int64, limit int) (*Statement, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	s := &Statement{Account: account, Lines: []Line{}}
	err = tx.QueryRow(ctx, `SELECT currency, frozen, available, held FROM ledger.accounts WHERE id = $1`, account).
		Scan(&s.Currency, &s.Frozen, &s.Available, &s.Held)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoAccount
	}
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT op_id, saga_id, step, at, av, he, av_after, he_after FROM (
			SELECT e.op_id, o.saga_id, o.step, o.at,
			       coalesce(sum(e.amount) FILTER (WHERE e.bucket = 'available'), 0) AS av,
			       coalesce(sum(e.amount) FILTER (WHERE e.bucket = 'held'), 0) AS he,
			       sum(coalesce(sum(e.amount) FILTER (WHERE e.bucket = 'available'), 0)) OVER w AS av_after,
			       sum(coalesce(sum(e.amount) FILTER (WHERE e.bucket = 'held'), 0)) OVER w AS he_after
			FROM ledger.entries e JOIN ledger.operations o ON o.id = e.op_id
			WHERE e.account_id = $1
			GROUP BY e.op_id, o.saga_id, o.step, o.at
			WINDOW w AS (ORDER BY e.op_id)
		) l WHERE op_id > $2 ORDER BY op_id LIMIT $3`, account, after, limit+1)
	if err != nil {
		return nil, err
	}
	s.Lines, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Line, error) {
		var l Line
		return l, r.Scan(&l.Op, &l.SagaID, &l.Step, &l.At, &l.Available, &l.Held, &l.AvailableAfter, &l.HeldAfter)
	})
	if err != nil {
		return nil, err
	}
	if len(s.Lines) > limit {
		s.Lines = s.Lines[:limit]
		s.Next = s.Lines[limit-1].Op
	}
	return s, nil
}

// Handler serves GET /accounts/{id}/statement?after=&limit= (limit 1..1000,
// default 100) and /healthz.
func Handler(db *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /accounts/{id}/statement", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		code := statement(w, r, db)
		metrics.HTTP.WithLabelValues("statement", strconv.Itoa(code)).Observe(time.Since(start).Seconds())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func statement(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool) int {
	q := r.URL.Query()
	id, errID := strconv.ParseInt(r.PathValue("id"), 10, 64)
	after, errAfter := int64(0), error(nil)
	if v := q.Get("after"); v != "" {
		after, errAfter = strconv.ParseInt(v, 10, 64)
	}
	limit, errLimit := 100, error(nil)
	if v := q.Get("limit"); v != "" {
		limit, errLimit = strconv.Atoi(v)
	}
	if errors.Join(errID, errAfter, errLimit) != nil || id <= 0 || after < 0 || limit < 1 || limit > 1000 {
		return reply(w, http.StatusBadRequest, map[string]string{"error": "want /accounts/{positive id}/statement?after=op&limit=1..1000"})
	}
	s, err := ReadStatement(r.Context(), db, id, after, limit)
	switch {
	case errors.Is(err, ErrNoAccount):
		return reply(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case err != nil:
		slog.Error("statement", "account", id, "err", err)
		return reply(w, http.StatusInternalServerError, map[string]string{"error": "statement failed"})
	}
	return reply(w, http.StatusOK, s)
}

func reply(w http.ResponseWriter, code int, body any) int {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
	return code
}
