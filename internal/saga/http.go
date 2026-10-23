package saga

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/chipontix/ledger-saga/internal/metrics"
)

// API is the public HTTP surface:
//
//	POST /transfers            Idempotency-Key required
//	GET  /transfers/{id}?wait= long-poll until terminal, up to 30s
type API struct {
	Store *Store

	mu      sync.Mutex
	waiters map[uuid.UUID][]chan struct{}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /transfers", timed("create", a.create))
	mux.HandleFunc("GET /transfers/{id}", timed("get", a.get))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

type transferJSON struct {
	ID         uuid.UUID  `json:"id"`
	From       int64      `json:"from"`
	To         int64      `json:"to"`
	Amount     int64      `json:"amount"`
	State      State      `json:"state"`
	Reason     string     `json:"reason,omitempty"`
	Attempt    int        `json:"attempt"`
	CreatedAt  time.Time  `json:"created_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func render(v View) transferJSON {
	return transferJSON{v.ID, v.From, v.To, v.Amount, v.State, v.Reason, v.Attempt, v.CreatedAt, v.FinishedAt}
}

func (a *API) create(w http.ResponseWriter, r *http.Request) int {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		return fail(w, http.StatusBadRequest, "Idempotency-Key header is required (max 200 chars)")
	}
	var req Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&struct {
		From   *int64 `json:"from"`
		To     *int64 `json:"to"`
		Amount *int64 `json:"amount"`
	}{&req.From, &req.To, &req.Amount}); err != nil {
		return fail(w, http.StatusBadRequest, "body must be {\"from\":int,\"to\":int,\"amount\":int}")
	}
	if req.Amount <= 0 || req.From <= 0 || req.To <= 0 || req.From == req.To {
		return fail(w, http.StatusUnprocessableEntity, "amount must be positive and from/to distinct positive account ids")
	}
	v, created, err := a.Store.Create(r.Context(), key, req)
	switch {
	case errors.Is(err, ErrKeyReuse):
		return fail(w, http.StatusConflict, err.Error())
	case err != nil:
		slog.Error("create transfer", "err", err)
		return fail(w, http.StatusInternalServerError, "try again with the same Idempotency-Key")
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	return reply(w, code, render(v))
}

func (a *API) get(w http.ResponseWriter, r *http.Request) int {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return fail(w, http.StatusBadRequest, "bad id")
	}
	wait, _ := time.ParseDuration(r.URL.Query().Get("wait"))
	wait = min(wait, 30*time.Second)

	var done chan struct{}
	if wait > 0 {
		// Register before reading, so a finish between the read and the
		// wait is not missed.
		done = a.subscribe(id)
		defer a.unsubscribe(id, done)
	}
	v, err := a.Store.Get(r.Context(), id)
	if err == nil && wait > 0 && !v.State.Terminal() {
		select {
		case <-done:
		case <-time.After(wait):
		case <-r.Context().Done():
		}
		v, err = a.Store.Get(r.Context(), id)
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return fail(w, http.StatusNotFound, "no such transfer")
	case err != nil:
		return fail(w, http.StatusInternalServerError, "lookup failed")
	}
	return reply(w, http.StatusOK, render(v))
}

// Finished wakes long-polls waiting on id. Wire it to Store.OnFinish.
// ponytail: in-process only; with several API replicas a waiter on another
// replica falls back to its timeout. LISTEN/NOTIFY if that matters.
func (a *API) Finished(id uuid.UUID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, c := range a.waiters[id] {
		close(c)
	}
	delete(a.waiters, id)
}

func (a *API) subscribe(id uuid.UUID) chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.waiters == nil {
		a.waiters = map[uuid.UUID][]chan struct{}{}
	}
	c := make(chan struct{})
	a.waiters[id] = append(a.waiters[id], c)
	return c
}

func (a *API) unsubscribe(id uuid.UUID, c chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ws := a.waiters[id]
	for i, w := range ws {
		if w == c {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(a.waiters, id)
	} else {
		a.waiters[id] = ws
	}
}

func timed(route string, h func(http.ResponseWriter, *http.Request) int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		code := h(w, r)
		metrics.HTTP.WithLabelValues(route, strconv.Itoa(code)).Observe(time.Since(start).Seconds())
	}
}

func reply(w http.ResponseWriter, code int, body any) int {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
	return code
}

func fail(w http.ResponseWriter, code int, msg string) int {
	return reply(w, code, map[string]string{"error": msg})
}
