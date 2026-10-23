// Package sim runs the real state machine and the real ledger decision
// function against an adversarial in-memory network: messages are delivered
// in any order, duplicated, dropped, and timeouts fire at arbitrary points.
// rapid explores the schedules and shrinks any failure to a minimal one.
package sim

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"

	"github.com/chipontix/ledger-saga/internal/ledger"
	"github.com/chipontix/ledger-saga/internal/saga"
	"github.com/chipontix/ledger-saga/internal/wire"
)

type opKey struct {
	saga uuid.UUID
	step wire.Step
}

type posted struct {
	saga uuid.UUID
	ledger.Entry
}

type world struct {
	t        *rapid.T
	p        saga.Policy
	now      time.Time
	sagas    map[uuid.UUID]saga.Transfer
	order    []uuid.UUID
	accounts map[int64]*ledger.Account
	ops      map[opKey]ledger.Decision
	journal  []posted
	orchSeen map[string]bool
	ledgSeen map[string]bool
	inflight []any
	initial  int64
}

func newWorld(t *rapid.T) *world {
	w := &world{
		t:        t,
		p:        saga.Policy{StepTimeout: time.Second, MaxBackoff: 8 * time.Second, HoldAttempts: 2},
		now:      time.Unix(1_700_000_000, 0),
		sagas:    map[uuid.UUID]saga.Transfer{},
		accounts: map[int64]*ledger.Account{},
		ops:      map[opKey]ledger.Decision{},
		orchSeen: map[string]bool{},
		ledgSeen: map[string]bool{},
	}
	n := rapid.IntRange(2, 6).Draw(t, "accounts")
	for id := int64(1); id <= int64(n); id++ {
		bal := rapid.Int64Range(0, 100).Draw(t, "balance")
		w.accounts[id] = &ledger.Account{ID: id, Exists: true, Available: bal,
			Frozen: rapid.IntRange(0, 4).Draw(t, "frozen") == 0}
		w.journal = append(w.journal,
			posted{Entry: ledger.Entry{Account: ledger.Equity, Bucket: ledger.Available, Amount: -bal}},
			posted{Entry: ledger.Entry{Account: id, Bucket: ledger.Available, Amount: bal}})
		w.initial += bal
	}
	return w
}

func (w *world) record(id uuid.UUID, from saga.State, next saga.Transfer) {
	if !saga.Legal(from, next.State) {
		w.t.Fatalf("illegal transition %s -> %s", from, next.State)
	}
	w.sagas[id] = next
}

func (w *world) start() {
	n := int64(len(w.accounts))
	from := rapid.Int64Range(1, n).Draw(w.t, "from")
	to := rapid.Int64Range(1, n+1).Draw(w.t, "to") // n+1 does not exist
	if to == from {
		return
	}
	id := uuid.New()
	tr, cmd := saga.Start(saga.Transfer{ID: id, From: from, To: to,
		Amount: rapid.Int64Range(1, 60).Draw(w.t, "amount")}, w.now, w.p)
	w.record(id, "", tr)
	w.order = append(w.order, id)
	w.inflight = append(w.inflight, cmd)
}

// ledgerHandle mirrors ledger.Store.Handle: inbox dedup, replay of a recorded
// step, otherwise Decide and apply atomically.
func (w *world) ledgerHandle(c wire.Command) {
	if w.ledgSeen[c.MsgID] {
		return
	}
	w.ledgSeen[c.MsgID] = true
	k := opKey{c.SagaID, c.Step}
	d, done := w.ops[k]
	if !done {
		prior := ledger.Prior{}
		for _, s := range []wire.Step{wire.Hold, wire.Release, wire.Capture, wire.Credit, wire.Refund} {
			if o, ok := w.ops[opKey{c.SagaID, s}]; ok {
				prior[s] = o.Outcome
			}
		}
		acct := ledger.Account{ID: c.Account}
		if a, ok := w.accounts[c.Account]; ok {
			acct = *a
		}
		d = ledger.Decide(c, prior, acct)
		w.ops[k] = d
		for _, e := range d.Entries {
			w.journal = append(w.journal, posted{c.SagaID, e})
			if a, ok := w.accounts[e.Account]; ok {
				if e.Bucket == ledger.Available {
					a.Available += e.Amount
				} else {
					a.Held += e.Amount
				}
				if a.Available < 0 || a.Held < 0 {
					w.t.Fatalf("negative balance on %d after %s", a.ID, c.Step)
				}
			}
		}
	}
	w.inflight = append(w.inflight, wire.Reply{MsgID: wire.ReplyID(c.SagaID, c.Step),
		SagaID: c.SagaID, Step: c.Step, Outcome: d.Outcome, Reason: d.Reason})
}

func (w *world) orchHandle(r wire.Reply) {
	if w.orchSeen[r.MsgID] {
		return
	}
	w.orchSeen[r.MsgID] = true
	cur := w.sagas[r.SagaID]
	next, cmd, ok := saga.OnReply(cur, r, w.now, w.p)
	if !ok {
		return
	}
	w.record(r.SagaID, cur.State, next)
	if cmd != nil {
		w.inflight = append(w.inflight, *cmd)
	}
}

func (w *world) deliver(i int) {
	m := w.inflight[i]
	w.inflight = slices.Delete(w.inflight, i, i+1)
	switch m := m.(type) {
	case wire.Command:
		w.ledgerHandle(m)
	case wire.Reply:
		w.orchHandle(m)
	}
}

func (w *world) tick(d time.Duration) {
	w.now = w.now.Add(d)
	for _, id := range w.order {
		tr := w.sagas[id]
		if tr.State.Terminal() || tr.Deadline.After(w.now) {
			continue
		}
		next, cmd := saga.OnTimeout(tr, w.now, w.p)
		w.record(id, tr.State, next)
		w.inflight = append(w.inflight, cmd)
	}
}

func (w *world) done() bool {
	for _, id := range w.order {
		if !w.sagas[id].State.Terminal() {
			return false
		}
	}
	return true
}

func (w *world) check() {
	t := w.t
	var total, customer, held, clearing int64
	for _, p := range w.journal {
		total += p.Amount
		if p.Account < ledger.Equity {
			clearing += p.Amount
		}
	}
	for _, a := range w.accounts {
		customer += a.Available + a.Held
		held += a.Held
	}
	if total != 0 {
		t.Fatalf("journal does not balance: %d", total)
	}
	if customer != w.initial || clearing != 0 || held != 0 {
		t.Fatalf("money not conserved: customer %d initial %d clearing %d held %d", customer, w.initial, clearing, held)
	}
	for _, id := range w.order {
		tr := w.sagas[id]
		got := map[wire.Step]string{}
		for k, d := range w.ops {
			if k.saga == id {
				got[k.step] = string(d.Outcome)
				if k.step == wire.Release {
					got[k.step] += "/" + d.Reason
				}
			}
		}
		if err := saga.CheckEffects(tr.State, got); err != nil {
			t.Fatalf("saga %s in %s: %v (ops %v)", id, tr.State, err, got)
		}
		var src, dst int64
		for _, p := range w.journal {
			if p.saga == id && p.Account == tr.From {
				src += p.Amount
			}
			if p.saga == id && p.Account == tr.To {
				dst += p.Amount
			}
		}
		wantSrc, wantDst := int64(0), int64(0)
		if tr.State == saga.Completed {
			wantSrc, wantDst = -tr.Amount, tr.Amount
		}
		if src != wantSrc || dst != wantDst {
			t.Fatalf("saga %s in %s moved src %d dst %d", id, tr.State, src, dst)
		}
	}
}

func TestSagaUnderAdversarialNetwork(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := newWorld(t)
		for range rapid.IntRange(1, 6).Draw(t, "transfers") {
			w.start()
		}
		// Adversarial phase: any message, any order, duplicates, losses, time jumps.
		for range rapid.IntRange(0, 80).Draw(t, "steps") {
			if len(w.inflight) == 0 {
				w.tick(time.Duration(rapid.IntRange(1, 3000).Draw(t, "idle")) * time.Millisecond)
				continue
			}
			i := rapid.IntRange(0, len(w.inflight)-1).Draw(t, "msg")
			switch rapid.IntRange(0, 9).Draw(t, "action") {
			case 0: // lose it
				w.inflight = slices.Delete(w.inflight, i, i+1)
			case 1: // duplicate it, deliver later
				w.inflight = append(w.inflight, w.inflight[i])
			case 2:
				w.tick(time.Duration(rapid.IntRange(1, 5000).Draw(t, "jump")) * time.Millisecond)
			default:
				w.deliver(i)
			}
		}
		// Fair phase: the network heals; everything must reach a terminal state.
		for round := 0; !w.done(); round++ {
			if round > 200 {
				t.Fatalf("sagas did not terminate")
			}
			for len(w.inflight) > 0 {
				w.deliver(0)
			}
			w.tick(10 * time.Second)
		}
		for len(w.inflight) > 0 { // late duplicates must not change anything
			w.deliver(0)
		}
		w.check()
	})
}
