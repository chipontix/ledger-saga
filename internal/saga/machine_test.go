package saga

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/chipontix/ledger-saga/internal/wire"
)

var t0 = time.Unix(1_700_000_000, 0)

func reply(tr Transfer, step wire.Step, o wire.Outcome) wire.Reply {
	return wire.Reply{SagaID: tr.ID, Step: step, Outcome: o, Reason: "r"}
}

func TestHappyPathAndRefund(t *testing.T) {
	p := DefaultPolicy()
	tr, cmd := Start(Transfer{ID: uuid.New(), From: 1, To: 2, Amount: 10}, t0, p)
	if tr.State != HoldPending || cmd.Step != wire.Hold || cmd.Account != 1 {
		t.Fatalf("start: %v %v", tr.State, cmd)
	}
	steps := []struct {
		step    wire.Step
		outcome wire.Outcome
		want    State
		next    wire.Step
		account int64
	}{
		{wire.Hold, wire.OK, CapturePending, wire.Capture, 1},
		{wire.Capture, wire.OK, CreditPending, wire.Credit, 2},
		{wire.Credit, wire.Rejected, RefundPending, wire.Refund, 1},
	}
	for _, s := range steps {
		var c *wire.Command
		var ok bool
		tr, c, ok = OnReply(tr, reply(tr, s.step, s.outcome), t0, p)
		if !ok || tr.State != s.want || c.Step != s.next || c.Account != s.account || c.Attempt != 1 {
			t.Fatalf("after %s/%s: state %s cmd %+v", s.step, s.outcome, tr.State, c)
		}
	}
	tr, c, ok := OnReply(tr, reply(tr, wire.Refund, wire.OK), t0, p)
	if !ok || tr.State != Refunded || c != nil || tr.Reason != "r" {
		t.Fatalf("refund: %s %v", tr.State, c)
	}
}

func TestStaleRepliesAreIgnored(t *testing.T) {
	p := DefaultPolicy()
	tr, _ := Start(Transfer{ID: uuid.New(), From: 1, To: 2, Amount: 10}, t0, p)
	tr, _, _ = OnReply(tr, reply(tr, wire.Hold, wire.OK), t0, p)
	for _, r := range []wire.Reply{reply(tr, wire.Hold, wire.OK), reply(tr, wire.Hold, wire.Rejected), reply(tr, wire.Credit, wire.OK)} {
		if _, _, ok := OnReply(tr, r, t0, p); ok {
			t.Fatalf("stale %s/%s applied in %s", r.Step, r.Outcome, tr.State)
		}
	}
	// A refused capture is a bug on the ledger side; the saga must not move.
	if _, _, ok := OnReply(tr, reply(tr, wire.Capture, wire.Rejected), t0, p); ok {
		t.Fatal("refused capture moved the saga")
	}
}

func TestHoldTimeoutCompensatesOtherStepsRetry(t *testing.T) {
	p := Policy{StepTimeout: time.Second, MaxBackoff: 4 * time.Second, HoldAttempts: 3}
	tr, _ := Start(Transfer{ID: uuid.New(), From: 1, To: 2, Amount: 10}, t0, p)
	var c wire.Command
	for want := 2; want <= 3; want++ {
		tr, c = OnTimeout(tr, t0, p)
		if tr.State != HoldPending || c.Attempt != want || c.MsgID != wire.CommandID(tr.ID, wire.Hold, want) {
			t.Fatalf("hold retry %d: %s %+v", want, tr.State, c)
		}
	}
	tr, c = OnTimeout(tr, t0, p)
	if tr.State != ReleasePending || c.Step != wire.Release || c.Attempt != 1 {
		t.Fatalf("after %d hold attempts: %s %+v", p.HoldAttempts, tr.State, c)
	}

	tr.State, tr.Attempt = CreditPending, 1
	for range 50 {
		tr, c = OnTimeout(tr, t0, p)
	}
	if tr.State != CreditPending || c.Step != wire.Credit || c.Attempt != 51 {
		t.Fatalf("credit gave up: %s %+v", tr.State, c)
	}
}

func TestBackoffIsExponentialCappedAndDeterministic(t *testing.T) {
	p := Policy{StepTimeout: time.Second, MaxBackoff: 8 * time.Second}
	id := uuid.New()
	prev := time.Duration(0)
	for attempt := 1; attempt <= 10; attempt++ {
		d := p.deadline(id, attempt, t0).Sub(t0)
		if d != p.deadline(id, attempt, t0).Sub(t0) {
			t.Fatal("deadline not deterministic")
		}
		base := min(time.Second<<(attempt-1), 8*time.Second)
		if d < base || d > base+base/4 {
			t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, d, base, base+base/4)
		}
		if attempt <= 4 && d <= prev {
			t.Fatalf("attempt %d not growing: %v <= %v", attempt, d, prev)
		}
		prev = d
	}
}

func TestEdgesCoverMachine(t *testing.T) {
	for s := range pending {
		if len(Edges[s]) == 0 {
			t.Fatalf("pending state %s has no edges", s)
		}
	}
	if Legal(Completed, RefundPending) || Legal(HoldPending, Completed) {
		t.Fatal("illegal edge accepted")
	}
}
