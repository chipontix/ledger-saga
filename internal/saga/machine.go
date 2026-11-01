// Package saga is the transfer orchestrator. machine.go is the pure state
// machine: no I/O, no clock reads, so it can be property-tested exhaustively
// and replayed by the chaos checker.
package saga

import (
	"hash/fnv"
	"time"

	"github.com/google/uuid"

	"github.com/chipontix/ledger-saga/internal/wire"
)

type State string

const (
	HoldPending    State = "HOLD_PENDING"
	CapturePending State = "CAPTURE_PENDING"
	CreditPending  State = "CREDIT_PENDING"
	RefundPending  State = "REFUND_PENDING"
	ReleasePending State = "RELEASE_PENDING"

	Completed State = "COMPLETED" // src -X, dst +X
	Refunded  State = "REFUNDED"  // captured, credit refused, money back at src
	Rejected  State = "REJECTED"  // hold refused, nothing moved
	Cancelled State = "CANCELLED" // hold timed out and was released/fenced
)

func (s State) Terminal() bool {
	switch s {
	case Completed, Refunded, Rejected, Cancelled:
		return true
	}
	return false
}

// pending maps every waiting state to the step it is waiting on.
var pending = map[State]wire.Step{
	HoldPending:    wire.Hold,
	CapturePending: wire.Capture,
	CreditPending:  wire.Credit,
	RefundPending:  wire.Refund,
	ReleasePending: wire.Release,
}

// Edges is every legal transition. Self-loops are timeouts that resend the
// same step. The chaos checker replays the transition log against this.
var Edges = map[State][]State{
	"":             {HoldPending},
	HoldPending:    {HoldPending, CapturePending, Rejected, ReleasePending},
	CapturePending: {CapturePending, CreditPending},
	CreditPending:  {CreditPending, Completed, RefundPending},
	RefundPending:  {RefundPending, Refunded},
	ReleasePending: {ReleasePending, Cancelled},
}

func Legal(from, to State) bool {
	for _, s := range Edges[from] {
		if s == to {
			return true
		}
	}
	return false
}

type Transfer struct {
	ID       uuid.UUID
	From, To int64
	Amount   int64
	Currency string
	State    State
	Attempt  int
	Deadline time.Time
	Reason   string
}

// Policy is the timeout schedule.
type Policy struct {
	StepTimeout  time.Duration // deadline of the first attempt of every step
	MaxBackoff   time.Duration // cap on the exponential growth
	HoldAttempts int           // hold attempts before compensating with a release
}

func DefaultPolicy() Policy {
	return Policy{StepTimeout: 2 * time.Second, MaxBackoff: 30 * time.Second, HoldAttempts: 3}
}

// deadline is StepTimeout*2^(attempt-1), capped, plus up to 25% jitter. The
// jitter is a hash of (saga, attempt) so a replay computes the same deadline.
func (p Policy) deadline(id uuid.UUID, attempt int, now time.Time) time.Time {
	d := p.StepTimeout
	for i := 1; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	d = min(d, p.MaxBackoff)
	h := fnv.New32a()
	h.Write(id[:])
	h.Write([]byte{byte(attempt)})
	jitter := time.Duration(h.Sum32()%1000) * d / 4000
	return now.Add(d + jitter)
}

// Command is what the transfer must send while in its current state.
func (t Transfer) Command(now time.Time) wire.Command {
	step := pending[t.State]
	acct := t.From
	if step == wire.Credit {
		acct = t.To
	}
	return wire.Command{
		MsgID:    wire.CommandID(t.ID, step, t.Attempt),
		SagaID:   t.ID,
		Step:     step,
		Account:  acct,
		Amount:   t.Amount,
		Currency: t.Currency,
		Attempt:  t.Attempt,
		SentAt:   now,
	}
}

func (t Transfer) enter(s State, now time.Time, p Policy) Transfer {
	t.State = s
	t.Attempt = 1
	if !s.Terminal() {
		t.Deadline = p.deadline(t.ID, 1, now)
	}
	return t
}

// Start puts a new transfer into HOLD_PENDING and returns the first command.
func Start(t Transfer, now time.Time, p Policy) (Transfer, wire.Command) {
	t = t.enter(HoldPending, now, p)
	return t, t.Command(now)
}

// OnReply applies a ledger reply. ok=false means the reply is stale or a
// duplicate for the current state and must be ignored; that is the
// business-level dedup that makes reordered and replayed replies harmless.
func OnReply(t Transfer, r wire.Reply, now time.Time, p Policy) (next Transfer, cmd *wire.Command, ok bool) {
	if pending[t.State] != r.Step {
		return t, nil, false
	}
	var to State
	switch {
	case t.State == HoldPending && r.Outcome == wire.OK:
		to = CapturePending
	case t.State == HoldPending && r.Outcome == wire.Rejected:
		to = Rejected
	case t.State == CapturePending && r.Outcome == wire.OK:
		to = CreditPending
	case t.State == CreditPending && r.Outcome == wire.OK:
		to = Completed
	case t.State == CreditPending && r.Outcome == wire.Rejected:
		to = RefundPending
	case t.State == RefundPending && r.Outcome == wire.OK:
		to = Refunded
	case t.State == ReleasePending && r.Outcome == wire.OK:
		to = Cancelled
	default:
		// Capture/refund/release cannot be refused while the saga is in the
		// state that sends them; a refusal there is a bug. Keep waiting so
		// it surfaces as a stuck saga rather than a silent wrong terminal.
		return t, nil, false
	}
	next = t.enter(to, now, p)
	if r.Outcome == wire.Rejected {
		next.Reason = r.Reason
	}
	if to.Terminal() {
		return next, nil, true
	}
	c := next.Command(now)
	return next, &c, true
}

// OnTimeout fires when the deadline passed with no reply. Hold is the only
// step that gives up: after HoldAttempts it compensates with a release, which
// also fences any hold still in flight. Every later step is past the point
// where giving up is safe, so it retries with capped exponential backoff.
func OnTimeout(t Transfer, now time.Time, p Policy) (Transfer, wire.Command) {
	if t.State == HoldPending && t.Attempt >= p.HoldAttempts {
		t = t.enter(ReleasePending, now, p)
		t.Reason = "hold timed out"
		return t, t.Command(now)
	}
	t.Attempt++
	t.Deadline = p.deadline(t.ID, t.Attempt, now)
	return t, t.Command(now)
}
