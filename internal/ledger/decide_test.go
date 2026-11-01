package ledger

import (
	"testing"

	"github.com/chipontix/ledger-saga/internal/wire"
)

func sum(es []Entry) (s int64) {
	for _, e := range es {
		s += e.Amount
	}
	return
}

func TestDecide(t *testing.T) {
	acct := Account{ID: 5, Exists: true, Currency: "USD", Available: 100}
	eur := Account{ID: 5, Exists: true, Currency: "EUR", Available: 100}
	cases := []struct {
		name    string
		step    wire.Step
		prior   Prior
		acct    Account
		outcome wire.Outcome
		reason  string
		legs    int
	}{
		{"hold ok", wire.Hold, Prior{}, acct, wire.OK, "", 2},
		{"hold insufficient", wire.Hold, Prior{}, Account{ID: 5, Exists: true, Currency: "USD", Available: 9}, wire.Rejected, "insufficient funds", 0},
		{"hold frozen", wire.Hold, Prior{}, Account{ID: 5, Exists: true, Currency: "USD", Frozen: true, Available: 100}, wire.Rejected, "account frozen", 0},
		{"hold in another currency", wire.Hold, Prior{}, eur, wire.Rejected, "currency mismatch", 0},
		{"hold missing", wire.Hold, Prior{}, Account{ID: 5}, wire.Rejected, "account not found", 0},
		{"hold after release is fenced", wire.Hold, Prior{wire.Release: wire.OK}, acct, wire.Fenced, "released before hold arrived", 0},
		{"release active hold", wire.Release, Prior{wire.Hold: wire.OK}, acct, wire.OK, "released", 2},
		{"release before hold", wire.Release, Prior{}, acct, wire.OK, "tombstone", 0},
		{"release after refused hold", wire.Release, Prior{wire.Hold: wire.Rejected}, acct, wire.OK, "tombstone", 0},
		{"release after capture", wire.Release, Prior{wire.Hold: wire.OK, wire.Capture: wire.OK}, acct, wire.Rejected, "already captured", 0},
		{"capture", wire.Capture, Prior{wire.Hold: wire.OK}, acct, wire.OK, "", 2},
		{"capture without hold", wire.Capture, Prior{}, acct, wire.Rejected, "no active hold", 0},
		{"capture released hold", wire.Capture, Prior{wire.Hold: wire.OK, wire.Release: wire.OK}, acct, wire.Rejected, "no active hold", 0},
		{"credit", wire.Credit, Prior{}, acct, wire.OK, "", 2},
		{"credit in another currency", wire.Credit, Prior{}, eur, wire.Rejected, "currency mismatch", 0},
		{"credit frozen", wire.Credit, Prior{}, Account{ID: 5, Exists: true, Frozen: true}, wire.Rejected, "account frozen", 0},
		{"refund", wire.Refund, Prior{wire.Hold: wire.OK, wire.Capture: wire.OK, wire.Credit: wire.Rejected}, acct, wire.OK, "", 2},
		{"refund uncaptured", wire.Refund, Prior{wire.Hold: wire.OK}, acct, wire.Rejected, "nothing captured", 0},
	}
	for _, c := range cases {
		d := Decide(wire.Command{Step: c.step, Account: 5, Amount: 10, Currency: "USD"}, c.prior, c.acct)
		if d.Outcome != c.outcome || d.Reason != c.reason || len(d.Entries) != c.legs || sum(d.Entries) != 0 {
			t.Errorf("%s: got %+v", c.name, d)
		}
		for _, e := range d.Entries {
			if e.Account != 5 && e.Account != Clearing(Shard(5)) {
				t.Errorf("%s: leg %+v leaves shard %d", c.name, e, Shard(5))
			}
		}
	}
}

func TestShards(t *testing.T) {
	for s := range Shards {
		if Shard(Clearing(s)) != s {
			t.Fatalf("clearing account of shard %d maps to %d", s, Shard(Clearing(s)))
		}
	}
	if Shard(Equity) != -1 || Shard(7) != 3 {
		t.Fatal("shard mapping")
	}
}
