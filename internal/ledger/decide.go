// Package ledger is the saga participant that owns the money. decide.go is
// the pure decision function; store.go persists its output in one local
// transaction per command.
package ledger

import (
	"github.com/chipontix/ledger-saga/internal/wire"
)

// Shards is the number of ledger shards. A command only ever touches one
// account and that account's shard clearing account, so a transfer between
// shards cannot be one database transaction: that is why it is a saga.
const Shards = 4

// System accounts are negative. They have journal entries but no balance
// row, so the hottest rows in the system are never locked.
const Equity int64 = -1 // source of opening balances

func Clearing(shard int) int64 { return -10 - int64(shard) }

func Shard(account int64) int {
	if account < 0 {
		if account == Equity {
			return -1
		}
		return int(-10 - account)
	}
	return int(account % Shards)
}

type Bucket string

const (
	Available Bucket = "available"
	Held      Bucket = "held"
)

// Entry is one leg of a double-entry posting. The legs of every operation
// sum to zero.
type Entry struct {
	Account int64
	Bucket  Bucket
	Amount  int64
}

type Account struct {
	ID        int64
	Exists    bool
	Frozen    bool
	Available int64
	Held      int64
}

// Prior is what this ledger already recorded for the saga, by step.
type Prior map[wire.Step]wire.Outcome

type Decision struct {
	Outcome wire.Outcome
	Reason  string
	Entries []Entry
}

// Decide handles a command whose step is not yet in prior; a step already in
// prior is replayed verbatim by the caller and never decided twice. That is
// what makes a duplicate hold return the original rejection even if the
// account has since been topped up.
func Decide(cmd wire.Command, prior Prior, a Account) Decision {
	x := cmd.Amount
	clearing := Clearing(Shard(a.ID))
	reject := func(reason string) Decision { return Decision{Outcome: wire.Rejected, Reason: reason} }

	switch cmd.Step {
	case wire.Hold:
		if _, released := prior[wire.Release]; released {
			return Decision{Outcome: wire.Fenced, Reason: "released before hold arrived"}
		}
		switch {
		case !a.Exists:
			return reject("account not found")
		case a.Frozen:
			return reject("account frozen")
		case a.Available < x:
			return reject("insufficient funds")
		}
		return Decision{Outcome: wire.OK, Entries: []Entry{{a.ID, Available, -x}, {a.ID, Held, x}}}

	case wire.Release:
		if _, captured := prior[wire.Capture]; captured {
			return reject("already captured")
		}
		if prior[wire.Hold] == wire.OK {
			return Decision{Outcome: wire.OK, Reason: "released", Entries: []Entry{{a.ID, Held, -x}, {a.ID, Available, x}}}
		}
		// No hold yet, or it was refused: record a tombstone that fences it.
		return Decision{Outcome: wire.OK, Reason: "tombstone"}

	case wire.Capture:
		_, released := prior[wire.Release]
		if prior[wire.Hold] != wire.OK || released {
			return reject("no active hold")
		}
		return Decision{Outcome: wire.OK, Entries: []Entry{{a.ID, Held, -x}, {clearing, Available, x}}}

	case wire.Credit:
		switch {
		case !a.Exists:
			return reject("account not found")
		case a.Frozen:
			return reject("account frozen")
		}
		return Decision{Outcome: wire.OK, Entries: []Entry{{clearing, Available, -x}, {a.ID, Available, x}}}

	case wire.Refund:
		if prior[wire.Capture] != wire.OK {
			return reject("nothing captured")
		}
		return Decision{Outcome: wire.OK, Entries: []Entry{{clearing, Available, -x}, {a.ID, Available, x}}}
	}
	return reject("unknown step")
}
