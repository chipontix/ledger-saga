package saga

import (
	"fmt"
	"maps"

	"github.com/chipontix/ledger-saga/internal/wire"
)

// effects lists, for each terminal state, every set of ledger operations the
// saga may have left behind: step -> outcome (release also carries its
// reason). Anything else is a double or missing effect.
var effects = map[State][]map[wire.Step]string{
	Completed: {{wire.Hold: "ok", wire.Capture: "ok", wire.Credit: "ok"}},
	Refunded:  {{wire.Hold: "ok", wire.Capture: "ok", wire.Credit: "rejected", wire.Refund: "ok"}},
	Rejected:  {{wire.Hold: "rejected"}},
	Cancelled: {
		{wire.Hold: "ok", wire.Release: "ok/released"},
		{wire.Release: "ok/tombstone"},
		{wire.Release: "ok/tombstone", wire.Hold: "fenced"},
		{wire.Release: "ok/tombstone", wire.Hold: "rejected"},
	},
}

// CheckEffects is the per-saga exactly-once rule used by the property test
// and the chaos checker.
func CheckEffects(s State, got map[wire.Step]string) error {
	for _, want := range effects[s] {
		if maps.Equal(want, got) {
			return nil
		}
	}
	return fmt.Errorf("ledger effects %v not allowed for %s", got, s)
}
