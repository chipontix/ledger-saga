// Package wire holds the messages exchanged between the orchestrator and the
// ledger. Both sides only ever talk through these types over Kafka.
package wire

import (
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Step is one ledger operation a saga asks for.
type Step string

const (
	Hold    Step = "hold"    // reserve funds on the source account
	Release Step = "release" // cancel a hold (and fence a late one)
	Capture Step = "capture" // turn the hold into a debit into the shard's clearing account
	Credit  Step = "credit"  // pay the destination out of its shard's clearing account
	Refund  Step = "refund"  // return a captured amount to the source
)

// Outcome is the ledger's definitive answer to a command.
type Outcome string

const (
	OK       Outcome = "ok"
	Rejected Outcome = "rejected" // business refusal: insufficient funds, frozen account, ...
	Fenced   Outcome = "fenced"   // a hold that arrived after its release; nothing happened
)

// Command is sent orchestrator -> ledger, keyed by account so one account's
// commands share a partition.
type Command struct {
	MsgID    string    `json:"msg_id"`
	SagaID   uuid.UUID `json:"saga_id"`
	Step     Step      `json:"step"`
	Account  int64     `json:"account"`
	Amount   int64     `json:"amount"`
	Currency string    `json:"currency"` // ISO 4217; the ledger checks it against the account
	Attempt  int       `json:"attempt"`
	SentAt   time.Time `json:"sent_at"`
}

// Reply is sent ledger -> orchestrator, keyed by saga.
type Reply struct {
	MsgID   string    `json:"msg_id"`
	SagaID  uuid.UUID `json:"saga_id"`
	Step    Step      `json:"step"`
	Outcome Outcome   `json:"outcome"`
	Reason  string    `json:"reason,omitempty"`
	SentAt  time.Time `json:"sent_at"`
}

// CommandID is unique per attempt: retries are distinct messages and the
// ledger dedups them on (saga, step), not on the transport id.
func CommandID(saga uuid.UUID, step Step, attempt int) string {
	return fmt.Sprintf("%s:%s:%d", saga, step, attempt)
}

// ReplyID is the same for every reply to (saga, step), so a replayed reply is
// a transport duplicate the orchestrator's inbox drops.
func ReplyID(saga uuid.UUID, step Step) string {
	return fmt.Sprintf("%s:%s:reply", saga, step)
}

func (c Command) Key() string { return strconv.FormatInt(c.Account, 10) }
func (r Reply) Key() string   { return r.SagaID.String() }
