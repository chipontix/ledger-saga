// Package outbox writes outgoing messages and dedups incoming ones inside the
// caller's business transaction. A relay (internal/relay) ships the outbox
// to Kafka; nothing else ever produces.
package outbox

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
)

// Schemas that own an outbox/inbox pair.
const (
	Saga   = "saga"
	Ledger = "ledger"
)

// Put stages a message. It is published only if tx commits.
func Put(ctx context.Context, tx pgx.Tx, schema, topic, key, msgID string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+schema+`.outbox (topic, key, msg_id, payload) VALUES ($1, $2, $3, $4)`,
		topic, key, msgID, string(b))
	return err
}

// Seen records msgID in the inbox and reports whether it was already there.
// Because the insert is part of tx, a crash before commit forgets it and the
// redelivered message is processed again, exactly once in effect.
func Seen(ctx context.Context, tx pgx.Tx, schema, msgID string) (bool, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO `+schema+`.inbox (msg_id) VALUES ($1) ON CONFLICT DO NOTHING`, msgID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 0, nil
}
