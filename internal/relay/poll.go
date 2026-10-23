// Package relay ships outbox rows to Kafka. Two interchangeable modes, both
// at-least-once: a poller (poll.go) and logical-replication CDC (cdc.go).
// ADR-0004 compares them.
package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/chipontix/ledger-saga/internal/failpoint"
	"github.com/chipontix/ledger-saga/internal/metrics"
)

func NewWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1000,
		BatchTimeout: time.Nanosecond, // WriteMessages already hands us a whole batch
	}
}

// Poller drains one schema's outbox: lock a batch, write it to Kafka, delete
// it, commit. A crash between the write and the commit re-sends the batch;
// consumers dedup (ADR-0005).
type Poller struct {
	DB       *pgxpool.Pool
	Writer   *kafka.Writer
	Schema   string
	Batch    int
	Interval time.Duration // sleep when the outbox is empty
}

func (p *Poller) Run(ctx context.Context) {
	backoff := p.Interval
	for ctx.Err() == nil {
		n, err := p.once(ctx)
		switch {
		case err != nil:
			slog.Warn("outbox poll failed", "schema", p.Schema, "err", err)
			backoff = min(2*backoff, 2*time.Second)
			sleep(ctx, backoff)
		case n == 0:
			backoff = p.Interval
			sleep(ctx, p.Interval)
		default:
			backoff = p.Interval
		}
	}
}

func (p *Poller) once(ctx context.Context) (int, error) {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	// SKIP LOCKED: a second poller takes the next batch instead of waiting.
	rows, err := tx.Query(ctx, `SELECT id, topic, key, payload FROM `+p.Schema+`.outbox
		ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, p.Batch)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var msgs []kafka.Message
	for rows.Next() {
		var id int64
		var topic, key, payload string
		if err := rows.Scan(&id, &topic, &key, &payload); err != nil {
			return 0, err
		}
		ids = append(ids, id)
		msgs = append(msgs, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(payload)})
	}
	if err := rows.Err(); err != nil || len(ids) == 0 {
		return 0, err
	}
	failpoint.Hit("relay.poll.before_publish")
	if err := p.Writer.WriteMessages(ctx, msgs...); err != nil {
		return 0, err
	}
	failpoint.Hit("relay.poll.after_publish")
	if _, err := tx.Exec(ctx, `DELETE FROM `+p.Schema+`.outbox WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	metrics.Relayed.WithLabelValues("poll", p.Schema).Add(float64(len(ids)))
	return len(ids), nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
