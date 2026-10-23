// Package consumer wires a handler to Kafka through kafka-konsumer with three
// layers of failure handling:
//
//  1. in-process retry with exponential backoff for transient errors
//     (connection loss, deadlock, serialization failure);
//  2. the retry topic, re-consumed on a schedule, for errors that outlive
//     the in-process budget;
//  3. the dead-letter topic for Permanent errors and for messages that
//     exhausted the retry topic.
//
// Delivery is at-least-once: offsets are committed only after the handler
// returned for the whole batch. Handlers must be idempotent (ADR-0005).
package consumer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Trendyol/kafka-konsumer/v2"
	segkafka "github.com/segmentio/kafka-go"

	"github.com/chipontix/ledger-saga/internal/metrics"
)

// Handler processes one message and returns a result label for metrics.
type Handler func(ctx context.Context, value []byte) (string, error)

// Permanent marks an error that retrying cannot fix: undecodable payload,
// unknown saga. The message goes straight to the dead-letter topic.
type Permanent struct{ Err error }

func (p Permanent) Error() string { return p.Err.Error() }
func (p Permanent) Unwrap() error { return p.Err }

type Config struct {
	Name        string
	Brokers     []string
	Topic       string
	Group       string
	Concurrency int
	RetryEvery  string        // cron spec for the retry topic, e.g. "@every 10s"
	RetryWork   time.Duration // how long each cron tick consumes; >= the interval means non-stop
	// ChaosKey names what a message is about (e.g. saga+step, not attempt),
	// so Chaos.Defer picks the same messages across retries.
	ChaosKey func(value []byte) string
	// SessionTimeout bounds how long partitions of a killed consumer stay
	// assigned to it; the broker's floor is group.min.session.timeout.ms.
	SessionTimeout time.Duration
	Chaos          Chaos
}

func RetryTopic(topic string) string { return topic + ".retry" }
func DLQTopic(topic string) string   { return topic + ".dlq" }

const inProcessAttempts = 4

func Start(ctx context.Context, cfg Config, h Handler) (stop func(), err error) {
	ch := newChaos(cfg.Chaos, cfg.ChaosKey)
	process := func(m *kafka.Message) error {
		var err error
		for attempt := 1; attempt <= inProcessAttempts; attempt++ {
			var result string
			if result, err = h(ctx, m.Value); err == nil {
				metrics.Consumed.WithLabelValues(cfg.Name, result).Inc()
				return nil
			}
			if errors.As(err, &Permanent{}) || ctx.Err() != nil {
				break
			}
			// 25ms, 100ms, 400ms with full jitter.
			time.Sleep(time.Duration(rand.Int64N(int64(25*time.Millisecond) << (2 * (attempt - 1)))))
		}
		if errors.As(err, &Permanent{}) {
			m.SendDirectToDeadLetter = true
			m.ErrDescription = err.Error()
			metrics.Consumed.WithLabelValues(cfg.Name, "dead_letter").Inc()
			slog.Error("dead-lettering message", "consumer", cfg.Name, "err", err)
		} else {
			metrics.Consumed.WithLabelValues(cfg.Name, "retried").Inc()
			slog.Warn("handing message to retry topic", "consumer", cfg.Name, "err", err)
		}
		return err
	}

	c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers:           cfg.Brokers,
			Topic:             cfg.Topic,
			GroupID:           cfg.Group,
			MaxWait:           250 * time.Millisecond,
			SessionTimeout:    cfg.SessionTimeout,
			RebalanceTimeout:  cfg.SessionTimeout,
			HeartbeatInterval: cfg.SessionTimeout / 6,
		},
		Concurrency:          cfg.Concurrency,
		MessageGroupDuration: 2 * time.Millisecond,
		LogLevel:             kafka.LogLevelWarn,
		ConsumeFn:            ch.wrap(cfg.Topic, process),
		TransactionalRetry:   kafka.NewBoolPtr(false), // rung 1 is ours, with backoff
		DeadLetterTopic:      DLQTopic(cfg.Topic),
		RetryEnabled:         true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:             cfg.Brokers,
			Topic:               RetryTopic(cfg.Topic),
			DeadLetterTopic:     DLQTopic(cfg.Topic),
			StartTimeCron:       cfg.RetryEvery,
			WorkDuration:        cfg.RetryWork,
			MaxRetry:            3,
			BackOffStrategyName: "exponential",
			LogLevel:            kafka.LogLevelWarn,
			// Library defaults are acks=none (a retry could vanish) and a 1s
			// batch timeout, which blocks the worker and, through the batch
			// barrier, every message in flight with it.
			ProducerRequiredAcks: segkafka.RequireAll,
			ProducerBatchTimeout: time.Millisecond,
		},
	})
	if err != nil {
		return nil, err
	}
	c.Consume()
	return func() { c.Stop() }, nil
}

// Chaos injects the delivery faults the system must tolerate, on top of the
// real ones: duplicates, reordering inside the in-flight window, and late
// redelivery of an already-processed message. Seeded, so a run repeats.
type Chaos struct {
	Seed      uint64
	Defer     float64       // share of ChaosKeys whose first deliveries fail, so they travel the retry topic and arrive late
	Dup       float64       // process the message twice in a row
	Delay     float64       // hold the message back up to MaxDelay so later ones overtake it
	Redeliver float64       // process it again after up to MaxLate
	MaxDelay  time.Duration // kafka-konsumer waits for the whole in-flight batch, so this stalls it
	MaxLate   time.Duration
}

type chaos struct {
	Chaos
	key func([]byte) string
	mu  sync.Mutex
	rng *rand.Rand
}

func newChaos(c Chaos, key func([]byte) string) *chaos {
	return &chaos{Chaos: c, key: key, rng: rand.New(rand.NewPCG(c.Seed, c.Seed^0x5eed))}
}

// deferred is a pure function of (seed, key): every attempt of a chosen
// saga step is late, while its compensation is chosen independently. That
// is the schedule in which a release overtakes all of its holds.
func (c *chaos) deferred(value []byte) bool {
	if c.Defer == 0 || c.key == nil {
		return false
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%d|%s", c.Seed, c.key(value))
	return float64(h.Sum64()%10000)/10000 < c.Defer
}

func (c *chaos) roll(p float64, upTo time.Duration) (bool, time.Duration) {
	if p == 0 {
		return false, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rng.Float64() < p, time.Duration(c.rng.Int64N(int64(upTo) + 1))
}

var errDeferred = errors.New("chaos: deferred to the retry topic")

func (c *chaos) wrap(topic string, process func(*kafka.Message) error) kafka.ConsumeFn {
	return func(m *kafka.Message) error {
		if m.Topic == topic { // only first deliveries, or it would loop into the DLQ
			if c.deferred(m.Value) {
				metrics.Consumed.WithLabelValues("chaos", "deferred").Inc()
				return errDeferred
			}
		}
		if ok, d := c.roll(c.Delay, c.MaxDelay); ok {
			time.Sleep(d) // other workers keep committing later messages meanwhile
		}
		err := process(m)
		if ok, _ := c.roll(c.Dup, 0); ok && err == nil {
			err = process(m)
		}
		if ok, d := c.roll(c.Redeliver, c.MaxLate); ok && err == nil {
			late := *m
			late.Value = append([]byte(nil), m.Value...)
			go func() { time.Sleep(d); _ = process(&late) }()
		}
		return err
	}
}
