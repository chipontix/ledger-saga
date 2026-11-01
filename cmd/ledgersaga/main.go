// Command ledgersaga runs every role of the system from one binary:
//
//	ledgersaga migrate | seed | orchestrator | ledger | relay | check | load | chaos
//
// Configuration is environment variables (see env.go).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/chipontix/ledger-saga/internal/consumer"
	"github.com/chipontix/ledger-saga/internal/db"
	"github.com/chipontix/ledger-saga/internal/ledger"
	"github.com/chipontix/ledger-saga/internal/metrics"
	"github.com/chipontix/ledger-saga/internal/relay"
	"github.com/chipontix/ledger-saga/internal/saga"
	"github.com/chipontix/ledger-saga/internal/wire"
)

var cmds = map[string]func(ctx context.Context, args []string) error{
	"migrate":      migrate,
	"seed":         seed,
	"orchestrator": orchestrator,
	"ledger":       ledgerd,
	"relay":        relayd,
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if len(os.Args) < 2 || cmds[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: ledgersaga migrate|seed|orchestrator|ledger|relay|check|load|chaos [flags]")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := cmds[os.Args[1]](ctx, os.Args[2:]); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error(os.Args[1], "err", err)
		os.Exit(1)
	}
}

func topics() (commands, replies string) {
	p := env("TOPIC_PREFIX", "")
	return p + "ledger.commands", p + "saga.replies"
}

func pool(ctx context.Context) (*pgxpool.Pool, error) {
	return db.Open(ctx, env("PG_URL", "postgres://postgres:postgres@localhost:5432/ledger?sslmode=disable"),
		int32(envInt("PG_MAX_CONNS", 32)))
}

func brokers() []string { return []string{env("KAFKA_BROKERS", "localhost:9092")} }

func serveMetrics(ctx context.Context) {
	serve(ctx, env("METRICS_ADDR", ":9100"), promhttp.Handler())
}

func serve(ctx context.Context, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http", "addr", addr, "err", err)
			os.Exit(1)
		}
	}()
}

func migrate(ctx context.Context, _ []string) error {
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	if err := db.Migrate(ctx, p); err != nil {
		return err
	}
	commands, replies := topics()
	var ts []kafka.TopicConfig
	for _, t := range []string{commands, replies} {
		for _, name := range []string{t, consumer.RetryTopic(t), consumer.DLQTopic(t)} {
			ts = append(ts, kafka.TopicConfig{Topic: name, NumPartitions: envInt("PARTITIONS", 16), ReplicationFactor: 1})
		}
	}
	return createTopics(ctx, ts)
}

func createTopics(ctx context.Context, ts []kafka.TopicConfig) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers()...), Timeout: 10 * time.Second}
	var err error
	for range 30 { // the broker may still be starting
		var resp *kafka.CreateTopicsResponse
		if resp, err = client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: ts}); err == nil {
			for _, e := range resp.Errors {
				if e != nil && !errors.Is(e, kafka.TopicAlreadyExists) {
					err = e
				}
			}
		}
		if err == nil {
			return nil
		}
		slog.Warn("create topics, retrying", "err", err)
		time.Sleep(time.Second)
	}
	return err
}

func seed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	n := fs.Int("accounts", 10_000, "accounts to open")
	bal := fs.Int64("balance", 1_000_000, "opening balance of each account, minor units")
	frozen := fs.Int("frozen-every", 50, "freeze every n-th account (0 = none)")
	currencies := fs.String("currencies", "USD", "comma-separated; consecutive blocks of ledger.Shards accounts take turns")
	fs.Parse(args)
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	return ledger.Seed(ctx, p, *n, *bal, *frozen, strings.Split(*currencies, ","))
}

func policy() saga.Policy {
	return saga.Policy{
		StepTimeout:  envDur("STEP_TIMEOUT", 2*time.Second),
		MaxBackoff:   envDur("MAX_BACKOFF", 30*time.Second),
		HoldAttempts: envInt("HOLD_ATTEMPTS", 3),
	}
}

func chaosCfg() consumer.Chaos {
	return consumer.Chaos{
		Seed:      uint64(envInt("CHAOS_SEED", 0)),
		Defer:     envFloat("CHAOS_DEFER", 0),
		Dup:       envFloat("CHAOS_DUP", 0),
		Delay:     envFloat("CHAOS_DELAY", 0),
		Redeliver: envFloat("CHAOS_REDELIVER", 0),
		MaxDelay:  envDur("CHAOS_MAX_DELAY", 200*time.Millisecond),
		MaxLate:   envDur("CHAOS_MAX_LATE", time.Second),
	}
}

func decode[T any](value []byte, sentAt func(T) time.Time, name string) (T, error) {
	var v T
	if err := json.Unmarshal(value, &v); err != nil {
		return v, consumer.Permanent{Err: fmt.Errorf("decode: %w", err)}
	}
	metrics.Delivery.WithLabelValues(name).Observe(time.Since(sentAt(v)).Seconds())
	return v, nil
}

// chaosKey is saga+step: the same for every attempt of a step.
func chaosKey(value []byte) string {
	var m struct {
		SagaID string `json:"saga_id"`
		Step   string `json:"step"`
	}
	json.Unmarshal(value, &m)
	return m.SagaID + "/" + m.Step
}

func orchestrator(ctx context.Context, _ []string) error {
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	commands, replies := topics()
	api := &saga.API{}
	store := &saga.Store{DB: p, Policy: policy(), Commands: commands, Now: time.Now, OnFinish: api.Finished}
	api.Store = store

	stop, err := consumer.Start(ctx, consumer.Config{
		Name: "orchestrator", Brokers: brokers(), Topic: replies, Group: env("TOPIC_PREFIX", "") + "orchestrator",
		Concurrency: envInt("CONSUMER_CONCURRENCY", 32), RetryEvery: env("RETRY_EVERY", "@every 10s"),
		RetryWork: envDur("RETRY_WORK", 5*time.Second), Chaos: chaosCfg(),
		SessionTimeout: envDur("KAFKA_SESSION_TIMEOUT", 6*time.Second), ChaosKey: chaosKey,
	}, func(ctx context.Context, value []byte) (string, error) {
		r, err := decode(value, func(r wire.Reply) time.Time { return r.SentAt }, "orchestrator")
		if err != nil {
			return "", err
		}
		res, err := store.HandleReply(ctx, r)
		if errors.Is(err, saga.ErrNotFound) {
			err = consumer.Permanent{Err: err}
		}
		return res, err
	})
	if err != nil {
		return err
	}
	defer stop()

	go func() {
		// A restarted orchestrator must not judge deadlines before its reply
		// consumer is back in the group: replies that arrived while it was
		// down are still unread, and timing out on them would cancel holds
		// that succeeded. The grace covers the group session timeout.
		sleepCtx(ctx, envDur("TIMEOUT_GRACE", 10*time.Second))
		tick := time.NewTicker(envDur("SWEEP_EVERY", 100*time.Millisecond))
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				for { // drain everything due before sleeping again
					n, err := store.SweepTimeouts(ctx, 500)
					if err != nil && ctx.Err() == nil {
						slog.Warn("sweep", "err", err)
					}
					if n < 500 {
						break
					}
				}
			}
		}
	}()

	go func() {
		after := envDur("STUCK_AFTER", time.Minute)
		tick := time.NewTicker(envDur("STUCK_EVERY", 5*time.Second))
		defer tick.Stop()
		for {
			if _, _, err := store.Stuck(ctx, after); err != nil && ctx.Err() == nil {
				slog.Warn("stuck sagas", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()

	serveMetrics(ctx)
	serve(ctx, env("HTTP_ADDR", ":8080"), api.Handler())
	slog.Info("orchestrator up")
	<-ctx.Done()
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func ledgerd(ctx context.Context, _ []string) error {
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	commands, replies := topics()
	store := &ledger.Store{DB: p, Replies: replies}
	stop, err := consumer.Start(ctx, consumer.Config{
		Name: "ledger", Brokers: brokers(), Topic: commands, Group: env("TOPIC_PREFIX", "") + "ledger",
		Concurrency: envInt("CONSUMER_CONCURRENCY", 32), RetryEvery: env("RETRY_EVERY", "@every 10s"),
		RetryWork: envDur("RETRY_WORK", 5*time.Second), Chaos: chaosCfg(),
		SessionTimeout: envDur("KAFKA_SESSION_TIMEOUT", 6*time.Second), ChaosKey: chaosKey,
	}, func(ctx context.Context, value []byte) (string, error) {
		c, err := decode(value, func(c wire.Command) time.Time { return c.SentAt }, "ledger")
		if err != nil {
			return "", err
		}
		return store.Handle(ctx, c)
	})
	if err != nil {
		return err
	}
	defer stop()
	serveMetrics(ctx)
	slog.Info("ledger up")
	<-ctx.Done()
	return nil
}

func relayd(ctx context.Context, _ []string) error {
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	serveMetrics(ctx)
	mode := env("RELAY_MODE", "poll")
	slog.Info("relay up", "mode", mode)
	switch mode {
	case "poll":
		w := relay.NewWriter(brokers())
		defer w.Close()
		done := make(chan struct{})
		for _, schema := range []string{"saga", "ledger"} {
			go func() {
				(&relay.Poller{DB: p, Writer: w, Schema: schema, Batch: envInt("POLL_BATCH", 500),
					Interval: envDur("POLL_INTERVAL", 10*time.Millisecond)}).Run(ctx)
				done <- struct{}{}
			}()
		}
		<-done
		<-done
		return nil
	case "cdc":
		return (&relay.CDC{
			PGURL: env("PG_URL", "postgres://postgres:postgres@localhost:5432/ledger"), Brokers: brokers(),
			Slot: env("CDC_SLOT", "outbox"), Tick: envDur("CDC_TICK", 5*time.Millisecond),
			MetricsPort: envInt("CDC_METRICS_PORT", 9101), DB: p, Retention: envDur("CDC_RETENTION", time.Minute),
		}).Run(ctx)
	}
	return fmt.Errorf("RELAY_MODE must be poll or cdc, got %q", mode)
}
