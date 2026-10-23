//go:build integration

// Package it runs the real components against real Postgres and Kafka in
// Testcontainers: go test -tags integration ./internal/it
package it

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"

	"github.com/chipontix/ledger-saga/internal/check"
	"github.com/chipontix/ledger-saga/internal/consumer"
	"github.com/chipontix/ledger-saga/internal/db"
	"github.com/chipontix/ledger-saga/internal/ledger"
	"github.com/chipontix/ledger-saga/internal/relay"
	"github.com/chipontix/ledger-saga/internal/saga"
	"github.com/chipontix/ledger-saga/internal/wire"
)

var (
	pgURL   string
	brokers []string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:17.6-alpine",
		tcpostgres.WithDatabase("ledger"), tcpostgres.WithUsername("postgres"), tcpostgres.WithPassword("postgres"),
		testcontainers.WithCmd("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=16", "-c", "fsync=off"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres:", err)
		os.Exit(1)
	}
	if pgURL, err = pg.ConnectionString(ctx, "sslmode=disable"); err != nil {
		panic(err)
	}
	kc, broker, err := startKafka(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka:", err)
		os.Exit(1)
	}
	brokers = []string{broker}
	code := m.Run()
	kc.Terminate(ctx)
	pg.Terminate(ctx)
	os.Exit(code)
}

// startKafka runs apache/kafka in KRaft mode. The advertised listener must
// carry the host port, so the port is chosen before the container starts.
func startKafka(ctx context.Context) (testcontainers.Container, string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/kafka:3.9.1",
			ExposedPorts: []string{"9092/tcp"},
			Env: map[string]string{
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "EXTERNAL://:9092,CONTROLLER://:9093",
				"KAFKA_ADVERTISED_LISTENERS":                     fmt.Sprintf("EXTERNAL://localhost:%d", port),
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "EXTERNAL",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{network.MustParsePort("9092/tcp"): {{HostPort: fmt.Sprint(port)}}}
			},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
		},
	})
	return c, fmt.Sprintf("localhost:%d", port), err
}

// system is one isolated deployment: its own database and topic prefix.
type system struct {
	t        *testing.T
	pool     *pgxpool.Pool
	api      *httptest.Server
	commands string
	replies  string
	cancel   context.CancelFunc
}

func newSystem(t *testing.T, relayMode string, accounts int, policy saga.Policy) *system {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	name := strings.ToLower(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name()))
	admin, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatal(err)
	}
	admin.Close()
	url := strings.Replace(pgURL, "/ledger?", "/"+name+"?", 1)
	pool, err := db.Open(ctx, url, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Seed(ctx, pool, accounts, 1000, 10); err != nil {
		t.Fatal(err)
	}
	s := &system{t: t, pool: pool, commands: name + ".commands", replies: name + ".replies", cancel: cancel}
	var topics []kafka.TopicConfig
	for _, base := range []string{s.commands, s.replies} {
		for _, tp := range []string{base, consumer.RetryTopic(base), consumer.DLQTopic(base)} {
			topics = append(topics, kafka.TopicConfig{Topic: tp, NumPartitions: 2, ReplicationFactor: 1})
		}
	}
	if _, err := (&kafka.Client{Addr: kafka.TCP(brokers...)}).CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: topics}); err != nil {
		t.Fatal(err)
	}

	api := &saga.API{}
	store := &saga.Store{DB: pool, Policy: policy, Commands: s.commands, Now: time.Now, OnFinish: api.Finished}
	api.Store = store
	led := &ledger.Store{DB: pool, Replies: s.replies}
	cfg := func(name, topic string) consumer.Config {
		return consumer.Config{Name: name, Brokers: brokers, Topic: topic, Group: topic + ".g", Concurrency: 4,
			RetryEvery: "@every 1s", RetryWork: 500 * time.Millisecond, SessionTimeout: 6 * time.Second}
	}
	stopO, err := consumer.Start(ctx, cfg("orchestrator", s.replies), func(ctx context.Context, v []byte) (string, error) {
		var r wire.Reply
		if err := json.Unmarshal(v, &r); err != nil {
			return "", consumer.Permanent{Err: err}
		}
		return store.HandleReply(ctx, r)
	})
	if err != nil {
		t.Fatal(err)
	}
	stopL, err := consumer.Start(ctx, cfg("ledger", s.commands), func(ctx context.Context, v []byte) (string, error) {
		var c wire.Command
		if err := json.Unmarshal(v, &c); err != nil {
			return "", consumer.Permanent{Err: err}
		}
		return led.Handle(ctx, c)
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for ctx.Err() == nil {
			store.SweepTimeouts(ctx, 100)
			time.Sleep(50 * time.Millisecond)
		}
	}()
	switch relayMode {
	case "poll":
		w := relay.NewWriter(brokers)
		for _, schema := range []string{"saga", "ledger"} {
			go (&relay.Poller{DB: pool, Writer: w, Schema: schema, Batch: 100, Interval: 5 * time.Millisecond}).Run(ctx)
		}
	case "cdc":
		go func() {
			err := (&relay.CDC{PGURL: url, Brokers: brokers, Slot: name, Tick: 5 * time.Millisecond,
				MetricsPort: freePort(t), DB: pool, Retention: time.Minute}).Run(ctx)
			if err != nil && ctx.Err() == nil {
				t.Errorf("cdc relay: %v", err)
			}
		}()
	}
	s.api = httptest.NewServer(api.Handler())
	t.Cleanup(func() {
		s.api.Close()
		stopO()
		stopL()
		cancel()
		time.Sleep(200 * time.Millisecond)
		pool.Close()
	})
	return s
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type view struct {
	ID     string     `json:"id"`
	State  saga.State `json:"state"`
	Reason string     `json:"reason"`
}

func (s *system) post(key string, from, to, amount int64) (int, view) {
	body := fmt.Sprintf(`{"from":%d,"to":%d,"amount":%d}`, from, to, amount)
	req, _ := http.NewRequest(http.MethodPost, s.api.URL+"/transfers", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	var v view
	json.NewDecoder(resp.Body).Decode(&v)
	return resp.StatusCode, v
}

func (s *system) wait(id string) view {
	resp, err := http.Get(s.api.URL + "/transfers/" + id + "?wait=30s")
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	var v view
	json.NewDecoder(resp.Body).Decode(&v)
	return v
}

func (s *system) audit() {
	rep, err := check.Run(context.Background(), s.pool, true)
	if err != nil {
		s.t.Fatal(err)
	}
	for _, v := range rep.Violations {
		s.t.Error(v)
	}
}

func TestTransfersEndToEnd(t *testing.T) {
	for _, mode := range []string{"poll", "cdc"} {
		t.Run(mode, func(t *testing.T) {
			s := newSystem(t, mode, 20, saga.Policy{StepTimeout: 5 * time.Second, MaxBackoff: 10 * time.Second, HoldAttempts: 3})
			cases := []struct {
				from, to, amount int64
				want             saga.State
			}{
				{1, 2, 100, saga.Completed},
				{3, 4, 5000, saga.Rejected}, // balance is 1000
				{5, 10, 50, saga.Refunded},  // every 10th account is frozen
				{6, 99, 50, saga.Refunded},  // no such account
				{10, 1, 50, saga.Rejected},  // frozen source
			}
			for i, c := range cases {
				code, v := s.post(fmt.Sprintf("k%d", i), c.from, c.to, c.amount)
				if code != http.StatusCreated {
					t.Fatalf("POST %d: %d", i, code)
				}
				if got := s.wait(v.ID); got.State != c.want {
					t.Errorf("%d->%d %d: %s (%s), want %s", c.from, c.to, c.amount, got.State, got.Reason, c.want)
				}
			}
			s.audit()
		})
	}
}

func TestIdempotencyKeyUnderConcurrency(t *testing.T) {
	s := newSystem(t, "poll", 5, saga.DefaultPolicy())
	var mu sync.Mutex
	ids := map[string]int{}
	codes := map[int]int{}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			code, v := s.post("same-key", 1, 2, 10)
			mu.Lock()
			ids[v.ID]++
			codes[code]++
			mu.Unlock()
		})
	}
	wg.Wait()
	if len(ids) != 1 || codes[http.StatusCreated] != 1 || codes[http.StatusOK] != 31 {
		t.Fatalf("32 concurrent POSTs with one key: ids %v codes %v", ids, codes)
	}
	if code, _ := s.post("same-key", 1, 2, 11); code != http.StatusConflict {
		t.Fatalf("same key, different body: %d, want 409", code)
	}
	for id := range ids {
		if v := s.wait(id); v.State != saga.Completed {
			t.Fatalf("state %s", v.State)
		}
	}
	s.audit()
}

func TestPoisonMessageIsDeadLettered(t *testing.T) {
	s := newSystem(t, "poll", 5, saga.DefaultPolicy())
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), BatchTimeout: time.Millisecond, RequiredAcks: kafka.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(context.Background(), kafka.Message{Topic: s.commands, Key: []byte("1"), Value: []byte("{not json")}); err != nil {
		t.Fatal(err)
	}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: consumer.DLQTopic(s.commands),
		GroupID: "dlq-inspector", StartOffset: kafka.FirstOffset, MaxWait: 100 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	m, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("no dead letter: %v", err)
	}
	if string(m.Value) != "{not json" {
		t.Fatalf("dead letter carries %q", m.Value)
	}
	var reason string
	for _, h := range m.Headers {
		if h.Key == "x-error-message" {
			reason = string(h.Value)
		}
	}
	if !strings.Contains(reason, "invalid character") {
		t.Fatalf("x-error-message = %q", reason)
	}
	// The consumer moved on: a real transfer still completes.
	_, v := s.post("after-poison", 1, 2, 10)
	if got := s.wait(v.ID); got.State != saga.Completed {
		t.Fatalf("after poison: %s", got.State)
	}
}

func TestLedgerReplaysRecordedDecision(t *testing.T) {
	ctx := context.Background()
	s := newSystem(t, "none", 3, saga.DefaultPolicy())
	led := &ledger.Store{DB: s.pool, Replies: s.replies}
	id := uuid.New()
	hold := wire.Command{SagaID: id, Step: wire.Hold, Account: 1, Amount: 1500, Attempt: 1, MsgID: wire.CommandID(id, wire.Hold, 1)}
	if res, err := led.Handle(ctx, hold); err != nil || res != "applied" {
		t.Fatalf("first hold: %s %v", res, err)
	}
	// Top the account up so a re-decision would now succeed.
	if _, err := s.pool.Exec(ctx, `UPDATE ledger.accounts SET available = available + 1000 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if res, _ := led.Handle(ctx, hold); res != "duplicate" {
		t.Fatalf("same message again: %s, want duplicate", res)
	}
	hold.Attempt, hold.MsgID = 2, wire.CommandID(id, wire.Hold, 2)
	if res, err := led.Handle(ctx, hold); err != nil || res != "replayed" {
		t.Fatalf("retry: %s %v, want replayed", res, err)
	}
	var outcomes []string
	rows, _ := s.pool.Query(ctx, `SELECT payload FROM ledger.outbox ORDER BY id`)
	for rows.Next() {
		var p string
		rows.Scan(&p)
		var r wire.Reply
		json.Unmarshal([]byte(p), &r)
		outcomes = append(outcomes, string(r.Outcome)+"/"+r.Reason+"/"+r.MsgID[len(r.MsgID)-10:])
	}
	if len(outcomes) != 2 || outcomes[0] != outcomes[1] || !strings.HasPrefix(outcomes[0], "rejected/insufficient funds") {
		t.Fatalf("replies %v: want the same rejection twice", outcomes)
	}
	var held int64
	s.pool.QueryRow(ctx, `SELECT held FROM ledger.accounts WHERE id = 1`).Scan(&held)
	if held != 0 {
		t.Fatalf("held %d after a replayed rejection", held)
	}
}
