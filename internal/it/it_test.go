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

	"github.com/prometheus/client_golang/prometheus"

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
	// Accounts 1-4 are USD, 5-8 EUR, 9-12 USD, ... (ledger.SeedCurrency).
	currencies = []string{"USD", "EUR"}
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
	if err := ledger.Seed(ctx, pool, accounts, 1000, 10, currencies); err != nil {
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

// post sends a transfer in the source's currency.
func (s *system) post(key string, from, to, amount int64) (int, view) {
	return s.postIn(key, ledger.SeedCurrency(from, currencies), from, to, amount)
}

func (s *system) postIn(key, currency string, from, to, amount int64) (int, view) {
	body := fmt.Sprintf(`{"from":%d,"to":%d,"amount":%d,"currency":%q}`, from, to, amount, currency)
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
				currency         string // "" = the source's
				want             saga.State
				reason           string
			}{
				{1, 2, 100, "", saga.Completed, ""},
				{5, 13, 100, "", saga.Completed, ""}, // EUR to EUR, same shard
				{6, 15, 100, "", saga.Completed, ""}, // EUR to EUR, shard 2 -> 3
				{3, 4, 5000, "", saga.Rejected, "insufficient funds"},
				{5, 10, 50, "", saga.Refunded, "account frozen"},
				{6, 99, 50, "", saga.Refunded, "account not found"},
				{10, 1, 50, "", saga.Rejected, "account frozen"},
				{1, 5, 50, "", saga.Refunded, "currency mismatch"},    // USD source, EUR destination
				{2, 3, 50, "EUR", saga.Rejected, "currency mismatch"}, // client names the wrong currency
			}
			for i, c := range cases {
				cur := c.currency
				if cur == "" {
					cur = ledger.SeedCurrency(c.from, currencies)
				}
				code, v := s.postIn(fmt.Sprintf("k%d", i), cur, c.from, c.to, c.amount)
				if code != http.StatusCreated {
					t.Fatalf("POST %d: %d", i, code)
				}
				if got := s.wait(v.ID); got.State != c.want || got.Reason != c.reason {
					t.Errorf("%d->%d %d %s: %s (%s), want %s (%s)", c.from, c.to, c.amount, cur,
						got.State, got.Reason, c.want, c.reason)
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
	hold := wire.Command{SagaID: id, Step: wire.Hold, Account: 1, Amount: 1500, Currency: "USD", Attempt: 1,
		MsgID: wire.CommandID(id, wire.Hold, 1)}
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

func TestCurrencyRequiredAndKeyed(t *testing.T) {
	s := newSystem(t, "poll", 5, saga.DefaultPolicy())
	for _, cur := range []string{"", "usd", "US", "USDT"} {
		if code, _ := s.postIn("bad-"+cur, cur, 1, 2, 10); code != http.StatusUnprocessableEntity {
			t.Errorf("currency %q: HTTP %d, want 422", cur, code)
		}
	}
	if code, _ := s.postIn("k", "USD", 1, 2, 10); code != http.StatusCreated {
		t.Fatalf("first POST: %d", code)
	}
	if code, _ := s.postIn("k", "EUR", 1, 2, 10); code != http.StatusConflict {
		t.Fatalf("same key, other currency: %d, want 409", code)
	}
}

// The audit must see money crossing currencies even when every operation
// balances: a forged EUR operation that pays a USD account out of clearing.
func TestAuditCatchesCrossCurrencyPosting(t *testing.T) {
	ctx := context.Background()
	s := newSystem(t, "none", 8, saga.DefaultPolicy())
	if _, err := s.pool.Exec(ctx, `
		WITH op AS (INSERT INTO ledger.operations (saga_id, step, account_id, amount, currency, outcome)
		            VALUES (gen_random_uuid(), 'forged', 1, 5, 'EUR', 'ok') RETURNING id)
		INSERT INTO ledger.entries (op_id, account_id, bucket, amount)
		SELECT id, a, 'available', x FROM op, (VALUES (1::bigint, 5::bigint), ($1::bigint, -5::bigint)) v(a, x)`,
		ledger.Clearing(ledger.Shard(1))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE ledger.accounts SET available = available + 5 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	rep, err := check.Run(ctx, s.pool, false)
	if err != nil {
		t.Fatal(err)
	}
	var cross, usd, eur bool
	for _, v := range rep.Violations {
		cross = cross || strings.Contains(v, "posts to account 1 in another currency")
		usd = usd || strings.HasPrefix(v, "USD: customer money")
		eur = eur || strings.HasPrefix(v, "EUR: customer money")
	}
	if !cross || !usd || !eur {
		t.Fatalf("violations %q: want the cross-currency leg and both currencies out of balance", rep.Violations)
	}
}

// A saga that cannot move is counted once it is older than the threshold,
// and a finished one is not. With no relay, nothing is ever delivered: the
// sweeper retries hold, then moves to RELEASE_PENDING and waits forever.
func TestStuckSagasAreCounted(t *testing.T) {
	ctx := context.Background()
	s := newSystem(t, "none", 8, saga.Policy{StepTimeout: 100 * time.Millisecond, MaxBackoff: time.Second, HoldAttempts: 2})
	past := &saga.Store{DB: s.pool, Policy: saga.DefaultPolicy(), Commands: s.commands,
		Now: func() time.Time { return time.Now().Add(-2 * time.Minute) }}
	for i := range 2 {
		if _, _, err := past.Create(ctx, fmt.Sprintf("old-%d", i), saga.Request{From: 1, To: 2, Amount: 1, Currency: "USD"}); err != nil {
			t.Fatal(err)
		}
	}
	if code, _ := s.post("fresh", 1, 2, 1); code != http.StatusCreated {
		t.Fatalf("POST: %d", code)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE saga.transfers SET state = 'COMPLETED', finished_at = now()
		WHERE idem_key = 'old-1'`); err != nil {
		t.Fatal(err)
	}
	now := &saga.Store{DB: s.pool, Now: time.Now}
	stuck, oldest, err := now.Stuck(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, n := range stuck {
		total += n
	}
	if total != 1 || oldest < 2*time.Minute-5*time.Second {
		t.Fatalf("stuck %v, oldest %v: want one stuck, about 2m old", stuck, oldest)
	}
	if g := gauges(t); g["saga_stuck"] != 1 || g["saga_oldest_open_seconds"] != oldest.Seconds() {
		t.Fatalf("exported %v, want saga_stuck 1 and the oldest age", g)
	}
}

// gauges sums each saga_* gauge over its labels, as exported.
func gauges(t *testing.T) map[string]float64 {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if g := m.GetGauge(); g != nil && strings.HasPrefix(mf.GetName(), "saga_") {
				out[mf.GetName()] += g.GetValue()
			}
		}
	}
	return out
}

// Statements are the journal as an account holder sees it. Read page by
// page in any page size they are the same list, each ends at the balance
// row, refusals do not appear, and per currency the closing balances add up
// to the opening total, as the audit says they must.
func TestStatementAgreesWithAudit(t *testing.T) {
	ctx := context.Background()
	s := newSystem(t, "poll", 12, saga.Policy{StepTimeout: 5 * time.Second, MaxBackoff: 10 * time.Second, HoldAttempts: 3})
	for i, c := range []struct{ from, to, amount int64 }{
		{1, 2, 100},  // completed
		{1, 5, 50},   // USD to EUR: captured, then refunded
		{3, 4, 5000}, // insufficient funds: rejected, posts nothing
	} {
		_, v := s.post(fmt.Sprintf("d%d", i), c.from, c.to, c.amount)
		s.wait(v.ID)
	}
	var wg sync.WaitGroup
	for i := range 40 { // accounts 6-12 only, so 1-5 keep the lines above
		wg.Add(1)
		go func() {
			defer wg.Done()
			from, to := int64(6+i%7), int64(6+(i*3+1)%7)
			if from == to {
				return
			}
			_, v := s.post(fmt.Sprintf("r%d", i), from, to, int64(1+i*7%300))
			s.wait(v.ID)
		}()
	}
	wg.Wait()
	s.audit()

	srv := httptest.NewServer(ledger.Handler(s.pool))
	defer srv.Close()
	walk := func(id int64, limit int) []ledger.Line {
		var lines []ledger.Line
		var last ledger.Statement
		for after := int64(0); ; after = last.Next {
			resp, err := http.Get(fmt.Sprintf("%s/accounts/%d/statement?limit=%d&after=%d", srv.URL, id, limit, after))
			if err != nil {
				t.Fatal(err)
			}
			last = ledger.Statement{}
			json.NewDecoder(resp.Body).Decode(&last)
			resp.Body.Close()
			lines = append(lines, last.Lines...)
			if last.Next == 0 {
				break
			}
		}
		if n := len(lines); n == 0 || lines[n-1].AvailableAfter != last.Available || lines[n-1].HeldAfter != last.Held {
			t.Fatalf("account %d: statement does not end at the balance %d/%d: %+v", id, last.Available, last.Held, lines)
		}
		return lines
	}
	closing := map[string]int64{}
	for id := int64(1); id <= 12; id++ {
		all := walk(id, 1000)
		for _, limit := range []int{1, 3} {
			if got := walk(id, limit); fmt.Sprint(got) != fmt.Sprint(all) {
				t.Fatalf("account %d, pages of %d:\n%v\nwant\n%v", id, limit, got, all)
			}
		}
		closing[ledger.SeedCurrency(id, currencies)] += all[len(all)-1].AvailableAfter + all[len(all)-1].HeldAfter
		var steps []string
		for _, l := range all {
			steps = append(steps, l.Step)
		}
		want := map[int64]string{1: "[open hold capture hold capture refund]", 2: "[open credit]", 3: "[open]", 5: "[open]"}[id]
		if want != "" && fmt.Sprint(steps) != want {
			t.Errorf("account %d steps %v, want %s", id, steps, want)
		}
	}
	rep, err := check.Run(ctx, s.pool, true)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]int64{"USD": 8 * 1000, "EUR": 4 * 1000}; fmt.Sprint(closing) != fmt.Sprint(want) || rep.CustomerSum != 12*1000 {
		t.Fatalf("statements close at %v, audit customer sum %d; want %v", closing, rep.CustomerSum, want)
	}
	for u, code := range map[string]int{"/accounts/99/statement": 404, "/accounts/1/statement?limit=0": 400, "/accounts/x/statement": 400} {
		resp, err := http.Get(srv.URL + u)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != code {
			t.Errorf("GET %s: %d, want %d", u, resp.StatusCode, code)
		}
	}
}

// A daily limit holds under concurrent holds on one account, and the audit
// reports holds over it (ADR-0011).
func TestDailyLimit(t *testing.T) {
	ctx := context.Background()
	s := newSystem(t, "poll", 12, saga.Policy{StepTimeout: 5 * time.Second, MaxBackoff: 10 * time.Second, HoldAttempts: 3})
	if _, err := s.pool.Exec(ctx, `UPDATE ledger.accounts SET daily_limit = 300 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	// Ten holds of 100 race for the row lock of account 1, which has the
	// balance for all of them but the limit for three.
	var wg sync.WaitGroup
	got := make([]view, 10)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, v := s.post(fmt.Sprintf("l%d", i), 1, 2, 100)
			got[i] = s.wait(v.ID)
		}()
	}
	wg.Wait()
	n := map[string]int{}
	for _, v := range got {
		n[string(v.State)+"/"+v.Reason]++
	}
	if n["COMPLETED/"] != 3 || n["REJECTED/daily limit exceeded"] != 7 {
		t.Fatalf("outcomes %v, want 3 completed and 7 over the limit", n)
	}
	// A completed transfer stays counted, and the limit is per account.
	if _, v := s.post("l-more", 1, 2, 1); s.wait(v.ID).Reason != "daily limit exceeded" {
		t.Error("account 1 took more after reaching its limit")
	}
	if _, v := s.post("l-other", 3, 2, 400); s.wait(v.ID).State != saga.Completed {
		t.Error("account 3 has no limit but was refused")
	}
	s.audit()

	// A forged hold that the ledger would have refused is reported.
	if _, err := s.pool.Exec(ctx, `INSERT INTO ledger.operations (saga_id, step, account_id, amount, currency, outcome)
		VALUES (gen_random_uuid(), 'hold', 1, 1, 'USD', 'ok')`); err != nil {
		t.Fatal(err)
	}
	rep, err := check.Run(ctx, s.pool, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(rep.Violations, "\n"), "account 1: holds took 301 on ") {
		t.Fatalf("violations %q: want account 1 over its daily limit", rep.Violations)
	}
}
