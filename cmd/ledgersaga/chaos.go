package main

// The crash harness. For every (crash point, seed) it builds a fresh
// database and fresh topics, starts the three services as real processes
// with the point armed in one of them, drives a seeded workload through the
// HTTP API, lets the failpoint SIGKILL the process, restarts it, waits until
// every saga is terminal and audits the result with internal/check. Message
// duplication, reordering and late redelivery are on in every run.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/chipontix/ledger-saga/internal/check"
	"github.com/chipontix/ledger-saga/internal/db"
	"github.com/chipontix/ledger-saga/internal/failpoint"
	"github.com/chipontix/ledger-saga/internal/ledger"
	"github.com/chipontix/ledger-saga/internal/saga"
)

func init() {
	cmds["chaos"] = chaos
	cmds["check"] = checkCmd
}

type run struct {
	Point    string             `json:"point"`
	Process  string             `json:"process"`
	Relay    string             `json:"relay"`
	Seed     int                `json:"seed"`
	Hit      int                `json:"hit"`
	Restart  time.Duration      `json:"restart_delay_ns"`
	Kills    int                `json:"kills"`
	Keys     int                `json:"keys"`
	States   map[saga.State]int `json:"states"`
	Absorbed map[string]float64 `json:"consumer_results"`
	Quiesce  time.Duration      `json:"quiesce_ns"`
	Problems []string           `json:"problems"`
	Pass     bool               `json:"pass"`
}

func chaos(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	seeds := fs.Int("seeds", 5, "seeds per crash point")
	only := fs.String("points", "", "comma-separated crash points (default: none + every point)")
	transfers := fs.Int("transfers", 200, "transfers per run")
	accounts := fs.Int("accounts", 40, "accounts per run")
	parallel := fs.Int("parallel", 4, "runs at once")
	out := fs.String("out", "docs/chaos-matrix.md", "markdown report")
	jsonOut := fs.String("json", "results/chaos.json", "machine-readable results")
	logs := fs.String("logs", "results/raw", "per-process logs")
	keep := fs.Bool("keep", false, "keep databases and logs of passing runs too")
	admin := fs.String("admin-url", env("PG_ADMIN_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"), "")
	fs.Parse(args)

	bin, err := os.Executable()
	if err != nil {
		return err
	}
	type point struct{ Name, Process string }
	points := []point{{"none", "-"}}
	for _, p := range failpoint.Points {
		points = append(points, point{p.Name, p.Process})
	}
	if *only != "" {
		points = slices.DeleteFunc(points, func(p point) bool { return !slices.Contains(strings.Split(*only, ","), p.Name) })
	}
	var runs []*run
	for _, p := range points {
		for s := 1; s <= *seeds; s++ {
			relay := []string{"poll", "cdc"}[s%2]
			switch p.Process {
			case "relay-poll":
				relay = "poll"
			case "relay-cdc":
				relay = "cdc"
			}
			r := &run{Point: p.Name, Process: p.Process, Relay: relay, Seed: s,
				Hit: 1 + (s*7)%10, Restart: time.Duration(300+(s*37)%10*250) * time.Millisecond}
			if p.Name == "none" {
				r.Hit = 0
			}
			runs = append(runs, r)
		}
	}

	started := time.Now()
	slots := make(chan int, *parallel)
	for i := range *parallel {
		slots <- i
	}
	var wg sync.WaitGroup
	for _, r := range runs {
		slot := <-slots
		wg.Go(func() {
			defer func() { slots <- slot }()
			h := &harness{run: r, bin: bin, admin: *admin, slot: slot, transfers: *transfers,
				accounts: *accounts, logDir: filepath.Join(*logs, runName(r)), keep: *keep}
			h.execute(ctx)
			mark := "PASS"
			if !r.Pass {
				mark = "FAIL " + strings.Join(r.Problems, "; ")
			}
			fmt.Printf("%-28s seed %d %-4s hit %-2d %6.1fs  %v  %s\n", r.Point, r.Seed, r.Relay, r.Hit,
				r.Quiesce.Seconds(), r.States, mark)
		})
		if ctx.Err() != nil {
			break
		}
	}
	wg.Wait()

	if err := os.MkdirAll(filepath.Dir(*jsonOut), 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(runs, "", "  ")
	if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(*out, []byte(matrix(runs, *seeds, *transfers, time.Since(started), os.Args)), 0o644); err != nil {
		return err
	}
	for _, r := range runs {
		if !r.Pass {
			return fmt.Errorf("invariants violated, see %s", *out)
		}
	}
	return nil
}

func runName(r *run) string {
	return fmt.Sprintf("chaos_%s_%d", strings.ReplaceAll(r.Point, ".", "_"), r.Seed)
}

// stamp makes topic names unique per invocation, so a failed run's leftover
// messages can never leak into a later run with the same name.
var stamp = strconv.FormatInt(time.Now().Unix(), 36)

type harness struct {
	*run
	bin, admin, logDir  string
	slot                int
	transfers, accounts int
	keep                bool
	procs               []*proc
}

func (h *harness) problem(format string, args ...any) {
	h.Problems = append(h.Problems, fmt.Sprintf(format, args...))
}

func (h *harness) execute(ctx context.Context) {
	name := runName(h.run)
	if err := h.do(ctx, name); err != nil {
		h.problem("%v", err)
	}
	for _, p := range h.procs {
		p.stop()
		if h.Point != "none" && p.armed != "" {
			h.Kills = p.kills
		} else if p.kills > 0 {
			h.problem("%s killed %d times without an armed failpoint", p.name, p.kills)
		}
		for _, e := range p.unexpected {
			h.problem("%s exited unexpectedly: %s", p.name, e)
		}
	}
	if h.Point != "none" && h.Kills != 1 {
		h.problem("failpoint fired %d times, want 1", h.Kills)
	}
	h.Pass = len(h.Problems) == 0
	h.cleanup(name, h.Pass && !h.keep) // a failed run keeps its database and logs
}

func (h *harness) do(ctx context.Context, name string) error {
	if err := os.MkdirAll(h.logDir, 0o755); err != nil {
		return err
	}
	adm, err := db.Open(ctx, h.admin, 2)
	if err != nil {
		return err
	}
	defer adm.Close()
	h.dropSlot(ctx, name)
	if _, err := adm.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
		return err
	}
	if _, err := adm.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		return err
	}
	u, _ := url.Parse(h.admin)
	u.Path = "/" + name
	base := 21000 + h.slot*10
	envs := []string{
		"PG_URL=" + u.String(), "TOPIC_PREFIX=" + name + "." + stamp + ".", "PARTITIONS=4", "PG_MAX_CONNS=8",
		"STEP_TIMEOUT=1s", "HOLD_ATTEMPTS=2", "MAX_BACKOFF=4s", "SWEEP_EVERY=50ms", "RETRY_EVERY=@every 5s", "RETRY_WORK=1s",
		"CONSUMER_CONCURRENCY=8", "TIMEOUT_GRACE=7s", // grace must cover the 6s group session timeout
		"RELAY_MODE=" + h.Relay, "CDC_SLOT=" + name, "CDC_TICK=5ms",
		"CDC_METRICS_PORT=" + strconv.Itoa(base+4),
		"CHAOS_SEED=" + strconv.Itoa(h.Seed), "CHAOS_DEFER=0.10", "CHAOS_DUP=0.05", "CHAOS_DELAY=0.02", "CHAOS_MAX_DELAY=200ms",
		"CHAOS_REDELIVER=0.05", "CHAOS_MAX_LATE=1500ms",
	}
	for _, step := range [][]string{{"migrate"}, {"seed", "-accounts", strconv.Itoa(h.accounts), "-balance", "1000", "-frozen-every", "10",
		"-currencies", strings.Join(chaosCurrencies, ",")}} {
		cmd := exec.CommandContext(ctx, h.bin, step...)
		cmd.Env = append(os.Environ(), envs...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("%s: %v: %s", step[0], err, out)
		}
	}
	api := "http://localhost:" + strconv.Itoa(base)
	const grace = 7 * time.Second
	booted := time.Now()
	roles := []struct {
		name, process string
		env           []string
	}{
		{"orchestrator", "orchestrator", []string{"HTTP_ADDR=:" + strconv.Itoa(base), "METRICS_ADDR=:" + strconv.Itoa(base+1)}},
		{"ledger", "ledger", []string{"METRICS_ADDR=:" + strconv.Itoa(base+2)}},
		{"relay", "relay-" + h.Relay, []string{"METRICS_ADDR=:" + strconv.Itoa(base+3)}},
	}
	for _, r := range roles {
		p := &proc{name: r.name, bin: h.bin, args: []string{r.name}, env: append(slices.Clone(envs), r.env...),
			log: filepath.Join(h.logDir, r.name+".log"), restart: h.Restart}
		if r.process == h.Process {
			p.armed = fmt.Sprintf("%s:%d", h.Point, h.Hit)
		}
		if err := p.start(); err != nil {
			return err
		}
		h.procs = append(h.procs, p)
	}
	if err := waitHTTP(ctx, api+"/healthz", 20*time.Second); err != nil {
		return err
	}
	warm, err := warmup(ctx, api)
	if err != nil {
		return err
	}
	// Start the workload after the orchestrator's timeout grace, so a crash
	// of the ledger or relay actually drives sagas into timeouts and releases.
	time.Sleep(time.Until(booted.Add(grace)))

	h.workload(ctx, api)
	h.Keys += warm
	start := time.Now()
	pg, err := db.Open(ctx, u.String(), 2)
	if err != nil {
		return err
	}
	defer pg.Close()
	if err := h.quiesce(ctx, pg); err != nil {
		return err
	}
	h.Quiesce = time.Since(start)

	rep, err := check.Run(ctx, pg, true)
	if err != nil {
		return err
	}
	h.States = rep.States
	h.Problems = append(h.Problems, rep.Violations...)
	var n int
	if err := pg.QueryRow(ctx, `SELECT count(*) FROM saga.transfers`).Scan(&n); err != nil {
		return err
	}
	if n != h.Keys {
		h.problem("%d transfers for %d idempotency keys", n, h.Keys)
	}
	h.Absorbed = map[string]float64{}
	for i, port := range []int{base + 1, base + 2} {
		for k, v := range scrape(fmt.Sprintf("http://localhost:%d/metrics", port)) {
			h.Absorbed[[]string{"orch_", "ledger_"}[i]+k] += v
		}
	}
	return nil
}

// chaosCurrencies are the currencies of a chaos run's accounts.
var chaosCurrencies = []string{"USD", "EUR"}

// workload sends the seeded transfers through the API. Every POST is retried
// with its key until it gets an answer, the way a client riding out an API
// crash would; one in ten is re-sent after success and must get the same id.
func (h *harness) workload(ctx context.Context, api string) {
	rng := rand.New(rand.NewPCG(uint64(h.Seed), 7))
	type job struct {
		key, body string
		resend    bool
	}
	jobs := make(chan job)
	go func() {
		defer close(jobs)
		pace := time.NewTicker(40 * time.Millisecond) // ~25/s, so the crash lands mid-workload
		defer pace.Stop()
		for i := range h.transfers {
			<-pace.C
			from := 1 + rng.IntN(h.accounts)
			to := 1 + rng.IntN(h.accounts+1) // accounts+1 does not exist
			cur := func(id int) string { return ledger.SeedCurrency(int64(id), chaosCurrencies) }
			// Three in four cross-currency pairs are redrawn, so most transfers
			// can complete; about one in five still crosses and must refund.
			for to <= h.accounts && cur(to) != cur(from) && rng.IntN(4) != 0 {
				to = 1 + rng.IntN(h.accounts)
			}
			if to == from {
				to = from%h.accounts + 1
			}
			currency := cur(from)
			if rng.IntN(20) == 0 { // the client names the wrong currency: hold refuses
				currency = cur(from + ledger.Shards)
			}
			jobs <- job{fmt.Sprintf("seed%d-%d", h.Seed, i),
				fmt.Sprintf(`{"from":%d,"to":%d,"amount":%d,"currency":%q}`, from, to, 1+rng.IntN(400), currency),
				rng.IntN(10) == 0}
		}
	}()
	client := &http.Client{Timeout: 5 * time.Second}
	var mu sync.Mutex
	ids := map[string]string{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for j := range jobs {
				id, err := post(ctx, client, api, j.key, j.body)
				if err == nil && j.resend {
					var again string
					if again, err = post(ctx, client, api, j.key, j.body); err == nil && again != id {
						err = fmt.Errorf("key %s answered %s then %s", j.key, id, again)
					}
				}
				mu.Lock()
				if err != nil {
					h.problem("%v", err)
				} else {
					ids[j.key] = id
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	h.Keys = len(ids)
}

// warmup waits until a transfer completes end to end, i.e. both consumer
// groups have their partitions, so the workload measures the system and not
// the first rebalance. Its key counts like any other.
func warmup(ctx context.Context, api string) (int, error) {
	c := &http.Client{Timeout: 35 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		id, err := post(ctx, c, api, fmt.Sprintf("warmup-%d", i), `{"from":1,"to":2,"amount":1,"currency":"USD"}`)
		if err != nil {
			return i + 1, err
		}
		resp, err := c.Get(api + "/transfers/" + id + "?wait=30s")
		if err != nil {
			continue
		}
		var v struct{ State saga.State }
		json.NewDecoder(resp.Body).Decode(&v)
		resp.Body.Close()
		if v.State == saga.Completed {
			return i + 1, nil
		}
	}
	return 0, errors.New("no warm-up transfer completed within 60s")
}

func post(ctx context.Context, c *http.Client, api, key, body string) (string, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, api+"/transfers", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		resp, err := c.Do(req)
		if err == nil {
			var v struct{ ID string }
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch {
			case resp.StatusCode == 200 || resp.StatusCode == 201:
				err = json.Unmarshal(b, &v)
				return v.ID, err
			case resp.StatusCode < 500:
				return "", fmt.Errorf("key %s: HTTP %d %s", key, resp.StatusCode, b)
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return "", fmt.Errorf("key %s: no answer within 60s", key)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// quiesce waits until every saga is terminal and both outboxes are drained
// (poll mode), then a little longer for late redeliveries and the retry
// topic, which may still replay but must not change anything.
func (h *harness) quiesce(ctx context.Context, pg *pgxpool.Pool) error {
	deadline := time.Now().Add(120 * time.Second)
	for {
		var open, pending int
		err := pg.QueryRow(ctx, `SELECT (SELECT count(*) FROM saga.transfers WHERE finished_at IS NULL),
			(SELECT count(*) FROM saga.outbox) + (SELECT count(*) FROM ledger.outbox)`).Scan(&open, &pending)
		if err == nil && open == 0 && (h.Relay == "cdc" || pending == 0) {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("not quiescent after 120s: %d sagas open, %d outbox rows (err %v)", open, pending, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(6 * time.Second) // late redeliveries (<= 1.5s) and one retry-topic cycle (5s)
	return nil
}

func scrape(u string) map[string]float64 {
	out := map[string]float64{}
	resp, err := http.Get(u)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "consumer_messages_total{") {
			continue
		}
		_, rest, _ := strings.Cut(line, `result="`)
		res, val, _ := strings.Cut(rest, `"} `)
		v, _ := strconv.ParseFloat(val, 64)
		out[res] += v
	}
	return out
}

func waitHTTP(ctx context.Context, u string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if resp, err := http.Get(u); err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s not up after %v", u, d)
}

func (h *harness) dropSlot(ctx context.Context, name string) {
	adm, err := db.Open(ctx, h.admin, 1)
	if err != nil {
		return
	}
	defer adm.Close()
	adm.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, name)
}

func (h *harness) cleanup(name string, dropDB bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.dropSlot(ctx, name)
	if adm, err := db.Open(ctx, h.admin, 1); err == nil {
		if dropDB {
			adm.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
			os.RemoveAll(h.logDir)
		}
		adm.Close()
	}
	var topics []string
	for _, t := range []string{"ledger.commands", "saga.replies"} {
		for _, s := range []string{"", ".retry", ".dlq"} {
			topics = append(topics, name+"."+stamp+"."+t+s)
		}
	}
	client := &kafka.Client{Addr: kafka.TCP(brokers()...), Timeout: 10 * time.Second}
	client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: topics})
}

// proc supervises one service process: a SIGKILL from its failpoint is
// expected once and followed by a restart without the failpoint; any other
// exit is reported.
type proc struct {
	name, bin, log, armed string
	args, env             []string
	restart               time.Duration

	mu         sync.Mutex
	cmd        *exec.Cmd
	exited     chan struct{}
	stopping   bool
	kills      int
	unexpected []string
}

func (p *proc) start() error {
	f, err := os.OpenFile(p.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(p.bin, p.args...)
	cmd.Env = append(os.Environ(), p.env...)
	if p.kills == 0 && p.armed != "" {
		cmd.Env = append(cmd.Env, "FAILPOINT="+p.armed)
	}
	fmt.Fprintf(f, "--- %s start (failpoint %q)\n", time.Now().Format(time.RFC3339Nano), p.armed)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return err
	}
	exited := make(chan struct{})
	p.mu.Lock()
	p.cmd, p.exited = cmd, exited
	p.mu.Unlock()
	go func() {
		err := cmd.Wait()
		f.Close()
		close(exited)
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.stopping {
			return
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
				p.kills++
			} else {
				p.unexpected = append(p.unexpected, err.Error())
			}
		} else {
			p.unexpected = append(p.unexpected, fmt.Sprintf("exit %v", err))
		}
		go func() {
			time.Sleep(p.restart)
			p.mu.Lock()
			stopping := p.stopping
			p.mu.Unlock()
			if !stopping {
				if err := p.start(); err != nil {
					p.mu.Lock()
					p.unexpected = append(p.unexpected, "restart: "+err.Error())
					p.mu.Unlock()
				}
			}
		}()
	}()
	return nil
}

func (p *proc) stop() {
	p.mu.Lock()
	p.stopping = true
	cmd, exited := p.cmd, p.exited
	p.mu.Unlock()
	if cmd == nil {
		return
	}
	cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		<-exited
	}
}

func checkCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	rest := fs.Bool("quiescent", false, "also check the at-rest invariants")
	fs.Parse(args)
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	rep, err := check.Run(ctx, p, *rest)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if len(rep.Violations) > 0 {
		return fmt.Errorf("%d violations", len(rep.Violations))
	}
	return nil
}

func matrix(runs []*run, seeds, transfers int, took time.Duration, argv []string) string {
	var b strings.Builder
	commit, _ := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	pass, kills, keys := 0, 0, 0
	totals := map[string]float64{}
	states := map[saga.State]int{}
	for _, r := range runs {
		if r.Pass {
			pass++
		}
		kills += r.Kills
		keys += r.Keys
		for k, v := range r.Absorbed {
			totals[k] += v
		}
		for k, v := range r.States {
			states[k] += v
		}
	}
	fmt.Fprintf(&b, "# Crash matrix\n\nGenerated by `%s` at commit `%s`, %s, wall time %s.\n\n",
		strings.Join(append([]string{"ledgersaga"}, argv[1:]...), " "), strings.TrimSpace(string(commit)),
		time.Now().UTC().Format("2006-01-02 15:04 UTC"), took.Round(time.Second))
	fmt.Fprintf(&b, "**%d/%d runs passed** · %d transfers · %d SIGKILLs · every run with 10%% of saga steps having all their first deliveries "+
		"deferred through the retry topic (so later messages overtake them), 5%% duplicated, 2%% delayed inside the batch and 5%% "+
		"redelivered up to 1.5s late, on both consumers. Accounts are in %s; some transfers name the wrong currency "+
		"or cross currencies and must be refused or refunded.\n\n", pass, len(runs), keys, kills, strings.Join(chaosCurrencies, " and "))
	fmt.Fprintf(&b, "Terminal states over all runs: %v.\n\n", states)
	fmt.Fprintf(&b, "Absorbed by the consumers that were alive at the end of each run (killed processes lose their counters): "+
		"orchestrator %v duplicate / %v stale replies, ledger %v duplicate / %v replayed commands; %v first deliveries "+
		"deferred through the retry topic by the chaos layer, %v sent there by real handler errors.\n\n",
		totals["orch_duplicate"], totals["orch_stale"], totals["ledger_duplicate"], totals["ledger_replayed"],
		totals["orch_deferred"]+totals["ledger_deferred"], totals["orch_retried"]+totals["ledger_retried"])
	b.WriteString("Each cell: ✅ all invariants held, ❌ violation. Relay alternates by seed unless the point is in a relay.\n\n")
	b.WriteString("| crash point | process |")
	for s := 1; s <= seeds; s++ {
		fmt.Fprintf(&b, " seed %d |", s)
	}
	b.WriteString("\n|---|---|" + strings.Repeat("---|", seeds) + "\n")
	for i := 0; i < len(runs); i += seeds {
		fmt.Fprintf(&b, "| `%s` | %s |", runs[i].Point, runs[i].Process)
		for _, r := range runs[i:min(i+seeds, len(runs))] {
			mark := "✅"
			if !r.Pass {
				mark = "❌"
			}
			hit := ""
			if r.Hit > 0 {
				hit = fmt.Sprintf(" hit %d", r.Hit)
			}
			fmt.Fprintf(&b, " %s %s%s |", mark, r.Relay, hit)
		}
		b.WriteString("\n")
	}
	b.WriteString("\n## Runs\n\n| point | seed | relay | killed at hit | restart after | keys | completed | refunded | rejected | cancelled | quiesce | problems |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range runs {
		fmt.Fprintf(&b, "| `%s` | %d | %s | %d | %v | %d | %d | %d | %d | %d | %.1fs | %s |\n", r.Point, r.Seed, r.Relay,
			r.Hit, r.Restart, r.Keys, r.States[saga.Completed], r.States[saga.Refunded], r.States[saga.Rejected],
			r.States[saga.Cancelled], r.Quiesce.Seconds(), strings.Join(r.Problems, "<br>"))
	}
	b.WriteString("\nInvariants checked after every run (`internal/check`): journal sums to zero globally and per operation; " +
		"balance rows equal the journal; no negative balance; no operation crosses a shard; every leg is in its account's currency " +
		"and every operation in its saga's; per currency, nothing in clearing at rest and customer money equals the opening total; " +
		"nothing held at rest; every saga terminal with exactly the ledger operations its state " +
		"allows and exactly -amount/+amount moved (completed) or nothing; every transition log is a legal path ending in the " +
		"saga's state; one transfer per idempotency key, and a re-sent key returns the same transfer.\n")
	return b.String()
}
