package main

// Closed-loop load generator. Each of -c clients repeatedly creates a
// transfer and long-polls it until terminal, so the measured latency is the
// whole saga (hold, capture, credit, via two relays and two consumers), not
// the POST. A client that loses the API retries with the same key.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/chipontix/ledger-saga/internal/saga"
)

func init() {
	cmds["load"] = load
	cmds["recovery"] = recovery
}

type loadResult struct {
	Label       string             `json:"label"`
	Concurrency int                `json:"concurrency"`
	Window      float64            `json:"window_s"`
	Sagas       int                `json:"sagas"`
	PerSec      float64            `json:"sagas_per_s"`
	Latency     map[string]float64 `json:"saga_latency_ms"`
	PostP99     float64            `json:"post_p99_ms"`
	States      map[saga.State]int `json:"states"`
	Errors      int                `json:"client_retries"`
	Timeline    []int              `json:"completions_per_s"` // from the start, including warm-up
	Started     time.Time          `json:"started"`
}

func load(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	api := fs.String("api", env("API_URL", "http://localhost:8080"), "")
	c := fs.Int("c", 100, "concurrent clients")
	d := fs.Duration("d", 60*time.Second, "measured duration")
	warm := fs.Duration("warmup", 10*time.Second, "unmeasured lead-in")
	accounts := fs.Int("accounts", 10_000, "account ids to draw from")
	label := fs.String("label", "", "")
	fs.Parse(args)

	client := &http.Client{Transport: &http.Transport{MaxIdleConns: *c * 2, MaxIdleConnsPerHost: *c * 2,
		IdleConnTimeout: time.Minute}, Timeout: 40 * time.Second}
	start := time.Now()
	measureFrom, end := start.Add(*warm), start.Add(*warm+*d)
	var mu sync.Mutex
	res := &loadResult{Label: *label, Concurrency: *c, Window: d.Seconds(), States: map[saga.State]int{},
		Timeline: make([]int, int((*warm+*d)/time.Second)+60), Started: start}
	var lat, posts []time.Duration

	var wg sync.WaitGroup
	for w := range *c {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), uint64(start.UnixNano())))
			for time.Now().Before(end) && ctx.Err() == nil {
				from := 1 + rng.Int64N(int64(*accounts))
				to := 1 + rng.Int64N(int64(*accounts))
				if to == from {
					continue
				}
				body := fmt.Sprintf(`{"from":%d,"to":%d,"amount":%d}`, from, to, 1+rng.IntN(100))
				t0 := time.Now()
				v, retries := createAndWait(ctx, client, *api, uuid.NewString(), body)
				done := time.Now()
				mu.Lock()
				res.Errors += retries
				if v.State.Terminal() {
					if sec := int(done.Sub(start) / time.Second); sec < len(res.Timeline) {
						res.Timeline[sec]++
					}
					if t0.After(measureFrom) && done.Before(end) {
						lat = append(lat, done.Sub(t0))
						posts = append(posts, v.post)
						res.States[v.State]++
					}
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	res.Sagas = len(lat)
	res.PerSec = float64(len(lat)) / d.Seconds()
	slices.Sort(lat)
	slices.Sort(posts)
	ms := func(xs []time.Duration, q float64) float64 {
		if len(xs) == 0 {
			return 0
		}
		return float64(xs[min(len(xs)-1, int(q*float64(len(xs))))].Microseconds()) / 1000
	}
	res.Latency = map[string]float64{"p50": ms(lat, .50), "p90": ms(lat, .90), "p99": ms(lat, .99), "max": ms(lat, 1)}
	res.PostP99 = ms(posts, .99)
	last := len(res.Timeline)
	for last > 0 && res.Timeline[last-1] == 0 {
		last--
	}
	res.Timeline = res.Timeline[:last]
	b, _ := json.Marshal(res)
	fmt.Println(string(b))
	fmt.Fprintf(os.Stderr, "%s c=%d: %.0f sagas/s, p50 %.1fms p99 %.1fms max %.1fms, POST p99 %.1fms, %v, %d client retries\n",
		*label, *c, res.PerSec, res.Latency["p50"], res.Latency["p99"], res.Latency["max"], res.PostP99, res.States, res.Errors)
	return nil
}

type polled struct {
	State saga.State `json:"state"`
	ID    string     `json:"id"`
	post  time.Duration
}

// createAndWait POSTs (retrying with the same key on any failure) and then
// long-polls until the transfer is terminal.
func createAndWait(ctx context.Context, c *http.Client, api, key, body string) (polled, int) {
	var v polled
	retries := 0
	for ctx.Err() == nil {
		t0 := time.Now()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, api+"/transfers", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		if ok := do(c, req, &v); ok {
			v.post = time.Since(t0)
			break
		}
		retries++
		time.Sleep(50 * time.Millisecond)
	}
	for ctx.Err() == nil && !v.State.Terminal() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, api+"/transfers/"+v.ID+"?wait=30s", nil)
		post := v.post
		if !do(c, req, &v) {
			retries++
			time.Sleep(50 * time.Millisecond)
		}
		v.post = post
	}
	return v, retries
}

func do(c *http.Client, req *http.Request, v any) bool {
	resp, err := c.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		io.Copy(io.Discard, resp.Body)
		return false
	}
	return json.NewDecoder(resp.Body).Decode(v) == nil
}

// recovery reports how the system came back after a process was restarted
// at -restarted: when the first saga finished after it, and when the last
// saga that had been accepted before it finished (the backlog drained).
func recovery(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recovery", flag.ExitOnError)
	killed := fs.String("killed", "", "RFC3339Nano time the process was killed")
	restarted := fs.String("restarted", "", "RFC3339Nano time it was started again")
	fs.Parse(args)
	k, err := time.Parse(time.RFC3339Nano, *killed)
	if err != nil {
		return err
	}
	r, err := time.Parse(time.RFC3339Nano, *restarted)
	if err != nil {
		return err
	}
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	var firstAfter, drained *time.Time
	var stuck, backlog, cancelled int
	if err := p.QueryRow(ctx, `SELECT
			(SELECT min(finished_at) FROM saga.transfers WHERE finished_at > $2::timestamptz),
			(SELECT max(finished_at) FROM saga.transfers WHERE created_at < $2),
			(SELECT count(*) FROM saga.transfers WHERE created_at < $2 AND finished_at IS NULL),
			(SELECT count(*) FROM saga.transfers WHERE created_at < $2 AND (finished_at IS NULL OR finished_at > $2)),
			(SELECT count(*) FROM saga.transfers WHERE created_at BETWEEN $1::timestamptz - interval '10 seconds' AND $2::timestamptz AND state = 'CANCELLED')`,
		k, r).Scan(&firstAfter, &drained, &stuck, &backlog, &cancelled); err != nil {
		return err
	}
	out := map[string]any{"down_s": r.Sub(k).Seconds(), "backlog_at_restart": backlog, "still_open": stuck,
		"cancelled_around_outage": cancelled}
	if firstAfter != nil {
		out["first_completion_after_restart_s"] = firstAfter.Sub(r).Seconds()
	}
	if drained != nil {
		out["backlog_drained_after_restart_s"] = drained.Sub(r).Seconds()
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
	return nil
}
