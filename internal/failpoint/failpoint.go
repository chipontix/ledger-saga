// Package failpoint kills the process with SIGKILL at a named point in the
// code, on the n-th time execution reaches it. FAILPOINT=name:n arms one
// point per process; unarmed points cost one string compare.
package failpoint

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
)

// Points is every crash point and the process it lives in. The chaos
// harness iterates over this list.
var Points = []struct{ Name, Process string }{
	{"api.before_commit", "orchestrator"},        // saga row + hold command written, not committed
	{"api.after_commit", "orchestrator"},         // committed, HTTP response not sent
	{"orch.reply.before_commit", "orchestrator"}, // transition computed, not committed
	{"orch.reply.after_commit", "orchestrator"},  // committed, Kafka offset not committed
	{"orch.timeout.before_commit", "orchestrator"},
	{"ledger.before_commit", "ledger"}, // money moved in tx, not committed
	{"ledger.after_commit", "ledger"},  // committed, Kafka offset not committed
	{"relay.poll.before_publish", "relay-poll"},
	{"relay.poll.after_publish", "relay-poll"}, // in Kafka, outbox rows not deleted
	{"relay.cdc.handler", "relay-cdc"},         // decoded from WAL, not yet in Kafka
	{"relay.cdc.after_publish", "relay-cdc"},   // in Kafka, LSN not acknowledged
}

var (
	armed string
	nth   int64
	hits  atomic.Int64
)

func init() {
	name, n, ok := strings.Cut(os.Getenv("FAILPOINT"), ":")
	if !ok {
		return
	}
	v, err := strconv.ParseInt(n, 10, 64)
	if err != nil || v < 1 {
		panic(fmt.Sprintf("FAILPOINT=%s:%s: hit count must be a positive integer", name, n))
	}
	armed, nth = name, v
	slog.Info("failpoint armed", "point", armed, "hit", nth)
}

func Hit(name string) {
	if name != armed || hits.Add(1) != nth {
		return
	}
	fmt.Fprintf(os.Stderr, "FAILPOINT %s hit %d: SIGKILL\n", name, nth)
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {} // SIGKILL is not instant; never run past the point
}
