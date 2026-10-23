#!/usr/bin/env bash
# Load and crash-recovery benchmarks. Everything, the load generator
# included, runs inside Docker on one network, and every run starts from an
# empty database and empty topics. Results land in results/bench/.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; . ./.env; set +a; fi
OUT=results/bench
mkdir -p "$OUT"
DURATION=${DURATION:-60s}
C="docker compose"

fresh() { # relay mode
  $C --profile app --profile load down -v --remove-orphans >/dev/null 2>&1 || true
  RELAY_MODE=$1 $C --profile app up -d --build --wait >/dev/null
  $C up -d prometheus >/dev/null # grafana is not needed to record numbers
}

load() { # label clients duration
  $C --profile load run --rm -T load load -api http://orchestrator:8080 -c "$2" -d "$3" -warmup 10s -label "$1"
}

# observe label seconds: VM vmstat and per-container CPU while a run is going
observe() {
  colima ssh -p "${COLIMA_PROFILE:-default}" -- vmstat -t 1 "$2" > "$OUT/$1.vmstat.txt" 2>/dev/null &
  ( for _ in $(seq 1 $(( $2 / 5 ))); do
      docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' | sed "s/^ledger-saga-//"; echo "--"; sleep 4
    done > "$OUT/$1.cpu.txt" ) &
}

# prom label window: outbox-to-consumer delivery quantiles over the last window
prom() {
  local q out=""
  for p in 0.5 0.99; do
    q="histogram_quantile($p, sum by (le, consumer) (increase(outbox_delivery_seconds_bucket[$2])))"
    out+=$(curl -s "http://localhost:${PROMETHEUS_PORT:-9090}/api/v1/query" --data-urlencode "query=$q" |
      python3 -c "import json,sys; d=json.load(sys.stdin)['data']['result']; print(' '.join('p%s_%s_ms=%.1f' % ('$p'[2:].ljust(2,'0'), r['metric']['consumer'], float(r['value'][1])*1000) for r in d))")" "
  done
  echo "$out" > "$OUT/$1.delivery.txt"
}

check() { # label: audit the invariants once the run has drained
  $C --profile load run --rm -T load check -quiescent > "$OUT/$1.check.json" || echo "INVARIANTS VIOLATED in $1" >&2
}

hardware() {
  {
    echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null) cores, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GiB"
    echo "docker vm: $(docker info --format '{{.NCPU}} vCPU, {{.MemTotal}} bytes, {{.OperatingSystem}}, kernel {{.KernelVersion}}')"
    echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD)"
  } > "$OUT/hardware.txt"
}

hardware
for relay in ${LOAD_RELAYS-cdc poll}; do
  for c in 100 1000; do
    fresh "$relay"
    observe "$relay-c$c" 75
    load "$relay-c$c" "$c" "$DURATION" > "$OUT/$relay-c$c.json"
    prom "$relay-c$c" 60s
    wait
    check "$relay-c$c"
  done
done

# Recovery: SIGKILL one process 30s into a 100-client run, start it again 5s
# later, and ask the database how long the backlog took to drain.
for victim in ${VICTIMS-ledger orchestrator relay}; do
  fresh cdc
  observe "recovery-$victim" 95
  load "recovery-$victim" 100 80s > "$OUT/recovery-$victim.load.json" &
  sleep 40 # 10s warm-up + 30s
  id=$($C ps -q "$victim")
  docker kill -s KILL "$id" >/dev/null
  sleep 5
  docker start "$id" >/dev/null
  wait
  killed=$(docker inspect -f '{{.State.FinishedAt}}' "$id")
  restarted=$(docker inspect -f '{{.State.StartedAt}}' "$id")
  $C --profile load run --rm -T load recovery -killed "$killed" -restarted "$restarted" > "$OUT/recovery-$victim.json"
  check "recovery-$victim"
done
echo "results in $OUT"
