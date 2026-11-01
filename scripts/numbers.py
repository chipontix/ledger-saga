#!/usr/bin/env python3
"""Print the README's numbers as markdown, read only from results/.

    python3 scripts/numbers.py            # after make chaos and make bench
"""
import glob
import json
import os
import re

R = os.path.join(os.path.dirname(__file__), "..", "results")
B = os.path.join(R, "bench")


def load(path):
    with open(path) as f:
        return json.load(f)


def chaos():
    runs = load(os.path.join(R, "chaos.json"))
    points = {r["point"] for r in runs if r["point"] != "none"}
    seeds = max(r["seed"] for r in runs)
    passed = sum(r["pass"] for r in runs)
    keys = sum(r["keys"] for r in runs)
    kills = sum(r["kills"] for r in runs)
    print(f"- crash matrix: {len(points)} crash points × {seeds} seeds + {seeds} no-crash baselines = "
          f"**{passed}/{len(runs)} runs pass**, {keys:,} transfers, {kills} SIGKILLs")


def bench():
    audited = violations = 0
    checks = sorted(glob.glob(os.path.join(B, "*.check.json")))
    for p in checks:
        c = load(p)
        audited += sum(c["states"].values())
        violations += len(c["violations"] or [])
    print(f"- audited after every benchmark run: **{audited:,} sagas over {len(checks)} runs, {violations} violations**")
    print()
    print("| relay | clients | sagas/s | saga p50 | saga p99 | outbox → consumer p50 / p99 (ledger, orchestrator) |")
    print("|---|---|---|---|---|---|")
    for relay in ("cdc", "poll"):
        for c in (100, 1000):
            p = os.path.join(B, f"{relay}-c{c}.json")
            if not os.path.exists(p):
                continue
            r = load(p)
            d = dict(re.findall(r"(\w+)=([\d.]+)", open(os.path.join(B, f"{relay}-c{c}.delivery.txt")).read()))
            dl = (f"{d.get('p50_ledger_ms', '?')} / {d.get('p99_ledger_ms', '?')} ms, "
                  f"{d.get('p50_orchestrator_ms', '?')} / {d.get('p99_orchestrator_ms', '?')} ms")
            lat = r["saga_latency_ms"]
            print(f"| {relay} | {c} | {r['sagas_per_s']:.0f} | {lat['p50']:.0f} ms | {lat['p99']:.0f} ms | {dl} |")
    print()
    for victim in ("ledger", "orchestrator", "relay"):
        p = os.path.join(B, f"recovery-{victim}.json")
        if os.path.exists(p):
            r = load(p)
            print(f"- recovery, {victim} SIGKILLed for {r['down_s']:.1f} s: backlog of {r['backlog_at_restart']} "
                  f"terminal {r['backlog_drained_after_restart_s']:.1f} s after restart, "
                  f"{r['cancelled_around_outage']} cancelled, {r['still_open']} still open")
    idle = os.path.join(B, "idle-db-tx.txt")
    if os.path.exists(idle):
        tx = dict(re.findall(r"^(no relay|poll relay|cdc relay)[^:]*:\s+([\d-]+) tx/s", open(idle).read(), re.M))
        print(f"- idle database transactions/s: {tx}")
    hw = os.path.join(B, "hardware.txt")
    if os.path.exists(hw):
        print("- hardware: " + "; ".join(open(hw).read().split("\n")).strip("; "))


if __name__ == "__main__":
    chaos()
    bench()
