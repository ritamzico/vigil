#!/usr/bin/env python3
"""WAL recovery benchmark harness for vigil.

For each cell (branch x lines x interruption_pct x interruption_type) it runs
`trials` independent measurements of *recovery time*: the wall-clock time from
a fresh daemon start to "ready" (index fully loaded and queryable), where ready
is detected by polling `vigil "| count"` until it equals the expected number of
lines.

Per trial:
  1. wipe socket + data dir (fresh, no pre-existing snapshot/WAL)
  2. INGEST: start daemon, wait until count == expected (build index + WAL)
  3. settle: sleep >= fsync interval so the WAL tail is durable
  4. interrupt: `--stop` (graceful, forces final checkpoint) or kill -9 (crash)
  5. RECOVERY (measured): restart daemon, time until count == expected again
  6. teardown

The no-WAL (main) binary ignores the persistence flags, so for it recovery is
always a full re-read from line 0 -- the apples-to-apples baseline.

usage:
  bench.py --wal-bin bench/bin/vigil-wal --nowal-bin bench/bin/vigil-nowal \
           --out bench/out/raw.csv
"""
import argparse
import csv
import os
import shutil
import signal
import subprocess
import time

SOCKET = "/tmp/vigil.sock"
DATA_DIR = "/tmp/vigil-bench-data"
TIME_FIELD = "timestamp"


def sh(*args):
    return subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def query_count(bin_path):
    """Return integer count from `bin "| count"`, or None if not ready/parseable."""
    try:
        r = subprocess.run([bin_path, "| count"], capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return None
    if r.returncode != 0:
        return None
    out = r.stdout.strip()
    try:
        return int(out)
    except ValueError:
        return None


def wipe():
    if os.path.exists(SOCKET):
        os.remove(SOCKET)
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)


def daemon_cmd(bin_path, log_path, is_wal, fsync_ms, ckpt_secs):
    cmd = [bin_path, "--watch", log_path, "--time-field", TIME_FIELD]
    if is_wal:
        cmd += ["--data-dir", DATA_DIR,
                "--fsync-interval-ms", str(fsync_ms),
                "--checkpoint-interval-secs", str(ckpt_secs)]
    return cmd


def start_daemon(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_ready(bin_path, expected, timeout_s, poll_s=0.003):
    """Poll until count == expected. Returns elapsed seconds, or None on timeout."""
    t0 = time.perf_counter()
    deadline = t0 + timeout_s
    while time.perf_counter() < deadline:
        if query_count(bin_path) == expected:
            return time.perf_counter() - t0
        time.sleep(poll_s)
    return None


def graceful_stop(bin_path, proc):
    sh(bin_path, "--stop")
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def hard_kill(proc):
    try:
        proc.send_signal(signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()
    # crash leaves the socket file behind; the next start removes it, but clean up
    if os.path.exists(SOCKET):
        os.remove(SOCKET)


def run_trial(bin_path, is_wal, log_path, expected, interrupt, fsync_ms, ckpt_secs,
              settle_s, ingest_timeout, recovery_timeout):
    wipe()
    cmd = daemon_cmd(bin_path, log_path, is_wal, fsync_ms, ckpt_secs)

    # --- ingest phase ---
    proc = start_daemon(cmd)
    if wait_ready(bin_path, expected, ingest_timeout) is None:
        proc.kill(); proc.wait(); wipe()
        return None  # ingest failed

    time.sleep(settle_s)  # let WAL fsync (and any periodic checkpoint) land

    if interrupt == "stop":
        graceful_stop(bin_path, proc)
    else:  # kill
        hard_kill(proc)

    # --- recovery phase (measured) ---
    proc2 = start_daemon(cmd)
    rec = wait_ready(bin_path, expected, recovery_timeout)
    recovery_ms = None if rec is None else rec * 1000.0

    # teardown
    sh(bin_path, "--stop")
    try:
        proc2.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc2.kill(); proc2.wait()
    wipe()
    return recovery_ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wal-bin", required=True)
    ap.add_argument("--nowal-bin", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--log-dir", default="bench/logs")
    ap.add_argument("--sizes", type=int, nargs="+",
                    default=[1000, 10000, 100000, 500000, 1000000])
    ap.add_argument("--pcts", type=int, nargs="+", default=[10, 50, 100])
    ap.add_argument("--types", nargs="+", default=["stop", "kill"])
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--fsync-ms", type=int, default=200)
    ap.add_argument("--ckpt-secs", type=int, default=2)
    ap.add_argument("--settle-s", type=float, default=0.5)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    os.makedirs(args.log_dir, exist_ok=True)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    # pre-generate one log file per distinct line count (deterministic / reused)
    line_counts = sorted({int(round(n * p / 100.0))
                          for n in args.sizes for p in args.pcts if int(round(n * p / 100.0)) > 0})
    log_for = {}
    for lc in line_counts:
        path = os.path.join(args.log_dir, f"log_{lc}.log")
        if not (os.path.exists(path) and sum(1 for _ in open(path)) == lc):
            subprocess.run(["python3", "scripts/generate_logs.py",
                            "--lines", str(lc), "--output", path,
                            "--seed", str(args.seed), "--time-field", TIME_FIELD], check=True)
        log_for[lc] = path

    branches = [("wal", args.wal_bin, True), ("nowal", args.nowal_bin, False)]

    f = open(args.out, "w", newline="")
    w = csv.writer(f)
    w.writerow(["branch", "lines", "interruption_pct", "interruption_type", "trial", "recovery_ms"])

    total = len(branches) * len(args.sizes) * len(args.pcts) * len(args.types) * args.trials
    done = 0
    for n in args.sizes:
        for pct in args.pcts:
            expected = int(round(n * pct / 100.0))
            if expected <= 0:
                continue
            log_path = log_for[expected]
            # generous-but-bounded timeouts scaled to size (ingest/recovery of
            # 1M lines is only a few seconds; cap a true hang at ~200s, not 500s)
            ingest_to = max(60, expected / 5000.0)
            recov_to = max(60, expected / 5000.0)
            for itype in args.types:
                for bname, bpath, is_wal in branches:
                    for t in range(1, args.trials + 1):
                        ms = run_trial(bpath, is_wal, log_path, expected, itype,
                                       args.fsync_ms, args.ckpt_secs, args.settle_s,
                                       ingest_to, recov_to)
                        w.writerow([bname, n, pct, itype, t,
                                    "" if ms is None else f"{ms:.3f}"])
                        f.flush()
                        done += 1
                    print(f"[{done}/{total}] {bname} n={n} pct={pct} {itype} "
                          f"last={'FAIL' if ms is None else f'{ms:.1f}ms'}", flush=True)
    f.close()
    print("DONE ->", args.out)


if __name__ == "__main__":
    main()
