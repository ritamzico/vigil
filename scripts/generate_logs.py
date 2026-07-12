#!/usr/bin/env python3
"""Generate N schema-valid flat JSON log lines for vigil benchmarking.

Each line is a flat JSON object of indexable scalars (string/number/bool),
matching the format documented in vigil's README. Output is deterministic
for a given --seed so runs are reproducible.

usage: generate_logs.py --lines N --output path/to/file.log [--seed 42]
"""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
LEVEL_WEIGHTS = [40, 40, 15, 5]
METHODS = ["GET", "POST", "PUT", "DELETE"]
PATHS = ["/api/orders", "/api/users", "/api/login", "/healthz", "/api/search"]
STATUSES = [200, 201, 301, 400, 404, 500, 503]
STATUS_WEIGHTS = [50, 10, 5, 10, 10, 10, 5]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--time-field", default="timestamp",
        help="name of the RFC3339 timestamp field (matches daemon --time-field)")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    base = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    with open(args.output, "w") as f:
        for i in range(args.lines):
            # monotonically increasing timestamp, fixed 100ms spacing -> reproducible
            ts = base + timedelta(milliseconds=100 * i)
            entry = {
                args.time_field: ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z",
                "level": rng.choices(LEVELS, weights=LEVEL_WEIGHTS)[0],
                "status": rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
                "latency_ms": rng.randint(1, 5000),
                "method": rng.choice(METHODS),
                "path": rng.choice(PATHS),
                "request_id": f"req-{rng.randint(0, 10**9):09d}",
                "bytes": rng.randint(0, 1 << 20),
                "active": rng.random() < 0.5,
            }
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
