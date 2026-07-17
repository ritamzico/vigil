# vigil

A fast, queryable log watcher for structured JSON logs. Run it as a daemon pointed at a log file to tail the file and build an in-memory index. Query from any terminal instantly without re-scanning the file.

```
vigil --watch app.log --time-field timestamp
vigil "level = ERROR AND status >= 500 | count"
vigil "level = ERROR | p99 latency_ms"
vigil -f "level = ERROR"
```

## How it works

`vigil --watch` starts a daemon that tails the file and indexes every JSON log line into memory. It maintains a field index (for fast equality and comparison queries) and a time index (for time range queries). Queries are sent to the daemon over a Unix socket and return immediately from the index.

To survive restarts, the daemon persists its progress with a write-ahead log (WAL) and periodic snapshots. On restart it reloads the last snapshot and replays the WAL instead of re-reading the whole file from the beginning. See [Architecture](docs/architecture.md) for the full design.

## Installation

```
cargo install --path .
```

## Quick start

Each log line must be a flat JSON object:

```json
{"timestamp": "2026-01-15T12:00:00Z", "level": "ERROR", "status": 500, "latency_ms": 243, "method": "POST", "path": "/api/orders"}
```

Start the daemon, query, and stop:

```sh
# Start a background daemon tailing the file, with time indexing
vigil --watch /var/log/app.log --time-field timestamp -d

# Filter: raw matching lines, oldest first
vigil "level = ERROR AND status >= 500"

# Aggregate
vigil "status >= 500 | count"
vigil "level = ERROR | p99 latency_ms"

# Time ranges (requires --time-field)
vigil "timestamp > -1h | count"

# Limit results
vigil "level = ERROR | tail 5"

# Stop the daemon
vigil --stop
```

## Documentation

- [Query language](docs/query-language.md) — filters, operators, `AND`/`OR`/`NOT`, time ranges, aggregations, `limit`/`tail`
- [Operations](docs/operations.md) — CLI flags, socket, data directory, persistence tuning, retention, log rotation, troubleshooting
- [Architecture](docs/architecture.md) — tailer, index, WAL, snapshots, recovery semantics, durability guarantees and their limits

## Limitations

- **In-memory index.** The entire log file is indexed in RAM. Not suited for files larger than available memory. Use [`--max-events`](docs/operations.md#retention---max-events) to bound memory.
- **One daemon per machine, one file at a time.** The daemon listens on a fixed socket path; running `vigil --watch` while a daemon is up re-points it at the new file.
- **Time range queries require `--time-field`.** Without it, the time field is treated as a regular string field.
