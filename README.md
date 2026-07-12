# vigil

A fast, queryable log watcher for structured JSON logs. Run it as a daemon pointed at a log file to tail the file and build an in-memory index. Query from any terminal instantly without re-scanning the file.

```
vigil --watch app.log --time-field timestamp
vigil "level = ERROR AND status >= 500 | count"
vigil "level = ERROR | p99 latency_ms"
```

## How it works

`vigil --watch` starts a daemon that tails the file and indexes every JSON log line into memory. It maintains a field index (for fast equality and comparison queries) and a time index (for time range queries). Queries are sent to the daemon over a Unix socket and return immediately from the index.

To survive restarts, the daemon persists its progress with a **write-ahead log (WAL)** and periodic **snapshots** (see [Persistence & crash recovery](#persistence--crash-recovery)). On restart it reloads the last snapshot and replays the WAL instead of re-reading the whole file from the beginning.

## Installation

```
cargo install --path .
```

## Usage

### Start the daemon

```
vigil --watch <file> [--time-field <field>] [-d]
```

| Flag | Description |
|------|-------------|
| `--watch <file>` | Path to the JSON log file to tail |
| `--time-field <field>` | JSON field containing RFC 3339 timestamps (enables time range queries) |
| `-d`, `--detach` | Run the daemon in the background |

The daemon watches the file for new lines as they are appended. It must be running before queries can be issued.

**Examples:**

```sh
# Foreground (useful for debugging)
vigil --watch /var/log/app.log

# With time indexing
vigil --watch /var/log/app.log --time-field timestamp

# Detached background daemon
vigil --watch /var/log/app.log --time-field timestamp -d
```

### Query

```
vigil "<query>"
```

Queries are sent to the running daemon. Results are printed to stdout, one raw log line per result (or a single number for aggregations).

### Stop the daemon

```
vigil --stop
```

---

## Persistence & crash recovery

By default the daemon persists its index so a restart doesn't have to re-read and re-parse the entire log file. Two mechanisms work together:

- **Write-ahead log (WAL).** Every indexed line is appended to `wal.log` and periodically fsync'd, so recently ingested events survive a crash.
- **Snapshots.** Periodically (and on a graceful `--stop`) the full in-memory index is written to `snapshot.bin`, and the WAL is truncated.

On startup the daemon loads the latest snapshot, replays any WAL records newer than the snapshot, and resumes tailing from where it left off — far faster than a cold re-scan for large files.

Persisted state lives in a per-file data directory: `$HOME/.local/share/vigil/<hash>/` by default, where `<hash>` is derived from the watched file's absolute path (so each watched file gets its own snapshot/WAL).

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir <path>` | `$HOME/.local/share/vigil` | Base directory for snapshots and the WAL |
| `--fsync-interval-ms <ms>` | `500` | How often the WAL is flushed and fsync'd to disk |
| `--checkpoint-interval-secs <secs>` | `60` | How often a full snapshot is written and the WAL truncated |
| `--no-persist` | off | Disable persistence entirely (pure in-memory; restart re-reads the file) |

**Trade-offs.** A shorter `--fsync-interval-ms` narrows the window of events that could be lost on a hard crash, at the cost of more frequent disk I/O. A shorter `--checkpoint-interval-secs` keeps the WAL small and speeds crash recovery, at the cost of more frequent snapshot writes.

**Examples:**

```sh
# Default persistence (snapshots every 60s, WAL fsync every 500ms)
vigil --watch /var/log/app.log --time-field timestamp

# Durable: fsync every 100ms, snapshot every 10s
vigil --watch /var/log/app.log --fsync-interval-ms 100 --checkpoint-interval-secs 10

# Custom data directory
vigil --watch /var/log/app.log --data-dir /mnt/fast-ssd/vigil

# Opt out of persistence
vigil --watch /var/log/app.log --no-persist
```

---

## Query language

A query is a filter expression, optionally followed by an aggregation using `|`.

```
<filter> | <aggregation>
```

### Filters

#### Field comparison

```
<field> <op> <value>
```

| Operator | Meaning |
|----------|---------|
| `=` | equal |
| `!=` | not equal |
| `<` | less than |
| `<=` | less than or equal |
| `>` | greater than |
| `>=` | greater than or equal |

Values are automatically typed: `500` is a number, `true`/`false` are booleans, anything else is a string.

```
status = 500
level = ERROR
latency_ms > 1000
active != true
```

#### Boolean logic

Use `AND` and `OR` to combine filters. `AND` binds tighter than `OR`.

```
status = 500 AND level = ERROR
status = 500 OR status = 503
level = ERROR AND method = POST OR level = WARN AND status >= 400
```

#### Time range

Requires `--time-field <field>` when starting the daemon. The field's value must be an RFC 3339 timestamp (e.g. `2026-01-15T00:00:00Z`).

```
<time-field> > <timestamp>    # events after this time
<time-field> < <timestamp>    # events before this time
```

`<timestamp>` is one of:

| Form | Meaning |
|------|---------|
| RFC 3339 | absolute time, e.g. `2026-01-15T00:00:00Z` |
| `now` | the current time |
| `-<N><s\|m\|h\|d>` | `N` seconds/minutes/hours/days ago, e.g. `-1h` |

Relative values resolve when the query runs, so `timestamp > -1h` always means the last hour.

```
timestamp > -1h                 # events from the last hour
timestamp > -30m | count        # count of events from the last 30 minutes
```

Combine with `AND` for a range:

```
timestamp > 2026-01-15T00:00:00Z AND timestamp < 2026-01-15T06:00:00Z
timestamp > -1d AND timestamp < -12h
```

#### Match all

Start with `|` to skip filtering and aggregate over all events:

```
| count
```

### Aggregations

Append `| <aggregation>` to a filter to compute a result instead of returning raw events.

| Aggregation | Output |
|-------------|--------|
| `count` | Number of matching events |
| `count by <field>` | Number of matching events that have `<field>` |
| `avg <field>` | Average of `<field>` across matching events |
| `p<N> <field>` | Nth percentile of `<field>` (N is 1–100) |

```
level = ERROR | count
level = ERROR | count by request_id
status >= 500 | avg latency_ms
status >= 500 | p99 latency_ms
status >= 500 | p50 latency_ms
| count
```

---

## Supported log format

Each line must be a flat JSON object. Scalar field types are indexed:

| JSON type | Supported |
|-----------|-----------|
| string | yes |
| number | yes |
| boolean | yes |
| array | no (ignored) |
| object | no (ignored) |

**Example log line:**
```json
{"timestamp": "2026-01-15T12:00:00Z", "level": "ERROR", "status": 500, "latency_ms": 243, "method": "POST", "path": "/api/orders"}
```

---

## Limitations

- **In-memory index.** The entire log file is indexed in RAM. Not suited for files larger than available memory.
- **One file per daemon.** Each daemon instance watches a single file.
- **No parentheses.** Complex boolean expressions use operator precedence (`AND` before `OR`) rather than grouping.
- **Time range queries require `--time-field`.** Without it, the time field is treated as a regular string field.
