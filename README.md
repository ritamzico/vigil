# vigil

A fast, queryable log watcher for structured JSON logs. Run it as a daemon pointed at a log file to tail the file and build an in-memory index. Query from any terminal instantly without re-scanning the file.

```
vigil --watch app.log --time-field timestamp
vigil "level = ERROR AND status >= 500 | count"
vigil "level = ERROR | p99 latency_ms"
```

## How it works

`vigil --watch` starts a daemon that tails the file and indexes every JSON log line into memory. It maintains a field index (for fast equality and comparison queries) and a time index (for time range queries). Queries are sent to the daemon over a Unix socket and return immediately from the index.

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

Combine with `AND` for a range:

```
timestamp > 2026-01-15T00:00:00Z AND timestamp < 2026-01-15T06:00:00Z
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

- **In-memory only.** The entire log file is indexed in RAM. Not suited for very large files.
- **No persistence.** Restarting the daemon re-reads the file from scratch.
- **One file per daemon.** Each daemon instance watches a single file.
- **No parentheses.** Complex boolean expressions use operator precedence (`AND` before `OR`) rather than grouping.
- **Time range queries require `--time-field`.** Without it, the time field is treated as a regular string field.
