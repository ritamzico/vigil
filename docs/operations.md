# Operations

## CLI reference

```
vigil --watch <file> [options]     # start the daemon
vigil "<query>"                    # query the running daemon
vigil --stop                       # stop the daemon
```

| Flag | Default | Description |
|------|---------|-------------|
| `-w`, `--watch <file>` | — | Path to the JSON log file to tail |
| `--time-field <field>` | — | JSON field containing RFC 3339 timestamps (enables time range queries) |
| `-d`, `--detach` | off | Run the daemon in the background |
| `--stop` | — | Gracefully stop the running daemon (writes a final snapshot) |
| `--data-dir <path>` | `$HOME/.local/share/vigil` | Base directory for snapshots and the WAL |
| `--fsync-interval-ms <ms>` | `500` | How often the WAL is flushed and fsync'd to disk |
| `--checkpoint-interval-secs <secs>` | `60` | How often a full snapshot is written and the WAL truncated |
| `--no-persist` | off | Disable persistence entirely (pure in-memory; restart re-reads the file) |
| `--max-events <n>` | `2000000` | Retain only the most recent events, evicting the oldest to keep memory bounded |

The daemon must be running before queries can be issued. Run it in the foreground (useful for debugging — warnings about skipped lines and WAL recovery go to stderr) or detached with `-d`:

```sh
# Foreground
vigil --watch /var/log/app.log

# With time indexing
vigil --watch /var/log/app.log --time-field timestamp

# Detached background daemon
vigil --watch /var/log/app.log --time-field timestamp -d
```

Queries print to stdout, one raw log line per result (or a single number for aggregations). See the [query language reference](query-language.md).

## One daemon per machine

The daemon listens on the fixed socket path `/tmp/vigil.sock`, so only one daemon runs per machine. Running `vigil --watch <other-file>` while a daemon is up does not start a second daemon — it tells the existing daemon to switch to the new file. The daemon checkpoints the old file's state first, so switching back later recovers from persisted state instead of re-reading.

## Socket

- **Path:** `/tmp/vigil.sock`, created when the daemon starts and removed on shutdown.
- **Permissions:** `0600` — only the user who started the daemon can query or stop it.
- **Stale sockets:** if the daemon crashed and left the socket file behind, the next `vigil --watch` detects the dead socket, removes it, and starts fresh.
- **Request cap:** requests larger than 1 MiB are rejected.

## Data directory

Persisted state lives in a per-file data directory: `<data-dir>/<hash>/`, where `<hash>` is a CRC32 of the watched file's canonicalized absolute path — each watched file gets its own state. Each directory contains:

| File | Contents |
|------|----------|
| `wal.log` | Write-ahead log of events since the last snapshot |
| `snapshot.bin` | Latest full snapshot of the index |
| `snapshot.tmp` | Transient; a snapshot mid-write (atomically renamed to `snapshot.bin`) |

Deleting a data directory is safe while the daemon is stopped — the next start re-reads the log file from the beginning.

## Persistence tuning

A shorter `--fsync-interval-ms` narrows the window of WAL records that could be lost on a hard crash (though the tailer re-reads that window from the source file if it still exists — see [durability guarantees](architecture.md#durability-guarantees-and-their-limits)), at the cost of more frequent disk I/O. A shorter `--checkpoint-interval-secs` keeps the WAL small and speeds crash recovery, at the cost of more frequent snapshot writes.

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

## Retention (`--max-events`)

With `--max-events <n>`, queries only ever see the most recent `n` events — older events are dropped once the cap is exceeded. Eviction is amortized, so the index may transiently hold up to ~1024 events beyond `n` before compacting back down. The default cap is 2,000,000 events.

```sh
# Cap memory: keep only the most recent 1M events
vigil --watch /var/log/app.log --max-events 1000000
```

## Log format

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

CRLF line endings are fine. Lines still being written (no trailing newline yet) are picked up on a later poll, never half-ingested.

**Malformed lines.** If the file already contains a non-JSON line when the daemon starts, startup fails with the line number and content — a wrong `--watch` target errors clearly. A malformed line appended while the daemon is running is skipped with a warning on stderr; the daemon keeps going.

## Log rotation

The tailer detects rotation automatically, without stalling or crashing:

- **Rename-based rotation** (e.g. logrotate's default: `app.log` → `app.log.1`, new `app.log` created): detected via inode change; the new file is ingested from the beginning.
- **Truncation / copytruncate:** detected when the file shrinks below the tailer's offset; ingestion restarts from the beginning of the file.

Events indexed from before the rotation stay in the index (subject to `--max-events`). Any lines written to the old file after vigil's last poll but before the rotation are not picked up.

## Troubleshooting

- **`Failed to connect to daemon. Check if the daemon is running.`** — queries and `--stop` need a running daemon; start one with `vigil --watch <file>`.
- **Daemon exits at startup with `line N is not valid JSON`** — the watched file isn't line-delimited JSON (or you pointed `--watch` at the wrong file).
- **Missing recent events after a query** — the tailer polls every 100 ms and only ingests newline-terminated lines; an event flushed without a trailing newline won't appear until the writer completes the line.
- **`WAL: dropping N bytes of torn or corrupt data...` on startup** — a previous hard crash left a torn WAL tail; recovery kept everything up to the last intact record and re-reads the rest from the source file. Not an error.
- **Time range queries behave like string comparisons** — the daemon was started without `--time-field` (or with a field name that doesn't match the logs).
