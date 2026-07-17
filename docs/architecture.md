# Architecture

A running daemon is four cooperating pieces:

- **Tailer** — polls the watched file and turns new lines into indexed events.
- **Index** — the in-memory store all queries run against.
- **WAL + checkpointer** — persistence, so a restart doesn't re-read the whole file.
- **Socket handler** — accepts queries and control messages on a Unix socket.

## Tailer

The tailer polls the watched file every 100 ms and reads any bytes past its current offset.

- **Only complete lines are ingested.** A line without a trailing newline is assumed to be mid-write and left for the next poll, so a torn line is never indexed or half-counted. CRLF line endings are handled; byte offsets always count exactly what was consumed.
- **Startup is strict, tailing is lenient.** Before the query socket opens, the daemon ingests the file's existing contents; a malformed line there is a hard error (so a wrong `--watch` target fails loudly). Once tailing, a malformed appended line is skipped with a warning on stderr — it must not take the daemon down.
- **Rotation detection.** Each poll compares the file's inode and size against the last poll. If the inode changed (rename-based rotation) or the file shrank (truncation / copytruncate), the tailer resets its offset to 0 and ingests the new file from the beginning. See [Operations](operations.md#log-rotation) for the practical implications.

## Index

The index holds every event in insertion order (`Vec<Event>`), plus two lookup structures:

- a **field index** — `field → value → positions`, backing equality, comparison, and substring filters;
- a **time index** — a `BTreeMap` keyed by timestamp (populated only when `--time-field` matched), backing time range queries.

Query results are always returned in insertion (chronological) order. Queries take a read lock on the index; ingestion takes a write lock per event.

When `--max-events` is exceeded, eviction is amortized: the index may grow up to 1024 events past the cap before the oldest events are drained and both lookup structures are rebuilt.

## WAL

Every indexed event is appended to `wal.log` as a record containing a sequence number, the byte offset in the source file after that event, and a CRC32-protected payload. Appends go to a buffered writer; a flush task fsyncs the WAL every `--fsync-interval-ms` (default 500 ms).

The tailer appends to the WAL and pushes to the index while holding the WAL lock. The checkpointer holds the same lock while snapshotting, so a snapshot can never record a WAL position without also containing the event written at that position.

There is no cap on record size — a single log line larger than 4 MiB round-trips through the WAL fine (payloads are CRC-verified before deserialization, so a corrupt length prefix can't trigger a huge allocation).

## Snapshots and checkpoints

Every `--checkpoint-interval-secs` (default 60), and once more on a graceful `vigil --stop` (or when re-pointing the daemon at a new file), the checkpointer:

1. serializes the full in-memory index to `snapshot.tmp`,
2. atomically renames it to `snapshot.bin` and fsyncs the data directory (so a crash can't surface an old snapshot next to an already-truncated WAL),
3. truncates the WAL.

The snapshot records the next WAL sequence number and the byte offset into the source file, so recovery knows exactly where to resume.

## Recovery

On startup (unless `--no-persist`), the daemon:

1. loads `snapshot.bin` if present and rebuilds the index from it;
2. replays `wal.log`, applying only records with sequence numbers not already covered by the snapshot (a crash between snapshot write and WAL truncation leaves overlap, which is filtered out);
3. resumes tailing the source file from the recovered byte offset — the pre-socket initial read picks up anything written while the daemon was down.

**Corrupt or torn WAL tails are self-healing.** If replay hits a record whose CRC doesn't match, whose length prefix exceeds the remaining file, or that is simply cut off, the intact prefix is kept, the bad tail is truncated off the file (with a warning), and the daemon starts normally. Appends after such a recovery land after the intact prefix and survive the next replay.

## Durability guarantees and their limits

- **Graceful stop loses nothing.** `vigil --stop` writes a final snapshot before exiting.
- **Hard crash:** the WAL is fsync'd every `--fsync-interval-ms`, so up to that window of WAL records can be lost. Because the recovered byte offset comes from the last durable record, the tailer simply re-reads the lost window from the source file — events are only truly lost if the source file was also rotated or truncated in that window.
- **Corrupt WAL tail:** records after the last intact record are dropped (and, as above, re-read from the source file when it still contains them).
- **`--no-persist` guarantees nothing across restarts** — the daemon re-reads the file from the beginning on every start.

## Socket handler

The daemon listens on `/tmp/vigil.sock` (restricted to `0600` — owner only). Each client connection is handled in its own task, so a slow or stalled client can't block the accept loop or shutdown. Requests are capped at 1 MiB, and the query parser caps parenthesis nesting at 128 levels; both protect the daemon from memory exhaustion and stack overflow. Invalid or malformed requests get an error response rather than affecting the daemon.
