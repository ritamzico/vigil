# Query language

A query is a filter expression, optionally followed by a pipeline stage using `|`. The stage is either an aggregation or a result limit — one or the other, not both.

```
<filter>
<filter> | <aggregation>
<filter> | limit <N>
<filter> | tail <N>
```

Without an aggregation, results are raw log lines in chronological (ingestion) order.

Queries are sent to the daemon as-is over the Unix socket. Two hard limits apply: a request may be at most 1 MiB, and parentheses may nest at most 128 levels deep. Queries beyond either limit are rejected with an error.

## Filters

### Field comparison

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
| `~` | contains substring (string fields only) |

Values are automatically typed: `500` is a number, `true`/`false` are booleans, anything else is a string. The `~` search term is always treated as a string, and it only matches string fields.

```
status = 500
level = ERROR
latency_ms > 1000
active != true
message ~ timeout
```

`~` matches a literal substring only — the search term can't contain spaces (queries are tokenized on whitespace), and there is no regex support. A regex operator (`=~`, backed by the `regex` crate) is a possible future extension.

### Boolean logic

Use `AND`, `OR`, and `NOT` to combine filters, and parentheses to group. The keywords are case-sensitive (uppercase). `NOT` binds tighter than `AND`, which binds tighter than `OR`.

```
status = 500 AND level = ERROR
status = 500 OR status = 503
level = ERROR AND method = POST OR level = WARN AND status >= 400
NOT status = 500
status = 500 AND NOT level = INFO
(level = ERROR OR level = WARN) AND status >= 500
```

`NOT` matches every event the inner filter does not — including events that lack the field entirely.

### Time range

Requires `--time-field <field>` when starting the daemon. The field's value must be an RFC 3339 timestamp (e.g. `2026-01-15T00:00:00Z`).

```
<time-field> > <timestamp>    # events at or after this time
<time-field> < <timestamp>    # events at or before this time
```

Only `<` and `>` are valid on the time field, and both bounds are inclusive: an event stamped exactly `<timestamp>` matches either form.

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

### Match all

Start with `|` to skip filtering and run a pipeline stage over all events:

```
| count
| tail 10
```

## Aggregations

Append `| <aggregation>` to a filter to compute a result instead of returning raw events. Aggregation keywords are case-insensitive.

| Aggregation | Output |
|-------------|--------|
| `count` | Number of matching events |
| `count by <field>` | Number of matching events that have `<field>` |
| `avg <field>` | Average of `<field>` across matching events |
| `sum <field>` | Sum of `<field>` across matching events |
| `min <field>` | Minimum of `<field>` across matching events |
| `max <field>` | Maximum of `<field>` across matching events |
| `p<N> <field>` | Nth percentile of `<field>` (N is an integer, 1–100) |

```
level = ERROR | count
level = ERROR | count by request_id
status >= 500 | avg latency_ms
status >= 500 | sum latency_ms
status >= 500 | min latency_ms
status >= 500 | max latency_ms
status >= 500 | p99 latency_ms
status >= 500 | p50 latency_ms
| count
```

## Result limiting

Append `| limit N` or `| tail N` to a filter to cap how many raw events are returned. Results are in chronological (ingestion) order: `limit N` keeps the first N matches, `tail N` keeps the last N matches (still oldest-first). A limit occupies the same pipeline slot as an aggregation, so a query can have one or the other, not both. Like aggregation keywords, `limit` and `tail` are case-insensitive.

```
level = ERROR | limit 20        # first 20 matching events
level = ERROR | tail 5          # last 5 matching events
| tail 10                       # last 10 events overall
```
