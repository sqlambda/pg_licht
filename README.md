# pg-licht

A PostgreSQL MCP (Model Context Protocol) server that exposes schema exploration tools over JSON-RPC 2.0.

Motivation to create another PostgreSQL MCP:

- Only queries the catalog to inspect the structure (lower risk to leak data)
- Allow to describe database model and routines based on what is in the database
- Fast inspection of available indexes and relationships when analyzing a query plan

## Safety

Every catalog query is parameterized — no schema, table, or search-term argument is ever
concatenated into SQL text. Every tool call runs inside its own `READ ONLY` transaction,
so even a bug that let a query attempt a write would fail rather than succeed silently.
That guard is transaction-scoped rather than session-scoped, which is what makes it hold
behind a connection pooler in transaction mode.

The same transaction sets `statement_timeout`, so no call can occupy a backend
indefinitely. It defaults to two minutes and is per connection:

```ini
[billing_prod]
service              = billing-prod
statement_timeout_ms = 600000      ; 0 removes the ceiling
```

Most operations are catalog reads that never approach it. The ones that can are
those whose cost scales with the server rather than with the query —
`tableBloat` with `exact: true`, `indexBloat` on a btree or hash index,
`bufferCacheContents`, and `listTableSizes` on a wide schema — so raise it for a
connection where such a scan is the point of the call. Reaching it is reported as the ceiling being reached, naming
the value and the key to change, rather than as an error.

Full rationale, including the guarded exception for `explainQuery`, is in the
SECURITY CONSIDERATIONS section of `man pg_licht_mcp`.

## Quick start

```bash
brew tap sqlambda/pg-licht && brew install pg-licht

claude mcp add --transport stdio pg-licht \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  -- /usr/local/bin/pg_licht_mcp

man pg_licht_mcp
```

Other install channels — deb, rpm, tarball, source — are in [INSTALL.md](INSTALL.md).

## Remote databases

A tool call opens no connection of its own beyond the first: connections are
held between calls and closed after 60 seconds idle, one per configured
connection and at most 32 at a time. Startup opens none at all — the registry is
validated without touching the network, so one unreachable database no longer
prevents the server from starting.

The ceiling matters only above 32 configured databases, where the least recently
used connection is dropped to make room and the next call to that database
reconnects. It exists so a single wide sweep cannot hold one socket per member
for a minute and exhaust a file-descriptor limit — macOS defaults to 256.

Fan-out sweeps visit up to 16 members concurrently, one connection per server.
A thirteen-member replication group that took 449 ms sequentially takes 91 ms.
The payload stays in configuration order regardless of which member answers
first.

For databases across a WAN, a connection pooler in front of pg_licht is still
worth running — it amortises the TLS handshake across restarts, and
`server_idle_timeout` gives you the same idle policy at the pooler. Point every
`connections.ini` entry at the local pooler and let its `[databases]` section
carry the real hosts. Note that PgBouncer reads neither `.pgpass` nor
`service=`, so that section has to be generated rather than shared, and in
transaction mode it does not run `server_reset_query` at all by default.

## Output format

A client that negotiates MCP revision `2025-06-18` or later receives
`structuredContent`; every client receives the text block as well.

Both are sent by default, because a client can advertise a revision it does not
fully implement and this server cannot tell. Set `PG_LICHT_STRUCTURED_ONLY=1`
to send only the structured payload once you have confirmed your client reads
it, or `PG_LICHT_MAX_PROTOCOL=2025-03-26` to refuse to negotiate anything
newer.

Every tool declares an `outputSchema` to clients that can use it. The schemas
describe but never reject: payloads here are version-conditional, and any tool
may answer `{error, hint}` when an extension is absent.

Results that the protocol marks cacheable carry `ttlMs` and `cacheScope`.
`tools/list` is about 93 kB and entirely static, so it is hinted at one hour
and scoped `private` — it varies by negotiated revision and names the
configured default connection, so it must not be served to another caller from
a shared cache. Catalog-derived results are hinted at one minute.

List operations accept an opaque `cursor` and return `nextCursor`. The page
size is larger than anything this server lists today, so a client that ignores
cursors still sees every tool; `resources/list` is the one that grows, being
connections × schemas.

## Resources and prompts

Structure is also served as **resources** — documents a client can browse and
pin into context without a tool call:

```
pglicht://{conn}/schemas
pglicht://{conn}/schema/{schema}
pglicht://{conn}/schema/{schema}/table/{table}
pglicht://{conn}/schema/{schema}/{functions,enums,types}
pglicht://{conn}/server/{roles,extensions,settings}
```

Readings stay tools, because the model has to decide *when* to take them.
`resources/list` enumerates connections and schemas and reaches tables through
a template, so a 10 000-table database does not produce a 10 000-entry
response.

Ten **prompts** encode an order of investigation that is easy to get wrong:
`diagnose-slow-query`, `triage-lock-contention`, `diagnose-deadlock`,
`bloat-and-vacuum-review`, `buffer-cache-review`, `capacity-check`,
`replication-slot-review`, `plan-schema-change`, `check-role-access` and
`explain-and-fix`. They are static text and touch no
database until the model acts on them, and each one whose tools are
privilege-gated opens by calling `checkPrivileges`.

`check-role-access` answers whether a role can use a table, view, function or
procedure — walking the gates in order, since the one people forget is `USAGE`
on the schema and the resulting error names the table. It treats row-level
security as a second gate that opens after the grants already said yes: RLS
enabled with no applicable permissive policy is deny-all, so every grant checks
out and the table returns nothing.

`diagnose-slow-query` is the hub — a slow statement can end in a plan fix, a
vacuum change, an index, or a schema change, and it routes to the others
accordingly. `plan-schema-change` answers "how should I apply this DDL
here?" by measuring the table rather than prescribing a method — it carries no
DDL recipes, because the same statement that is instant on an empty table is an
outage at a billion rows, and only the row count, measured size, existing
indexes and current lock waits can tell the two apart. **Completions** are offered for the `conn`,
`schema` and `table` variables.

## Tools

59 read-only operations, grouped as schema exploration, catalog search, cluster-wide
objects, extensibility and text search, foreign data and replication, monitoring and
statistics, diagnostics and query planning, topology, and connections. Highlights include
`tableDetails` (columns, indexes, constraints, foreign keys in both directions, triggers,
policies), `searchTables` (full-text search across names, descriptions, and enum values),
and `explainQuery` (recover a slow statement from `pg_stat_statements` by `queryid` and get
its `EXPLAIN` plan).

`checkPrivileges` reports which of them the current role can actually use on a given
connection. Most work for any role that can connect, since the catalog is world-readable:
measured on PostgreSQL 18, a bare login role runs 48 of 59 at full fidelity, the monitoring
role 55, and the ones that remain are those that read row data. Worth calling first
against an unfamiliar connection — a privilege-filtered answer is easy to mistake for an
empty one, since `tableStats` on a role without `SELECT` returns columns with null
statistics, exactly like a table that was never analyzed.

`evaluateIndex` plans a statement as if the indexes were different, using
[hypopg](https://github.com/HypoPG/hypopg): `create` tests a candidate index without
building it, `hide` plans without an existing one — which is how to ask whether an index is
safe to drop. Nothing is built, nothing is locked, and the statement is never executed. It
reports whether the planner actually *used* each index, which is the answer a cost figure
hides.

Tables are covered by three tools rather than one, split by what the answer costs and how
fast it goes stale:

| | tools | reads |
|---|---|---|
| **structure** — changes only on DDL | `tableDetails`, `listTables`, `searchTables` | catalog |
| **statistics** — changes continuously, free to ask | `tableStats`, `listTableStats` | catalog and the statistics collector |
| **measured size** — changes continuously, costs a lock | `tableSize`, `listTableSizes` | `stat()` per segment, under `AccessShareLock` |

The structure tools return no sample column values and no counters, so the payload an agent
reads most often to understand a schema carries neither row data nor anything that moves
under it. `tableStats` carries the `pg_stats` histograms — including `most_common_vals`,
which is literal values sampled from the column — and a free `size_estimate` from
`relpages`, dated by `estimated_from` so its staleness is visible. Reach for `tableSize`
only when that estimate is too stale to act on: it opens each relation with
`AccessShareLock`, so during a rewrite it waits behind the `ALTER TABLE`.

For operational triage there are `wraparoundStatus` (XID and multixact headroom, per
database and per table, TOAST tables included), `checkpointStats` (timed against requested
checkpoints, backend-written buffers, WAL volume — normalized across the PostgreSQL 17
`pg_stat_checkpointer` split), `progressStats` (every running VACUUM, CREATE INDEX, COPY…
with a completion percentage), `ioStats` (`pg_stat_io` per backend type and context, 16+),
`tableIOStats` (cache hit ratio per table and per index), `duplicateIndexes` (identical and
prefix-redundant indexes, compared by column expression, opclass, collation and sort order),
`indexBloat` (per-index physical statistics, dispatched on the access method — btree leaf
density and fragmentation, GIN pending list against the `fastupdate` settings that bound it,
hash bucket and overflow pages), and `hostCapacity` (memory settings against the host's
actual RAM and vCPU count — see below).

`bufferCacheSummary` and `bufferCacheContents` read `pg_buffercache`. The summary needs
extension version 1.4, which ships with PostgreSQL 16; on 14 and 15 it says so and points
at `bufferCacheContents`, which reads the view and works everywhere. `tableIOStats` counts
only what `shared_buffers` served, so a miss there may still have come from the OS page
cache at RAM speed; these are the only in-core view of that split. Read the usage-count
histogram rather than a hit ratio: mass at 2–5 is a stable working set, everything at 0–1
with no unused buffers is clock-sweep churn, and those are the same ratio with opposite
diagnoses. `bufferCacheContents` aggregates per relation and fork, never raw per-buffer
rows.

`listTopology` and `verifyTopology` cover the connection topology — see below.

The monitoring tools take optional `pid` and `query_id` filters, so a symptom can be
followed to its cause rather than read out of a full dump:

```
statementStats → query_id → currentActivity(query_id) → pid → currentLocks(pid)
                                                            → progressStats(pid)
                                                            → ioStats(pid)
                                                            → explainQuery(query_id)
```

`currentLocks(pid)` resolves the transitive blocking chain and tags each row with
`chain_depth`, so the backend at the root of a pile-up is the one with the highest depth.

Each one is documented, with its arguments, in `man pg_licht_mcp` under OPERATIONS. Every
operation also accepts an optional `connection` argument to select one of several
configured databases, and most accept `instance`, `replication_group` or `group` to run
across several at once — see below.

## Topology

With more than one database configured, three labels say how they relate. They are
deliberately separate, because only the first two license a conclusion:

```ini
[billing_prod]
service           = billing-prod
instance          = pg-prod-01      # one postmaster
replication_group = billing-ha      # a primary and its replicas
group             = prod, billing   # an operator label

[billing_ro]
service           = billing-replica
instance          = pg-prod-02
replication_group = billing-ha
group             = prod, billing, reporting
```

- **`instance`** — one postmaster. Its databases share `shared_buffers`, WAL, autovacuum
  workers, `max_connections` and disk, so one database's checkpoint storm really is
  another's latency. PostgreSQL's own glossary calls this a *database cluster*.
- **`replication_group`** — a primary and its replicas: the same data on different
  servers, each keeping its own statistics counters.
- **`group`** — an arbitrary label. Implies nothing, which is the point: it is how an
  agent asks about "dev" without knowing any connection name.

There is no `cluster` key. PostgreSQL uses that word for the first sense and RDS/Aurora
for the second, so it means opposite things to the two people most likely to read the
file. There is no `role` key either: primary or replica is observed on every connection
via `pg_is_in_recovery()` and never cached, because failover swaps it and failover is
exactly when this server gets used.

An `instance` is inferred when two connections share a host **and** port, and reported as
`inferred` rather than `declared` — behind a pooler one endpoint can front several
instances, so it groups output without licensing a contention claim. `verifyTopology`
settles it, by connecting to each server and reading its system identifier: same
identifier on the same endpoint is one instance, the same identifier on different hosts is
a replication lineage, and a disagreement means the config is wrong.

Passing `instance`, `replication_group` or `group` instead of `connection` runs the tool
once per member and returns one result per member, in config file order. A member that
cannot be reached is reported in place rather than failing the sweep. Which of the three a
tool accepts depends on where its answer actually varies, and its input schema says which:

| | varies across the databases of one instance | varies across members of a replication group |
|---|---|---|
| catalogs, `tableBloat`, the structure and size tools | yes | no — a physical replica is byte-identical |
| `duplicateIndexes`, `indexBloat`, `tableIOStats`, `tableStats`, `listTableStats`, `subscriptionStats` | yes | **yes** — they carry `idx_scan`, or a worker of their own |
| `currentActivity`, `currentLocks`, `statementStats`, buffer cache | no — instance-wide | yes |

That middle row is the one worth knowing: an index that reads as unused on the primary may
be carrying a replica's entire reporting workload, and only that replica's `idx_scan`
shows it. `role: "primary"` or `role: "replica"` narrows a sweep to one side.

Sweeps are sequential — the server is a single-threaded loop — so width costs wall clock.
Call `listTopology` first; it reads the config file and opens no connection.

A worked example covering all of this is in
[cpp/test/connections.example.ini](cpp/test/connections.example.ini).

## Host capacity

Total RAM and vCPU count live outside the catalog, so `hostCapacity` cannot read them and
will not guess. Inject them per connection in the connections file:

```ini
[prod]
service      = db01_ro
host_ram_mb  = 65536
host_vcpus   = 16
```

With several databases on one postmaster, declare it once for the instance instead and the
members inherit it; an explicit per-connection value still wins:

```ini
[instance:pg-prod-01]
host_ram_mb  = 65536
host_vcpus   = 16
```

Inheritance follows `instance` only, never `replication_group`: replicas routinely run on
smaller machines, and inheriting the primary's RAM would give every replica a confidently
wrong `shared_buffers` ratio.

Or, with a single `DATABASE_URL`, via `PG_LICHT_HOST_RAM_MB` and `PG_LICHT_HOST_VCPUS`. An
agent that inspects the host at run time can instead pass `ram_mb` and `vcpus` straight to
the tool, which takes precedence over both.

## Documentation

| | |
|---|---|
| `man pg_licht_mcp` | configuration, connection strings, all 59 operations, MCP client setup |
| [INSTALL.md](INSTALL.md) | Homebrew, deb, rpm, tarball, verifying, uninstalling |
| [BUILD.md](BUILD.md) | building from source, tests, sanitizers, CI, release process |
| [CHANGES.md](CHANGES.md) | changelog |

Before installing, the manual page can be read straight from the source tree:

```bash
man --local-file cpp/man/pg_licht_mcp.1
```

## PostgreSQL version support

PostgreSQL 14 and newer, verified in CI on 14–18. Every operation returns its full result
on every supported version, apart from `ioStats`, which needs the PostgreSQL 16
`pg_stat_io` view and says so on older servers. Individual fields are omitted where the
underlying column does not exist, and views that changed shape upstream —
`pg_stat_checkpointer` in 17, the `pg_stat_wal` and `pg_stat_progress_vacuum` columns in
17 and 18 — are normalized to one set of field names. The COMPATIBILITY section of
`man pg_licht_mcp` lists every gated field by version.

## License

Apache 2.0 — see [LICENSE](LICENSE).
