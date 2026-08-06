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

## Tools

47 read-only operations, grouped as schema exploration, catalog search, cluster-wide
objects, extensibility and text search, foreign data and replication, monitoring and
statistics, diagnostics and query planning, and connections. Highlights include
`tableDetails` (columns, indexes, constraints, foreign keys in both directions, triggers,
policies), `searchTables` (full-text search across names, descriptions, and enum values),
and `explainQuery` (recover a slow statement from `pg_stat_statements` by `queryid` and get
its `EXPLAIN` plan).

For operational triage there are `wraparoundStatus` (XID and multixact headroom, per
database and per table, TOAST tables included), `checkpointStats` (timed against requested
checkpoints, backend-written buffers, WAL volume — normalized across the PostgreSQL 17
`pg_stat_checkpointer` split), `progressStats` (every running VACUUM, CREATE INDEX, COPY…
with a completion percentage), `ioStats` (`pg_stat_io` per backend type and context, 16+),
`tableIOStats` (cache hit ratio per table and per index), `duplicateIndexes` (identical and
prefix-redundant indexes, compared by column expression, opclass, collation and sort order),
and `hostCapacity` (memory settings against the host's actual RAM and vCPU count — see
below).

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
configured databases.

## Host capacity

Total RAM and vCPU count live outside the catalog, so `hostCapacity` cannot read them and
will not guess. Inject them per connection in the connections file:

```ini
[prod]
service      = db01_ro
host_ram_mb  = 65536
host_vcpus   = 16
```

or, with a single `DATABASE_URL`, via `PG_LICHT_HOST_RAM_MB` and `PG_LICHT_HOST_VCPUS`. An
agent that inspects the host at run time can instead pass `ram_mb` and `vcpus` straight to
the tool, which takes precedence over both.

## Documentation

| | |
|---|---|
| `man pg_licht_mcp` | configuration, connection strings, all 47 operations, MCP client setup |
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
