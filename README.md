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

40 read-only operations, grouped as schema exploration, catalog search, cluster-wide
objects, extensibility and text search, foreign data and replication, monitoring and
statistics, diagnostics and query planning, and connections. Highlights include
`tableDetails` (columns, indexes, constraints, foreign keys in both directions, triggers,
policies), `searchTables` (full-text search across names, descriptions, and enum values),
and `explainQuery` (recover a slow statement from `pg_stat_statements` by `queryid` and get
its `EXPLAIN` plan).

Each one is documented, with its arguments, in `man pg_licht_mcp` under OPERATIONS. Every
operation also accepts an optional `connection` argument to select one of several
configured databases.

## Documentation

| | |
|---|---|
| `man pg_licht_mcp` | configuration, connection strings, all 40 operations, MCP client setup |
| [INSTALL.md](INSTALL.md) | Homebrew, deb, rpm, tarball, verifying, uninstalling |
| [BUILD.md](BUILD.md) | building from source, tests, sanitizers, CI, release process |
| [CHANGES.md](CHANGES.md) | changelog |

Before installing, the manual page can be read straight from the source tree:

```bash
man --local-file cpp/man/pg_licht_mcp.1
```

## PostgreSQL version support

PostgreSQL 14 and newer, verified in CI on 14–18. See the COMPATIBILITY section of
`man pg_licht_mcp` for the two fields that are version-gated.

## License

Apache 2.0 — see [LICENSE](LICENSE).
