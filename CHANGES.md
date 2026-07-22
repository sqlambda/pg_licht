# Changelog

## 2.1.0 (unreleased)

### Security

- **The read-only guard is now per transaction, not per session.** Previously the server ran `SET default_transaction_read_only = on` once at startup and committed. Behind a connection pooler in transaction mode — PgBouncer's `pool_mode = transaction` with `server_reset_query = DISCARD ALL` — the server connection is returned to the pool at commit and its session state is wiped, so that setting silently stopped applying to every subsequent tool call. Each tool call now opens its own `READ ONLY` transaction, which is the scope a transaction pooler preserves. The setting cannot be moved into the connection string instead: PgBouncer rejects GUCs in the startup packet (`unsupported startup parameter in options: ...`).

### New tools

- **`explainQuery`** — returns the raw `EXPLAIN (FORMAT JSON)` plan for a statement, closing the loop left open by `statementStats`: you could see *that* a query was slow but never *why*. Takes either a `queryid` (the full untruncated text is re-fetched from `pg_stat_statements` server-side) or `sql` directly, so a proposed rewrite can be explained and compared against the original.

  Statements recovered from `pg_stat_statements` are normalized, with literals replaced by `$1`/`$2`. Plain `EXPLAIN` rejects that text outright, so they are planned with `EXPLAIN (GENERIC_PLAN)` (PostgreSQL 16+). Supplying `params` instead has the statement `PREPARE`d and planned with real values, which also enables `ANALYZE`.

  Nothing that modifies data is ever executed. Four layers, in order: utility statements are rejected before any SQL is sent (`pg_stat_statements` tracks `CREATE DATABASE`, `SET`, `VACUUM`, … which have no plan at all); multi-statement text is rejected; the plan is inspected for a `ModifyTable` node anywhere in the tree, which catches data-modifying CTEs such as `WITH d AS (DELETE … RETURNING *) SELECT * FROM d` where the node is nested under a CTE subplan; and the enclosing `READ ONLY` transaction aborts anything a plan cannot reveal, such as a `VOLATILE` function that writes at runtime. `analyze: true` additionally requires an explicit `timeout_ms` (clamped to 100–30000) — these are by construction the slowest statements in the cluster, so the caller has to state a bound rather than inherit one.

  Parameter values reach `EXECUTE` as SQL text, since `EXECUTE` arguments cannot be bind parameters. Every non-null value is emitted as a quoted literal of unknown type through a single escaping path and coerced by PostgreSQL to the inferred parameter type, so there is one place to get escaping right rather than one per JSON type.

  The output is the plan verbatim plus `generic`/`analyzed`/`read_only` flags, the timeout used, the statement text actually explained, and the `pg_stat_statements` row. No heuristics and no generated DDL — interpretation is left to the caller.

- **`listConnections`** — returns the configured connections by name, with the libpq `service` name or host/port/dbname/user for each, and which is the default. Passwords are never returned and a service file is never expanded.

### Enhancements

- **Multiple databases from one server.** Connections are defined as named sections in an INI file (`--config`, `$PG_LICHT_CONFIG`, or `~/.config/pg_licht/connections.ini`). Every tool accepts an optional `connection` argument selecting one; omitting it uses the default. `DATABASE_URL` and `argv[1]` keep working unchanged and become the single `default` connection.
- **libpq `service` support** — a section may use `service = name` in place of `host`/`port`/`dbname`/`user`, resolved from `~/.pg_service.conf`, `$PGSERVICEFILE`, or `$PGSYSCONFDIR/pg_service.conf`, and may combine it with explicit keys. Each section must set either `service` or at least `dbname`, so a misconfigured section is named at startup rather than failing obscurely at connect time.
- **Connect-execute-release.** Connections are opened per tool call and closed afterwards rather than held open, which suits a pooler and leaves no idle backends or lingering session state.
- Config files must not be group- or world-accessible (the `~/.pgpass` rule), since sections may carry credentials. `options` is rejected with an explanation, as PgBouncer refuses it.

### Compatibility

- **Supports PostgreSQL 14 and newer** (previously effectively 16+), verified in CI on 14 through 18. The blocker was pervasive: ~40 catalog queries used aliasless `LEFT JOIN LATERAL (...) ON true`, which only PostgreSQL 16+ accepts — every such subquery now carries an alias, valid on all versions. Three version-gated catalog fields are handled too: `explainQuery`'s `GENERIC_PLAN` path (PG16+) returns an actionable hint on 14/15 instead of failing (supplying `params` works everywhere); index `last_use` (`last_idx_scan`, PG16+) and subscription `two_phase` (`subtwophasestate`, PG15+) are omitted where the column doesn't exist. Server version is read from the connection handshake (no extra round trip) and gates per connection, so mixed-version multi-database setups are correct.

### Testing

- The CI matrix (sanitizers, valgrind, pooled) now spans PostgreSQL 14, 15, 16, 17, and 18.
- **`cpp/test/run-pooled-tests.sh`** — stands up a throwaway PostgreSQL cluster and a companion PgBouncer in transaction mode (`server_reset_query=DISCARD ALL`) in a temp directory, runs the full suite both directly and through the pooler, and tears it all down. This is the end-to-end proof that the transaction-scoped read-only guard holds under transaction pooling — the deployment a session-scoped guard silently fails on. Verified: 176/176 tests pass in both modes on PostgreSQL 18.
- The main test fixture now drops its scratch database with `WITH (FORCE)`, so teardown succeeds even when a transaction pooler is still holding idle server connections to it.

## 1.3.0 (unreleased)

### New tools

- **`checkKey`** — checks whether a row exists by primary key. Accepts `schema`, `table`, and `values` (an ordered array matching the PK columns). Validates each value's type against the actual PK column type (integer, float, string, UUID, boolean) before issuing the query, returning `{ "exists": true/false }`.

### Enhancements

- **`tableDetails`** now includes a `primary_key` field: an ordered array of the PK column names (e.g. `["id"]` or `["tenant_id", "order_id"]`). Absent when the table has no primary key.
- **`tableDetails`** FK column ordering fix — `source_columns` and `target_columns` inside both `foreign_keys` and `referenced_by` are now ordered by their position in the constraint definition (via `array_position`) rather than by system attribute number, so composite FK column lists are always in the correct order.

## 1.2.0 (2026-04-30)

### Enhancements

- **`tableDetails`** now returns `referenced_by` — an object of all foreign keys from *other* tables that point at the described table (inbound references). Each entry includes `source_table`, `source_columns`, `target_columns`, and `definition`. Absent when no other table references this one.
- **`tableDetails`** column entries now include a `default` field with the SQL expression for the column's default value (e.g. `now()`, `nextval('orders_id_seq'::regclass)`, `'pending'::order_status`). Absent when the column has no default.

## 1.1.0 (2026-04-27)

### New tools

- **`databaseSize`** — returns the current database name and its total disk size via `pg_database_size(current_database())`. Takes no parameters; cannot query other databases.

### Enhancements

- **`searchTables`** now also searches column names and column descriptions, making it possible to find tables by the columns they contain (e.g. searching `username` returns every table with that column).
- **`tableDetails`** now reports accurate disk sizes:
  - `size` uses `pg_table_size()` (table data only, excluding indexes).
  - `indexes_size` (new field) uses `pg_indexes_size()` for the total size of all indexes on the table.
  - Each entry in `indexes` now includes a `size` field via `pg_relation_size()` for that individual index.

## 1.0.0

Initial release with the following tools:

- **`listSchemas`** — lists non-system schemas with their tables and ACL roles.
- **`listTables`** — lists tables, views, and materialized views in a schema with basic statistics.
- **`tableDetails`** — full table introspection: columns with stats and histograms, indexes, constraints, foreign keys, triggers, and ACL roles.
- **`searchTables`** — full-text search across table names, descriptions, role names, and enum types used by the table's columns.
- **`listFunctions`** — lists functions and procedures in a schema.
- **`functionDetails`** — function details including source, trigger usage, and ACL roles.
- **`searchFunctions`** — full-text search across function names, source, language, trigger names, and descriptions.
- **`listEnums`** — lists enum types in a schema with their ordered values and descriptions.
- **`enumDetails`** — returns full details for a single enum type including every column that references it.
- **`searchEnums`** — full-text search across enum names, values, and descriptions.
