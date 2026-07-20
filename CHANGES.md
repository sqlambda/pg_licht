# Changelog

## 2.1.0 (unreleased)

### Security

- **The read-only guard is now per transaction, not per session.** Previously the server ran `SET default_transaction_read_only = on` once at startup and committed. Behind a connection pooler in transaction mode — PgBouncer's `pool_mode = transaction` with `server_reset_query = DISCARD ALL` — the server connection is returned to the pool at commit and its session state is wiped, so that setting silently stopped applying to every subsequent tool call. Each tool call now opens its own `READ ONLY` transaction, which is the scope a transaction pooler preserves. The setting cannot be moved into the connection string instead: PgBouncer rejects GUCs in the startup packet (`unsupported startup parameter in options: ...`).

### New tools

- **`listConnections`** — returns the configured connections by name, with the libpq `service` name or host/port/dbname/user for each, and which is the default. Passwords are never returned and a service file is never expanded.

### Enhancements

- **Multiple databases from one server.** Connections are defined as named sections in an INI file (`--config`, `$PG_LICHT_CONFIG`, or `~/.config/pg_licht/connections.ini`). Every tool accepts an optional `connection` argument selecting one; omitting it uses the default. `DATABASE_URL` and `argv[1]` keep working unchanged and become the single `default` connection.
- **libpq `service` support** — a section may use `service = name` in place of `host`/`port`/`dbname`/`user`, resolved from `~/.pg_service.conf`, `$PGSERVICEFILE`, or `$PGSYSCONFDIR/pg_service.conf`, and may combine it with explicit keys. Each section must set either `service` or at least `dbname`, so a misconfigured section is named at startup rather than failing obscurely at connect time.
- **Connect-execute-release.** Connections are opened per tool call and closed afterwards rather than held open, which suits a pooler and leaves no idle backends or lingering session state.
- Config files must not be group- or world-accessible (the `~/.pgpass` rule), since sections may carry credentials. `options` is rejected with an explanation, as PgBouncer refuses it.

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
