# pg-licht

A PostgreSQL MCP (Model Context Protocol) server that exposes schema exploration tools over JSON-RPC 2.0.

Motivation to create another PostgreSQL MCP:

- Only queries the catalog to inspect the structure (lower risk to leak data)
- Allow to describe database model and routines based on what is in the database
- Fast inspection of available indexes and relationships when analyzing a query plan

Every query is parameterized (`$1`/`$2` bindings or `::regnamespace`/`::regclass` casts) — no schema, table, or search-term argument is ever concatenated into SQL text. The one deliberate exception is `explainQuery`, which has to place the statement under analysis into the `EXPLAIN` text because `EXPLAIN` cannot take a bind parameter; it is guarded by a leading-keyword whitelist, a single-statement check, `PREPARE`d parameter binding with every value escaped through libpq, a `ModifyTable` gate that refuses to execute anything that writes, and a bounded `statement_timeout`. Every tool call runs inside its own `READ ONLY` transaction, so even a bug that let a query attempt a write would still fail (SQLSTATE `25006`) rather than succeed silently.

The read-only guard is deliberately *transaction*-scoped rather than session-scoped. Under a connection pooler in transaction mode (PgBouncer's `pool_mode = transaction` with `server_reset_query = DISCARD ALL`), the server connection is returned to the pool at commit and its session state is wiped — so a `SET` issued once at startup silently stops applying to later calls. Transaction scope is the scope a pooler preserves. The setting cannot be pushed into the connection string either: PgBouncer rejects GUCs in the startup packet outright (`unsupported startup parameter in options: ...`).

## Tools

| Tool | Description |
|------|-------------|
| `listSchemas` | All schemas with their table/view names and role grants |
| `listTables` | Tables and views in a schema with kind, row counts, sizes, scan stats, autovacuum stats (dead/live tuples, mods-since-analyze, inserts-since-vacuum, last vacuum/analyze, per-table reloptions), columns (name, description, and per-column index count), and index/constraint counts. Full index and constraint definitions live in `tableDetails`, not here — this tool is intentionally light for browsing a schema |
| `tableDetails` | Full detail: columns (types, nullability, defaults, pg_stats histograms, per-column TOAST storage strategy and compression method), TOAST table (name, size, index size — `null` if the table has no toastable columns), primary key (single or composite, as ordered column list), indexes (size, usage counts), constraints, foreign keys, inbound foreign keys (`referenced_by`), triggers (timing, events, when condition, language), rules (`CREATE RULE`, excludes the implicit view `_RETURN` rule), row-level security status and policies, view definition, scan stats, and role grants |
| `searchTables` | Full-text search across table/schema names, descriptions, column names/descriptions, enum values used by columns, and role names; returns the same fields as `listTables` plus `roles` |
| `listFunctions` | Functions and procedures in a schema with kind, language, return type, arguments, volatility, security_definer, and is_strict |
| `functionDetails` | Full function detail: source code, full definition, arguments, volatility, trigger usage, and role grants |
| `searchFunctions` | Full-text search across function names, source code, language, trigger names, and descriptions |
| `listEnums` | Enum types in a schema with their ordered values and descriptions |
| `enumDetails` | Full enum detail: ordered values and all columns (across all tables) that reference it |
| `searchEnums` | Full-text search across enum names, values, and descriptions |
| `listTypes` | Composite types, domains, and range types in a schema (excludes enums, implicit table/view row types, and the auto-generated multirange companion of each range type); composites include their attribute list, domains include base type/nullability/default/constraints, ranges include subtype and the multirange type name |
| `typeDetails` | Full composite type, domain, or range type detail: attributes/constraints/subtype and which columns use it |
| `listRoles` | Cluster-wide roles with kind (login/group), attributes (superuser, create_role, create_db, replication, bypass_rls, connection_limit, valid_until), and group memberships |
| `listForeignTables` | Foreign tables in a schema with their foreign server, FDW, options, and columns (never exposes user mapping credentials) |
| `listForeignServers` | Cluster-wide foreign servers with FDW, owner, and options (host/port/dbname-style only, never user mapping credentials) |
| `listTablespaces` | Cluster-wide tablespaces with owner, filesystem location, options, and description |
| `listCollations` | Collations usable in the current database's encoding for a schema, with provider, locale settings, and determinism flag |
| `listEventTriggers` | Cluster-wide event triggers with event type, tags, function, owner, enabled status, and description |
| `listPublications` | Logical replication publications with owner, all-tables flag, per-operation flags, and member tables |
| `listSubscriptions` | Logical replication subscriptions for the current database with owner, enabled status, publications, slot name, and sync settings (never exposes the connection string) |
| `listLanguages` | Procedural languages installed in the current database (e.g. plpgsql, plpython3u) with owner, trusted/procedural flags, handler function, and description |
| `listExtendedStatistics` | Extended statistics objects (`CREATE STATISTICS`) for a schema with target table, columns, statistics kinds (ndistinct, dependencies, mcv), and description |
| `listOperators` | Custom operators in a schema with left/right operand types, result type, and implementing function; mostly relevant for schemas using extensions with custom types (e.g. PostGIS) |
| `listOperatorClasses` | Operator classes in a schema with their index access method, input type, and default flag; describes what index types (btree/gist/gin/etc) a type supports |
| `listAccessMethods` | Index and table access methods available in the cluster (btree, gist, gin, heap, etc) with type and handler function |
| `listCasts` | Type casts involving at least one user-defined type (excludes built-in-to-built-in casts) with source/target types, context, and method |
| `listTextSearchConfigs` | Full-text search configurations for a schema with parser and the token-type-to-dictionary mapping |
| `listSequences` | Sequences in a schema with data type, range, increment, cycle, cache size, current value, and owning table.column (for `SERIAL`/`IDENTITY` columns) |
| `listExtensions` | Installed PostgreSQL extensions with version, schema, relocatable flag, and description |
| `databaseSize` | Current database name and total disk size |
| `serverSettings` | All PostgreSQL server settings (`pg_settings`) grouped by category; each setting includes current value, unit, description, context, type, source, and `pending_restart` flag |
| `currentActivity` | Current server connections and running queries (`pg_stat_activity`) across all databases: pid, database, user, application_name, backend_type, state, wait event, and query text |
| `currentLocks` | Current locks (`pg_locks`) joined with the holding backend's query and user, plus which pids are blocking each waiting lock; use to diagnose lock contention |
| `replicationSlots` | Replication slots with retained WAL bytes; a lagging or unused slot holds back WAL indefinitely and is a common cause of disk bloat incidents |
| `databaseStats` | Per-database statistics (`pg_stat_database`) for every database in the cluster: connections, commits/rollbacks, block hit ratio inputs, tuple counts, conflicts, deadlocks, temp file usage, and checksum failures |
| `statementStats` | The slowest tracked queries by total execution time (`pg_stat_statements`), with calls, timing, row counts, and buffer usage; returns a clear error with setup instructions if the extension is not installed |
| `tableBloat` | Physical storage bloat for a table (`pgstattuple`/`pgstattuple_approx`): live/dead tuple counts and percentages, free space and percentage. Defaults to the cheap visibility-map-based approximation; `exact: true` runs a precise but I/O-heavy full table scan. More accurate than the ANALYZE-time estimates in `listTables`/`tableDetails`. Returns a clear error with setup instructions if the extension is not installed |
| `checkKey` | Check if a row exists by primary key (single or composite); validates value types against PK column types before querying |
| `explainQuery` | Raw `EXPLAIN (FORMAT JSON)` plan for a statement, recovered from `pg_stat_statements` by `queryid` (full untruncated text) or supplied directly as `sql`. Runs in a read-only transaction bounded by `statement_timeout`. Statements with `$n` placeholders are planned with `GENERIC_PLAN`; supply `params` to have the statement `PREPARE`d and planned with real values. `analyze: true` runs `EXPLAIN (ANALYZE, BUFFERS)` — but only after the plan is proven free of any `ModifyTable` node, so data-modifying statements (including data-modifying CTEs) are never executed, and it requires an explicit `timeout_ms`. Returns the plan verbatim plus `generic`/`analyzed`/`read_only` flags and the `pg_stat_statements` row; no heuristics and no generated DDL — the plan is yours to interpret |
| `listConnections` | The configured database connections by name, with the libpq `service` name or host/port/dbname/user for each, and which is the default. Passwords are never returned and a service file is never expanded |

Every tool also accepts an optional `connection` argument naming one of the configured connections (see [Configuration](#configuration)); omit it to use the default.

## Install

### Homebrew (macOS and Linux)

```bash
brew tap sqlambda/pg-licht
brew install pg-licht
```

### Pre-built packages and binaries

Download the latest release for your platform from the [releases page](https://github.com/sqlambda/pg_licht/releases):

| Platform | File | Format |
|----------|------|--------|
| Debian 13 (Trixie) x86_64 | `pg_licht_mcp-linux-x86_64-debian13.deb` | `.deb` |
| Debian 13 (Trixie) arm64 | `pg_licht_mcp-linux-arm64-debian13.deb` | `.deb` |
| Rocky Linux 9 x86_64 | `pg_licht_mcp-linux-x86_64-rocky9.rpm` | `.rpm` |
| Rocky Linux 9 arm64 | `pg_licht_mcp-linux-arm64-rocky9.rpm` | `.rpm` |
| Linux x86_64 (generic) | `pg_licht_mcp-linux-x86_64.tar.gz` | tarball |
| Linux arm64 (generic) | `pg_licht_mcp-linux-arm64.tar.gz` | tarball |
| macOS Apple Silicon | `pg_licht_mcp-macos-arm64.tar.gz` | tarball |

**Debian / Ubuntu / other apt-based distros (`.deb`):**

```bash
sudo apt install ./pg_licht_mcp-linux-x86_64-debian13.deb
pg_licht_mcp "postgresql://user:pass@host/dbname"
```

`apt install ./file.deb` (not `dpkg -i`) so apt resolves the declared `libpq5` dependency automatically. Installs to `/usr/bin/pg_licht_mcp`, already on `PATH`.

**Rocky Linux / RHEL / Fedora / other dnf-based distros (`.rpm`):**

```bash
sudo dnf install ./pg_licht_mcp-linux-x86_64-rocky9.rpm
pg_licht_mcp "postgresql://user:pass@host/dbname"
```

Same reasoning: `dnf install ./file.rpm` resolves the declared `libpq5` dependency; `rpm -i` won't. Installs to `/usr/bin/pg_licht_mcp`.

**Generic tarball (any glibc-based Linux, or macOS):**

```bash
tar -xzf pg_licht_mcp-<platform>.tar.gz
./pg_licht_mcp "postgresql://user:pass@host/dbname"
```

### Build from source

Requirements: CMake 3.20+, a C++23 compiler, libpqxx, libpq, nlohmann_json, pkg-config

Release binaries link libpqxx statically (built from a pinned version) so every platform runs the same libpqxx regardless of what each distro packages; libpq stays a dynamic runtime dependency since its C ABI is stable and distros patch it independently. Local builds use whatever libpqxx your system provides — dynamic or static both work.

```bash
cmake -S cpp -B cpp/build -DBUILD_TESTING=OFF
cmake --build cpp/build
```

## Run

```bash
DATABASE_URL="postgresql://user:pass@host/dbname" ./pg_licht_mcp

# libpq key-value format also works
DATABASE_URL="host=localhost port=5432 dbname=mydb" ./pg_licht_mcp

# or as an argument
./pg_licht_mcp "postgresql://user:pass@host/dbname"

# or several named databases from a config file
./pg_licht_mcp --config ~/.config/pg_licht/connections.ini
```

## Configuration

A single database needs nothing beyond `DATABASE_URL`. To reach several databases from one
server, list them in an INI file:

```ini
[default]
port    = 6432
dbname  = pglicht
user    = daniel
sslmode = prefer
; no password here — use ~/.pgpass or a service file

[prod]
; a libpq service name, resolved from ~/.pg_service.conf,
; $PGSERVICEFILE, or $PGSYSCONFDIR/pg_service.conf
service = podb01_ro

[staging]
service = podb01_ro        ; the service supplies the defaults…
dbname  = podb01_staging   ; …and explicit keys override them
```

Pass a section name as the `connection` argument of any tool to run it against that
database; omit it to use `[default]` (or, if there is no `[default]`, the first section).
`listConnections` reports what is configured.

Keys are passed through to libpq, so anything libpq accepts works — including `service`,
which may be used on its own in place of `host`/`port`/`dbname`/`user`. Each section must
set either `service` or at least `dbname`. `options` is rejected, because PgBouncer refuses
GUCs in the startup packet; pg-licht sets what it needs per transaction instead.

The file is resolved in this order: `--config <path>`, `$PG_LICHT_CONFIG`,
`~/.config/pg_licht/connections.ini` (only if it exists), then `DATABASE_URL`, then
`argv[1]`.

Because sections may carry passwords, the file must not be group- or world-accessible —
pg-licht refuses to load it otherwise, the same rule libpq applies to `~/.pgpass`:

```bash
chmod 600 ~/.config/pg_licht/connections.ini
```

Note that libpq enforces that rule on `~/.pgpass` but *not* on `pg_service.conf`, so if a
service file holds a password, its permissions are yours to manage.

## Test

```bash
DATABASE_URL="port=5432 dbname=mydb" ./cpp/build/pg_licht_mcp_test
# or
DATABASE_URL="port=5432 dbname=mydb" ctest --test-dir cpp/build --output-on-failure
```

Tests create and destroy a temporary database named `pg_licht_test_<PID>` automatically.

### Through a connection pooler

Because the read-only guard is transaction-scoped specifically so it survives a
transaction-mode pooler, there is a script that verifies exactly that. It stands up a
throwaway PostgreSQL cluster and a companion PgBouncer (`pool_mode=transaction`,
`server_reset_query=DISCARD ALL`) in a temp directory, runs the full suite both directly
and through the pooler, and tears everything down — touching nothing else on the machine:

```bash
cmake --build cpp/build          # the script needs the test binary built
cpp/test/run-pooled-tests.sh
```

Requires `initdb`/`pg_ctl` (PostgreSQL 16+) and `pgbouncer` on the machine; it picks the
newest installed PostgreSQL and free defaults, all overridable via environment
(`PG_BINDIR`, `PGBOUNCER`, `PG_PORT`, `BOUNCER_PORT`).

## Debugging with MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) lets you call tools interactively and inspect responses without an AI client.

```bash
npx @modelcontextprotocol/inspector \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  ./cpp/build/pg_licht_mcp
```

## MCP Configuration

### Claude Code (`claude mcp add`)

The `--scope`/`-s` flag controls where the configuration is saved:

| Scope | Flag | Stored in | Use when |
|-------|------|-----------|----------|
| Local (default) | `--scope local` | `~/.claude.json`, under this project's entry | personal, current project only, not shared |
| Project | `--scope project` | `.mcp.json` in project root | shared with the team via version control (each teammate approves it once) |
| User | `--scope user` | `~/.claude.json` | available across all your projects |

```bash
claude mcp add --transport stdio pg-licht \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  -- /path/to/pg-licht/cpp/build/pg_licht_mcp
```

Add `--scope project` or `--scope user` to change where it's saved; omit for the local-only default.

### Manual — `.mcp.json` / `~/.claude.json`

Project-scoped servers live in `.mcp.json` at the project root (commit this to share with your team):

```json
{
  "mcpServers": {
    "pg-licht": {
      "command": "/path/to/pg-licht/cpp/build/pg_licht_mcp",
      "args": [],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@host/dbname"
      }
    }
  }
}
```

Local- and user-scoped servers live in `~/.claude.json` instead (edit via `claude mcp add`/`claude mcp add-json` rather than by hand, since that file also holds per-project state).

### Claude Desktop

Add the same JSON block above under `mcpServers` in `claude_desktop_config.json`.

### Grok Build (`grok mcp add`)

Grok Build reads `.mcp.json` and `~/.claude.json` for compatibility, so an MCP server already configured for Claude Code (above) is picked up automatically — no extra steps needed.

To configure it directly instead:

```bash
grok mcp add pg-licht -- /path/to/pg-licht/cpp/build/pg_licht_mcp
```

Environment variables for stdio servers are set in the config file rather than on the command line. `--scope project` writes to `.grok/config.toml` in the project (add it to your repo to share); omit it for the user-level `~/.grok/config.toml`:

```toml
[mcp_servers.pg-licht]
command = "/path/to/pg-licht/cpp/build/pg_licht_mcp"
env = { DATABASE_URL = "postgresql://user:pass@host/dbname" }
```

Verify with `grok mcp list` or `grok mcp doctor pg-licht`.

## License

Apache 2.0 — see [LICENSE](LICENSE).
