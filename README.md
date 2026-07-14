# pg-licht

A PostgreSQL MCP (Model Context Protocol) server that exposes schema exploration tools over JSON-RPC 2.0.

Motivation to create another PostgreSQL MCP:

- Only queries the catalog to inspect the structure (lower risk to leak data)
- Allow to describe database model and routines based on what is in the database
- Fast inspection of available indexes and relationships when analyzing a query plan

Every query is parameterized (`$1`/`$2` bindings or `::regnamespace`/`::regclass` casts) — no schema, table, or search-term argument is ever concatenated into SQL text. On connect, the session sets `default_transaction_read_only = on`, so even a bug that let a query attempt a write would still fail rather than succeed silently.

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

## Install

### Homebrew (macOS and Linux)

```bash
brew tap sqlambda/pg-licht
brew install pg-licht
```

### Pre-built binaries

Download the latest binary for your platform from the [releases page](https://github.com/sqlambda/pg_licht/releases):

| Platform | File |
|----------|------|
| Linux x86_64 | `pg_licht_mcp-linux-x86_64.tar.gz` |
| Linux arm64 | `pg_licht_mcp-linux-arm64.tar.gz` |
| macOS Apple Silicon | `pg_licht_mcp-macos-arm64.tar.gz` |

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
```

## Test

```bash
DATABASE_URL="port=5432 dbname=mydb" ./cpp/build/pg_licht_mcp_test
# or
DATABASE_URL="port=5432 dbname=mydb" ctest --test-dir cpp/build --output-on-failure
```

Tests create and destroy a temporary database named `pg_licht_test_<PID>` automatically.

## Debugging with MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) lets you call tools interactively and inspect responses without an AI client.

```bash
npx @modelcontextprotocol/inspector \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  ./cpp/build/pg_licht_mcp
```

## MCP Configuration

### Claude Code (`claude mcp add`)

The `--scope` flag controls where the configuration is saved:

| Scope | Flag | Config file | Use when |
|-------|------|-------------|----------|
| Project (default) | `--scope project` | `.claude.json` in project root | shared with the repo |
| User | `--scope user` | `~/.claude.json` | available across all projects |
| Local | `--scope local` | `.claude.json.local` (git-ignored) | personal overrides, not committed |

```bash
claude mcp add --scope project pg-licht \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  -- /path/to/pg-licht/cpp/build/pg_licht_mcp
```

Replace `--scope project` with `--scope user` or `--scope local` as needed.

### Manual — `.claude.json`

You can also edit `.claude.json` (project root), `~/.claude.json` (user), or `.claude.json.local` (local) directly:

```json
{
  "mcpServers": {
    "pg-licht": {
      "command": "/path/to/pg-licht/cpp/build/pg_licht_mcp",
      "env": {
        "DATABASE_URL": "postgresql://user:pass@host/dbname"
      }
    }
  }
}
```

### Claude Desktop

Add the same JSON block above under `mcpServers` in `claude_desktop_config.json`.

## License

Apache 2.0 — see [LICENSE](LICENSE).
