# Changelog

## 3.1.1 (2026-08-18)

### Fixed

- **The release workflow no longer dead-ends when it is re-run.** `Create GitHub release` ran `gh release create` unconditionally, and that command fails with "a release with the same tag name already exists" once the release is there. The two steps that follow it — the source checksum and the Homebrew formula update — therefore became unreachable the moment anything after the release creation failed: the release is made once, everything after it is not, and re-running the job to reach those steps fails at the first one instead. Recovering meant deleting a published release, with its download links, to get back to a state the job could run from.

  The step now checks whether the release exists and refreshes its assets with `gh release upload --clobber` instead, so the job is idempotent from the release onward and a re-run resumes at whatever actually failed. This is what stranded 3.1.0's tap update at 3.0.1.

- **The Homebrew tap step says what is wrong with its token.** The step cloned the tap with the token embedded in the URL, so an expired or missing `HOMEBREW_TAP_TOKEN` surfaced as git's generic `remote: Invalid username or token. Password authentication is not supported for Git operations.` That message describes the credential git *fell back to*, not the one the workflow supplied, and it reads like the workflow transmitted a password — sending whoever is debugging it after a leak that never happened rather than after an expiry. Fine-grained tokens default to 30 days, so this is a failure the workflow should expect.

  The token is now checked before anything is cloned: empty is reported as an unset secret, and a token the GitHub API rejects for the tap repository is reported as expired, revoked, or misscoped, each naming the permission to grant. The clone itself dropped the credential — the tap is public and never needed one — which also keeps the token out of the checkout's `.git/config`, and the push supplies it per-invocation rather than storing it in a remote.

## 3.1.0 (2026-08-18)

### New tools

- **`indexBloat`** — physical statistics for one index, from whichever `pgstattuple` function fits its access method: `pgstatindex` for btree (tree level, internal/leaf/empty/deleted pages, average leaf density, leaf fragmentation), `pgstatginindex` for GIN (pending list pages and tuples), `pgstathashindex` for hash (bucket, overflow, bitmap and unused pages, live and dead items, free percent).

  The access method is resolved from the catalog rather than asked of the caller. The three functions share no name and no columns, and `pgstatindex` raises a bare "is not a btree index" when pointed at the wrong kind — so a caller made to choose would first have to look the index up, which is the work this tool exists to do. `gist`, `spgist` and `brin` have no `pgstattuple` function at all and are reported as unsupported by name, pointing at `pageinspect`; a partitioned index is reported as having no storage of its own.

  The metrics are deliberately **not** normalized across access methods, unlike `tableBloat`'s exact/approximate pair: `leaf_fragmentation` and `pending_tuples` are not two spellings of one quantity, and shared field names would invent a comparison that does not exist. `access_method` says which set came back.

  GIN's `pending_pages` is returned alongside the `fastupdate` reloption and the effective `gin_pending_list_limit` — the pending list only exists when `fastupdate` is on, and its size is meaningless without the limit it is filling toward. It is also the one variant that is free: `pgstatginindex` reads the metapage only, where the btree and hash functions read the whole index.

  `index_size` and `idx_scan` accompany every result, since a fragmented index that nothing has scanned since the last statistics reset is a candidate for dropping rather than for `REINDEX` — a call that cannot be made from density alone.

### Fixed

- **`tableBloat` now explains a permission failure instead of raising.** Every `pgstattuple` function is installed with `EXECUTE` revoked from `PUBLIC` and granted to `pg_stat_scan_tables`, so a role holding every read privilege on the data is still refused the statistics with SQLSTATE 42501. That surfaced as an unhandled exception reading like an internal fault, sending the caller after a bug rather than after the one grant that fixes it. Both `tableBloat` and `indexBloat` now return the error with the grant named.

## 3.0.1 (2026-08-12)

### Fixed

- **Extensions installed outside `public` are now found.** `tableBloat`, `statementStats` and `explainQuery`'s `queryid` path referenced `pgstattuple`, `pgstattuple_approx`, `pg_stat_statements` and `pg_stat_statements_info` unqualified, and so depended on the extension's schema happening to be on the connecting role's `search_path`. It commonly is not: `CREATE EXTENSION ... SCHEMA extensions` is ordinary practice for keeping operator tooling out of an application's schemas, and a hardened cluster keeps extensions out of `public` entirely. The tools reported "pgstattuple is not installed" for an extension that was plainly installed, and the hint told the operator to run a `CREATE EXTENSION` that would then fail as already existing — a dead end.

  Each extension's schema is now read from `pg_extension` and every object reference is qualified with it. A `search_path` could not have fixed this: the server sets no session state, because the target deployment is a transaction-mode pooler that discards it, and PgBouncer rejects a GUC passed through the `options` connection keyword outright.

  The location is resolved once per configured connection and remembered, since one connection addresses one database for the life of the process. Only a positive answer is cached — caching "not installed" would pin that verdict until a restart, so a `CREATE EXTENSION` run in response to the tool's own hint would never be picked up — and a remembered location is discarded whenever the object turns out not to be there, so `ALTER EXTENSION ... SET SCHEMA` is picked up on the next call.

  The test fixtures now install `pgstattuple` and `pg_stat_statements` into a schema that is on no `search_path`, which makes every existing `tableBloat`, `statementStats` and `explainQuery` test a regression test for this: eleven of them fail against the previous code.

## 3.0.0 (2026-08-06)

### Breaking

- **`statementStats` now returns an object, not an array.** The rows moved under `statements`, joined by an `info` block from `pg_stat_statements_info` and the configured `max`. The reason is `info.dealloc`: it counts how often the extension has evicted its least-used entries, and a non-zero value means the returned list is not the slowest queries in the cluster but the slowest of those that survived eviction. That cannot be inferred from the rows themselves, and a caller reading the old array had no way to know it was looking at a truncated picture.

- **`query_id` is now text everywhere.** `statementStats` previously returned it as a JSON number. A queryid is 64-bit, so any client parsing JSON numbers as doubles — every JavaScript-based MCP client — silently rounded it, and the rounded value would then not be found by `explainQuery`, which has always documented the field as a string for exactly this reason. `explainQuery`'s own echoed `statement.query_id` changed with it, and the new `currentActivity.query_id` follows the same rule.

### New tools

- **`wraparoundStatus`** — transaction id and multixact headroom, the one failure mode where a monitoring gap ends in a full cluster outage rather than a slowdown. Reports `age(datfrozenxid)` and `age(datminmxid)` for every database and the oldest tables by `age(relfrozenxid)`, each expressed both as a percentage of `autovacuum_freeze_max_age` (where an anti-wraparound autovacuum is forced) and of the hard limit of 2 146 483 647 (where the cluster stops accepting commands that assign a transaction id) — two thresholds that are an order of magnitude apart and are routinely confused.

  Per-table freeze storage parameters are resolved, so a table with its own `autovacuum_freeze_max_age` is measured against *its* limit, not the server's, and the output says which one applied. TOAST tables are included and labelled with the table they belong to: they carry their own `relfrozenxid`, never appear in `pg_stat_user_tables`, and are frequently the relation actually holding the horizon back.

- **`duplicateIndexes`** — indexes that duplicate, or are covered by, another index on the same table. `identical` groups indexes matching on key columns, operator classes, collations, sort order, `INCLUDE` columns and partial predicate; `redundant` reports an index whose keys are a leading prefix of a wider index that also covers its `INCLUDE` columns.

  The comparison key is a per-column signature built from the column *expression* rather than `indkey`. Comparing attribute numbers instead would call any two expression indexes identical (an expression column is attnum 0) and would treat `(a DESC)` as covered by `(a, b)`, which it cannot serve. A unique index is never reported as redundant for being a prefix — dropping it would drop a constraint the wider index does not enforce. Each entry carries size, `idx_scan`, the backing constraint name, and the replica identity and validity flags, since those decide whether the index can be dropped at all.

- **`checkpointStats`** — checkpoint, WAL and background writer activity: timed against requested checkpoints and the ratio between them, write and sync time, buffers written by the checkpointer, by the background writer, and directly by backends, plus `pg_stat_wal` volume and the settings that govern all of it.

  PostgreSQL 17 split the checkpointer counters out of `pg_stat_bgwriter` into `pg_stat_checkpointer`, renamed them, and moved the backend-written buffer counts to `pg_stat_io`; PostgreSQL 18 dropped four more columns from `pg_stat_wal`. All of that is gated on the server version and normalized to one set of field names, so a caller never branches on the major; a `source` field names the views that produced the numbers.

- **`tableIOStats`** — buffer cache hit ratio per object from `pg_statio_all_tables`: `heap_blks_read` against `heap_blks_hit`, the index pair, the TOAST and TOAST-index pairs, and a combined ratio, with relation size and scan counts. Naming a single table adds a per-index breakdown. A ratio is null rather than zero for an object that has seen no reads, so "no traffic" is not misread as "every read missed the cache".

- **`hostCapacity`** — memory and parallelism settings correlated with the machine PostgreSQL runs on. Every memory-related setting is resolved to bytes whatever unit it is counted in (8kB pages for `shared_buffers`, kB for `work_mem`, …), alongside `shared_buffers` and `effective_cache_size` as a percentage of RAM, `work_mem × max_connections`, `maintenance_work_mem × autovacuum_max_workers`, their combined total, and parallel workers per vCPU.

  Numbers only, no verdicts, matching `explainQuery`'s stance that interpretation belongs to the caller — with two factual caveats returned inline, since the worst-case figures are easy to misread: `work_mem` is a per-node rather than per-connection limit, and the OS page cache is not counted.

- **`progressStats`** — every long-running maintenance command currently reporting progress: `VACUUM`, `ANALYZE`, `CREATE INDEX`, `CLUSTER`, `COPY`, and base backups, each with its phase, blocks or tuples done against the total, a completion percentage, and elapsed time. This is the companion to `wraparoundStatus`: knowing an XID age is only half of "will the vacuum finish in time". All six categories are always present, empty when nothing is running, so an absent key never has to be read as either "idle" or "unsupported here".

  PostgreSQL 17 renamed the vacuum dead-tuple columns from counts to bytes (`max_dead_tuples` → `max_dead_tuple_bytes`, `num_dead_tuples` → `num_dead_item_ids`). Both are reported under their own names with a `dead_tuple_unit` label rather than forced into one field, because they measure genuinely different things.

- **`ioStats`** — `pg_stat_io` per backend type, object and context: reads, writes, extends, hits, evictions, reuses, fsyncs and their timings, with a hit percentage. This is where buffers written directly by backends, vacuum's ring-buffer reuse, and bulk read/write I/O become visible separately from the aggregate counters. Rows with no activity are omitted, since the view is a dense matrix of combinations most of which are structurally impossible. Requires PostgreSQL 16, and says so plainly on older servers rather than failing with an undefined-table error.

### Targeted lookups

The monitoring tools were all-or-nothing: they returned the whole view, which is fine on a test box and unusable on a server with several hundred backends. They now take optional filters, all of which preserve the unfiltered shape when omitted. There are only two correlation keys in play, `pid` and `query_id`, and threading both through turns the tool set into a path rather than a pile:

```
statementStats → query_id → currentActivity(query_id) → pid → currentLocks(pid)
                                                            → progressStats(pid)
                                                            → ioStats(pid)
                                                            → explainQuery(query_id)
```

- **`currentActivity`** takes `pid`, `query_id`, `min_duration_s` and `state`. A `pid` brings that backend's parallel workers with it, matched on `leader_pid` — being shown a leader without its workers hides where the work is happening, which is the reason for asking. `min_duration_s` excludes plain idle backends, since an idle connection's `query_start` dates a statement that already finished and would otherwise be reported as a long-running query.

- **`currentLocks`** takes `pid`, and resolves the **transitive** blocking chain through `pg_blocking_pids` rather than merely filtering rows. Each row is tagged with `chain_depth`: 0 is the backend asked about, and the largest depth is the one at the root of the pile-up. A flat lock dump left you to reconstruct that graph by hand, which is the work the tool should be doing. The recursion carries a path array, because a deadlock is a cycle in that graph and would otherwise not terminate.

- **`ioStats`** takes `pid`, `backend_type`, `object` and `context`. With a `pid` it reports one backend via `pg_stat_get_backend_io`, plus its WAL volume from `pg_stat_get_backend_wal` — a backend can be quiet in I/O and still be generating WAL heavily. That is a different source rather than a filter, since `pg_stat_io` has no pid column at all, and it requires PostgreSQL 18.

- **`statementStats`** takes `query_id`, `order_by` and `min_calls`. `order_by` is arguably a bug fix: the list was hardcoded to `total_exec_time DESC`, which buries a statement called twice at 40 seconds under one called ten million times at 2 ms, and there was no way to ask the other question. The sort column cannot be a bind parameter, so it is resolved through a fixed table and anything outside it is rejected — nothing caller-supplied reaches the SQL text. Naming a `query_id` also returns the query text whole instead of truncated.

- **`progressStats`** takes `pid` and `relation`. A relation is matched by name against `pg_class` rather than cast to `regclass`, so a name that matches nothing comes back as an empty result rather than as an error.

### Enriched existing tools

- **`currentActivity`** now returns `query_id`, the join key to `statementStats` and `explainQuery` — without it there was no route from "this statement is running now" to "this is its plan". Also added: `leader_pid` (which parallel worker belongs to which query), `backend_xid` and `backend_xmin`, and pre-computed transaction and query durations. On PostgreSQL 17+ each wait event carries its prose description from `pg_wait_events`, turning a bare `BufFileRead` into something actionable without leaving the output.

- **`replicationSlots`** now returns `wal_status` and `safe_wal_size` — the verdict that `retained_wal_bytes` only hints at, since `extended` means the slot is already past `max_wal_size` and `lost` means the WAL it needs is gone and the slot is unusable. It also joins `pg_stat_replication_slots`, a different view entirely, for the spill and stream counters: logical decoding spilling large transactions to disk is invisible in the slot's own row and is a common, silent throughput cliff. Plus `two_phase`, `conflicting` (16+), and `invalidation_reason` / `inactive_since` (17+).

- **`databaseStats`** now returns the session counters `session_time`, `active_time`, `idle_in_transaction_time`, `sessions`, `sessions_abandoned`, `sessions_fatal` and `sessions_killed`, which distinguish a database that is busy from one merely holding transactions open — something the commit and rollback counts cannot tell apart. On PostgreSQL 18, `parallel_workers_to_launch` and `parallel_workers_launched`, whose shortfall means queries planned for parallelism ran without it.

- **`statementStats`** now also returns temporary block I/O and WAL volume per statement, plus `stats_since` / `minmax_stats_since` (17+) and `wal_buffers_full` and the parallel worker counters (18+).

- **`listTables` and `tableDetails`** now return `n_tup_newpage_upd` and `last_seq_scan` on PostgreSQL 16 and newer. The first counts updates that had to move the row to another page — the direct measure of failed HOT updates, usually a too-high fillfactor or an index on a frequently updated column. The second dates the last sequential scan, which turns a large `seq_scan` count into something you can act on.

- **`wraparoundStatus`** now returns `frozen_percent` (from `pg_class.relallfrozen`) and the cumulative vacuum and autovacuum times per table on PostgreSQL 18. These turn an age into an estimate of work remaining: an old `relfrozenxid` on a table that is already almost entirely frozen is a different problem from the same age with nothing frozen, and the timings say whether autovacuum has been trying and failing or has simply never run.

- **`checkpointStats`** now returns `checkpoints_done` and `slru_written` on PostgreSQL 18.

### Configuration

- **Host capacity injection.** Total RAM and vCPU count are properties of the host, not of the cluster: no catalog holds them, and a backend could not read them portably in any case — it would read the *client's* host. Three paths supply them, and `hostCapacity` reports which one did:

  - `host_ram_mb`, `host_vcpus`, `host_storage` and `host_note` keys per section of the connections file. They are consumed by pg-licht and never reach libpq, which would reject the whole connection string over an unrecognised keyword. A non-numeric or non-positive value is rejected at startup naming the section and the key: `64GB` where megabytes are meant would be wrong by three orders of magnitude and would silently invalidate every derived ratio.
  - `PG_LICHT_HOST_RAM_MB`, `PG_LICHT_HOST_VCPUS`, `PG_LICHT_HOST_STORAGE` and `PG_LICHT_HOST_NOTE`, honoured only with the single-connection `DATABASE_URL` form, where there is no question which host they describe. With a connections file each section declares its own, since the sections may live on different machines.
  - `ram_mb` and `vcpus` arguments to `hostCapacity` itself, taking precedence over both — the path for an agent that inspects the host at run time.

  Nothing is ever inferred: with no RAM figure, every ratio that needs one is null and the result carries a hint naming the three ways to supply it.

- **`listConnections`** now echoes the configured host capacity per connection, so a caller can see which ones still need it injected.

## 2.2.0 (2026-08-04)

### Documentation

- **`pg_licht_mcp(1)` manual page.** Configuration, connection strings, all 40 operations with their arguments, MCP client setup, environment, files, diagnostics, and compatibility now live in a real man page, readable from the terminal where the server actually runs. It is hand-written mdoc (FreeBSD style), installed to `share/man/man1` by every channel — Homebrew, deb, rpm, and tarball — so `man pg_licht_mcp` works after any install. A `manpage_lint` ctest gates it with `mandoc -Tlint -Wwarning`.
- **README split by audience.** `README.md` drops from 297 to ~70 lines and now covers only purpose, safety summary, quick start, and links. Installing pre-built packages moved to `INSTALL.md`; building from source, testing, sanitizers, CI, and the release process moved to `BUILD.md`.
- **Release tarballs now carry the man page.** They are staged through `cmake --install` instead of being tarred out of the build tree, so they contain `bin/pg_licht_mcp` + `share/man/man1/pg_licht_mcp.1` and can be unpacked straight onto a prefix (`sudo tar -xzf … -C /usr/local`). This changes the archive layout from a single flat binary.
- `--help` output is now English and points at `pg_licht_mcp(1)`.

## 2.1.0 (2026-07-22)

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
- **`cpp/test/run-pooled-tests.sh`** — stands up a throwaway PostgreSQL cluster and a companion PgBouncer in transaction mode (`server_reset_query=DISCARD ALL`) in a temp directory, runs the full suite both directly and through the pooler, and tears it all down. This is the end-to-end proof that the transaction-scoped read-only guard holds under transaction pooling — the deployment a session-scoped guard silently fails on. Verified: 176/176 tests pass in both modes on PostgreSQL 14 through 18.
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
