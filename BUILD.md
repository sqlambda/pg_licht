# Building pg-licht from source

For pre-built packages, see [INSTALL.md](INSTALL.md). This document covers compiling,
testing, and the CI setup — it is aimed at contributors.

## Requirements

- CMake 3.20+
- A C++23 compiler
- libpqxx
- libpq
- nlohmann_json
- pkg-config
- GoogleTest (only when building the test suite, which is the default)

## Build

```bash
cmake -S cpp -B cpp/build -DBUILD_TESTING=OFF
cmake --build cpp/build
```

Release binaries link libpqxx statically (built from a pinned version) so every platform
runs the same libpqxx regardless of what each distro packages; libpq stays a dynamic
runtime dependency, since its C ABI is stable and distros patch it independently. Local
builds use whatever libpqxx your system provides — dynamic or static both work.

### Build options

| Option | Default | Purpose |
|--------|---------|---------|
| `BUILD_TESTING` | `ON` | Build `pg_licht_mcp_test` and register the ctest suite. Requires GoogleTest. |
| `CMAKE_BUILD_TYPE` | *(unset)* | `Release` for packaging, `RelWithDebInfo` for debugging and sanitizers. |
| `PGLICHT_SANITIZER` | `NONE` | `ADDRESS`, `UNDEFINED`, `ADDRESS_UNDEFINED`, or `THREAD`. |
| `CMAKE_INSTALL_PREFIX` | `/usr/local` | Install destination. |

Hardening flags are always on, not opt-in: `-Wall -Wextra -Wpedantic -Wconversion
-Wsign-conversion -Wshadow -Werror`, plus `_GLIBCXX_ASSERTIONS` / `_LIBCPP_HARDENING_MODE`.
Expect to add explicit `static_cast`s for any `size_t` / `int` / `pqxx::result::size_type`
conversion.

### Installing a local build

```bash
cmake --install cpp/build --prefix ~/.local
```

This installs `bin/pg_licht_mcp` and `share/man/man1/pg_licht_mcp.1`, which is what puts
`pg_licht_mcp(1)` on your `MANPATH`.

## Test

```bash
DATABASE_URL="port=5432 dbname=mydb" ./cpp/build/pg_licht_mcp_test
# or
DATABASE_URL="port=5432 dbname=mydb" ctest --test-dir cpp/build --output-on-failure
```

The fixtures create and destroy a temporary database named `pg_licht_test_<PID>`
automatically, so `DATABASE_URL` only needs to name a database they can connect to in
order to issue `CREATE DATABASE`. Installing the `pg_stat_statements`, `postgres_fdw`, and
`pgstattuple` extensions requires superuser.

`ctest` also runs, when the tools are present:

- `*_valgrind` — registered automatically by `pglicht_add_valgrind_test()` whenever
  Valgrind is installed and no sanitizer is active.
- `manpage_lint` — `mandoc -Tlint -Wwarning` over `cpp/man/pg_licht_mcp.1`.

### Through a connection pooler

Because the read-only guard is transaction-scoped specifically so it survives a
transaction-mode pooler, there is a script that verifies exactly that. It stands up a
whole rig in a temp directory, runs the full suite both directly and through the pooler,
and tears everything down — touching nothing else on the machine:

| | why it exists |
|---|---|
| primary, `pg_stat_statements` preloaded, `wal_level = logical` | the suite's own database, and the publisher |
| a `pg_basebackup` streaming standby | shares the primary's system identifier, which is the only way to check that the topology tools tell a replica from a second instance rather than trusting the config |
| a separately `initdb`-ed logical subscriber | `subscriptionStats` has nothing to report on a server that subscribes to nothing. It has to be a third cluster: logical replication between two databases of one cluster deadlocks, because `CREATE SUBSCRIPTION` waits for the slot it is creating and slot creation waits for every transaction older than itself — including that one |
| PgBouncer, `pool_mode=transaction`, `server_reset_query=DISCARD ALL` | the deployment a session-scoped guard fails on silently |

Tests that need a server other than the primary are skipped when it is absent, so the
binary still runs against a plain `DATABASE_URL`; the rig exports `STANDBY_URL` and
`SUBSCRIBER_URL` to switch them on.

```bash
cmake --build cpp/build          # the script needs the test binary built
cpp/test/run-pooled-tests.sh
```

Requires `initdb`, `pg_ctl`, `createdb`, `pg_basebackup` and `psql` (PostgreSQL 14+) plus
`pgbouncer`. It picks the newest installed PostgreSQL and free default ports, all
overridable via the environment: `PG_BINDIR`, `PGBOUNCER`, `TEST_BIN`, `PG_PORT`,
`BOUNCER_PORT`, `STANDBY_PORT`, `SUBSCRIBER_PORT`. To pin a specific major:

```bash
PG_BINDIR=/usr/lib/postgresql/16/bin cpp/test/run-pooled-tests.sh
```

This is also the only path that covers the `pg_stat_statements` queryid tests, because the
rig preloads the extension while a stock server usually does not.

### Debugging with MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) lets you call tools
interactively and inspect responses without an AI client:

```bash
npx @modelcontextprotocol/inspector \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  ./cpp/build/pg_licht_mcp
```

## PostgreSQL version support

Supports PostgreSQL 14 and newer, verified in CI on 14–18. One feature degrades gracefully
below 16: `explainQuery` can only produce a plan for a `pg_stat_statements` statement
(whose text is normalized to `$1`/`$2`) by using `EXPLAIN (GENERIC_PLAN)`, which is
PostgreSQL 16+. On 14/15 that path returns an actionable hint instead — supplying `params`
(which prepares and plans the statement with real values) works on every supported version.

Two catalog fields are omitted where the column does not exist: index `last_use`
(`pg_stat_user_indexes.last_idx_scan`, PG16+) and subscription `two_phase`
(`pg_subscription.subtwophasestate`, PG15+). Both are gated on the server version reported
by the connection handshake, so a mixed-version multi-database setup behaves correctly.

## Editing the man page

`cpp/man/pg_licht_mcp.1` is a hand-written mdoc source, deliberately **not** generated: the
release containers (`debian:trixie`, `rockylinux:9`) carry no doc toolchain, and the
release build runs `--target pg_licht_mcp` rather than `all`, so a generator target would
never run and CPack would then fail on a missing install file.

```bash
mandoc -Tlint -Wwarning cpp/man/pg_licht_mcp.1   # what CI gates on
groff -mdoc -Tascii -ww cpp/man/pg_licht_mcp.1 >/dev/null   # second renderer
man --local-file cpp/man/pg_licht_mcp.1          # preview
```

Adding a tool to `get_tools_list()` in `cpp/src/server.h` means adding a matching
`.It Ic <name>` entry under the right `.Ss` group. This check keeps the two in sync:

```bash
diff <(grep -oE '\{"name", "[A-Za-z]+"' cpp/src/server.h | sed 's/.*"name", "//; s/"//' | sort) \
     <(grep -oE '^\.It Ic [A-Za-z]+' cpp/man/pg_licht_mcp.1 | awk '{print $3}' | sort -u)
```

Keep the tools table wording in the man page authoritative; `README.md` deliberately does
not duplicate it.

## CI

- `.github/workflows/sanitizers.yml` — ASan/UBSan, TSan, Valgrind, and the pooled-connection
  job, each across PostgreSQL 14, 15, 16, 17, and 18.
- `.github/workflows/release.yml` — 7 build targets (Linux x86_64/arm64, Debian 13 deb,
  Rocky 9 rpm, macOS arm64). Tarballs are staged through `cmake --install`; deb and rpm are
  produced by CPack, which picks up the same install rules. libpqxx is pinned via
  `PQXX_VERSION`.

## Release process

Push a `v*` tag on `main`. The release workflow builds and uploads all seven artifacts,
creates the GitHub release, and bumps the Homebrew tap formula (URL and sha256)
automatically.
