#!/usr/bin/env bash
#
# Run the pg-licht test suite twice against a throwaway PostgreSQL cluster:
# once connected directly, once through a companion PgBouncer in transaction
# mode. This is the end-to-end check that the per-transaction read-only guard
# and the connect-execute-release flow behave correctly behind a
# transaction-pooling proxy (pool_mode=transaction, server_reset_query=DISCARD
# ALL) -- the deployment that a session-scoped guard silently fails on.
#
# Nothing outside a temporary directory is touched: a private cluster is
# created with initdb, a private PgBouncer is started alongside it, and both
# are torn down on exit.
#
# Requirements: initdb/pg_ctl/createdb (PostgreSQL 14+), pgbouncer, and a built
# test binary (cmake --build cpp/build).
#
# Overridable via environment:
#   PG_BINDIR   directory holding initdb/pg_ctl/createdb (default: autodetect)
#   PGBOUNCER   pgbouncer binary                          (default: autodetect)
#   TEST_BIN    the gtest binary                          (default: cpp/build/pg_licht_mcp_test)
#   PG_PORT     port for the temp cluster                 (default: 55432)
#   BOUNCER_PORT port for the companion pooler            (default: 56432)
#   STANDBY_PORT port for the streaming standby           (default: 57432)
#
# A physical standby is streamed off the primary with pg_basebackup and its
# conninfo is exported as STANDBY_URL. The role and topology tests use it to
# check the replica side of pg_is_in_recovery() and the shared system
# identifier; they skip when STANDBY_URL is unset, so a plain `ctest` run is
# unaffected.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cpp_dir="$(dirname "$here")"

# --- locate binaries -------------------------------------------------------
find_pg_bindir() {
  [ -n "${PG_BINDIR:-}" ] && { echo "$PG_BINDIR"; return; }
  # Prefer the newest installed major version. Sort by the version initdb
  # reports, not by path: layouts differ (/usr/lib/postgresql/18/bin vs
  # /usr/pgsql-16/bin), so a path sort would pick the wrong one. The server
  # supports PostgreSQL 14+; newest is preferred so a plain dev run exercises
  # the latest, and CI pins each supported major via PG_BINDIR.
  local d v best="" bestv=0
  for d in /usr/lib/postgresql/*/bin /usr/pgsql-*/bin \
           /opt/homebrew/opt/postgresql*/bin; do
    [ -x "$d/initdb" ] || continue
    v="$("$d/initdb" --version 2>/dev/null | grep -oE '[0-9]+' | head -1)"
    [ -n "$v" ] && [ "$v" -gt "$bestv" ] && { bestv="$v"; best="$d"; }
  done
  [ -n "$best" ] && { echo "$best"; return; }
  command -v initdb >/dev/null 2>&1 && { dirname "$(command -v initdb)"; return; }
  echo "ERROR: could not find initdb; set PG_BINDIR" >&2; exit 1
}
PG_BINDIR="$(find_pg_bindir)"
PGBOUNCER="${PGBOUNCER:-$(command -v pgbouncer || echo /usr/sbin/pgbouncer)}"
TEST_BIN="${TEST_BIN:-$cpp_dir/build/pg_licht_mcp_test}"
PG_PORT="${PG_PORT:-55432}"
BOUNCER_PORT="${BOUNCER_PORT:-56432}"
STANDBY_PORT="${STANDBY_PORT:-57432}"

for req in "$PG_BINDIR/initdb" "$PG_BINDIR/pg_ctl" "$PG_BINDIR/createdb" \
           "$PG_BINDIR/pg_basebackup" "$PGBOUNCER" "$TEST_BIN"; do
  [ -x "$req" ] || { echo "ERROR: missing or not executable: $req" >&2
                     [ "$req" = "$TEST_BIN" ] && echo "  build it: cmake --build $cpp_dir/build" >&2
                     exit 1; }
done

# --- scratch + teardown ----------------------------------------------------
# A short socket directory: PostgreSQL rejects a Unix socket path over 107 bytes.
work="$(mktemp -d "${TMPDIR:-/tmp}/pglicht-rig.XXXXXX")"
PGDATA="$work/pg"
SBDATA="$work/standby"
BDIR="$work/bouncer"
mkdir -p "$PGDATA" "$BDIR"

cleanup() {
  [ -f "$BDIR/pgbouncer.pid" ] && kill "$(cat "$BDIR/pgbouncer.pid")" 2>/dev/null || true
  "$PG_BINDIR/pg_ctl" -D "$SBDATA" -m immediate stop >/dev/null 2>&1 || true
  "$PG_BINDIR/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

# --- throwaway postgres ----------------------------------------------------
echo "--- initdb (superuser: pglicht, trust auth) @ PostgreSQL $("$PG_BINDIR/initdb" --version | grep -oE '[0-9]+' | head -1)"
"$PG_BINDIR/initdb" -D "$PGDATA" -U pglicht --auth=trust -E UTF8 >/dev/null

cat >> "$PGDATA/postgresql.conf" <<CONF
port = $PG_PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$work'
shared_preload_libraries = 'pg_stat_statements'
fsync = off
full_page_writes = off
CONF

echo "--- start postgres on $PG_PORT (pg_stat_statements preloaded)"
"$PG_BINDIR/pg_ctl" -D "$PGDATA" -l "$PGDATA/pg.log" -w start >/dev/null
"$PG_BINDIR/createdb" -h 127.0.0.1 -p "$PG_PORT" -U pglicht pglicht

# --- streaming standby -----------------------------------------------------
# Taken after createdb so the test database is already in the base backup. The
# standby inherits postgresql.conf from the primary, so the port and socket
# directory are overridden by appending: the last assignment wins.
#
# This exists for the role and topology tests. A standby shares its primary's
# system identifier -- that is what makes the identifier a replication lineage
# rather than a postmaster -- so it is the only way to check that the two are
# told apart by host and port rather than by identity.
echo "--- pg_basebackup a standby on $STANDBY_PORT"
"$PG_BINDIR/pg_basebackup" -h 127.0.0.1 -p "$PG_PORT" -U pglicht \
    -D "$SBDATA" -R -X stream >/dev/null

cat >> "$SBDATA/postgresql.conf" <<CONF
port = $STANDBY_PORT
unix_socket_directories = '$work'
CONF

"$PG_BINDIR/pg_ctl" -D "$SBDATA" -l "$SBDATA/pg.log" -w start >/dev/null
STANDBY_URL="host=127.0.0.1 port=$STANDBY_PORT dbname=pglicht user=pglicht"
export STANDBY_URL

# --- companion pooler (transaction mode) -----------------------------------
# auth_type=any + a forced user: this is a local throwaway, so the point is not
# to test auth but to reproduce transaction pooling + DISCARD ALL faithfully.
cat > "$BDIR/pgbouncer.ini" <<INI
[databases]
* = host=127.0.0.1 port=$PG_PORT user=pglicht

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = $BOUNCER_PORT
unix_socket_dir = $work
auth_type = any
pool_mode = transaction
server_reset_query = DISCARD ALL
max_client_conn = 200
default_pool_size = 25
logfile = $BDIR/pgbouncer.log
pidfile = $BDIR/pgbouncer.pid
INI

echo "--- start pgbouncer on $BOUNCER_PORT (pool_mode=transaction, DISCARD ALL)"
"$PGBOUNCER" -d "$BDIR/pgbouncer.ini"
for _ in $(seq 1 20); do
  grep -q "listening on 127.0.0.1:$BOUNCER_PORT" "$BDIR/pgbouncer.log" 2>/dev/null && break
  sleep 0.2
done

# --- run the suite both ways -----------------------------------------------
rc=0
DATABASE_URL="host=127.0.0.1 port=$PG_PORT dbname=pglicht user=pglicht" "$TEST_BIN" >/tmp/pgl.direct.$$ 2>&1 || rc=$?
echo "================ DIRECT (port $PG_PORT) ================"; tail -4 /tmp/pgl.direct.$$; rm -f /tmp/pgl.direct.$$
[ "$rc" -eq 0 ] || { echo "DIRECT run failed"; exit 1; }

DATABASE_URL="host=127.0.0.1 port=$BOUNCER_PORT dbname=pglicht user=pglicht" "$TEST_BIN" >/tmp/pgl.pooled.$$ 2>&1 || rc=$?
echo; echo "================ POOLED (port $BOUNCER_PORT) ================"; tail -4 /tmp/pgl.pooled.$$; rm -f /tmp/pgl.pooled.$$
[ "$rc" -eq 0 ] || { echo "POOLED run failed"; exit 1; }

echo
echo "OK: suite passed both directly and through the transaction-mode pooler"
echo "    (standby on $STANDBY_PORT covered the replica-side role tests)."
