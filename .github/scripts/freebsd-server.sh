#!/bin/sh
#
# The one part of pg_licht that depends on the SERVER's platform, run against
# a PostgreSQL on FreeBSD: statementKernelStats reads pg_stat_kcache, whose
# counters come from getrusage(), and FreeBSD's kernel does not fill them the
# way Linux's does (user and system time are split by sampling, I/O bytes are
# not attributed). The tool says which fields the platform measures; this
# checks that against a real FreeBSD server instead of a faked version().
#
# Starts a throwaway cluster with pg_stat_statements and pg_stat_kcache
# preloaded, runs the kernel-counter tests against it, and removes it.
# FreeBSD packages neither hypopg nor pg_wait_sampling for PostgreSQL 18, so
# everything else is tested against the Linux rig.
#
# Usage: freebsd-server.sh <test binary>      (as root, or as the cluster owner)
# Needs: postgresql18-server postgresql18-contrib pg_stat_kcache
set -eu
bin=$1
port=${FBSD_PG_PORT:-45999}
dir=$(mktemp -d /var/tmp/pglicht-fbsd.XXXXXX)

# The postmaster refuses to run as root, which is what a CI VM runs as.
if [ "$(id -u)" = 0 ]; then
  chown postgres "$dir"
  as() { su -m postgres -c "$1"; }
else
  as() { sh -c "$1"; }
fi

stop() {
  as "pg_ctl -D $dir/data -m immediate -w stop" >/dev/null 2>&1 || true
  rm -rf "$dir"
}
trap stop EXIT

as "initdb -D $dir/data -U pglicht -A trust" >/dev/null
as "cat >> $dir/data/postgresql.conf" <<CONF
port = $port
listen_addresses = '127.0.0.1'
unix_socket_directories = ''
shared_preload_libraries = 'pg_stat_statements,pg_stat_kcache'
compute_query_id = on
CONF
as "pg_ctl -D $dir/data -l $dir/log -w start" >/dev/null || { cat "$dir/log"; exit 1; }

url="host=127.0.0.1 port=$port dbname=postgres user=pglicht"
psql -X -qtA "$url" -c "SELECT version()"

# Required, so that a pg_stat_kcache that is not usable fails here instead of
# letting both tests return early and pass.
out=$(DATABASE_URL="$url" PGLICHT_REQUIRE_PRELOAD_EXTENSIONS=1 "$bin" \
        --gtest_filter='PreloadExtTest.KernelCounters*:PreloadExtTest.StatementKernelStats*' 2>&1) || {
  echo "$out"; exit 1; }
echo "$out"
# And both must have run: a filter that matches nothing passes too.
echo "$out" | grep -q '^\[  PASSED  \] 2 tests\.$'
