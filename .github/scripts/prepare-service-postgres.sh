#!/usr/bin/env bash
#
# Give a stock postgres:NN service container the extensions the suite needs
# beyond contrib, preloaded, so the sanitizer and valgrind jobs exercise the
# code behind them instead of skipping it.
#
# A service container cannot be given a command of its own, so the libraries
# are installed into the running container, preloaded with ALTER SYSTEM, and
# the container restarted. The official images are Debian with the PGDG apt
# repository already configured, and the extension packages are in its `main`
# component.
#
# Usage: prepare-service-postgres.sh <container id>
set -euo pipefail

c=$1
pg=$(docker exec "$c" bash -c 'echo "$PG_MAJOR"')

# Bounded, unlike a bare until-loop: a preload naming a library that is not
# there stops the postmaster at startup, and an unbounded wait turns that into
# a job hung until the runner's own timeout instead of a failure with a log.
wait_ready() {
  for _ in $(seq 1 60); do
    docker exec "$c" pg_isready -U postgres >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "postgres in $c did not become ready; its log:" >&2
  docker logs --tail 30 "$c" >&2
  return 1
}

wait_ready
docker exec "$c" bash -c "apt-get update -qq && DEBIAN_FRONTEND=noninteractive \
  apt-get install -y -qq --no-install-recommends \
    postgresql-$pg-hypopg postgresql-$pg-pg-wait-sampling \
    postgresql-$pg-pg-stat-kcache postgresql-$pg-pg-qualstats >/dev/null"

# The list is unquoted on purpose. ALTER SYSTEM SET x = 'a,b' sets a list
# setting to ONE element named "a,b", and the postmaster then refuses to start
# looking for a library by that name -- verified against postgres:17. As bare
# items it is the list it looks like. pg_stat_kcache must follow
# pg_stat_statements.
docker exec "$c" psql -U postgres -v ON_ERROR_STOP=1 -qc \
  "ALTER SYSTEM SET shared_preload_libraries = pg_stat_statements, pg_stat_kcache, pg_wait_sampling, pg_qualstats"

docker restart "$c" >/dev/null
wait_ready

# Only now, with the library loaded: before 15, ALTER SYSTEM refuses a
# setting whose extension is not loaded ("unrecognized configuration
# parameter"), which 15 and later accept -- verified against postgres:14. It
# is user-settable, so a reload applies it. 1 records every statement rather
# than one in max_connections, so what a test sees is not a matter of chance.
docker exec "$c" psql -U postgres -v ON_ERROR_STOP=1 -qc \
  "ALTER SYSTEM SET pg_qualstats.sample_rate = 1" -c "SELECT pg_reload_conf()"

loaded=$(docker exec "$c" psql -U postgres -Atc "SHOW shared_preload_libraries")
echo "postgres $pg preloads: $loaded"
for lib in pg_stat_statements pg_stat_kcache pg_wait_sampling pg_qualstats; do
  case ", $loaded, " in
    *", $lib, "*) ;;
    *) echo "$lib is not preloaded" >&2; exit 1 ;;
  esac
done
