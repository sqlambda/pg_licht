# Security policy

## Reporting a vulnerability

Report it privately through GitHub:
[**Report a vulnerability**](https://github.com/sqlambda/pg_licht/security/advisories/new).
Please do not open a public issue for it. A report is acknowledged as soon as it is
read, and you will hear what happens to it; a fix is released as a patch version, with
the advisory published once the release is out.

Fixes go into the latest release. There are no long-term support branches: upgrading to
the newest version is how a fix is received.

## What pg_licht guarantees

These are the properties a report would be measured against. Each is stated in full,
with its limits, in the manual's SECURITY CONSIDERATIONS section (`man pg_licht_mcp`).

- **It never writes to a database.** Every statement sent to a database runs inside a
  transaction opened with `SET TRANSACTION READ ONLY` and ended with `ROLLBACK`, so even
  a bug that let a query attempt a write fails with SQLSTATE 25006 instead of succeeding.
  The guard is per transaction, not per session, so it holds behind a connection pooler
  in transaction mode.
- **It executes nothing it did not write, with one bounded exception.** Catalog queries
  are parameterized; no argument is concatenated into SQL text. `explainQuery` with
  `analyze` executes the caller's statement only after its plan is proven free of any
  `ModifyTable` node, so data-modifying statements, data-modifying CTEs included, are
  never run. It requires an explicit timeout.
- **A PgBouncer admin console gets fixed `SHOW` commands only.** The console refuses
  transactions, so the read-only guard cannot apply there. The pooler tools send only
  `SHOW` commands that are constants in the server, with nothing from the caller in them,
  to a connection declared `kind = pgbouncer`. The connection's user should be one of
  PgBouncer's `stats_users`, which may run `SHOW` and nothing else. That second guard is
  the operator's to set up: pg_licht cannot verify which list a user belongs to.
- **It never returns rows from your tables**, with three exceptions that return no rows
  either: `checkKey` answers only whether a primary key exists, `rowScatter` counts the
  rows holding a value and the pages they are on, and `explainQuery` with `analyze`
  returns the plan, with row counts and timings.
- **It never returns a password it holds.** `listConnections` does not return one and
  never expands a service file; `listForeignServers` omits user mappings;
  `listSubscriptions` omits each subscription's connection string.
- **The connections file must be private.** pg_licht refuses to load it if it is group-
  or world-accessible, the rule libpq applies to `~/.pgpass`.

## What can still reach the caller

Values that came from your data, by design: the text of statements other sessions are
running (`currentActivity`, `currentLocks`) and that `pg_stat_statements` recorded
(`statementStats`, `explainQuery`), including any literal they carry; setting values
(`serverSettings`, which for a privileged role can include a standby's
`primary_conninfo`); and, from a PgBouncer console, client addresses, application
names, backend hosts and pool users. The manual's "What reaches the caller" lists every
case. Run pg_licht as a role whose visibility you are content to hand to the client.

## The build

The Linux release binaries are built with PIE, full RELRO, a non-executable stack, the
stack protector and, against glibc, `_FORTIFY_SOURCE=3`; `cpp/test/hardening-check.sh` checks
each property on every Linux binary the release ships. GitHub Actions are pinned to
commit SHAs and kept current by Dependabot.
