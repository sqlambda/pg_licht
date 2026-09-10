# Changelog

## 4.3.0 (2026-09-09)

Six new tools, from the twenty-four PostgreSQL catalogs this server did not
read. Each closes a question the existing tools could only get halfway to, and
one of them makes an instruction this server already gave finally executable.

Sixty-eight operations. Measured on PostgreSQL 18: a bare login role runs 57 of
them at full fidelity, a role with `pg_monitor` 63.

### Added

- **`listPartitions` and `partitionDetails`.** Nothing here read
  `pg_partitioned_table` or `pg_inherits`. `relkind = 'p'` appeared in five
  places and only ever printed the words "partitioned table", so a parent's
  children, its key, its bounds and its default partition were all invisible —
  while `tableSize`'s own description told the caller to *"measure the
  partitions"*, an instruction there was no way to follow. That is why this is
  the first item of the release: it makes existing advice executable rather than
  adding a new answer.

  `listPartitions` summarises every parent in a schema — strategy, key,
  partition count, combined rows and size, and whether a `DEFAULT` partition
  exists and how many rows it holds. It reads `reltuples` and `relpages`, which
  are catalog columns, so it opens no relation and takes no lock. Measured sizes
  are deliberately not folded in: a parent with three hundred children would be
  three hundred relation opens behind an innocent-looking call.

  `partitionDetails` carries the per-partition statistics, which is the point of
  it. **Autovacuum runs per partition**, so a parent has no vacuum state of its
  own — its `n_dead_tup` cannot move — and `bloat-and-vacuum-review` was ranking
  a relation whose counters are permanently zero while the child that had fallen
  behind went unlisted. The prompt now takes partitions from here and ranks
  those.

  Bounds are returned **verbatim, never parsed**. A bound carries whatever types
  the key columns have, so parsing one generically is not possible and a
  misparsed boundary is worse than an unparsed one. For a range parent, the
  highest upper bound against `now()` is how to see that next period's partition
  was never created — the classic overnight failure — and that is readable off
  the text.

  `default_rows` is the finding to look for: rows land in a `DEFAULT` partition
  when they match no bound, so a growing default is a missing partition that has
  not failed loudly *yet*.

- **`roleDependencies`** — the inverse of `checkRoleAccess`. That tool answers
  whether a role may *use* an object; nothing answered what depends *on* it,
  which is the whole of

  ```
  ERROR:  role "x" cannot be dropped because some objects depend on it
  DETAIL: 4 objects in database app
  ```

  — a message that reports a count and names nothing. `by_kind` separates
  `owner`, which blocks `DROP ROLE` outright and is cleared by `REASSIGN OWNED`,
  from `acl` and `policy`, which `DROP OWNED` clears; that distinction is the
  fix, not a detail.

  `pg_shdepend` is shared across the cluster, and that cuts both ways. The count
  and the per-database breakdown cover **every** database. The names do not: an
  object id resolves only from the database it lives in, so `objects` carries
  this database and the shared catalogs while others appear as counts.
  Reporting only what is resolvable would have answered *"nothing depends on
  this role"* to somebody about to drop it.

  Reads `pg_roles`, never `pg_authid`, so a bare login role can ask. That is
  the role most likely to be asking: the one that cannot run `DROP ROLE`
  itself and wants to know what to hand the person who can.

- **`defaultPrivileges`** — `ALTER DEFAULT PRIVILEGES`, which decides what
  grants the **next** object gets. `checkRoleAccess` answers about the objects
  that exist, and a correct answer today that is wrong for tomorrow's table is
  the standing cause of "the new table is not readable and every old one is" —
  which presents as a broken grant and is a missing default. Scope `global`
  (`defaclnamespace = 0`) overrides the hard-wired defaults while per-schema
  entries are *added* to them, so two entries for one type are cumulative rather
  than conflicting, and `granted_by` is reported because a default applies only
  to objects that role creates.

- **`largeObjects`** — growth no size tool can see. Large objects live in a
  catalog rather than in any user relation, so `tableSize`, `listTableSizes` and
  `tableStats` are all blind to them while `diskUsage.databases` counts their
  bytes. The signature is *"the database grew and no table did"*, which
  `triage-disk-space` could not resolve: it would rank tables, find nothing, and
  stop.

  Counted and owned, **never sized**. The bytes are in `pg_largeobject`, which
  the documentation says is no longer publicly readable and directs callers away
  from, so a size is not available here and is not invented. The prompt is told
  not to propose `lo_unlink` from a count either: an object is unreferenced only
  if no column holds its oid, which the catalog cannot answer, and unlinking a
  live one loses data.

- **`replicationStats`** — the publisher side, and the only source of lag as a
  **time**. `replicationSlots` reports what a slot *retains*, in bytes;
  `subscriptionStats` cannot measure lag at all, because neither of its LSN
  columns references the publisher. So "how far behind is this replica, in
  seconds" had no answer anywhere. Serves physical standbys and logical
  subscribers alike, and both halves run on their own savepoint because they
  fail differently.

  Senders are keyed by `application_name` **plus pid**. A walreceiver's default
  `application_name` is the standby's `cluster_name`, and Debian packaging sets
  that per major rather than per host — `18/main` on every install — while
  unpackaged builds all default to `walreceiver`. Two such standbys share a
  name, and an object keyed by name alone keeps one of them, silently. The tool
  whose reason to exist is "which replica is behind" cannot be allowed to lose
  a replica.

  Two readings it states because both are routinely misread. The view is
  security-restricted **per row** rather than refused, so a role without
  `pg_read_all_stats` sees the senders exist with many columns null — which
  looks like an idle replica and is a permission answer. And the lag columns
  revert to `NULL` a short time after a standby has entirely caught up and WAL
  activity stops, so a null lag on an idle replica means caught up, while a
  non-null one there is the last measurement rather than the current state.

- **`explainQuery` and `evaluateIndex` take `settings` and `plan_as_role`.**
  4.2.1 made the mismatch visible — every `EXPLAIN` gained `SETTINGS`, so a plan
  said what environment produced it, and `hostCapacity.overrides` said what
  production uses. Neither removed it. `work_mem` alone changes the algorithm
  rather than the cost: measured on PostgreSQL 18 over 400,000 rows with
  identical statistics, one statement planned as `GroupAggregate` over a `Sort`
  at 64kB and as `HashAggregate` at 512MB.

  Three decisions carry the safety:

  `set_config(name, value, is_local := true)` rather than a built `SET LOCAL`
  string. It is a function call with bound parameters, so a value cannot escape
  into SQL text at all, and `is_local` means the setting reverts when the
  transaction ends — it cannot leak across a connection a pooler hands to
  somebody else. That is the property that made the read-only guard
  transaction-scoped rather than session-scoped, and the same reason. A test
  asserts it by planning again afterwards and requiring the setting to be gone.

  **An allowlist, not a denylist.** An arbitrary passthrough would let a caller
  turn off `default_transaction_read_only`, remove `statement_timeout`, or
  change `role` and `session_authorization` — three of this server's four safety
  properties, handed away through a convenience argument. The set that changes a
  plan is bounded and known, and every entry is `USERSET`. `enable_*` is
  admitted by prefix, because every one of them is a planner toggle by
  convention and new ones arrive most releases.

  **An unknown name is refused and nothing is planned.** Silently dropping it
  would return a plan the caller believes was built under an environment that
  was never applied — the exact failure class 4.2.1 and 4.2.2 kept correcting,
  reintroduced through the fix for it.

  `search_path` is excluded even though it changes plans, because it does so by
  changing *which objects the statement resolves to* rather than how they are
  joined; planning against a different table than the caller meant is worse than
  refusing, and the refusal says so. `plan_as_role` reads `pg_db_role_setting`
  and applies only the planner half, listing what it skipped rather than
  dropping it. A per-database entry (`ALTER ROLE ... IN DATABASE`) overrides
  the role-wide one, which is the precedence the server itself gives them; the
  entries are applied in that order so the last one applied is the one
  production runs under. Each is applied on its own savepoint: a refused value
  is reported under `skipped_from_role` and the rest still apply, rather than
  one bad entry aborting the transaction and taking the plan with it.

  `evaluateIndex` takes both for a reason of its own: its whole output is a
  before/after cost comparison, so an environment that does not match production
  makes **both halves** answer a different question.

  `explain-and-fix` and `diagnose-slow-query` now plan twice and compare rather
  than noting a caveat. It is the same shape as the generic-versus-custom
  comparison they already make: two plans, one difference, and the difference is
  the answer. Where they agree, the environment is ruled *out* instead of
  carried.

- **`listPublications` reports the schemas published via `FOR TABLES IN SCHEMA`**
  (PostgreSQL 15+). The members were never missing —
  `pg_publication_tables` resolves them either way — but the *declaration* was,
  and it decides what happens next: a table created later in a published schema
  joins the publication by itself, while one added to a table-list publication
  does not. Two publications with identical members today can behave differently
  tomorrow.

- **`roleDependencies` names parameters granted with `GRANT SET ON PARAMETER`**
  (PostgreSQL 15+). Those are shared dependencies like any other grant, so the
  rows were already in the total and only the name was absent.

### Fixed

- **Tablespace comments were never readable.** `listTablespaces` read its
  description through `obj_description(oid, 'pg_tablespace')`. A tablespace is a
  **shared** object, so `COMMENT ON TABLESPACE` writes to `pg_shdescription`,
  and `obj_description()` reads `pg_description` — so every commented tablespace
  has reported `''` since the tool existed. Not null, not missing: empty, which
  reads as "no comment".

  Verified on PostgreSQL 18 before changing anything: the same tablespace gave
  `NULL` through `obj_description` and the comment through `shobj_description`.

### Compatibility

Additive except for one corrected field.

`listTablespaces.description` now returns the comment where one exists, having
previously returned `''` for every tablespace. A caller that treated empty as
"no comment" will start seeing text.

Everything else adds. `listPublications` gains `schemas` on PostgreSQL 15 and
later; `explainQuery` and `evaluateIndex` gain two optional input properties and
a `planning_environment` key that appears only when one of them is used; the six
new tools are new names. No existing tool's name or input schema changes and no
key is removed.

### Deliberately not built

`pg_seclabel`, `pg_shseclabel` and `pg_init_privs`. Security labels are
meaningful only with a label provider in use, and `init_privs` is extension
bookkeeping. Neither has an operational question behind it, which is the bar
this project sets for a new tool.

## 4.2.2 (2026-09-07)

4.2.1 was exercised against a real 617 GB cluster — 24 schemas, 3,165 tables,
723 roles — by running all twelve prompts end to end. Four tools returned
**nothing at all**, and one of them was a regression 4.2.1 had introduced hours
earlier.

The failure mode is the point. A tool whose payload exceeds the client's limit
does not return a truncated answer; it returns an empty one. So an oversized
reply is indistinguishable from an empty database, and a caller reads "no
roles", "no publications", "no schemas" and moves on. Every fix here is the
same shape: report the count, name a bounded sample, and say when the rest were
dropped.

### Fixed

- **`hostCapacity` returned nothing on a multi-tenant cluster — a 4.2.1
  regression.** That release added `overrides` from `pg_db_role_setting` and
  emitted one row per role. On a database with one login role per tenant that is
  one row per tenant: 701 rows, 105 kB, over the limit. The tool returned
  nothing and took `capacity-check` step 2 and `triage-active-sessions` step 1
  with it — both had a working answer before 4.2.1 and none after.

  It is now collapsed by `(scope, name, value)`, the only grouping that loses
  nothing: two roles with **different** values stay separate rows, and a value
  that differs from the global one is the entire reason to read this. What
  collapses is repetition. Each group carries `count`, up to five example names
  and `names_truncated`.

  Verified against the cluster that broke it: **701 rows became 9 groups**, the
  largest collapsing 688 roles into one, and all three overrides that mattered
  survived as their own rows.

  Worth recording plainly: this walked into a hazard already written down. The
  roadmap lists two unbounded payloads as known defects, and `tableStats`' own
  comment calls inlining a full histogram "the unbounded-payload mistake this
  project has already made once elsewhere". Same day, same mistake, in the
  commit that was fixing the tool.

- **`listSchemas` named every relation in every schema.** One schema held 1,816
  tables; the payload was 89 kB. Now `table_count` with up to 25 names and
  `tables_truncated`. The count is what a summary needs; `listTables` names them
  and takes one schema at a time precisely so the caller bounds its size.

- **`listPublications` inlined every member table.** A publication
  `FOR ALL TABLES` resolves through `pg_publication_tables` to every table in
  the database, so one line of DDL produces an unbounded list and nothing in the
  declaration hints at the size. Now `table_count`, up to 50 names, and
  `tables_truncated`.

- **`listRoles` returned every role.** 723 of them, 689 `client_*` with default
  attributes and no memberships, at 165 kB. Capped at 200 and **ordered so every
  role carrying a non-default attribute or a group membership comes first**, so
  the cap drops the identical crowd rather than the superusers. A new `pattern`
  argument searches by name when the role wanted falls outside the cap.

- **`serverSettings` dumped all 456 settings**, 99 kB, of which one category
  alone held 95. It now defaults to the settings that differ from their built-in
  default — the set that describes *this server* rather than PostgreSQL, which
  is the same set `EXPLAIN (SETTINGS)` reports and for the same reason.
  `all:true` restores the full dump, and `pattern` searches by name or category.

- **`idx_scan` was reported without the window it covers.** `pg_stat_reset()`
  zeroes `idx_scan` and `seq_scan` with everything else, so `idx_scan = 0` means
  "not since the reset", never "never". `stats_reset` was reported by six tools
  and by none of the six sites emitting a scan counter — the one place its
  absence changes a decision.

  This had already cost a wrong recommendation. On a database whose statistics
  were reset four days earlier the review produced *"1.91 GB reclaimable
  immediately — 0 scans, a drop candidate"*. Four days of evidence. A month-end
  report, a quarterly job and a failover path all read as zero over that window,
  and dropping a 1.9 GB index is not reversible in an afternoon.

  `duplicateIndexes` and `tableStats` now carry `counters_since`, and
  `bloat-and-vacuum-review` step 5 reads it **before** `idx_scan`, saying plainly
  that where the window is short `idx_scan = 0` is not evidence and waiting
  beats dropping. It is documented as a lower bound:
  `pg_stat_reset_single_table_counters()` zeroes one relation without moving
  `pg_stat_database.stats_reset`.

- **A missing schema read as a clean one.** An empty result from a schema-scoped
  tool had two causes that look identical and mean opposite things.
  `bloat-and-vacuum-review` defaults to `public`, and on a database that has no
  `public` schema a caller following that default got `{}` and would reasonably
  conclude the schema was clean. `listTables`, `listTableStats` and
  `listTableSizes` now check `pg_namespace` and name the missing schema, while a
  schema that exists and is empty still returns an empty answer rather than an
  error.

  One existing test asserted the old silence — `TablesUnknownSchemaReturnsEmpty`
  was pinning the defect — and is renamed and inverted.

- **An unplannable statement now says why, and whether params were applied.**
  `explainQuery` met SQLSTATE 42883 on a statement recovered from
  `pg_stat_statements`, fell through to a bare "explain failed", and gave the
  caller nothing. Supplying `params` produced the identical error with no
  indication they had been applied, so the natural next step was to retry with
  more params forever.

  The cause is the normalization rather than the statement: replacing a literal
  with `$n` strips its type, and a placeholder nothing constrains — 
  `CASE WHEN … THEN $6 ELSE $7` is the usual one — is assigned `text` when the
  statement is **parsed**, before any value is bound. No `params` argument can
  change that. The error now carries `params_supplied` and says that params
  cannot help here, along with the two things that do: inline the literals, or
  add explicit casts.

### Compatibility

Three tools change what a **default** call returns. Each previously returned
nothing on the databases affected, so every one of these is strictly more
information than before, not less.

| tool | before | after | who sees a change |
|---|---|---|---|
| `listSchemas` | every relation name per schema | `table_count` + 25 names + `tables_truncated` | any schema with more than 25 relations |
| `listPublications` | every member table | `table_count` + 50 names + `tables_truncated` | any publication with more than 50 tables |
| `listRoles` | every role | 200, attribute- and membership-carrying first | clusters with more than 200 roles |
| `serverSettings` | all settings | only those differing from the built-in default | **everyone**; `all:true` restores it |
| `hostCapacity` `overrides` | one row per role | collapsed by (scope, name, value) | anyone with per-role settings |

`serverSettings` is the one that changes on every server rather than only large
ones, which is why it takes an explicit `all` flag rather than a silent cap.

Additive alongside them: `counters_since` on `duplicateIndexes` and
`tableStats`; `pattern` on `listRoles`; `pattern` and `all` on `serverSettings`;
`params_supplied` on the 42883 error. `listTables`, `listTableStats` and
`listTableSizes` return an error object where they previously returned `{}` for
a schema that does not exist — a shape change only on an input that was already
a mistake.

## 4.2.1 (2026-09-07)

Every PostgreSQL-factual claim in the tool descriptions, the twelve prompts and
the man page, checked against `postgresql.org/docs/18` and — where the
documentation is silent — against the PostgreSQL source. Thirteen findings.
Eight were behaviour rather than prose, and six of those change a value a
caller reads.

The organising fact is that most of these were not documentation errors sitting
on top of a correct implementation. They were wrong answers with confident
names, and prose was simply the surface they were visible on. 4.2.0's audit
asked *"does every prompt read what its tools return"*; this one asks the
question underneath it — *"does every tool return what PostgreSQL says it
does"* — and the answer was no thirteen times.

**Three of the six failed in the same direction as 4.2.0's `last_vacuum`:
wrong in the way that reads as healthy.** Two million transactions of
wraparound headroom that did not exist, a memory worst case computed from
settings a role can override, and a replication slot losing its WAL described
as one holding it. That is now four occurrences across two releases, and the
common cause is not carelessness about PostgreSQL — it is that each was written
from a plausible mental model and never checked against the sentence that
defines the field. They survived review because they read correctly.

The check that catches this class is mechanical, and is now the rule for new
work: **for every field named after a catalog column, view column or GUC, quote
the documented definition beside it.**

### Fixed

- **The wraparound stop limit was off by 2,000,000 transactions, overstating
  the budget.** `varsup.c`'s `SetTransactionIdLimit()` computes

  ```c
  xidWrapLimit = oldest_datfrozenxid + (MaxTransactionId >> 1)
  xidStopLimit = xidWrapLimit - 3000000
  xidWarnLimit = xidWrapLimit - 40000000
  ```

  so the age at which a server refuses to assign new transaction ids is
  **2144483647**. `wraparoundStatus` used 2146483647 — a 1,000,000 delta
  PostgreSQL has not used for many releases — and reported two million
  transactions of headroom that do not exist.

  The warning threshold was not reported at all. It fires first, and it is what
  produces `WARNING: database "..." must be vacuumed within N transactions` in
  the log, so the one threshold an operator has usually already seen was the one
  this tool could not show them.

- **`wraparoundStatus` prescribed single-user mode. The documentation
  prescribes against it**, in as many words:

  > "contrary to what was sometimes recommended in earlier releases, it is not
  > necessary or desirable to stop the postmaster or enter single user-mode in
  > order to restore normal operation."

  A plain database-wide `VACUUM` in normal multi-user mode is the documented
  recovery, and the description now carries the two traps the docs name and it
  did not: `VACUUM FULL` requires an xid of its own and will fail, and
  `VACUUM FREEZE` does more than the minimum needed to restore service. This is
  the one finding here whose cost was an unnecessary outage rather than a wrong
  number, in the text a model reads at the moment it is deciding what to tell
  somebody at three in the morning.

- **`wal_status: unreserved` meant the opposite of what both prompts said.**
  `triage-disk-space` and `replication-slot-review` glossed it as "the slot is
  the only thing keeping that WAL". The documentation:

  > "`unreserved` means that the slot **no longer retains** the required WAL
  > files and some of them are to be removed at the next checkpoint. […] This
  > state can return to `reserved` or `extended`."

  It is the server having *stopped* retaining, one step short of `lost`. The
  direction matters most in the prompt that could least afford it: on a
  disk-space page it means the space is about to come back on its own and the
  consumer is about to break. Both prompts now also carry that the state is
  reversible, which is what decides between waiting and dropping the slot.

  `extended` was incomplete the same way — the WAL is retained "either by the
  replication slot **or by `wal_keep_size`**", so the old gloss could send
  someone hunting a slot when a GUC was the retainer.

  Both passages also replaced a derived budget with the reported one:
  `safe_wal_size` is documented as the bytes that can still be written before a
  slot is in danger of becoming `lost`, and both prompts were reconstructing it
  from WAL volume and `max_slot_wal_keep_size`. Its null is its own finding —
  `max_slot_wal_keep_size` is `-1` and nothing bounds the retention, which is
  worse than a large number rather than better.

- **"Ordinary VACUUM frees nothing back to the filesystem" was too absolute.**
  The documentation says space is not returned "(in most cases)", and `VACUUM`'s
  truncation does return empty pages at the end of a table. On a full disk that
  exception is the case worth knowing, so `triage-disk-space` now names it, and
  names `vacuum_truncate` as the reloption that disables it.

- **Statistics hidden by row-level security were indistinguishable from
  statistics never collected.** `pg_stats` is defined `WITH (security_barrier)`
  and its `WHERE` clause ends

  ```sql
  AND (c.relrowsecurity = false OR NOT row_security_active(c.oid))
  ```

  so for a role that RLS applies to the view returns **no row at all** — not a
  filtered row. Every per-column statistic in `tableStats` and the whole of
  `columnHistogram` then came back null, reading exactly like a table nobody
  has analyzed.

  This is not a corner case where RLS is the convention: on a multi-tenant
  schema it is the normal path, and three prompts now reach for these values.
  `explain-and-fix` and `diagnose-slow-query` separate skew from staleness with
  them and `plan-schema-change` picks partition boundaries off the bounds — all
  three would have concluded "never analyzed" about a table whose statistics
  the planner is actively using.

  Both tools now report `stats_hidden_by_rls`, and `columnHistogram`'s note
  says which of the three causes applies rather than listing two of them. The
  owner is exempt unless `FORCE ROW LEVEL SECURITY`, so the flag is false for
  exactly the role that does not have the problem.

- **`size_estimate` assumed an 8kB page.** It computed `relpages * 8192`.
  `pg_class.relpages` is documented as a count of pages "of size `BLCKSZ`", and
  `BLCKSZ` is a compile-time option between 1kB and 32kB. On a cluster built
  with any other page size every estimate was wrong by the ratio, silently.
  Now `relpages * current_setting('block_size')`.

- **`hostCapacity`'s worst case ignored every per-role and per-database
  override.** It multiplied `work_mem` by `max_connections`, reading `work_mem`
  from `pg_settings` — which reports the value for *this* session. An
  `ALTER ROLE app SET work_mem = '512MB'` is invisible to anyone connected as
  somebody else, so wherever the application role carries a larger `work_mem`
  the committed worst case was understated.

  `pg_db_role_setting` is that layer and nothing here read it. `overrides` now
  carries every entry with its scope — cluster, role, database or
  role-in-database — and `committed_worst_case_bytes` is computed from
  `GREATEST(global, any override)`, with `work_mem_is_overridden` saying when
  those differ. The two `work_mem_times_max_connections` fields keep the global
  value: they are named after the setting and are literally what their names
  say; `committed_worst_case` is the field that promises a worst case, so it is
  the one that had to change.

  All overrides are reported rather than only the memory ones. A
  `statement_timeout` of 0 or a `search_path` set on one role explains as much
  as a memory setting does, and none of it is visible in `pg_settings`.

- **`listExtendedStatistics` could not tell a defined statistics object from a
  built one.** `pg_statistic_ext` is the definition; `pg_statistic_ext_data` is
  the data. A `CREATE STATISTICS` that has never been `ANALYZE`d has the first
  and not the second — a catalog row, no statistics, no effect on any plan —
  and this reported it identically to a working one.

  That became load-bearing in 4.2.0, whose prompt audit added "check whether
  extended statistics exist" to `explain-and-fix` and `diagnose-slow-query`
  precisely so neither would recommend creating what was already there. Both
  then read existence as usability, and the advice came out as "statistics are
  already in place" when the correct advice was `ANALYZE`.

  Now reports `built` and `built_kinds` — which of the declared kinds were
  actually computed, since `ANALYZE` can build some and not others — and
  `built_for_inherited`, which carries `stxdinherit` on PostgreSQL 15 and later
  and is null on 14, where the question cannot arise.

- **A plan was reported without saying what environment produced it.**
  `explainQuery` plans in *this* server's session, not the one the statement
  runs in, and said nothing about the difference. `work_mem` alone changes the
  algorithm rather than the cost. Measured on PostgreSQL 18, 400,000 rows,
  identical statistics, one statement:

  ```
  work_mem = 64kB              work_mem = 512MB
    GroupAggregate               HashAggregate
      -> Sort                      -> Seq Scan on t
           -> Seq Scan on t
  ```

  Everything `explain-and-fix` reads off node types, `Sort Method` or whether a
  node spilled is then read off a plan production never runs — and the report
  gave a reader nothing to notice.

  Every `EXPLAIN` now carries `SETTINGS` — on all six paths: bare, generic,
  prepared, analyzed, and both of `evaluateIndex`'s — which names the settings
  differing from the built-in default and so names the environment the plan was
  built in. PostgreSQL 12 and later, so no gate on any supported major.

  **That half only became useful because of the fix above it.** `SETTINGS` says
  what planned the statement; `hostCapacity.overrides` says what production runs
  it with. Neither answers the question alone, which is exactly why this was
  invisible until the other existed. `explain-and-fix` and `diagnose-slow-query`
  both make the comparison a step, and both say plainly that where the executing
  role has its own `work_mem`, this plan is not the plan production gets —
  before anything else is drawn from it.

  This release reports the mismatch; it does not remove it. Planning under
  another role's settings needs a new argument and a new input schema, so it is
  the next minor release rather than a patch.

- **Multixacts had no wraparound budget, though both documents promised one.**
  The tool description and the man page both said "each age as a percentage of
  … the hard limit" while only the xid half was reported. Multixacts have the
  same three-million stop limit and the same forty-million warning, and
  members-space exhaustion is its own incident with its own ceiling.

- **`vacuum_failsafe_age` was described short, and with the wrong subject.** It
  said "autovacuum stops yielding and skips index cleanup". It is *any*
  `VACUUM`, not only autovacuum — and there is a third effect that was missing:
  the Buffer Access Strategy is disabled, so the vacuum is free to use all of
  `shared_buffers`. That is why the cache looks wrecked during a failsafe
  vacuum, a reading `bufferCacheSummary` will show and nothing else here
  explained. The default of 1.6 billion and the silent floor at 105% of
  `autovacuum_freeze_max_age` are stated too, since the effective value is not
  the configured one.

- **Five smaller claims narrowed to what the documentation supports.**
  `inactive_since` "dates the problem exactly" — except that once a slot becomes
  invalid the value is never updated again, so on an invalidated slot it dates
  the invalidation, which is precisely the slot the prompt exists for.
  `conflicting` is always null for a physical slot while `invalidation_reason` is
  set for both kinds, so the pair does not cover the same slots.
  `diskUsage` named one privilege gate and has two — `pg_ls_*` needs
  `pg_monitor`, while the tablespace and database sizes need `CREATE` on the
  tablespace or `pg_read_all_stats`, so a bare role gets the four cheap sections
  and is refused the two expensive ones, which is a partial answer that arrives
  fast and reads like a complete one. The histogram array is
  `statistics_target`**+1** entries wide. And `pgstattuple` ships no
  `pgstat*index` function for gist, spgist or brin, which is narrower and truer
  than "no function".

- **`subscriptionStats`' "no byte lag" rationale is now definitional.** It
  argued observationally that `received_lsn` and `latest_end_lsn` "track each
  other". The documented reason settles the question instead: one is the last
  WAL location **received** and the other the last one **reported back** to the
  publisher's WAL sender, so neither references the publisher's position and no
  difference between them *can* express backlog. What it expresses is feedback
  latency, bounded by the status-report interval. Stated the other way round it
  read as an empirical accident a later release might revisit. Also notes that
  both LSNs and both `last_msg_*` timestamps are null for a parallel apply
  worker, so such a row is not broken.

- **`progressStats`' description undersold its own implementation.** It has
  returned `num_dead_item_ids`, `indexes_total` and `indexes_processed` on
  PostgreSQL 17 since 4.2.0, and mentioned only blocks, tuples and
  `dead_tuple_unit` — omitting index vacuuming, which is the phase a long
  `VACUUM` usually sits in and where block counts look stalled throughout.

### Added

- **`wraparound_warn_limit`, and `xids_until_warn_limit` per database.** The
  log warning fires forty million transactions before wraparound, thirty-seven
  million before the stop, and nothing here reported it.

- **`mxid_percent_of_wraparound_limit`, `mxids_until_wraparound_limit` and
  `mxids_until_warn_limit`**, so both axes carry both budgets.

- **`hostCapacity.overrides`**, with `work_mem_effective_max_bytes` and
  `work_mem_is_overridden` beside it.

- **`stats_hidden_by_rls`** on `tableStats` and `columnHistogram`.

- **`built`, `built_kinds` and `built_for_inherited`** on
  `listExtendedStatistics`.

- **A `Settings` block on every plan**, from `EXPLAIN (SETTINGS)`.

- **`delay_time_ms` and `delay_percent`** on a running vacuum, from
  `pg_stat_progress_vacuum.delay_time` (PostgreSQL 18). The total time a vacuum
  has spent *asleep* on the cost-based delay. A throttled vacuum and a vacuum on
  a slow disk look identical in blocks scanned per second and have opposite
  fixes — raise `vacuum_cost_limit`, or buy I/O — and nothing else here told
  them apart. `bloat-and-vacuum-review` reads it before concluding autovacuum is
  too slow.

### Changed

Nothing observable, and it is recorded here because it is the reason several of
the fixes above were cheap.

- **One tool registry replacing two tables five thousand lines apart.** A tool's
  name, description, input schema and implementation lived in a JSON literal in
  `get_tools_list` and an if/else chain of 61 branches in `dispatch_tool`, with
  nothing making them agree. They did agree — verified 62/62 with no orphan on
  either side while the new table was generated from them — but a tool
  advertised and not dispatchable would have built cleanly and failed at runtime.

  They are now one table; `get_tools_list` projects over it and `dispatch_tool`
  is a map lookup. Twenty-four tools had re-typed the schema default longhand,
  where one copy differing from the rest would have been invisible; 52 of the 62
  handlers are now a single line.

- **The version gates are named.** Twenty-nine bare comparisons against
  `150000`, `160000`, `170000` and `180000`, scattered through five thousand
  lines, are one enum and one switch. `sess.has(Feature::PgStatIo)` says why the
  branch exists. Adding a new major is reading one table — and the two
  version-gated additions in this release each cost one line there rather than
  another magic number.

  This is the part of "compile a separate binary per PostgreSQL version" that is
  worth having. The rest is not: the version is a per-connection runtime fact,
  and `verifyTopology` deliberately talks to servers of several majors in one
  sweep and reports each one's version.

- **The two registries compile as their own translation units.** `server.h` was
  the whole program, so every edit recompiled everything. Measured, Release:

  | | before | after |
  |---|---|---|
  | full clean, `-j4` | 49.9s | **39.4s** |
  | touch `server.h`, `-j4` | 49.2s | **39.4s** |
  | touch `tool_defs.cpp` | — | **12.7s** |
  | touch `prompt_defs.cpp` | — | **4.4s** |

  The honest cost: a serial `-j1` clean build goes 72.3s → 78.8s, because the
  header is now parsed by four translation units instead of two. More total
  work, far less of it on the critical path.

  Verified rather than assumed: `tools/list`, `prompts/list` and
  `resources/list` are byte-identical across all three negotiated protocol
  revisions before and after each of the three refactor commits.

### Compatibility

Additive except for six corrected values, stated here rather than buried
because they are observable.

| field | before | after | who sees a change |
|---|---|---|---|
| `wraparoundStatus` `limits.wraparound_limit` | `2146483647` | `2144483647` | everyone |
| `wraparoundStatus` `xids_until_wraparound_limit` | 2,000,000 too high | correct | everyone |
| `wraparoundStatus` `xid_percent_of_wraparound_limit` | reads low | correct | everyone |
| `hostCapacity` `derived.committed_worst_case_bytes` | global `work_mem` only | largest applicable | anyone using `ALTER ROLE … SET work_mem` |
| `hostCapacity` `derived.committed_worst_case_percent_of_ram` | as above | as above | as above |
| `tableStats` / `listTableStats` `size_estimate` | `relpages * 8192` | `relpages * block_size` | non-default `BLCKSZ` builds only |

Each is a fix rather than a redefinition, on the bar §7 sets and the reasoning
4.2.0 used for `last_vacuum`: **the field is named for a documented PostgreSQL
quantity and returned a different one.** No key is removed, no type changes, and
every one of them moves a number toward what its name already claims.

Everything else is additive. No tool's name or input schema changes, and the
`Settings` block arrives inside the plan payload `explainQuery` already returned
verbatim.

## 4.2.0 (2026-09-05)

Three paths that walked the configured databases one at a time now use the
bounded worker pool 4.1.0 built, the connection cache that release added is
given a ceiling, and logical replication gains the runtime half it never had.

### Added

- **`subscriptionStats`** — the runtime state of every logical replication
  subscription in the current database. `listSubscriptions` reads
  `pg_subscription`, which is entirely static configuration: a subscription
  that is `enabled: true` and eight hours behind was indistinguishable from a
  healthy one. This reads the three sources that say otherwise —
  `pg_stat_subscription` for each worker, `pg_subscription_rel` for per-table
  sync state, and `pg_stat_subscription_stats` for the error and conflict
  counters — and answers whether the subscriber is keeping up and, if not,
  whether it is stuck copying a table or failing to apply.

  Tables that are not yet ready are listed individually; the ready ones are
  counted, because on a large subscription that list is the whole catalog and
  says nothing.

  **It reports no byte lag, deliberately.** `received_lsn` and `latest_end_lsn`
  track each other rather than the publisher: measured against a 19 MB backlog
  on a live subscriber, both sat at the same LSN and their difference was zero
  in either direction, before and after catch-up. Any field derived from them
  would read "no lag" at exactly the moment there was some. Byte lag is a
  publisher-side quantity — the slot's `confirmed_flush_lsn` against
  `pg_current_wal_lsn()` — and `replicationSlots` already reports it as
  `retained_wal_bytes`. What this tool carries instead is `msg_age_s`, which
  needs no clock agreement with the caller.

  It composes with `progressStats`: a table still copying has a worker with a
  real backend pid, and passing that pid to `progressStats` gives the byte and
  tuple counts of that exact copy. Note that `bytes_total` is zero there — a
  table sync streams from the publisher rather than reading a file of known
  size — so `bytes_percent` and `elapsed_s` are null and `bytes_processed` is
  the figure that moves.

  No role grants it. The catalog is world-readable here, so this works for a
  bare login role; `checkPrivileges` gains no entry for it and the counts move
  to 48 of 59 for a bare role and 55 of 59 for `pg_monitor`.

  Version-gated four ways: `errors` is absent entirely on PostgreSQL 14, which
  has no `pg_stat_subscription_stats`; `leader_pid` needs 16, `worker_type`
  needs 17 (below it the type is inferred from whether the worker is bound to a
  relation), and the seven `conflicts` counters need 18. An absent key means
  the server cannot answer, which is not the same as zero.

- **Every prompt was audited against what its tools actually return**, and one
  named a field that did not exist. `bloat-and-vacuum-review` said to read
  `last_vacuum` *and* `last_autovacuum`, and no payload contained the second.
  That turned out to be the tool's bug rather than the prompt's — see the
  `last_vacuum` entry under Fixed — so the field now exists and the prompt
  reads both, which is what it wanted all along.

  Six prompts gained readings the tools were already returning:

  | prompt | now reads | because |
  |---|---|---|
  | `bloat-and-vacuum-review` | `n_ins_since_vacuum`, `n_tup_newpage_upd` | ranking by dead tuples misses insert-only tables entirely, and failing HOT updates are a cause of bloat rather than a symptom of it |
  | `triage-lock-contention` | `mode`/`lock_type`, `granted`, `wait_start`, `leader_pid`, `backend_xmin`, `application_name`/`client_addr` | it concluded "the fix is in the application that left it open" without ever saying how to name that application |
  | `capacity-check` | `temp_files`/`temp_bytes`, the session counters | it computed worst-case memory and never checked whether anything actually spilled |
  | `buffer-cache-review` | `usage_counts`/`usagecount_avg`, `evictions`/`reuses` | it asked in step 5 whether the cache is being churned, using a tool that answers exactly that in step 1 |
  | `diagnose-slow-query`, `explain-and-fix` | `temp_blks_written`, `listExtendedStatistics` | the spill is already in hand from the first call, and extended statistics were recommended without checking whether any exist |

- **`explain-and-fix` was rebuilt to investigate before it proposes.** It went
  from plan to fix, and a plan is evidence rather than a verdict — a statement
  can be slow with a perfect plan because the data is cold, the table is
  bloated, or it waited instead of working.

  It had a functional bug: it told the model to call `explainQuery` with
  `analyze: true`, which is refused without a `timeout_ms`. That call errored.

  The larger gap was `params`. Supplying them decides which plan comes back —
  without them a statement carrying `$n` placeholders is planned `GENERIC_PLAN`,
  which is what a prepared statement settles on — and the *difference between
  the two plans* is the whole diagnosis for "fast when I run it by hand, slow
  from the application". The prompt now asks for that comparison in both
  directions and reads the `generic`/`analyzed` flags rather than assuming.

  It reads what `EXPLAIN (ANALYZE, BUFFERS)` was already returning and it was
  ignoring: `shared_read` against `shared_hit` (a cold cache or an oversized
  working set is a capacity answer, not a query one), `Heap Fetches` on an
  index-only scan (a stale visibility map, fixed by vacuum and not by an
  index), and time no node accounts for (it waited). It grounds all of it in
  `tableStats` — row count first, since a sequential scan of a few thousand rows
  is the right plan, then the analyze timestamps, then `most_common_vals` for
  the skew that makes one plan right for a common value and wrong for a rare
  one.

  It can now conclude that no fix is needed, which is a legitimate and often
  correct outcome. An index proposal is shaped deliberately rather than named:
  `duplicateIndexes` first because a prefix of an existing index buys nothing
  and costs writes forever, then column order (equality before range), `INCLUDE`
  for an index-only scan, a partial predicate, and the operator class where the
  default will not be used. And the report says how anyone would confirm the fix
  afterwards, which it never did.

  `triage-lock-contention` also hands off to `bloat-and-vacuum-review` when the
  chain root holds an old `backend_xmin`, since a backend blocking one wait
  chain is simultaneously stopping vacuum cluster-wide.

- **`replication-slot-review` reads the fields the tool was already
  returning.** It ranked slots by `active`, `restart_lsn` and `wal_status` and
  ignored three readings that answer questions it was otherwise estimating:
  `inactive_since` dates the problem exactly, where a slot idle for an hour and
  one idle since a deploy three weeks ago are different findings with identical
  `retained_wal_bytes`; `conflicting` and `invalidation_reason` say the server
  retired the slot rather than it merely falling behind, and `wal_removed`,
  `rows_removed`, `wal_level_insufficient` and `idle_timeout` each need a
  different fix; and the spill counters separate logical decoding that does not
  fit in `logical_decoding_work_mem` from a consumer that is simply slow.

  It also gains the state that looks like abandonment and is not. A
  subscription created with `disable_on_error` switches *itself* off the first
  time apply fails, so the symptom is an inactive slot, a disabled
  subscription, and WAL piling up behind a consumer that is neither broken nor
  gone. Dropping the slot there destroys a subscriber that was one
  `ALTER SUBSCRIPTION ENABLE` from resuming — which is why the prompt now reads
  `enabled` beside `disable_on_error` rather than treating inactive as
  abandoned.

- **`columnHistogram`**, and three points off the histogram added to
  `tableStats`. Both exist to answer one question: which real values to re-plan
  a statement with.

  `tableStats` claimed to return "the per-column `pg_stats` histograms" and
  returned everything except the histogram. It now carries `histogram_bounds`
  as `low`, `mid`, `high` and `count` — the observed minimum, median and maximum
  of the distribution. These are values that **actually occur in the column**,
  so they can be passed straight back to `explainQuery` as parameters: a plan
  built for them is a plan for real data rather than for an invented constant
  that may match nothing. Paired with `most_common_vals`, which is the other end
  of the same question, they make a test matrix — the plan for a frequent value
  against the plan for a rare one is where parameter sensitivity shows itself.

  Three points rather than the array, because the array is `statistics_target`
  wide — 101 entries by default, up to 10001 — and inlining that for every
  column of a wide table is the unbounded-payload defect this project already
  has one of.

  `columnHistogram` is the detail half, for one named column: the full bounds
  array, the MCV list with frequencies, and `statistics_target`, which says why
  the histogram is the width it is and is the knob that changes it. Two tools
  rather than a flag, on the reasoning locked for `bufferCacheSummary` and
  `bufferCacheContents` — a summary asked of every column and a detail asked
  about one have different call frequencies and different shapes.

  The two halves of a distribution are complements, not alternatives, and the
  descriptions say so: `ANALYZE` puts the most frequent values in
  `most_common_vals` and builds the histogram from **what is left**, so a value
  in the MCV list never appears in the bounds however common it is. A column
  with too few distinct values has no histogram at all, and that comes back as
  a null with the reason rather than as an empty array.

  Writing the test caught the summary and the detail disagreeing about types —
  `"1"` against `1`, because the summary reached the array through a `text[]`
  cast. Both now go through `to_jsonb`, so an integer column gives numbers in
  both and the summary compares equal to the detail it summarises.

  These return more literal column values than anything else here. `pg_stats`
  filters on `has_column_privilege`, so a role without `SELECT` on the column
  gets nulls rather than data.

  Three prompts reach for them. `explain-and-fix` uses the three points to
  decide whether the plan is stable and escalates to `columnHistogram` when it
  needs to find *where* the plan turns rather than merely that it does.
  `diagnose-slow-query` uses them to separate skew from staleness — statistics
  that are current with an estimate still wrong is a different fix from
  statistics nobody has refreshed. And `plan-schema-change` uses the full array
  for partition boundaries: choosing them without the distribution is guessing,
  the guess is usually uniform when the data is not, and a table partitioned by
  month is a table with one enormous partition wherever the traffic actually
  was. The bounds are equal-frequency, so every Nth one divides the existing
  rows evenly.

- **`checkRoleAccess`** — whether one role holds privileges on one table, view,
  sequence, function or procedure, answered by `has_table_privilege` and its
  relatives rather than reconstructed from ACLs. Role inheritance, grants to
  `PUBLIC`, ownership and superuser fold in the way the server folds them, which
  is laborious and easy to get subtly wrong by hand.

  It needs no grant of its own — those functions and the catalog are
  world-readable, so any role that can connect may ask about any other. The
  argument is `grantee` rather than `role`, because `role` is reserved
  server-wide for narrowing a sweep to a primary or a replica.

  Three things it reports rather than folds in, because folding them in would
  produce a confident wrong answer:

  - **Schema `USAGE` and database `CONNECT` come back beside the object
    privileges.** `SELECT` on a table is inert without `USAGE` on its schema,
    and the error names the *table*, which sends people to the wrong object.
  - **Column-level grants are listed when the table-level answer is no**, so a
    partial yes does not read as a flat no.
  - **Row-level security is not considered by `has_table_privilege` at all.** A
    true can still return zero rows. The `row_level_security` block carries
    whether RLS is on, whether it is *forced*, whether this particular role is
    subject to it, and every policy with whether it applies — because the owner
    is exempt unless forced, which is why checking as the owner proves nothing
    about anyone else.

  A routine name reports every overload with its signature and
  `security_definer`. A missing role and a missing object are both answers
  rather than errors.

  Measured against a purpose-built rig while writing it, and one result
  corrected the design: `has_table_privilege` **does** honour `rolinherit`, so a
  `NOINHERIT` member of a granted group gets `false` even though the privilege
  is one `SET ROLE` away. A false is therefore "not right now" rather than
  "never", and `role.inherits` and `role.member_of` are returned so the two can
  be told apart.

- **`diskUsage`, and `triage-disk-space` to go with it.** "The disk is filling"
  is the classic night page and was the one alert with no prompt — but a prompt
  alone would have been unexecutable, because answering it normally means `df`
  and this server speaks only SQL.

  It turns out almost all of it *is* reachable: `pg_ls_waldir()`,
  `pg_ls_archive_statusdir()`, `pg_ls_tmpdir()` and `pg_ls_logdir()` return
  names and sizes, and `pg_tablespace_size()` and `pg_database_size()` cover the
  data. So `diskUsage` reports WAL size and file count, the archive backlog,
  temp files on disk right now, the log directory, and sizes per tablespace and
  per database across the whole cluster — with no shell on the server.

  **What is genuinely unreachable is the headroom.** PostgreSQL exposes no
  function for total or free space, so the tool says what is consuming space and
  how it divides, never how much is left, and says so in its own payload rather
  than letting a caller assume otherwise.

  Two findings it makes available that were previously invisible here. A
  climbing `.ready` count in the archive status is a failing `archive_command`
  retaining every segment it has not archived — **indistinguishable from an
  abandoned replication slot by WAL size alone**, and a completely different
  fix. And a tablespace carries its location, so the database that is growing
  and the volume that is full need not be the same device; on a real server this
  showed a tablespace and the log directory on two volumes neither of which was
  `pg_default`.

  Each section runs on its own savepoint, because they fail independently and
  for ordinary reasons: `pg_ls_logdir()` *raises* rather than returning empty
  when `logging_collector` is off, and a role short of `pg_monitor` is refused
  the `pg_ls_*` family outright. One refusal returns an error in that key and
  leaves the rest answered.

  What it costs, measured: the four directory sections are 0.7 ms together, and
  the tablespace and database sizes are the whole cost — 314 ms for the complete
  call against a cluster of roughly a terabyte, where a bare `databaseSize` is
  already 163 ms. They walk the tree and `stat()` every segment, so they scale
  with **file count rather than bytes**, and the two overlap:
  `pg_tablespace_size(pg_default)` covers the same subtree the per-database
  sizes do. They are kept separate anyway because they answer different
  questions — which *volume* is full against which *database* grew — and
  deriving one from the other is wrong wherever a relation lives in a
  non-default tablespace. Nothing is read, no relation is opened and no lock is
  taken; it is metadata, not I/O. But those figures were measured with directory
  entries warm in the page cache, and a filling disk is exactly when they are
  not, so `statement_timeout` bounding the call matters — the failure mode is a
  timeout that names itself rather than a hang.

  The prompt ranks what can be freed by reversibility and leads with what makes
  the obvious response wrong: `VACUUM FULL` needs free space equal to the table
  and its indexes *before* it releases any, and holds an `AccessExclusiveLock`
  throughout — so it is not a disk-full action, and saying that is more useful
  than proposing it.

- **`wraparoundStatus` carries the runbook that would otherwise have been a
  prompt.** Wraparound is a single tool call, so the reasoning belongs in the
  description where it is read at the moment of the call rather than in a
  thirteenth prompt. It now separates the two alarms — past
  `autovacuum_freeze_max_age` is loud maintenance working as designed, while
  approaching the wraparound limit ends in a server refusing writes — and states
  the fact that makes most responses to that alert fail: **nothing can be frozen
  past the oldest transaction still visible to something**, so more workers and
  a manual `VACUUM FREEZE` achieve nothing while the horizon is held. It names
  the four holders, including the one this server cannot see —
  `pg_prepared_xacts`, invisible in `pg_stat_activity` and the answer whenever
  nothing else explains it.

- **`triage-active-sessions`, for a server running more sessions at once than
  it has cores.** Two things it establishes before anything else, because both
  are routinely assumed instead.

  **`active` does not mean on-CPU.** `pg_stat_activity` calls a backend active
  while it is executing a statement, and a backend waiting on a lock, on a disk
  read or on a parallel sibling is executing a statement. Fifty active sessions
  of which forty-five are lock waits is one blocker and a queue, not a shortage
  of cores. Only backends with no wait event compete for CPU, and that is the
  number compared against the core count — after parallel workers are collapsed
  into their leaders via `leader_pid`, since one query with four workers is five
  rows and one unit of user concurrency.

  The split by `wait_event_type` then decides which investigation this actually
  is, and hands off: locks to `triage-lock-contention`, I/O to
  `buffer-cache-review`, IPC back to the parallelism settings, LWLock to the
  write path.

  **High concurrency is usually a symptom.** Sessions in flight is arrival rate
  multiplied by duration, so it rises when statements get slower even though
  nothing about the load changed. Where the server really is CPU-bound,
  `statementStats` says which term moved: more calls is growth and a capacity
  answer, the same calls taking longer is a regression that belongs to
  `diagnose-slow-query`. A plan that flipped, statistics gone stale, a table
  that bloated or a working set that outgrew the cache all present as a load
  problem and none of them is one — and the prompt says plainly that a
  connection pooler bounds the damage without making any statement faster.

  `capacity-check` flags `max_parallel_workers_per_gather` as the setting that
  lets one query oversubscribe a machine; this is where that consequence is
  observed, and the two now name each other.

- **`check-role-access`, a tenth prompt.** Whether a role can use a table,
  view, function or procedure — and, separately, which rows it then sees.

  Access is a chain of gates and the first failure is the whole answer, so the
  prompt walks them in order rather than reporting an object ACL and stopping.
  The gate people miss is `USAGE` on the schema, because `SELECT` on the table
  is inert without it and the resulting error names the *table*, sending
  everyone to the wrong object. It also covers the cases where a grant map
  answers a different question than it appears to: `PUBLIC` is a grantee like
  any other, `EXECUTE` is granted to `PUBLIC` by default on every new function,
  an owner holds everything implicitly and need not appear at all, and a
  `NOINHERIT` member holds a role's privileges only after `SET ROLE` — which
  application code almost never issues, so a grant that looks present is absent
  in practice.

  **Row-level security is treated as a second gate, opening only after the
  grants have already said yes.** That ordering is the point: RLS enabled with
  no applicable permissive policy is deny-all, so every grant checks out,
  `has_table_privilege` says yes, and the table returns nothing — the state
  nobody can explain. The owner bypasses RLS unless `FORCE ROW LEVEL SECURITY`
  is set, which is why checking as the owner proves nothing about anyone else;
  permissive policies OR together while restrictive ones AND, so one
  restrictive policy vetoes everything; and `USING` gates reads while
  `WITH CHECK` gates writes, so reading a row you cannot write back is normal
  rather than broken.

  Views and functions each redirect whose privileges apply: a view's base
  tables are read as the *view owner*, unless `security_invoker` (PostgreSQL 15
  and later) flips it to the caller and brings base-table grants and policies
  back into play; a `SECURITY DEFINER` function runs its body as the owner, so
  `EXECUTE` on it is effectively a grant of whatever the body can do.

  The prompt leads with `checkRoleAccess` rather than reconstructing the answer,
  and the manual walk below it is how a verdict becomes something an operator
  can act on: which gate said no, or what a yes does not cover.

  `checkPrivileges` is unrelated despite the name and now says so: that tool
  reports which of *this server's operations* the *connecting* role can run.

- **`diagnose-deadlock`, a ninth prompt.** A deadlock is over before anyone
  reads about it: PostgreSQL detects the cycle, kills a transaction and
  releases the other, so the pids in the log no longer exist and the live lock
  views describe a server that has moved on. `triage-lock-contention` follows a
  wait chain you can still see and is the wrong tool for it, which is why this
  is a separate prompt rather than a branch of that one.

  It reconstructs the deadlock from the log entry plus the schema. The lock
  kind in the report already names the shape — `ShareLock on transaction N` is
  a row-ordering problem, `ExclusiveLock on tuple` is a queue for one row, a
  relation lock means DDL was in the cycle — and the statements alone do not
  show the lock footprint: a foreign key locks the parent row the statement
  never names, a trigger runs statements the log never shows, and the plan
  decides the order rows are locked, so an index scan against a sequential scan
  is enough to start a deadlock that was not happening last week.

  It ranks fixes as consistent acquisition order, then a smaller lock
  footprint, then a shorter transaction, and says plainly that
  `deadlock_timeout` is not a fix — it changes when the cycle is detected,
  never whether it forms — and that a retry loop is a mitigation that hides an
  ordering bug rather than fixing one.

  Recurrence escalates in three stages, and in each of them the statement
  merely *appearing* in a view is the finding, before any counter is read.
  Still in `statementStats` on a cluster that is evicting means it runs often
  enough to survive eviction, which is exactly what a deadlock needs — so
  `info.dealloc` has to be read first, because it decides what presence means.
  Absence settles nothing: evicted, reset, or never tracked are three different
  things and the prompt asks which were ruled out. Present in
  `currentActivity` means it is running now, so the next occurrence can be
  watched rather than the last one inferred. Live and slow earns
  `currentLocks`, because a deadlock is a wait chain that closed into a loop
  and the chains forming now are the same edges caught before the last one
  joined up — at which point `triage-lock-contention` takes over.

- **`listSubscriptions` returns five more of the columns it can already read.**
  `stream`, `disable_on_error`, `origin`, `run_as_owner` and `failover` were
  omitted; it returned 6 of the 17 publicly-readable `pg_subscription` columns.
  `disable_on_error` in particular changes how an apply error count should be
  read. Version-gated the same way, and `subconninfo` remains excluded — the
  catalog revokes that one column from `public` and grants the other
  seventeen, so selecting it would fail for any non-superuser as well as
  exposing a password.

### Fixed

- **`last_vacuum` did not mean `last_vacuum`.** `tableStats` and
  `listTableStats` returned `GREATEST(last_vacuum, last_autovacuum)` under the
  name `last_vacuum`, and the same for analyze. The four
  `pg_stat_user_tables` timestamps are now four fields with the catalog's own
  names and meanings.

  The merge collapsed the one distinction those columns exist to draw. A recent
  `last_autovacuum` says autovacuum is reaching the table; a recent
  `last_vacuum` beside a null `last_autovacuum` says the opposite — somebody is
  keeping the table alive by hand and the cron job is hiding the finding rather
  than being it. Merged, both read as "vacuumed recently", and *"is autovacuum
  keeping up"* is the question these tools exist to answer.

  Measured on a real database while fixing it: a table reporting
  `last_analyze: 2026-09-04T03:30:30` under 4.1.1 reports
  `last_analyze: null, last_autoanalyze: 2026-09-04T03:30:30` now. Nobody had
  ever run `ANALYZE` on it; the old shape said otherwise.

  `estimated_from` stays the latest of all four. That merge is correct and
  unchanged: it dates `relpages` and `reltuples`, which any of the four
  refreshes equally.

- **`verifyTopology` no longer walks the registry one server at a time.** It
  opened one connection per configured database sequentially, which made the
  one tool whose entire job is to touch every server the slowest thing in the
  server. Measured against twelve unreachable hosts at the five-second sweep
  connect timeout: **60.1s before, 5.0s after** — one timeout rather than
  twelve. The cost scaled with the registry, so a 400-database configuration
  spent roughly half an hour on a call that reads as cheap.

  Only the observing is parallel. Every conclusion the tool draws comes from
  comparing members against each other, so the comparison passes still run
  sequentially over results held in configuration order, and the payload is
  byte-identical to 4.1.1's.

- **`role:` is no longer a serial pre-pass in front of a parallel sweep.**
  Filtering a sweep by observed role costs one connect per member before the
  tool's own, and that loop sat directly in front of the worker pool without
  using it. On twelve members with no topology labels to collapse, adding
  `role: "primary"` took the same call from **5.0s to 60.1s**; it is now 5.0s
  either way.

  This is the call an operator makes during a failover, when they can least
  afford to wait. Every member is still probed even when only the primary is
  wanted — stopping at the first would hide split brain — and the ordering of
  `matched`, of the skip reasons and of the split-brain note is unchanged.

- **The connection cache is bounded.** It held one idle connection per database
  touched, evicted only by the sixty-second idle timer, so a single sweep of a
  large registry left one open socket per member for a full minute. macOS ships
  a 256 file-descriptor limit, so this did not present as slowness: the members
  that ran after the limit was reached reported connect failures, which read as
  an unreachable fleet rather than a local resource limit.

  At most 32 idle connections are now held, the least recently released being
  dropped first. An ordinary registry never reaches the cap and nothing about
  its behaviour changes; past it the cache reconnects, which is what every
  release before 4.1.0 did.

### Compatibility

Additive except for one corrected field, and that exception is stated here
rather than buried, because it is observable.

`tableStats` and `listTableStats` change what `last_vacuum` and `last_analyze`
contain. Both previously carried the later of the manual and automatic
timestamp; each now carries the manual one, with `last_autovacuum` and
`last_autoanalyze` added beside them. A caller reading `last_vacuum` will start
seeing null for any table only autovacuum has touched.

This is shipped as a fix rather than held for a major version because the field
was not doing what its name says: it is named for a `pg_stat_user_tables`
column and returned something else, in the direction that reads as healthy. No
other tool ever merged them — `wraparoundStatus` has always reported both — so
the change also removes a disagreement between two tools about the same two
catalog columns.

Everything else is additive. No tool's name or input schema changes,
`listSubscriptions` gains keys, and every other payload is byte-identical to
4.1.1's. The performance work is timing and resource behaviour only.


## 4.1.1 (2026-09-04)

A compatibility fix for the output format 4.0.0 introduced, and the escape
hatch that release should have shipped with.

### Fixed

- **Every client gets a text block again, by default.** 4.0.0 sent
  `structuredContent` *instead of* `content` to any client that negotiated MCP
  revision `2025-06-18` or later, on the reasoning that sending both serialises
  the same payload twice and a client feeding results into a model's context
  may inject both — doubling the tokens on every call. The compatibility the
  spec's "send both" advice protects was assumed to be covered by the
  negotiated revision: a client that cannot read `structuredContent` would not
  ask for a revision that has it.

  **That assumption was wrong, and a first-party client disproved it on the
  first day.** Claude Code negotiates `2025-06-18` and reads `content`, so
  every tool call came back looking empty — reported as *"the content was
  missing from the response object"*. A client can advertise a revision it does
  not fully implement, and this server cannot tell.

  The default is now what the specification advises. The single-format
  behaviour is opt-in:

  ```
  PG_LICHT_STRUCTURED_ONLY=1
  ```

  The token saving is real — 27–46% on a typical call — but it is not worth a
  silently empty answer, and only an operator who has confirmed their client
  reads structured content can know it is safe. That is the right way round for
  a switch whose failure mode is a result that looks like nothing at all.

- **The mitigation documented in 4.0.0 was unreachable.** CHANGES, the README
  and the man page all said that a client advertising `2025-06-18` while
  reading only `content` could be pinned to `2025-03-26`. There is no such
  setting: `claude mcp add` offers transport, environment, headers and scope,
  and no protocol version — the revision is chosen by the client's MCP library.
  Documenting a workaround nobody can apply was worse than the original
  decision. It is corrected wherever it appeared, and replaced with a switch
  that works:

  ```
  PG_LICHT_MAX_PROTOCOL=2025-03-26
  ```

  `initialize` answers with the requested revision only when this server will
  speak it, so trimming that list is how an operator forces an older response
  shape from the server side, without the client offering any way to ask.

- **An unsupported revision is answered with the highest this server speaks,
  not the oldest.** The fallback was hardcoded to `2024-11-05`, so capping at
  `2025-03-26` and being asked for `2025-06-18` dropped all the way to
  `2024-11-05` — costing the client the annotations and titles it could have
  used. The specification says to answer with another revision the server
  supports; the highest one is the useful choice.

### Notes

- `PG_LICHT_MAX_PROTOCOL` governs the `initialize` handshake. The stateless
  revision carries its version per request in `_meta` and is not negotiated, so
  a client using it is unaffected by the cap — which is why the default change
  above matters more: it covers both eras.
- Both switches are read once per process. A server that changed its wire shape
  mid-session would be worse than one that cannot, so they are deliberately not
  reloadable.
- A new `protocol_compat` ctest pins the whole contract end to end against the
  real binary: which client gets a text block, what each switch does, and that
  a nonsense cap is ignored rather than leaving nothing to negotiate. It needs
  no database, because `listConnections` answers from the registry.

## 4.1.0 (2026-09-03)

Latency and startup, for registries of remote databases. No tool's name, input
schema or payload shape changed; nothing needs a configuration edit. But this
is a minor rather than a patch because the way pg_licht uses your servers
changes: it now holds connections open between calls, and a sweep opens
several at once.

The motivation was a Mac driving thirty databases over a VPN, where a tool call
answered in one millisecond of server time took the better part of a second.
Measured from the server's own log, one call was: connect, authenticate,
`BEGIN`, the read-only guard, a second round trip to set the statement ceiling,
the query, `ROLLBACK`, disconnect. Four of those are round trips before the
query, and the connect was paid again for the very next call to the same
database.

### Startup no longer touches the network

- **The constructor does not connect.** It parsed and validated the registry
  and then opened the default connection to prove it worked. That was one round
  trip to a remote database before the client could do anything, and it was
  fatal: an unreachable default aborted the constructor, `main` reported
  `Fatal DB Error`, and the server did not start — so a registry of thirty
  databases was unusable in its entirety because one of them sat behind a VPN
  that happened to be down.

  A malformed conninfo still fails at startup, because the registry is still
  parsed there. Only *reachability* moved to the first call that needs it,
  which is where it belongs: reachability is a property of the moment, not of
  startup.

- **`resources/list` does not connect either.** It enumerated schemas, which
  meant one connection per configured connection, sequentially, on a call
  clients make eagerly. Measured against a registry with one live database and
  three unroutable hosts: **it was still hanging after 75 seconds; it now
  returns in 0.00 s having opened no connection at all.** The schema list is
  still addressable — `pglicht://{conn}/schemas` returns it — but only when
  something reads it, and `pglicht://{conn}/schema/{schema}` was already a
  published template.

### Connections are reused

- **A connection is held between calls and closed after 60 seconds idle.**
  Measured: ten tool calls used to open ten connections and close ten; they now
  open **one**. Against a local socket that was free; against a remote database
  the connect, TLS handshake and authentication were most of what a call spent,
  repeated milliseconds later for the next call to the same database.

  The cache holds **only idle connections**: acquiring removes the entry,
  releasing puts it back with a timestamp. That is what makes the reaper thread
  safe without an in-use flag and without a race to get wrong — a connection
  being used by a call is not in the map, so the reaper cannot see it. It is
  keyed on the connection *name*, and every registry entry carries its own
  user, so a reused connection never crosses identities.

  A cached socket can be dead by the time the next call wants it —
  `idle_session_timeout`, a restart, a NAT table that forgot. There is no way
  to know but to use it, so a failure discards it and retries once. The retry
  is safe because nothing of the caller's has run at that point: only `BEGIN`
  and the setup batch, neither of which is anything to replay.

  The reaper thread is joined on shutdown rather than detached. A detached
  thread racing `PQfinish` against process exit is the classic source of an
  intermittent crash nobody reproduces by hand.

  **What to expect operationally:** idle connections belonging to pg_licht now
  appear in `pg_stat_activity` for up to a minute after use, one per connection
  you have configured and actually called.

### Sweeps run their members concurrently

- **A fan-out sweep visits up to 16 members at once**, where it visited them
  one at a time. On a thirteen-member replication group — a primary and a dozen
  replicas — measured **449 ms sequential against 91 ms, in a single round**.
  The width is sized so an ordinary production group finishes in one round.

  Concurrency is safe here in a way it would not be generally: sweep members
  are *distinct servers* by definition, and a group sweep already collapses
  members that would answer identically, so the width is spread one connection
  per machine rather than piled onto one.

  **The payload is byte-identical to the sequential version.** Results are
  written to fixed slots rather than appended, so members stay in configuration
  order however they finish. Two tests assert it, each running the sweep twice,
  because a reordering race would not show every time.

  Two pieces of state had to move to be per-thread: the last observed role and
  the last applied statement timeout were process-wide statics. Left shared,
  a parallel sweep would have attributed one member's role to another —
  silently, and precisely during a failover, which is when the tool is used.

### One fewer round trip on every call

- **The statement ceiling rides in the setup batch.** It was
  `SELECT set_config('statement_timeout', $1, true)` in a round trip of its
  own, because a parameterised statement cannot be batched with others in one
  `PQexec`. The value is an `int` this server computes — from the config file
  or from `explainQuery`'s own validated argument, never a string a caller
  supplies — so `SET LOCAL` with the number written out is exactly as safe and
  rides along with the read-only guard. Per call: **five application round
  trips become four**, which is a fifth of what a call spends once a pooler has
  removed the connect.

  Visible in `pg_stat_statements` as a `SET LOCAL statement_timeout` utility
  entry where there used to be a normalised `SELECT set_config(...)`.

### Notes

- Running pg_licht behind a connection pooler remains supported and tested, and
  is worth doing for remote databases even with reuse: the pooler amortises TLS
  across processes and survives restarts. See the man page.
- Verified on PostgreSQL 14 through 18, directly and through a transaction-mode
  pooler, and under AddressSanitizer, UndefinedBehaviorSanitizer and
  ThreadSanitizer.

## 4.0.0 (2026-09-02)

A breaking release, and the first since 3.0.0. It closes the roadmap: the
statistics split, structured output, and the resources and prompts that were
waiting on both. No configuration file needs an edit, and the connection
registry and CLI flags are untouched.

Two themes. The first is **volatility** — separating what changes only on DDL
from what changes continuously, which is what makes the sweep classification
correct and makes resources possible at all. The second is **one format per
client**: a client that negotiated `2025-06-18` now receives `structuredContent`
and no text block, which cuts 27–46% off the bytes of a typical call.

The reason for the break is not the fields. `listTables`, `searchTables` and
`tableDetails` each returned two unrelated kinds of thing in one payload — the
shape of an object, which changes when someone issues DDL, and readings taken
off it, which change continuously — and that made all three
`per_server: true` in the sweep classification. A `replication_group` sweep of
`tableDetails` therefore re-ran the entire structural payload (columns,
constraints, triggers, policies, view definitions) once per replica to get
byte-identical results back. Structure is replicated verbatim by definition;
only the counters legitimately differ. Splitting the tools is what lets the
classification be correct, and the classification is keyed on tool *name*, so
no argument or flag could have fixed it.

### Breaking

- **`listTables`, `searchTables` and `tableDetails` return structure only.**
  Every reading moved out. Where each field went:

  | field | was on | now on |
  |---|---|---|
  | `rows` | all three | `tableStats`, `listTableStats` |
  | `size` | `listTables`, `searchTables` (from `relpages`) | `tableStats`, `listTableStats` as **`size_estimate`** |
  | `size`, `indexes_size` | `tableDetails` (measured) | `tableSize` |
  | `seq_scan`, `idx_scan` | all three | `tableStats`, `listTableStats` |
  | `n_live_tup`, `n_dead_tup` | all three | `tableStats`, `listTableStats` |
  | `n_mod_since_analyze`, `n_ins_since_vacuum` | all three | `tableStats`, `listTableStats` |
  | `last_vacuum`, `last_analyze` | all three | `tableStats`, `listTableStats` |
  | `n_tup_newpage_upd`, `last_seq_scan` (16+) | all three | `tableStats`, `listTableStats` |
  | `columns.*.null_frac`, `avg_width`, `n_distinct`, `physical_order_correlation`, `most_common_vals`, `most_common_freqs` | `tableDetails` | `tableStats` |
  | `indexes.*.index_uses`, `last_use` | `tableDetails` | `tableStats` |
  | `indexes.*.size` | `tableDetails` | `tableSize` |
  | `toast.size`, `toast.index_size` | `tableDetails` | `tableSize` |

  `toast.name` stays on `tableDetails`, so it still answers whether a table
  stores anything out of line. `reloptions` stays: it is DDL.

- **`size` is now `size_estimate` on the statistics tools, and it is not the
  same number.** It is `relpages * 8192`, which is only as fresh as the last
  vacuum or analyze — on a table that has doubled since, it is half the truth.
  The rename is deliberate: the old field had the same name in `listTables`
  (estimated) and `tableDetails` (measured), and callers had no way to tell.
  An `estimated_from` timestamp now carries the reading that produced it, and
  `tableSize` is where a measured number comes from.

- **`tableDetails` no longer returns sampled column values.**
  `most_common_vals` is literal data out of the column — on a customers table,
  customer names — and it was returned by the tool an agent calls most often
  to inspect structure, which the README describes as querying the catalog
  "to inspect the structure (lower risk to leak data)". It is now on
  `tableStats` alone, and that tool's description says what it contains.

- **Every tool payload is a JSON object.** `structuredContent` may only be an
  object, so two shapes that were legal before are not any more:

  - **`listConnections` returns `{connections: [...]}`** and **`currentLocks`
    returns `{locks: [...]}`**, where both previously returned a bare array.
    This is the same fix, for the same reason, that 3.0.0 applied to
    `statementStats`.
  - **An empty result is `{}`, not `null`.** Any tool whose query matched no
    rows previously returned JSON `null`. Beyond being unrepresentable as
    structured content, `{}` is the better answer: `null` reads as "the tool
    failed", `{}` says "nothing there".

### Output format

- **A client that negotiated `2025-06-18` or later receives `structuredContent`
  and no `content` text block.** A client on an earlier revision receives the
  text block and no `structuredContent`. Never both.

  The spec suggests serving both for backwards compatibility, and this
  deliberately does not. Sending both serialises the same payload twice in one
  response, and a client that feeds tool results into a model's context may
  inject both — doubling the tokens on every call. The compatibility that
  suggestion protects is already covered by the negotiated revision: a client
  that cannot read `structuredContent` does not ask for a revision that has it.
  This is the rule 3.2.0 settled on for `annotations` and `title` — a
  protocol-revision feature reaches only a client that negotiated the revision
  defining it.

  If you have a client that advertises `2025-06-18` but only reads `content`,
  see 4.1.1: this was the wrong default, and the mitigation originally
  documented here — "pin it to `2025-03-26`" — was not something a client could
  be made to do.

- **The text block is serialised compactly.** It was pretty-printed with
  two-space indentation, which measured 18–39% of the payload across real
  catalog reads and which no consumer reads — every one parses the text as
  JSON. Combined with dropping the duplicate block, a modern client's wire
  bytes fall by 27–46%:

  | call | 3.2.1 | 4.0.0 | saved |
  |---|---:|---:|---:|
  | `listTables pg_catalog` | 163,225 B | 88,312 B | 45.9% |
  | `tableDetails pg_catalog.pg_class` | 8,656 B | 5,158 B | 40.4% |
  | `listTableStats pg_catalog` | 58,502 B | 40,040 B | 31.6% |
  | `listFunctions pg_catalog` | 1,126,238 B | 819,030 B | 27.3% |

- **Errors stay a text block in both eras.** `structuredContent` is the format
  for a tool's answer; an error is a message about why there is no answer, and
  the spec pairs `isError` with `content`.

- **`outputSchema` is declared for all 58 tools**, to clients that negotiated
  `2025-06-18`. The schemas are deliberately permissive — they type the keys
  that are unconditional, mark nothing `required`, and allow additional
  properties. This is the design, not a shortcut: payloads here are
  version-conditional (`tableStats` gains two fields on PostgreSQL 16), every
  tool may answer `{error, hint}` when an extension is absent, and a client
  that validates against a schema turns any drift into a failed call. A schema
  that can describe but never reject is the only safe kind here. A test walks
  real payloads from 35 tools against their declared schema on every run, which
  already caught one wrong type before release.

### Caching hints and pagination

Both are required or supported by revision `2026-07-28` and were previously
absent. Both are gated on `resultType: "complete"` — that is, on the modern
era — for the same reason `resultType` itself is.

- **`ttlMs` and `cacheScope`** now accompany every result the caching utility
  names as cacheable: `server/discover`, `tools/list`, `prompts/list`,
  `resources/list`, `resources/templates/list` and `resources/read`. The spec
  says a client SHOULD treat a result with no `ttlMs` as immediately stale, so
  their absence meant **`tools/list` — about 93 kB once 58 tools carry
  descriptions, annotations, titles and output schemas — was re-fetched
  whenever a client needed the tool list.** That payload is entirely static,
  and it grew in this release.

  | result | `ttlMs` | `cacheScope` |
  |---|---:|---|
  | `server/discover` | 1 h | `public` |
  | `tools/list` | 1 h | `private` |
  | `prompts/list` | 1 h | `public` |
  | `resources/templates/list` | 1 h | `public` |
  | `resources/list` | 1 min | `private` |
  | `resources/read` | 1 min | `private` |

  An hour for anything compiled in or read from the config at startup: none of
  it can change while the process lives, and no `listChanged` capability is
  declared, so there is no mechanism by which it could. A minute for anything
  read from a live catalog — structure changes only on DDL, which is the
  premise of serving it as a resource, but DDL does happen.

  `tools/list` is `private` rather than `public` for two independent reasons.
  Its content varies by negotiated revision (`annotations`, `title` and
  `outputSchema` are each gated) while the cache key is the method and its
  params, and the revision travels in `_meta`; and the `connection` property
  description carries the operator's configured default connection name.
  Neither belongs in a cache a shared gateway may serve to another caller. A
  private cache still gives the calling client the full benefit, which is where
  the saving actually lands.

- **Pagination** (`cursor` in, `nextCursor` out) on `tools/list`,
  `prompts/list`, `resources/list` and `resources/templates/list`. An invalid
  cursor is rejected with `-32602`.

  The page size is deliberately larger than any list this server currently
  produces, so **nothing is truncated for a client that ignores `nextCursor`**
  — 58 tools, 8 prompts and 5 templates are each a single page. The list that
  can genuinely outgrow a page is `resources/list`, which is connections ×
  schemas, and that is the case pagination exists for. Cursors are opaque and
  stable.

### New: resources, prompts and completions

These were blocked on the statistics split, and are the reason it went first.
Before it, `listTables` and `tableDetails` carried `n_dead_tup`, `last_vacuum`
and `idx_scan` — serving them as documents would have invited a client to pin a
number that moves under it.

- **Resources** (`resources/list`, `resources/templates/list`,
  `resources/read`) serve structure as documents under a per-connection URI
  scheme:

  ```
  pglicht://{conn}/schemas
  pglicht://{conn}/schema/{schema}
  pglicht://{conn}/schema/{schema}/table/{table}
  pglicht://{conn}/schema/{schema}/{functions,enums,types}
  pglicht://{conn}/server/{roles,extensions,settings}
  ```

  `resources/list` is bounded: it enumerates connections and their schemas, and
  exposes tables, functions, enums and types through templates instead — a
  database with 10 000 tables would otherwise produce a 10 000-entry response
  on a call clients make eagerly. Enumerating schemas costs one connection per
  configured connection; one that cannot be reached is skipped rather than
  failing the list. The topology axes are deliberately absent from the URI
  space: a resource names exactly one object in one database, and partial
  failure across members can only be reported per member, which belongs to
  tools.

  Readings stay tools. `currentActivity`, `currentLocks`, `statementStats`,
  `progressStats`, `tableStats`, `tableSize`, `tableBloat`, `indexBloat`,
  `wraparoundStatus`, `explainQuery` and both buffer-cache tools are
  measurements, and the model must decide when to take them.

- **Prompts** (`prompts/list`, `prompts/get`) — eight templates, each encoding
  an order of investigation that is easy to get wrong. Static text with
  argument substitution; they touch no database until the model acts on them.

  | prompt | arguments |
  |---|---|
  | `diagnose-slow-query` | `query_id?`, `min_duration_s?` |
  | `triage-lock-contention` | `connection?` |
  | `bloat-and-vacuum-review` | `schema?` |
  | `buffer-cache-review` | `connection?` |
  | `capacity-check` | `connection?` |
  | `replication-slot-review` | `connection?` |
  | `plan-schema-change` | `change`, `schema?`, `table?` |
  | `explain-and-fix` | `sql`, `params?` |

  **`diagnose-slow-query` is the hub.** A slow statement can end in a plan fix,
  a vacuum change, an index, or a schema change, so it now branches: if the
  plan shows a sequential scan where an index exists, or an index scan fetching
  far more heap pages than it returns rows, it routes to `tableBloat` and
  `indexBloat`; if bloat is confirmed it routes to `bloat-and-vacuum-review`
  rather than reaching for a REINDEX; and if the answer is DDL it routes to
  `plan-schema-change`, because an index that is *correct* and an index that is
  *safe to create on a live server* are different questions.

  **`replication-slot-review`** is the runbook for the failure mode that
  presents as something else: an unconsumed slot holds back the xmin horizon so
  vacuum cannot remove dead tuples anywhere in the cluster, and pins WAL until
  the disk fills. Both get diagnosed as bloat or as a disk problem. It reads
  `wal_status` (`reserved` / `extended` / `unreserved` / `lost`), establishes
  whose slot it is before proposing anything, weighs `max_slot_wal_keep_size`
  against the disk, and treats dropping a slot as irreversible for its
  consumer.

  **`plan-schema-change`** answers "how should I apply this DDL here?" by
  measuring the table rather than by prescribing a method. It deliberately
  contains **no DDL recipes**. A model already knows what
  `CREATE INDEX CONCURRENTLY` is; what it cannot know is this table's row
  count, measured size, existing indexes and what is holding a lock right now,
  and those are the only things that decide the answer.

  The reason is that a recipe is wrong at one end of the scale. The same
  statement that is instant on one table is an outage on another: a table with
  no rows can be rewritten in place and nobody notices, while the same rewrite
  at a billion rows is hours under an exclusive lock. Partitioning is the
  extreme case — trivial before there is data, a migration with a cutover
  after it. Reaching for the safest-at-scale approach on a small table is
  machinery nobody needs; reaching for the simple one on a large table is the
  outage.

  So the prompt sends the model to measure — and to measure **every object the
  change touches, not only the one named**. A statement that names one table
  routinely locks more than one: a foreign key locks the table it references as
  well as the table it is added to, a partitioned table means the parent and
  every partition, and the object you did not name is often the busier one.
  The readings are `tableStats` for rows and `most_common_vals`, `tableSize`
  for what a rewrite would move, `tableDetails` for the indexes and constraints
  that multiply it and for foreign keys in both directions, `currentActivity`
  and `currentLocks` for the oldest transaction and anything already queued on
  any of those objects, `replicationSlots` if it rewrites — then asks it to classify the change as metadata-only, a scan,
  or a full rewrite, and to state **the size at which its answer would flip**,
  so the reasoning can be checked against a different table later. It also
  insists that lock *level* settles nothing on its own: a strong lock held for
  a millisecond is safe and a weak one held for an hour may not be, and
  duration comes from the measurements.

  **`diagnose-slow-query` and `explain-and-fix` now choose the *kind* of fix
  rather than defaulting to DDL.** Both end by classifying the remedy as one of
  four — a query rewrite, a statistics fix, a configuration change, or DDL —
  and preferring them in that order, because that is the order of what they
  cost: an `ANALYZE` is free and instant, a rewrite costs a deploy and nothing
  in the database, configuration changes the behaviour of every other query
  too, and an index is a write cost paid by every `INSERT` and `UPDATE` for as
  long as it exists to buy speed for one read pattern. They must say why the
  cheaper options were rejected rather than passing over them.

  They then verify what can be verified, which is an asymmetry this server can
  actually exploit: a rewrite is checkable on the spot — call `explainQuery` on
  the rewritten statement and show the plan changed — while an index's benefit
  is a prediction, and must be presented as one. Index creation is handed to
  `plan-schema-change`, because whether an index is *correct* and whether it is
  *safe to build on this server* are different questions.

  **Every prompt whose core tools are privilege-gated now opens by calling
  `checkPrivileges`.** A restricted role gets null statistics rather than an
  error, so a plan built on tools that will not answer looks like it worked.

- **Completions** (`completion/complete`) for the `conn`, `schema` and `table`
  variables that appear in prompt arguments and resource templates, backed by
  the queries that already exist. A completion is a convenience, so an argument
  it does not know or a catalog it cannot read returns an empty list rather
  than an error.

- The `resources`, `prompts` and `completions` capabilities are declared in
  both `initialize` and `server/discover`, so a modern client does not see a
  smaller server than a legacy one.

### New tools

- **`evaluateIndex`** — plan a statement as if the indexes were different,
  using [hypopg](https://github.com/HypoPG/hypopg). `create` takes
  `CREATE INDEX` statements to plan against without building them; `hide` takes
  the names of existing indexes to plan *without*, which is how to ask whether
  an index is safe to drop — the question `duplicateIndexes` raises and cannot
  settle, since neither redundancy nor a zero `idx_scan` proves the planner
  would not miss it.

  Nothing is built, no lock is taken, no catalog row is written, and the
  statement is never executed: hypopg cannot serve `EXPLAIN ANALYZE`, so this
  is plan-only and strictly safer than `explainQuery` with `analyze`. It
  returns the plan and total cost before and after, and **for each index
  whether the planner actually used it** — a proposed index the planner ignores
  is the common case, and a cost figure alone hides it.

  Measured on hypopg 1.4.3: a candidate index on a 200k-row table moved the
  plan from a Seq Scan at cost 4970 to a Bitmap Index Scan at 2709; hiding a
  primary key moved it the other way, 8.44 → 4970, which is the answer to "can
  I drop this".

  **The reset bracket is not optional, and this is the finding worth carrying
  forward.** A hypothetical index lives in backend-local memory for the whole
  session and is cleared by none of the things that would be expected to clear
  it — measured: not `ROLLBACK`, not a new transaction, and **not `DISCARD
  ALL`**, PgBouncer's default `server_reset_query`. Only `hypopg_reset()`
  removes it. Behind a transaction-mode pooler that would leave one caller's
  hypothetical index on the backend, silently reshaping the next caller's
  plans — and the pooler cannot be asked to help: in transaction mode it runs
  no reset query at all unless `server_reset_query_always` is set, and forcing
  that on does not clear hypopg either, because `DISCARD ALL` does not.
  Measured end to end through a real PgBouncer, both ways. So `hypopg_reset()` runs on the way in, protecting this
  call from whatever a previous one left, and again on the way out through a
  scope guard that survives an exception. A test asserts a second call's
  baseline is unchanged — it can only fail behind the pooler, and passes
  trivially without one.

  `hide` needs hypopg 1.4.0+ and is gated on the extension version, not the
  server version — the same treatment `pg_buffercache` needed, for the same
  reason: the two move independently.

- **`checkPrivileges`** — which tools the current role can actually use on this
  connection, and how the rest fall short.

  Most of this server works for any role that can connect, because the catalog
  is world-readable. Measured on PostgreSQL 18: a bare `LOGIN` role with no
  grants runs **47 of 58 tools** at full fidelity; the monitoring role takes
  that to **54**; the three that remain are exactly the three that read row
  data (`tableStats` histograms, `checkKey`, `explainQuery`).

  The reason to ask once rather than discover it tool by tool is that the
  discovery is misleading. `tableStats` on a role without `SELECT` returns
  every column present with null statistics — byte-for-byte what a table that
  was never analyzed looks like. `statementStats` returns rows whose query text
  is `<insufficient privilege>`, with no count of what was hidden.

  Three deliberate choices in the payload:

  - **Tools absent from both lists are fully available**, and `available` is a
    count rather than a list. Enumerating 53 working tool names would be most
    of the payload, and the exceptions are the answer.
  - **No role memberships and no `GRANT` statements.** Which predefined role
    gates a tool is PostgreSQL's business; the caller needs to know what works.
    And emitting DDL would contradict what this server tells every client about
    itself — plans verbatim, no heuristics, no generated DDL. Naming a grant in
    the hint of a tool that *failed* is diagnosis; listing grants beside every
    unavailable tool is a standing recommendation to escalate privilege, which
    is not this server's to make.
  - **`denied` means the tool cannot answer at all; `degraded` means it answers
    less.** The three row-data tools are always `degraded`, never `denied`,
    because privilege there is per object — a role without blanket read access
    may still hold `SELECT` on some tables and not others. Reporting them as
    unavailable would be as wrong as reporting them as available.

  It distinguishes an absent extension from a missing privilege, because those
  send an operator to different fixes — the same defect the `42501` handling
  corrected in 3.2.0.

- **`tableStats`** and **`listTableStats`** — the readings, from
  `pg_stat_user_tables`, `pg_stats` and the `pg_class` counters that ANALYZE
  maintains. No relation is opened and no file is measured, so they cost a
  catalog scan and nothing else. `tableStats` adds the per-column histograms
  and per-index scan counts; `listTableStats` omits the histograms, because
  `default_statistics_target` sample values for every column of every table in
  a schema is a payload nobody asked for.

  Both are `{per_database: true, per_server: true, primary_authoritative:
  true}`: scan counts and dead tuples are each server's own, and vacuum only
  runs on a primary. This is the half of the split that is worth sweeping, and
  the reason to do it.

- **`tableSize`** and **`listTableSizes`** — measured size, and the only place
  a measured number now comes from. `tableSize` reports the main fork, table
  size including TOAST and the free space and visibility maps, index size,
  grand total, the TOAST relation, and each index individually;
  `listTableSizes` reports table, index and total size for every relation in a
  schema.

  These are separate tools rather than a flag because of what they cost, and
  the cost is not what it looks like. `pg_table_size()` and its relatives are
  not physical reads — they `stat()` one file per 1 GB segment and read no
  blocks. What makes them worth gating is the lock: each opens the relation
  with `AccessShareLock`, so a table being rewritten by an `ALTER TABLE` holds
  the call behind `AccessExclusiveLock` until `statement_timeout_ms` fires.
  Across a schema that means the call stalls on precisely the table an
  incident is about. A separate tool puts that warning in the description the
  model reads while *choosing* a tool, rather than in an argument it reads
  after having already chosen.

  Both are `per_server: false`: the same files are measured on every member of
  a replication group.

### Changed

- **Every session now ends in `ROLLBACK`, explicitly.** `pqxx::work` already
  aborted an uncommitted transaction on destruction, so no behaviour changed —
  but nothing here ever commits, every statement runs under
  `SET TRANSACTION READ ONLY`, and stating it makes "the server is as you found
  it" provable rather than incidental. A test asserts it across a spread of
  tools.

  One thing rollback does *not* undo, worth naming because the assumption is
  natural: backend-local state set by an extension. A hypopg hypothetical index
  survives `ROLLBACK`, the next transaction, and `DISCARD ALL`, which is why
  `evaluateIndex` resets it explicitly.


- **The sweep classification is corrected, which is the point of the
  release.** `listTables`, `searchTables` and `tableDetails` are now
  `{per_database: true, per_server: false, primary_authoritative: false}` and
  no longer advertise `replication_group` or `role`; a sweep across replicas
  is refused with the same reasoning `tableBloat` has always used. `tableSize`
  and `listTableSizes` join them. `tableStats` and `listTableStats` take their
  place on the per-server row.

- **`tableDetails` is no longer version-conditional.** Every field it dropped
  on PostgreSQL 14 and 15 was a statistic, so the `server_version()` probe and
  the string-erase fixup that removed `last_idx_scan` from the finished query
  are both gone. The gate lives in `tableStats` now, and is built by
  concatenation rather than by erasing a literal — the old approach failed
  silently if the literal ever drifted from the query.

- `tableBloat`'s description pointed at "the ANALYZE-time estimates in
  `listTables`/`tableDetails`"; it now points at `listTableStats`/`tableStats`.

- `tableIOStats` and `tableStats` now cross-reference each other. They sit
  next to each other alphabetically and report different views —
  `pg_statio_all_tables` against `pg_stat_user_tables` — so each description
  now says what the other one answers.

- Tool count is 58, up from 52. The server now also serves 9 resource kinds,
  5 resource templates and 8 prompts.

## 3.2.1 (2026-08-21)

Three fixes to behaviour shipped in 3.2.0 and earlier. No payload shape
changed and no configuration file needs an edit, but one default did change:
every call now runs under a `statement_timeout`.

### Fixed

- **A fan-out sweep did not bound the connection that did its work.** 3.2.0
  added `with_connect_timeout()` so that "one unreachable host must cost
  seconds, not whatever the kernel's TCP timeout happens to be", and applied it
  in `verifyTopology` and in the optional role probe. The connection each
  member's tool actually opened went through `active_cfg()` instead, which
  returns the configured conninfo unchanged — and nothing supplies a default
  `connect_timeout`.

  A member that refuses a connection fails immediately, so this was invisible
  in testing; a member that *drops* the packets — a decommissioned database, a
  security group that changed, a host that went away — cost the kernel's
  SYN-retry budget instead. On a Linux default of six retries that is about
  two minutes, and sweeps are sequential, so each such member added two minutes
  to the whole call. A handful of stale sections in a connections file was the
  difference between a sweep that took seconds and one that took a quarter of
  an hour.

  There was an inversion in it worth naming: a sweep narrowed with
  `role: "primary"` was fast, because the role probe *was* bounded and skipped
  the member before its tool ever ran, while the same sweep without a role
  filter was slow. Fast when narrowed and slow when not is the opposite of what
  anyone would predict, which is part of why this took a while to see.

  The sweep now swaps in a bounded copy of the member's config for the whole of
  that member's turn, so every connection it opens carries the 5-second limit,
  not just the probe. The regression test uses a TEST-NET-1 address
  (192.0.2.1, RFC 5737) rather than a refused port, because a refused port
  never exercises a timeout at all: against the previous code that test takes
  134 seconds and fails, against this one it takes 5 and passes.

- **Only `explainQuery` bounded how long a statement could run.** `Session`
  took a `timeout_ms` defaulting to 0 and issued `SET LOCAL statement_timeout`
  only when it was positive, and `explainQuery` was the sole caller that passed
  one. Every other operation — all fifty-odd — ran with no upper bound.

  For a catalog read that is academic. It is not academic for the three whose
  cost scales with the server rather than with the query: `tableBloat` calls
  `pgstattuple()`, a full sequential scan of the table (and `pgstattuple_approx()`,
  the default, still reads every page the visibility map does not mark
  all-visible, which on a write-heavy table is most of them); `indexBloat` reads
  the whole index for btree and hash; and `bufferCacheContents` scans every
  buffer in `shared_buffers`. On the machines this server is written for, those
  are the calls that run for ten minutes, and nothing stopped them.

  Nor could an operator stop them. `statement_timeout` is a GUC rather than a
  libpq keyword, so it cannot go in a conninfo, and the `options` keyword that
  could carry it is rejected at parse time — with a message that said pg-licht
  "sets what it needs per transaction (BEGIN READ ONLY, SET LOCAL
  statement_timeout) instead". The first half was true and the second was not,
  so the message blocked the only available workaround by pointing at a setting
  that was not being applied.

  Every transaction now sets `statement_timeout`, defaulting to 120000ms and
  configurable per connection with `statement_timeout_ms` (0 restores the old
  unbounded behaviour), or with `PG_LICHT_STATEMENT_TIMEOUT_MS` on the single
  `DATABASE_URL` path. It is transaction-scoped for the same reason the
  read-only guard is: that is the scope a transaction-mode pooler preserves.
  The `options` message now names the key that actually works.

  A statement that reaches the ceiling is reported as the ceiling being
  reached, with the value and the key to raise it, rather than as an execution
  error — the two call for opposite responses and PostgreSQL's own message does
  not distinguish them for the caller. Testing `sqlstate() == "57014"` is
  reliable here, unlike the `42501` comparison fixed in 3.2.0: libpqxx has no
  dedicated class for a cancelled statement, so it arrives as a plain
  `pqxx::sql_error` that does carry its code. Verified against libpqxx 7.10,
  the same version that leaves it empty on `insufficient_privilege`.

- **`bufferCacheContents` measured every cached relation to return twenty.**
  `pg_relation_size()` was called inside the CTE that resolved relation names,
  so it ran once per relation holding a buffer and the `LIMIT` applied after.
  Each call is a `stat()`. On a 16GB `shared_buffers` instance with 12,417
  relations, that was 9,762 syscalls to produce 20 rows; on network storage
  with a cold dentry cache it is the dominant cost of the tool. The limit is
  now applied before any fork is measured. Measured at 513ms → 464ms locally,
  where `stat()` is nearly free; the gap widens with the storage.

  The join to `pg_class` was left alone deliberately. It reads
  `pg_relation_filenode(c.oid)`, which cannot use
  `pg_class_tblspc_relfilenode_index`, but a mapped catalog carries
  `relfilenode = 0` in `pg_class` and can only be resolved through that
  function — so an index-friendly predicate would drop exactly the relations
  that are most heavily cached. Measured at ~25ms of 513ms, it is not where the
  time goes.

### Known limitations

- The cost of `bufferCacheContents` and `bufferCacheSummary` is O(`shared_buffers`)
  and independent of any argument: `pg_buffercache` materialises one row per
  buffer before any `WHERE` clause is evaluated, so narrowing the question does
  not narrow the scan. Measured at 2.1M buffers: 465ms for the contents,
  5ms for the summary, which uses `pg_buffercache_summary()` and never
  materialises rows. Prefer the summary for routine checks.

## 3.2.0 (2026-08-20)

No existing tool's payload shape changed, and a legacy protocol response is
byte-identical to 3.1.1. An existing configuration file and an existing client
config keep working with no edit.

### Added

- **Connection topology: `instance`, `replication_group` and `group`.** Three
  optional keys per section in the connections file say how the configured
  databases relate. An `instance` is one postmaster, whose databases share
  `shared_buffers`, WAL, autovacuum workers and disk. A `replication_group` is a
  primary and its replicas: the same data on different servers, each keeping its
  own statistics counters. A `group` is an arbitrary operator label and implies
  nothing, which is what makes it the way to ask about "dev" without knowing any
  connection name.

  There is no `cluster` key, deliberately. PostgreSQL's glossary uses that word
  for the first sense — the databases one instance manages — and RDS and Aurora
  use it for the second, so it means opposite things to the two people most
  likely to read the file.

  An `instance` is inferred when two sections share a host *and* port, and
  reported as `inferred` rather than `declared`: behind a pooler one endpoint can
  front several instances, so it groups output without licensing a claim about
  shared memory.

- **Fan-out.** Passing `instance`, `replication_group` or `group` in place of
  `connection` runs the tool once per member and returns one result per member,
  in configuration file order. A member that cannot be reached is reported in
  place rather than failing the sweep — a partial answer during an incident beats
  an exception.

  Which of the three a tool accepts depends on where its answer actually varies,
  and its input schema now says which. `currentActivity`, `currentLocks`,
  `statementStats` and the buffer cache tools report the whole instance from any
  one of its databases, so sweeping them across an instance would repeat one
  answer and is refused with `-32602`. Catalogs and `tableBloat` are
  byte-identical across a replication group for the mirror-image reason.

  `duplicateIndexes`, `indexBloat` and `tableIOStats` are the case the feature
  exists for: they carry `idx_scan`, which is each server's own. An index that
  reads as unused on the primary may be carrying a replica's entire reporting
  workload, and only that replica's scan counts show it.

- **Role is observed, never configured.** Every connection reports `primary` or
  `replica` from `pg_is_in_recovery()`, read on connect and never cached —
  failover swaps it, and failover is exactly when this server gets used. The
  probe rides along on the round trip the read-only guard already spends, so it
  costs nothing. `role: "primary"` or `role: "replica"` narrows a sweep; two
  primaries are reported as split brain with none chosen, and no primary at all
  is stated as a finding rather than returned as an empty result.

- **`listTopology`** — every instance, replication group and group with its
  members, plus the connections carrying no label. Reads the configuration file
  and opens no connection.

- **`verifyTopology`** — connects to each configured connection and checks the
  declared topology against what the servers report. A system identifier names a
  replication *lineage*, not a postmaster: a physical replica carries its
  primary's value forever, so the identifier is read alongside the endpoint. Same
  identifier and endpoint is one instance; same identifier on different hosts is
  a replication group. Reports contradicted declarations, connections that share
  an identifier without being declared together, and split brain. Logical
  replication is reported as "cannot verify" rather than as a mismatch.

- **`bufferCacheSummary`** and **`bufferCacheContents`** — `pg_buffercache`.
  `tableIOStats` counts only what `shared_buffers` served, so a miss there may
  still have come from the OS page cache at RAM speed; these are the only
  in-core view of that split. `bufferCacheContents` aggregates per relation and
  fork, never raw per-buffer rows. Requires `pg_monitor`; the summary needs
  extension version 1.4, gated on the *extension* version rather than the server
  version. 1.4 ships with PostgreSQL 16, so on 14 and 15 the summary says there
  is no 1.4 to update to and points at `bufferCacheContents`, which reads the
  view and works on every supported version.

- **Host capacity may be declared once per instance.** A reserved
  `[instance:<name>]` section carries the four host capacity keys and its members
  inherit them; an explicit per-connection value still wins. Inheritance follows
  `instance` only, never `replication_group`: replicas routinely run on smaller
  machines, and inheriting the primary's RAM would give every replica a
  confidently wrong `shared_buffers` ratio.

- **Dual-era MCP.** The stateless `2026-07-28` revision is served alongside the
  handshake-based ones. The era is discriminated on the presence of
  `params._meta["io.modelcontextprotocol/protocolVersion"]` specifically, never on
  `_meta` itself — `progressToken` has lived in `_meta` since the legacy
  revisions, and keying on `_meta` would reject a working 3.1.1 client for using
  a legacy feature correctly. Adds `server/discover`, `resultType` and
  `io.modelcontextprotocol/serverInfo` on modern results, `-32022` for an
  unsupported version and `-32602` for a modern request missing a required
  `_meta` key. Legacy responses are unchanged.

- **Tool annotations.** `readOnlyHint`, `destructiveHint` and `openWorldHint` on
  every tool, honest because every statement runs inside `SET TRANSACTION READ
  ONLY`; `explainQuery` additionally carries `idempotentHint: false`, since with
  `analyze: true` it really executes. `annotations` and `title` are emitted only
  to a client that negotiated the revision defining them (`2025-03-26` and
  `2025-06-18`), so a `2024-11-05` client's `tools/list` is unchanged.

- **`initialize` honours `params.protocolVersion`.** 3.1.1 ignored it and always
  replied `2024-11-05`. The requested version is now echoed when it is one CI
  exercises, and otherwise falls back rather than claiming support for something
  untested.

### Fixed

- **Permission errors reported the raw exception instead of the missing grant.**
  `tableBloat` and `indexBloat` tested `e.sqlstate() == "42501"` to turn a denied
  `pgstattuple` call into the `GRANT pg_stat_scan_tables` hint, but libpqxx
  constructs `pqxx::insufficient_privilege` with an *empty* `sqlstate()` — verified
  against libpqxx 7.10, where `undefined_table` and `undefined_function` both
  carry their codes and `insufficient_privilege` does not. The comparison
  therefore never matched, the branch never ran, and a read-only role missing the
  grant got a raw exception. All four privilege branches now catch the exception
  by type, where the string is unreliable.

### Changed

- `listConnections` entries carry their `instance`, `replication_group` and
  `group` labels where configured. Additive; no existing field changed.
- Tool descriptions state where each tool's answer varies, since the payload
  cannot say it and a sweep that silently repeated one answer would read as
  agreement between databases.

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
