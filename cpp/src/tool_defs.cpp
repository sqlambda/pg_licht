// Generated-adjacent, hand-maintained: the tool registry, moved out of
// server.h so that editing a tool definition does not recompile every translation
// unit that merely uses the server.
#include "server.h"

auto PostgresMCPServer::tool_defs() -> const std::vector<ToolDef>& {
    static const std::vector<ToolDef> defs = {
      {"listSchemas",
       "return schema list with basic summaries: table_count, up to 25 table names, and role grants per schema. tables_truncated says when a schema holds more than the names shown -- listTables is the tool that names every relation in one schema. pattern narrows to schemas whose name contains it: a schema-per-tenant database passes 100 KB of answer at around 140 schemas",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pattern", {{"type", "string"}, {"description", "only schemas whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.schemas(a.str("pattern")); }},
      {"listTables",
       "return the structure of every table, view and materialised view in a schema: kind, comment, storage options, columns and their per-column index counts, index count and constraint count. Structure only -- it changes when someone issues DDL and not otherwise. For row counts, scan counters, dead tuples and vacuum times call listTableStats; for measured on-disk sizes call listTableSizes",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"pattern", {{"type", "string"}, {"description", "only relations whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed. On a large schema the whole list is more than a model should read at once"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.tables(a.str("schema", "public"), a.str("pattern")); }},
      {"tableDetails",
       "return the structure of one table: columns with types, defaults, storage and compression, primary key, indexes with their definition and whether each is valid, constraints, foreign keys, inbound foreign keys (referenced_by), triggers with their enabled state, rules, row-level security, policies and privileges. Two of those say when an object is not doing what its definition suggests: an index with valid false is what a failed CREATE INDEX CONCURRENTLY leaves behind -- it occupies disk and is maintained on every write while the planner never uses it -- and a trigger with enabled 'disabled' still has its full definition, since ALTER TABLE ... DISABLE TRIGGER changes no text. 'replica' and 'always' are the session_replication_role states. Structure only -- it changes when someone issues DDL and not otherwise, and it returns no sample column values. For row counts, scan counters, dead tuples, vacuum times and the pg_stats column histograms call tableStats; for measured on-disk sizes call tableSize",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"table", {{"type", "string"}}},
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"table", "schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.table(a.str("schema", "public"), a.str("table", "")); }},
      {"searchTables",
       "find tables across every non-system schema by full-text search over table names, comments, column names and comments, enum labels and grantee names, and return their structure. Structure only -- there is no statistics counterpart, because a text search is how you find a table, not how you read a counter: name a match to tableStats or tableSize for those",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"web_search", {{"type", "string"}, {"description", "full-text search, not a substring: websearch_to_tsquery syntax in English, so words are stemmed (users finds user), \"a phrase\" in double quotes must match in order, or between words means either, and -word excludes. A fragment of a word does not match. Matched against table and schema names split into words at underscores and capitals, the table's comment, its columns' names and comments, the grantee roles, and the names, comments and values of enum types its columns use. To match part of a table name, use listTables with pattern"}}}
   		  }},
   		{"required", {"web_search"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.search(a.str("web_search", "")); }},
      {"evaluateIndex",
       "plan a statement as if the indexes were different, using hypopg. 'create' takes CREATE INDEX statements to plan against without building them; 'hide' takes the names of existing indexes to plan without, which is how to ask whether an index is safe to drop. Nothing is built, no lock is taken and no catalog row is written, and the statement is never executed -- hypopg cannot serve EXPLAIN ANALYZE, so this is plan-only and safer than explainQuery with analyze. Returns the plan and total cost before and after, each carrying a Settings block naming the settings that differ from the built-in default -- both halves are planned in THIS server's session, so a per-role work_mem elsewhere makes both costs answer a different question than the one asked; compare against hostCapacity.overrides. Also reports, for each index, whether the planner actually used it, which is the answer that matters: a proposed index the planner ignores is the common case and a cost figure alone hides it. The cost is the planner's estimate, not a measurement. For a statement recovered from pg_stat_statements, call explainQuery with its queryid first and pass the sql it echoes back. Reports a clear error with setup instructions if hypopg is not installed",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"sql", {{"type", "string"}}},
   		    {"create", {{"type", "array"}, {"items", {{"type", "string"}}},
   		                {"description", "CREATE INDEX statements to plan against"}}},
   		    {"hide", {{"type", "array"}, {"items", {{"type", "string"}}},
   		              {"description", "names of existing indexes to plan without"}}},
   		    {"settings", {{"type", "object"},
   		                  {"description", "planner settings to apply for both halves of the comparison, e.g. {\"work_mem\": \"512MB\"}. Applied with set_config(is_local) so they revert with the transaction. Allowlisted; an unknown name is refused and nothing is planned"}}},
   		    {"plan_as_role", {{"type", "string"},
   		                      {"description", "compare under what this role carries in pg_db_role_setting. Both the before and the after plan use it, which is the point: an environment that does not match production makes both costs answer a different question"}}}
   		  }},
   		{"required", {"sql"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         json settings = a.contains("settings") ? a["settings"] : json::object();
         return s.evaluate_index(a.str("sql", ""), a.arr("create"), a.arr("hide"),
                                 settings, a.str("plan_as_role")); }},
      {"predicateStats",
       "return the WHERE and JOIN predicates pg_qualstats has sampled in this database, one entry per relation, column, operator and way of evaluating it: how often it ran, how many rows it was evaluated against, how many it filtered out, and how far the planner's selectivity estimate was from what happened. evaluated_as \"filter\" is a predicate applied to rows after they were fetched -- a high filtered_pct there is rows read only to be thrown away, which is what an index would avoid; \"index\" means an index already serves it. mean_err_estimate_ratio is the factor by which the planner's row estimate missed -- 166 when 60 rows were expected and 10,000 arrived -- and 0 when it did not miss; mean_err_estimate_num is the same miss in rows. A large factor is the planner misjudging selectivity, which ANALYZE, a larger statistics target or extended statistics (listExtendedStatistics) fixes and an index does not. NEVER returns a constant: a predicate reads table.column op ?, and pg_qualstats' constvalue column and example-query functions are not read. query_ids joins to statementStats, at most 10 per predicate with queries giving the full count. pg_qualstats samples one query in pg_qualstats.sample_rate (by default 1/max_connections), so counts are samples rather than totals. Requires pg_qualstats in shared_preload_libraries. Without it the extension does not fail -- it silently reports only the calling session's own predicates -- so this tool detects that from the extension's own settings, which any role can read, and refuses rather than return an empty answer that looks like a real one",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"limit", {{"type", "integer"}, {"description", "how many predicates to return. Defaults to 20, at most 500"}}},
   		    {"query_id", {{"type", "string"}, {"description", "only predicates of this statement, as a decimal string from statementStats"}}},
   		    {"order_by", {{"type", "string"},
   		                  {"description", "ranking: filtered (default, rows removed), execution_count, occurrences, or err_estimate_ratio (the planner's worst selectivity misestimate)"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.predicate_stats(a.num("limit", 20), a.str("query_id"), a.str("order_by")); }},
      {"suggestIndexes",
       "return index suggestions from pg_qualstats' own index advisor for this database: each is a complete CREATE INDEX statement with the query_ids of the statements it would serve, plus not_indexable, the predicates that filter heavily but use an operator no access method supports, which an index cannot fix. Nothing is built: the advisor only reads pg_qualstats. A suggestion is a hypothesis, not a result -- pass its ddl to evaluateIndex's create, with a statement from explainQuery or statementStats for one of its query_ids, to see whether the planner would actually use it. On PostgreSQL 14 and 15 a statement recovered from pg_stat_statements keeps its $n placeholders, which only 16's EXPLAIN (GENERIC_PLAN) can plan, so substitute representative values before passing it. The advisor considers filter predicates averaging at least min_filter rows removed per execution and removing at least min_selectivity percent of the rows they see; lower both on a small or quiet database. Constants are never returned, as with predicateStats. Requires pg_qualstats 2.1 or later, the first whose advisor names the statements each suggestion serves, in shared_preload_libraries, and refuses rather than advise from an empty sample when it is installed without it",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"min_filter", {{"type", "integer"}, {"description", "average rows a predicate must remove per execution to be considered. Defaults to 1000"}}},
   		    {"min_selectivity", {{"type", "integer"}, {"description", "percentage of evaluated rows a predicate must remove to be considered, 0 to 100. Defaults to 30"}}},
   		    {"forbidden_am", {{"type", "array"}, {"items", {{"type", "string"}}},
   		                      {"description", "access methods never to suggest, e.g. [\"hash\"]"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.suggest_indexes(a.bignum("min_filter", 1000), a.bignum("min_selectivity", 30),
                                  a.arr("forbidden_am")); }},
      {"checkPrivileges",
       "report which tools the current role can actually use on this connection, and how the rest fall short. Most of this server works for any role that can connect, because the catalog is world-readable; what varies is the monitoring extras and whether the role can read table data. Call this first when working against an unfamiliar connection or a restricted role -- the alternative is discovering the limits tool by tool, and a privilege-filtered answer is easy to mistake for an empty one. Names no role memberships and no GRANT statements: what a caller needs is which tools work. This is about THIS server's operations for the CONNECTING role, and is not an object permission check -- for whether some other role may read a given table, view or function, and which rows row-level security then leaves it, use the check-role-access prompt. Tools absent from both lists are fully available",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.check_privileges(); }},
      {"tableStats",
       "return the statistics PostgreSQL keeps for one table: estimated row count, seq_scan and idx_scan counts, live and dead tuples, rows modified since the last analyze, rows inserted since the last vacuum, the manual and automatic vacuum and analyze times as four separate fields (last_vacuum and last_analyze are the manual ones, exactly as in pg_stat_user_tables -- a recent last_vacuum beside a null last_autovacuum means the table is being kept alive by hand and autovacuum is not reaching it), per-index scan counts, and the per-column pg_stats histograms (null_frac, avg_width, n_distinct, physical order correlation, most_common_vals and their frequencies, and three points off the histogram -- histogram_bounds gives low, mid and high, the observed extremes and median of the distribution, which are real values that occur in the column and so are usable directly as parameters to re-plan a statement with. It is three points rather than the whole array because the array is statistics_target+1 entries wide, 101 by default; for the whole distribution of one column call columnHistogram. Null when the column has no histogram, meaning every value is in the MCV list or the column was never analyzed). IMPORTANT: pg_stats returns no row at all for a table whose row-level security is active for the connecting role, so on such a table every per-column statistic here is null and looks exactly like a table nobody has analyzed -- stats_hidden_by_rls says which it is, and the analyze timestamps beside it prove the statistics exist. Reads the catalog and the statistics collector only -- no relation is opened and no file is measured. counters_since is when this database's statistics were last reset; seq_scan, idx_scan and the tuple counters cover only the period since, so a zero means 'not since then' rather than 'never'. size_estimate is relpages*block_size (the server's BLCKSZ, 8192 unless it was built otherwise) and is only as fresh as estimated_from says: for a measured size call tableSize. Note that most_common_vals and histogram_bounds both contain literal values sampled from the column. Not to be confused with tableIOStats, which reports pg_statio_all_tables -- whether reads came from the buffer cache or the disk",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"table", {{"type", "string"}}},
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"table", "schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.table_stats(a.str("schema", "public"), a.str("table", "")); }},
      {"roleDependencies",
       "return what depends on one role, cluster-wide, from pg_shdepend. checkRoleAccess answers whether a role may USE an object; this answers the inverse, which is the whole of \"role cannot be dropped because some objects depend on it\" -- a message that reports a count and names nothing. by_kind separates owner (which blocks DROP ROLE outright and is cleared by REASSIGN OWNED) from acl and policy (cleared by DROP OWNED), because that decides whether you reassign or hunt. Every row this database can resolve is named: tables, functions, schemas and types as schema.name, and every other class -- policies, default privileges, large objects, languages, foreign servers, subscriptions, parameters granted with GRANT SET ON PARAMETER -- by PostgreSQL's own pg_identify_object, so a policy reads as 'p on public.t'. IMPORTANT: pg_shdepend is shared across the cluster, so total and by_database cover EVERY database; but an object id is only resolvable from the database it lives in, so 'objects' names only those in this database and the shared catalogs. A row counted in another database is real and unnamed here -- connect there and ask again. Reporting only what this database can see would answer 'nothing depends on it' to somebody about to DROP the role",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"role", {{"type", "string"}, {"description", "role name to ask about"}}}
   		  }},
   		{"required", {"role"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.role_dependencies(a.str("role")); }},
      {"defaultPrivileges",
       "return ALTER DEFAULT PRIVILEGES entries (pg_default_acl): what grants the NEXT object of each type will get, per granting role and per schema. checkRoleAccess answers about the objects that exist; this is the only thing that answers about the ones that do not yet, and it is the standing cause of \"the new table is not readable and every old one is\" -- which presents as a broken grant and is a missing default. scope 'global' (defaclnamespace = 0) overrides the hard-wired defaults for that object type; scope 'schema' entries are ADDED to the global ones, so two entries for one type are cumulative rather than conflicting. Note that defaults apply only to objects created by the granting role, which is why granted_by is reported beside every entry",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}, {"description", "restrict to one schema's entries plus the global ones, which apply there too because per-schema entries are added to them; omit for every entry. A schema that does not exist is an error"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.default_privileges(a.str("schema")); }},
      {"largeObjects",
       "return how many large objects this database holds and who owns them (pg_largeobject_metadata). Large objects live in a catalog rather than in any user relation, so tableSize, listTableSizes and tableStats are all blind to them while diskUsage.databases counts their bytes -- the signature is \"the database grew and no table did\", which triage-disk-space could not otherwise resolve: it ranks tables, finds nothing, and stops. NO SIZES: the bytes are in pg_largeobject, which is not publicly readable and which the documentation directs callers away from, so a size sum is not reliably available here and is not invented. An unreferenced large object cannot be identified from the catalog alone either -- it is unreferenced only if no column holds its oid -- and lo_unlink on a live oid loses data, so confirm against the application before deleting",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.large_objects(); }},
      {"replicationStats",
       "return every WAL sender on this server (pg_stat_replication) with its state, sync_state, sent/write/flush/replay LSNs, the byte gap to replay (on a cascading standby, measured from the WAL it has received), and write_lag/flush_lag/replay_lag as SECONDS -- plus replication origin progress. Senders are keyed by application_name plus pid, because a walreceiver's default application_name is its cluster_name and Debian sets that per major rather than per host, so two standbys routinely share one; a name-only key would fold them into a single entry. This is the only source of replication lag as a TIME: replicationSlots reports what a slot RETAINS in bytes, and subscriptionStats cannot measure lag at all because neither of its LSN columns references the publisher. Serves physical standbys and logical subscribers alike. Two readings that are routinely misread: the view is security-restricted PER ROW rather than refused, so a role without pg_read_all_stats sees the senders exist with many columns null, which looks like an idle replica rather than a permission answer; and the lag columns revert to NULL a short time after a standby has entirely caught up and WAL activity stops, so a null lag on an idle replica means caught up while a non-null one is the last measurement rather than the current state",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.replication_stats(); }},
      {"listPartitions",
       "return every partitioned table in a schema with its partitioning strategy (range/list/hash), the partition key, how many partitions it has, the combined estimated row count and size of every leaf partition at any depth (a sub-partitioned child holds nothing itself), whether a DEFAULT partition exists and how many rows it holds, and how many partitions are themselves partitioned. rows and size_estimate cover only the leaves that have been analyzed, and never_analyzed says how many leaf_partitions they could not see; both are null when no leaf has been measured. default_rows is null, not 0, when the default partition has never been analyzed: reltuples stays -1 until then even after rows arrive, and has_default says whether a default exists at all. Reads reltuples and relpages from the catalog, so no relation is opened and no lock is taken -- for measured sizes call listTableSizes on the schema the partitions live in. A growing default partition is the finding to look for: rows land there when they match no bound, so it is a missing partition that has not failed loudly yet. Name a parent to partitionDetails for the per-partition bounds and vacuum state",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.list_partitions(a.str("schema", "public")); }},
      {"partitionDetails",
       "return one partitioned table with every partition: its bound expression verbatim, whether it is the DEFAULT, whether it is itself partitioned, estimated rows and size (both null, not 0, for a partition never analyzed), and the per-partition live/dead tuples, scan counters and vacuum and analyze timestamps. Those statistics are the reason this exists: autovacuum runs per PARTITION, so a parent has no vacuum state of its own and ranking parents finds nothing while one child falls behind. Bounds are returned verbatim rather than parsed -- a bound carries whatever types the key columns have, and a misparsed boundary is worse than an unparsed one; for a RANGE parent, comparing the highest upper bound against now() is how to see that next period's partition was never created. counters_since says when the scan counters were last reset. Returns a clear error naming the relkind if the table exists but is not partitioned. At most limit partitions are returned, 100 by default, with partition_count and partitions_truncated beside them: on a table partitioned by day each partition costs about 400 bytes, so three years of them is ~430 KB. To find the partition falling behind, rank with order_by=dead_tuples or oldest_vacuum rather than reading them by name",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"table", {{"type", "string"}}},
   		    {"schema", {{"type", "string"}}},
   		    {"limit", {{"type", "integer"}, {"description", "at most this many partitions, 100 by default and at most 10000. partition_count and partitions_truncated say how many there are; the DEFAULT partition is always kept"}}},
   		    {"order_by", {{"type", "string"},
   		                  {"description", "which partitions are kept and in what order: name (default, the DEFAULT partition last), dead_tuples, mod_since_analyze, oldest_vacuum (never-vacuumed first), or size"}}}
   		  }},
   		{"required", {"table", "schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.partition_details(a.str("schema", "public"), a.str("table"),
                                    a.num("limit", 100), a.str("order_by")); }},
      {"listTableStats",
       "return the statistics PostgreSQL keeps for every table in a schema: estimated row count, seq_scan and idx_scan counts, live and dead tuples, rows modified since the last analyze, rows inserted since the last vacuum, and the manual and automatic vacuum and analyze times as four separate fields (last_vacuum and last_analyze are the manual ones, exactly as in pg_stat_user_tables -- a recent last_vacuum beside a null last_autovacuum means the table is being kept alive by hand and autovacuum is not reaching it). Reads the catalog and the statistics collector only -- no relation is opened and no file is measured. Carries no per-column histograms; name one table to tableStats for those. size_estimate is relpages*block_size (the server's BLCKSZ, 8192 unless it was built otherwise) and is only as fresh as estimated_from says: for measured sizes call listTableSizes",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"pattern", {{"type", "string"}, {"description", "only relations whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed. On a large schema the whole list is more than a model should read at once"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.list_table_stats(a.str("schema", "public"), a.str("pattern")); }},
      {"tableSize",
       "measure one table on disk: main fork, total table size including TOAST and the free space and visibility maps, index size, grand total, the TOAST relation and each index individually. COSTS MORE THAN IT LOOKS: these functions open the relation with AccessShareLock, so on a table an ALTER TABLE is rewriting the call waits behind AccessExclusiveLock until statement_timeout fires. Prefer size_estimate from tableStats, which is free, and call this when the estimate is too stale to act on. A partitioned table reports its own storage, which is zero -- measure the partitions, which partitionDetails names",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"table", {{"type", "string"}}},
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"table", "schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.table_size(a.str("schema", "public"), a.str("table", "")); }},
      {"listTableSizes",
       "measure every table in a schema on disk: table size, index size and grand total per relation. COSTS MORE THAN IT LOOKS, and more here than in tableSize: one relation is opened per table, each taking AccessShareLock, so a single table held under AccessExclusiveLock by an ALTER TABLE blocks the whole call rather than one row of it, and on a large schema this is thousands of file-metadata calls. Prefer size_estimate from listTableStats, which is free, and call this when the estimates are too stale to act on. Partitioned tables report their own storage, which is zero; listPartitions summarises them and partitionDetails names their partitions",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"pattern", {{"type", "string"}, {"description", "only relations whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed. On a large schema the whole list is more than a model should read at once"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.list_table_sizes(a.str("schema", "public"), a.str("pattern")); }},
      {"listFunctions",
       "return function and procedure list for a schema",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"pattern", {{"type", "string"}, {"description", "only functions whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed. On a large schema the whole list is more than a model should read at once"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.functions(a.str("schema", "public"), a.str("pattern")); }},
      {"functionDetails",
       "return detailed function or procedure info including source and trigger usage",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"function", {{"type", "string"}}}
   		  }},
   		{"required", {"schema", "function"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.function_detail(a.str("schema", "public"), a.str("function", "")); }},
      {"searchFunctions",
       "search functions and procedures by name, source, language, trigger name, or description. PostgreSQL's own functions in pg_catalog and information_schema are left out unless include_system is true or one of those schemas is named: they were 97-99% of a typical answer, and enough of them to bury the database's own functions",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"web_search", {{"type", "string"}, {"description", "full-text search, not a substring: websearch_to_tsquery syntax in English, so words are stemmed (users finds user), \"a phrase\" in double quotes must match in order, or between words means either, and -word excludes. A fragment of a word does not match. Matched against function names split into words at underscores and capitals, their source code and descriptions, and the names of triggers that call them -- plus the language name as a literal substring, so plpgsql is found by sql. To match part of a function name, use listFunctions with pattern"}}},
   		    {"schema", {{"type", "string"}, {"description", "only functions in this schema"}}},
   		    {"include_system", {{"type", "boolean"}, {"description", "also search pg_catalog and information_schema. Defaults to false"}}}
   		  }},
   		{"required", {"web_search"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.search_functions(a.str("web_search", ""), a.str("schema"),
                                   a.flag("include_system", false)); }},
      {"listEnums",
       "return enum type list for a schema with their values and descriptions",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.enums(a.str("schema", "public")); }},
      {"enumDetails",
       "return enum type details including values and which columns use it",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"enum", {{"type", "string"}}}
   		  }},
   		{"required", {"schema", "enum"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.enum_detail(a.str("schema", "public"), a.str("enum", "")); }},
      {"searchEnums",
       "search enum types by name, values, or description",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"web_search", {{"type", "string"}, {"description", "full-text search, not a substring: websearch_to_tsquery syntax in English, so words are stemmed (users finds user), \"a phrase\" in double quotes must match in order, or between words means either, and -word excludes. A fragment of a word does not match. Matched against enum type names split into words at underscores and capitals, their comments and their values"}}}
   		  }},
   		{"required", {"web_search"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.search_enums(a.str("web_search", "")); }},
      {"listTypes",
       "return composite type, domain, and range type list for a schema (excludes enums and implicit table/view row types); composites include their attribute list, domains include base type/nullability/default/constraints, ranges include subtype and the auto-generated multirange type name",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.types(a.str("schema", "public")); }},
      {"typeDetails",
       "return composite type, domain, or range type details including attributes/constraints/subtype and which columns use it",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"type", {{"type", "string"}}}
   		  }},
   		{"required", {"schema", "type"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.type_detail(a.str("schema", "public"), a.str("type", "")); }},
      {"listRoles",
       "return cluster-wide roles with kind (login/group), attributes (superuser, create_role, create_db, replication, bypass_rls, connection_limit, valid_until), and group memberships. Without 'pattern' the list is capped at 200, ordered so every role carrying a non-default attribute or a group membership comes first -- on a multi-tenant cluster with one login role per tenant the crowd is identical and the cap drops it rather than the superusers. Pass 'pattern' to search by name (case-insensitive substring) when the role you want is outside the cap",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pattern", {{"type", "string"}, {"description", "only roles whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.roles(a.str("pattern")); }},
      {"listForeignTables",
       "return foreign tables in a schema with their foreign server, FDW, options, and columns (does not expose user mapping credentials)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.foreign_tables(a.str("schema", "public")); }},
      {"listForeignServers",
       "return cluster-wide foreign servers with their FDW, owner, and options (host/port/dbname-style options only, never user mapping credentials)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.foreign_servers(); }},
      {"listTablespaces",
       "return cluster-wide tablespaces with owner, filesystem location, options, and description. The description comes from pg_shdescription: a tablespace is a shared object, so COMMENT ON TABLESPACE does not land in pg_description and obj_description() cannot see it",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.tablespaces(); }},
      {"listCollations",
       "return collations usable in the current database's encoding for a schema, with provider, locale settings, and determinism flag",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.collations(a.str("schema", "public")); }},
      {"listEventTriggers",
       "return cluster-wide event triggers with event type, tags, function, owner, enabled status, and description",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.event_triggers(); }},
      {"listPublications",
       "return logical replication publications with owner, all-tables flag, per-operation flags (insert/update/delete/truncate), table_count, up to 50 member table names, and on PostgreSQL 15+ the schemas published wholesale via FOR TABLES IN SCHEMA. That declaration is not cosmetic: a table created later in a published schema joins the publication by itself, while one added to a table-list publication does not -- so two publications with identical members today can behave differently tomorrow. tables_truncated says when a publication carries more than the names shown -- a publication FOR ALL TABLES resolves to every table in the database, so the member list is unbounded by construction and the count is the figure that scales",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.publications(); }},
      {"listSubscriptions",
       "return logical replication subscriptions for the current database with owner, enabled status, publications, slot name, and sync settings (never exposes the connection string, which may contain credentials). Structure only -- what CREATE SUBSCRIPTION declared. For whether the subscriber is actually keeping up call subscriptionStats",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.subscriptions(); }},
      {"diskUsage",
       "report what PostgreSQL is holding on disk without needing a shell on the server: WAL directory size and file count, the archive status backlog, temporary files currently on disk, log directory size, per-tablespace sizes and per-database sizes across the whole cluster. Answers \"what is filling the disk\" from SQL alone, which otherwise needs df. IT CANNOT SAY HOW MUCH ROOM IS LEFT: PostgreSQL exposes no function for total or free space, so this reports what is consuming space and how it divides, never the headroom. A climbing .ready count in archive_status is a failing archive_command retaining every segment it has not archived -- indistinguishable from an abandoned replication slot by size alone, and this is what tells them apart. Two different privilege gates, not one: the pg_ls_* sections need pg_monitor, and the tablespace and database sizes need CREATE on the tablespace or pg_read_all_stats (except the current database's default tablespace) and CONNECT or pg_read_all_stats respectively. A pg_monitor role clears both since it holds pg_read_all_stats; a bare role can get the cheap directory sections and be refused the expensive size ones, which is easy to misread as success. Each section is guarded independently, so one refusal returns an error in that key and leaves the rest answered rather than failing the call. Costs: the four directory sections are trivial, and the tablespace and database sizes are the whole cost -- they walk the directory tree and stat every segment file, so they scale with FILE COUNT rather than with bytes. Measured at 314 ms for the whole call against a cluster of roughly a terabyte, where a bare databaseSize is already 163 ms. Nothing is read, no relation is opened and no lock is taken -- this is metadata rather than I/O -- but it was measured with directory entries warm in the page cache, and a filling disk is exactly when they are not. statement_timeout bounds it, so the failure mode is a timeout that names itself",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.disk_usage(); }},
      {"columnHistogram",
       "return the whole value distribution of one column: the most common values with their frequencies, and the full histogram of everything else. tableStats carries three points off that histogram -- low, mid and high -- which is enough to pick parameters to re-plan with; this is the tool for when the shape of the distribution itself is the question. The two are complements rather than alternatives: ANALYZE puts the most frequent values in most_common_vals and builds the histogram only from what is LEFT, so a value in the MCV list never appears in the bounds however common it is, and reading either alone misdescribes the column. The bounds are equal-frequency, so consecutive entries delimit buckets holding roughly the same number of rows -- bounds bunched together are a dense region and a wide gap is a sparse one, which is what makes them usable as sample points across the distribution rather than one corner of it. statistics_target says why the histogram is the width it is and is the knob that changes it. COSTS NOTHING TO READ but RETURNS LITERAL COLUMN VALUES, more of them than any other operation here: the bounds and the MCVs are rows sampled out of the table. pg_stats filters on has_column_privilege, so a role without SELECT on the column gets nulls rather than data -- and it also drops the row entirely when row-level security is active on the table for this role, which is the same nulls for a completely different reason. stats_hidden_by_rls distinguishes them; the owner is exempt from RLS unless FORCE ROW LEVEL SECURITY is set, so asking as the owner is usually how to see the distribution",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"table", {{"type", "string"}}},
   		    {"column", {{"type", "string"}}}
   		  }},
   		{"required", {"schema", "table", "column"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.column_histogram(
           a.str("schema"),
           a.str("table"),
           a.str("column")); }},
      {"checkRoleAccess",
       "answer whether one role holds privileges on one table, view, sequence, function or procedure -- as PostgreSQL evaluates it, via has_table_privilege and its relatives, so role inheritance, grants to PUBLIC, ownership and superuser are all folded in the way the server folds them rather than reconstructed from ACLs. Needs no grant of its own: these functions and the catalog are world-readable, so any role that can connect may ask about any other. Returns schema USAGE and database CONNECT beside the object privileges, because a grant on the table is inert without them and the resulting error names the table. Where a table-level privilege is absent but individual columns carry it, the columns are listed. IMPORTANT: has_table_privilege does not consider row-level security, so a true here can still return no rows -- row_level_security carries whether RLS is on, whether this role is subject to it (the owner is exempt unless FORCE ROW LEVEL SECURITY), and every policy with whether it applies to this role. RLS enabled with no applicable permissive policy denies everything. Not to be confused with checkPrivileges, which reports which of THIS SERVER's operations the CONNECTING role can run",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"grantee", {{"type", "string"}, {"description", "the role to ask about; need not be the role this server connects as, and need not hold an actual GRANT -- ownership and superuser answer true too. Named grantee rather than role because role is reserved server-wide for narrowing a sweep to a primary or a replica"}}},
   		    {"schema", {{"type", "string"}}},
   		    {"object", {{"type", "string"}, {"description", "table, view, sequence, function or procedure name. A routine name reports every overload"}}}
   		  }},
   		{"required", {"grantee", "schema", "object"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.check_role_access(
           a.str("grantee"),
           a.str("schema"),
           a.str("object")); }},
      {"subscriptionStats",
       "return the runtime state of every logical replication subscription in this database: each worker with its type, pid, the relation it is syncing and how long since it last heard from the publisher; per-table sync state, with the tables that are not yet ready listed individually; and the apply and sync error counters plus the per-conflict-type counters. Answers whether a subscriber is keeping up and, if not, whether it is stuck syncing a table or failing to apply. Reports no byte lag, and this is definitional rather than a limitation to be revisited: received_lsn is the last WAL location RECEIVED and latest_end_lsn the last one REPORTED BACK to the publisher's WAL sender, so neither references the publisher's WAL position and no difference between them can express backlog. What it does express is feedback latency, bounded by the status-report interval rather than by lag -- which is why it reads zero with a subscriber far behind. Note also that received_lsn, latest_end_lsn and both last_msg_* timestamps are null for a parallel apply worker, so such a row is not broken. For lag in bytes call replicationSlots on the PUBLISHER and read retained_wal_bytes. When a table is still copying, its worker pid is a real backend pid: pass it to progressStats to get the byte count of that exact copy. Complements listSubscriptions, which is the structural half",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.subscription_stats(); }},
      {"listLanguages",
       "return procedural languages installed in the current database (e.g. plpgsql, plpython3u) with owner, trusted/procedural flags, handler function, and description",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.languages(); }},
      {"listExtendedStatistics",
       "return extended statistics objects (CREATE STATISTICS) for a schema with target table, columns, the statistics kinds declared (ndistinct, dependencies, mcv, expressions), and description -- plus whether they have actually been BUILT. pg_statistic_ext holds the definition and pg_statistic_ext_data holds the data, and an object that has never been ANALYZEd has the first and not the second: a catalog row, no statistics, and no effect on any plan. 'built' is false there and 'built_kinds' is empty, which is the difference between a fix that is in place and one that was declared and never finished -- the answer to the second is ANALYZE, not another CREATE STATISTICS. built_for_inherited says whether the data covers the parent alone, the whole inheritance tree, or both (PostgreSQL 15+, where stxdinherit became part of the key; null on 14, where the question cannot arise)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.extended_statistics(a.str("schema", "public")); }},
      {"listOperators",
       "return custom operators in a schema with left/right operand types, result type, and implementing function; mostly relevant for schemas using extensions with custom types (e.g. PostGIS)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.operators(a.str("schema", "public")); }},
      {"listOperatorClasses",
       "return operator classes in a schema with their index access method, input type, and default flag; describes what index types (btree/gist/gin/etc) a type supports",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.operator_classes(a.str("schema", "public")); }},
      {"listAccessMethods",
       "return index and table access methods available in the cluster (btree, gist, gin, heap, etc) with type and handler function",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.access_methods(); }},
      {"listCasts",
       "return type casts involving at least one user-defined type (excludes built-in-to-built-in casts) with source/target types, context (implicit/assignment/explicit), and method",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.casts(); }},
      {"listTextSearchConfigs",
       "return full-text search configurations for a schema with parser and the token-type-to-dictionary mapping",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.text_search_configs(a.str("schema", "public")); }},
      {"listSequences",
       "return sequence list for a schema with type, range, increment, cycle, cache, current value, and owning table.column (for SERIAL/IDENTITY columns)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"pattern", {{"type", "string"}, {"description", "only sequences whose name contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed. On a large schema the whole list is more than a model should read at once"}}}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.sequences(a.str("schema", "public"), a.str("pattern")); }},
      {"listExtensions",
       "return installed PostgreSQL extensions with version, schema, relocatable flag, and description",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.extensions(); }},
      {"databaseSize",
       "return the current database name and its total disk size",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.database_size(); }},
      {"serverSettings",
       "return PostgreSQL server settings (pg_settings) grouped by category, each with current value, unit, description, context, type, source, and pending_restart flag. By DEFAULT only settings that differ from their built-in default -- the set that describes THIS server rather than PostgreSQL, which is the same set EXPLAIN (SETTINGS) reports and for the same reason. A full dump is hundreds of settings and on a cluster with extensions it exceeds the client's payload limit, at which point the tool returns nothing and the forty settings that describe the machine are lost with the four hundred that describe the software. Pass all:true for everything, or 'pattern' to search by setting name or category (case-insensitive substring)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pattern", {{"type", "string"}, {"description", "only settings whose name or category contains this: a literal, case-insensitive substring, so _ and % match themselves and nothing is stemmed"}}},
   		    {"all", {{"type", "boolean"}, {"description", "include settings still at their built-in default; defaults to false"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.server_settings(a.str("pattern"), a.flag("all", false)); }},
      {"currentActivity",
       "return current server connections and running queries (pg_stat_activity) across all databases: pid, database, user, application_name, backend_type, state, wait event, query text, transaction and query duration, leader_pid for parallel workers, and the backend's xid and xmin. query_id is returned as a decimal string and is the join key to statementStats and explainQuery, so a statement seen running here can be looked up and planned. All filters are optional and combine; with none the whole view is returned, which on a busy server is mostly idle connections and internal processes",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pid", {
   			{"type", "integer"},
   			{"description", "a single backend, together with its parallel workers (any backend whose leader_pid is this pid)"}
   		      }},
   		    {"query_id", {
   			{"type", "string"},
   			{"description", "only backends running this query_id, as a decimal string; use it to find who is running a statement identified by statementStats. Requires compute_query_id to be enabled (the default 'auto' enables it when pg_stat_statements is loaded)"}
   		      }},
   		    {"min_duration_s", {
   			{"type", "number"},
   			{"description", "only backends whose current query has been running at least this many seconds. Plain idle backends are excluded, since their query_start dates a statement that already finished"}
   		      }},
   		    {"state", {
   			{"type", "string"},
   			{"description", "only backends in this pg_stat_activity state, e.g. \"active\" or \"idle in transaction\""}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         int pid = a.contains("pid") && a["pid"].is_number_integer()
           ? a["pid"].get<int>() : 0;
         std::string qid = a.contains("query_id") && a["query_id"].is_string()
           ? a["query_id"].get<std::string>() : "";
         double min_dur = a.contains("min_duration_s") && a["min_duration_s"].is_number()
           ? a["min_duration_s"].get<double>() : 0;
         std::string st = a.contains("state") && a["state"].is_string()
           ? a["state"].get<std::string>() : "";
         return s.activity(pid, qid, min_dur, st); }},
      {"currentLocks",
       "return current locks (pg_locks) joined with the holding backend's query and user, plus which pids are blocking each waiting lock; use to diagnose lock contention. Given a pid, returns that backend's locks together with every backend blocking it transitively, each tagged with chain_depth: 0 is the pid asked about, and the largest depth is the backend at the root of the pile-up, which is the one to look at first",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pid", {
   			{"type", "integer"},
   			{"description", "restrict to this backend and its transitive blockers, resolved through pg_blocking_pids. Omit for every lock in the cluster"}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         int pid = a.contains("pid") && a["pid"].is_number_integer()
           ? a["pid"].get<int>() : 0;
         return s.locks(pid); }},
      {"replicationSlots",
       "return replication slots with retained WAL bytes; a lagging or unused slot holds back WAL indefinitely and is a common cause of disk bloat incidents. A logical slot's consumer is a subscriber on another server: call subscriptionStats there to see whether it is stuck, and note that retained_wal_bytes here is the byte lag that a subscriber cannot measure for itself",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.replication_slots(); }},
      {"databaseStats",
       "return per-database statistics (pg_stat_database) for every database in the cluster: connections, commits/rollbacks, block hit ratio inputs, tuple counts, conflicts, deadlocks, temp file usage, and checksum failures",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.database_stats(); }},
      {"statementStats",
       "return tracked queries from pg_stat_statements under 'statements', with calls, timing, row counts, buffer usage, temporary block I/O and WAL volume, alongside an 'info' block from pg_stat_statements_info whose dealloc counter says whether entries are being evicted -- if it is climbing, this is not the slowest queries in the cluster but the slowest of those that survived eviction. query_id is a decimal string, ready to pass to explainQuery. Returns a clear error with setup instructions if the extension is not installed",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"limit", {{"type", "integer"}, {"description", "how many statements to return. Defaults to 20"}}},
   		    {"query_id", {
   			{"type", "string"},
   			{"description", "only statements with this queryid, as a decimal string; their query text is returned whole rather than truncated. pg_stat_statements keeps one entry per user and database, so a queryid can match more than one row"}
   		      }},
   		    {"order_by", {
   			{"type", "string"},
   			{"description", "ranking column: total_exec_time (default), mean_exec_time, max_exec_time, calls, rows, shared_blks_read, temp_blks_written, or wal_bytes. Ranking by total time buries a statement called twice at 40s under one called ten million times at 2ms; mean_exec_time is the other question"}
   		      }},
   		    {"min_calls", {
   			{"type", "integer"},
   			{"description", "ignore statements called fewer times than this, to keep one-off maintenance queries out of a mean_exec_time ranking"}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         int limit = a.contains("limit") ? a["limit"].get<int>() : 20;
         std::string qid;
         if (a.contains("query_id")) {
           if (a["query_id"].is_string()) qid = a["query_id"].get<std::string>();
           else if (a["query_id"].is_number_integer())
             qid = std::to_string(a["query_id"].get<long long>());
         }
         std::string ord = a.contains("order_by") && a["order_by"].is_string()
           ? a["order_by"].get<std::string>() : "";
         long long min_calls = a.contains("min_calls") && a["min_calls"].is_number_integer()
           ? a["min_calls"].get<long long>() : 0;
         return s.statement_stats(limit, qid, ord, min_calls); }},
      {"waitEventProfile",
       "return what this instance has spent its time waiting on, from pg_wait_sampling's accumulated profile: one entry per wait event (or per query_id and wait event with group_by=\"query\"), with its sample count, its percent of the samples counted (the idle ones below are not), and estimated_ms. currentActivity is a single sample of what is waiting now; this is the answer to what the server has been waiting on -- IO/DataFileRead dominating and LWLock/WALWrite dominating are opposite stories that look identical in a throughput figure. samples are collector ticks, every profile_period_ms per backend, so estimated_ms is backend-time summed over all backends rather than wall-clock time, and the counts accumulate from the last reset or server start: this server never resets them, so a window is the difference between two readings. Activity samples -- background processes idling in their main loops -- are excluded unless include_idle is true and counted in excluded_idle_samples. Client/ClientRead is kept, and it includes idle connections waiting for their next statement, so on a pooled server it measures connections times time as much as anything. An entry whose event_type and event are null is a backend sampled while not waiting at all -- running, which usually means on CPU -- and on a busy server it is often the largest entry. query_id is null for samples with no statement, and every query_id is 0 (so null) when pg_wait_sampling.profile_queries is off. Requires pg_wait_sampling in shared_preload_libraries, and says so as an error when it is installed without it; preloaded is null only when neither shared_preload_libraries nor the extension's own settings could tell",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"group_by", {{"type", "string"}, {"enum", {"event", "query"}},
   		                  {"description", "\"event\" (default) sums every backend and statement per wait event; \"query\" keeps query_id, to join to statementStats"}}},
   		    {"query_id", {{"type", "string"}, {"description", "only samples taken while this statement ran, as a decimal string from statementStats"}}},
   		    {"limit", {{"type", "integer"}, {"description", "how many entries to return, most-sampled first. Defaults to 30, at most 500"}}},
   		    {"include_idle", {{"type", "boolean"}, {"description", "keep the Activity samples of background processes idling in their main loops. Defaults to false"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.wait_event_profile(a.str("group_by"), a.str("query_id"),
                                     a.num("limit", 30), a.flag("include_idle", false)); }},
      {"statementKernelStats",
       "return what the operating system measured each tracked statement costing, from pg_stat_kcache: CPU time split into user and system, bytes actually read from and written to storage, page faults, and context switches, separately for planning and execution, per query_id. PostgreSQL's own counters cannot see this: a shared_blks_read in statementStats is a request to the kernel, answered from its page cache or from the device, and only exec.reads_bytes says which -- compare it with shared_blks_read times block_size, both returned here. A high nivcsws (involuntary context switches) is CPU contention; majflts is memory pressure reaching disk. Counters are cumulative from stats_since or the last reset. query_id joins to statementStats and explainQuery; for a role without pg_read_all_stats, pg_stat_statements hides the queryid of other roles' statements, so their calls, total_exec_ms and shared_blks_read are null while the kernel counters stay complete. Requires pg_stat_kcache in shared_preload_libraries after pg_stat_statements, at version 2.2 or later",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"limit", {{"type", "integer"}, {"description", "how many statements to return. Defaults to 20, at most 500"}}},
   		    {"query_id", {{"type", "string"}, {"description", "only this statement, as a decimal string from statementStats. One entry per user, database and nesting level, so it can match more than one row"}}},
   		    {"order_by", {{"type", "string"},
   		                  {"description", "ranking: exec_cpu_time (default, user plus system), plan_cpu_time, exec_reads, exec_writes, exec_majflts, or exec_nivcsws"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.statement_kernel_stats(a.num("limit", 20), a.str("query_id"),
                                         a.str("order_by")); }},
      {"wraparoundStatus",
       "return transaction id and multixact wraparound headroom: age(datfrozenxid) and age(datminmxid) for every database, the oldest tables by age(relfrozenxid) including TOAST tables (often the relation actually holding the horizon back), each xid and multixact age as a percentage of the effective freeze_max_age and of the limit at which the cluster stops accepting write transactions, plus the per-table freeze storage parameters and last vacuum times. That limit is 2144483647, not 2^31: PostgreSQL refuses new transaction ids three million short of wraparound, and warns in the log forty million short of it -- wraparound_warn_limit and xids_until_warn_limit carry the earlier one, which is the threshold an operator has usually already seen fire. Two alarms live in these numbers and only one is an emergency: past autovacuum_freeze_max_age PostgreSQL forces an anti-wraparound vacuum, which is loud maintenance working as designed, while approaching the wraparound limit ends in the server refusing writes -- xid_percent_of_freeze_max_age against xid_percent_of_wraparound_limit tells them apart and xids_until_wraparound_limit is the budget. Recovery does NOT need single-user mode: the documentation says plainly that stopping the postmaster is neither necessary nor desirable, and the fix is a plain database-wide VACUUM in normal multi-user mode. Not VACUUM FULL, which needs an xid of its own and will fail; not VACUUM FREEZE, which does more than the minimum needed to restore service. Past vacuum_failsafe_age VACUUM takes extraordinary measures -- and it is any VACUUM, not only autovacuum: the cost-based delay stops being applied, non-essential work such as index vacuuming is bypassed, AND the Buffer Access Strategy is disabled so the vacuum is free to use all of shared_buffers. That third effect is why the cache looks wrecked during one, which bufferCacheSummary will show and nothing else explains. Default 1.6 billion, silently raised to no less than 105% of autovacuum_freeze_max_age. Rank tables by relfrozenxid age rather than size, since the oldest object sets the horizon however small it is, and read toast_for -- a TOAST table is frequently the offender and carries nobody's name. If the age will not fall, vacuum is not the problem: nothing can be frozen past the oldest transaction still visible to something, so more workers and a manual VACUUM FREEZE achieve nothing while the horizon is held. Four things hold it -- a replication slot (replicationSlots), a long-running transaction (currentActivity.backend_xmin), a standby with hot_standby_feedback, and a prepared transaction, which is invisible in pg_stat_activity and not read by this server: query pg_prepared_xacts directly when nothing else explains it",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {
   			{"type", "string"},
   			{"description", "restrict the table list to one schema; omit to cover the whole database, which is what wraparound risk is actually measured over"}
   		      }},
   		    {"limit", {
   			{"type", "integer"},
   			{"description", "how many tables to return, oldest first. Defaults to 20"}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
   // No schema default here: wraparound is a whole-database property, and
   // silently scoping it to "public" would understate the risk.
         return s.wraparound_status(a.str("schema", ""), a.num("limit", 20)); }},
      {"progressStats",
       "return every long-running maintenance command currently reporting progress (pg_stat_progress_vacuum, _analyze, _create_index, _cluster, _copy, _basebackup), with the phase, the blocks or tuples done against the total, a completion percentage, and how long it has been running. Use it to decide whether a VACUUM will finish before wraparound, or whether a CREATE INDEX is stuck waiting on a locker. The PostgreSQL 17 rename of the vacuum dead-tuple columns is normalized, and 'dead_tuple_unit' says whether the server counts tuples or bytes; on 17 and newer it also returns num_dead_item_ids alongside the byte counts, and indexes_total against indexes_processed -- index vacuuming is the phase a long VACUUM usually sits in, and blocks scanned alone will look stalled throughout it. On PostgreSQL 18 and newer, delay_time_ms is the total time this vacuum has spent asleep on the cost-based delay and delay_percent is that against its elapsed time -- a vacuum that is mostly sleeping is being throttled by vacuum_cost_delay/vacuum_cost_limit and looks identical, in blocks per second, to one on a slow disk. That is the difference between raising a cost limit and buying I/O. A logical replication table sync also reports here: take the worker pid from subscriptionStats and pass it as pid. Note that such a copy streams from the publisher rather than reading a file, so bytes_total is 0 and bytes_percent is null -- bytes_processed and tuples_processed are the figures that move",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pid", {
   			{"type", "integer"},
   			{"description", "only the command running in this backend; pair with the pid from currentActivity"}
   		      }},
   		    {"relation", {
   			{"type", "string"},
   			{"description", "only commands operating on this table, named bare or schema-qualified. A base backup has no relation, so this excludes that category entirely"}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         int pid = a.contains("pid") && a["pid"].is_number_integer()
           ? a["pid"].get<int>() : 0;
         std::string rel = a.contains("relation") && a["relation"].is_string()
           ? a["relation"].get<std::string>() : "";
         return s.progress_stats(pid, rel); }},
      {"ioStats",
       "return cumulative I/O statistics under 'io', per backend type, object and context (pg_stat_io, PostgreSQL 16+): reads, writes, extends, hits, evictions, reuses, fsyncs and their timings, with a hit percentage. This is where backend-written buffers, vacuum's ring-buffer reuse, and bulk read/write I/O become visible separately from the aggregate counters in checkpointStats. Rows with no activity are omitted unless a filter was given. Given a pid, reports that one backend instead, via pg_stat_get_backend_io, plus its WAL volume under 'wal' -- a backend can be quiet in I/O and still be generating WAL heavily; that requires PostgreSQL 18, since pg_stat_io itself has no pid column. Returns a clear error on PostgreSQL 15 and older, where the view does not exist",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pid", {
   			{"type", "integer"},
   			{"description", "report this one backend's I/O and WAL instead of the cluster-wide aggregate. PostgreSQL 18 and newer only"}
   		      }},
   		    {"backend_type", {
   			{"type", "string"},
   			{"description", "only this backend type, e.g. \"client backend\", \"autovacuum worker\", \"checkpointer\""}
   		      }},
   		    {"object", {
   			{"type", "string"},
   			{"description", "only this object class, e.g. \"relation\" or \"temp relation\""}
   		      }},
   		    {"context", {
   			{"type", "string"},
   			{"description", "only this I/O context, e.g. \"normal\", \"vacuum\", \"bulkread\", \"bulkwrite\""}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         int pid = a.contains("pid") && a["pid"].is_number_integer()
           ? a["pid"].get<int>() : 0;
         std::string bt = a.contains("backend_type") && a["backend_type"].is_string()
           ? a["backend_type"].get<std::string>() : "";
         std::string ob = a.contains("object") && a["object"].is_string()
           ? a["object"].get<std::string>() : "";
         std::string cx = a.contains("context") && a["context"].is_string()
           ? a["context"].get<std::string>() : "";
         return s.io_stats(pid, bt, ob, cx); }},
      {"checkpointStats",
       "return checkpoint, WAL, and background writer activity (pg_stat_checkpointer and pg_stat_bgwriter on PostgreSQL 17+, pg_stat_bgwriter alone before that, plus pg_stat_wal) with field names normalized across both shapes: timed versus requested checkpoint counts and the ratio between them, write and sync time, buffers written by the checkpointer, by the background writer, and directly by backends, WAL record/FPI/byte counts, and the related settings (checkpoint_timeout, max_wal_size, checkpoint_completion_target, the bgwriter knobs)",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.checkpoint_stats(); }},
      {"tableIOStats",
       "return per-object buffer cache hit ratios (pg_statio_all_tables): heap_blks_read versus heap_blks_hit, idx_blks_read versus idx_blks_hit, the TOAST and TOAST-index pairs, and a combined ratio, with relation size and scan counts. Naming a single table adds a per-index breakdown from pg_statio_all_indexes. Ratios are null, not zero, for an object that has seen no reads at all. Not to be confused with tableStats, which reports pg_stat_user_tables -- scans, tuples and vacuum state; this tool answers only whether those reads came from the buffer cache or the disk",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"table",  {
   			{"type", "string"},
   			{"description", "a single table; omit to sweep the schema. Only a named table gets the per-index breakdown"}
   		      }},
   		    {"limit", {
   			{"type", "integer"},
   			{"description", "how many tables to return, most physical reads first. Defaults to 20"}
   		      }}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.table_io_stats(a.str("schema", "public"), a.str("table", ""), a.num("limit", 20)); }},
      {"hostCapacity",
       "correlate memory and parallelism settings with the capacity of the machine PostgreSQL runs on. Host RAM and vCPU count exist outside the catalog, so they must be injected: pass them as arguments, set host_ram_mb/host_vcpus in the connection's section of the connections file, or export PG_LICHT_HOST_RAM_MB/PG_LICHT_HOST_VCPUS. Returns the host facts with the source they came from, every memory-related setting resolved to bytes, and derived ratios (shared_buffers and effective_cache_size as a percentage of RAM, work_mem times max_connections, maintenance_work_mem times autovacuum_max_workers, parallel workers per vCPU). Ratios are null when no RAM figure was supplied; nothing is ever guessed. IMPORTANT: 'settings' is what pg_settings reports for THIS session, so an ALTER ROLE ... SET or ALTER DATABASE ... SET made for another role is not in it. 'overrides' carries those from pg_db_role_setting, collapsed by (scope, name, value): two roles with different values stay separate rows, while N roles sharing one value become a single row with 'count' and up to five example names ('names_truncated' says when the rest were dropped). On a multi-tenant cluster the same search_path on every tenant is otherwise one row per tenant and swamps the three overrides that matter. committed_worst_case uses the largest work_mem any role is configured with rather than this session's -- work_mem_is_overridden says when the two differ. That layer is invisible to pg_settings and is the usual explanation for a statement that is slow only from the application",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"ram_mb", {
   			{"type", "integer"},
   			{"description", "total host memory in megabytes, overriding any configured value"}
   		      }},
   		    {"vcpus", {
   			{"type", "integer"},
   			{"description", "number of vCPUs or cores available to the host, overriding any configured value"}
   		      }},
   		    {"storage", {
   			{"type", "string"},
   			{"description", "free-text description of the storage, e.g. \"local nvme\" or \"gp3 3000 iops\"; echoed back, never interpreted"}
   		      }}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         long long ram_mb = a.contains("ram_mb") && a["ram_mb"].is_number_integer()
           ? a["ram_mb"].get<long long>() : 0;
         int vcpus = a.contains("vcpus") && a["vcpus"].is_number_integer()
           ? a["vcpus"].get<int>() : 0;
         std::string storage = a.contains("storage") && a["storage"].is_string()
           ? a["storage"].get<std::string>() : "";
         return s.host_capacity(ram_mb, vcpus, storage); }},
      {"duplicateIndexes",
       "return indexes that duplicate or are covered by another index on the same table. 'identical' groups indexes whose key columns, operator classes, collations, sort order, INCLUDE columns and partial predicate all match; 'redundant' reports an index whose key columns are a leading prefix of a wider index that also covers its INCLUDE columns. Comparison is by column expression rather than attribute number, so expression indexes and differing sort orders are handled correctly, and a unique index is never called redundant for being a prefix. Each entry carries size, idx_scan, the backing constraint name, and the replica identity and validity flags, since those decide whether it can be dropped at all. READ counters_since BEFORE idx_scan: it is when this database's statistics were last reset, and idx_scan counts only since then. A zero on a recently reset database says nothing about a monthly or quarterly index. It is a lower bound -- pg_stat_reset_single_table_counters() zeroes one relation without moving it",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"table",  {
   			{"type", "string"},
   			{"description", "restrict to one table; omit to check every table in the schema"}
   		      }}
   		  }},
   		{"required", {"schema"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.duplicate_indexes(a.str("schema", "public"), a.str("table", "")); }},
      {"tableBloat",
       "return physical storage bloat for a table (pgstattuple/pgstattuple_approx): table size, live/dead tuple counts and percentages, free space and percentage. Defaults to the cheap visibility-map-based approximation; set exact=true for a precise but I/O-heavy full table scan. More accurate than the ANALYZE-time estimates in listTableStats/tableStats. Returns a clear error with setup instructions if the pgstattuple extension is not installed",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"table",  {{"type", "string"}}},
   		    {"exact",  {{"type", "boolean"}}}
   		  }},
   		{"required", {"schema", "table"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.table_bloat(a.str("schema", "public"), a.str("table", ""), a.flag("exact", false)); }},
      {"indexBloat",
       "return physical statistics for one index, from whichever pgstattuple function matches its access method: pgstatindex for btree (tree level, leaf/internal/empty/deleted pages, average leaf density, leaf fragmentation), pgstatginindex for GIN (pending list pages and tuples, alongside the fastupdate setting and pending list limit that bound them), pgstathashindex for hash (bucket/overflow/bitmap/unused pages, live and dead items, free percent). The access method is resolved from the catalog, so the caller does not need to know it; gist, spgist and brin are reported as unsupported by name, since pgstattuple ships no pgstat*index function for them. Metrics are deliberately NOT normalized across access methods -- 'access_method' says which set came back. Index size and idx_scan travel with the metrics, because a fragmented index nothing has scanned is a candidate for dropping rather than REINDEX. btree and hash read the whole index; GIN reads only the metapage and is always cheap",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"index",  {{"type", "string"},
   				{"description", "the index's own name, not the name of the table it is on"}}}
   		  }},
   		{"required", {"schema", "index"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.index_bloat(a.str("schema", "public"), a.str("index", "")); }},
      {"checkKey",
       "check if a row exists by primary key; validates value types against the PK column types before querying",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"schema", {{"type", "string"}}},
   		    {"table",  {{"type", "string"}}},
   		    {"values", {{"type", "array"}}}
   		  }},
   		{"required", {"schema", "table", "values"}}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.check_key(a.str("schema", "public"), a.str("table", ""), a.arr("values")); }},
      {"explainQuery",
       "return the raw EXPLAIN (FORMAT JSON) plan for a statement, either recovered from pg_stat_statements by queryid (full untruncated text) or supplied directly as sql. Runs in a read-only transaction bounded by statement_timeout. Statements with $n placeholders are planned with GENERIC_PLAN unless concrete params are supplied, in which case the statement is PREPAREd and planned with real values. analyze:true runs EXPLAIN (ANALYZE, BUFFERS), which really executes the statement, and is honoured only after the plan is proven free of any ModifyTable node -- so data-modifying statements, including data-modifying CTEs, are never executed; it also requires an explicit timeout_ms. Returns the plan verbatim plus generic/analyzed/read_only flags and the pg_stat_statements row; no heuristics and no generated DDL, the plan is yours to interpret. Every plan carries a Settings block (EXPLAIN SETTINGS) naming the settings that differ from the built-in default, because the plan is built in THIS server's session and not in the one the statement really runs in -- work_mem alone can change the algorithm rather than the cost, turning a HashAggregate into a Sort plus GroupAggregate. Compare it against hostCapacity.overrides, which reports the per-role and per-database settings pg_settings cannot show: where they differ, this plan is not the plan production gets -- and plan_as_role then plans it under what that role actually carries, so the difference between the two plans becomes the finding rather than a caveat. planning_environment reports what was applied and, for plan_as_role, what was skipped",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"queryid", {
   			{"type", "string"},
   			{"description", "pg_stat_statements queryid as a decimal string (it is a 64-bit value and does not survive JSON number precision). Mutually exclusive with 'sql'"}
   		      }},
   		    {"sql", {
   			{"type", "string"},
   			{"description", "a single SELECT/INSERT/UPDATE/DELETE/MERGE/WITH/TABLE/VALUES statement to explain. Utility statements (SET, CREATE, VACUUM, ...) are rejected. Mutually exclusive with 'queryid'"}
   		      }},
   		    {"params", {
   			{"type", "array"},
   			{"description", "concrete values for the statement's $1..$n placeholders, in order. Supply these for a real (non-generic) plan; required to use analyze. Values are bound as literals of unknown type and coerced by PostgreSQL to the inferred parameter types; use null for SQL NULL"}
   		      }},
   		    {"analyze", {
   			{"type", "boolean"},
   			{"description", "run EXPLAIN (ANALYZE, BUFFERS), which really executes the statement. Requires timeout_ms. Ignored with an explanatory 'note' if the statement modifies data or could only be planned generically. Default false"}
   		      }},
   		    {"timeout_ms", {
   			{"type", "integer"},
   			{"description", "statement_timeout for the explain, in milliseconds, clamped to [100, 30000]. Required when analyze is true; defaults to 5000 for plan-only calls"}
   		      }},
   		    {"settings", {{"type", "object"},
   		                  {"description", "planner settings to apply for this plan only, e.g. {\"work_mem\": \"512MB\"}. Applied with set_config(is_local) so they revert with the transaction and cannot leak to another session. Allowlisted to settings that change a PLAN; an unknown name is refused and nothing is planned, rather than ignored. With analyze the statement is EXECUTED under them only within a budget taken from the host capacity declared for this connection (host_ram_mb and host_vcpus): the worst case the plan's memory limits permit must stay within a share of RAM (10% unless budgets.ini says otherwise), and the plan may use at most one parallel worker per four vCPUs (likewise). With no declared capacity no settings change is executed at all. Either way a refused plan is still returned, analyzed stays false, and planning_environment.execution_budget carries the arithmetic. plan_as_role alone is not budgeted: it is what that role already runs with"}}},
   		    {"plan_as_role", {{"type", "string"},
   		                      {"description", "plan under what this role carries in pg_db_role_setting, filtered to planner settings. A per-database entry (ALTER ROLE ... IN DATABASE) overrides the role-wide one, as the server itself applies them. Non-planner entries such as search_path or statement_timeout are reported under skipped_from_role rather than dropped silently"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         // queryid is documented as a string because a 64-bit value does not
         // survive JSON number precision, but accept a number too rather than
         // fail with a raw nlohmann type error.
         std::string qid;
         if (a.contains("queryid")) {
           if (a["queryid"].is_string())
             qid = a["queryid"].get<std::string>();
           else if (a["queryid"].is_number_integer())
             qid = std::to_string(a["queryid"].get<long long>());
         }
         std::string sql = a.contains("sql") ? a["sql"].get<std::string>() : "";
         json prms = a.contains("params") ? a["params"] : json::array();
         bool do_analyze = a.contains("analyze") ? a["analyze"].get<bool>() : false;
         int tmo = a.contains("timeout_ms") ? a["timeout_ms"].get<int>() : 0;
         json settings = a.contains("settings") ? a["settings"] : json::object();
         std::string as_role = a.str("plan_as_role");
         return s.explain_query(qid, sql, prms, do_analyze, tmo, settings, as_role); }},
      {"verifyTopology",
       "connect to every configured connection and report what each server actually is: its role (primary or replica, from pg_is_in_recovery(), observed now rather than configured), its system identifier, database, address, port and version -- then check the declared topology against them. A physical replica carries the same system identifier as its primary forever, so the identifier alone cannot separate the two axes: same identifier with the same host and port is one instance, same identifier on different hosts is a replication group. Reports declarations the servers contradict, connections that share an identifier but are not declared together (an undeclared replica is where 'is this index used?' quietly gets the wrong answer), a replication group with no primary, and split brain. Logical replication cannot be verified this way and is reported as such rather than as a mismatch. Connects once per configured connection, sequentially, with a short connect timeout; a connection that fails is reported and does not abort the rest",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.verify_topology(); }},
      {"bufferCacheSummary",
       "return how much of shared_buffers is used, dirty and pinned, with the usage-count histogram from pg_buffercache. Cheap enough for a routine health sweep beside checkpointStats. tableIOStats counts only what shared_buffers served, so a miss there may still have come from the OS page cache at RAM speed; this is the only in-core view of that split. Read the histogram rather than a hit ratio: mass at usage_count 2-5 is a stable working set, everything at 0-1 with no unused buffers is clock-sweep churn, and those are the same ratio with opposite diagnoses. One sample is weak evidence -- two samples minutes apart are the method. The readings cover the whole instance, not this database alone. Requires the pg_buffercache extension at version 1.4 or later, and a role with pg_monitor",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", json::object()}
   	      }; },
       [](PostgresMCPServer& s, const Args&) -> json {
         return s.buffer_cache_summary(); }},
      {"bufferCacheContents",
       "return which relations own shared_buffers, aggregated per relation and fork and ranked by buffers held: cached bytes, percent of that fork resident, percent of shared_buffers consumed, dirty buffers, average usagecount and pins. Never raw per-buffer rows. Answers which relation is driving checkpoint writeback (pair with checkpointStats), whether the visibility-map fork is resident enough for index-only scans to pay off, and -- on a multi-tenant instance -- which database's working set is displacing the others. Only buffers belonging to this database and the shared catalogs can be resolved to names; buffers held by other databases on the same instance are visible to PostgreSQL but deliberately not reported here. Cost is O(shared_buffers) and does not vary with the limit or with anything else asked: pg_buffercache materialises one row per buffer before any filter applies, so narrowing the question does not narrow the scan. Around 0.5s per 16GB of shared_buffers, and it is subject to the connection's statement_timeout like every other call. bufferCacheSummary reads the same memory through a function that returns one row and is roughly a hundred times cheaper, so prefer it for anything routine. Requires the pg_buffercache extension and a role with pg_monitor",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"limit", {{"type", "integer"}, {"description", "how many relation/fork rows to return, ranked by buffers held. Defaults to 20, capped at 200"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         int limit = a.contains("limit") && a["limit"].is_number_integer()
           ? a["limit"].get<int>() : 20;
         return s.buffer_cache_contents(limit); }},
      {"listTopology",
       "return the configured topology: which connections share an instance (one postmaster, so they share shared_buffers, WAL, autovacuum workers and disk), which belong to the same replication_group (a primary and its replicas, holding the same data on different servers), and which carry each operator group label. Reads the config file only and opens no database connection, so it is cheap to call before deciding how wide a sweep to run. An instance whose source is \"inferred\" was derived from an identical host and port rather than declared, and is a hint for grouping output, not evidence of shared memory. Roles are not here: primary or replica is observed per call, never configured -- use verifyTopology. Pass 'pattern' to narrow it: a case-insensitive substring matched against each topology name AND its member connection names, so \"where does billing_prod live\" and \"show me the ha group\" are the same argument. A pattern that matches nothing returns empty lists rather than an error",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pattern", {{"type", "string"}, {"description", "a literal, case-insensitive substring of an instance, replication_group or group name, or of a connection name belonging to one; _ and % match themselves and nothing is stemmed"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.topology(a.str("pattern")); }},
      {"listConnections",
       "return the configured database connections by name, with the libpq service name or host/port/dbname/user for each, its instance, replication_group and group labels where configured, and which is the default; passwords are never returned and a service file is never expanded. Pass a name as the 'connection' argument of any other tool to run that tool against that database. See listTopology for the same labels indexed the other way round, by topology name rather than by connection. Pass 'pattern' to narrow the list: a case-insensitive substring matched against the connection name AND its instance, replication_group and group labels. That is how to find one connection on a registry holding hundreds -- this answer carries every configured connection otherwise, and it has no cursor. A pattern that matches nothing returns an empty list rather than an error",
       []() -> json { return {
   		{"type", "object"},
   		{"properties", {
   		    {"pattern", {{"type", "string"}, {"description", "a literal, case-insensitive substring of the connection name or of its instance, replication_group or group label; _ and % match themselves and nothing is stemmed"}}}
   		  }}
   	      }; },
       [](PostgresMCPServer& s, const Args& a) -> json {
         return s.connections(a.str("pattern")); }},
    };
    return defs;
  }
