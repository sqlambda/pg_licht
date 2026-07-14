#pragma once

#include <iostream>
#include <set>
#include <string>
#include <nlohmann/json.hpp>
#if defined(__GNUC__) && !defined(__clang__)
// GCC-only false positive from std::variant inside pqxx headers; Clang
// doesn't have this warning group at all, and with -Werror active it would
// hard-fail on "unknown warning group" if this pragma weren't guarded.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <pqxx/pqxx>
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

using json = nlohmann::json;

// pqxx 7.9+ renamed exec_params to exec(sql, pqxx::params).
// Use PQXX_VERSION_MINOR to select the right overload at compile time.
template<typename P>
inline pqxx::result pqxx_exec(pqxx::work& txn, const std::string& sql, P&& params) {
#if PQXX_VERSION_MAJOR > 7 || (PQXX_VERSION_MAJOR == 7 && PQXX_VERSION_MINOR >= 9)
  return txn.exec(sql, std::forward<P>(params));
#else
  return txn.exec_params(sql, std::forward<P>(params));
#endif
}

class PostgresMCPServer {
public:
  PostgresMCPServer(const std::string& conn_str) : conn(conn_str) {
    pqxx::work txn{conn};
    // set_config() (not plain SET) so application_name is bound as a real
    // parameter rather than concatenated into SQL text.
    pqxx_exec(txn, "SELECT set_config('application_name', $1, false)",
              pqxx::params{std::string("pg-licht-cpp/") + PGLICHT_VERSION});
    // Defense in depth: this server only ever reads. Rejecting writes at the
    // session level means even a bug that let a query mutate data would still
    // fail, rather than succeed silently.
    txn.exec("SET default_transaction_read_only = on");
    txn.commit();
  }

  void run() {
    std::string line;
    while (std::getline(std::cin, line)) {
      try {
        auto request = json::parse(line);
        handle_request(request);
      } catch (const std::exception& e) {
        std::cerr << "Standard Exception: " << e.what() << std::endl;
        std::cout << json{{"jsonrpc", "2.0"}, {"error", {{"code", -32700}, {"message", "Parse error"}}}}.dump() << std::endl;
      }
    }
  }

  // Test-accessible query methods
  const json call_schemas() { return schemas(); }
  const json call_tables(const std::string& schema) { return tables(schema); }
  const json call_search(const std::string& web_search) { return search(web_search); }
  const json call_table(const std::string& schema, const std::string& table_name) {
    return table(schema, table_name);
  }
  const json call_functions(const std::string& schema) { return functions(schema); }
  const json call_function_detail(const std::string& schema, const std::string& func_name) {
    return function_detail(schema, func_name);
  }
  const json call_search_functions(const std::string& web_search) { return search_functions(web_search); }
  const json call_enums(const std::string& schema) { return enums(schema); }
  const json call_enum_detail(const std::string& schema, const std::string& enum_name) {
    return enum_detail(schema, enum_name);
  }
  const json call_search_enums(const std::string& web_search) { return search_enums(web_search); }
  const json call_types(const std::string& schema) { return types(schema); }
  const json call_type_detail(const std::string& schema, const std::string& type_name) {
    return type_detail(schema, type_name);
  }
  const json call_roles() { return roles(); }
  const json call_foreign_tables(const std::string& schema) { return foreign_tables(schema); }
  const json call_foreign_servers() { return foreign_servers(); }
  const json call_tablespaces() { return tablespaces(); }
  const json call_collations(const std::string& schema) { return collations(schema); }
  const json call_event_triggers() { return event_triggers(); }
  const json call_publications() { return publications(); }
  const json call_subscriptions() { return subscriptions(); }
  const json call_languages() { return languages(); }
  const json call_extended_statistics(const std::string& schema) { return extended_statistics(schema); }
  const json call_operators(const std::string& schema) { return operators(schema); }
  const json call_operator_classes(const std::string& schema) { return operator_classes(schema); }
  const json call_access_methods() { return access_methods(); }
  const json call_casts() { return casts(); }
  const json call_text_search_configs(const std::string& schema) { return text_search_configs(schema); }
  const json call_sequences(const std::string& schema) { return sequences(schema); }
  const json call_extensions() { return extensions(); }
  const json call_database_size() { return database_size(); }
  const json call_server_settings() { return server_settings(); }
  const json call_activity() { return activity(); }
  const json call_locks() { return locks(); }
  const json call_replication_slots() { return replication_slots(); }
  const json call_database_stats() { return database_stats(); }
  const json call_statement_stats(int limit) { return statement_stats(limit); }
  const json call_table_bloat(const std::string& schema, const std::string& table_name, bool exact) {
    return table_bloat(schema, table_name, exact);
  }
  const json call_check_key(const std::string& schema, const std::string& table_name, const json& values) {
    return check_key(schema, table_name, values);
  }

private:
  pqxx::connection conn;

  const json get_tools_list() {
    return {
      {"tools", {
	  {
	    {"name", "listSchemas"},
	    {"description", "return schema list with basic summaries"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listTables"},
	    {"description", "return table list with basic statistics"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "tableDetails"},
	    {"description", "return table details like columns, foreign keys, inbound foreign keys (referenced_by), indexes, data histograms"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"table", {{"type", "string"}}},
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"table", "schema"}}
	      }}
	  },
	  {
	    {"name", "searchTables"},
	    {"description", "return table list with basic statistics based on text search"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"web_search", {{"type", "string"}}}
		  }},
		{"required", {"web_search"}}
	      }}
	  },
	  {
	    {"name", "listFunctions"},
	    {"description", "return function and procedure list for a schema"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "functionDetails"},
	    {"description", "return detailed function or procedure info including source and trigger usage"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"function", {{"type", "string"}}}
		  }},
		{"required", {"schema", "function"}}
	      }}
	  },
	  {
	    {"name", "searchFunctions"},
	    {"description", "search functions and procedures by name, source, language, trigger name, or description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"web_search", {{"type", "string"}}}
		  }},
		{"required", {"web_search"}}
	      }}
	  },
	  {
	    {"name", "listEnums"},
	    {"description", "return enum type list for a schema with their values and descriptions"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "enumDetails"},
	    {"description", "return enum type details including values and which columns use it"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"enum", {{"type", "string"}}}
		  }},
		{"required", {"schema", "enum"}}
	      }}
	  },
	  {
	    {"name", "searchEnums"},
	    {"description", "search enum types by name, values, or description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"web_search", {{"type", "string"}}}
		  }},
		{"required", {"web_search"}}
	      }}
	  },
	  {
	    {"name", "listTypes"},
	    {"description", "return composite type, domain, and range type list for a schema (excludes enums and implicit table/view row types); composites include their attribute list, domains include base type/nullability/default/constraints, ranges include subtype and the auto-generated multirange type name"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "typeDetails"},
	    {"description", "return composite type, domain, or range type details including attributes/constraints/subtype and which columns use it"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"type", {{"type", "string"}}}
		  }},
		{"required", {"schema", "type"}}
	      }}
	  },
	  {
	    {"name", "listRoles"},
	    {"description", "return cluster-wide roles with kind (login/group), attributes (superuser, create_role, create_db, replication, bypass_rls, connection_limit, valid_until), and group memberships"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listForeignTables"},
	    {"description", "return foreign tables in a schema with their foreign server, FDW, options, and columns (does not expose user mapping credentials)"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listForeignServers"},
	    {"description", "return cluster-wide foreign servers with their FDW, owner, and options (host/port/dbname-style options only, never user mapping credentials)"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listTablespaces"},
	    {"description", "return cluster-wide tablespaces with owner, filesystem location, options, and description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listCollations"},
	    {"description", "return collations usable in the current database's encoding for a schema, with provider, locale settings, and determinism flag"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listEventTriggers"},
	    {"description", "return cluster-wide event triggers with event type, tags, function, owner, enabled status, and description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listPublications"},
	    {"description", "return logical replication publications with owner, all-tables flag, per-operation flags (insert/update/delete/truncate), and member tables"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listSubscriptions"},
	    {"description", "return logical replication subscriptions for the current database with owner, enabled status, publications, slot name, and sync settings (never exposes the connection string, which may contain credentials)"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listLanguages"},
	    {"description", "return procedural languages installed in the current database (e.g. plpgsql, plpython3u) with owner, trusted/procedural flags, handler function, and description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listExtendedStatistics"},
	    {"description", "return extended statistics objects (CREATE STATISTICS) for a schema with target table, columns, statistics kinds (ndistinct, dependencies, mcv), and description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listOperators"},
	    {"description", "return custom operators in a schema with left/right operand types, result type, and implementing function; mostly relevant for schemas using extensions with custom types (e.g. PostGIS)"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listOperatorClasses"},
	    {"description", "return operator classes in a schema with their index access method, input type, and default flag; describes what index types (btree/gist/gin/etc) a type supports"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listAccessMethods"},
	    {"description", "return index and table access methods available in the cluster (btree, gist, gin, heap, etc) with type and handler function"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listCasts"},
	    {"description", "return type casts involving at least one user-defined type (excludes built-in-to-built-in casts) with source/target types, context (implicit/assignment/explicit), and method"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listTextSearchConfigs"},
	    {"description", "return full-text search configurations for a schema with parser and the token-type-to-dictionary mapping"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listSequences"},
	    {"description", "return sequence list for a schema with type, range, increment, cycle, cache, current value, and owning table.column (for SERIAL/IDENTITY columns)"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "listExtensions"},
	    {"description", "return installed PostgreSQL extensions with version, schema, relocatable flag, and description"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "databaseSize"},
	    {"description", "return the current database name and its total disk size"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "serverSettings"},
	    {"description", "return all PostgreSQL server settings (pg_settings) grouped by category, each with current value, unit, description, context, type, source, and pending_restart flag"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "currentActivity"},
	    {"description", "return current server connections and running queries (pg_stat_activity) across all databases: pid, database, user, application_name, backend_type, state, wait event, and query text"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "currentLocks"},
	    {"description", "return current locks (pg_locks) joined with the holding backend's query and user, plus which pids are blocking each waiting lock; use to diagnose lock contention"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "replicationSlots"},
	    {"description", "return replication slots with retained WAL bytes; a lagging or unused slot holds back WAL indefinitely and is a common cause of disk bloat incidents"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "databaseStats"},
	    {"description", "return per-database statistics (pg_stat_database) for every database in the cluster: connections, commits/rollbacks, block hit ratio inputs, tuple counts, conflicts, deadlocks, temp file usage, and checksum failures"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "statementStats"},
	    {"description", "return the slowest tracked queries by total execution time (pg_stat_statements), with calls, timing, row counts, and buffer usage; returns a clear error with setup instructions if the extension is not installed"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"limit", {{"type", "integer"}}}
		  }}
	      }}
	  },
	  {
	    {"name", "tableBloat"},
	    {"description", "return physical storage bloat for a table (pgstattuple/pgstattuple_approx): table size, live/dead tuple counts and percentages, free space and percentage. Defaults to the cheap visibility-map-based approximation; set exact=true for a precise but I/O-heavy full table scan. More accurate than the ANALYZE-time estimates in listTables/tableDetails. Returns a clear error with setup instructions if the pgstattuple extension is not installed"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"table",  {{"type", "string"}}},
		    {"exact",  {{"type", "boolean"}}}
		  }},
		{"required", {"schema", "table"}}
	      }}
	  },
	  {
	    {"name", "checkKey"},
	    {"description", "check if a row exists by primary key; validates value types against the PK column types before querying"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"table",  {{"type", "string"}}},
		    {"values", {{"type", "array"}}}
		  }},
		{"required", {"schema", "table", "values"}}
	      }}
	  }
	}}
    };
  }

  const json schemas() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(nspname,
              JSONB_BUILD_OBJECT(
               'tables', relnames,
               'roles', COALESCE(roles, '{}'::jsonb)))
      FROM pg_namespace
      LEFT JOIN LATERAL (SELECT JSONB_AGG(relname ORDER BY relname) AS relnames
                         FROM pg_class
                         WHERE relnamespace = pg_namespace.oid
                           AND relkind IN ('r','m','f','p','v')) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(pg_namespace.nspacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) ON true
      WHERE nspname NOT LIKE 'pg_%'
        AND nspname <> 'information_schema'
        AND relnames IS NOT NULL;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_schemas = res[0][0].as<std::string>();
      return json::parse(pgsql_schemas);
    } else {
      return {};
    }
  }

  const json tables(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(c.relname,
              JSONB_BUILD_OBJECT(
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
               'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan, 'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
               'n_mod_since_analyze', s.n_mod_since_analyze, 'n_ins_since_vacuum', s.n_ins_since_vacuum,
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum), 'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
               'reloptions', c.reloptions,
               'columns', columns, 'index_count', COALESCE(index_count, 0), 'constraint_count', COALESCE(constraint_count, 0)))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(a.attname,
                        JSONB_BUILD_OBJECT(
                         'description', col_description(c.oid, a.attnum),
                         'index_count', (SELECT COUNT(DISTINCT ix.indexrelid)
                                         FROM pg_index AS ix, LATERAL unnest(ix.indkey) AS attnum
                                         WHERE ix.indrelid = c.oid AND attnum = a.attnum))) AS columns
                       FROM pg_attribute AS a
                       WHERE a.attnum > 0
                         AND a.attrelid = c.oid
                         AND NOT a.attisdropped) ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS index_count
                         FROM pg_indexes AS i
                         WHERE i.schemaname = $1
                           AND i.tablename = c.relname) ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS constraint_count
                         FROM pg_constraint
                         WHERE conrelid = c.oid) ON true
      WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND c.relkind IN ('r', 'p', 'm', 'v');
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_tables = res[0][0].as<std::string>();
      return json::parse(pgsql_tables);
    } else {
      return {};
    }
  }

  const json search(const std::string& web_search) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(c.relnamespace::regnamespace::name || '.' || c.relname,
              JSONB_BUILD_OBJECT(
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
               'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan, 'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
               'n_mod_since_analyze', s.n_mod_since_analyze, 'n_ins_since_vacuum', s.n_ins_since_vacuum,
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum), 'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
               'reloptions', c.reloptions,
               'columns', columns, 'index_count', COALESCE(index_count, 0), 'constraint_count', COALESCE(constraint_count, 0),
               'roles', COALESCE(roles, '{}'::jsonb)))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(a.attname,
                                JSONB_BUILD_OBJECT(
                                 'description', col_description(c.oid, a.attnum),
                                 'index_count', (SELECT COUNT(DISTINCT ix.indexrelid)
                                                 FROM pg_index AS ix, LATERAL unnest(ix.indkey) AS attnum
                                                 WHERE ix.indrelid = c.oid AND attnum = a.attnum))) AS columns,
                                STRING_AGG(
                                    REGEXP_REPLACE(REGEXP_REPLACE(a.attname, '_', ' ', 'g'), '([[:upper:]])', ' \1', 'g') || ' ' ||
                                    COALESCE(col_description(c.oid, a.attnum), ''),
                                    ' ') AS col_text
                         FROM pg_attribute AS a
                         WHERE a.attnum > 0
                           AND a.attrelid = c.oid
                           AND NOT a.attisdropped) ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS index_count
                         FROM pg_indexes AS i
                         WHERE i.schemaname = c.relnamespace::regnamespace::name
                           AND i.tablename = c.relname) ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS constraint_count
                         FROM pg_constraint
                         WHERE conrelid = c.oid) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles,
                                STRING_AGG(grantee, ' ' ORDER BY grantee) AS role_names
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(c.relacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) ON true
      LEFT JOIN LATERAL (
          SELECT STRING_AGG(
              REGEXP_REPLACE(REGEXP_REPLACE(et.typname, '_', ' ', 'g'), '([[:upper:]])', ' \1', 'g') || ' ' ||
              COALESCE(obj_description(et.oid, 'pg_type'), '') || ' ' ||
              COALESCE(values_text, ''), ' ') AS enum_text
          FROM pg_attribute AS a
          JOIN pg_type AS et ON et.oid = a.atttypid AND et.typtype = 'e'
          LEFT JOIN LATERAL (
              SELECT STRING_AGG(ev.enumlabel, ' ') AS values_text
              FROM pg_enum AS ev WHERE ev.enumtypid = et.oid
          ) ON true
          WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid
      ) ON true
      WHERE TO_TSVECTOR('english',
              REGEXP_REPLACE(REGEXP_REPLACE(c.relname, '_', ' ', 'g'), '([[:upper:]])', ' \1', 'g') || ' ' ||
              REGEXP_REPLACE(REGEXP_REPLACE(c.relnamespace::regnamespace::name, '_', ' ', 'g'), '([[:upper:]])', ' \1', 'g') || ' ' ||
              COALESCE(obj_description(c.oid, 'pg_class'), '') || ' ' ||
              COALESCE(enum_text, '') || ' ' ||
              COALESCE(role_names, '') || ' ' ||
              COALESCE(col_text, '')
            ) @@ websearch_to_tsquery('english', $1)
        AND c.relkind IN ('r', 'p', 'm', 'v')
        AND c.relnamespace NOT IN (
            SELECT oid FROM pg_namespace
            WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{web_search});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_tables = res[0][0].as<std::string>();
      return json::parse(pgsql_tables);
    } else {
      return {};
    }
  }

  const json functions(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')',
               JSONB_BUILD_OBJECT(
                 'description',      COALESCE(obj_description(p.oid, 'pg_proc'), ''),
                 'kind',             CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' END,
                 'language',         l.lanname,
                 'return_type',      pg_get_function_result(p.oid),
                 'arguments',        pg_get_function_arguments(p.oid),
                 'volatility',       CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' WHEN 'v' THEN 'volatile' END,
                 'security_definer', p.prosecdef,
                 'is_strict',        p.proisstrict
               )
             )
      FROM   pg_proc AS p
      JOIN   pg_language AS l ON l.oid = p.prolang
      WHERE  p.pronamespace = $1::regnamespace
        AND  p.prokind IN ('f', 'p');
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_functions = res[0][0].as<std::string>();
      return json::parse(pgsql_functions);
    } else {
      return {};
    }
  }

  const json function_detail(const std::string& schema, const std::string& func_name) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')',
               JSONB_BUILD_OBJECT(
                 'description',      COALESCE(obj_description(p.oid, 'pg_proc'), ''),
                 'kind',             CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' END,
                 'language',         l.lanname,
                 'return_type',      pg_get_function_result(p.oid),
                 'arguments',        pg_get_function_arguments(p.oid),
                 'volatility',       CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' WHEN 'v' THEN 'volatile' END,
                 'security_definer', p.prosecdef,
                 'is_strict',        p.proisstrict,
                 'source',           p.prosrc,
                 'definition',       pg_get_functiondef(p.oid),
                 'used_in_triggers', used_in_triggers,
                 'roles',            COALESCE(roles, '{}'::jsonb)
               )
             )
      FROM   pg_proc AS p
      JOIN   pg_language AS l ON l.oid = p.prolang
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'trigger_name', t.tgname,
                   'table',        t.tgrelid::regclass::text
                 )) AS used_in_triggers
          FROM   pg_trigger AS t
          WHERE  t.tgfoid = p.oid AND NOT t.tgisinternal
      ) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(p.proacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) ON true
      WHERE  p.pronamespace = $1::regnamespace
        AND  p.proname = $2
        AND  p.prokind IN ('f', 'p');
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, func_name});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_function = res[0][0].as<std::string>();
      return json::parse(pgsql_function);
    } else {
      return {};
    }
  }

  const json search_functions(const std::string& web_search) {
    if (web_search.empty()) {
      return {};
    }

    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               p.pronamespace::regnamespace::text || '.' ||
                 p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')',
               JSONB_BUILD_OBJECT(
                 'description',      COALESCE(obj_description(p.oid, 'pg_proc'), ''),
                 'kind',             CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' END,
                 'language',         l.lanname,
                 'return_type',      pg_get_function_result(p.oid),
                 'arguments',        pg_get_function_arguments(p.oid),
                 'volatility',       CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' WHEN 'v' THEN 'volatile' END,
                 'security_definer', p.prosecdef,
                 'is_strict',        p.proisstrict,
                 'trigger_names',    trigger_names
               )
             )
      FROM   pg_proc AS p
      JOIN   pg_language AS l ON l.oid = p.prolang
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(t.tgname) AS trigger_names
          FROM   pg_trigger AS t
          WHERE  t.tgfoid = p.oid AND NOT t.tgisinternal
      ) ON true
      WHERE  p.prokind IN ('f', 'p')
        AND (
            TO_TSVECTOR('english',
              REGEXP_REPLACE(REGEXP_REPLACE(p.proname, '_', ' ', 'g'), '([[:upper:]])', ' \1', 'g'))
              @@ websearch_to_tsquery('english', $1)
         OR TO_TSVECTOR('english', COALESCE(p.prosrc, ''))
              @@ websearch_to_tsquery('english', $1)
         OR l.lanname ILIKE '%' || $1 || '%'
         OR p.oid IN (SELECT tgfoid FROM pg_trigger
                      WHERE NOT tgisinternal
                        AND to_tsvector('english', tgname) @@ websearch_to_tsquery('english', $1))
         OR p.oid IN (SELECT objoid FROM pg_description
                      WHERE to_tsvector('english', description) @@ websearch_to_tsquery('english', $1)
                        AND classoid = 'pg_proc'::regclass
                        AND objsubid = 0)
        );
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{web_search});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_functions = res[0][0].as<std::string>();
      return json::parse(pgsql_functions);
    } else {
      return {};
    }
  }

  const json server_settings() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(category, settings ORDER BY category)
      FROM (
        SELECT category,
               JSONB_OBJECT_AGG(name,
                 JSONB_BUILD_OBJECT(
                   'setting',         setting,
                   'unit',            unit,
                   'short_desc',      short_desc,
                   'context',         context,
                   'vartype',         vartype,
                   'source',          source,
                   'pending_restart', pending_restart
                 ) ORDER BY name
               ) AS settings
        FROM pg_settings
        GROUP BY category
      ) s;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json extensions() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               e.extname,
               JSONB_BUILD_OBJECT(
                 'version',     e.extversion,
                 'schema',      n.nspname,
                 'relocatable', e.extrelocatable,
                 'description', COALESCE(obj_description(e.oid, 'pg_extension'), '')
               )
             )
      FROM pg_extension AS e
      JOIN pg_namespace AS n ON n.oid = e.extnamespace;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json activity() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(pid::text,
               JSONB_BUILD_OBJECT(
                 'database',         datname,
                 'user',             usename,
                 'application_name', application_name,
                 'client_addr',      client_addr::text,
                 'backend_type',     backend_type,
                 'state',            state,
                 'wait_event_type',  wait_event_type,
                 'wait_event',       wait_event,
                 'backend_start',    backend_start,
                 'xact_start',       xact_start,
                 'query_start',      query_start,
                 'state_change',     state_change,
                 'query',            query
               ))
      FROM pg_stat_activity
      WHERE pid != pg_backend_pid();
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json locks() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                 'pid',              l.pid,
                 'lock_type',        l.locktype,
                 'relation',         rel.relname,
                 'mode',             l.mode,
                 'granted',          l.granted,
                 'wait_start',       l.waitstart,
                 'blocked_by',       COALESCE(blk.blockers, '[]'::jsonb),
                 'query',            a.query,
                 'user',             a.usename,
                 'application_name', a.application_name
               ) ORDER BY l.granted ASC, l.pid
             )
      FROM pg_locks AS l
      LEFT JOIN pg_stat_activity AS a ON a.pid = l.pid
      LEFT JOIN LATERAL (
          SELECT c.relnamespace::regnamespace::text || '.' || c.relname AS relname
          FROM pg_class AS c WHERE c.oid = l.relation
      ) rel ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(bp) AS blockers
          FROM unnest(pg_blocking_pids(l.pid)) AS bp
      ) blk ON true
      WHERE l.pid != pg_backend_pid();
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return json::array();
    }
  }

  const json replication_slots() {
    pqxx::work txn{conn};

    // retained_wal_bytes is the key diagnostic: a lagging or unused slot holds
    // back WAL cleanup indefinitely, a common cause of disk bloat incidents.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(slot_name,
               JSONB_BUILD_OBJECT(
                 'plugin',              plugin,
                 'slot_type',           slot_type,
                 'database',            database,
                 'temporary',           temporary,
                 'active',              active,
                 'active_pid',          active_pid,
                 'restart_lsn',         restart_lsn::text,
                 'confirmed_flush_lsn', confirmed_flush_lsn::text,
                 'retained_wal_bytes',  CASE WHEN restart_lsn IS NOT NULL
                                           THEN pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) END
               ))
      FROM pg_replication_slots;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json database_stats() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(datname,
               JSONB_BUILD_OBJECT(
                 'numbackends',       numbackends,
                 'xact_commit',       xact_commit,
                 'xact_rollback',     xact_rollback,
                 'blks_read',         blks_read,
                 'blks_hit',          blks_hit,
                 'tup_returned',      tup_returned,
                 'tup_fetched',       tup_fetched,
                 'tup_inserted',      tup_inserted,
                 'tup_updated',       tup_updated,
                 'tup_deleted',       tup_deleted,
                 'conflicts',         conflicts,
                 'temp_files',        temp_files,
                 'temp_bytes',        temp_bytes,
                 'deadlocks',         deadlocks,
                 'checksum_failures', checksum_failures,
                 'blk_read_time',     blk_read_time,
                 'blk_write_time',    blk_write_time,
                 'stats_reset',       stats_reset
               ))
      FROM pg_stat_database
      WHERE datname IS NOT NULL;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json statement_stats(int limit) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_AGG(row_json)
      FROM (
          SELECT JSONB_BUILD_OBJECT(
                   'query_id',         pss.queryid,
                   'query',            LEFT(pss.query, 500),
                   'calls',            pss.calls,
                   'total_exec_ms',    pss.total_exec_time,
                   'mean_exec_ms',     pss.mean_exec_time,
                   'min_exec_ms',      pss.min_exec_time,
                   'max_exec_ms',      pss.max_exec_time,
                   'rows',             pss.rows,
                   'shared_blks_hit',  pss.shared_blks_hit,
                   'shared_blks_read', pss.shared_blks_read,
                   'user',             r.rolname,
                   'database',         d.datname
                 ) AS row_json
          FROM pg_stat_statements AS pss
          LEFT JOIN pg_roles AS r ON r.oid = pss.userid
          LEFT JOIN pg_database AS d ON d.oid = pss.dbid
          ORDER BY pss.total_exec_time DESC
          LIMIT $1
      ) sub;
    )";

    try {
      pqxx::result res = pqxx_exec(txn, query, pqxx::params{limit});

      if (!res.empty() && !res[0][0].is_null()) {
        return json::parse(res[0][0].as<std::string>());
      } else {
        return json::array();
      }
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42P01") { // undefined_table
        return {
          {"error", "pg_stat_statements is not installed"},
          {"hint", "Add 'pg_stat_statements' to shared_preload_libraries in "
                   "postgresql.conf, restart PostgreSQL, then run: "
                   "CREATE EXTENSION pg_stat_statements;"}
        };
      }
      throw;
    }
  }

  const json table_bloat(const std::string& schema, const std::string& table_name, bool exact) {
    pqxx::work txn{conn};

    // pgstattuple() does a full sequential scan (real I/O on large tables);
    // pgstattuple_approx() uses the visibility map for a cheap estimate.
    // Field names are normalized across both so callers don't need to branch
    // on 'method' to know what to read; 'method' and (approx-only)
    // 'scanned_percent' disclose which one produced the numbers.
    std::string query = exact ? R"(
      SELECT CASE WHEN bt.table_len IS NOT NULL THEN
               JSONB_BUILD_OBJECT(
                 'method',             'exact',
                 'table_len',          bt.table_len,
                 'tuple_count',        bt.tuple_count,
                 'tuple_len',          bt.tuple_len,
                 'tuple_percent',      bt.tuple_percent,
                 'dead_tuple_count',   bt.dead_tuple_count,
                 'dead_tuple_len',     bt.dead_tuple_len,
                 'dead_tuple_percent', bt.dead_tuple_percent,
                 'free_space',         bt.free_space,
                 'free_percent',       bt.free_percent
               )
             END
      FROM pgstattuple(
             (SELECT oid FROM pg_class WHERE relnamespace = $1::regnamespace AND relname = $2)
           ) AS bt;
    )" : R"(
      SELECT CASE WHEN bt.table_len IS NOT NULL THEN
               JSONB_BUILD_OBJECT(
                 'method',             'approx',
                 'table_len',          bt.table_len,
                 'scanned_percent',    bt.scanned_percent,
                 'tuple_count',        bt.approx_tuple_count,
                 'tuple_len',          bt.approx_tuple_len,
                 'tuple_percent',      bt.approx_tuple_percent,
                 'dead_tuple_count',   bt.dead_tuple_count,
                 'dead_tuple_len',     bt.dead_tuple_len,
                 'dead_tuple_percent', bt.dead_tuple_percent,
                 'free_space',         bt.approx_free_space,
                 'free_percent',       bt.approx_free_percent
               )
             END
      FROM pgstattuple_approx(
             (SELECT oid FROM pg_class WHERE relnamespace = $1::regnamespace AND relname = $2)
           ) AS bt;
    )";

    try {
      pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table_name});

      if (!res.empty() && !res[0][0].is_null()) {
        return json::parse(res[0][0].as<std::string>());
      } else {
        return {};
      }
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42883") { // undefined_function
        return {
          {"error", "pgstattuple is not installed"},
          {"hint", "Run: CREATE EXTENSION pgstattuple;"}
        };
      }
      throw;
    }
  }

  const json database_size() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'database', current_database(),
               'size',     pg_database_size(current_database())
             );
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json check_key(const std::string& schema, const std::string& table_name, const json& values) {
    if (!values.is_array())
      throw std::runtime_error("values must be an array");

    pqxx::work txn{conn};

    // Step 1: fetch PK column names, types, and type schemas
    pqxx::result pk_res = txn.exec(
      "SELECT a.attname, t.typname, n.nspname "
      "FROM pg_constraint pk "
      "JOIN pg_attribute a ON a.attrelid = pk.conrelid AND a.attnum = ANY(pk.conkey) "
      "JOIN pg_type t ON t.oid = a.atttypid "
      "JOIN pg_namespace n ON n.oid = t.typnamespace "
      "WHERE pk.conrelid = (" +
        txn.quote(schema) + " || '.' || " + txn.quote(table_name) +
      ")::regclass AND pk.contype = 'p' "
      "ORDER BY array_position(pk.conkey, a.attnum)"
    );

    if (pk_res.empty())
      throw std::runtime_error("table " + schema + "." + table_name + " has no primary key");
    if (values.size() != static_cast<size_t>(pk_res.size()))
      throw std::runtime_error("primary key has " + std::to_string(pk_res.size()) +
                               " column(s), got " + std::to_string(values.size()) + " value(s)");

    // Step 2: validate value types
    static const std::set<std::string> INT_TYPES   = {"int2","int4","int8","oid","xid","cid"};
    static const std::set<std::string> FLOAT_TYPES = {"float4","float8","numeric","money"};
    static const std::set<std::string> STR_TYPES   = {"text","varchar","bpchar","char","name","citext"};

    for (pqxx::result::size_type i = 0; i < pk_res.size(); i++) {
      std::string col  = pk_res[i][0].as<std::string>();
      std::string type = pk_res[i][1].as<std::string>();
      const json& v    = values[static_cast<size_t>(i)];

      if (INT_TYPES.count(type)) {
        if (!v.is_number_integer())
          throw std::runtime_error("column \"" + col + "\" (" + type + ") expects an integer");
      } else if (FLOAT_TYPES.count(type)) {
        if (!v.is_number())
          throw std::runtime_error("column \"" + col + "\" (" + type + ") expects a number");
      } else if (STR_TYPES.count(type)) {
        if (!v.is_string())
          throw std::runtime_error("column \"" + col + "\" (" + type + ") expects a string");
      } else if (type == "uuid") {
        if (!v.is_string())
          throw std::runtime_error("column \"" + col + "\" (uuid) expects a string");
        std::string s = v.get<std::string>();
        if (s.size() != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-')
          throw std::runtime_error("column \"" + col + "\" (uuid) expects a UUID string");
      } else if (type == "bool") {
        if (!v.is_boolean())
          throw std::runtime_error("column \"" + col + "\" (bool) expects a boolean");
      } else {
        if (!v.is_string())
          throw std::runtime_error("column \"" + col + "\" (" + type + ") expects a string");
      }
    }

    // Step 3: build quoted identifier helper, WHERE clause, and params
    auto qi = [](const std::string& s) {
      std::string r = "\"";
      for (char c : s) { if (c == '"') r += "\"\""; else r += c; }
      return r + "\"";
    };

    std::string where;
    pqxx::params params;
    for (pqxx::result::size_type i = 0; i < pk_res.size(); i++) {
      if (i > 0) where += " AND ";
      std::string type_schema = pk_res[i][2].as<std::string>();
      std::string cast = type_schema != "pg_catalog"
        ? "::" + qi(type_schema) + "." + qi(pk_res[i][1].as<std::string>())
        : "";
      where += qi(pk_res[i][0].as<std::string>()) + " = $" + std::to_string(i + 1) + cast;
      const json& v = values[static_cast<size_t>(i)];
      if (v.is_number_integer())     params.append(v.get<int64_t>());
      else if (v.is_number_float())  params.append(v.get<double>());
      else if (v.is_string())        params.append(v.get<std::string>());
      else if (v.is_boolean())       params.append(v.get<bool>());
      else                           params.append(v.dump());
    }

    std::string sql = "SELECT EXISTS(SELECT 1 FROM " +
                      qi(schema) + "." + qi(table_name) +
                      " WHERE " + where + ")";

    pqxx::result res = pqxx_exec(txn, sql, params);
    bool exists = res[0][0].as<bool>();
    return {{"exists", exists}};
  }

  const json enums(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               t.typname,
               JSONB_BUILD_OBJECT(
                 'description', COALESCE(obj_description(t.oid, 'pg_type'), ''),
                 'values', values
               )
             )
      FROM pg_type AS t
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(e.enumlabel ORDER BY e.enumsortorder) AS values
          FROM pg_enum AS e
          WHERE e.enumtypid = t.oid
      ) ON true
      WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND t.typtype = 'e';
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json enum_detail(const std::string& schema, const std::string& enum_name) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'description', COALESCE(obj_description(t.oid, 'pg_type'), ''),
               'values', values,
               'used_by_columns', COALESCE(used_by_columns, '[]'::jsonb)
             )
      FROM pg_type AS t
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(e.enumlabel ORDER BY e.enumsortorder) AS values
          FROM pg_enum AS e
          WHERE e.enumtypid = t.oid
      ) ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'table', c.relnamespace::regnamespace::text || '.' || c.relname,
                   'column', a.attname
                 )) AS used_by_columns
          FROM pg_attribute AS a
          JOIN pg_class AS c ON c.oid = a.attrelid
          WHERE a.atttypid = t.oid
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND c.relkind IN ('r', 'p', 'm', 'v')
      ) ON true
      WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND t.typname = $2
        AND t.typtype = 'e';
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, enum_name});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json search_enums(const std::string& web_search) {
    if (web_search.empty()) {
      return {};
    }

    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               t.typnamespace::regnamespace::text || '.' || t.typname,
               JSONB_BUILD_OBJECT(
                 'description', COALESCE(obj_description(t.oid, 'pg_type'), ''),
                 'values', values
               )
             )
      FROM pg_type AS t
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(e.enumlabel ORDER BY e.enumsortorder) AS values,
                 STRING_AGG(e.enumlabel, ' ') AS values_text
          FROM pg_enum AS e
          WHERE e.enumtypid = t.oid
      ) ON true
      WHERE t.typtype = 'e'
        AND t.typnamespace NOT IN (
            SELECT oid FROM pg_namespace
            WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema')
        AND (
            TO_TSVECTOR('english',
              REGEXP_REPLACE(REGEXP_REPLACE(t.typname, '_', ' ', 'g'), '([[:upper:]])', ' \1', 'g'))
              @@ websearch_to_tsquery('english', $1)
         OR TO_TSVECTOR('english', COALESCE(obj_description(t.oid, 'pg_type'), ''))
              @@ websearch_to_tsquery('english', $1)
         OR TO_TSVECTOR('english', COALESCE(values_text, ''))
              @@ websearch_to_tsquery('english', $1)
        );
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{web_search});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json types(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               t.typname,
               JSONB_BUILD_OBJECT(
                 'kind',            CASE t.typtype WHEN 'c' THEN 'composite' WHEN 'd' THEN 'domain' WHEN 'r' THEN 'range' END,
                 'description',     COALESCE(obj_description(t.oid, 'pg_type'), ''),
                 'attributes',      attributes,
                 'base_type',       CASE WHEN t.typtype = 'd' THEN format_type(t.typbasetype, t.typtypmod) END,
                 'not_null',        CASE WHEN t.typtype = 'd' THEN t.typnotnull END,
                 'default',         CASE WHEN t.typtype = 'd' THEN pg_get_expr(t.typdefaultbin, 0) END,
                 'constraints',     COALESCE(constraints, '[]'::jsonb),
                 'subtype',         rng.subtype,
                 'subtype_diff',    rng.subtype_diff,
                 'multirange_type', rng.multirange_type
               )
             )
      FROM pg_type AS t
      LEFT JOIN pg_class AS ct ON ct.oid = t.typrelid
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT('name', a.attname, 'type', format_type(a.atttypid, a.atttypmod))
                            ORDER BY a.attnum) AS attributes
          FROM pg_attribute AS a
          WHERE a.attrelid = t.typrelid
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND t.typtype = 'c'
      ) ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(pg_get_constraintdef(con.oid)) AS constraints
          FROM pg_constraint AS con
          WHERE con.contypid = t.oid
            AND t.typtype = 'd'
      ) ON true
      LEFT JOIN LATERAL (
          SELECT r.rngsubtype::regtype::text AS subtype,
                 CASE WHEN r.rngsubdiff != 0 THEN r.rngsubdiff::regproc::text END AS subtype_diff,
                 r.rngmultitypid::regtype::text AS multirange_type
          FROM pg_range AS r
          WHERE r.rngtypid = t.oid
            AND t.typtype = 'r'
      ) rng ON true
      WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND (
          t.typtype = 'd'
          OR (t.typtype = 'c' AND ct.relkind = 'c')
          OR t.typtype = 'r'
        );
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json type_detail(const std::string& schema, const std::string& type_name) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'kind',            CASE t.typtype WHEN 'c' THEN 'composite' WHEN 'd' THEN 'domain' WHEN 'r' THEN 'range' END,
               'description',     COALESCE(obj_description(t.oid, 'pg_type'), ''),
               'attributes',      attributes,
               'base_type',       CASE WHEN t.typtype = 'd' THEN format_type(t.typbasetype, t.typtypmod) END,
               'not_null',        CASE WHEN t.typtype = 'd' THEN t.typnotnull END,
               'default',         CASE WHEN t.typtype = 'd' THEN pg_get_expr(t.typdefaultbin, 0) END,
               'constraints',     COALESCE(constraints, '[]'::jsonb),
               'subtype',         rng.subtype,
               'subtype_diff',    rng.subtype_diff,
               'multirange_type', rng.multirange_type,
               'used_by_columns', COALESCE(used_by_columns, '[]'::jsonb)
             )
      FROM pg_type AS t
      LEFT JOIN pg_class AS ct ON ct.oid = t.typrelid
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT('name', a.attname, 'type', format_type(a.atttypid, a.atttypmod))
                            ORDER BY a.attnum) AS attributes
          FROM pg_attribute AS a
          WHERE a.attrelid = t.typrelid
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND t.typtype = 'c'
      ) ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(pg_get_constraintdef(con.oid)) AS constraints
          FROM pg_constraint AS con
          WHERE con.contypid = t.oid
            AND t.typtype = 'd'
      ) ON true
      LEFT JOIN LATERAL (
          SELECT r.rngsubtype::regtype::text AS subtype,
                 CASE WHEN r.rngsubdiff != 0 THEN r.rngsubdiff::regproc::text END AS subtype_diff,
                 r.rngmultitypid::regtype::text AS multirange_type
          FROM pg_range AS r
          WHERE r.rngtypid = t.oid
            AND t.typtype = 'r'
      ) rng ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'table', c.relnamespace::regnamespace::text || '.' || c.relname,
                   'column', a.attname
                 )) AS used_by_columns
          FROM pg_attribute AS a
          JOIN pg_class AS c ON c.oid = a.attrelid
          WHERE a.atttypid = t.oid
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND c.relkind IN ('r', 'p', 'm', 'v')
      ) ON true
      WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND t.typname = $2
        AND (
          t.typtype = 'd'
          OR (t.typtype = 'c' AND ct.relkind = 'c')
          OR t.typtype = 'r'
        );
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, type_name});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json roles() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               r.rolname,
               JSONB_BUILD_OBJECT(
                 'kind',             CASE WHEN r.rolcanlogin THEN 'login' ELSE 'group' END,
                 'description',      COALESCE(shobj_description(r.oid, 'pg_authid'), ''),
                 'superuser',        r.rolsuper,
                 'inherit',          r.rolinherit,
                 'create_role',      r.rolcreaterole,
                 'create_db',        r.rolcreatedb,
                 'replication',      r.rolreplication,
                 'bypass_rls',       r.rolbypassrls,
                 'connection_limit', r.rolconnlimit,
                 'valid_until',      r.rolvaliduntil,
                 'member_of',        COALESCE(member_of, '[]'::jsonb)
               )
             )
      FROM pg_roles AS r
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(g.rolname ORDER BY g.rolname) AS member_of
          FROM pg_auth_members AS m
          JOIN pg_roles AS g ON g.oid = m.roleid
          WHERE m.member = r.oid
      ) ON true;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json foreign_tables(const std::string& schema) {
    pqxx::work txn{conn};

    // Deliberately excludes user mapping options: pg_user_mapping/pg_user_mappings
    // expose credentials (e.g. password) in cleartext to superusers, which would
    // contradict this tool's catalog-only, low-data-leak-risk design.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               c.relname,
               JSONB_BUILD_OBJECT(
                 'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
                 'server',      fs.srvname,
                 'fdw',         fdw.fdwname,
                 'options',     ft.ftoptions,
                 'columns',     COALESCE(columns, '{}'::jsonb)
               )
             )
      FROM pg_class AS c
      JOIN pg_foreign_table AS ft ON ft.ftrelid = c.oid
      JOIN pg_foreign_server AS fs ON fs.oid = ft.ftserver
      JOIN pg_foreign_data_wrapper AS fdw ON fdw.oid = fs.srvfdw
      LEFT JOIN LATERAL (
          SELECT JSONB_OBJECT_AGG(a.attname, format_type(a.atttypid, a.atttypmod)) AS columns
          FROM pg_attribute AS a
          WHERE a.attrelid = c.oid
            AND a.attnum > 0
            AND NOT a.attisdropped
      ) ON true
      WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1);
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json foreign_servers() {
    pqxx::work txn{conn};

    // Server options only (host/port/dbname-style); never user mapping credentials.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               fs.srvname,
               JSONB_BUILD_OBJECT(
                 'fdw',     fdw.fdwname,
                 'owner',   fs.srvowner::regrole::text,
                 'type',    fs.srvtype,
                 'version', fs.srvversion,
                 'options', fs.srvoptions
               )
             )
      FROM pg_foreign_server AS fs
      JOIN pg_foreign_data_wrapper AS fdw ON fdw.oid = fs.srvfdw;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json tablespaces() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               spcname,
               JSONB_BUILD_OBJECT(
                 'owner',       spcowner::regrole::text,
                 'location',    COALESCE(NULLIF(pg_tablespace_location(oid), ''), '(default)'),
                 'options',     spcoptions,
                 'description', COALESCE(obj_description(oid, 'pg_tablespace'), '')
               )
             )
      FROM pg_tablespace;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json collations(const std::string& schema) {
    pqxx::work txn{conn};

    // Restrict to collations usable in this database's encoding (collencoding = -1
    // means "any encoding"); otherwise libc ships many same-named rows per locale,
    // one per encoding variant, which would silently collide in the result map.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               collname,
               JSONB_BUILD_OBJECT(
                 'provider',      CASE collprovider WHEN 'd' THEN 'default' WHEN 'c' THEN 'libc'
                                                     WHEN 'i' THEN 'icu' WHEN 'b' THEN 'builtin' END,
                 'collate',       collcollate,
                 'ctype',         collctype,
                 'deterministic', collisdeterministic,
                 'description',   COALESCE(obj_description(oid, 'pg_collation'), '')
               )
             )
      FROM pg_collation
      WHERE collnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = current_database()));
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json event_triggers() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               evtname,
               JSONB_BUILD_OBJECT(
                 'event',       evtevent,
                 'function',    evtfoid::regproc::text,
                 'enabled',     evtenabled != 'D',
                 'tags',        evttags,
                 'owner',       evtowner::regrole::text,
                 'description', COALESCE(obj_description(oid, 'pg_event_trigger'), '')
               )
             )
      FROM pg_event_trigger;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json publications() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               p.pubname,
               JSONB_BUILD_OBJECT(
                 'owner',      p.pubowner::regrole::text,
                 'all_tables', p.puballtables,
                 'insert',     p.pubinsert,
                 'update',     p.pubupdate,
                 'delete',     p.pubdelete,
                 'truncate',   p.pubtruncate,
                 'tables',     COALESCE(tables, '[]'::jsonb)
               )
             )
      FROM pg_publication AS p
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(pt.schemaname || '.' || pt.tablename) AS tables
          FROM pg_publication_tables AS pt
          WHERE pt.pubname = p.pubname
      ) ON true;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json subscriptions() {
    pqxx::work txn{conn};

    // Deliberately excludes subconninfo: pg_subscription is a shared (cluster-wide)
    // catalog and that column holds the full connection string, which may embed a
    // password. Scoped to the current database via subdbid to avoid leaking
    // subscriptions that belong to other databases in the same cluster.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               subname,
               JSONB_BUILD_OBJECT(
                 'owner',              subowner::regrole::text,
                 'enabled',            subenabled,
                 'publications',       subpublications,
                 'slot_name',          subslotname,
                 'synchronous_commit', subsynccommit,
                 'two_phase',          subtwophasestate,
                 'binary',             subbinary
               )
             )
      FROM pg_subscription
      WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database());
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json languages() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               l.lanname,
               JSONB_BUILD_OBJECT(
                 'owner',       l.lanowner::regrole::text,
                 'trusted',     l.lanpltrusted,
                 'procedural',  l.lanispl,
                 'handler',     CASE WHEN l.lanplcallfoid != 0 THEN l.lanplcallfoid::regproc::text END,
                 'description', COALESCE(obj_description(l.oid, 'pg_language'), '')
               )
             )
      FROM pg_language AS l;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json extended_statistics(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               s.stxname,
               JSONB_BUILD_OBJECT(
                 'table',       s.stxrelid::regclass::text,
                 'columns',     COALESCE(cols, '[]'::jsonb),
                 'kinds',       COALESCE(kinds, '[]'::jsonb),
                 'description', COALESCE(obj_description(s.oid, 'pg_statistic_ext'), '')
               )
             )
      FROM pg_statistic_ext AS s
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(attname ORDER BY attnum) AS cols
          FROM pg_attribute
          WHERE attrelid = s.stxrelid
            AND attnum = ANY(s.stxkeys)
      ) ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(CASE k WHEN 'd' THEN 'ndistinct' WHEN 'f' THEN 'dependencies'
                                   WHEN 'm' THEN 'mcv' WHEN 'e' THEN 'expressions' END) AS kinds
          FROM unnest(s.stxkind) AS k
      ) ON true
      WHERE s.stxnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1);
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json operators(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               o.oprname || '(' || COALESCE(NULLIF(o.oprleft, 0)::regtype::text, 'NONE') || ',' ||
                                    COALESCE(NULLIF(o.oprright, 0)::regtype::text, 'NONE') || ')',
               JSONB_BUILD_OBJECT(
                 'left_type',   NULLIF(o.oprleft, 0)::regtype::text,
                 'right_type',  NULLIF(o.oprright, 0)::regtype::text,
                 'result_type', o.oprresult::regtype::text,
                 'function',    o.oprcode::regproc::text,
                 'description', COALESCE(obj_description(o.oid, 'pg_operator'), '')
               )
             )
      FROM pg_operator AS o
      WHERE o.oprnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1);
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json operator_classes(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               oc.opcname || ' (' || am.amname || ')',
               JSONB_BUILD_OBJECT(
                 'access_method', am.amname,
                 'input_type',    oc.opcintype::regtype::text,
                 'default',       oc.opcdefault,
                 'description',   COALESCE(obj_description(oc.oid, 'pg_opclass'), '')
               )
             )
      FROM pg_opclass AS oc
      JOIN pg_am AS am ON am.oid = oc.opcmethod
      WHERE oc.opcnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1);
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json access_methods() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               amname,
               JSONB_BUILD_OBJECT(
                 'type',        CASE amtype WHEN 'i' THEN 'index' WHEN 't' THEN 'table' END,
                 'handler',     amhandler::regproc::text,
                 'description', COALESCE(obj_description(oid, 'pg_am'), '')
               )
             )
      FROM pg_am;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json casts() {
    pqxx::work txn{conn};

    // Filtered to casts involving at least one user-defined type; unfiltered this
    // returns hundreds of built-in numeric/string coercions that are pure noise
    // for exploring an application schema.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               st.oid::regtype::text || '->' || tt.oid::regtype::text,
               JSONB_BUILD_OBJECT(
                 'source',   st.oid::regtype::text,
                 'target',   tt.oid::regtype::text,
                 'context',  CASE ca.castcontext WHEN 'e' THEN 'explicit' WHEN 'a' THEN 'assignment' WHEN 'i' THEN 'implicit' END,
                 'method',   CASE ca.castmethod WHEN 'f' THEN 'function' WHEN 'i' THEN 'inout' WHEN 'b' THEN 'binary coercible' END,
                 'function', CASE WHEN ca.castfunc != 0 THEN ca.castfunc::regproc::text END
               )
             )
      FROM pg_cast AS ca
      JOIN pg_type AS st ON st.oid = ca.castsource
      JOIN pg_type AS tt ON tt.oid = ca.casttarget
      WHERE st.typnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg\_%' ESCAPE '\' OR nspname = 'information_schema')
         OR tt.typnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg\_%' ESCAPE '\' OR nspname = 'information_schema');
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json text_search_configs(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               c.cfgname,
               JSONB_BUILD_OBJECT(
                 'parser',      p.prsname,
                 'description', COALESCE(obj_description(c.oid, 'pg_ts_config'), ''),
                 'mappings',    COALESCE(mp.mappings, '{}'::jsonb)
               )
             )
      FROM pg_ts_config AS c
      JOIN pg_ts_parser AS p ON p.oid = c.cfgparser
      LEFT JOIN LATERAL (
          SELECT JSONB_OBJECT_AGG(tt.alias, dd.dicts) AS mappings
          FROM (SELECT DISTINCT cm.maptokentype FROM pg_ts_config_map AS cm WHERE cm.mapcfg = c.oid) m
          JOIN LATERAL ts_token_type(c.cfgparser) tt ON tt.tokid = m.maptokentype
          LEFT JOIN LATERAL (
              SELECT JSONB_AGG(d.dictname ORDER BY cm2.mapseqno) AS dicts
              FROM pg_ts_config_map AS cm2
              JOIN pg_ts_dict AS d ON d.oid = cm2.mapdict
              WHERE cm2.mapcfg = c.oid
                AND cm2.maptokentype = m.maptokentype
          ) dd ON true
      ) mp ON true
      WHERE c.cfgnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1);
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json sequences(const std::string& schema) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(
               ps.sequencename,
               JSONB_BUILD_OBJECT(
                 'description',  COALESCE(obj_description(c.oid, 'pg_class'), ''),
                 'data_type',    ps.data_type,
                 'start_value',  ps.start_value,
                 'min_value',    ps.min_value,
                 'max_value',    ps.max_value,
                 'increment_by', ps.increment_by,
                 'cycle',        ps.cycle,
                 'cache_size',   ps.cache_size,
                 'last_value',   ps.last_value,
                 'owned_by',     owned_by
               )
             )
      FROM pg_sequences AS ps
      JOIN pg_class AS c ON c.relname = ps.sequencename
                         AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = ps.schemaname)
      LEFT JOIN LATERAL (
          SELECT oc.relname || '.' || a.attname AS owned_by
          FROM pg_depend AS d
          JOIN pg_class AS oc ON oc.oid = d.refobjid
          JOIN pg_attribute AS a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
          WHERE d.objid = c.oid
            AND d.deptype IN ('a', 'i')
          LIMIT 1
      ) owned ON true
      WHERE ps.schemaname = $1;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json table(const std::string& schema, const std::string& table) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'table', c.relname, 'rows', c.reltuples, 'size', pg_table_size(c.oid), 'indexes_size', pg_indexes_size(c.oid),
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'definition', CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END,
               'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan, 'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
               'n_mod_since_analyze', s.n_mod_since_analyze, 'n_ins_since_vacuum', s.n_ins_since_vacuum,
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum), 'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
               'reloptions', c.reloptions,
               'columns', columns,
               'toast', CASE WHEN c.reltoastrelid != 0 THEN
                          JSONB_BUILD_OBJECT(
                            'name',       tc.relname,
                            'size',       pg_relation_size(c.reltoastrelid),
                            'index_size', pg_indexes_size(c.reltoastrelid))
                        END,
               'primary_key', COALESCE(primary_key, '[]'::jsonb),
               'indexes', COALESCE(indexes, '{}'::jsonb),
               'constraints', COALESCE(constraints, '{}'::jsonb),
               'foreign_keys', COALESCE(foreign_keys, '{}'::jsonb),
               'referenced_by', COALESCE(referenced_by, '{}'::jsonb),
               'triggers', COALESCE(triggers, '{}'::jsonb),
               'rules', COALESCE(rules, '{}'::jsonb),
               'row_level_security', JSONB_BUILD_OBJECT(
                 'enabled', c.relrowsecurity, 'forced', c.relforcerowsecurity),
               'policies', COALESCE(policies, '{}'::jsonb),
               'roles', COALESCE(roles, '{}'::jsonb))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN pg_class AS tc ON tc.oid = c.reltoastrelid
      LEFT JOIN LATERAL (SELECT JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(a.attname,
                        JSONB_BUILD_OBJECT(
                         'description', col_description(c.oid, attnum),
                         'type', t.typname,
                         'format_type', format_type(a.atttypid, a.atttypmod),
                         'size', NULLIF(a.attlen, -1),
                         'not_null', a.attnotnull,
                         'default', pg_get_expr(ad.adbin, ad.adrelid),
                         'storage', CASE a.attstorage WHEN 'p' THEN 'plain' WHEN 'e' THEN 'external'
                                                       WHEN 'm' THEN 'main' WHEN 'x' THEN 'extended' END,
                         'compression', CASE a.attcompression WHEN 'p' THEN 'pglz' WHEN 'l' THEN 'lz4' ELSE 'default' END,
                         'null_frac', ps.null_frac,
                         'avg_width', ps.avg_width,
                         'n_distinct', ps.n_distinct,
                         'physical_order_correlation', ps.correlation,
                         'most_common_vals', ps.most_common_vals,
                         'most_common_freqs', ps.most_common_freqs))) AS columns
                       FROM pg_attribute AS a
                       JOIN pg_type AS t ON t.oid = a.atttypid
                       LEFT JOIN pg_attrdef AS ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                       LEFT JOIN pg_stats AS ps ON ps.schemaname = $1 AND ps.tablename = $2 AND ps.attname = a.attname
                       WHERE attnum > 0
                         AND attrelid = c.oid
                         AND NOT attisdropped) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(indexname,
                          JSONB_BUILD_OBJECT(
                           'definition', indexdef, 'size', pg_relation_size(si.indexrelid), 'index_uses', idx_scan, 'last_use', last_idx_scan)) AS indexes
                         FROM pg_indexes AS i
                         JOIN pg_stat_user_indexes si ON si.indexrelname = i.indexname
                                                      AND si.schemaname = i.schemaname
                                                      AND si.relname = i.tablename
                         WHERE i.schemaname = $1
                           AND i.tablename = c.relname) ON true
      LEFT JOIN LATERAL (SELECT JSONB_AGG(a.attname ORDER BY array_position(pk.conkey, a.attnum)) AS primary_key
                         FROM pg_constraint pk
                         JOIN pg_attribute a ON a.attrelid = pk.conrelid AND a.attnum = ANY(pk.conkey)
                         WHERE pk.conrelid = c.oid AND pk.contype = 'p') ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(conname,
                          JSONB_BUILD_OBJECT(
                           'definition', pg_get_constraintdef(oid))) AS constraints
                         FROM pg_constraint
                         WHERE conrelid = c.oid
                           AND contype != 'f') ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(fk.conname,
                          JSONB_BUILD_OBJECT(
                           'target_table', fk.confrelid::regclass::text,
                           'source_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.conkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.conrelid AND a.attnum = ANY(fk.conkey)),
                           'target_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.confkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.confrelid AND a.attnum = ANY(fk.confkey)),
                           'definition', pg_get_constraintdef(fk.oid))) AS foreign_keys
                         FROM pg_constraint fk
                         WHERE fk.conrelid = c.oid
                           AND fk.contype = 'f') ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(fk.conrelid::regclass::text || '.' || fk.conname,
                          JSONB_BUILD_OBJECT(
                           'source_table', fk.conrelid::regclass::text,
                           'source_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.conkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.conrelid AND a.attnum = ANY(fk.conkey)),
                           'target_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.confkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.confrelid AND a.attnum = ANY(fk.confkey)),
                           'definition', pg_get_constraintdef(fk.oid))) AS referenced_by
                         FROM pg_constraint fk
                         WHERE fk.confrelid = c.oid
                           AND fk.contype = 'f') ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(t.tgname,
                          JSONB_BUILD_OBJECT(
                           'function',    p.proname,
                           'language',    l.lanname,
                           'return_type', pg_get_function_result(p.oid),
                           'description', COALESCE(obj_description(p.oid, 'pg_proc'), ''),
                           'timing',      CASE
                                            WHEN (t.tgtype & 2)  <> 0 THEN 'BEFORE'
                                            WHEN (t.tgtype & 64) <> 0 THEN 'INSTEAD OF'
                                            ELSE 'AFTER'
                                          END,
                           'events',      array_to_string(ARRAY[
                                            CASE WHEN (t.tgtype & 4)  <> 0 THEN 'INSERT'   END,
                                            CASE WHEN (t.tgtype & 8)  <> 0 THEN 'DELETE'   END,
                                            CASE WHEN (t.tgtype & 16) <> 0 THEN 'UPDATE'   END,
                                            CASE WHEN (t.tgtype & 32) <> 0 THEN 'TRUNCATE' END
                                          ]::text[], ' OR '),
                           'when',        CASE WHEN t.tgqual IS NOT NULL
                                            THEN (regexp_match(pg_get_triggerdef(t.oid), 'WHEN [(](.+)[)] EXECUTE'))[1]
                                            ELSE NULL END)) AS triggers
                         FROM   pg_trigger AS t
                         JOIN   pg_proc AS p ON p.oid = t.tgfoid
                         JOIN   pg_language AS l ON l.oid = p.prolang
                         WHERE  t.tgrelid = c.oid
                           AND  NOT t.tgisinternal) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(r.rulename,
                          JSONB_BUILD_OBJECT(
                           'event',      CASE r.ev_type WHEN '1' THEN 'SELECT' WHEN '2' THEN 'UPDATE'
                                                         WHEN '3' THEN 'INSERT' WHEN '4' THEN 'DELETE' END,
                           'instead',    r.is_instead,
                           'definition', pg_get_ruledef(r.oid))) AS rules
                         FROM pg_rewrite AS r
                         WHERE r.ev_class = c.oid
                           AND r.rulename != '_RETURN') ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(pol.polname,
                          JSONB_BUILD_OBJECT(
                           'command',     CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT'
                                                           WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' ELSE 'ALL' END,
                           'permissive',  pol.polpermissive,
                           'roles',       COALESCE((SELECT JSONB_AGG(rolname ORDER BY rolname)
                                                     FROM pg_roles WHERE oid = ANY(pol.polroles)), '["PUBLIC"]'::jsonb),
                           'using',       pg_get_expr(pol.polqual, pol.polrelid),
                           'with_check',  pg_get_expr(pol.polwithcheck, pol.polrelid))) AS policies
                         FROM pg_policy AS pol
                         WHERE pol.polrelid = c.oid) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(c.relacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) ON true
      WHERE c.relnamespace = $1::regnamespace
        AND c.relname = $2;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_table = res[0][0].as<std::string>();
      return json::parse(pgsql_table);
    } else {
      return {};
    }
  }

  void initialize(const json& id) {
    send_response(id, {
        {"protocolVersion", "2024-11-05"},
        {"capabilities", {
            {"tools", json::object()}
	  }},
        {"serverInfo", {{"name", "pg-licht-cpp"}, {"version", PGLICHT_VERSION}}}
      });
  }

  void handle_request(const json& req) {
    std::string method = req.value("method", "");

    if (method == "initialize") {
      initialize(req["id"]);
    }
    else if (method == "notifications/initialized") {
      return;
    }
    else if (method == "tools/list") {
      send_response(req["id"], get_tools_list());
    }
    else if (method == "tools/call") {
      auto params = req.value("params", json::object());
      std::string tool_name = params.value("name", "");
      auto arguments = params.value("arguments", json::object());

      try {
	json result_content;

	if (tool_name == "listSchemas") {
	  result_content = schemas();
	}
	else if (tool_name == "listTables") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = tables(target_schema);
	}
	else if (tool_name == "tableDetails") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string target_table = arguments.contains("table") ? arguments["table"].get<std::string>() : "";
	  result_content = table(target_schema, target_table);
	}
	else if (tool_name == "searchTables") {
	  std::string web_search = arguments.contains("web_search") ? arguments["web_search"].get<std::string>() : "";
	  result_content = search(web_search);
	}
	else if (tool_name == "listFunctions") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = functions(target_schema);
	}
	else if (tool_name == "functionDetails") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string func_name = arguments.contains("function") ? arguments["function"].get<std::string>() : "";
	  result_content = function_detail(target_schema, func_name);
	}
	else if (tool_name == "searchFunctions") {
	  std::string web_search = arguments.contains("web_search") ? arguments["web_search"].get<std::string>() : "";
	  result_content = search_functions(web_search);
	}
	else if (tool_name == "listEnums") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = enums(target_schema);
	}
	else if (tool_name == "enumDetails") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string enum_name = arguments.contains("enum") ? arguments["enum"].get<std::string>() : "";
	  result_content = enum_detail(target_schema, enum_name);
	}
	else if (tool_name == "searchEnums") {
	  std::string web_search = arguments.contains("web_search") ? arguments["web_search"].get<std::string>() : "";
	  result_content = search_enums(web_search);
	}
	else if (tool_name == "listTypes") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = types(target_schema);
	}
	else if (tool_name == "typeDetails") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string type_name = arguments.contains("type") ? arguments["type"].get<std::string>() : "";
	  result_content = type_detail(target_schema, type_name);
	}
	else if (tool_name == "listRoles") {
	  result_content = roles();
	}
	else if (tool_name == "listForeignTables") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = foreign_tables(target_schema);
	}
	else if (tool_name == "listForeignServers") {
	  result_content = foreign_servers();
	}
	else if (tool_name == "listTablespaces") {
	  result_content = tablespaces();
	}
	else if (tool_name == "listCollations") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = collations(target_schema);
	}
	else if (tool_name == "listEventTriggers") {
	  result_content = event_triggers();
	}
	else if (tool_name == "listPublications") {
	  result_content = publications();
	}
	else if (tool_name == "listSubscriptions") {
	  result_content = subscriptions();
	}
	else if (tool_name == "listLanguages") {
	  result_content = languages();
	}
	else if (tool_name == "listExtendedStatistics") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = extended_statistics(target_schema);
	}
	else if (tool_name == "listOperators") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = operators(target_schema);
	}
	else if (tool_name == "listOperatorClasses") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = operator_classes(target_schema);
	}
	else if (tool_name == "listAccessMethods") {
	  result_content = access_methods();
	}
	else if (tool_name == "listCasts") {
	  result_content = casts();
	}
	else if (tool_name == "listTextSearchConfigs") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = text_search_configs(target_schema);
	}
	else if (tool_name == "listSequences") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  result_content = sequences(target_schema);
	}
	else if (tool_name == "listExtensions") {
	  result_content = extensions();
	}
	else if (tool_name == "databaseSize") {
	  result_content = database_size();
	}
	else if (tool_name == "serverSettings") {
	  result_content = server_settings();
	}
	else if (tool_name == "currentActivity") {
	  result_content = activity();
	}
	else if (tool_name == "currentLocks") {
	  result_content = locks();
	}
	else if (tool_name == "replicationSlots") {
	  result_content = replication_slots();
	}
	else if (tool_name == "databaseStats") {
	  result_content = database_stats();
	}
	else if (tool_name == "statementStats") {
	  int limit = arguments.contains("limit") ? arguments["limit"].get<int>() : 20;
	  result_content = statement_stats(limit);
	}
	else if (tool_name == "tableBloat") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string target_table  = arguments.contains("table")  ? arguments["table"].get<std::string>()  : "";
	  bool exact = arguments.contains("exact") ? arguments["exact"].get<bool>() : false;
	  result_content = table_bloat(target_schema, target_table, exact);
	}
	else if (tool_name == "checkKey") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string target_table  = arguments.contains("table")  ? arguments["table"].get<std::string>()  : "";
	  json vals = arguments.contains("values") ? arguments["values"] : json::array();
	  result_content = check_key(target_schema, target_table, vals);
	}
	else {
	  send_error(req["id"], -32601, "Tool not found: " + tool_name);
	  return;
	}

	send_response(req["id"], {
	    {"content", {{
                  {"type", "text"},
                  {"text", result_content.dump(2)}
		}}},
	    {"isError", false}
          });

      } catch (const std::exception& e) {
	send_response(req["id"], {
	    {"content", {{{"type", "text"}, {"text", std::string("Execution error: ") + e.what()}}}},
	    {"isError", true}
          });
      }
    }
    else {
      if (req.contains("id")) {
        send_error(req["id"], -32601, "Method not available");
      }
    }
  }

  void send_response(const json& id, const json& result) {
    json res = {{"jsonrpc", "2.0"}, {"id", id}, {"result", result}};
    std::cout << res.dump() << std::endl;
  }

  void send_error(const json& id, int code, const std::string& msg) {
    json err = {{"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", msg}}}};
    std::cout << err.dump() << std::endl;
  }
};
