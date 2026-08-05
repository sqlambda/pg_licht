#pragma once

#include <cctype>
#include <iostream>
#include <set>
#include <string>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include "config.h"
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

// One unit of work: connect, open a READ ONLY transaction, roll back and
// disconnect on scope exit.
//
// Why connect per call rather than hold one connection: the target deployment
// is PgBouncer in transaction mode (pool_mode=transaction,
// server_reset_query=DISCARD ALL). There the server connection is handed back
// to the pool at COMMIT and wiped, so *session* state does not survive between
// transactions. An earlier version set `default_transaction_read_only` once at
// startup and committed; behind such a pooler that setting was silently
// discarded and every later call ran without the read-only guard.
//
// So nothing here relies on session state. The read-only guarantee and the
// statement timeout are both transaction-scoped, which is exactly the scope a
// transaction pooler preserves. The GUC cannot be pushed into the connection
// string either -- PgBouncer rejects it outright:
//   FATAL: unsupported startup parameter in options: default_transaction_read_only
class Session {
public:
  explicit Session(const pglicht::ConnConfig& cfg, int timeout_ms = 0)
    : conn_(cfg.conninfo), txn_(conn_) {
    // Must be the first statement in the transaction. This is the real write
    // backstop: it catches writes a query plan cannot reveal, such as a
    // VOLATILE function that modifies data, aborting with SQLSTATE 25006.
    txn_.exec("SET TRANSACTION READ ONLY");
    if (timeout_ms > 0) {
      // set_config(..., is_local => true) is SET LOCAL: transaction-scoped,
      // and binds the value as a parameter instead of concatenating it.
      pqxx_exec(txn_, "SELECT set_config('statement_timeout', $1, true)",
                pqxx::params{std::to_string(timeout_ms)});
    }
  }

  pqxx::work& txn() { return txn_; }

  // Server version as an integer (e.g. 160004 for 16.4), from the startup
  // handshake -- no round trip. Used to gate catalog columns and SQL features
  // that don't exist on older majors; correct per connection, which matters
  // when different configured connections point at different-version servers.
  int server_version() const { return conn_.server_version(); }

  // txn_ is destroyed before conn_, rolling back; conn_ then disconnects.
  ~Session() = default;

  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;

private:
  pqxx::connection conn_;
  pqxx::work txn_;
};

class PostgresMCPServer {
public:
  // Single-connection form: DATABASE_URL, an explicit conninfo, or argv[1].
  explicit PostgresMCPServer(const std::string& conn_str)
    : registry_(pglicht::ConnectionRegistry::from_url(conn_str, app_name())) {
    probe();
  }

  // Multi-connection form, from an INI file of named sections.
  explicit PostgresMCPServer(pglicht::ConnectionRegistry registry)
    : registry_(std::move(registry)) {
    probe();
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

  const json call_wraparound_status(const std::string& schema, int limit) {
    return wraparound_status(schema, limit);
  }
  const json call_duplicate_indexes(const std::string& schema, const std::string& table_name) {
    return duplicate_indexes(schema, table_name);
  }
  const json call_checkpoint_stats() { return checkpoint_stats(); }
  const json call_table_io_stats(const std::string& schema, const std::string& table_name, int limit) {
    return table_io_stats(schema, table_name, limit);
  }
  const json call_host_capacity(long long ram_mb, int vcpus, const std::string& storage) {
    return host_capacity(ram_mb, vcpus, storage);
  }

  const json call_connections() { return connections(); }
  const json call_explain_query(const std::string& queryid, const std::string& sql,
                                const json& params, bool analyze, int timeout_ms) {
    return explain_query(queryid, sql, params, analyze, timeout_ms);
  }

private:
  pglicht::ConnectionRegistry registry_;
  // Which configured connection the in-flight tool call targets. Resolved once
  // per request by handle_request so the ~40 query methods keep their existing
  // signatures. The server reads stdin line by line, so only one call is ever
  // in flight and a member is safe.
  std::string active_;
  unsigned long long explain_seq_ = 0;

  static std::string app_name() {
    return std::string("pg-licht-cpp/") + PGLICHT_VERSION;
  }

  const pglicht::ConnConfig& active_cfg() const { return registry_.get(active_); }

  // Fail fast on an unusable default connection, matching the previous
  // behaviour where the constructor connected eagerly.
  void probe() {
    Session s{registry_.get(registry_.default_name())};
    s.txn().exec("SELECT 1");
  }

  const json connections() {
    json out = json::array();
    for (const auto& name : registry_.names()) {
      const auto& c = registry_.get(name);
      json entry = {{"name", name}, {"default", name == registry_.default_name()}};
      // Only non-secret fields. A service name is reported as-is and never
      // expanded: the service file may hold a password, and resolving it here
      // would leak it into tool output.
      if (!c.service.empty()) entry["service"] = c.service;
      if (!c.host.empty())    entry["host"]    = c.host;
      if (!c.port.empty())    entry["port"]    = c.port;
      if (!c.dbname.empty())  entry["dbname"]  = c.dbname;
      if (!c.user.empty())    entry["user"]    = c.user;
      // Host capacity, so a caller can see at a glance which connections still
      // need it injected before hostCapacity can compute anything.
      if (c.capacity.ram_mb > 0)        entry["host_ram_mb"]  = c.capacity.ram_mb;
      if (c.capacity.vcpus > 0)         entry["host_vcpus"]   = c.capacity.vcpus;
      if (!c.capacity.storage.empty())  entry["host_storage"] = c.capacity.storage;
      if (!c.capacity.note.empty())     entry["host_note"]    = c.capacity.note;
      out.push_back(entry);
    }
    return out;
  }

  const json get_tools_list() {
    json list = {
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
	    {"name", "wraparoundStatus"},
	    {"description", "return transaction id and multixact wraparound headroom: age(datfrozenxid) and age(datminmxid) for every database, the oldest tables by age(relfrozenxid) including TOAST tables (often the relation actually holding the horizon back), each age as a percentage of the effective autovacuum_freeze_max_age and of the 2^31 hard limit at which the cluster stops accepting write transactions, plus the per-table freeze storage parameters and last vacuum times"},
	    {"inputSchema", {
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
	      }}
	  },
	  {
	    {"name", "checkpointStats"},
	    {"description", "return checkpoint, WAL, and background writer activity (pg_stat_checkpointer and pg_stat_bgwriter on PostgreSQL 17+, pg_stat_bgwriter alone before that, plus pg_stat_wal) with field names normalized across both shapes: timed versus requested checkpoint counts and the ratio between them, write and sync time, buffers written by the checkpointer, by the background writer, and directly by backends, WAL record/FPI/byte counts, and the related settings (checkpoint_timeout, max_wal_size, checkpoint_completion_target, the bgwriter knobs)"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "tableIOStats"},
	    {"description", "return per-object buffer cache hit ratios (pg_statio_all_tables): heap_blks_read versus heap_blks_hit, idx_blks_read versus idx_blks_hit, the TOAST and TOAST-index pairs, and a combined ratio, with relation size and scan counts. Naming a single table adds a per-index breakdown from pg_statio_all_indexes. Ratios are null, not zero, for an object that has seen no reads at all"},
	    {"inputSchema", {
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
	      }}
	  },
	  {
	    {"name", "hostCapacity"},
	    {"description", "correlate memory and parallelism settings with the capacity of the machine PostgreSQL runs on. Host RAM and vCPU count exist outside the catalog, so they must be injected: pass them as arguments, set host_ram_mb/host_vcpus in the connection's section of the connections file, or export PG_LICHT_HOST_RAM_MB/PG_LICHT_HOST_VCPUS. Returns the host facts with the source they came from, every memory-related setting resolved to bytes, and derived ratios (shared_buffers and effective_cache_size as a percentage of RAM, work_mem times max_connections, maintenance_work_mem times autovacuum_max_workers, parallel workers per vCPU). Ratios are null when no RAM figure was supplied; nothing is ever guessed"},
	    {"inputSchema", {
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
	      }}
	  },
	  {
	    {"name", "duplicateIndexes"},
	    {"description", "return indexes that duplicate or are covered by another index on the same table. 'identical' groups indexes whose key columns, operator classes, collations, sort order, INCLUDE columns and partial predicate all match; 'redundant' reports an index whose key columns are a leading prefix of a wider index that also covers its INCLUDE columns. Comparison is by column expression rather than attribute number, so expression indexes and differing sort orders are handled correctly, and a unique index is never called redundant for being a prefix. Each entry carries size, idx_scan, the backing constraint name, and the replica identity and validity flags, since those decide whether it can be dropped at all"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"table",  {
			{"type", "string"},
			{"description", "restrict to one table; omit to check every table in the schema"}
		      }}
		  }},
		{"required", {"schema"}}
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
	  },
	  {
	    {"name", "explainQuery"},
	    {"description", "return the raw EXPLAIN (FORMAT JSON) plan for a statement, either recovered from pg_stat_statements by queryid (full untruncated text) or supplied directly as sql. Runs in a read-only transaction bounded by statement_timeout. Statements with $n placeholders are planned with GENERIC_PLAN unless concrete params are supplied, in which case the statement is PREPAREd and planned with real values. analyze:true runs EXPLAIN (ANALYZE, BUFFERS), which really executes the statement, and is honoured only after the plan is proven free of any ModifyTable node -- so data-modifying statements, including data-modifying CTEs, are never executed; it also requires an explicit timeout_ms. Returns the plan verbatim plus generic/analyzed/read_only flags and the pg_stat_statements row; no heuristics and no generated DDL, the plan is yours to interpret"},
	    {"inputSchema", {
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
		      }}
		  }}
	      }}
	  },
	  {
	    {"name", "listConnections"},
	    {"description", "return the configured database connections by name, with the libpq service name or host/port/dbname/user for each, and which is the default; passwords are never returned and a service file is never expanded. Pass a name as the 'connection' argument of any other tool to run that tool against that database"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  }
	}}
    };

    // Every tool accepts an optional `connection`. Injecting it here keeps the
    // ~40 tool definitions and their method signatures untouched; the name is
    // resolved once per request in handle_request.
    json conn_prop = {
      {"type", "string"},
      {"description", "name of a configured connection (see listConnections); "
                      "defaults to \"" + registry_.default_name() + "\""}
    };
    for (auto& tool : list["tools"]) {
      if (tool["name"] == "listConnections") continue;
      tool["inputSchema"]["properties"]["connection"] = conn_prop;
    }
    return list;
  }

  const json schemas() {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(nspname,
              JSONB_BUILD_OBJECT(
               'tables', relnames,
               'roles', COALESCE(roles, '{}'::jsonb)))
      FROM pg_namespace
      LEFT JOIN LATERAL (SELECT JSONB_AGG(relname ORDER BY relname) AS relnames
                         FROM pg_class
                         WHERE relnamespace = pg_namespace.oid
                           AND relkind IN ('r','m','f','p','v')) _lat1 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(pg_namespace.nspacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) _lat2 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
                         AND NOT a.attisdropped) _lat3 ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS index_count
                         FROM pg_indexes AS i
                         WHERE i.schemaname = $1
                           AND i.tablename = c.relname) _lat4 ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS constraint_count
                         FROM pg_constraint
                         WHERE conrelid = c.oid) _lat5 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
                           AND NOT a.attisdropped) _lat6 ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS index_count
                         FROM pg_indexes AS i
                         WHERE i.schemaname = c.relnamespace::regnamespace::name
                           AND i.tablename = c.relname) _lat7 ON true
      LEFT JOIN LATERAL (SELECT COUNT(*) AS constraint_count
                         FROM pg_constraint
                         WHERE conrelid = c.oid) _lat8 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles,
                                STRING_AGG(grantee, ' ' ORDER BY grantee) AS role_names
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(c.relacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) _lat9 ON true
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
          ) _lat10 ON true
          WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid
      ) _lat11 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat12 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(p.proacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) _lat13 ON true
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

    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat14 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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

  const json wraparound_status(const std::string& schema, int limit) {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    // 2^31 - 1000000: the point at which PostgreSQL stops accepting commands
    // that assign new transaction ids. It is the outage threshold, and is
    // distinct from autovacuum_freeze_max_age, which is merely where an
    // anti-wraparound autovacuum is forced.
    //
    // TOAST tables are included deliberately. They carry their own
    // relfrozenxid, are invisible in pg_stat_user_tables, and a TOAST or
    // catalog relation is very often the one actually holding the horizon
    // back; excluding them would report the risk as lower than it is.
    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
        'limits',
          (SELECT JSONB_OBJECT_AGG(name, setting::bigint)
             FROM pg_settings
            WHERE name IN ('autovacuum_freeze_max_age', 'autovacuum_multixact_freeze_max_age',
                           'vacuum_freeze_min_age', 'vacuum_freeze_table_age',
                           'vacuum_multixact_freeze_min_age', 'vacuum_multixact_freeze_table_age',
                           'vacuum_failsafe_age', 'vacuum_multixact_failsafe_age'))
          || JSONB_BUILD_OBJECT('wraparound_limit', 2146483647::bigint),
        'databases',
          (SELECT JSONB_OBJECT_AGG(d.datname, JSONB_BUILD_OBJECT(
                    'xid_age', age(d.datfrozenxid),
                    'xid_percent_of_freeze_max_age',
                      round(100.0 * age(d.datfrozenxid)
                            / NULLIF(current_setting('autovacuum_freeze_max_age')::bigint, 0), 1),
                    'xid_percent_of_wraparound_limit',
                      round(100.0 * age(d.datfrozenxid) / 2146483647, 3),
                    'xids_until_wraparound_limit', 2146483647 - age(d.datfrozenxid),
                    'mxid_age', mxid_age(d.datminmxid),
                    'mxid_percent_of_freeze_max_age',
                      round(100.0 * mxid_age(d.datminmxid)
                            / NULLIF(current_setting('autovacuum_multixact_freeze_max_age')::bigint, 0), 1),
                    'datfrozenxid', d.datfrozenxid::text,
                    'datminmxid', d.datminmxid::text))
             FROM pg_database AS d
            WHERE d.datallowconn),
        'tables', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'schema', r.schema,
                   'name', r.name,
                   'kind', r.kind,
                   'toast_for', r.toast_for,
                   'xid_age', r.xid_age,
                   'xid_percent_of_freeze_max_age',
                     round(100.0 * r.xid_age / NULLIF(r.freeze_max_age, 0), 1),
                   'xid_percent_of_wraparound_limit',
                     round(100.0 * r.xid_age / 2146483647, 3),
                   'mxid_age', r.mxid_age,
                   'relfrozenxid', r.relfrozenxid::text,
                   'freeze_max_age', r.freeze_max_age,
                   'freeze_max_age_source', r.freeze_max_age_source,
                   'size', r.size,
                   'last_vacuum', r.last_vacuum,
                   'last_autovacuum', r.last_autovacuum,
                   'n_dead_tup', r.n_dead_tup) ORDER BY r.xid_age DESC)
          FROM (
            SELECT n.nspname AS schema,
                   c.relname AS name,
                   CASE c.relkind WHEN 'r' THEN 'table'
                                  WHEN 'm' THEN 'materialized view'
                                  WHEN 't' THEN 'toast table' END AS kind,
                   CASE WHEN c.relkind = 't' THEN tn.nspname || '.' || tp.relname END AS toast_for,
                   age(c.relfrozenxid) AS xid_age,
                   mxid_age(c.relminmxid) AS mxid_age,
                   c.relfrozenxid,
                   COALESCE(o.option_value::bigint,
                            current_setting('autovacuum_freeze_max_age')::bigint) AS freeze_max_age,
                   CASE WHEN o.option_value IS NOT NULL THEN 'reloptions' ELSE 'server' END
                     AS freeze_max_age_source,
                   pg_relation_size(c.oid) AS size,
                   s.last_vacuum, s.last_autovacuum, s.n_dead_tup
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            LEFT JOIN pg_class AS tp ON c.relkind = 't' AND tp.reltoastrelid = c.oid
            LEFT JOIN pg_namespace AS tn ON tn.oid = tp.relnamespace
            LEFT JOIN pg_stat_all_tables AS s ON s.relid = c.oid
            LEFT JOIN LATERAL (SELECT option_value
                                 FROM pg_options_to_table(c.reloptions)
                                WHERE option_name = 'autovacuum_freeze_max_age') AS o ON true
            WHERE c.relkind IN ('r', 'm', 't')
              AND c.relfrozenxid <> '0'::xid
              AND ($1 = '' OR COALESCE(tn.nspname, n.nspname) = $1)
            ORDER BY age(c.relfrozenxid) DESC
            LIMIT $2
          ) AS r), '[]'::jsonb)
      );
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, limit});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json duplicate_indexes(const std::string& schema, const std::string& table_name) {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    // Two indexes are interchangeable only if every property the planner cares
    // about matches, so the comparison key is a per-column signature: the
    // column or expression text, its operator class, its collation, and its
    // indoption bits (DESC / NULLS FIRST). Comparing indkey alone would call
    // (a) and (a DESC) duplicates, and two different expression indexes
    // identical -- an expression column has attnum 0 in indkey.
    //
    // key_columns is the same list without opclass/collation/ordering, and is
    // what the INCLUDE coverage test compares against: an included column is
    // covered by a key column of the wider index regardless of how that key is
    // sorted or compared.
    std::string query = R"(
      WITH idx AS (
        SELECT i.indexrelid,
               i.indrelid,
               n.nspname  AS schema,
               tc.relname AS table_name,
               ic.relname AS index_name,
               am.amname  AS access_method,
               i.indisunique     AS is_unique,
               i.indisprimary    AS is_primary,
               i.indisvalid      AS is_valid,
               i.indisreplident  AS is_replica_identity,
               con.conname       AS constraint_name,
               pg_get_expr(i.indpred, i.indrelid, true) AS predicate,
               pg_get_indexdef(i.indexrelid)  AS definition,
               pg_relation_size(i.indexrelid) AS size,
               s.idx_scan,
               (SELECT ARRAY_AGG(pg_get_indexdef(i.indexrelid, k::int, true) ORDER BY k)
                  FROM generate_series(1, i.indnkeyatts) AS k) AS key_columns,
               (SELECT ARRAY_AGG(pg_get_indexdef(i.indexrelid, k::int, true) || ' '
                                 || COALESCE((SELECT op.opcname FROM pg_opclass AS op
                                               WHERE op.oid = i.indclass[k - 1]), '?')
                                 || ' '
                                 || COALESCE(NULLIF(i.indcollation[k - 1], 0)::regcollation::text, '')
                                 || ' ' || i.indoption[k - 1]::text
                                 ORDER BY k)
                  FROM generate_series(1, i.indnkeyatts) AS k) AS key_signature,
               (SELECT COALESCE(ARRAY_AGG(pg_get_indexdef(i.indexrelid, k::int, true)
                                          ORDER BY k), '{}')
                  FROM generate_series(i.indnkeyatts + 1, i.indnatts) AS k) AS included_columns
        FROM pg_index AS i
        JOIN pg_class AS ic ON ic.oid = i.indexrelid
        JOIN pg_class AS tc ON tc.oid = i.indrelid
        JOIN pg_namespace AS n ON n.oid = tc.relnamespace
        JOIN pg_am AS am ON am.oid = ic.relam
        LEFT JOIN pg_constraint AS con
               ON con.conindid = i.indexrelid AND con.contype IN ('p', 'u', 'x')
        LEFT JOIN pg_stat_all_indexes AS s ON s.indexrelid = i.indexrelid
        WHERE n.nspname = $1
          AND ($2 = '' OR tc.relname = $2)
      )
      SELECT JSONB_BUILD_OBJECT(
        'identical', COALESCE((
          SELECT JSONB_AGG(g ORDER BY g->>'table')
          FROM (
            SELECT JSONB_BUILD_OBJECT(
                     'table', a.schema || '.' || a.table_name,
                     'access_method', a.access_method,
                     'key_columns', a.key_columns,
                     'included_columns', a.included_columns,
                     'predicate', a.predicate,
                     'indexes', JSONB_AGG(JSONB_BUILD_OBJECT(
                                  'name', a.index_name,
                                  'definition', a.definition,
                                  'size', a.size,
                                  'idx_scan', a.idx_scan,
                                  'unique', a.is_unique,
                                  'primary_key', a.is_primary,
                                  'constraint', a.constraint_name,
                                  'replica_identity', a.is_replica_identity,
                                  'valid', a.is_valid) ORDER BY a.index_name)) AS g
            FROM idx AS a
            GROUP BY a.indrelid, a.schema, a.table_name, a.access_method,
                     a.key_columns, a.key_signature, a.included_columns, a.predicate
            HAVING COUNT(*) > 1
          ) AS dup), '[]'::jsonb),
        'redundant', COALESCE((
          SELECT JSONB_AGG(red.r ORDER BY red.size DESC)
          FROM (
            SELECT DISTINCT ON (a.indexrelid) JSONB_BUILD_OBJECT(
                     'table', a.schema || '.' || a.table_name,
                     'index', a.index_name,
                     'definition', a.definition,
                     'size', a.size,
                     'idx_scan', a.idx_scan,
                     'covered_by', b.index_name,
                     'covered_by_definition', b.definition,
                     'covered_by_size', b.size,
                     'covered_by_idx_scan', b.idx_scan,
                     'reason',
                       CASE WHEN array_length(a.key_signature, 1) < array_length(b.key_signature, 1)
                            THEN 'key columns are a leading prefix of ' || b.index_name
                            ELSE 'same key columns, and ' || b.index_name ||
                                 ' also covers the included columns'
                       END,
                     'constraint', a.constraint_name,
                     'replica_identity', a.is_replica_identity,
                     'valid', a.is_valid) AS r,
                   a.size
            FROM idx AS a
            JOIN idx AS b
              ON b.indrelid = a.indrelid
             AND b.indexrelid <> a.indexrelid
             AND b.access_method = a.access_method
             AND a.predicate IS NOT DISTINCT FROM b.predicate
             AND array_length(a.key_signature, 1) <= array_length(b.key_signature, 1)
             AND b.key_signature[1:array_length(a.key_signature, 1)] = a.key_signature
             AND a.included_columns <@ (b.key_columns || b.included_columns)
             -- With equal key lists the wider index must cover something the
             -- narrower one does not, otherwise the two would report each
             -- other and the pair belongs in 'identical' anyway.
             AND (array_length(a.key_signature, 1) < array_length(b.key_signature, 1)
                  OR NOT (b.included_columns <@ (a.key_columns || a.included_columns)))
            -- A unique index is never redundant merely for being a prefix: it
            -- enforces a constraint the wider index does not.
            WHERE NOT a.is_unique
            ORDER BY a.indexrelid, b.size
          ) AS red), '[]'::jsonb)
      );
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table_name});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json checkpoint_stats() {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    // PostgreSQL 17 split the checkpointer counters out of pg_stat_bgwriter
    // into pg_stat_checkpointer, renamed them, and moved the backend-written
    // buffer counts to pg_stat_io. Field names are normalized across both
    // shapes so a caller never has to branch on the server version; 'source'
    // says which views produced the numbers.
    const bool split = sess.server_version() >= 170000;

    // pg_stat_wal lost wal_write/wal_sync and their timings in PostgreSQL 18,
    // where they moved to pg_stat_io. The remaining four columns exist on
    // every supported major.
    const std::string wal_timing = sess.server_version() < 180000
      ? R"(, 'wal_write', w.wal_write,
            'wal_sync', w.wal_sync,
            'wal_write_time_ms', w.wal_write_time,
            'wal_sync_time_ms', w.wal_sync_time)"
      : "";

    const std::string checkpointer = split ? R"(
        'source', 'pg_stat_checkpointer + pg_stat_bgwriter + pg_stat_io',
        'checkpointer', (SELECT JSONB_BUILD_OBJECT(
             'checkpoints_timed',        c.num_timed,
             'checkpoints_requested',    c.num_requested,
             'checkpoints_total',        c.num_timed + c.num_requested,
             'timed_percent',            round(100.0 * c.num_timed
                                               / NULLIF(c.num_timed + c.num_requested, 0), 1),
             'restartpoints_timed',      c.restartpoints_timed,
             'restartpoints_requested',  c.restartpoints_req,
             'restartpoints_done',       c.restartpoints_done,
             'write_time_ms',            c.write_time,
             'sync_time_ms',             c.sync_time,
             'buffers_written',          c.buffers_written,
             'stats_reset',              c.stats_reset,
             'seconds_since_reset',      round(EXTRACT(EPOCH FROM now() - c.stats_reset)),
             'checkpoints_per_hour',     round((c.num_timed + c.num_requested)
                                               / NULLIF(EXTRACT(EPOCH FROM now() - c.stats_reset)
                                                        / 3600.0, 0), 2),
             'mean_seconds_between_checkpoints',
                                         round(EXTRACT(EPOCH FROM now() - c.stats_reset)
                                               / NULLIF(c.num_timed + c.num_requested, 0))
           ) FROM pg_stat_checkpointer AS c),
        'bgwriter', (SELECT JSONB_BUILD_OBJECT(
             'buffers_clean',    b.buffers_clean,
             'maxwritten_clean', b.maxwritten_clean,
             'buffers_alloc',    b.buffers_alloc,
             'stats_reset',      b.stats_reset
           ) FROM pg_stat_bgwriter AS b),
        -- buffers_backend and buffers_backend_fsync were removed from
        -- pg_stat_bgwriter in 17; this is the same measurement, per backend
        -- type, from where it now lives.
        'backend_io', (SELECT JSONB_OBJECT_AGG(backend_type, JSONB_BUILD_OBJECT(
             'writes', writes, 'fsyncs', fsyncs, 'extends', extends,
             'evictions', evictions, 'reads', reads))
           FROM (SELECT backend_type,
                        SUM(writes) AS writes, SUM(fsyncs) AS fsyncs,
                        SUM(extends) AS extends, SUM(evictions) AS evictions,
                        SUM(reads) AS reads
                 FROM pg_stat_io
                 WHERE object = 'relation'
                 GROUP BY backend_type) AS io),
    )" : R"(
        'source', 'pg_stat_bgwriter',
        'checkpointer', (SELECT JSONB_BUILD_OBJECT(
             'checkpoints_timed',        b.checkpoints_timed,
             'checkpoints_requested',    b.checkpoints_req,
             'checkpoints_total',        b.checkpoints_timed + b.checkpoints_req,
             'timed_percent',            round(100.0 * b.checkpoints_timed
                                               / NULLIF(b.checkpoints_timed + b.checkpoints_req, 0), 1),
             'write_time_ms',            b.checkpoint_write_time,
             'sync_time_ms',             b.checkpoint_sync_time,
             'buffers_written',          b.buffers_checkpoint,
             'stats_reset',              b.stats_reset,
             'seconds_since_reset',      round(EXTRACT(EPOCH FROM now() - b.stats_reset)),
             'checkpoints_per_hour',     round((b.checkpoints_timed + b.checkpoints_req)
                                               / NULLIF(EXTRACT(EPOCH FROM now() - b.stats_reset)
                                                        / 3600.0, 0), 2),
             'mean_seconds_between_checkpoints',
                                         round(EXTRACT(EPOCH FROM now() - b.stats_reset)
                                               / NULLIF(b.checkpoints_timed + b.checkpoints_req, 0))
           ) FROM pg_stat_bgwriter AS b),
        'bgwriter', (SELECT JSONB_BUILD_OBJECT(
             'buffers_clean',          b.buffers_clean,
             'maxwritten_clean',       b.maxwritten_clean,
             'buffers_backend',        b.buffers_backend,
             'buffers_backend_fsync',  b.buffers_backend_fsync,
             'buffers_alloc',          b.buffers_alloc,
             'stats_reset',            b.stats_reset
           ) FROM pg_stat_bgwriter AS b),
    )";

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
    )" + checkpointer + R"(
        'wal', (SELECT JSONB_BUILD_OBJECT(
             'wal_records',      w.wal_records,
             'wal_fpi',          w.wal_fpi,
             'wal_bytes',        w.wal_bytes,
             'wal_buffers_full', w.wal_buffers_full,
             'stats_reset',      w.stats_reset
           )" + wal_timing + R"(
           ) FROM pg_stat_wal AS w),
        'settings', (SELECT JSONB_OBJECT_AGG(name, JSONB_BUILD_OBJECT(
             'setting', setting, 'unit', unit))
           FROM pg_settings
          WHERE name IN ('checkpoint_timeout', 'checkpoint_completion_target',
                         'checkpoint_flush_after', 'checkpoint_warning',
                         'max_wal_size', 'min_wal_size', 'wal_buffers',
                         'wal_writer_delay', 'wal_writer_flush_after',
                         'wal_level', 'wal_compression', 'full_page_writes',
                         'synchronous_commit', 'fsync',
                         'bgwriter_delay', 'bgwriter_lru_maxpages',
                         'bgwriter_lru_multiplier', 'bgwriter_flush_after',
                         'shared_buffers'))
      );
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json table_io_stats(const std::string& schema, const std::string& table_name, int limit) {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    // A per-index breakdown is attached only when a single table was named:
    // over a whole schema it multiplies the payload by the index count without
    // making the table-level ratios any easier to read.
    //
    // Ratios are null rather than 0 when nothing has been read yet, so "no
    // traffic" is never mistaken for "every read missed the cache".
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(r.schemaname || '.' || r.relname, JSONB_BUILD_OBJECT(
               'heap_blks_read', r.heap_blks_read,
               'heap_blks_hit',  r.heap_blks_hit,
               'heap_hit_percent',
                 round(100.0 * r.heap_blks_hit
                       / NULLIF(r.heap_blks_hit + r.heap_blks_read, 0), 2),
               'idx_blks_read', r.idx_blks_read,
               'idx_blks_hit',  r.idx_blks_hit,
               'idx_hit_percent',
                 round(100.0 * r.idx_blks_hit
                       / NULLIF(r.idx_blks_hit + r.idx_blks_read, 0), 2),
               'toast_blks_read', r.toast_blks_read,
               'toast_blks_hit',  r.toast_blks_hit,
               'tidx_blks_read',  r.tidx_blks_read,
               'tidx_blks_hit',   r.tidx_blks_hit,
               'total_blks_read', r.total_read,
               'total_blks_hit',  r.total_hit,
               'total_hit_percent',
                 round(100.0 * r.total_hit / NULLIF(r.total_hit + r.total_read, 0), 2),
               'size',       r.size,
               'seq_scan',   r.seq_scan,
               'idx_scan',   r.idx_scan,
               'n_live_tup', r.n_live_tup,
               'indexes',    r.indexes))
      FROM (
        SELECT io.schemaname, io.relname,
               io.heap_blks_read, io.heap_blks_hit,
               io.idx_blks_read, io.idx_blks_hit,
               io.toast_blks_read, io.toast_blks_hit,
               io.tidx_blks_read, io.tidx_blks_hit,
               COALESCE(io.heap_blks_read, 0) + COALESCE(io.idx_blks_read, 0)
                 + COALESCE(io.toast_blks_read, 0) + COALESCE(io.tidx_blks_read, 0) AS total_read,
               COALESCE(io.heap_blks_hit, 0) + COALESCE(io.idx_blks_hit, 0)
                 + COALESCE(io.toast_blks_hit, 0) + COALESCE(io.tidx_blks_hit, 0) AS total_hit,
               pg_total_relation_size(io.relid) AS size,
               st.seq_scan, st.idx_scan, st.n_live_tup,
               CASE WHEN $2 <> '' THEN (
                 SELECT JSONB_OBJECT_AGG(ix.indexrelname, JSONB_BUILD_OBJECT(
                          'idx_blks_read', ix.idx_blks_read,
                          'idx_blks_hit',  ix.idx_blks_hit,
                          'idx_hit_percent',
                            round(100.0 * ix.idx_blks_hit
                                  / NULLIF(ix.idx_blks_hit + ix.idx_blks_read, 0), 2),
                          'idx_scan', si.idx_scan,
                          'idx_tup_read', si.idx_tup_read,
                          'idx_tup_fetch', si.idx_tup_fetch,
                          'size', pg_relation_size(ix.indexrelid)))
                 FROM pg_statio_all_indexes AS ix
                 LEFT JOIN pg_stat_all_indexes AS si ON si.indexrelid = ix.indexrelid
                 WHERE ix.relid = io.relid
               ) END AS indexes
        FROM pg_statio_all_tables AS io
        LEFT JOIN pg_stat_all_tables AS st ON st.relid = io.relid
        WHERE io.schemaname = $1
          AND ($2 = '' OR io.relname = $2)
        ORDER BY COALESCE(io.heap_blks_read, 0) + COALESCE(io.idx_blks_read, 0)
                 + COALESCE(io.toast_blks_read, 0) + COALESCE(io.tidx_blks_read, 0) DESC
        LIMIT $3
      ) AS r;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table_name, limit});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  // Host RAM and vCPU count are not in any catalog, so they arrive from
  // outside: a hostCapacity argument (an agent that just inspected the box),
  // the connections file, or the environment. Whichever is used is named in
  // 'source', because every derived ratio is only as good as that figure.
  const json host_capacity(long long ram_mb_arg, int vcpus_arg,
                           const std::string& storage_arg) {
    pglicht::HostCapacity cap = active_cfg().capacity;
    if (ram_mb_arg > 0 || vcpus_arg > 0 || !storage_arg.empty()) {
      // Arguments win over configuration, and are reported as a distinct
      // source: a value passed per call is a claim about right now, while the
      // file may have been written for a machine that has since been resized.
      pglicht::HostCapacity from_args;
      from_args.ram_mb  = ram_mb_arg > 0 ? ram_mb_arg : cap.ram_mb;
      from_args.vcpus   = vcpus_arg  > 0 ? vcpus_arg  : cap.vcpus;
      from_args.storage = !storage_arg.empty() ? storage_arg : cap.storage;
      from_args.note    = cap.note;
      from_args.source  = cap.configured() ? "argument (over " + cap.source + ")" : "argument";
      cap = from_args;
    }

    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    // Byte-valued GUCs are reported in their own unit (8kB pages for
    // shared_buffers, kB for work_mem, ...), so the multiplier is applied here
    // rather than leaving every caller to rediscover it. A negative setting
    // means "derive from another GUC" (autovacuum_work_mem = -1) and yields a
    // null byte count rather than a negative one.
    std::string query = R"(
      WITH host AS (
        SELECT NULLIF($1, '')::bigint AS ram_bytes,
               NULLIF($2, '')::int    AS vcpus
      ),
      g AS (
        SELECT name, setting, unit,
               CASE WHEN setting::bigint < 0 THEN NULL
                    ELSE setting::bigint * CASE unit
                                             WHEN 'B'   THEN 1
                                             WHEN 'kB'  THEN 1024
                                             WHEN 'MB'  THEN 1048576
                                             WHEN 'GB'  THEN 1073741824
                                             WHEN '8kB' THEN 8192
                                           END
               END AS bytes
        FROM pg_settings
        WHERE vartype = 'integer'
          AND name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem',
                       'autovacuum_work_mem', 'temp_buffers', 'wal_buffers',
                       'effective_cache_size', 'min_wal_size', 'max_wal_size',
                       'logical_decoding_work_mem',
                       'max_connections', 'superuser_reserved_connections',
                       'max_worker_processes', 'max_parallel_workers',
                       'max_parallel_workers_per_gather',
                       'max_parallel_maintenance_workers', 'autovacuum_max_workers',
                       'effective_io_concurrency', 'maintenance_io_concurrency')
      ),
      v AS (
        SELECT (SELECT bytes   FROM g WHERE name = 'shared_buffers')       AS shared_buffers,
               (SELECT bytes   FROM g WHERE name = 'work_mem')             AS work_mem,
               (SELECT bytes   FROM g WHERE name = 'maintenance_work_mem') AS maint_work_mem,
               (SELECT bytes   FROM g WHERE name = 'effective_cache_size') AS effective_cache_size,
               (SELECT setting::bigint FROM g WHERE name = 'max_connections')        AS max_connections,
               (SELECT setting::bigint FROM g WHERE name = 'autovacuum_max_workers') AS av_workers
      )
      SELECT JSONB_BUILD_OBJECT(
        'server', JSONB_BUILD_OBJECT(
          'version', current_setting('server_version'),
          'database', current_database()),
        'settings', (SELECT JSONB_OBJECT_AGG(name, JSONB_BUILD_OBJECT(
                        'setting', setting, 'unit', unit, 'bytes', bytes)) FROM g),
        'derived', (SELECT JSONB_BUILD_OBJECT(
          'shared_buffers_percent_of_ram',
            round(100.0 * v.shared_buffers / NULLIF(host.ram_bytes, 0), 1),
          'effective_cache_size_percent_of_ram',
            round(100.0 * v.effective_cache_size / NULLIF(host.ram_bytes, 0), 1),
          'work_mem_times_max_connections_bytes',
            v.work_mem * v.max_connections,
          'work_mem_times_max_connections_percent_of_ram',
            round(100.0 * v.work_mem * v.max_connections / NULLIF(host.ram_bytes, 0), 1),
          'maintenance_work_mem_times_autovacuum_workers_bytes',
            v.maint_work_mem * v.av_workers,
          'maintenance_work_mem_times_autovacuum_workers_percent_of_ram',
            round(100.0 * v.maint_work_mem * v.av_workers / NULLIF(host.ram_bytes, 0), 1),
          'committed_worst_case_bytes',
            v.shared_buffers + v.work_mem * v.max_connections
              + v.maint_work_mem * v.av_workers,
          'committed_worst_case_percent_of_ram',
            round(100.0 * (v.shared_buffers + v.work_mem * v.max_connections
                           + v.maint_work_mem * v.av_workers)
                  / NULLIF(host.ram_bytes, 0), 1),
          'max_parallel_workers_per_vcpu',
            round((SELECT setting::numeric FROM g WHERE name = 'max_parallel_workers')
                  / NULLIF(host.vcpus, 0), 2),
          'max_worker_processes_per_vcpu',
            round((SELECT setting::numeric FROM g WHERE name = 'max_worker_processes')
                  / NULLIF(host.vcpus, 0), 2))
          FROM v, host),
        'notes', JSONB_BUILD_ARRAY(
          'work_mem is a per-node limit, not a per-connection one: a single query '
          'with several sorts or hash joins can use a multiple of it, and parallel '
          'workers each get their own. work_mem_times_max_connections is therefore a '
          'floor on the worst case, not a ceiling.',
          'shared_buffers is counted once here; the operating system page cache is '
          'not, which is what effective_cache_size is meant to describe.')
      );
    )";

    pqxx::result res = pqxx_exec(
      txn, query,
      pqxx::params{cap.ram_mb > 0 ? std::to_string(cap.ram_mb * 1048576LL) : std::string{},
                   cap.vcpus  > 0 ? std::to_string(cap.vcpus)              : std::string{}});

    json out = (!res.empty() && !res[0][0].is_null())
      ? json::parse(res[0][0].as<std::string>()) : json::object();

    json host = {{"configured", cap.configured()}};
    if (cap.ram_mb > 0) {
      host["ram_mb"]    = cap.ram_mb;
      host["ram_bytes"] = cap.ram_mb * 1048576LL;
    }
    if (cap.vcpus > 0)        host["vcpus"]   = cap.vcpus;
    if (!cap.storage.empty()) host["storage"] = cap.storage;
    if (!cap.note.empty())    host["note"]    = cap.note;
    if (!cap.source.empty())  host["source"]  = cap.source;
    out["host"] = host;

    if (cap.ram_mb <= 0) {
      out["hint"] =
        "no host RAM is configured, so every percent_of_ram field is null. Supply "
        "ram_mb (and vcpus) as arguments to this tool, set host_ram_mb/host_vcpus "
        "in the connection's section of the connections file, or export "
        "PG_LICHT_HOST_RAM_MB/PG_LICHT_HOST_VCPUS. PostgreSQL cannot report the "
        "host's memory itself, and pg-licht will not guess it";
    }
    return out;
  }

  // A plan is read-only iff no ModifyTable node appears anywhere in the tree.
  // Checking the whole tree rather than just the root is what catches a
  // data-modifying CTE -- WITH d AS (DELETE ... RETURNING *) SELECT * FROM d --
  // where the ModifyTable is nested under a CTE subplan.
  static bool plan_has_modify(const json& node) {
    if (node.is_array()) {
      for (const auto& e : node) if (plan_has_modify(e)) return true;
      return false;
    }
    if (!node.is_object()) return false;
    auto it = node.find("Node Type");
    if (it != node.end() && it->is_string() && it->get<std::string>() == "ModifyTable")
      return true;
    for (const auto& e : node.items()) if (plan_has_modify(e.value())) return true;
    return false;
  }

  // First keyword of a statement, uppercased, skipping leading whitespace and
  // comments. pg_stat_statements also tracks utility statements (CREATE
  // DATABASE, SET, VACUUM, ...), and EXPLAIN cannot take those at all --
  // "EXPLAIN SET work_mem='4MB'" is a syntax error, not a graceful failure.
  static std::string leading_keyword(const std::string& sql) {
    size_t i = 0;
    for (;;) {
      while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) i++;
      if (i + 1 < sql.size() && sql[i] == '-' && sql[i + 1] == '-') {
        while (i < sql.size() && sql[i] != '\n') i++;
      } else if (i + 1 < sql.size() && sql[i] == '/' && sql[i + 1] == '*') {
        size_t e = sql.find("*/", i + 2);
        if (e == std::string::npos) return "";
        i = e + 2;
      } else {
        break;
      }
    }
    // A leading "(" means a parenthesised SELECT, e.g. (SELECT ...) UNION ...
    if (i < sql.size() && sql[i] == '(') return "SELECT";
    size_t s = i;
    while (i < sql.size() && std::isalpha(static_cast<unsigned char>(sql[i]))) i++;
    std::string kw = sql.substr(s, i - s);
    for (char& c : kw) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return kw;
  }

  // Strip one trailing semicolon; reject anything that looks like a second
  // statement. pqxx uses the simple protocol for exec(), which would happily
  // run "SELECT 1; DROP TABLE t" as two statements once concatenated after
  // EXPLAIN, so caller-supplied text must be proven to be a single statement.
  static std::string require_single_statement(const std::string& sql) {
    std::string t = sql;
    size_t end = t.find_last_not_of(" \t\r\n");
    if (end != std::string::npos) t = t.substr(0, end + 1);
    if (!t.empty() && t.back() == ';') {
      t.pop_back();
      size_t e2 = t.find_last_not_of(" \t\r\n");
      t = (e2 == std::string::npos) ? "" : t.substr(0, e2 + 1);
    }
    if (t.find(';') != std::string::npos)
      throw std::runtime_error(
        "sql must be a single statement; found an embedded ';'");
    return t;
  }

  // Build the literal list for EXECUTE. EXECUTE arguments are parsed as
  // expressions and cannot be bind parameters, so the values have to appear in
  // the SQL text. Every non-null value is emitted as a quoted string literal of
  // unknown type and left for PostgreSQL to coerce into the prepared
  // statement's inferred parameter type. That is deliberate: it means there is
  // exactly one escaping path (txn.quote, i.e. libpq's escaping) rather than
  // one per JSON type, and no numeric or boolean value is ever spliced in raw.
  static std::string build_execute_literals(pqxx::work& txn, const json& params) {
    std::string lits;
    for (size_t i = 0; i < params.size(); i++) {
      if (i > 0) lits += ", ";
      const json& v = params[i];
      if (v.is_null())          lits += "NULL";
      else if (v.is_string())   lits += txn.quote(v.get<std::string>());
      else if (v.is_boolean())  lits += txn.quote(std::string(v.get<bool>() ? "true" : "false"));
      else                      lits += txn.quote(v.dump());
    }
    return lits;
  }

  // json::value() throws if a key exists but holds null, which happens
  // routinely here: pg_stat_statements keeps entries for dropped databases, so
  // the LEFT JOIN to pg_database yields a null name, and query text can be null
  // when the caller lacks permission to read it.
  static std::string json_str(const json& j, const char* key,
                              const std::string& fallback = "") {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return fallback;
    return it->get<std::string>();
  }

  const json explain_query(const std::string& queryid, const std::string& sql_in,
                           const json& params, bool analyze, int timeout_ms) {
    // --- argument validation (caller errors -> isError:true via dispatch) ---
    if (queryid.empty() && sql_in.empty())
      throw std::runtime_error("one of queryid or sql is required");
    if (!queryid.empty() && !sql_in.empty())
      throw std::runtime_error("queryid and sql are mutually exclusive");
    if (!params.is_array())
      throw std::runtime_error("params must be an array");
    if (params.size() > 64)
      throw std::runtime_error("params has " + std::to_string(params.size()) +
                               " entries; at most 64 are supported");
    if (!queryid.empty()) {
      bool ok = !queryid.empty() && queryid.size() <= 20;
      for (size_t i = 0; ok && i < queryid.size(); i++) {
        if (i == 0 && queryid[i] == '-') { ok = queryid.size() > 1; continue; }
        if (!std::isdigit(static_cast<unsigned char>(queryid[i]))) ok = false;
      }
      if (!ok)
        throw std::runtime_error("queryid must be a decimal integer string");
    }
    // EXPLAIN ANALYZE really executes the statement, and these are by
    // construction the slowest queries in the cluster. The caller must state a
    // bound consciously rather than inherit a default.
    if (analyze && timeout_ms <= 0)
      throw std::runtime_error(
        "analyze requires an explicit timeout_ms (100-30000): EXPLAIN ANALYZE "
        "executes the statement, and statements recovered from "
        "pg_stat_statements are the slowest ones in the cluster");

    int tmo = timeout_ms > 0 ? timeout_ms : 5000;
    if (tmo < 100)   tmo = 100;
    if (tmo > 30000) tmo = 30000;

    Session sess{active_cfg(), tmo};
    pqxx::work& txn = sess.txn();

    // --- resolve the statement text ---
    std::string sql = sql_in;
    json stats = nullptr;

    if (!queryid.empty()) {
      // Full, untruncated text: recovering it is the whole point of the
      // queryid path, so this deliberately omits statementStats' LEFT(query,500).
      std::string q = R"(
        SELECT JSONB_BUILD_OBJECT(
                 'query',            pss.query,
                 'query_id',         pss.queryid,
                 'calls',            pss.calls,
                 'total_exec_ms',    pss.total_exec_time,
                 'mean_exec_ms',     pss.mean_exec_time,
                 'min_exec_ms',      pss.min_exec_time,
                 'max_exec_ms',      pss.max_exec_time,
                 'rows',             pss.rows,
                 'shared_blks_hit',  pss.shared_blks_hit,
                 'shared_blks_read', pss.shared_blks_read,
                 'user',             r.rolname,
                 'database',         d.datname,
                 -- oid serialises to JSON as a string, so make it text
                 -- explicitly rather than leave the type to chance.
                 'database_oid',     pss.dbid::text,
                 'is_current_db',    COALESCE(d.datname = current_database(), false)
               )
        FROM pg_stat_statements AS pss
        LEFT JOIN pg_roles AS r ON r.oid = pss.userid
        LEFT JOIN pg_database AS d ON d.oid = pss.dbid
        WHERE pss.queryid = $1::bigint
        ORDER BY (d.datname = current_database()) DESC NULLS LAST,
                 pss.total_exec_time DESC
        LIMIT 1;
      )";

      try {
        pqxx::result res = pqxx_exec(txn, q, pqxx::params{queryid});
        if (res.empty() || res[0][0].is_null()) {
          return {
            {"error", "no pg_stat_statements entry for queryid " + queryid},
            {"hint", "queryids are reset by pg_stat_statements_reset() and evicted "
                     "once pg_stat_statements.max is exceeded; re-run statementStats "
                     "for a current queryid"}
          };
        }
        stats = json::parse(res[0][0].as<std::string>());
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

      // Planning a statement from another database against this one's catalogs
      // gives a plan that is either an error or, worse, silently wrong.
      if (!stats.value("is_current_db", false)) {
        // A null name means the database has since been dropped;
        // pg_stat_statements keeps the entry regardless.
        std::string db = json_str(stats, "database");
        bool dropped = db.empty();
        return {
          {"error", "queryid " + queryid + " belongs to " +
                    (dropped ? "a database that no longer exists (oid " +
                               json_str(stats, "database_oid", "?") + ")"
                             : "database \"" + db + "\"") +
                    ", not the connected database"},
          {"hint", dropped
             ? "pg_stat_statements retains entries for dropped databases; there is "
               "nothing to explain. Re-run statementStats for a current queryid"
             : "point pg-licht at that database with the 'connection' argument "
               "(see listConnections), or pass the statement text via 'sql'"},
          {"statement", stats}
        };
      }

      sql = json_str(stats, "query");
      if (sql.empty()) {
        return {
          {"error", "pg_stat_statements has no query text for queryid " + queryid},
          {"hint", "the text is hidden unless you are a superuser or a member of "
                   "pg_read_all_stats, and it can be evicted under memory pressure; "
                   "pass the statement via the 'sql' argument instead"},
          {"statement", stats}
        };
      }
    }

    sql = require_single_statement(sql);
    if (sql.empty())
      throw std::runtime_error("the statement is empty");

    static const std::set<std::string> EXPLAINABLE = {
      "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "WITH", "TABLE", "VALUES"
    };
    std::string kw = leading_keyword(sql);
    if (!EXPLAINABLE.count(kw))
      throw std::runtime_error(
        "statement cannot be EXPLAINed: it starts with \"" +
        (kw.empty() ? std::string("(nothing)") : kw) +
        "\". EXPLAIN accepts only SELECT/INSERT/UPDATE/DELETE/MERGE/WITH/TABLE/"
        "VALUES; pg_stat_statements also tracks utility statements (SET, CREATE, "
        "VACUUM, ...), which have no plan");

    std::string prepared;
    std::string lits;
    json plan;
    bool generic = false;

    try {
      // --- Phase A: produce a plan without executing anything ---
      if (params.empty()) {
        try {
          // A savepoint, so that the expected failure below leaves the
          // transaction usable rather than aborted.
          pqxx::subtransaction sub{txn};
          pqxx::result r = sub.exec("EXPLAIN (FORMAT JSON) " + sql);
          plan = json::parse(r[0][0].as<std::string>());
          sub.commit();
        } catch (const pqxx::sql_error& e) {
          // 42P02 undefined_parameter: the statement has $n placeholders and no
          // values were supplied. Whether that is the case is decided by
          // PostgreSQL rather than by scanning for "$n" ourselves, which would
          // false-positive inside string literals and dollar-quoted bodies.
          if (e.sqlstate() != "42P02") throw;
          // GENERIC_PLAN is PostgreSQL 16+. On older servers attempting it
          // yields a confusing "unrecognized EXPLAIN option" rather than the
          // real problem, so short-circuit with the actionable hint instead.
          if (sess.server_version() < 160000) {
            json out = {
              {"error", "the statement has $n placeholders and no params were supplied"},
              {"hint", "supply values via the params argument so the statement can be "
                       "prepared and planned; planning a normalized statement without "
                       "params needs EXPLAIN (GENERIC_PLAN), which requires PostgreSQL 16+"}
            };
            // On the queryid path the statement was already recovered; return it
            // so the caller still gets the stats and can retry with params.
            if (!stats.is_null()) out["statement"] = stats;
            return out;
          }
          pqxx::result r = txn.exec("EXPLAIN (GENERIC_PLAN, FORMAT JSON) " + sql);
          plan = json::parse(r[0][0].as<std::string>());
          generic = true;
        }
      } else {
        prepared = "pg_licht_explain_" + std::to_string(static_cast<long long>(::getpid())) +
                   "_" + std::to_string(++explain_seq_);
        txn.exec("PREPARE " + prepared + " AS " + sql);

        pqxx::result pr = pqxx_exec(
          txn,
          "SELECT COALESCE(array_length(parameter_types, 1), 0) "
          "FROM pg_prepared_statements WHERE name = $1",
          pqxx::params{prepared});
        size_t want = (pr.empty() || pr[0][0].is_null())
                        ? 0u : static_cast<size_t>(pr[0][0].as<long long>());
        if (want != params.size())
          throw std::runtime_error("statement takes " + std::to_string(want) +
                                   " parameter(s), got " + std::to_string(params.size()));

        lits = build_execute_literals(txn, params);
        pqxx::result r = txn.exec(
          "EXPLAIN (FORMAT JSON) EXECUTE " + prepared + "(" + lits + ")");
        plan = json::parse(r[0][0].as<std::string>());
      }

      // --- the write gate ---
      bool read_only = !plan_has_modify(plan);
      bool analyzed = false;
      std::string note;

      // --- Phase B: optionally execute, only once proven safe ---
      if (analyze) {
        if (!read_only) {
          note = "not analyzed: the plan contains a ModifyTable node, so the "
                 "statement modifies data; only the plan is returned";
        } else if (generic) {
          note = "not analyzed: the statement has $n placeholders and no params "
                 "were supplied, so only a generic plan could be produced; supply "
                 "params to get a real plan and enable ANALYZE";
        } else {
          std::string target = prepared.empty()
            ? sql : ("EXECUTE " + prepared + "(" + lits + ")");
          pqxx::result r = txn.exec(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + target);
          plan = json::parse(r[0][0].as<std::string>());
          analyzed = true;
        }
      }

      if (!prepared.empty()) txn.exec("DEALLOCATE " + prepared);

      json out = {
        {"plan",       plan},
        {"generic",    generic},
        {"analyzed",   analyzed},
        {"read_only",  read_only},
        {"timeout_ms", tmo},
        {"source",     queryid.empty() ? "sql" : "pg_stat_statements"},
        {"sql",        sql}
      };
      if (!note.empty())    out["note"] = note;
      if (!stats.is_null()) out["statement"] = stats;
      return out;

    } catch (const pqxx::sql_error& e) {
      // Returned as successful results with error/hint, matching the pattern
      // used for missing extensions: the caller needs to read the reason and
      // retry differently, not just be told something failed.
      // Direct-init, not copy-init: libpqxx 8's sqlstate() returns
      // std::string_view (explicit conversion to std::string), while 7.x
      // returns std::string. Parens accept both.
      std::string ss(e.sqlstate());
      if (ss == "42601")
        return {{"error", "the statement could not be parsed"},
                {"hint", "pg_stat_statements truncates query text at "
                         "track_activity_query_size; raise that setting, or pass "
                         "the full statement via the 'sql' argument"},
                {"detail", e.what()}};
      if (ss == "42P18")
        return {{"error", "PostgreSQL could not infer the type of one or more parameters"},
                {"hint", "add an explicit cast in the statement, e.g. "
                         "WHERE col = $1::uuid, and retry"},
                {"detail", e.what()}};
      if (ss == "57014")
        return {{"error", "the explain exceeded the statement timeout of " +
                          std::to_string(tmo) + " ms"},
                {"hint", "this is expected for a genuinely slow statement; raise "
                         "timeout_ms (max 30000), or omit analyze to plan without "
                         "executing"}};
      if (ss == "25006")
        return {{"error", "the statement attempted to write in a read-only transaction"},
                {"hint", "pg-licht never executes data-modifying statements; only "
                         "the plan can be returned"},
                {"detail", e.what()}};
      if (ss == "0A000")
        return {{"error", "this statement cannot be explained on this server"},
                {"hint", "EXPLAIN (GENERIC_PLAN) requires PostgreSQL 16 or newer; "
                         "supply concrete values via params instead"},
                {"detail", e.what()}};
      if (ss == "42P02")
        return {{"error", "the statement has parameters that could not be planned generically"},
                {"hint", "supply values via the params argument"},
                {"detail", e.what()}};
      if (ss == "42P01")
        return {{"error", "a relation referenced by the statement does not exist"},
                {"hint", "the statement may target a different database; check "
                         "listTables, or select another database with the "
                         "'connection' argument"},
                {"detail", e.what()}};
      return {{"error", "explain failed"}, {"sqlstate", ss}, {"detail", e.what()}};
    }
  }

  const json table_bloat(const std::string& schema, const std::string& table_name, bool exact) {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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

    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat15 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat16 ON true
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
      ) _lat17 ON true
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

    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat18 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat19 ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(pg_get_constraintdef(con.oid)) AS constraints
          FROM pg_constraint AS con
          WHERE con.contypid = t.oid
            AND t.typtype = 'd'
      ) _lat20 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat21 ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(pg_get_constraintdef(con.oid)) AS constraints
          FROM pg_constraint AS con
          WHERE con.contypid = t.oid
            AND t.typtype = 'd'
      ) _lat22 ON true
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
      ) _lat23 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat24 ON true;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json foreign_tables(const std::string& schema) {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat25 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat26 ON true;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json subscriptions() {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

    // Deliberately excludes subconninfo: pg_subscription is a shared (cluster-wide)
    // catalog and that column holds the full connection string, which may embed a
    // password. Scoped to the current database via subdbid to avoid leaking
    // subscriptions that belong to other databases in the same cluster.
    // subtwophasestate is PostgreSQL 15+; omit it on older servers rather than
    // fail with an undefined-column error. subbinary is PG14, the supported floor.
    std::string two_phase =
      sess.server_version() >= 150000 ? ", 'two_phase', subtwophasestate" : "";
    std::string query =
      "SELECT JSONB_OBJECT_AGG(subname, JSONB_BUILD_OBJECT("
      "  'owner',              subowner::regrole::text"
      ", 'enabled',            subenabled"
      ", 'publications',       subpublications"
      ", 'slot_name',          subslotname"
      ", 'synchronous_commit', subsynccommit"
      ", 'binary',             subbinary" + two_phase +
      ")) "
      "FROM pg_subscription "
      "WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database());";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json languages() {
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
      ) _lat27 ON true
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(CASE k WHEN 'd' THEN 'ndistinct' WHEN 'f' THEN 'dependencies'
                                   WHEN 'm' THEN 'mcv' WHEN 'e' THEN 'expressions' END) AS kinds
          FROM unnest(s.stxkind) AS k
      ) _lat28 ON true
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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
    Session sess{active_cfg()};
    pqxx::work& txn = sess.txn();

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
                         AND NOT attisdropped) _lat29 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(indexname,
                          JSONB_BUILD_OBJECT(
                           'definition', indexdef, 'size', pg_relation_size(si.indexrelid), 'index_uses', idx_scan, 'last_use', last_idx_scan)) AS indexes
                         FROM pg_indexes AS i
                         JOIN pg_stat_user_indexes si ON si.indexrelname = i.indexname
                                                      AND si.schemaname = i.schemaname
                                                      AND si.relname = i.tablename
                         WHERE i.schemaname = $1
                           AND i.tablename = c.relname) _lat30 ON true
      LEFT JOIN LATERAL (SELECT JSONB_AGG(a.attname ORDER BY array_position(pk.conkey, a.attnum)) AS primary_key
                         FROM pg_constraint pk
                         JOIN pg_attribute a ON a.attrelid = pk.conrelid AND a.attnum = ANY(pk.conkey)
                         WHERE pk.conrelid = c.oid AND pk.contype = 'p') _lat31 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(conname,
                          JSONB_BUILD_OBJECT(
                           'definition', pg_get_constraintdef(oid))) AS constraints
                         FROM pg_constraint
                         WHERE conrelid = c.oid
                           AND contype != 'f') _lat32 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(fk.conname,
                          JSONB_BUILD_OBJECT(
                           'target_table', fk.confrelid::regclass::text,
                           'source_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.conkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.conrelid AND a.attnum = ANY(fk.conkey)),
                           'target_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.confkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.confrelid AND a.attnum = ANY(fk.confkey)),
                           'definition', pg_get_constraintdef(fk.oid))) AS foreign_keys
                         FROM pg_constraint fk
                         WHERE fk.conrelid = c.oid
                           AND fk.contype = 'f') _lat33 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(fk.conrelid::regclass::text || '.' || fk.conname,
                          JSONB_BUILD_OBJECT(
                           'source_table', fk.conrelid::regclass::text,
                           'source_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.conkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.conrelid AND a.attnum = ANY(fk.conkey)),
                           'target_columns', (SELECT jsonb_agg(a.attname ORDER BY array_position(fk.confkey, a.attnum)) FROM pg_attribute a WHERE a.attrelid = fk.confrelid AND a.attnum = ANY(fk.confkey)),
                           'definition', pg_get_constraintdef(fk.oid))) AS referenced_by
                         FROM pg_constraint fk
                         WHERE fk.confrelid = c.oid
                           AND fk.contype = 'f') _lat34 ON true
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
                           AND  NOT t.tgisinternal) _lat35 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(r.rulename,
                          JSONB_BUILD_OBJECT(
                           'event',      CASE r.ev_type WHEN '1' THEN 'SELECT' WHEN '2' THEN 'UPDATE'
                                                         WHEN '3' THEN 'INSERT' WHEN '4' THEN 'DELETE' END,
                           'instead',    r.is_instead,
                           'definition', pg_get_ruledef(r.oid))) AS rules
                         FROM pg_rewrite AS r
                         WHERE r.ev_class = c.oid
                           AND r.rulename != '_RETURN') _lat36 ON true
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
                         WHERE pol.polrelid = c.oid) _lat37 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                         FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                      JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                               FROM aclexplode(c.relacl) AS a
                               LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                               GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) _lat38 ON true
      WHERE c.relnamespace = $1::regnamespace
        AND c.relname = $2;
    )";

    // pg_stat_user_indexes.last_idx_scan is PostgreSQL 16+; drop the index
    // 'last_use' field on older servers so the query still parses. If this
    // literal ever drifts from the query above, the pg14/pg15 CI jobs fail
    // loudly with an undefined-column error rather than silently.
    if (sess.server_version() < 160000) {
      const std::string frag = ", 'last_use', last_idx_scan";
      auto pos = query.find(frag);
      if (pos != std::string::npos) query.erase(pos, frag.size());
    }

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

	// Resolve the target connection once, before dispatch. get() throws a
	// message listing the configured names if this one is unknown, so a
	// typo fails here rather than as a confusing connect error later.
	active_ = arguments.contains("connection") && arguments["connection"].is_string()
	  ? arguments["connection"].get<std::string>() : std::string{};
	(void)registry_.get(active_);

	if (tool_name == "listConnections") {
	  result_content = connections();
	}
	else if (tool_name == "listSchemas") {
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
	else if (tool_name == "wraparoundStatus") {
	  // No schema default here: wraparound is a whole-database property, and
	  // silently scoping it to "public" would understate the risk.
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "";
	  int limit = arguments.contains("limit") ? arguments["limit"].get<int>() : 20;
	  result_content = wraparound_status(target_schema, limit);
	}
	else if (tool_name == "checkpointStats") {
	  result_content = checkpoint_stats();
	}
	else if (tool_name == "tableIOStats") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string target_table  = arguments.contains("table")  ? arguments["table"].get<std::string>()  : "";
	  int limit = arguments.contains("limit") ? arguments["limit"].get<int>() : 20;
	  result_content = table_io_stats(target_schema, target_table, limit);
	}
	else if (tool_name == "hostCapacity") {
	  long long ram_mb = arguments.contains("ram_mb") && arguments["ram_mb"].is_number_integer()
	    ? arguments["ram_mb"].get<long long>() : 0;
	  int vcpus = arguments.contains("vcpus") && arguments["vcpus"].is_number_integer()
	    ? arguments["vcpus"].get<int>() : 0;
	  std::string storage = arguments.contains("storage") && arguments["storage"].is_string()
	    ? arguments["storage"].get<std::string>() : "";
	  result_content = host_capacity(ram_mb, vcpus, storage);
	}
	else if (tool_name == "duplicateIndexes") {
	  std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
	  std::string target_table  = arguments.contains("table")  ? arguments["table"].get<std::string>()  : "";
	  result_content = duplicate_indexes(target_schema, target_table);
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
	else if (tool_name == "explainQuery") {
	  // queryid is documented as a string because a 64-bit value does not
	  // survive JSON number precision, but accept a number too rather than
	  // fail with a raw nlohmann type error.
	  std::string qid;
	  if (arguments.contains("queryid")) {
	    if (arguments["queryid"].is_string())
	      qid = arguments["queryid"].get<std::string>();
	    else if (arguments["queryid"].is_number_integer())
	      qid = std::to_string(arguments["queryid"].get<long long>());
	  }
	  std::string sql = arguments.contains("sql") ? arguments["sql"].get<std::string>() : "";
	  json prms = arguments.contains("params") ? arguments["params"] : json::array();
	  bool do_analyze = arguments.contains("analyze") ? arguments["analyze"].get<bool>() : false;
	  int tmo = arguments.contains("timeout_ms") ? arguments["timeout_ms"].get<int>() : 0;
	  result_content = explain_query(qid, sql, prms, do_analyze, tmo);
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
