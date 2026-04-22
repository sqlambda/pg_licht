#pragma once

#include <iostream>
#include <string>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>

using json = nlohmann::json;

class PostgresMCPServer {
public:
  PostgresMCPServer(const std::string& conn_str) : conn(conn_str) {}

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
	    {"description", "return table details like columns, foreign keys, indexes, data histograms"},
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
	  }
	}}
    };
  }

  const json schemas() {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(nspname,
              JSONB_BUILD_OBJECT(
               'tables', relnames))
      FROM pg_namespace
      LEFT JOIN LATERAL (SELECT JSONB_AGG(relname ORDER BY relname) AS relnames
                         FROM pg_class
                         WHERE relnamespace = pg_namespace.oid
                           AND relkind IN ('r','m','f','p','v')
                           AND relpersistence <> 't') ON true
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
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum), 'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
               'columns', columns, 'indexes', indexes, 'constraints', constraints))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(attname, col_description(c.oid, attnum)) AS columns
                         FROM pg_attribute
                         WHERE attnum > 0
                           AND attrelid = c.oid
                           AND NOT attisdropped) ON true
      LEFT JOIN LATERAL (SELECT JSONB_AGG(indexdef) AS indexes
                         FROM pg_indexes AS i
                         WHERE i.schemaname = $1
                           AND i.tablename = c.relname) ON true
      LEFT JOIN LATERAL (SELECT JSONB_AGG(pg_get_constraintdef(oid)) AS constraints
                         FROM pg_constraint
                         WHERE conrelid = c.oid) ON true
      WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND c.relkind IN ('r', 'p', 'm', 'v');
    )";

    pqxx::result res = txn.exec(query, pqxx::params{schema});

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
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum), 'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
               'columns', columns, 'indexes', indexes, 'constraints', constraints))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(attname, col_description(c.oid, attnum)) AS columns
                         FROM pg_attribute
                         WHERE attnum > 0
                           AND attrelid = c.oid
                           AND NOT attisdropped) ON true
      LEFT JOIN LATERAL (SELECT JSONB_AGG(indexdef) AS indexes
                         FROM pg_indexes AS i
                         WHERE i.schemaname = c.relnamespace::regnamespace::name
                           AND i.tablename = c.relname) ON true
      LEFT JOIN LATERAL (SELECT JSONB_AGG(pg_get_constraintdef(oid)) AS constraints
                         FROM pg_constraint
                         WHERE conrelid = c.oid) ON true
      WHERE (c.oid IN (SELECT DISTINCT objoid
                       FROM pg_description
                       WHERE to_tsvector('english', description) @@ websearch_to_tsquery('english', $1)
                         AND classoid = 'pg_class'::regclass
                         AND objsubid = 0)
          OR TO_TSVECTOR('english',
               REGEXP_REPLACE(
                 REGEXP_REPLACE(c.relname, '_', ' ', 'g'),
                 '([[:upper:]])', ' \1', 'g'))
               @@ websearch_to_tsquery('english', $1))
        AND c.relkind IN ('r', 'p', 'm', 'v')
        AND c.relnamespace NOT IN (
            SELECT oid FROM pg_namespace
            WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');
    )";

    pqxx::result res = txn.exec(query, pqxx::params{web_search});

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

    pqxx::result res = txn.exec(query, pqxx::params{schema});

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
                 'used_in_triggers', used_in_triggers
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
      WHERE  p.pronamespace = $1::regnamespace
        AND  p.proname = $2
        AND  p.prokind IN ('f', 'p');
    )";

    pqxx::result res = txn.exec(query, pqxx::params{schema, func_name});

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

    pqxx::result res = txn.exec(query, pqxx::params{web_search});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_functions = res[0][0].as<std::string>();
      return json::parse(pgsql_functions);
    } else {
      return {};
    }
  }

  const json table(const std::string& schema, const std::string& table) {
    pqxx::work txn{conn};

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'table', c.relname, 'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'definition', CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END,
               'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan, 'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum), 'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
               'columns', columns,
               'indexes', COALESCE(indexes, '{}'::jsonb),
               'constraints', COALESCE(constraints, '{}'::jsonb),
               'foreign_keys', COALESCE(foreign_keys, '{}'::jsonb),
               'triggers', COALESCE(triggers, '{}'::jsonb))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN LATERAL (SELECT JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(a.attname,
                        JSONB_BUILD_OBJECT(
                         'description', col_description(c.oid, attnum),
                         'type', t.typname,
                         'format_type', format_type(a.atttypid, a.atttypmod),
                         'size', NULLIF(a.attlen, -1),
                         'not_null', a.attnotnull,
                         'null_frac', ps.null_frac,
                         'avg_width', ps.avg_width,
                         'n_distinct', ps.n_distinct,
                         'physical_order_correlation', ps.correlation,
                         'most_common_vals', ps.most_common_vals,
                         'most_common_freqs', ps.most_common_freqs))) AS columns
                       FROM pg_attribute AS a
                       JOIN pg_type AS t ON t.oid = a.atttypid
                       LEFT JOIN pg_stats AS ps ON ps.schemaname = $1 AND ps.tablename = $2 AND ps.attname = a.attname
                       WHERE attnum > 0
                         AND attrelid = c.oid
                         AND NOT attisdropped) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(indexname,
                          JSONB_BUILD_OBJECT(
                           'definition', indexdef, 'index_uses', idx_scan, 'last_use', last_idx_scan)) AS indexes
                         FROM pg_indexes AS i
                         JOIN pg_stat_user_indexes si ON si.indexrelname = i.indexname
                                                      AND si.schemaname = i.schemaname
                                                      AND si.relname = i.tablename
                         WHERE i.schemaname = $1
                           AND i.tablename = c.relname) ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(conname,
                          JSONB_BUILD_OBJECT(
                           'definition', pg_get_constraintdef(oid))) AS constraints
                         FROM pg_constraint
                         WHERE conrelid = c.oid
                           AND contype != 'f') ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(fk.conname,
                          JSONB_BUILD_OBJECT(
                           'target_table', fk.confrelid::regclass::text,
                           'source_columns', (SELECT jsonb_agg(a.attname) FROM pg_attribute a WHERE a.attrelid = fk.conrelid AND a.attnum = ANY(fk.conkey)),
                           'target_columns', (SELECT jsonb_agg(a.attname) FROM pg_attribute a WHERE a.attrelid = fk.confrelid AND a.attnum = ANY(fk.confkey)),
                           'definition', pg_get_constraintdef(fk.oid))) AS foreign_keys
                         FROM pg_constraint fk
                         WHERE fk.conrelid = c.oid
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
                                          ]::text[], ' OR '))) AS triggers
                         FROM   pg_trigger AS t
                         JOIN   pg_proc AS p ON p.oid = t.tgfoid
                         JOIN   pg_language AS l ON l.oid = p.prolang
                         WHERE  t.tgrelid = c.oid
                           AND  NOT t.tgisinternal) ON true
      WHERE c.relnamespace = $1::regnamespace
        AND c.relname = $2;
    )";

    pqxx::result res = txn.exec(query, pqxx::params{schema, table});

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
        {"serverInfo", {{"name", "pg-licht-cpp"}, {"version", "1.0.0"}}}
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
