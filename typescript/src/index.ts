#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";

const { Client } = pg;

const connStr = process.env.DATABASE_URL ?? process.argv[2];
if (!connStr) {
  process.stderr.write(
    "Usage: pg-licht-mcp <connection-string>\n" +
    "Or set the DATABASE_URL environment variable.\n"
  );
  process.exit(1);
}

function parseConnStr(s: string): pg.ClientConfig {
  if (s.startsWith("postgresql://") || s.startsWith("postgres://")) {
    return { connectionString: s };
  }
  // libpq key=value format
  const cfg: pg.ClientConfig = {};
  const re = /(\w+)\s*=\s*(?:'([^']*)'|(\S+))/g;
  for (const m of s.matchAll(re)) {
    const val = m[2] ?? m[3];
    switch (m[1]) {
      case "host":     cfg.host     = val; break;
      case "port":     cfg.port     = parseInt(val, 10); break;
      case "dbname":
      case "database": cfg.database = val; break;
      case "user":     cfg.user     = val; break;
      case "password": cfg.password = val; break;
    }
  }
  // no host → Unix socket (same default as libpq)
  if (!cfg.host) cfg.host = "/var/run/postgresql";
  return cfg;
}

const client = new Client(parseConnStr(connStr));
await client.connect();

async function query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T | null> {
  const res = await client.query(sql, params);
  const val = res.rows[0]?.[res.fields[0]?.name];
  if (val == null) return null;
  return (typeof val === "string" ? JSON.parse(val) : val) as T;
}

// ---------------------------------------------------------------------------
// Tool implementations — SQL is identical to the C++ server.h queries
// ---------------------------------------------------------------------------

async function listSchemas() {
  return await query(`
    SELECT JSONB_OBJECT_AGG(nspname,
            JSONB_BUILD_OBJECT('tables', relnames))
    FROM pg_namespace
    LEFT JOIN LATERAL (SELECT JSONB_AGG(relname ORDER BY relname) AS relnames
                       FROM pg_class
                       WHERE relnamespace = pg_namespace.oid
                         AND relkind IN ('r','m','f','p')
                         AND relpersistence <> 't') ON true
    WHERE nspname NOT LIKE 'pg_%'
      AND nspname <> 'information_schema'
      AND relnames IS NOT NULL
  `);
}

async function listTables(schema: string) {
  return await query(`
    SELECT JSONB_OBJECT_AGG(c.relname,
            JSONB_BUILD_OBJECT(
             'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
             'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
             'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
             'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
             'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
             'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
             'columns', columns, 'indexes', indexes, 'constraints', constraints))
    FROM pg_class AS c
    JOIN pg_stat_user_tables AS s ON s.relid = c.oid
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
      AND c.relkind IN ('r', 'p', 'm')
  `, [schema]);
}

async function tableDetails(schema: string, table: string) {
  return await query(`
    SELECT JSONB_BUILD_OBJECT(
             'table', c.relname, 'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
             'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
             'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
             'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
             'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
             'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
             'columns', columns,
             'indexes', COALESCE(indexes, '{}'::jsonb),
             'constraints', COALESCE(constraints, '{}'::jsonb),
             'foreign_keys', COALESCE(foreign_keys, '{}'::jsonb),
             'triggers', COALESCE(triggers, '{}'::jsonb))
    FROM pg_class AS c
    JOIN pg_stat_user_tables AS s ON s.relid = c.oid
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
      AND c.relname = $2
  `, [schema, table]);
}

async function searchTables(webSearch: string) {
  return await query(`
    SELECT JSONB_OBJECT_AGG(c.relnamespace::regnamespace::name || '.' || c.relname,
            JSONB_BUILD_OBJECT(
             'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
             'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
             'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
             'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
             'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
             'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
             'columns', columns, 'indexes', indexes, 'constraints', constraints))
    FROM pg_class AS c
    JOIN pg_stat_user_tables AS s ON s.relid = c.oid
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
    WHERE c.oid IN (SELECT DISTINCT objoid
                    FROM pg_description
                    WHERE to_tsvector('english', description) @@ websearch_to_tsquery('english', $1)
                      AND classoid = 'pg_class'::regclass
                      AND objsubid = 0)
       OR TO_TSVECTOR('english',
            REGEXP_REPLACE(
              REGEXP_REPLACE(c.relname, '_', ' ', 'g'),
              '([[:upper:]])', ' \\1', 'g'))
            @@ websearch_to_tsquery('english', $1)
  `, [webSearch]);
}

async function listFunctions(schema: string) {
  return await query(`
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
      AND  p.prokind IN ('f', 'p')
  `, [schema]);
}

async function functionDetails(schema: string, funcName: string) {
  return await query(`
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
      AND  p.prokind IN ('f', 'p')
  `, [schema, funcName]);
}

async function searchFunctions(webSearch: string) {
  if (!webSearch.trim()) return null;
  return await query(`
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
            REGEXP_REPLACE(REGEXP_REPLACE(p.proname, '_', ' ', 'g'), '([[:upper:]])', ' \\1', 'g'))
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
      )
  `, [webSearch]);
}

// ---------------------------------------------------------------------------
// MCP server
// ---------------------------------------------------------------------------

const server = new Server(
  { name: "pg-licht-mcp", version: "1.0.0" },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "listSchemas",
      description: "return schema list with basic summaries",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "listTables",
      description: "return table list with basic statistics",
      inputSchema: {
        type: "object",
        properties: { schema: { type: "string" } },
        required: ["schema"],
      },
    },
    {
      name: "tableDetails",
      description: "return table details like columns, foreign keys, indexes, triggers, data histograms",
      inputSchema: {
        type: "object",
        properties: {
          schema: { type: "string" },
          table: { type: "string" },
        },
        required: ["schema", "table"],
      },
    },
    {
      name: "searchTables",
      description: "return table list with basic statistics based on text search",
      inputSchema: {
        type: "object",
        properties: { web_search: { type: "string" } },
        required: ["web_search"],
      },
    },
    {
      name: "listFunctions",
      description: "return function and procedure list for a schema",
      inputSchema: {
        type: "object",
        properties: { schema: { type: "string" } },
        required: ["schema"],
      },
    },
    {
      name: "functionDetails",
      description: "return detailed function or procedure info including source and trigger usage",
      inputSchema: {
        type: "object",
        properties: {
          schema: { type: "string" },
          function: { type: "string" },
        },
        required: ["schema", "function"],
      },
    },
    {
      name: "searchFunctions",
      description: "search functions and procedures by name, source, language, trigger name, or description",
      inputSchema: {
        type: "object",
        properties: { web_search: { type: "string" } },
        required: ["web_search"],
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args = {} } = req.params;

  try {
    let result: unknown;

    switch (name) {
      case "listSchemas":
        result = await listSchemas();
        break;

      case "listTables":
        result = await listTables((args.schema as string) ?? "public");
        break;

      case "tableDetails":
        result = await tableDetails(
          (args.schema as string) ?? "public",
          (args.table as string) ?? ""
        );
        break;

      case "searchTables":
        result = await searchTables((args.web_search as string) ?? "");
        break;

      case "listFunctions":
        result = await listFunctions((args.schema as string) ?? "public");
        break;

      case "functionDetails":
        result = await functionDetails(
          (args.schema as string) ?? "public",
          (args.function as string) ?? ""
        );
        break;

      case "searchFunctions":
        result = await searchFunctions((args.web_search as string) ?? "");
        break;

      default:
        return {
          content: [{ type: "text", text: `Unknown tool: ${name}` }],
          isError: true,
        };
    }

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
      isError: false,
    };
  } catch (err) {
    return {
      content: [{ type: "text", text: `Error: ${(err as Error).message}` }],
      isError: true,
    };
  }
});

const transport = new StdioServerTransport();
await server.connect(transport);
