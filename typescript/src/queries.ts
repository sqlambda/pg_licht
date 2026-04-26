import pg from "pg";

export function parseConnStr(s: string): pg.ClientConfig {
  if (s.startsWith("postgresql://") || s.startsWith("postgres://")) {
    return { connectionString: s };
  }
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
  if (!cfg.host) cfg.host = "/var/run/postgresql";
  return cfg;
}

async function run<T = Record<string, unknown>>(client: pg.Client, sql: string, params: unknown[] = []): Promise<T | null> {
  const res = await client.query(sql, params);
  const val = res.rows[0]?.[res.fields[0]?.name];
  if (val == null) return null;
  return (typeof val === "string" ? JSON.parse(val) : val) as T;
}

export async function listSchemas(client: pg.Client) {
  return await run(client, `
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
      AND relnames IS NOT NULL
  `);
}

export async function listTables(client: pg.Client, schema: string) {
  return await run(client, `
    SELECT JSONB_OBJECT_AGG(c.relname,
            JSONB_BUILD_OBJECT(
             'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                    WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
             'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
             'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
             'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
             'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
             'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
             'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
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
      AND c.relkind IN ('r', 'p', 'm', 'v')
  `, [schema]);
}

export async function tableDetails(client: pg.Client, schema: string, table: string) {
  return await run(client, `
    SELECT JSONB_BUILD_OBJECT(
             'table', c.relname, 'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
             'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
             'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                    WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
             'definition', CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END,
             'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
             'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
             'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
             'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
             'columns', columns,
             'indexes', COALESCE(indexes, '{}'::jsonb),
             'constraints', COALESCE(constraints, '{}'::jsonb),
             'foreign_keys', COALESCE(foreign_keys, '{}'::jsonb),
             'triggers', COALESCE(triggers, '{}'::jsonb),
             'roles', COALESCE(roles, '{}'::jsonb))
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
    LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles
                       FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                                    JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                             FROM aclexplode(c.relacl) AS a
                             LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                             GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub) ON true
    WHERE c.relnamespace = $1::regnamespace
      AND c.relname = $2
  `, [schema, table]);
}

export async function searchTables(client: pg.Client, webSearch: string) {
  return await run(client, `
    SELECT JSONB_OBJECT_AGG(c.relnamespace::regnamespace::name || '.' || c.relname,
            JSONB_BUILD_OBJECT(
             'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                    WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
             'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
             'rows', c.reltuples, 'size', c.relpages::bigint * 8192,
             'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
             'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
             'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
             'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze),
             'columns', columns, 'indexes', indexes, 'constraints', constraints,
             'roles', COALESCE(roles, '{}'::jsonb)))
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
    LEFT JOIN LATERAL (
        SELECT JSONB_OBJECT_AGG(grantee, privs) AS roles,
               STRING_AGG(grantee, ' ' ORDER BY grantee) AS role_names
        FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                     JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
              FROM aclexplode(c.relacl) AS a
              LEFT JOIN pg_roles AS r ON r.oid = a.grantee
              GROUP BY COALESCE(r.rolname, 'PUBLIC')) sub
    ) ON true
    LEFT JOIN LATERAL (
        SELECT STRING_AGG(
            REGEXP_REPLACE(REGEXP_REPLACE(et.typname, '_', ' ', 'g'), '([[:upper:]])', ' \\1', 'g') || ' ' ||
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
            REGEXP_REPLACE(REGEXP_REPLACE(c.relname, '_', ' ', 'g'), '([[:upper:]])', ' \\1', 'g') || ' ' ||
            REGEXP_REPLACE(REGEXP_REPLACE(c.relnamespace::regnamespace::name, '_', ' ', 'g'), '([[:upper:]])', ' \\1', 'g') || ' ' ||
            COALESCE(obj_description(c.oid, 'pg_class'), '') || ' ' ||
            COALESCE(enum_text, '') || ' ' ||
            COALESCE(role_names, '')
          ) @@ websearch_to_tsquery('english', $1)
      AND c.relkind IN ('r', 'p', 'm', 'v')
      AND c.relnamespace NOT IN (
          SELECT oid FROM pg_namespace
          WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema')
  `, [webSearch]);
}

export async function listFunctions(client: pg.Client, schema: string) {
  return await run(client, `
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

export async function functionDetails(client: pg.Client, schema: string, funcName: string) {
  return await run(client, `
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
      AND  p.prokind IN ('f', 'p')
  `, [schema, funcName]);
}

export async function searchFunctions(client: pg.Client, webSearch: string) {
  if (!webSearch.trim()) return null;
  return await run(client, `
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

export async function listEnums(client: pg.Client, schema: string) {
  return await run(client, `
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
      AND t.typtype = 'e'
  `, [schema]);
}

export async function enumDetails(client: pg.Client, schema: string, enumName: string) {
  return await run(client, `
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
      AND t.typtype = 'e'
  `, [schema, enumName]);
}

export async function searchEnums(client: pg.Client, webSearch: string) {
  if (!webSearch.trim()) return null;
  return await run(client, `
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
            REGEXP_REPLACE(REGEXP_REPLACE(t.typname, '_', ' ', 'g'), '([[:upper:]])', ' \\1', 'g'))
            @@ websearch_to_tsquery('english', $1)
       OR TO_TSVECTOR('english', COALESCE(obj_description(t.oid, 'pg_type'), ''))
            @@ websearch_to_tsquery('english', $1)
       OR TO_TSVECTOR('english', COALESCE(values_text, ''))
            @@ websearch_to_tsquery('english', $1)
      )
  `, [webSearch]);
}
