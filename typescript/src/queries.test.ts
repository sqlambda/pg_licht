import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import {
  parseConnStr,
  listSchemas,
  listTables,
  tableDetails,
  searchTables,
  listFunctions,
  functionDetails,
  searchFunctions,
  listEnums,
  enumDetails,
  searchEnums,
} from "./queries.js";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  process.stderr.write(
    "ERROR: DATABASE_URL environment variable is required to run tests.\n" +
    `  Example: DATABASE_URL="port=5555 dbname=pglitch" node --import tsx/esm --test src/queries.test.ts\n`
  );
  process.exit(1);
}

const testDbName = `pg_licht_test_${process.pid}`;
let adminClient: pg.Client;
let db: pg.Client;

before(async () => {
  const baseConfig = parseConnStr(DATABASE_URL);

  adminClient = new pg.Client(baseConfig);
  await adminClient.connect();
  await adminClient.query(`CREATE DATABASE "${testDbName}"`);

  db = new pg.Client({ ...baseConfig, database: testDbName });
  await db.connect();

  await db.query("CREATE SCHEMA grocery");

  await db.query(`
    CREATE TABLE grocery.users (
      id    SERIAL PRIMARY KEY,
      name  VARCHAR(100) NOT NULL,
      email VARCHAR(200) UNIQUE
    )
  `);
  await db.query("COMMENT ON TABLE grocery.users IS 'user account records'");
  await db.query("COMMENT ON COLUMN grocery.users.email IS 'unique email address'");
  await db.query("INSERT INTO grocery.users(name, email) VALUES ('Alice', 'alice@example.com')");
  await db.query("INSERT INTO grocery.users(name, email) VALUES ('Bob', 'bob@example.com')");
  await db.query("ANALYZE grocery.users");

  await db.query(`
    CREATE TABLE grocery.orders (
      id      SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES grocery.users(id),
      amount  NUMERIC(10,2) CHECK (amount > 0)
    )
  `);
  await db.query("INSERT INTO grocery.orders(user_id, amount) VALUES (1, 99.99)");
  await db.query("ANALYZE grocery.orders");

  await db.query(`
    CREATE TABLE grocery.user_account_log (
      id        SERIAL PRIMARY KEY,
      user_id   INT REFERENCES grocery.users(id),
      action    TEXT,
      logged_at TIMESTAMPTZ DEFAULT now()
    )
  `);

  await db.query("CREATE TABLE grocery.bare_notes (note TEXT)");

  await db.query(`
    CREATE FUNCTION grocery.log_user_action() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      INSERT INTO grocery.user_account_log(user_id, action) VALUES (NEW.id, TG_OP);
      RETURN NEW;
    END;
    $$
  `);
  await db.query("COMMENT ON FUNCTION grocery.log_user_action() IS 'audit trigger for user table'");
  await db.query(`
    CREATE TRIGGER trg_user_audit
    AFTER INSERT OR UPDATE ON grocery.users
    FOR EACH ROW EXECUTE FUNCTION grocery.log_user_action()
  `);
  await db.query(`
    CREATE FUNCTION grocery.get_user_count() RETURNS bigint LANGUAGE sql AS $$
      SELECT COUNT(*) FROM grocery.users;
    $$
  `);

  await db.query(
    "CREATE VIEW grocery.active_users AS SELECT id, name FROM grocery.users WHERE name IS NOT NULL"
  );
  await db.query(
    "CREATE MATERIALIZED VIEW grocery.user_stats AS SELECT COUNT(*) AS user_count FROM grocery.users"
  );
  await db.query("CREATE UNIQUE INDEX ON grocery.user_stats(user_count)");
  await db.query("REFRESH MATERIALIZED VIEW grocery.user_stats");
  await db.query("ANALYZE grocery.user_stats");

  await db.query(
    "CREATE TYPE grocery.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled')"
  );
  await db.query("COMMENT ON TYPE grocery.order_status IS 'status of a customer order'");
  await db.query("ALTER TABLE grocery.orders ADD COLUMN status grocery.order_status DEFAULT 'pending'");
  await db.query("CREATE TYPE grocery.user_role AS ENUM ('admin', 'user', 'guest')");

  await db.query("GRANT USAGE ON SCHEMA grocery TO PUBLIC");
  await db.query("GRANT SELECT ON grocery.users TO PUBLIC");
  await db.query("GRANT EXECUTE ON FUNCTION grocery.get_user_count() TO PUBLIC");
  await db.query("DROP ROLE IF EXISTS tomato");
  await db.query("DROP ROLE IF EXISTS carrot");
  await db.query("CREATE ROLE tomato");
  await db.query("CREATE ROLE carrot");
  await db.query("GRANT SELECT ON grocery.users TO tomato");
});

after(async () => {
  await db?.end();
  await adminClient?.query(`DROP DATABASE IF EXISTS "${testDbName}"`);
  await adminClient?.query("DROP ROLE IF EXISTS tomato");
  await adminClient?.query("DROP ROLE IF EXISTS carrot");
  await adminClient?.end();
});

// --- listSchemas ---

test("listSchemas returns object", async () => {
  const result = await listSchemas(db) as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).length > 0);
});

test("listSchemas contains grocery schema with users and orders", async () => {
  const result = await listSchemas(db) as Record<string, { tables: string[] }>;
  assert.ok(result["grocery"]);
  const tables: string[] = result["grocery"].tables;
  assert.ok(Array.isArray(tables));
  assert.ok(tables.includes("users"));
  assert.ok(tables.includes("orders"));
});

test("listSchemas excludes system schemas", async () => {
  const result = await listSchemas(db) as Record<string, unknown>;
  assert.ok(!("pg_catalog" in result));
  assert.ok(!("information_schema" in result));
});

test("listSchemas has roles field", async () => {
  const result = await listSchemas(db) as Record<string, Record<string, unknown>>;
  assert.ok(result["grocery"]);
  assert.ok("roles" in result["grocery"]);
  assert.ok(typeof result["grocery"]["roles"] === "object" && !Array.isArray(result["grocery"]["roles"]));
});

test("listSchemas roles shows granted privilege", async () => {
  const result = await listSchemas(db) as Record<string, Record<string, Record<string, string[]>>>;
  const roles = result["grocery"]["roles"];
  assert.ok(roles["PUBLIC"]);
  assert.ok(roles["PUBLIC"].includes("USAGE"));
});

// --- listTables ---

test("listTables returns known tables", async () => {
  const result = await listTables(db, "grocery") as Record<string, unknown>;
  assert.ok(result["users"]);
  assert.ok(result["orders"]);
});

test("listTables has expected fields", async () => {
  const result = await listTables(db, "grocery") as Record<string, Record<string, unknown>>;
  const users = result["users"];
  for (const field of ["description", "rows", "size", "seq_scan", "idx_scan", "n_live_tup", "n_dead_tup", "last_vacuum", "last_analyze", "columns", "indexes", "constraints"]) {
    assert.ok(field in users, `missing field: ${field}`);
  }
});

test("listTables includes table with no indexes", async () => {
  const result = await listTables(db, "grocery") as Record<string, unknown>;
  assert.ok(result["bare_notes"]);
});

test("listTables unknown schema returns null", async () => {
  const result = await listTables(db, "does_not_exist_schema");
  assert.ok(result == null || (typeof result === "object" && Object.keys(result as object).length === 0));
});

// --- searchTables ---

test("searchTables by table name", async () => {
  const result = await searchTables(db, "users") as Record<string, unknown>;
  assert.ok(result);
  const keys = Object.keys(result);
  assert.ok(keys.some(k => k.endsWith(".users")));
});

test("searchTables by table description", async () => {
  const result = await searchTables(db, "user account") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.endsWith(".users")));
});

test("searchTables does not match column comment", async () => {
  const result = await searchTables(db, "unique email") as Record<string, unknown> | null;
  if (result) {
    assert.ok(!Object.keys(result).some(k => k.endsWith(".users")));
  }
});

test("searchTables result key includes schema", async () => {
  const result = await searchTables(db, "users") as Record<string, unknown>;
  for (const key of Object.keys(result)) {
    assert.equal(key.split(".").length, 2, `key should have exactly one dot: ${key}`);
  }
});

test("searchTables finds snake_case name by words", async () => {
  const result = await searchTables(db, "user account log") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.endsWith(".user_account_log")));
});

// --- tableDetails ---

test("tableDetails returns expected top-level keys", async () => {
  const result = await tableDetails(db, "grocery", "users") as Record<string, unknown>;
  for (const field of ["table", "rows", "size", "description", "seq_scan", "idx_scan", "columns", "indexes", "constraints", "foreign_keys"]) {
    assert.ok(field in result, `missing field: ${field}`);
  }
});

test("tableDetails columns have type info", async () => {
  const result = await tableDetails(db, "grocery", "users") as Record<string, Record<string, Record<string, unknown>>>;
  const name = result["columns"]["name"];
  assert.ok("type" in name);
  assert.ok("format_type" in name);
  assert.ok("not_null" in name);
});

test("tableDetails columns include stats for analyzed table", async () => {
  const result = await tableDetails(db, "grocery", "users") as Record<string, Record<string, Record<string, unknown>>>;
  const name = result["columns"]["name"];
  assert.ok("null_frac" in name);
  assert.ok("avg_width" in name);
  assert.ok("n_distinct" in name);
});

test("tableDetails works when table is unanalyzed", async () => {
  const result = await tableDetails(db, "grocery", "bare_notes") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(result["columns"]);
});

test("tableDetails foreign keys are separate from constraints", async () => {
  const result = await tableDetails(db, "grocery", "orders") as Record<string, Record<string, Record<string, unknown>>>;
  assert.ok(Object.keys(result["foreign_keys"]).length > 0);
  for (const fk of Object.values(result["foreign_keys"])) {
    assert.ok("target_table" in fk);
  }
  for (const con of Object.values(result["constraints"])) {
    const def = con["definition"] as string;
    assert.ok(!def.includes("REFERENCES"), "FK should not appear in constraints");
  }
});

test("tableDetails includes triggers", async () => {
  const result = await tableDetails(db, "grocery", "users") as Record<string, Record<string, Record<string, unknown>>>;
  assert.ok(result["triggers"]);
  const trig = result["triggers"]["trg_user_audit"];
  assert.ok(trig);
  assert.ok("timing" in trig);
  assert.ok("events" in trig);
  assert.ok("function" in trig);
  assert.equal(trig["timing"], "AFTER");
  assert.ok((trig["events"] as string).includes("INSERT"));
});

test("searchTables finds table by grantee role name", async () => {
  const result = await searchTables(db, "tomato") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.endsWith(".users")));
});

test("searchTables returns nothing for role with no grants", async () => {
  const result = await searchTables(db, "carrot");
  assert.ok(result == null || Object.keys(result as object).length === 0);
});

test("searchTables finds table by schema name", async () => {
  const result = await searchTables(db, "grocery") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.startsWith("grocery.")));
});

test("searchTables result includes roles", async () => {
  const result = await searchTables(db, "users") as Record<string, Record<string, unknown>>;
  const entry = Object.entries(result).find(([k]) => k.endsWith(".users"));
  assert.ok(entry, "users table not found in search results");
  const roles = entry[1]["roles"] as Record<string, string[]>;
  assert.ok(typeof roles === "object" && !Array.isArray(roles));
  assert.ok(roles["PUBLIC"]);
  assert.ok(roles["PUBLIC"].includes("SELECT"));
});

// --- listFunctions ---

test("listFunctions returns both functions", async () => {
  const result = await listFunctions(db, "grocery") as Record<string, unknown>;
  const keys = Object.keys(result);
  assert.ok(keys.some(k => k.includes("log_user_action")));
  assert.ok(keys.some(k => k.includes("get_user_count")));
});

test("listFunctions excludes aggregates and window functions", async () => {
  const result = await listFunctions(db, "grocery") as Record<string, Record<string, unknown>>;
  for (const value of Object.values(result)) {
    const kind = value["kind"] as string;
    assert.ok(kind === "function" || kind === "procedure");
  }
});

test("listFunctions has expected fields", async () => {
  const result = await listFunctions(db, "grocery") as Record<string, Record<string, unknown>>;
  const entry = Object.entries(result).find(([k]) => k.includes("get_user_count"))![1];
  assert.ok(entry);
  for (const field of ["kind", "language", "return_type", "arguments"]) {
    assert.ok(field in entry, `missing field: ${field}`);
  }
  assert.equal(entry["kind"], "function");
  assert.equal(entry["language"], "sql");
});

// --- functionDetails ---

test("functionDetails includes source and definition", async () => {
  const result = await functionDetails(db, "grocery", "get_user_count") as Record<string, Record<string, unknown>>;
  assert.ok(result);
  for (const value of Object.values(result)) {
    assert.ok("source" in value);
    assert.ok("definition" in value);
    assert.ok((value["source"] as string).length > 0);
  }
});

test("functionDetails shows trigger usage", async () => {
  const result = await functionDetails(db, "grocery", "log_user_action") as Record<string, Record<string, unknown>>;
  assert.ok(result);
  for (const value of Object.values(result)) {
    const triggers = value["used_in_triggers"] as unknown[];
    assert.ok(Array.isArray(triggers));
    assert.ok(triggers.length >= 1);
  }
});

// --- searchFunctions ---

test("searchFunctions by name", async () => {
  const result = await searchFunctions(db, "user count") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.includes("get_user_count")));
});

test("searchFunctions by source", async () => {
  const result = await searchFunctions(db, "COUNT") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.includes("get_user_count")));
});

test("searchFunctions by trigger name", async () => {
  const result = await searchFunctions(db, "audit") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.includes("log_user_action")));
});

// --- views and materialized views ---

test("listSchemas contains view", async () => {
  const result = await listSchemas(db) as Record<string, { tables: string[] }>;
  assert.ok(result["grocery"].tables.includes("active_users"));
});

test("listSchemas contains materialized view", async () => {
  const result = await listSchemas(db) as Record<string, { tables: string[] }>;
  assert.ok(result["grocery"].tables.includes("user_stats"));
});

test("listTables contains view and materialized view", async () => {
  const result = await listTables(db, "grocery") as Record<string, unknown>;
  assert.ok(result["active_users"]);
  assert.ok(result["user_stats"]);
});

test("listTables view has kind field", async () => {
  const result = await listTables(db, "grocery") as Record<string, Record<string, unknown>>;
  assert.equal(result["active_users"]["kind"], "view");
});

test("listTables materialized view has kind field", async () => {
  const result = await listTables(db, "grocery") as Record<string, Record<string, unknown>>;
  assert.equal(result["user_stats"]["kind"], "materialized view");
});

test("tableDetails works for view", async () => {
  const result = await tableDetails(db, "grocery", "active_users") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(result["columns"]);
  assert.ok(result["definition"]);
  assert.ok((result["definition"] as string).length > 0);
});

test("tableDetails works for materialized view", async () => {
  const result = await tableDetails(db, "grocery", "user_stats") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(result["columns"]);
  assert.ok(result["definition"]);
  assert.ok((result["definition"] as string).length > 0);
  assert.ok(Object.keys(result["indexes"] as object).length > 0);
});

// --- listEnums ---

test("listEnums returns known enums", async () => {
  const result = await listEnums(db, "grocery") as Record<string, unknown>;
  assert.ok(result["order_status"]);
  assert.ok(result["user_role"]);
});

test("listEnums has expected fields", async () => {
  const result = await listEnums(db, "grocery") as Record<string, Record<string, unknown>>;
  const os = result["order_status"];
  assert.ok("description" in os);
  assert.ok("values" in os);
  assert.ok(Array.isArray(os["values"]));
  assert.equal(os["description"], "status of a customer order");
});

test("listEnums values are ordered", async () => {
  const result = await listEnums(db, "grocery") as Record<string, Record<string, string[]>>;
  const values = result["order_status"]["values"];
  assert.ok(values.length >= 5);
  assert.equal(values[0], "pending");
  assert.equal(values[2], "shipped");
  assert.equal(values[4], "cancelled");
});

test("listEnums unknown schema returns null", async () => {
  const result = await listEnums(db, "does_not_exist_schema");
  assert.ok(result == null || (typeof result === "object" && Object.keys(result as object).length === 0));
});

// --- enumDetails ---

test("enumDetails has values and description", async () => {
  const result = await enumDetails(db, "grocery", "order_status") as Record<string, unknown>;
  assert.ok(result);
  assert.ok("description" in result);
  assert.ok("values" in result);
  assert.ok(Array.isArray(result["values"]));
  assert.equal(result["description"], "status of a customer order");
});

test("enumDetails has used_by_columns referencing orders", async () => {
  const result = await enumDetails(db, "grocery", "order_status") as Record<string, unknown>;
  const cols = result["used_by_columns"] as Array<{ table: string; column: string }>;
  assert.ok(Array.isArray(cols));
  assert.ok(cols.length >= 1);
  assert.ok(cols.some(c => c.table.includes("orders")));
});

test("enumDetails unused enum has empty used_by_columns", async () => {
  const result = await enumDetails(db, "grocery", "user_role") as Record<string, unknown>;
  const cols = result["used_by_columns"] as unknown[];
  assert.ok(Array.isArray(cols));
  assert.equal(cols.length, 0);
});

test("enumDetails not found returns null", async () => {
  const result = await enumDetails(db, "grocery", "nonexistent_enum");
  assert.ok(result == null || (typeof result === "object" && Object.keys(result as object).length === 0));
});

// --- searchEnums ---

test("searchEnums by name", async () => {
  const result = await searchEnums(db, "order status") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.includes("order_status")));
});

test("searchEnums by value", async () => {
  const result = await searchEnums(db, "shipped") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.includes("order_status")));
});

test("searchEnums by description", async () => {
  const result = await searchEnums(db, "customer order") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.includes("order_status")));
});

test("searchEnums result key includes schema", async () => {
  const result = await searchEnums(db, "order status") as Record<string, unknown>;
  for (const key of Object.keys(result)) {
    assert.equal(key.split(".").length, 2, `key should have exactly one dot: ${key}`);
  }
});

// --- searchTables via enum types ---

test("searchTables by enum value", async () => {
  const result = await searchTables(db, "shipped") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.endsWith(".orders")));
});

test("searchTables by enum name", async () => {
  const result = await searchTables(db, "order status") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.endsWith(".orders")));
});

test("searchTables by enum description", async () => {
  const result = await searchTables(db, "customer order") as Record<string, unknown>;
  assert.ok(result);
  assert.ok(Object.keys(result).some(k => k.endsWith(".orders")));
});

// --- roles in tableDetails and functionDetails ---

test("tableDetails has roles field", async () => {
  const result = await tableDetails(db, "grocery", "users") as Record<string, unknown>;
  assert.ok("roles" in result);
  assert.ok(typeof result["roles"] === "object" && !Array.isArray(result["roles"]));
});

test("tableDetails roles shows granted privilege", async () => {
  const result = await tableDetails(db, "grocery", "users") as Record<string, Record<string, string[]>>;
  const roles = result["roles"];
  assert.ok(roles["PUBLIC"]);
  assert.ok(roles["PUBLIC"].includes("SELECT"));
});

test("tableDetails no grants returns empty roles", async () => {
  const result = await tableDetails(db, "grocery", "bare_notes") as Record<string, unknown>;
  assert.ok("roles" in result);
  const roles = result["roles"] as Record<string, unknown>;
  assert.ok(typeof roles === "object" && !Array.isArray(roles));
  assert.equal(Object.keys(roles).length, 0);
});

test("functionDetails has roles field", async () => {
  const result = await functionDetails(db, "grocery", "get_user_count") as Record<string, Record<string, unknown>>;
  for (const value of Object.values(result)) {
    assert.ok("roles" in value);
    assert.ok(typeof value["roles"] === "object" && !Array.isArray(value["roles"]));
  }
});

test("functionDetails roles shows granted privilege", async () => {
  const result = await functionDetails(db, "grocery", "get_user_count") as Record<string, Record<string, Record<string, string[]>>>;
  for (const value of Object.values(result)) {
    assert.ok(value["roles"]["PUBLIC"]);
    assert.ok(value["roles"]["PUBLIC"].includes("EXECUTE"));
  }
});

test("functionDetails no grants returns empty roles", async () => {
  const result = await functionDetails(db, "grocery", "log_user_action") as Record<string, Record<string, unknown>>;
  for (const value of Object.values(result)) {
    assert.ok("roles" in value);
    const roles = value["roles"] as Record<string, unknown>;
    assert.ok(typeof roles === "object" && !Array.isArray(roles));
    assert.equal(Object.keys(roles).length, 0);
  }
});
