#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include <sstream>
#include <fstream>
#include <sys/stat.h>
#if defined(__GNUC__) && !defined(__clang__)
// GCC-only false positive inside <regex> internals at -O1+; Clang doesn't
// have this warning group at all, and with -Werror active it would hard-fail
// on "unknown warning group" if this pragma weren't guarded.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <regex>
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#include "server.h"

// Server version as libpq reports it (e.g. 160004 for 16.4), for gating tests
// on features that require a minimum PostgreSQL major -- explainQuery's
// GENERIC_PLAN path needs 16+.
static int pg_server_version_num(const std::string& url) {
  pqxx::connection c(url);
  return c.server_version();
}

class PostgresMCPServerTest : public ::testing::Test {
protected:
  static pqxx::connection* admin_conn;
  static PostgresMCPServer* srv;
  static std::string test_dbname;
  static std::string base_url;
  static std::string test_url;

  static void SetUpTestSuite() {
    const char* env_url = std::getenv("DATABASE_URL");
    base_url = env_url;

    // from_url picks host capacity up from the environment, so a developer who
    // exports these for their own cluster would otherwise change what the
    // hostCapacity tests see.
    ::unsetenv("PG_LICHT_HOST_RAM_MB");
    ::unsetenv("PG_LICHT_HOST_VCPUS");
    ::unsetenv("PG_LICHT_HOST_STORAGE");
    ::unsetenv("PG_LICHT_HOST_NOTE");

    try {
      admin_conn = new pqxx::connection(base_url);

      test_dbname = "pg_licht_test_" + std::to_string(getpid());

      pqxx::nontransaction ntxn(*admin_conn);
      pqxx::result check_res = ntxn.exec(
        "SELECT 1 FROM pg_database WHERE datname = " + ntxn.quote(test_dbname)
      );

      if (!check_res.empty()) {
        ADD_FAILURE() << "Test database " << test_dbname << " already exists. Remove it before running tests.";
        return;
      }

      ntxn.exec("CREATE DATABASE \"" + test_dbname + "\"");
      ntxn.commit();

      std::regex dbname_re(R"(\bdbname\s*=\s*\S+)");
      if (std::regex_search(base_url, dbname_re)) {
        test_url = std::regex_replace(base_url, dbname_re, "dbname=" + test_dbname);
      } else {
        size_t db_pos = base_url.rfind("/");
        if (db_pos != std::string::npos) {
          test_url = base_url.substr(0, db_pos + 1) + test_dbname;
        } else {
          test_url = base_url + " dbname=" + test_dbname;
        }
      }

      srv = new PostgresMCPServer(test_url);

      pqxx::connection test_conn(test_url);
      pqxx::work txn(test_conn);

      txn.exec("CREATE SCHEMA grocery");

      txn.exec(
        "CREATE TABLE grocery.users ("
        "  id    SERIAL PRIMARY KEY,"
        "  name  VARCHAR(100) NOT NULL,"
        "  email VARCHAR(200) UNIQUE"
        ")"
      );
      txn.exec("COMMENT ON TABLE grocery.users IS 'user account records'");
      txn.exec("COMMENT ON COLUMN grocery.users.email IS 'unique email address'");
      txn.exec("INSERT INTO grocery.users(name, email) VALUES ('Alice', 'alice@example.com')");
      txn.exec("INSERT INTO grocery.users(name, email) VALUES ('Bob', 'bob@example.com')");
      txn.exec("ANALYZE grocery.users");

      txn.exec(
        "CREATE TABLE grocery.orders ("
        "  id      SERIAL PRIMARY KEY,"
        "  user_id INT NOT NULL REFERENCES grocery.users(id),"
        "  amount  NUMERIC(10,2) CHECK (amount > 0)"
        ")"
      );
      txn.exec("INSERT INTO grocery.orders(user_id, amount) VALUES (1, 99.99)");
      txn.exec("ANALYZE grocery.orders");

      txn.exec("ALTER TABLE grocery.orders ENABLE ROW LEVEL SECURITY");
      txn.exec(
        "CREATE POLICY orders_owner_only ON grocery.orders"
        " FOR SELECT USING (user_id = current_setting('app.user_id', true)::int)"
      );

      txn.exec("CREATE SEQUENCE grocery.order_number_seq START 100 INCREMENT 1");
      txn.exec("COMMENT ON SEQUENCE grocery.order_number_seq IS 'external order numbering'");

      txn.exec(
        "CREATE TABLE grocery.user_account_log ("
        "  id         SERIAL PRIMARY KEY,"
        "  user_id    INT REFERENCES grocery.users(id),"
        "  action     TEXT,"
        "  logged_at  TIMESTAMPTZ DEFAULT now()"
        ")"
      );

      txn.exec("CREATE TABLE grocery.bare_notes (note TEXT)");

      txn.exec(
        "CREATE TABLE grocery.order_tags ("
        "  order_id INT  NOT NULL REFERENCES grocery.orders(id),"
        "  tag      TEXT NOT NULL,"
        "  PRIMARY KEY (order_id, tag)"
        ")"
      );
      txn.exec("INSERT INTO grocery.order_tags(order_id, tag) VALUES (1, 'organic')");

      txn.exec(
        "CREATE TABLE grocery.product_refs ("
        "  id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),"
        "  name TEXT"
        ")"
      );
      txn.exec("INSERT INTO grocery.product_refs(id, name) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'Widget')");

      txn.exec(
        "CREATE FUNCTION grocery.log_user_action() RETURNS trigger LANGUAGE plpgsql AS $$\n"
        "BEGIN\n"
        "  INSERT INTO grocery.user_account_log(user_id, action) VALUES (NEW.id, TG_OP);\n"
        "  RETURN NEW;\n"
        "END;\n"
        "$$"
      );
      txn.exec("COMMENT ON FUNCTION grocery.log_user_action() IS 'audit trigger for user table'");
      txn.exec(
        "CREATE TRIGGER trg_user_audit"
        " AFTER INSERT OR UPDATE ON grocery.users"
        " FOR EACH ROW EXECUTE FUNCTION grocery.log_user_action()"
      );
      txn.exec(
        "CREATE TRIGGER trg_user_audit_conditional"
        " AFTER UPDATE ON grocery.users"
        " FOR EACH ROW WHEN (OLD.name IS DISTINCT FROM NEW.name)"
        " EXECUTE FUNCTION grocery.log_user_action()"
      );
      txn.exec(
        "CREATE FUNCTION grocery.get_user_count() RETURNS bigint LANGUAGE sql AS $$"
        " SELECT COUNT(*) FROM grocery.users; $$"
      );

      txn.exec(
        "CREATE VIEW grocery.active_users AS "
        "  SELECT id, name FROM grocery.users WHERE name IS NOT NULL"
      );

      txn.exec(
        "CREATE MATERIALIZED VIEW grocery.user_stats AS "
        "  SELECT COUNT(*) AS user_count FROM grocery.users"
      );

      txn.exec("CREATE UNIQUE INDEX ON grocery.user_stats(user_count)");
      txn.exec("REFRESH MATERIALIZED VIEW grocery.user_stats");
      txn.exec("ANALYZE grocery.user_stats");

      txn.exec("CREATE TYPE grocery.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled')");
      txn.exec("COMMENT ON TYPE grocery.order_status IS 'status of a customer order'");
      txn.exec("ALTER TABLE grocery.orders ADD COLUMN status grocery.order_status DEFAULT 'pending'");
      txn.exec("CREATE TYPE grocery.user_role AS ENUM ('admin', 'user', 'guest')");
      txn.exec("CREATE TYPE grocery.unused_enum AS ENUM ('x', 'y')");

      txn.exec(
        "CREATE TABLE grocery.role_settings ("
        "  role         grocery.user_role PRIMARY KEY,"
        "  max_sessions INT DEFAULT 5"
        ")"
      );
      txn.exec("INSERT INTO grocery.role_settings(role) VALUES ('admin')");

      txn.exec("CREATE TYPE grocery.address AS (street TEXT, city TEXT, zip_code TEXT)");
      txn.exec("COMMENT ON TYPE grocery.address IS 'postal address components'");
      txn.exec("ALTER TABLE grocery.users ADD COLUMN home_address grocery.address");

      txn.exec("CREATE DOMAIN grocery.positive_amount AS NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (VALUE >= 0)");
      txn.exec("COMMENT ON DOMAIN grocery.positive_amount IS 'a non-negative monetary amount'");
      txn.exec("ALTER TABLE grocery.bare_notes ADD COLUMN price grocery.positive_amount");

      txn.exec("CREATE EXTENSION IF NOT EXISTS postgres_fdw");

      // Deliberately NOT in public, and in a schema that is on nobody's
      // search_path: "extensions" is the usual convention for keeping an
      // operator's tooling out of the application's schemas, and an
      // unqualified pgstattuple() cannot resolve here. Every tableBloat test
      // below is therefore also a regression test for schema qualification --
      // they all failed with "pgstattuple is not installed" when the tool
      // relied on name resolution.
      txn.exec("CREATE SCHEMA extensions");
      txn.exec("COMMENT ON SCHEMA extensions IS 'operator tooling, off the default search_path'");
      txn.exec("CREATE EXTENSION IF NOT EXISTS pgstattuple SCHEMA extensions");
      // Same reasoning for pg_buffercache: out of public, so the buffer cache
      // tools are exercised against a schema-qualified lookup rather than
      // whatever happens to be on the role's search_path.
      txn.exec("CREATE EXTENSION IF NOT EXISTS pg_buffercache SCHEMA extensions");
      // hypopg, also out of public. Optional: it is third-party rather than
      // contrib and is not packaged everywhere, so the tests that need it skip
      // rather than fail when it is absent.
      //
      // The savepoint is what makes that true, and catching the exception is
      // not enough on its own: a failed statement poisons the whole
      // transaction, so every command after it errors with 25P02 until a
      // rollback -- which took the rest of this fixture down with it. Same
      // reason explain_query and evaluate_index wrap their EXPLAINs.
      try {
        pqxx::subtransaction sub{txn};
        sub.exec("CREATE EXTENSION IF NOT EXISTS hypopg SCHEMA extensions");
        sub.commit();
      } catch (const std::exception&) {
      }
      txn.exec(
        "CREATE SERVER grocery_remote FOREIGN DATA WRAPPER postgres_fdw"
        " OPTIONS (host 'localhost', dbname 'probe', port '5432')"
      );
      txn.exec(
        "CREATE FOREIGN TABLE grocery.remote_orders (id INT, amount NUMERIC)"
        " SERVER grocery_remote OPTIONS (schema_name 'public', table_name 'orders')"
      );
      txn.exec("COMMENT ON FOREIGN TABLE grocery.remote_orders IS 'orders mirrored from a remote system'");

      txn.exec("CREATE COLLATION grocery.case_sensitive_c FROM \"C\"");
      txn.exec("COMMENT ON COLLATION grocery.case_sensitive_c IS 'byte-order comparison, copied from C'");

      txn.exec(
        "CREATE FUNCTION grocery.probe_event_trigger_fn() RETURNS event_trigger"
        " LANGUAGE plpgsql AS $$ BEGIN END; $$"
      );
      txn.exec(
        "CREATE EVENT TRIGGER grocery_ddl_audit ON ddl_command_end"
        " EXECUTE FUNCTION grocery.probe_event_trigger_fn()"
      );
      txn.exec("COMMENT ON EVENT TRIGGER grocery_ddl_audit IS 'audits DDL changes'");

      txn.exec("CREATE PUBLICATION grocery_users_pub FOR TABLE grocery.users");

      txn.exec("CREATE TYPE grocery.price_range AS RANGE (subtype = numeric)");

      txn.exec("CREATE STATISTICS grocery.orders_stats (dependencies) ON user_id, amount FROM grocery.orders");
      txn.exec("COMMENT ON STATISTICS grocery.orders_stats IS 'user_id/amount correlation'");

      txn.exec("CREATE RULE protect_bare_notes AS ON DELETE TO grocery.bare_notes DO INSTEAD NOTHING");

      txn.exec("CREATE FUNCTION grocery.add_ints(int, int) RETURNS int LANGUAGE sql AS 'SELECT $1 + $2'");
      txn.exec(
        "CREATE OPERATOR grocery.#+# (FUNCTION = grocery.add_ints, LEFTARG = int, RIGHTARG = int)"
      );

      txn.exec(
        "CREATE FUNCTION grocery.address_to_text(grocery.address) RETURNS text"
        " LANGUAGE sql AS 'SELECT ($1).street'"
      );
      txn.exec(
        "CREATE CAST (grocery.address AS text)"
        " WITH FUNCTION grocery.address_to_text(grocery.address) AS ASSIGNMENT"
      );

      txn.exec("CREATE TEXT SEARCH CONFIGURATION grocery.simple_english (COPY = pg_catalog.english)");

      // A table of its own for duplicateIndexes, so the index counts the other
      // tests assert on stay untouched. It carries one of every case the
      // detector has to get right: an exact duplicate, a prefix-redundant
      // index, and three near-misses that must NOT be reported -- a reversed
      // sort order, a partial index, and a unique index that happens to be a
      // prefix of a wider one.
      txn.exec(
        "CREATE TABLE grocery.index_zoo ("
        "  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
        "  a  INT,"
        "  b  INT,"
        "  c  TEXT,"
        "  d  TEXT"
        ")"
      );
      txn.exec("CREATE INDEX zoo_a_b     ON grocery.index_zoo (a, b)");
      txn.exec("CREATE INDEX zoo_a       ON grocery.index_zoo (a)");
      txn.exec("CREATE INDEX zoo_a_again ON grocery.index_zoo (a)");
      txn.exec("CREATE INDEX zoo_a_desc  ON grocery.index_zoo (a DESC)");
      txn.exec("CREATE INDEX zoo_a_part  ON grocery.index_zoo (a) WHERE b > 0");
      txn.exec("CREATE UNIQUE INDEX zoo_c_uq ON grocery.index_zoo (c)");
      txn.exec("CREATE UNIQUE INDEX zoo_c_d_uq ON grocery.index_zoo (c, d)");
      txn.exec("CREATE INDEX zoo_lower_c  ON grocery.index_zoo (lower(c))");
      txn.exec("CREATE INDEX zoo_lower_c2 ON grocery.index_zoo (lower(c))");

      // A per-table freeze override, so wraparoundStatus can be checked to
      // report the storage parameter rather than the server-wide default.
      txn.exec("ALTER TABLE grocery.index_zoo SET (autovacuum_freeze_max_age = 100000000)");

      // One index per access method indexBloat has to tell apart. They live on
      // index_zoo rather than on users so the index counts asserted elsewhere
      // stay put. The GIN index carries explicit fastupdate and
      // gin_pending_list_limit settings, since those are what make its
      // pending_pages readable and the tool reports them alongside.
      txn.exec("CREATE TABLE grocery.index_ams (id INT, doc TEXT, tags TEXT[], box_col BOX)");
      txn.exec("INSERT INTO grocery.index_ams(id, doc, tags, box_col)"
               " SELECT g, 'doc ' || g, ARRAY['t' || (g % 7), 'all'],"
               "        BOX(POINT(g, g), POINT(g + 1, g + 1))"
               " FROM generate_series(1, 500) AS g");
      txn.exec("CREATE INDEX ams_btree ON grocery.index_ams (id)");
      txn.exec("CREATE INDEX ams_gin   ON grocery.index_ams USING gin (tags)"
               " WITH (fastupdate = on, gin_pending_list_limit = 128)");
      txn.exec("CREATE INDEX ams_hash  ON grocery.index_ams USING hash (id)");
      // gist has no pgstattuple function at all; the tool must say so by name
      // rather than fail with a bare error from the wrong function.
      txn.exec("CREATE INDEX ams_gist  ON grocery.index_ams USING gist (box_col)");
      txn.exec("ANALYZE grocery.index_ams");

      // A partitioned index is a catalog entry with no storage of its own.
      txn.exec("CREATE TABLE grocery.index_parted (id INT, val TEXT) PARTITION BY RANGE (id)");
      txn.exec("CREATE TABLE grocery.index_parted_p1 PARTITION OF grocery.index_parted"
               " FOR VALUES FROM (0) TO (100)");
      txn.exec("CREATE INDEX parted_id_idx ON grocery.index_parted (id)");

      txn.exec("GRANT USAGE ON SCHEMA grocery TO PUBLIC");
      txn.exec("GRANT SELECT ON grocery.users TO PUBLIC");
      txn.exec("GRANT EXECUTE ON FUNCTION grocery.get_user_count() TO PUBLIC");
      txn.exec("DROP ROLE IF EXISTS tomato");
      txn.exec("DROP ROLE IF EXISTS carrot");
      txn.exec("CREATE ROLE tomato");
      txn.exec("CREATE ROLE carrot LOGIN");
      txn.exec("GRANT SELECT ON grocery.users TO tomato");
      txn.exec("GRANT tomato TO carrot");

      txn.commit();

    } catch (const std::exception& e) {
      ADD_FAILURE() << "SetUpTestSuite failed: " << e.what();
    }
  }

  static void TearDownTestSuite() {
    try {
      if (srv) {
        delete srv;
        srv = nullptr;
      }
      if (admin_conn) {
        pqxx::nontransaction ntxn(*admin_conn);
        // WITH (FORCE) terminates any remaining backends on the database. That
        // matters when the server under test is reached through a
        // transaction-mode pooler: the pooler keeps idle server connections
        // open to this database, and a plain DROP would fail with "database is
        // being accessed by other users". Matches the pgss fixture below.
        ntxn.exec("DROP DATABASE IF EXISTS \"" + test_dbname + "\" WITH (FORCE)");
        ntxn.exec("DROP ROLE IF EXISTS tomato");
        ntxn.exec("DROP ROLE IF EXISTS carrot");
        ntxn.commit();

        delete admin_conn;
        admin_conn = nullptr;
      }
    } catch (const std::exception& e) {
      ADD_FAILURE() << "TearDownTestSuite failed: " << e.what();
    }
  }

  void SetUp() override {
    if (!admin_conn || !srv) {
      GTEST_SKIP_("Database setup failed or DATABASE_URL not set");
    }
  }
};

pqxx::connection* PostgresMCPServerTest::admin_conn = nullptr;
PostgresMCPServer* PostgresMCPServerTest::srv = nullptr;
std::string PostgresMCPServerTest::test_dbname;
std::string PostgresMCPServerTest::base_url;
std::string PostgresMCPServerTest::test_url;

TEST_F(PostgresMCPServerTest, SchemasReturnsObject) {
  json result = srv->call_schemas();
  EXPECT_TRUE(result.is_object());
  EXPECT_GT(result.size(), 0);
}

TEST_F(PostgresMCPServerTest, SchemasContainsGrocery) {
  json result = srv->call_schemas();
  EXPECT_TRUE(result.contains("grocery"));
  EXPECT_TRUE(result["grocery"].contains("tables"));
  EXPECT_TRUE(result["grocery"]["tables"].is_array());

  auto tables = result["grocery"]["tables"];
  std::vector<std::string> table_names(tables.begin(), tables.end());
  EXPECT_NE(std::find(table_names.begin(), table_names.end(), "users"), table_names.end());
  EXPECT_NE(std::find(table_names.begin(), table_names.end(), "orders"), table_names.end());
}

TEST_F(PostgresMCPServerTest, SchemasExcludesSystemSchemas) {
  json result = srv->call_schemas();
  EXPECT_FALSE(result.contains("pg_catalog"));
  EXPECT_FALSE(result.contains("information_schema"));
}

TEST_F(PostgresMCPServerTest, SchemasHasRolesField) {
  json result = srv->call_schemas();
  ASSERT_TRUE(result.contains("grocery"));
  EXPECT_TRUE(result["grocery"].contains("roles"));
  EXPECT_TRUE(result["grocery"]["roles"].is_object());
}

TEST_F(PostgresMCPServerTest, SchemasRolesShowsGrantedPrivilege) {
  json result = srv->call_schemas();
  ASSERT_TRUE(result.contains("grocery"));
  ASSERT_TRUE(result["grocery"]["roles"].contains("PUBLIC"));
  auto& privs = result["grocery"]["roles"]["PUBLIC"];
  EXPECT_TRUE(std::any_of(privs.begin(), privs.end(),
    [](const json& p) { return p.get<std::string>() == "USAGE"; }));
}

TEST_F(PostgresMCPServerTest, TablesReturnsKnownTables) {
  json result = srv->call_tables("grocery");
  EXPECT_TRUE(result.contains("users"));
  EXPECT_TRUE(result.contains("orders"));
}

TEST_F(PostgresMCPServerTest, TablesHasExpectedFields) {
  json result = srv->call_tables("grocery");
  EXPECT_TRUE(result["users"].contains("kind"));
  EXPECT_TRUE(result["users"].contains("description"));
  EXPECT_TRUE(result["users"].contains("reloptions"));
  EXPECT_TRUE(result["users"].contains("columns"));
  EXPECT_TRUE(result["users"].contains("index_count"));
  EXPECT_TRUE(result["users"].contains("constraint_count"));
  EXPECT_GT(result["users"]["index_count"].get<int>(), 0);
  EXPECT_GT(result["users"]["constraint_count"].get<int>(), 0);
}

// 4.0.0: the structure tools carry no readings at all. Asserted by absence
// rather than trusted, because a field left behind here is exactly what would
// keep the tool wrongly classified per_server in tool_scopes().
TEST_F(PostgresMCPServerTest, TablesCarriesNoStatistics) {
  json result = srv->call_tables("grocery");
  for (const char* f : {"rows", "size", "size_estimate", "seq_scan", "idx_scan",
                        "n_live_tup", "n_dead_tup", "n_mod_since_analyze",
                        "n_ins_since_vacuum", "last_vacuum", "last_analyze",
                        "n_tup_newpage_upd", "last_seq_scan"})
    EXPECT_FALSE(result["users"].contains(f)) << f << " is still on listTables";
}

TEST_F(PostgresMCPServerTest, SearchCarriesNoStatistics) {
  json result = srv->call_search("users");
  ASSERT_TRUE(result.contains("grocery.users")) << result.dump(2);
  for (const char* f : {"rows", "size", "size_estimate", "seq_scan", "idx_scan",
                        "n_live_tup", "n_dead_tup", "last_vacuum", "last_analyze"})
    EXPECT_FALSE(result["grocery.users"].contains(f)) << f << " is still on searchTables";
  // What it does keep: structure, and the privileges that make it findable by
  // grantee name.
  EXPECT_TRUE(result["grocery.users"].contains("columns"));
  EXPECT_TRUE(result["grocery.users"].contains("roles"));
}

TEST_F(PostgresMCPServerTest, TablesColumnsIncludePerColumnIndexCount) {
  json result = srv->call_tables("grocery");
  ASSERT_TRUE(result["users"]["columns"].contains("id"));
  auto& id_col = result["users"]["columns"]["id"];
  EXPECT_TRUE(id_col.contains("description"));
  EXPECT_TRUE(id_col.contains("index_count"));
  EXPECT_GT(id_col["index_count"].get<int>(), 0); // primary key is indexed

  ASSERT_TRUE(result["orders"]["columns"].contains("amount"));
  EXPECT_EQ(result["orders"]["columns"]["amount"]["index_count"].get<int>(), 0); // not indexed
}

TEST_F(PostgresMCPServerTest, TableWithNoIndexesIsIncluded) {
  json result = srv->call_tables("grocery");
  ASSERT_TRUE(result.contains("bare_notes"));
  EXPECT_EQ(result["bare_notes"]["index_count"].get<int>(), 0);
}

TEST_F(PostgresMCPServerTest, TablesUnknownSchemaReturnsEmpty) {
  json result = srv->call_tables("does_not_exist_schema");
  EXPECT_TRUE(result.empty() || result.is_null());
}

TEST_F(PostgresMCPServerTest, SearchByTableName) {
  json result = srv->call_search("users");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".users") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchByTableDescription) {
  json result = srv->call_search("user account");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".users") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}


TEST_F(PostgresMCPServerTest, SearchResultKeyIncludesSchema) {
  json result = srv->call_search("users");
  for (auto& [key, value] : result.items()) {
    size_t dot_count = 0;
    for (char c : key) {
      if (c == '.') dot_count++;
    }
    EXPECT_EQ(dot_count, 1) << "Key should have exactly one dot: " << key;
  }
}

TEST_F(PostgresMCPServerTest, SearchByColumnName) {
  json result = srv->call_search("user_id");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".orders") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchByColumnDescription) {
  json result = srv->call_search("unique email address");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".users") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchSnakeCaseName) {
  json result = srv->call_search("user account log");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".user_account_log") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, TableReturnsExpectedTopLevelKeys) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result.contains("table"));
  EXPECT_TRUE(result.contains("kind"));
  EXPECT_TRUE(result.contains("description"));
  EXPECT_TRUE(result.contains("reloptions"));
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("indexes"));
  EXPECT_TRUE(result.contains("constraints"));
  EXPECT_TRUE(result.contains("foreign_keys"));
  EXPECT_TRUE(result.contains("referenced_by"));
  EXPECT_TRUE(result.contains("primary_key"));
  EXPECT_TRUE(result.contains("triggers"));
  EXPECT_TRUE(result.contains("rules"));
  EXPECT_TRUE(result.contains("row_level_security"));
  EXPECT_TRUE(result.contains("policies"));
  EXPECT_TRUE(result.contains("roles"));
}

TEST_F(PostgresMCPServerTest, TableDetailsCarriesNoStatistics) {
  json result = srv->call_table("grocery", "users");
  for (const char* f : {"rows", "size", "indexes_size", "size_estimate",
                        "seq_scan", "idx_scan", "n_live_tup", "n_dead_tup",
                        "n_mod_since_analyze", "n_ins_since_vacuum",
                        "last_vacuum", "last_analyze", "n_tup_newpage_upd",
                        "last_seq_scan"})
    EXPECT_FALSE(result.contains(f)) << f << " is still on tableDetails";

  // Reason 3 of the split: most_common_vals is literal column values, and this
  // is the tool an agent calls most often to inspect structure. It must return
  // none of them.
  ASSERT_TRUE(result["columns"].contains("email"));
  for (const char* f : {"null_frac", "avg_width", "n_distinct",
                        "physical_order_correlation", "most_common_vals",
                        "most_common_freqs"})
    EXPECT_FALSE(result["columns"]["email"].contains(f))
        << f << " is still on a tableDetails column";

  // Index definitions are structure and stay; their scan counters and sizes
  // do not.
  ASSERT_FALSE(result["indexes"].empty());
  for (auto& [name, idx] : result["indexes"].items()) {
    EXPECT_TRUE(idx.contains("definition")) << name;
    EXPECT_FALSE(idx.contains("size")) << name;
    EXPECT_FALSE(idx.contains("index_uses")) << name;
    EXPECT_FALSE(idx.contains("last_use")) << name;
  }
}

TEST_F(PostgresMCPServerTest, TableDetailsSingleColumnPrimaryKey) {
  json result = srv->call_table("grocery", "users");
  ASSERT_TRUE(result["primary_key"].is_array());
  ASSERT_EQ(result["primary_key"].size(), 1u);
  EXPECT_EQ(result["primary_key"][0].get<std::string>(), "id");
}

TEST_F(PostgresMCPServerTest, TableDetailsMultiColumnPrimaryKey) {
  json result = srv->call_table("grocery", "order_tags");
  ASSERT_TRUE(result["primary_key"].is_array());
  ASSERT_EQ(result["primary_key"].size(), 2u);
  EXPECT_EQ(result["primary_key"][0].get<std::string>(), "order_id");
  EXPECT_EQ(result["primary_key"][1].get<std::string>(), "tag");
}

TEST_F(PostgresMCPServerTest, TableDetailsNoPrimaryKeyReturnsEmptyArray) {
  json result = srv->call_table("grocery", "bare_notes");
  ASSERT_TRUE(result["primary_key"].is_array());
  EXPECT_TRUE(result["primary_key"].empty());
}

TEST_F(PostgresMCPServerTest, TableDetailsUsersHasReferencedBy) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result.contains("referenced_by"));
  EXPECT_TRUE(result["referenced_by"].is_object());
  EXPECT_GE(result["referenced_by"].size(), 1u);
  bool found = false;
  for (auto& [key, val] : result["referenced_by"].items()) {
    if (key.rfind("grocery.orders.", 0) == 0) {
      found = true;
      EXPECT_EQ(val["source_table"].get<std::string>(), "grocery.orders");
      EXPECT_TRUE(val.contains("source_columns"));
      EXPECT_TRUE(val.contains("target_columns"));
      EXPECT_TRUE(val.contains("definition"));
    }
  }
  EXPECT_TRUE(found) << "Expected inbound FK from grocery.orders";
}

TEST_F(PostgresMCPServerTest, TableDetailsBareNotesHasEmptyReferencedBy) {
  json result = srv->call_table("grocery", "bare_notes");
  EXPECT_TRUE(result.contains("referenced_by"));
  EXPECT_TRUE(result["referenced_by"].is_object());
  EXPECT_TRUE(result["referenced_by"].empty());
}

TEST_F(PostgresMCPServerTest, CheckKeyTrueForExistingSingleKey) {
  json result = srv->call_check_key("grocery", "users", json::array({1}));
  EXPECT_EQ(result["exists"].get<bool>(), true);
}

TEST_F(PostgresMCPServerTest, CheckKeyFalseForMissingSingleKey) {
  json result = srv->call_check_key("grocery", "users", json::array({999}));
  EXPECT_EQ(result["exists"].get<bool>(), false);
}

TEST_F(PostgresMCPServerTest, CheckKeyTrueForExistingCompositeKey) {
  json result = srv->call_check_key("grocery", "order_tags", json::array({1, "organic"}));
  EXPECT_EQ(result["exists"].get<bool>(), true);
}

TEST_F(PostgresMCPServerTest, CheckKeyFalseForMissingCompositeKey) {
  json result = srv->call_check_key("grocery", "order_tags", json::array({1, "missing"}));
  EXPECT_EQ(result["exists"].get<bool>(), false);
}

TEST_F(PostgresMCPServerTest, CheckKeyThrowsForNoPrimaryKey) {
  EXPECT_THROW(srv->call_check_key("grocery", "bare_notes", json::array({"x"})), std::exception);
}

TEST_F(PostgresMCPServerTest, CheckKeyThrowsForWrongValueCount) {
  EXPECT_THROW(srv->call_check_key("grocery", "users", json::array({1, 2})), std::exception);
}

TEST_F(PostgresMCPServerTest, CheckKeyThrowsForTypeMismatch) {
  EXPECT_THROW(srv->call_check_key("grocery", "users", json::array({"not-an-int"})), std::exception);
}

TEST_F(PostgresMCPServerTest, CheckKeyTrueForExistingUUIDKey) {
  json result = srv->call_check_key("grocery", "product_refs",
    json::array({"550e8400-e29b-41d4-a716-446655440000"}));
  EXPECT_EQ(result["exists"].get<bool>(), true);
}

TEST_F(PostgresMCPServerTest, CheckKeyFalseForMissingUUIDKey) {
  json result = srv->call_check_key("grocery", "product_refs",
    json::array({"00000000-0000-0000-0000-000000000000"}));
  EXPECT_EQ(result["exists"].get<bool>(), false);
}

TEST_F(PostgresMCPServerTest, CheckKeyThrowsForInvalidUUIDFormat) {
  EXPECT_THROW(srv->call_check_key("grocery", "product_refs",
    json::array({"not-a-uuid"})), std::exception);
}

TEST_F(PostgresMCPServerTest, CheckKeyAcceptsStringForCustomEnumKey) {
  json result = srv->call_check_key("grocery", "role_settings", json::array({"admin"}));
  EXPECT_EQ(result["exists"].get<bool>(), true);
}

TEST_F(PostgresMCPServerTest, CheckKeyThrowsNonStringForCustomEnumKey) {
  EXPECT_THROW(srv->call_check_key("grocery", "role_settings", json::array({42})), std::exception);
}

TEST_F(PostgresMCPServerTest, TableColumnsIncludeDefaultWhenPresent) {
  json result = srv->call_table("grocery", "user_account_log");
  EXPECT_EQ(result["columns"]["logged_at"]["default"].get<std::string>(), "now()");
}

TEST_F(PostgresMCPServerTest, TableColumnsHaveNoDefaultWhenAbsent) {
  json result = srv->call_table("grocery", "users");
  EXPECT_FALSE(result["columns"]["name"].contains("default"));
}

// 4.0.0 split this one test across the three tiers it now spans: the
// definition stayed on tableDetails, the scan count went to tableStats, and
// the size to tableSize.
TEST_F(PostgresMCPServerTest, TableIndexesHaveSizeField) {
  json structure = srv->call_table("grocery", "users");
  EXPECT_FALSE(structure["indexes"].empty());
  for (auto& [idx_name, idx_obj] : structure["indexes"].items())
    EXPECT_TRUE(idx_obj.contains("definition")) << idx_name;

  json stats = srv->call_table_stats("grocery", "users");
  for (auto& [idx_name, idx_obj] : stats["indexes"].items())
    EXPECT_TRUE(idx_obj.contains("index_uses")) << idx_name;

  json sizes = srv->call_table_size("grocery", "users");
  EXPECT_FALSE(sizes["indexes"].empty());
  for (auto& [idx_name, sz] : sizes["indexes"].items())
    EXPECT_TRUE(sz.is_number()) << "Index " << idx_name << " missing size";

  // Every index the structure tool names is measured and counted, so a caller
  // can join the three payloads on the index name without gaps.
  EXPECT_EQ(structure["indexes"].size(), sizes["indexes"].size());
  EXPECT_EQ(structure["indexes"].size(), stats["indexes"].size());
}

TEST_F(PostgresMCPServerTest, TableColumnsHaveTypeInfo) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result["columns"].contains("name"));
  EXPECT_TRUE(result["columns"]["name"].contains("type"));
  EXPECT_TRUE(result["columns"]["name"].contains("format_type"));
  EXPECT_TRUE(result["columns"]["name"].contains("not_null"));
}

TEST_F(PostgresMCPServerTest, TableColumnsIncludeStatsForAnalyzedTable) {
  // The histograms answer the same question they always did; 4.0.0 moved which
  // tool asks it.
  json result = srv->call_table_stats("grocery", "users");
  EXPECT_TRUE(result["columns"]["name"].contains("null_frac"));
  EXPECT_TRUE(result["columns"]["name"].contains("avg_width"));
  EXPECT_TRUE(result["columns"]["name"].contains("n_distinct"));
}

TEST_F(PostgresMCPServerTest, TableWorksWhenUnanalyzed) {
  json result = srv->call_table("grocery", "bare_notes");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result["columns"].contains("note"));
}

TEST_F(PostgresMCPServerTest, TableForeignKeyIsSeparateFromConstraints) {
  json result = srv->call_table("grocery", "orders");
  EXPECT_FALSE(result["foreign_keys"].empty());

  for (auto& [fk_name, fk_obj] : result["foreign_keys"].items()) {
    EXPECT_TRUE(fk_obj.contains("target_table"));
  }

  bool has_fk_in_constraints = false;
  for (auto& [con_name, con_obj] : result["constraints"].items()) {
    std::string def = con_obj["definition"].get<std::string>();
    if (def.find("REFERENCES") != std::string::npos) {
      has_fk_in_constraints = true;
      break;
    }
  }
  EXPECT_FALSE(has_fk_in_constraints) << "Foreign keys should not appear in constraints";
}

TEST_F(PostgresMCPServerTest, SearchTablesByGranteeRoleName) {
  json result = srv->call_search("tomato");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".users") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchTablesNoResultForUnassignedRole) {
  json result = srv->call_search("carrot");
  EXPECT_TRUE(result.empty() || result.is_null());
}

TEST_F(PostgresMCPServerTest, SearchTablesBySchemaName) {
  json result = srv->call_search("grocery");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.substr(0, 8) == "grocery.") { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchTablesResultIncludesRoles) {
  json result = srv->call_search("users");
  for (auto& [key, value] : result.items()) {
    if (key.find(".users") != std::string::npos) {
      ASSERT_TRUE(value.contains("roles"));
      EXPECT_TRUE(value["roles"].is_object());
      ASSERT_TRUE(value["roles"].contains("PUBLIC"));
      auto& privs = value["roles"]["PUBLIC"];
      EXPECT_TRUE(std::any_of(privs.begin(), privs.end(),
        [](const json& p) { return p.get<std::string>() == "SELECT"; }));
      return;
    }
  }
  FAIL() << "users table not found in search results";
}

// --- listFunctions tests ---

TEST_F(PostgresMCPServerTest, FunctionsReturnsBothFunctions) {
  json result = srv->call_functions("grocery");
  EXPECT_TRUE(result.is_object());
  bool has_log_user_action = false;
  bool has_get_user_count = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("log_user_action") != std::string::npos) has_log_user_action = true;
    if (key.find("get_user_count")  != std::string::npos) has_get_user_count = true;
  }
  EXPECT_TRUE(has_log_user_action);
  EXPECT_TRUE(has_get_user_count);
}

TEST_F(PostgresMCPServerTest, FunctionsExcludesAggregatesAndWindow) {
  json result = srv->call_functions("grocery");
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("kind"));
    std::string kind = value["kind"].get<std::string>();
    EXPECT_TRUE(kind == "function" || kind == "procedure");
  }
}

TEST_F(PostgresMCPServerTest, FunctionHasExpectedFields) {
  json result = srv->call_functions("grocery");
  json func_entry;
  for (auto& [key, value] : result.items()) {
    if (key.find("get_user_count") != std::string::npos) {
      func_entry = value;
      break;
    }
  }
  ASSERT_FALSE(func_entry.is_null());
  EXPECT_TRUE(func_entry.contains("kind"));
  EXPECT_TRUE(func_entry.contains("language"));
  EXPECT_TRUE(func_entry.contains("return_type"));
  EXPECT_TRUE(func_entry.contains("arguments"));
  EXPECT_EQ(func_entry["kind"].get<std::string>(), "function");
  EXPECT_EQ(func_entry["language"].get<std::string>(), "sql");
}

// --- functionDetails tests ---

TEST_F(PostgresMCPServerTest, FunctionDetailsIncludesSource) {
  json result = srv->call_function_detail("grocery", "get_user_count");
  EXPECT_TRUE(result.is_object());
  EXPECT_FALSE(result.empty());
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("source"));
    EXPECT_TRUE(value.contains("definition"));
    EXPECT_FALSE(value["source"].get<std::string>().empty());
  }
}

TEST_F(PostgresMCPServerTest, FunctionDetailsShowsTriggerUsage) {
  json result = srv->call_function_detail("grocery", "log_user_action");
  EXPECT_TRUE(result.is_object());
  EXPECT_FALSE(result.empty());
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("used_in_triggers"));
    EXPECT_FALSE(value["used_in_triggers"].is_null());
    EXPECT_TRUE(value["used_in_triggers"].is_array());
    EXPECT_GE(value["used_in_triggers"].size(), 1u);
  }
}

// --- searchFunctions tests ---

TEST_F(PostgresMCPServerTest, SearchFunctionsByName) {
  json result = srv->call_search_functions("user count");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("get_user_count") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchFunctionsBySource) {
  json result = srv->call_search_functions("COUNT");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("get_user_count") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchFunctionsByTriggerName) {
  json result = srv->call_search_functions("audit");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("log_user_action") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

// --- tableDetails trigger enhancement test ---

TEST_F(PostgresMCPServerTest, TableDetailsIncludesTriggers) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result.contains("triggers"));
  EXPECT_TRUE(result["triggers"].is_object());
  EXPECT_TRUE(result["triggers"].contains("trg_user_audit"));
  auto& trig = result["triggers"]["trg_user_audit"];
  EXPECT_TRUE(trig.contains("timing"));
  EXPECT_TRUE(trig.contains("events"));
  EXPECT_TRUE(trig.contains("function"));
  EXPECT_TRUE(trig.contains("when"));
  EXPECT_EQ(trig["timing"].get<std::string>(), "AFTER");
  EXPECT_NE(trig["events"].get<std::string>().find("INSERT"), std::string::npos);
  EXPECT_TRUE(trig["when"].is_null());
  EXPECT_TRUE(result["triggers"].contains("trg_user_audit_conditional"));
  auto& cond = result["triggers"]["trg_user_audit_conditional"];
  EXPECT_TRUE(cond["when"].is_string());
  EXPECT_FALSE(cond["when"].get<std::string>().empty());
}

TEST_F(PostgresMCPServerTest, TableDetailsIncludesRowLevelSecurityAndPolicies) {
  json result = srv->call_table("grocery", "orders");
  ASSERT_TRUE(result.contains("row_level_security"));
  EXPECT_TRUE(result["row_level_security"]["enabled"].get<bool>());
  EXPECT_FALSE(result["row_level_security"]["forced"].get<bool>());

  ASSERT_TRUE(result.contains("policies"));
  ASSERT_TRUE(result["policies"].contains("orders_owner_only"));
  auto& pol = result["policies"]["orders_owner_only"];
  EXPECT_EQ(pol["command"].get<std::string>(), "SELECT");
  EXPECT_TRUE(pol["permissive"].get<bool>());
  ASSERT_TRUE(pol["roles"].is_array());
  EXPECT_EQ(pol["roles"][0].get<std::string>(), "PUBLIC");
  EXPECT_NE(pol["using"].get<std::string>().find("user_id"), std::string::npos);
  EXPECT_TRUE(pol["with_check"].is_null());
}

TEST_F(PostgresMCPServerTest, TableDetailsNoRLSReturnsDisabledAndEmptyPolicies) {
  json result = srv->call_table("grocery", "users");
  ASSERT_TRUE(result.contains("row_level_security"));
  EXPECT_FALSE(result["row_level_security"]["enabled"].get<bool>());
  EXPECT_FALSE(result["row_level_security"]["forced"].get<bool>());
  EXPECT_TRUE(result["policies"].empty());
}

// --- view / materialized view tests ---

TEST_F(PostgresMCPServerTest, SchemasContainsView) {
  json result = srv->call_schemas();
  auto tables = result["grocery"]["tables"];
  std::vector<std::string> names(tables.begin(), tables.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "active_users"), names.end());
}

TEST_F(PostgresMCPServerTest, SchemasContainsMaterializedView) {
  json result = srv->call_schemas();
  auto tables = result["grocery"]["tables"];
  std::vector<std::string> names(tables.begin(), tables.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "user_stats"), names.end());
}

TEST_F(PostgresMCPServerTest, TablesContainsViewAndMV) {
  json result = srv->call_tables("grocery");
  EXPECT_TRUE(result.contains("active_users"));
  EXPECT_TRUE(result.contains("user_stats"));
}

TEST_F(PostgresMCPServerTest, TablesViewHasKindField) {
  json result = srv->call_tables("grocery");
  ASSERT_TRUE(result.contains("active_users"));
  EXPECT_EQ(result["active_users"]["kind"].get<std::string>(), "view");
}

TEST_F(PostgresMCPServerTest, TablesMVHasKindField) {
  json result = srv->call_tables("grocery");
  ASSERT_TRUE(result.contains("user_stats"));
  EXPECT_EQ(result["user_stats"]["kind"].get<std::string>(), "materialized view");
}

TEST_F(PostgresMCPServerTest, TableDetailsWorksForView) {
  json result = srv->call_table("grocery", "active_users");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("definition"));
  EXPECT_FALSE(result["definition"].is_null());
  EXPECT_FALSE(result["definition"].get<std::string>().empty());
}

TEST_F(PostgresMCPServerTest, TableDetailsWorksForMV) {
  json result = srv->call_table("grocery", "user_stats");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("definition"));
  EXPECT_FALSE(result["definition"].is_null());
  EXPECT_FALSE(result["definition"].get<std::string>().empty());
  EXPECT_FALSE(result["indexes"].empty());
}

// --- listEnums tests ---

TEST_F(PostgresMCPServerTest, EnumsReturnsKnownEnum) {
  json result = srv->call_enums("grocery");
  EXPECT_TRUE(result.is_object());
  EXPECT_TRUE(result.contains("order_status"));
  EXPECT_TRUE(result.contains("user_role"));
}

TEST_F(PostgresMCPServerTest, EnumsHasExpectedFields) {
  json result = srv->call_enums("grocery");
  ASSERT_TRUE(result.contains("order_status"));
  EXPECT_TRUE(result["order_status"].contains("description"));
  EXPECT_TRUE(result["order_status"].contains("values"));
  EXPECT_TRUE(result["order_status"]["values"].is_array());
  EXPECT_EQ(result["order_status"]["description"].get<std::string>(), "status of a customer order");
}

TEST_F(PostgresMCPServerTest, EnumsValuesAreOrdered) {
  json result = srv->call_enums("grocery");
  auto values = result["order_status"]["values"];
  ASSERT_GE(values.size(), 5u);
  EXPECT_EQ(values[0].get<std::string>(), "pending");
  EXPECT_EQ(values[2].get<std::string>(), "shipped");
  EXPECT_EQ(values[4].get<std::string>(), "cancelled");
}

TEST_F(PostgresMCPServerTest, EnumsUnknownSchemaReturnsEmpty) {
  json result = srv->call_enums("does_not_exist_schema");
  EXPECT_TRUE(result.empty() || result.is_null());
}

// --- enumDetails tests ---

TEST_F(PostgresMCPServerTest, EnumDetailsHasValuesAndDescription) {
  json result = srv->call_enum_detail("grocery", "order_status");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("description"));
  EXPECT_TRUE(result.contains("values"));
  EXPECT_TRUE(result["values"].is_array());
  EXPECT_EQ(result["description"].get<std::string>(), "status of a customer order");
}

TEST_F(PostgresMCPServerTest, EnumDetailsHasUsedByColumns) {
  json result = srv->call_enum_detail("grocery", "order_status");
  EXPECT_TRUE(result.contains("used_by_columns"));
  EXPECT_TRUE(result["used_by_columns"].is_array());
  EXPECT_GE(result["used_by_columns"].size(), 1u);
  bool found_orders = false;
  for (auto& col : result["used_by_columns"]) {
    if (col["table"].get<std::string>().find("orders") != std::string::npos) {
      found_orders = true;
      break;
    }
  }
  EXPECT_TRUE(found_orders);
}

TEST_F(PostgresMCPServerTest, EnumDetailsUnusedEnumHasEmptyUsedByColumns) {
  json result = srv->call_enum_detail("grocery", "unused_enum");
  EXPECT_TRUE(result.contains("used_by_columns"));
  EXPECT_TRUE(result["used_by_columns"].is_array());
  EXPECT_EQ(result["used_by_columns"].size(), 0u);
}

TEST_F(PostgresMCPServerTest, EnumDetailsNotFound) {
  json result = srv->call_enum_detail("grocery", "nonexistent_enum");
  EXPECT_TRUE(result.empty() || result.is_null());
}

// --- searchEnums tests ---

TEST_F(PostgresMCPServerTest, SearchEnumsByName) {
  json result = srv->call_search_enums("order status");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("order_status") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchEnumsByValue) {
  json result = srv->call_search_enums("shipped");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("order_status") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchEnumsByDescription) {
  json result = srv->call_search_enums("customer order");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find("order_status") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchEnumsResultKeyIncludesSchema) {
  json result = srv->call_search_enums("order status");
  for (auto& [key, value] : result.items()) {
    size_t dot_count = 0;
    for (char c : key) { if (c == '.') dot_count++; }
    EXPECT_EQ(dot_count, 1) << "Key should have exactly one dot: " << key;
  }
}

// --- roles in tableDetails and functionDetails ---

TEST_F(PostgresMCPServerTest, TableDetailsHasRolesField) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result.contains("roles"));
  EXPECT_TRUE(result["roles"].is_object());
}

TEST_F(PostgresMCPServerTest, TableDetailsRolesShowsGrantedPrivilege) {
  json result = srv->call_table("grocery", "users");
  ASSERT_TRUE(result.contains("roles"));
  ASSERT_TRUE(result["roles"].contains("PUBLIC"));
  auto& privs = result["roles"]["PUBLIC"];
  EXPECT_TRUE(std::any_of(privs.begin(), privs.end(),
    [](const json& p) { return p.get<std::string>() == "SELECT"; }));
}

TEST_F(PostgresMCPServerTest, TableDetailsNoGrantsReturnsEmptyRoles) {
  json result = srv->call_table("grocery", "bare_notes");
  EXPECT_TRUE(result.contains("roles"));
  EXPECT_TRUE(result["roles"].is_object());
  EXPECT_TRUE(result["roles"].empty());
}

TEST_F(PostgresMCPServerTest, FunctionDetailsHasRolesField) {
  json result = srv->call_function_detail("grocery", "get_user_count");
  EXPECT_FALSE(result.empty());
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("roles"));
    EXPECT_TRUE(value["roles"].is_object());
  }
}

TEST_F(PostgresMCPServerTest, FunctionDetailsRolesShowsGrantedPrivilege) {
  json result = srv->call_function_detail("grocery", "get_user_count");
  EXPECT_FALSE(result.empty());
  for (auto& [key, value] : result.items()) {
    ASSERT_TRUE(value["roles"].contains("PUBLIC"));
    auto& privs = value["roles"]["PUBLIC"];
    EXPECT_TRUE(std::any_of(privs.begin(), privs.end(),
      [](const json& p) { return p.get<std::string>() == "EXECUTE"; }));
  }
}

TEST_F(PostgresMCPServerTest, FunctionDetailsNoGrantsReturnsEmptyRoles) {
  json result = srv->call_function_detail("grocery", "log_user_action");
  EXPECT_FALSE(result.empty());
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("roles"));
    EXPECT_TRUE(value["roles"].is_object());
    EXPECT_TRUE(value["roles"].empty());
  }
}

// --- searchTables via enum types ---

TEST_F(PostgresMCPServerTest, SearchTablesByEnumValue) {
  json result = srv->call_search("shipped");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".orders") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchTablesByEnumName) {
  json result = srv->call_search("order status");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".orders") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, SearchTablesByEnumDescription) {
  json result = srv->call_search("customer order");
  bool found = false;
  for (auto& [key, value] : result.items()) {
    if (key.find(".orders") != std::string::npos) { found = true; break; }
  }
  EXPECT_TRUE(found);
}

// --- serverSettings ---

TEST_F(PostgresMCPServerTest, ServerSettingsReturnsCategoriesWithSettings) {
  json result = srv->call_server_settings();
  EXPECT_TRUE(result.is_object());
  EXPECT_GT(result.size(), 0u);
  for (auto& [cat, settings] : result.items()) {
    EXPECT_TRUE(settings.is_object());
    for (auto& [name, s] : settings.items()) {
      EXPECT_TRUE(s.contains("setting"));
      EXPECT_TRUE(s.contains("short_desc"));
      EXPECT_TRUE(s.contains("context"));
      EXPECT_TRUE(s.contains("vartype"));
      EXPECT_TRUE(s.contains("source"));
      EXPECT_TRUE(s.contains("pending_restart"));
    }
    break; // checking one category is enough
  }
}

// --- connection setup (application_name, read-only session) ---

// The read-only guarantee is per transaction, not per session. It used to be a
// session GUC set once at startup, which a transaction-pooling PgBouncer
// silently discarded (server_reset_query=DISCARD ALL) -- leaving every call
// after the first running unguarded. These tests assert the transaction-scoped
// mechanism that replaced it.
TEST_F(PostgresMCPServerTest, QueriesRunInAReadOnlyTransaction) {
  json result = srv->call_server_settings();
  bool found = false;
  for (auto& [category, settings] : result.items()) {
    if (settings.contains("transaction_read_only")) {
      EXPECT_EQ(settings["transaction_read_only"]["setting"].get<std::string>(), "on");
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

// The regression test for the pooler defect: a write must actually fail, not
// merely be discouraged by a setting we hope survived.
TEST_F(PostgresMCPServerTest, ReadOnlyTransactionRejectsWrites) {
  pglicht::ConnConfig cfg;
  cfg.name = "test";
  cfg.conninfo = test_url;
  Session sess{cfg};
  try {
    sess.txn().exec("CREATE TABLE grocery.should_not_exist(i int)");
    FAIL() << "a write succeeded inside a read-only transaction";
  } catch (const pqxx::sql_error& e) {
    EXPECT_EQ(e.sqlstate(), "25006");   // read_only_sql_transaction
  }
}

// Reproduces the pooler defect directly. A transaction-mode PgBouncer runs
// server_reset_query = DISCARD ALL when it returns a server connection to the
// pool, which wipes session state. The old design set
// default_transaction_read_only once at startup and committed, so behind such a
// pooler it was discarded and every subsequent call ran unguarded. The
// transaction-scoped replacement is immune because it re-establishes the
// guarantee inside each transaction.
TEST_F(PostgresMCPServerTest, ReadOnlyGuaranteeSurvivesPoolerSessionReset) {
  pqxx::connection c(test_url);
  {
    pqxx::nontransaction n(c);
    n.exec("SET default_transaction_read_only = on");   // what the old code did
  }
  {
    pqxx::nontransaction n(c);
    n.exec("DISCARD ALL");                              // what the pooler does
  }
  {
    pqxx::nontransaction n(c);
    pqxx::result r = n.exec("SHOW default_transaction_read_only");
    // The old guard is gone -- this is the bug, reproduced.
    EXPECT_EQ(r[0][0].as<std::string>(), "off");
  }

  // A Session is read-only regardless of any prior session state.
  pglicht::ConnConfig cfg;
  cfg.name = "test";
  cfg.conninfo = test_url;
  Session sess{cfg};
  pqxx::result r = sess.txn().exec("SHOW transaction_read_only");
  EXPECT_EQ(r[0][0].as<std::string>(), "on");
}

TEST_F(PostgresMCPServerTest, ConnectionSetsApplicationNameWithVersion) {
  // Connections are opened per call and closed after, so the server's backend
  // cannot be observed from another session. Read the GUC from inside the
  // server's own transaction instead.
  json result = srv->call_server_settings();
  bool found = false;
  for (auto& [category, settings] : result.items()) {
    if (settings.contains("application_name")) {
      std::string app_name = settings["application_name"]["setting"].get<std::string>();
      EXPECT_EQ(app_name.rfind("pg-licht-cpp/", 0), 0u);
      EXPECT_GT(app_name.size(), std::string("pg-licht-cpp/").size());
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}

// --- listTypes / typeDetails ---

TEST_F(PostgresMCPServerTest, ListTypesReturnsCompositeType) {
  json result = srv->call_types("grocery");
  ASSERT_TRUE(result.contains("address"));
  auto& addr = result["address"];
  EXPECT_EQ(addr["kind"].get<std::string>(), "composite");
  EXPECT_EQ(addr["description"].get<std::string>(), "postal address components");
  ASSERT_TRUE(addr["attributes"].is_array());
  EXPECT_EQ(addr["attributes"].size(), 3u);
  EXPECT_EQ(addr["attributes"][0]["name"].get<std::string>(), "street");
}

TEST_F(PostgresMCPServerTest, ListTypesReturnsDomain) {
  json result = srv->call_types("grocery");
  ASSERT_TRUE(result.contains("positive_amount"));
  auto& amt = result["positive_amount"];
  EXPECT_EQ(amt["kind"].get<std::string>(), "domain");
  EXPECT_TRUE(amt["not_null"].get<bool>());
  EXPECT_NE(amt["base_type"].get<std::string>().find("numeric"), std::string::npos);
  ASSERT_TRUE(amt["constraints"].is_array());
  EXPECT_GT(amt["constraints"].size(), 0u);
}

TEST_F(PostgresMCPServerTest, ListTypesExcludesTableRowTypesAndEnums) {
  json result = srv->call_types("grocery");
  EXPECT_FALSE(result.contains("users"));
  EXPECT_FALSE(result.contains("order_status"));
}

TEST_F(PostgresMCPServerTest, TypeDetailsCompositeIncludesUsedByColumns) {
  json result = srv->call_type_detail("grocery", "address");
  ASSERT_TRUE(result.contains("used_by_columns"));
  bool found = false;
  for (auto& col : result["used_by_columns"]) {
    if (col["column"].get<std::string>() == "home_address") { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, TypeDetailsDomainIncludesUsedByColumns) {
  json result = srv->call_type_detail("grocery", "positive_amount");
  ASSERT_TRUE(result.contains("used_by_columns"));
  bool found = false;
  for (auto& col : result["used_by_columns"]) {
    if (col["column"].get<std::string>() == "price") { found = true; break; }
  }
  EXPECT_TRUE(found);
}

TEST_F(PostgresMCPServerTest, TypeDetailsNotFoundReturnsEmpty) {
  json result = srv->call_type_detail("grocery", "does_not_exist");
  EXPECT_TRUE(result.empty());
}

// --- listRoles ---

TEST_F(PostgresMCPServerTest, ListRolesReturnsGroupAndLoginRoles) {
  json result = srv->call_roles();
  ASSERT_TRUE(result.contains("tomato"));
  EXPECT_EQ(result["tomato"]["kind"].get<std::string>(), "group");
  ASSERT_TRUE(result.contains("carrot"));
  EXPECT_EQ(result["carrot"]["kind"].get<std::string>(), "login");
}

TEST_F(PostgresMCPServerTest, ListRolesReturnsMembership) {
  json result = srv->call_roles();
  ASSERT_TRUE(result.contains("carrot"));
  auto& member_of = result["carrot"]["member_of"];
  ASSERT_TRUE(member_of.is_array());
  std::vector<std::string> groups(member_of.begin(), member_of.end());
  EXPECT_NE(std::find(groups.begin(), groups.end(), "tomato"), groups.end());
}

TEST_F(PostgresMCPServerTest, ListRolesHasAttributeFields) {
  json result = srv->call_roles();
  ASSERT_TRUE(result.contains("tomato"));
  auto& r = result["tomato"];
  EXPECT_TRUE(r.contains("superuser"));
  EXPECT_TRUE(r.contains("create_role"));
  EXPECT_TRUE(r.contains("create_db"));
  EXPECT_TRUE(r.contains("replication"));
  EXPECT_TRUE(r.contains("bypass_rls"));
  EXPECT_TRUE(r.contains("connection_limit"));
}

// --- listForeignTables / listForeignServers ---

TEST_F(PostgresMCPServerTest, ListForeignServersReturnsServerWithOptions) {
  json result = srv->call_foreign_servers();
  ASSERT_TRUE(result.contains("grocery_remote"));
  auto& srv_entry = result["grocery_remote"];
  EXPECT_EQ(srv_entry["fdw"].get<std::string>(), "postgres_fdw");
  ASSERT_TRUE(srv_entry["options"].is_array());
  std::vector<std::string> opts(srv_entry["options"].begin(), srv_entry["options"].end());
  EXPECT_NE(std::find(opts.begin(), opts.end(), "host=localhost"), opts.end());
}

TEST_F(PostgresMCPServerTest, ListForeignServersNeverExposesCredentials) {
  json result = srv->call_foreign_servers();
  std::string dumped = result.dump();
  EXPECT_EQ(dumped.find("password"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, ListForeignTablesReturnsTableWithServerAndColumns) {
  json result = srv->call_foreign_tables("grocery");
  ASSERT_TRUE(result.contains("remote_orders"));
  auto& ft = result["remote_orders"];
  EXPECT_EQ(ft["server"].get<std::string>(), "grocery_remote");
  EXPECT_EQ(ft["fdw"].get<std::string>(), "postgres_fdw");
  EXPECT_EQ(ft["description"].get<std::string>(), "orders mirrored from a remote system");
  ASSERT_TRUE(ft["columns"].contains("id"));
  ASSERT_TRUE(ft["columns"].contains("amount"));
}

TEST_F(PostgresMCPServerTest, ListForeignTablesUnknownSchemaReturnsEmpty) {
  json result = srv->call_foreign_tables("does_not_exist");
  EXPECT_TRUE(result.empty());
}

// --- listTablespaces ---

TEST_F(PostgresMCPServerTest, ListTablespacesReturnsDefaultTablespace) {
  json result = srv->call_tablespaces();
  ASSERT_TRUE(result.contains("pg_default"));
  EXPECT_TRUE(result["pg_default"].contains("owner"));
  EXPECT_TRUE(result["pg_default"].contains("location"));
}

// --- listCollations ---

TEST_F(PostgresMCPServerTest, ListCollationsReturnsCustomCollation) {
  json result = srv->call_collations("grocery");
  ASSERT_TRUE(result.contains("case_sensitive_c"));
  auto& coll = result["case_sensitive_c"];
  EXPECT_EQ(coll["provider"].get<std::string>(), "libc");
  EXPECT_EQ(coll["description"].get<std::string>(), "byte-order comparison, copied from C");
}

// --- listEventTriggers ---

TEST_F(PostgresMCPServerTest, ListEventTriggersReturnsEnabledTrigger) {
  json result = srv->call_event_triggers();
  ASSERT_TRUE(result.contains("grocery_ddl_audit"));
  auto& evt = result["grocery_ddl_audit"];
  EXPECT_EQ(evt["event"].get<std::string>(), "ddl_command_end");
  EXPECT_TRUE(evt["enabled"].get<bool>());
  EXPECT_EQ(evt["description"].get<std::string>(), "audits DDL changes");
  EXPECT_NE(evt["function"].get<std::string>().find("probe_event_trigger_fn"), std::string::npos);
}

// --- listPublications ---

TEST_F(PostgresMCPServerTest, ListPublicationsReturnsPublicationWithTable) {
  json result = srv->call_publications();
  ASSERT_TRUE(result.contains("grocery_users_pub"));
  auto& pub = result["grocery_users_pub"];
  EXPECT_FALSE(pub["all_tables"].get<bool>());
  ASSERT_TRUE(pub["tables"].is_array());
  std::vector<std::string> tables(pub["tables"].begin(), pub["tables"].end());
  EXPECT_NE(std::find(tables.begin(), tables.end(), "grocery.users"), tables.end());
}

// --- listSubscriptions ---

TEST_F(PostgresMCPServerTest, ListSubscriptionsReturnsEmptyWhenNoneExist) {
  json result = srv->call_subscriptions();
  EXPECT_TRUE(result.empty());
}

// --- listTypes range support ---

TEST_F(PostgresMCPServerTest, ListTypesReturnsRangeType) {
  json result = srv->call_types("grocery");
  ASSERT_TRUE(result.contains("price_range"));
  auto& pr = result["price_range"];
  EXPECT_EQ(pr["kind"].get<std::string>(), "range");
  EXPECT_EQ(pr["subtype"].get<std::string>(), "numeric");
  ASSERT_TRUE(pr["multirange_type"].is_string());
  EXPECT_FALSE(pr["multirange_type"].get<std::string>().empty());
}

TEST_F(PostgresMCPServerTest, ListTypesExcludesMultirangeCompanion) {
  json result = srv->call_types("grocery");
  json price_range = result["price_range"];
  std::string multirange_name = price_range["multirange_type"].get<std::string>();
  // strip schema qualification if present
  auto dot = multirange_name.find('.');
  if (dot != std::string::npos) multirange_name = multirange_name.substr(dot + 1);
  EXPECT_FALSE(result.contains(multirange_name));
}

// --- tableDetails rules ---

TEST_F(PostgresMCPServerTest, TableDetailsIncludesRules) {
  json result = srv->call_table("grocery", "bare_notes");
  ASSERT_TRUE(result.contains("rules"));
  ASSERT_TRUE(result["rules"].contains("protect_bare_notes"));
  auto& rule = result["rules"]["protect_bare_notes"];
  EXPECT_EQ(rule["event"].get<std::string>(), "DELETE");
  EXPECT_TRUE(rule["instead"].get<bool>());
  EXPECT_NE(rule["definition"].get<std::string>().find("INSTEAD"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, TableDetailsNoRulesReturnsEmpty) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result["rules"].empty());
}

// --- tableDetails TOAST info ---

TEST_F(PostgresMCPServerTest, TableDetailsIncludesToastInfoWhenPresent) {
  json result = srv->call_table("grocery", "users"); // has VARCHAR columns
  ASSERT_TRUE(result.contains("toast"));
  ASSERT_FALSE(result["toast"].is_null());
  // The name is a catalog fact and stays; the sizes are measured and moved to
  // tableSize in 4.0.0. What this tool still answers is "does this table store
  // anything out of line at all".
  EXPECT_TRUE(result["toast"]["name"].get<std::string>().rfind("pg_toast", 0) == 0);
  EXPECT_FALSE(result["toast"].contains("size"));
  EXPECT_FALSE(result["toast"].contains("index_size"));
}

TEST_F(PostgresMCPServerTest, TableDetailsToastIsNullWhenNoToastableColumns) {
  json result = srv->call_table("grocery", "role_settings"); // enum PK + int, no toastable column
  ASSERT_TRUE(result.contains("toast"));
  EXPECT_TRUE(result["toast"].is_null());
}

TEST_F(PostgresMCPServerTest, TableDetailsColumnsIncludeStorageAndCompression) {
  json result = srv->call_table("grocery", "users");
  ASSERT_TRUE(result["columns"].contains("email"));
  auto& email_col = result["columns"]["email"];
  EXPECT_EQ(email_col["storage"].get<std::string>(), "extended");
  EXPECT_EQ(email_col["compression"].get<std::string>(), "default");

  json fixed = srv->call_table("grocery", "role_settings");
  ASSERT_TRUE(fixed["columns"].contains("max_sessions"));
  EXPECT_EQ(fixed["columns"]["max_sessions"]["storage"].get<std::string>(), "plain");
}

// --- listLanguages ---

TEST_F(PostgresMCPServerTest, ListLanguagesReturnsPlpgsql) {
  json result = srv->call_languages();
  ASSERT_TRUE(result.contains("plpgsql"));
  auto& lang = result["plpgsql"];
  EXPECT_TRUE(lang["trusted"].get<bool>());
  EXPECT_TRUE(lang["procedural"].get<bool>());
}

// --- listExtendedStatistics ---

TEST_F(PostgresMCPServerTest, ListExtendedStatisticsReturnsStatsObject) {
  json result = srv->call_extended_statistics("grocery");
  ASSERT_TRUE(result.contains("orders_stats"));
  auto& stats = result["orders_stats"];
  EXPECT_EQ(stats["table"].get<std::string>(), "grocery.orders");
  EXPECT_EQ(stats["description"].get<std::string>(), "user_id/amount correlation");
  ASSERT_TRUE(stats["columns"].is_array());
  std::vector<std::string> cols(stats["columns"].begin(), stats["columns"].end());
  EXPECT_NE(std::find(cols.begin(), cols.end(), "user_id"), cols.end());
  EXPECT_NE(std::find(cols.begin(), cols.end(), "amount"), cols.end());
  ASSERT_TRUE(stats["kinds"].is_array());
  std::vector<std::string> kinds(stats["kinds"].begin(), stats["kinds"].end());
  EXPECT_NE(std::find(kinds.begin(), kinds.end(), "dependencies"), kinds.end());
}

// --- listOperators ---

TEST_F(PostgresMCPServerTest, ListOperatorsReturnsCustomOperator) {
  json result = srv->call_operators("grocery");
  ASSERT_TRUE(result.contains("#+#(integer,integer)"));
  auto& op = result["#+#(integer,integer)"];
  EXPECT_EQ(op["left_type"].get<std::string>(), "integer");
  EXPECT_EQ(op["right_type"].get<std::string>(), "integer");
  EXPECT_EQ(op["result_type"].get<std::string>(), "integer");
  EXPECT_NE(op["function"].get<std::string>().find("add_ints"), std::string::npos);
}

// --- listOperatorClasses ---

TEST_F(PostgresMCPServerTest, ListOperatorClassesReturnsBtreeInt4Ops) {
  json result = srv->call_operator_classes("pg_catalog");
  ASSERT_TRUE(result.contains("int4_ops (btree)"));
  auto& opc = result["int4_ops (btree)"];
  EXPECT_EQ(opc["access_method"].get<std::string>(), "btree");
  EXPECT_EQ(opc["input_type"].get<std::string>(), "integer");
  EXPECT_TRUE(opc["default"].get<bool>());
}

// --- listAccessMethods ---

TEST_F(PostgresMCPServerTest, ListAccessMethodsReturnsBtreeAndHeap) {
  json result = srv->call_access_methods();
  ASSERT_TRUE(result.contains("btree"));
  EXPECT_EQ(result["btree"]["type"].get<std::string>(), "index");
  ASSERT_TRUE(result.contains("heap"));
  EXPECT_EQ(result["heap"]["type"].get<std::string>(), "table");
}

// --- listCasts ---

TEST_F(PostgresMCPServerTest, ListCastsReturnsCustomCast) {
  json result = srv->call_casts();
  ASSERT_TRUE(result.contains("grocery.address->text"));
  auto& cast = result["grocery.address->text"];
  EXPECT_EQ(cast["context"].get<std::string>(), "assignment");
  EXPECT_EQ(cast["method"].get<std::string>(), "function");
  EXPECT_NE(cast["function"].get<std::string>().find("address_to_text"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, ListCastsExcludesBuiltinToBuiltinNoise) {
  json result = srv->call_casts();
  // bigint->smallint is a built-in-to-built-in cast; neither side is user-defined
  EXPECT_FALSE(result.contains("bigint->smallint"));
}

// --- listTextSearchConfigs ---

TEST_F(PostgresMCPServerTest, ListTextSearchConfigsReturnsCopiedConfig) {
  json result = srv->call_text_search_configs("grocery");
  ASSERT_TRUE(result.contains("simple_english"));
  auto& cfg = result["simple_english"];
  EXPECT_EQ(cfg["parser"].get<std::string>(), "default");
  ASSERT_TRUE(cfg["mappings"].is_object());
  EXPECT_TRUE(cfg["mappings"].contains("asciiword"));
}

// --- listSequences ---

TEST_F(PostgresMCPServerTest, ListSequencesReturnsSerialOwnedSequence) {
  json result = srv->call_sequences("grocery");
  ASSERT_TRUE(result.contains("users_id_seq"));
  auto& seq = result["users_id_seq"];
  EXPECT_TRUE(seq.contains("data_type"));
  EXPECT_TRUE(seq.contains("increment_by"));
  EXPECT_TRUE(seq.contains("cycle"));
  EXPECT_FALSE(seq["cycle"].get<bool>());
  ASSERT_TRUE(seq["owned_by"].is_string());
  EXPECT_EQ(seq["owned_by"].get<std::string>(), "users.id");
}

TEST_F(PostgresMCPServerTest, ListSequencesStandaloneSequenceHasNullOwnedBy) {
  json result = srv->call_sequences("grocery");
  ASSERT_TRUE(result.contains("order_number_seq"));
  auto& seq = result["order_number_seq"];
  EXPECT_EQ(seq["start_value"].get<int64_t>(), 100);
  EXPECT_EQ(seq["description"].get<std::string>(), "external order numbering");
  EXPECT_TRUE(seq["owned_by"].is_null());
}

TEST_F(PostgresMCPServerTest, ListSequencesUnknownSchemaReturnsEmpty) {
  json result = srv->call_sequences("does_not_exist");
  EXPECT_TRUE(result.empty());
}

// --- listExtensions ---

TEST_F(PostgresMCPServerTest, ListExtensionsReturnsKnownExtension) {
  json result = srv->call_extensions();
  EXPECT_TRUE(result.is_object());
  ASSERT_TRUE(result.contains("plpgsql"));
  auto& ext = result["plpgsql"];
  EXPECT_TRUE(ext.contains("version"));
  EXPECT_TRUE(ext.contains("schema"));
  EXPECT_TRUE(ext.contains("relocatable"));
  EXPECT_TRUE(ext.contains("description"));
}

// --- databaseSize ---

TEST_F(PostgresMCPServerTest, DatabaseSizeReturnsDatabaseNameAndSize) {
  json result = srv->call_database_size();
  EXPECT_TRUE(result.is_object());
  EXPECT_TRUE(result.contains("database"));
  EXPECT_TRUE(result.contains("size"));
  EXPECT_FALSE(result["database"].get<std::string>().empty());
  EXPECT_GT(result["size"].get<int64_t>(), 0);
}

// --- currentActivity ---

TEST_F(PostgresMCPServerTest, ActivityReturnsOtherBackends) {
  json result = srv->call_activity();
  EXPECT_TRUE(result.is_object());
  EXPECT_GT(result.size(), 0u); // admin_conn's backend is visible even if idle
  for (auto& [pid, entry] : result.items()) {
    EXPECT_TRUE(entry.contains("backend_type"));
    EXPECT_TRUE(entry.contains("state"));
    EXPECT_TRUE(entry.contains("query"));
    break;
  }
}

// --- currentLocks ---

TEST_F(PostgresMCPServerTest, LocksShowsHeldLockFromAnotherConnection) {
  pqxx::connection lock_conn(test_url);
  pqxx::work lock_txn(lock_conn);
  lock_txn.exec("LOCK TABLE grocery.users IN ACCESS EXCLUSIVE MODE");

  json result = srv->call_locks();
  // 4.0.0: the rows are under "locks" -- a top-level array cannot be a
  // structuredContent payload.
  ASSERT_TRUE(result.is_object());
  ASSERT_TRUE(result.contains("locks"));
  ASSERT_TRUE(result["locks"].is_array());

  bool found = false;
  for (auto& lock : result["locks"]) {
    if (lock.contains("relation") && !lock["relation"].is_null() &&
        lock["relation"].get<std::string>() == "grocery.users" &&
        lock["granted"].get<bool>()) {
      found = true;
      EXPECT_EQ(lock["mode"].get<std::string>(), "AccessExclusiveLock");
      break;
    }
  }
  EXPECT_TRUE(found);

  lock_txn.abort();
}

// --- replicationSlots ---

TEST_F(PostgresMCPServerTest, ReplicationSlotsReturnsEmptyWhenNoneConfigured) {
  json result = srv->call_replication_slots();
  EXPECT_TRUE(result.empty());
}

// --- databaseStats ---

TEST_F(PostgresMCPServerTest, DatabaseStatsIncludesCurrentDatabase) {
  json result = srv->call_database_stats();
  ASSERT_TRUE(result.contains(test_dbname));
  auto& stats = result[test_dbname];
  EXPECT_TRUE(stats.contains("numbackends"));
  EXPECT_TRUE(stats.contains("xact_commit"));
  EXPECT_TRUE(stats.contains("deadlocks"));
  EXPECT_GE(stats["numbackends"].get<int>(), 1); // srv's own connection
}

// --- statementStats ---

TEST_F(PostgresMCPServerTest, StatementStatsReturnsHintWhenNotInstalled) {
  json result = srv->call_statement_stats(20);
  // pg_stat_statements is not in shared_preload_libraries in the test environment.
  ASSERT_TRUE(result.contains("error"));
  EXPECT_EQ(result["error"].get<std::string>(), "pg_stat_statements is not installed");
  EXPECT_TRUE(result.contains("hint"));
}

// --- tableBloat ---

TEST_F(PostgresMCPServerTest, TableBloatApproxReturnsExpectedFields) {
  json result = srv->call_table_bloat("grocery", "users", false);
  EXPECT_EQ(result["method"].get<std::string>(), "approx");
  EXPECT_TRUE(result.contains("table_len"));
  EXPECT_TRUE(result.contains("scanned_percent"));
  EXPECT_TRUE(result.contains("tuple_count"));
  EXPECT_TRUE(result.contains("dead_tuple_percent"));
  EXPECT_TRUE(result.contains("free_percent"));
}

TEST_F(PostgresMCPServerTest, TableBloatExactReturnsExpectedFields) {
  json result = srv->call_table_bloat("grocery", "users", true);
  EXPECT_EQ(result["method"].get<std::string>(), "exact");
  EXPECT_FALSE(result.contains("scanned_percent")); // exact scans the whole table
  EXPECT_TRUE(result.contains("table_len"));
  EXPECT_TRUE(result.contains("tuple_count"));
  EXPECT_TRUE(result.contains("dead_tuple_percent"));
  EXPECT_TRUE(result.contains("free_percent"));
}

TEST_F(PostgresMCPServerTest, TableBloatUnknownTableReturnsEmpty) {
  json result = srv->call_table_bloat("grocery", "does_not_exist", false);
  EXPECT_TRUE(result.empty());
}

// --- indexBloat ---
//
// pgstattuple lives in the "extensions" schema in this fixture (see
// SetUpTestSuite), so every one of these also exercises schema qualification:
// none of the three functions is reachable through the default search_path.

TEST_F(PostgresMCPServerTest, IndexBloatBtreeReturnsPgstatindexFields) {
  json r = srv->call_index_bloat("grocery", "ams_btree");
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["access_method"].get<std::string>(), "btree");
  EXPECT_EQ(r["index"].get<std::string>(), "ams_btree");
  EXPECT_EQ(r["table"].get<std::string>(), "index_ams");
  EXPECT_EQ(r["schema"].get<std::string>(), "grocery");
  for (const char* f : {"version", "tree_level", "root_block_no", "internal_pages",
                        "leaf_pages", "empty_pages", "deleted_pages",
                        "avg_leaf_density", "leaf_fragmentation"})
    EXPECT_TRUE(r.contains(f)) << f << " missing from " << r.dump(2);
  EXPECT_GT(r["index_size"].get<long long>(), 0);
  EXPECT_GT(r["leaf_pages"].get<long long>(), 0);
  // GIN-only fields must not leak onto a btree result.
  EXPECT_FALSE(r.contains("pending_pages"));
  EXPECT_FALSE(r.contains("fastupdate"));
}

TEST_F(PostgresMCPServerTest, IndexBloatGinReturnsPendingListWithItsBounds) {
  json r = srv->call_index_bloat("grocery", "ams_gin");
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["access_method"].get<std::string>(), "gin");
  ASSERT_TRUE(r.contains("pending_pages")) << r.dump(2);
  ASSERT_TRUE(r.contains("pending_tuples"));
  EXPECT_TRUE(r.contains("version"));
  EXPECT_GE(r["pending_pages"].get<long long>(), 0);

  // pending_pages is only interpretable against the settings that bound it,
  // so the tool reports them alongside; both were set explicitly on this index.
  EXPECT_EQ(r["fastupdate"].get<std::string>(), "on");
  EXPECT_EQ(r["pending_list_limit_kb"].get<std::string>(), "128");

  // btree-only fields must not leak onto a GIN result.
  EXPECT_FALSE(r.contains("leaf_fragmentation"));
  EXPECT_FALSE(r.contains("avg_leaf_density"));
}

TEST_F(PostgresMCPServerTest, IndexBloatHashReturnsBucketAndOverflowPages) {
  json r = srv->call_index_bloat("grocery", "ams_hash");
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["access_method"].get<std::string>(), "hash");
  for (const char* f : {"version", "bucket_pages", "overflow_pages", "bitmap_pages",
                        "unused_pages", "live_items", "dead_items", "free_percent"})
    EXPECT_TRUE(r.contains(f)) << f << " missing from " << r.dump(2);
  EXPECT_GT(r["bucket_pages"].get<long long>(), 0);
  EXPECT_FALSE(r.contains("leaf_pages"));
}

// The point of resolving the access method from the catalog: a gist index
// must produce a stated answer naming what is supported, not the bare
// "relation is not a btree index" that pgstatindex would raise.
TEST_F(PostgresMCPServerTest, IndexBloatUnsupportedAccessMethodIsNamed) {
  json r = srv->call_index_bloat("grocery", "ams_gist");
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("gist"), std::string::npos);
  EXPECT_EQ(r["access_method"].get<std::string>(), "gist");
  ASSERT_TRUE(r.contains("hint"));
  EXPECT_NE(r["hint"].get<std::string>().find("pageinspect"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, IndexBloatPartitionedIndexHasNoStorageOfItsOwn) {
  json r = srv->call_index_bloat("grocery", "parted_id_idx");
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("partitioned index"), std::string::npos);
  EXPECT_TRUE(r.contains("hint"));
}

TEST_F(PostgresMCPServerTest, IndexBloatOnATableSaysToUseTableBloat) {
  json r = srv->call_index_bloat("grocery", "users");
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("is not an index"), std::string::npos);
  EXPECT_NE(r["hint"].get<std::string>().find("tableBloat"), std::string::npos);
}

// An unknown name is a stated result, not an exception -- and an unknown
// schema must behave the same, which a ::regnamespace cast would not.
TEST_F(PostgresMCPServerTest, IndexBloatUnknownIndexAndSchemaAreReportedNotThrown) {
  json r = srv->call_index_bloat("grocery", "does_not_exist");
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("no relation named"), std::string::npos);

  json s = srv->call_index_bloat("no_such_schema", "does_not_exist");
  ASSERT_TRUE(s.contains("error")) << s.dump(2);
  EXPECT_NE(s["error"].get<std::string>().find("no relation named"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, IndexBloatIsRegisteredAsATool) {
  json tools = srv->call_tools_list();
  bool found = false;
  for (const auto& t : tools)
    if (t.value("name", "") == "indexBloat") found = true;
  EXPECT_TRUE(found) << "indexBloat missing from tools/list";
}

// --- currentActivity enrichment ---

TEST_F(PostgresMCPServerTest, ActivityCarriesQueryIdAsTextForExplainQuery) {
  // query_id is the join key from a running statement to statementStats and
  // explainQuery. It must be text: a 64-bit value does not survive JSON
  // number precision, and explainQuery takes it as a string.
  pqxx::connection other(test_url);
  pqxx::work t(other);
  t.exec("SELECT pg_sleep(0)");

  json r = srv->call_activity();
  ASSERT_TRUE(r.is_object());

  bool saw_backend = false;
  for (auto& [pid, b] : r.items()) {
    (void)pid;
    ASSERT_TRUE(b.contains("query_id")) << b.dump(2);
    EXPECT_TRUE(b["query_id"].is_string() || b["query_id"].is_null());
    EXPECT_TRUE(b.contains("leader_pid"));
    EXPECT_TRUE(b.contains("backend_xid"));
    EXPECT_TRUE(b.contains("backend_xmin"));
    EXPECT_TRUE(b.contains("xact_duration_s"));
    EXPECT_TRUE(b.contains("query_duration_s"));
    saw_backend = true;
  }
  EXPECT_TRUE(saw_backend);
  t.abort();
}

TEST_F(PostgresMCPServerTest, ActivityDescribesWaitEventsOnPg17AndLater) {
  json r = srv->call_activity();
  const bool expected = pg_server_version_num(test_url) >= 170000;
  bool found = false;
  for (auto& [pid, b] : r.items()) {
    (void)pid;
    if (b.contains("wait_event_description")) found = true;
  }
  // pg_wait_events, the source of the descriptions, is PostgreSQL 17+.
  EXPECT_EQ(found, expected);
}

TEST_F(PostgresMCPServerTest, ActivityFiltersByPidAndState) {
  pqxx::connection other(test_url);
  pqxx::work t(other);
  int pid = t.exec("SELECT pg_backend_pid()")[0][0].as<int>();

  json r = srv->call_activity(pid, "", 0, "");
  ASSERT_EQ(r.size(), 1u) << r.dump(2);
  ASSERT_TRUE(r.contains(std::to_string(pid)));

  // A state that backend cannot be in, so the filter must exclude it rather
  // than being ignored.
  json none = srv->call_activity(pid, "", 0, "no such state");
  EXPECT_TRUE(none.is_object());
  EXPECT_TRUE(none.empty()) << none.dump(2);

  t.abort();
}

TEST_F(PostgresMCPServerTest, ActivityFiltersByMinDuration) {
  pqxx::connection other(test_url);
  pqxx::work t(other);
  int pid = t.exec("SELECT pg_backend_pid()")[0][0].as<int>();

  // That backend is idle in transaction with a query that finished instantly,
  // so an hour-long threshold must not match it. This is the regression guard
  // for measuring duration off a stale query_start.
  json r = srv->call_activity(pid, "", 3600, "");
  EXPECT_TRUE(r.empty()) << r.dump(2);

  // And a zero-ish threshold does match, so the filter is not simply broken.
  json any = srv->call_activity(pid, "", 0, "");
  EXPECT_FALSE(any.empty());

  t.abort();
}

TEST_F(PostgresMCPServerTest, ActivityFilterExcludesUnrelatedBackends) {
  pqxx::connection a(test_url), b(test_url);
  pqxx::work ta(a), tb(b);
  int pid_a = ta.exec("SELECT pg_backend_pid()")[0][0].as<int>();
  int pid_b = tb.exec("SELECT pg_backend_pid()")[0][0].as<int>();
  ASSERT_NE(pid_a, pid_b);

  json r = srv->call_activity(pid_a, "", 0, "");
  EXPECT_TRUE(r.contains(std::to_string(pid_a)));
  EXPECT_FALSE(r.contains(std::to_string(pid_b))) << r.dump(2);

  ta.abort();
  tb.abort();
}

// --- currentLocks blocking chain ---

TEST_F(PostgresMCPServerTest, LocksResolveTheTransitiveBlockingChain) {
  // holder takes the table; waiter then asks for a conflicting lock and
  // blocks. Asking about the waiter must surface the holder, which is the
  // whole point: a flat lock dump makes you reconstruct that yourself.
  pqxx::connection holder(test_url);
  pqxx::work hold(holder);
  hold.exec("LOCK TABLE grocery.users IN ACCESS EXCLUSIVE MODE");
  int holder_pid = hold.exec("SELECT pg_backend_pid()")[0][0].as<int>();

  pqxx::connection waiter(test_url);
  pqxx::work wait_txn(waiter);
  int waiter_pid = wait_txn.exec("SELECT pg_backend_pid()")[0][0].as<int>();

  pqxx::pipeline p(wait_txn);
  p.insert("LOCK TABLE grocery.users IN ACCESS EXCLUSIVE MODE");

  json chain;
  for (int i = 0; i < 40 && chain.is_null(); i++) {
    json r = srv->call_locks(waiter_pid);
    if (!r.is_object() || !r["locks"].is_array()) continue;
    for (const auto& l : r["locks"]) {
      if (l.contains("chain_depth") && l["chain_depth"].get<int>() > 0) chain = r["locks"];
    }
  }

  // Release the holder so the waiter can proceed, whatever the assertions do.
  hold.abort();
  try {
    p.complete();
  } catch (const pqxx::sql_error&) {
  }
  wait_txn.abort();

  if (chain.is_null()) GTEST_SKIP() << "the lock wait could not be observed";

  bool saw_waiter = false, saw_holder = false;
  for (const auto& l : chain) {
    int pid = l["pid"].get<int>();
    if (pid == waiter_pid) {
      saw_waiter = true;
      EXPECT_EQ(l["chain_depth"].get<int>(), 0);
    }
    if (pid == holder_pid) {
      saw_holder = true;
      // Depth 1: one hop up the chain from the backend asked about.
      EXPECT_EQ(l["chain_depth"].get<int>(), 1);
    }
    // Nothing outside the chain may appear.
    EXPECT_TRUE(pid == waiter_pid || pid == holder_pid) << l.dump(2);
  }
  EXPECT_TRUE(saw_waiter) << chain.dump(2);
  EXPECT_TRUE(saw_holder) << chain.dump(2);
}

// --- replicationSlots enrichment ---

TEST_F(PostgresMCPServerTest, ReplicationSlotsReportsWalStatusAndSpillCounters) {
  pqxx::connection c(test_url);
  {
    pqxx::nontransaction n(c);
    n.exec("SELECT pg_create_physical_replication_slot('pg_licht_probe_slot')");
  }

  json r = srv->call_replication_slots();
  ASSERT_TRUE(r.contains("pg_licht_probe_slot")) << r.dump(2);
  auto& s = r["pg_licht_probe_slot"];
  EXPECT_EQ(s["slot_type"].get<std::string>(), "physical");
  // wal_status is the verdict that retained_wal_bytes only hints at:
  // 'extended' is past max_wal_size, 'lost' means the slot is unusable.
  EXPECT_TRUE(s.contains("wal_status"));
  EXPECT_TRUE(s.contains("safe_wal_size"));
  EXPECT_TRUE(s.contains("two_phase"));
  // From pg_stat_replication_slots, a different view: logical decoding
  // spilling to disk is invisible in the slot's own row.
  EXPECT_TRUE(s.contains("spill_txns"));
  EXPECT_TRUE(s.contains("spill_bytes"));
  EXPECT_TRUE(s.contains("total_bytes"));

  if (pg_server_version_num(test_url) >= 160000) {
    EXPECT_TRUE(s.contains("conflicting"));
  }
  if (pg_server_version_num(test_url) >= 170000) {
    EXPECT_TRUE(s.contains("invalidation_reason"));
    EXPECT_TRUE(s.contains("inactive_since"));
  }

  pqxx::nontransaction drop(c);
  drop.exec("SELECT pg_drop_replication_slot('pg_licht_probe_slot')");
}

// --- databaseStats enrichment ---

TEST_F(PostgresMCPServerTest, DatabaseStatsIncludesSessionCounters) {
  json r = srv->call_database_stats();
  ASSERT_TRUE(r.contains(test_dbname));
  auto& s = r[test_dbname];
  // These distinguish a database that is busy from one merely holding
  // transactions open; the commit/rollback counts cannot tell them apart.
  EXPECT_TRUE(s.contains("session_time"));
  EXPECT_TRUE(s.contains("active_time"));
  EXPECT_TRUE(s.contains("idle_in_transaction_time"));
  EXPECT_TRUE(s.contains("sessions"));
  EXPECT_TRUE(s.contains("sessions_abandoned"));
  EXPECT_TRUE(s.contains("sessions_fatal"));
  EXPECT_TRUE(s.contains("sessions_killed"));

  const bool pg18 = pg_server_version_num(test_url) >= 180000;
  EXPECT_EQ(s.contains("parallel_workers_to_launch"), pg18);
  EXPECT_EQ(s.contains("parallel_workers_launched"), pg18);
}

// --- listTableStats / tableStats version-gated columns ---

TEST_F(PostgresMCPServerTest, TableStatsIncludeFailedHotUpdatesOnPg16AndLater) {
  const bool pg16 = pg_server_version_num(test_url) >= 160000;

  // The gate followed the fields out of the structure tools in 4.0.0.
  json listed = srv->call_list_table_stats("grocery");
  ASSERT_TRUE(listed.contains("users")) << listed.dump(2);
  // n_tup_newpage_upd counts updates that had to move the row to another
  // page: the direct measure of failed HOT updates.
  EXPECT_EQ(listed["users"].contains("n_tup_newpage_upd"), pg16);
  EXPECT_EQ(listed["users"].contains("last_seq_scan"), pg16);

  json one = srv->call_table_stats("grocery", "users");
  EXPECT_EQ(one.contains("n_tup_newpage_upd"), pg16);
  EXPECT_EQ(one.contains("last_seq_scan"), pg16);

  // pg_stat_user_indexes.last_idx_scan is gated on the same release, and it is
  // built by concatenation now rather than erased out of a finished query --
  // the older approach failed silently if the literal ever drifted.
  ASSERT_FALSE(one["indexes"].empty()) << one.dump(2);
  for (auto& [name, idx] : one["indexes"].items()) {
    EXPECT_TRUE(idx.contains("index_uses")) << name;
    EXPECT_EQ(idx.contains("last_use"), pg16) << name;
  }
}

// --- 4.0.0 statistics split ---

TEST_F(PostgresMCPServerTest, TableStatsCarriesTheReadingsTableDetailsLost) {
  json r = srv->call_table_stats("grocery", "users");
  for (const char* f : {"table", "rows", "size_estimate", "estimated_from",
                        "seq_scan", "idx_scan", "n_live_tup", "n_dead_tup",
                        "n_mod_since_analyze", "n_ins_since_vacuum",
                        "last_vacuum", "last_analyze", "columns", "indexes"})
    EXPECT_TRUE(r.contains(f)) << f << " missing from tableStats";
  EXPECT_EQ(r["table"].get<std::string>(), "users");

  // The pg_stats histograms live here now, including the sample values.
  ASSERT_TRUE(r["columns"].contains("email")) << r["columns"].dump(2);
  for (const char* f : {"null_frac", "avg_width", "n_distinct",
                        "physical_order_correlation", "most_common_vals",
                        "most_common_freqs"})
    EXPECT_TRUE(r["columns"]["email"].contains(f)) << f;
}

TEST_F(PostgresMCPServerTest, TableStatsListsEveryColumnEvenWithoutStatistics) {
  // Nulls are kept rather than stripped: a column with no row in pg_stats has
  // never been analyzed, and that is the actionable signal. Stripping it would
  // make "never analyzed" and "column does not exist" look the same.
  json r = srv->call_table_stats("grocery", "users");
  json structure = srv->call_table("grocery", "users");
  EXPECT_EQ(r["columns"].size(), structure["columns"].size());
}

TEST_F(PostgresMCPServerTest, ListTableStatsCoversTheSchemaWithoutHistograms) {
  json r = srv->call_list_table_stats("grocery");
  ASSERT_TRUE(r.contains("users")) << r.dump(2);
  for (const char* f : {"rows", "size_estimate", "estimated_from", "seq_scan",
                        "idx_scan", "n_live_tup", "n_dead_tup", "last_vacuum",
                        "last_analyze"})
    EXPECT_TRUE(r["users"].contains(f)) << f << " missing from listTableStats";
  // Per-column histograms for every table in a schema is a payload nobody
  // asked for; name one table to tableStats instead.
  EXPECT_FALSE(r["users"].contains("columns"));
}

TEST_F(PostgresMCPServerTest, SizeEstimateIsNamedAsAnEstimateAndDatedByIt) {
  json stats = srv->call_table_stats("grocery", "users");
  // The field is deliberately not called `size`: relpages is only as fresh as
  // the last vacuum or analyze, and estimated_from is what lets a caller judge
  // that rather than guess.
  EXPECT_TRUE(stats.contains("size_estimate"));
  EXPECT_FALSE(stats.contains("size"));
  EXPECT_TRUE(stats.contains("estimated_from"));

  json measured = srv->call_table_size("grocery", "users");
  // ...and the measured tool uses the plain name, so the two can never be
  // confused for one another.
  EXPECT_TRUE(measured.contains("size"));
  EXPECT_FALSE(measured.contains("size_estimate"));
}

TEST_F(PostgresMCPServerTest, TableSizeMeasuresEveryFork) {
  json r = srv->call_table_size("grocery", "users");
  for (const char* f : {"table", "kind", "main_size", "size", "indexes_size",
                        "total_size", "toast", "indexes"})
    EXPECT_TRUE(r.contains(f)) << f << " missing from tableSize";
  // pg_table_size covers the main fork plus the free space and visibility
  // maps, so it is at least the main fork; the grand total adds the indexes.
  EXPECT_GE(r["size"].get<long long>(), r["main_size"].get<long long>());
  EXPECT_GE(r["total_size"].get<long long>(), r["size"].get<long long>());
  ASSERT_FALSE(r["indexes"].empty());
  for (auto& [name, sz] : r["indexes"].items())
    EXPECT_TRUE(sz.is_number()) << name;
  // The TOAST sizes that left tableDetails landed here.
  ASSERT_FALSE(r["toast"].is_null());
  EXPECT_TRUE(r["toast"].contains("size"));
  EXPECT_TRUE(r["toast"].contains("index_size"));
}

TEST_F(PostgresMCPServerTest, ListTableSizesMeasuresEveryRelationInTheSchema) {
  json r = srv->call_list_table_sizes("grocery");
  ASSERT_TRUE(r.contains("users")) << r.dump(2);
  for (const char* f : {"kind", "size", "indexes_size", "total_size"})
    EXPECT_TRUE(r["users"].contains(f)) << f;
  EXPECT_GE(r["users"]["total_size"].get<long long>(),
            r["users"]["size"].get<long long>());
}

TEST_F(PostgresMCPServerTest, SizeToolsDoNotFailOnRelationsWithNoStorage) {
  // A view and a partitioned parent own no files. pg_table_size returns 0 for
  // them rather than raising, which is what lets listTableSizes cover a whole
  // schema without the caller pre-filtering by relkind -- but a caller reading
  // 0 off a partitioned table is being told about the parent, not the data,
  // and the tool description says so.
  json r = srv->call_list_table_sizes("grocery");
  bool saw_view = false;
  for (auto& [name, v] : r.items()) {
    if (v["kind"].get<std::string>() == "view") {
      saw_view = true;
      EXPECT_EQ(v["size"].get<long long>(), 0) << name;
    }
  }
  EXPECT_TRUE(saw_view) << "the fixture has no view to prove this with";
}

TEST_F(PostgresMCPServerTest, StatisticsSplitCorrectsTheSweepClassification) {
  // Reason 1, and the point of the whole exercise: structure is byte-identical
  // on a physical replica, so the structure tools must refuse a
  // replication_group sweep while the statistics tools accept one.
  json tools = srv->call_tools_list();
  std::map<std::string, json> by_name;
  for (const auto& t : tools) by_name[t.value("name", "")] = t;

  for (const char* n : {"listTables", "searchTables", "tableDetails",
                        "tableSize", "listTableSizes"}) {
    ASSERT_TRUE(by_name.count(n)) << n;
    EXPECT_FALSE(by_name[n]["inputSchema"]["properties"].contains("replication_group"))
        << n << " still advertises a replication-group sweep";
    EXPECT_NE(by_name[n].value("description", "").find("byte-identical here"),
              std::string::npos) << n;
  }
  for (const char* n : {"tableStats", "listTableStats"}) {
    ASSERT_TRUE(by_name.count(n)) << n;
    EXPECT_TRUE(by_name[n]["inputSchema"]["properties"].contains("replication_group"))
        << n << " cannot sweep, and it is the half that should";
    EXPECT_NE(by_name[n].value("description", "").find("each server's own"),
              std::string::npos) << n;
  }
}

TEST_F(PostgresMCPServerTest, TheGatedSizeToolsSayWhatTheyCost) {
  // The gate is the description, because MCP has no annotation for "expensive"
  // and a server cannot force a client to confirm. What it can do is make the
  // cost visible where the model chooses the tool.
  json tools = srv->call_tools_list();
  for (const auto& t : tools) {
    const std::string n = t.value("name", "");
    if (n != "tableSize" && n != "listTableSizes") continue;
    const std::string d = t.value("description", "");
    EXPECT_NE(d.find("AccessShareLock"), std::string::npos) << n;
    EXPECT_NE(d.find("AccessExclusiveLock"), std::string::npos) << n;
    EXPECT_NE(d.find("size_estimate"), std::string::npos)
        << n << " does not point at the free alternative";
  }
}

// --- progressStats ---

TEST_F(PostgresMCPServerTest, ProgressStatsReportsEveryCommandCategory) {
  json r = srv->call_progress_stats();
  // All six categories are always present, empty when nothing is running, so
  // a caller never has to guess whether a missing key means "idle" or
  // "unsupported on this version".
  for (const char* k : {"vacuum", "analyze", "create_index", "cluster", "copy", "basebackup"}) {
    ASSERT_TRUE(r.contains(k)) << r.dump(2);
    EXPECT_TRUE(r[k].is_array()) << k;
  }
}

TEST_F(PostgresMCPServerTest, ProgressStatsReportsARunningVacuum) {
  // Enough dead tuples, and a slow enough vacuum, that it is still running
  // when the tool is called.
  pqxx::connection setup(test_url);
  {
    pqxx::work w(setup);
    w.exec("CREATE TABLE grocery.vacuum_me AS "
           "SELECT g AS id, repeat('x', 200) AS pad FROM generate_series(1, 60000) g");
    w.exec("UPDATE grocery.vacuum_me SET pad = pad");
    w.commit();
  }

  // A separate connection, so the vacuum runs while this thread polls.
  pqxx::connection vac(test_url);
  {
    pqxx::nontransaction n(vac);
    n.exec("SET vacuum_cost_delay = 100");
    n.exec("SET vacuum_cost_limit = 10");
  }
  // pqxx has no async exec, so the vacuum is issued on a pipeline and the
  // tool is called while it is in flight.
  pqxx::nontransaction n(vac);
  pqxx::pipeline p(n);
  p.insert("VACUUM grocery.vacuum_me");

  json found, by_relation, by_pid, by_wrong_relation;
  for (int i = 0; i < 40 && found.is_null(); i++) {
    json r = srv->call_progress_stats();
    for (const auto& v : r["vacuum"]) {
      if (v.value("relation", "") == "grocery.vacuum_me") {
        found = v;
        // Exercise the filters while the command is genuinely in flight;
        // there is no other moment at which they can be observed working.
        by_relation = srv->call_progress_stats(0, "vacuum_me");
        by_pid = srv->call_progress_stats(v["pid"].get<int>(), "");
        by_wrong_relation = srv->call_progress_stats(0, "grocery.users");
      }
    }
  }

  // Cancel rather than wait: the vacuum was deliberately slowed to be
  // observable, so letting it run to completion would cost the suite a minute
  // for no extra coverage.
  if (!found.is_null()) {
    pqxx::connection killer(test_url);
    pqxx::nontransaction k(killer);
    k.exec("SELECT pg_cancel_backend(" + std::to_string(found["pid"].get<int>()) + ")");
  }
  try {
    p.complete();
  } catch (const pqxx::sql_error&) {
    // Expected: the cancellation above surfaces here.
  }

  pqxx::connection cleanup(test_url);
  pqxx::work drop(cleanup);
  drop.exec("DROP TABLE grocery.vacuum_me");
  drop.commit();

  if (found.is_null()) GTEST_SKIP() << "the vacuum finished before it could be observed";

  EXPECT_TRUE(found.contains("phase"));
  EXPECT_TRUE(found.contains("heap_blks_total"));
  EXPECT_TRUE(found.contains("scanned_percent"));
  EXPECT_TRUE(found.contains("elapsed_s"));
  EXPECT_EQ(found["query"].get<std::string>(), "VACUUM grocery.vacuum_me");
  // The PostgreSQL 17 rename is normalized behind a unit label rather than
  // pretended away: the old and new columns measure different things.
  if (pg_server_version_num(test_url) >= 170000) {
    EXPECT_EQ(found["dead_tuple_unit"].get<std::string>(), "bytes");
    EXPECT_TRUE(found.contains("max_dead_tuple_bytes"));
    EXPECT_TRUE(found.contains("num_dead_item_ids"));
    EXPECT_TRUE(found.contains("indexes_total"));
  } else {
    EXPECT_EQ(found["dead_tuple_unit"].get<std::string>(), "tuples");
    EXPECT_TRUE(found.contains("max_dead_tuples"));
    EXPECT_TRUE(found.contains("num_dead_tuples"));
  }

  // The bare relation name matches, and so does the pid.
  EXPECT_EQ(by_relation["vacuum"].size(), 1u) << by_relation.dump(2);
  EXPECT_EQ(by_pid["vacuum"].size(), 1u) << by_pid.dump(2);
  // A different table does not.
  EXPECT_TRUE(by_wrong_relation["vacuum"].empty()) << by_wrong_relation.dump(2);
  // A base backup has no relation, so a relation filter excludes that
  // category rather than matching everything in it.
  EXPECT_TRUE(by_relation["basebackup"].empty());
}

TEST_F(PostgresMCPServerTest, ProgressStatsUnknownRelationIsEmptyNotAnError) {
  // A regclass cast would raise "relation does not exist"; a diagnostic tool
  // should answer "nothing is running on that" instead.
  json r = srv->call_progress_stats(0, "no_such_table_anywhere");
  ASSERT_TRUE(r.contains("vacuum")) << r.dump(2);
  EXPECT_TRUE(r["vacuum"].empty());
  EXPECT_TRUE(r["create_index"].empty());
}

// --- ioStats ---

TEST_F(PostgresMCPServerTest, IoStatsReturnsRowsOrAVersionError) {
  json r = srv->call_io_stats();

  if (pg_server_version_num(test_url) < 160000) {
    ASSERT_TRUE(r.contains("error")) << r.dump(2);
    EXPECT_NE(r["error"].get<std::string>().find("PostgreSQL 16"), std::string::npos);
    EXPECT_TRUE(r.contains("hint"));
    return;
  }

  ASSERT_TRUE(r.contains("io")) << r.dump(2);
  ASSERT_TRUE(r["io"].is_array());
  ASSERT_FALSE(r["io"].empty());
  auto& row = r["io"][0];
  EXPECT_TRUE(row.contains("backend_type"));
  EXPECT_TRUE(row.contains("object"));
  EXPECT_TRUE(row.contains("context"));
  EXPECT_TRUE(row.contains("reads"));
  EXPECT_TRUE(row.contains("writes"));
  EXPECT_TRUE(row.contains("hit_percent"));
  // The byte counters replaced op_bytes in PostgreSQL 18; before that there
  // is no honest way to report them.
  EXPECT_EQ(row.contains("read_bytes"), pg_server_version_num(test_url) >= 180000);
}

TEST_F(PostgresMCPServerTest, IoStatsFiltersByBackendTypeAndContext) {
  if (pg_server_version_num(test_url) < 160000)
    GTEST_SKIP() << "pg_stat_io requires PostgreSQL 16";

  json r = srv->call_io_stats(0, "checkpointer", "", "");
  ASSERT_TRUE(r.contains("io")) << r.dump(2);
  for (const auto& row : r["io"])
    EXPECT_EQ(row["backend_type"].get<std::string>(), "checkpointer");

  json c = srv->call_io_stats(0, "", "", "vacuum");
  for (const auto& row : c["io"])
    EXPECT_EQ(row["context"].get<std::string>(), "vacuum");
}

TEST_F(PostgresMCPServerTest, IoStatsPerBackendNeedsPg18AndSaysSo) {
  if (pg_server_version_num(test_url) < 160000)
    GTEST_SKIP() << "pg_stat_io requires PostgreSQL 16";

  // A live backend of our own, so the pid certainly exists.
  pqxx::connection victim(test_url);
  pqxx::work t(victim);
  int pid = t.exec("SELECT pg_backend_pid()")[0][0].as<int>();

  json r = srv->call_io_stats(pid, "", "", "");

  if (pg_server_version_num(test_url) < 180000) {
    // pg_stat_io has no pid column at all; the per-backend function is 18+.
    ASSERT_TRUE(r.contains("error")) << r.dump(2);
    EXPECT_NE(r["error"].get<std::string>().find("PostgreSQL 18"), std::string::npos);
    EXPECT_TRUE(r.contains("hint"));
  } else {
    EXPECT_EQ(r["pid"].get<int>(), pid);
    ASSERT_TRUE(r.contains("io")) << r.dump(2);
    // WAL is a separate function: a backend can be quiet in I/O and still be
    // writing WAL heavily, so it is reported alongside rather than inferred.
    ASSERT_TRUE(r.contains("wal")) << r.dump(2);
    EXPECT_TRUE(r["wal"].contains("wal_records"));
    EXPECT_TRUE(r["wal"].contains("wal_bytes"));
    for (const auto& row : r["io"])
      EXPECT_EQ(row["backend_type"].get<std::string>(), "client backend");
  }
  t.abort();
}

// --- wraparoundStatus ---

namespace {

// The 'tables' array keyed by name, for the assertions below.
const json* find_table(const json& tables, const std::string& name) {
  for (const auto& t : tables)
    if (t.value("name", "") == name) return &t;
  return nullptr;
}

}  // namespace

TEST_F(PostgresMCPServerTest, WraparoundStatusReportsLimitsAndDatabases) {
  json r = srv->call_wraparound_status("", 20);
  ASSERT_TRUE(r.contains("limits")) << r.dump(2);
  EXPECT_TRUE(r["limits"].contains("autovacuum_freeze_max_age"));
  EXPECT_TRUE(r["limits"].contains("autovacuum_multixact_freeze_max_age"));
  EXPECT_TRUE(r["limits"].contains("vacuum_failsafe_age"));
  // The hard limit is the outage threshold, and is what the percentages that
  // actually matter are taken against.
  EXPECT_EQ(r["limits"]["wraparound_limit"].get<long long>(), 2146483647LL);

  ASSERT_TRUE(r.contains("databases"));
  ASSERT_TRUE(r["databases"].contains(test_dbname));
  auto& db = r["databases"][test_dbname];
  EXPECT_GE(db["xid_age"].get<long long>(), 0);
  EXPECT_GE(db["mxid_age"].get<long long>(), 0);
  EXPECT_LT(db["xid_percent_of_wraparound_limit"].get<double>(), 100.0);
  EXPECT_GT(db["xids_until_wraparound_limit"].get<long long>(), 0);
}

TEST_F(PostgresMCPServerTest, WraparoundStatusReportsPerTableFreezeOverride) {
  json r = srv->call_wraparound_status("grocery", 50);
  ASSERT_TRUE(r.contains("tables"));
  ASSERT_TRUE(r["tables"].is_array());

  const json* zoo = find_table(r["tables"], "index_zoo");
  ASSERT_NE(zoo, nullptr) << r["tables"].dump(2);
  // The fixture sets autovacuum_freeze_max_age on this table, so the effective
  // limit must come from reloptions rather than the server-wide setting.
  EXPECT_EQ((*zoo)["freeze_max_age"].get<long long>(), 100000000LL);
  EXPECT_EQ((*zoo)["freeze_max_age_source"].get<std::string>(), "reloptions");

  const json* users = find_table(r["tables"], "users");
  ASSERT_NE(users, nullptr);
  EXPECT_EQ((*users)["freeze_max_age_source"].get<std::string>(), "server");
  EXPECT_EQ((*users)["kind"].get<std::string>(), "table");
  EXPECT_TRUE((*users)["toast_for"].is_null());

  // relallfrozen and the cumulative vacuum timings are PostgreSQL 18+. They
  // turn an age into an estimate of the work left: an old relfrozenxid on a
  // table that is already fully frozen is a different problem entirely.
  const bool pg18 = pg_server_version_num(test_url) >= 180000;
  EXPECT_EQ(users->contains("frozen_percent"), pg18);
  EXPECT_EQ(users->contains("relallfrozen"), pg18);
  EXPECT_EQ(users->contains("total_autovacuum_time_ms"), pg18);
}

TEST_F(PostgresMCPServerTest, WraparoundStatusIncludesToastTables) {
  // A TOAST table carries its own relfrozenxid and is invisible in
  // pg_stat_user_tables, yet is frequently the relation holding the horizon
  // back; leaving it out would understate the risk.
  json r = srv->call_wraparound_status("", 5000);
  bool found_toast = false;
  for (const auto& t : r["tables"]) {
    if (t.value("kind", "") == "toast table") {
      found_toast = true;
      EXPECT_FALSE(t["toast_for"].is_null());
      break;
    }
  }
  EXPECT_TRUE(found_toast);
}

TEST_F(PostgresMCPServerTest, WraparoundStatusHonoursLimit) {
  json r = srv->call_wraparound_status("", 3);
  EXPECT_LE(r["tables"].size(), 3u);
}

// --- duplicateIndexes ---

namespace {

// Names of every index in an 'identical' group that contains `name`.
std::vector<std::string> identical_group_of(const json& r, const std::string& name) {
  for (const auto& g : r["identical"]) {
    std::vector<std::string> names;
    for (const auto& i : g["indexes"]) names.push_back(i["name"].get<std::string>());
    if (std::find(names.begin(), names.end(), name) != names.end()) return names;
  }
  return {};
}

bool is_reported_redundant(const json& r, const std::string& name) {
  for (const auto& e : r["redundant"])
    if (e["index"].get<std::string>() == name) return true;
  return false;
}

}  // namespace

TEST_F(PostgresMCPServerTest, DuplicateIndexesGroupsExactDuplicates) {
  json r = srv->call_duplicate_indexes("grocery", "index_zoo");
  ASSERT_TRUE(r.contains("identical")) << r.dump(2);

  auto group = identical_group_of(r, "zoo_a");
  ASSERT_EQ(group.size(), 2u) << r["identical"].dump(2);
  EXPECT_NE(std::find(group.begin(), group.end(), "zoo_a_again"), group.end());
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesGroupsIdenticalExpressionIndexes) {
  // Expression columns have attnum 0 in indkey, so a detector comparing indkey
  // alone would call any two expression indexes duplicates. These two really
  // are duplicates, and the next test proves two different ones are not.
  json r = srv->call_duplicate_indexes("grocery", "index_zoo");
  auto group = identical_group_of(r, "zoo_lower_c");
  ASSERT_EQ(group.size(), 2u) << r["identical"].dump(2);
  EXPECT_NE(std::find(group.begin(), group.end(), "zoo_lower_c2"), group.end());
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesReportsPrefixRedundancy) {
  json r = srv->call_duplicate_indexes("grocery", "index_zoo");
  ASSERT_TRUE(r.contains("redundant")) << r.dump(2);

  bool found = false;
  for (const auto& e : r["redundant"]) {
    if (e["index"].get<std::string>() == "zoo_a") {
      found = true;
      EXPECT_EQ(e["covered_by"].get<std::string>(), "zoo_a_b");
      EXPECT_NE(e["reason"].get<std::string>().find("leading prefix"), std::string::npos);
      EXPECT_TRUE(e.contains("size"));
      EXPECT_TRUE(e.contains("idx_scan"));
      EXPECT_TRUE(e["valid"].get<bool>());
      break;
    }
  }
  EXPECT_TRUE(found) << r["redundant"].dump(2);
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesSkipsDifferentSortOrder) {
  // (a DESC) cannot serve a query that wants (a, b) in ascending order, so it
  // is not covered by zoo_a_b however much the column list suggests otherwise.
  json r = srv->call_duplicate_indexes("grocery", "index_zoo");
  EXPECT_FALSE(is_reported_redundant(r, "zoo_a_desc")) << r["redundant"].dump(2);
  EXPECT_TRUE(identical_group_of(r, "zoo_a_desc").empty());
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesSkipsPartialIndex) {
  // A partial index covers a different row set entirely.
  json r = srv->call_duplicate_indexes("grocery", "index_zoo");
  EXPECT_FALSE(is_reported_redundant(r, "zoo_a_part")) << r["redundant"].dump(2);
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesNeverCallsAUniqueIndexRedundant) {
  // zoo_c_uq is a prefix of zoo_c_d_uq, but dropping it would drop the
  // uniqueness constraint on c, which the wider index does not enforce.
  json r = srv->call_duplicate_indexes("grocery", "index_zoo");
  EXPECT_FALSE(is_reported_redundant(r, "zoo_c_uq")) << r["redundant"].dump(2);
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesFindsNothingOnACleanTable) {
  json r = srv->call_duplicate_indexes("grocery", "users");
  EXPECT_TRUE(r["identical"].empty()) << r["identical"].dump(2);
  EXPECT_TRUE(r["redundant"].empty()) << r["redundant"].dump(2);
}

TEST_F(PostgresMCPServerTest, DuplicateIndexesWholeSchemaFindsTheZooTable) {
  json r = srv->call_duplicate_indexes("grocery", "");
  bool found = false;
  for (const auto& g : r["identical"])
    if (g["table"].get<std::string>() == "grocery.index_zoo") found = true;
  EXPECT_TRUE(found) << r["identical"].dump(2);
}

// --- checkpointStats ---

TEST_F(PostgresMCPServerTest, CheckpointStatsNormalizesAcrossServerVersions) {
  json r = srv->call_checkpoint_stats();
  ASSERT_TRUE(r.contains("source")) << r.dump(2);

  // Whichever view supplied them, the caller sees the same field names.
  ASSERT_TRUE(r.contains("checkpointer"));
  auto& c = r["checkpointer"];
  EXPECT_TRUE(c.contains("checkpoints_timed"));
  EXPECT_TRUE(c.contains("checkpoints_requested"));
  EXPECT_TRUE(c.contains("checkpoints_total"));
  EXPECT_TRUE(c.contains("timed_percent"));
  EXPECT_TRUE(c.contains("write_time_ms"));
  EXPECT_TRUE(c.contains("sync_time_ms"));
  EXPECT_TRUE(c.contains("buffers_written"));

  ASSERT_TRUE(r.contains("bgwriter"));
  EXPECT_TRUE(r["bgwriter"].contains("buffers_clean"));
  EXPECT_TRUE(r["bgwriter"].contains("buffers_alloc"));

  ASSERT_TRUE(r.contains("wal"));
  EXPECT_TRUE(r["wal"].contains("wal_records"));
  EXPECT_TRUE(r["wal"].contains("wal_bytes"));
  EXPECT_TRUE(r["wal"].contains("wal_buffers_full"));

  ASSERT_TRUE(r.contains("settings"));
  EXPECT_TRUE(r["settings"].contains("checkpoint_timeout"));
  EXPECT_TRUE(r["settings"].contains("max_wal_size"));
  EXPECT_TRUE(r["settings"].contains("checkpoint_completion_target"));
}

TEST_F(PostgresMCPServerTest, CheckpointStatsReportsBackendWritesWhereverTheyLive) {
  json r = srv->call_checkpoint_stats();
  // Before 17 they are columns of pg_stat_bgwriter; from 17 they moved to
  // pg_stat_io. Either way the count has to be reachable.
  if (pg_server_version_num(test_url) >= 170000) {
    ASSERT_TRUE(r.contains("backend_io")) << r.dump(2);
    ASSERT_TRUE(r["backend_io"].contains("client backend")) << r["backend_io"].dump(2);
    EXPECT_TRUE(r["backend_io"]["client backend"].contains("writes"));
  } else {
    EXPECT_TRUE(r["bgwriter"].contains("buffers_backend")) << r["bgwriter"].dump(2);
    EXPECT_TRUE(r["bgwriter"].contains("buffers_backend_fsync"));
  }
}

// --- tableIOStats ---

TEST_F(PostgresMCPServerTest, TableIOStatsReportsPerObjectHitRatios) {
  json r = srv->call_table_io_stats("grocery", "", 50);
  ASSERT_TRUE(r.contains("grocery.users")) << r.dump(2);
  auto& u = r["grocery.users"];
  EXPECT_TRUE(u.contains("heap_blks_read"));
  EXPECT_TRUE(u.contains("heap_blks_hit"));
  EXPECT_TRUE(u.contains("heap_hit_percent"));
  EXPECT_TRUE(u.contains("idx_blks_read"));
  EXPECT_TRUE(u.contains("idx_hit_percent"));
  EXPECT_TRUE(u.contains("total_hit_percent"));
  EXPECT_TRUE(u.contains("size"));
  // No per-index breakdown for a whole-schema sweep.
  EXPECT_TRUE(u["indexes"].is_null());
}

TEST_F(PostgresMCPServerTest, TableIOStatsAddsPerIndexBreakdownForANamedTable) {
  json r = srv->call_table_io_stats("grocery", "users", 20);
  ASSERT_EQ(r.size(), 1u);
  ASSERT_TRUE(r.contains("grocery.users"));
  auto& idx = r["grocery.users"]["indexes"];
  ASSERT_TRUE(idx.is_object()) << r.dump(2);
  ASSERT_TRUE(idx.contains("users_pkey")) << idx.dump(2);
  EXPECT_TRUE(idx["users_pkey"].contains("idx_blks_read"));
  EXPECT_TRUE(idx["users_pkey"].contains("idx_hit_percent"));
  EXPECT_TRUE(idx["users_pkey"].contains("idx_scan"));
  EXPECT_TRUE(idx["users_pkey"].contains("size"));
}

TEST_F(PostgresMCPServerTest, TableIOStatsRatioIsNullNotZeroWithoutTraffic) {
  // An untouched table has read 0 and hit 0. Reporting 0% would read as "every
  // access missed the cache", which is the opposite of what happened.
  pqxx::connection c(test_url);
  pqxx::work t(c);
  t.exec("CREATE TABLE grocery.never_read (id INT)");
  t.commit();

  json r = srv->call_table_io_stats("grocery", "never_read", 20);
  ASSERT_TRUE(r.contains("grocery.never_read")) << r.dump(2);
  EXPECT_TRUE(r["grocery.never_read"]["heap_hit_percent"].is_null());

  pqxx::work drop(c);
  drop.exec("DROP TABLE grocery.never_read");
  drop.commit();
}

// --- hostCapacity ---

TEST_F(PostgresMCPServerTest, HostCapacitySaysSoWhenNoHardwareWasInjected) {
  json r = srv->call_host_capacity(0, 0, "");
  ASSERT_TRUE(r.contains("host")) << r.dump(2);
  EXPECT_FALSE(r["host"]["configured"].get<bool>());
  ASSERT_TRUE(r.contains("hint"));
  EXPECT_NE(r["hint"].get<std::string>().find("PG_LICHT_HOST_RAM_MB"), std::string::npos);

  // The settings are still readable; only the ratios that need RAM are null.
  ASSERT_TRUE(r["settings"].contains("shared_buffers"));
  EXPECT_TRUE(r["derived"]["shared_buffers_percent_of_ram"].is_null());
  // ... and the ones that don't need it are still computed.
  EXPECT_FALSE(r["derived"]["work_mem_times_max_connections_bytes"].is_null());
}

TEST_F(PostgresMCPServerTest, HostCapacityComputesRatiosFromInjectedHardware) {
  json r = srv->call_host_capacity(4096, 8, "local nvme");
  ASSERT_TRUE(r["host"]["configured"].get<bool>()) << r.dump(2);
  EXPECT_EQ(r["host"]["ram_mb"].get<long long>(), 4096);
  EXPECT_EQ(r["host"]["ram_bytes"].get<long long>(), 4096LL * 1048576);
  EXPECT_EQ(r["host"]["vcpus"].get<int>(), 8);
  EXPECT_EQ(r["host"]["storage"].get<std::string>(), "local nvme");
  EXPECT_EQ(r["host"]["source"].get<std::string>(), "argument");
  EXPECT_FALSE(r.contains("hint"));

  auto& d = r["derived"];
  EXPECT_FALSE(d["shared_buffers_percent_of_ram"].is_null());
  EXPECT_GT(d["shared_buffers_percent_of_ram"].get<double>(), 0.0);
  EXPECT_FALSE(d["max_parallel_workers_per_vcpu"].is_null());
  EXPECT_FALSE(d["committed_worst_case_percent_of_ram"].is_null());
}

TEST_F(PostgresMCPServerTest, HostCapacityResolvesByteUnitsPerSetting) {
  json r = srv->call_host_capacity(0, 0, "");
  auto& s = r["settings"];
  // shared_buffers is counted in 8kB pages, work_mem in kB; both must come
  // back as bytes so a caller never has to know which.
  ASSERT_TRUE(s["shared_buffers"]["bytes"].is_number());
  EXPECT_EQ(s["shared_buffers"]["bytes"].get<long long>(),
            s["shared_buffers"]["setting"].get<std::string>().empty()
              ? 0 : std::stoll(s["shared_buffers"]["setting"].get<std::string>()) * 8192);
  ASSERT_TRUE(s["work_mem"]["bytes"].is_number());
  EXPECT_EQ(s["work_mem"]["bytes"].get<long long>(),
            std::stoll(s["work_mem"]["setting"].get<std::string>()) * 1024);
  // max_connections has no unit, so it has no byte count -- null, not zero.
  EXPECT_TRUE(s["max_connections"]["bytes"].is_null());
  // A "-1" setting means "derived from another GUC", not a negative size.
  if (s.contains("autovacuum_work_mem") &&
      s["autovacuum_work_mem"]["setting"].get<std::string>() == "-1") {
    EXPECT_TRUE(s["autovacuum_work_mem"]["bytes"].is_null());
  }
}

// --- explainQuery ---

TEST_F(PostgresMCPServerTest, ExplainQueryReturnsAPlan) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users", json::array(), false, 0);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_FALSE(r["generic"].get<bool>());
  EXPECT_FALSE(r["analyzed"].get<bool>());
  EXPECT_TRUE(r["read_only"].get<bool>());
  EXPECT_EQ(r["source"].get<std::string>(), "sql");
  ASSERT_TRUE(r["plan"].is_array());
  EXPECT_TRUE(r["plan"][0].contains("Plan"));
}

TEST_F(PostgresMCPServerTest, ExplainQueryAnalyzeAddsActualTimings) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users", json::array(), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_TRUE(r["analyzed"].get<bool>());
  EXPECT_TRUE(r["plan"][0].contains("Execution Time"));
  EXPECT_TRUE(r["plan"][0]["Plan"].contains("Actual Total Time"));
}

TEST_F(PostgresMCPServerTest, ExplainQueryAnalyzeRequiresExplicitTimeout) {
  EXPECT_THROW(srv->call_explain_query("", "SELECT 1", json::array(), true, 0),
               std::exception);
}

TEST_F(PostgresMCPServerTest, ExplainQueryPlaceholdersFallBackToGenericPlan) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users WHERE id = $1",
                                   json::array(), false, 0);
  if (pg_server_version_num(test_url) >= 160000) {
    ASSERT_TRUE(r.contains("plan")) << r.dump(2);
    EXPECT_TRUE(r["generic"].get<bool>());
    EXPECT_FALSE(r["analyzed"].get<bool>());
  } else {
    // GENERIC_PLAN is PG16+; older servers return an actionable hint instead.
    ASSERT_TRUE(r.contains("error")) << r.dump(2);
    EXPECT_TRUE(r.contains("hint"));
  }
}

TEST_F(PostgresMCPServerTest, ExplainQueryGenericPlanIsNeverAnalyzed) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users WHERE id = $1",
                                   json::array(), true, 5000);
  if (pg_server_version_num(test_url) >= 160000) {
    EXPECT_TRUE(r["generic"].get<bool>());
    EXPECT_FALSE(r["analyzed"].get<bool>());
    ASSERT_TRUE(r.contains("note"));
  } else {
    ASSERT_TRUE(r.contains("error")) << r.dump(2);
    EXPECT_TRUE(r.contains("hint"));
  }
}

TEST_F(PostgresMCPServerTest, ExplainQueryWithParamsGivesRealPlanAndAnalyzes) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users WHERE id = $1",
                                   json::array({1}), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_FALSE(r["generic"].get<bool>());
  EXPECT_TRUE(r["analyzed"].get<bool>());
  EXPECT_TRUE(r["plan"][0].contains("Execution Time"));
}

// The write gate: a plan is inspected for ModifyTable before anything runs.
TEST_F(PostgresMCPServerTest, ExplainQueryRefusesToAnalyzeUpdate) {
  json r = srv->call_explain_query(
    "", "UPDATE grocery.users SET name = 'zzz' WHERE id = 1", json::array(), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_FALSE(r["read_only"].get<bool>());
  EXPECT_FALSE(r["analyzed"].get<bool>());
  ASSERT_TRUE(r.contains("note"));
  json chk = srv->call_check_key("grocery", "users", json::array({1}));
  EXPECT_TRUE(chk["exists"].get<bool>());
}

// The case a root-node-only check would miss: the ModifyTable is nested under
// a CTE subplan.
TEST_F(PostgresMCPServerTest, ExplainQueryRefusesToAnalyzeDataModifyingCTE) {
  json r = srv->call_explain_query(
    "", "WITH d AS (DELETE FROM grocery.users WHERE id = 2 RETURNING *) SELECT * FROM d",
    json::array(), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_FALSE(r["read_only"].get<bool>());
  EXPECT_FALSE(r["analyzed"].get<bool>());
  json chk = srv->call_check_key("grocery", "users", json::array({2}));
  EXPECT_TRUE(chk["exists"].get<bool>());
}

TEST_F(PostgresMCPServerTest, ExplainQueryRejectsUtilityStatements) {
  EXPECT_THROW(srv->call_explain_query("", "SET work_mem = '4MB'", json::array(), false, 0),
               std::exception);
  EXPECT_THROW(srv->call_explain_query("", "VACUUM grocery.users", json::array(), false, 0),
               std::exception);
  EXPECT_THROW(srv->call_explain_query("", "CREATE TABLE t(i int)", json::array(), false, 0),
               std::exception);
}

TEST_F(PostgresMCPServerTest, ExplainQueryRejectsMultipleStatements) {
  EXPECT_THROW(srv->call_explain_query("", "SELECT 1; SELECT 2", json::array(), false, 0),
               std::exception);
}

TEST_F(PostgresMCPServerTest, ExplainQueryAcceptsATrailingSemicolon) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users;", json::array(), false, 0);
  EXPECT_TRUE(r.contains("plan")) << r.dump(2);
}

TEST_F(PostgresMCPServerTest, ExplainQueryRejectsBothOrNeitherSource) {
  EXPECT_THROW(srv->call_explain_query("", "", json::array(), false, 0), std::exception);
  EXPECT_THROW(srv->call_explain_query("123", "SELECT 1", json::array(), false, 0),
               std::exception);
}

TEST_F(PostgresMCPServerTest, ExplainQuerySyntaxErrorReturnsHint) {
  json r = srv->call_explain_query("", "SELECT FROM WHERE", json::array(), false, 0);
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_TRUE(r.contains("hint"));
}

// Values reach EXECUTE as SQL text, so escaping is the only thing standing
// between a parameter and statement injection.
TEST_F(PostgresMCPServerTest, ExplainQueryParamsAreNotInjectable) {
  json r = srv->call_explain_query(
    "", "SELECT * FROM grocery.users WHERE name = $1",
    json::array({"x'); DROP TABLE grocery.users; --"}), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_TRUE(r["analyzed"].get<bool>());
  json chk = srv->call_check_key("grocery", "users", json::array({1}));
  EXPECT_TRUE(chk["exists"].get<bool>());
}

TEST_F(PostgresMCPServerTest, ExplainQueryNullParamBindsAsSqlNull) {
  json r = srv->call_explain_query("", "SELECT * FROM grocery.users WHERE name = $1",
                                   json::array({nullptr}), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_TRUE(r["analyzed"].get<bool>());
}

TEST_F(PostgresMCPServerTest, ExplainQueryWrongParamCountThrows) {
  EXPECT_THROW(srv->call_explain_query("", "SELECT * FROM grocery.users WHERE id = $1",
                                       json::array({1, 2}), false, 0), std::exception);
}

TEST_F(PostgresMCPServerTest, ExplainQueryTimeoutIsClamped) {
  json r = srv->call_explain_query("", "SELECT 1", json::array(), true, 999999);
  EXPECT_EQ(r["timeout_ms"].get<int>(), 30000);
  json r2 = srv->call_explain_query("", "SELECT 1", json::array(), false, 0);
  EXPECT_EQ(r2["timeout_ms"].get<int>(), 5000);
}

TEST_F(PostgresMCPServerTest, ExplainQueryTimeoutFiresOnSlowAnalyze) {
  json r = srv->call_explain_query("", "SELECT pg_sleep(5)", json::array(), true, 200);
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("timeout"), std::string::npos);
  EXPECT_TRUE(r.contains("hint"));
}

// Prepared statements are session state and survive ROLLBACK, so a leak would
// accumulate on a reused connection. Two identical calls must both succeed.
TEST_F(PostgresMCPServerTest, ExplainQueryLeavesNoPreparedStatementBehind) {
  for (int i = 0; i < 3; i++) {
    json r = srv->call_explain_query("", "SELECT * FROM grocery.users WHERE id = $1",
                                     json::array({1}), true, 5000);
    ASSERT_TRUE(r["analyzed"].get<bool>()) << r.dump(2);
  }
}

TEST_F(PostgresMCPServerTest, ExplainQueryRejectsNonNumericQueryId) {
  EXPECT_THROW(srv->call_explain_query("abc", "", json::array(), false, 0), std::exception);
  EXPECT_THROW(srv->call_explain_query("1; DROP TABLE x", "", json::array(), false, 0),
               std::exception);
}

TEST_F(PostgresMCPServerTest, ExplainQueryByQueryIdReportsMissingExtension) {
  // pg_stat_statements is per-database and the test database is created fresh,
  // so the extension is absent here -- the same reason
  // StatementStatsReturnsHintWhenNotInstalled passes.
  json r = srv->call_explain_query("123456789", "", json::array(), false, 0);
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"].get<std::string>(), "pg_stat_statements is not installed");
  EXPECT_TRUE(r.contains("hint"));
}

// --- explainQuery against a real pg_stat_statements ---
//
// The main fixture's database deliberately has no pg_stat_statements (which is
// what StatementStatsReturnsHintWhenNotInstalled asserts), so the queryid path
// gets its own database and creates the extension there. It skips when the
// library is not preloaded, so this still passes on a server without it.
//
// This exists because both bugs found in the queryid path were null fields
// coming back from pg_stat_statements, which no amount of sql-argument testing
// would have caught.
class PgssMCPServerTest : public ::testing::Test {
protected:
  static pqxx::connection* admin;
  static PostgresMCPServer* srv;
  static std::string dbname;
  static std::string url;
  static bool available;

  static void SetUpTestSuite() {
    const char* env_url = std::getenv("DATABASE_URL");
    std::string base = env_url;
    dbname = "pg_licht_pgss_" + std::to_string(getpid());

    try {
      admin = new pqxx::connection(base);
      {
        pqxx::nontransaction n(*admin);
        n.exec("CREATE DATABASE \"" + dbname + "\"");
      }

      std::regex dbname_re(R"(\bdbname\s*=\s*\S+)");
      if (std::regex_search(base, dbname_re))
        url = std::regex_replace(base, dbname_re, "dbname=" + dbname);
      else
        url = base + " dbname=" + dbname;

      pqxx::connection c(url);
      {
        // In a schema of its own, off the default search_path, for the same
        // reason as pgstattuple in the main fixture: statementStats and
        // explainQuery's queryid path have to find the view by catalog
        // lookup, not by hoping it is in public.
        pqxx::work t(c);
        t.exec("CREATE SCHEMA extensions");
        t.exec("CREATE EXTENSION pg_stat_statements SCHEMA extensions");
        t.commit();
      }
      {
        // CREATE EXTENSION succeeds even when the library is not in
        // shared_preload_libraries (e.g. the stock postgres Docker image), but
        // *reading* the view then fails at runtime with "must be loaded via
        // shared_preload_libraries". Probe the view itself so the suite skips
        // in that case instead of failing every test body.
        pqxx::work t(c);
        t.exec("SELECT 1 FROM extensions.pg_stat_statements LIMIT 1");
        t.commit();
      }
      {
        // Generate a statement with a placeholder so the recovered text is
        // normalised, which is the case the tool has to cope with.
        pqxx::work t(c);
        t.exec("SELECT count(*) FROM pg_class WHERE relname LIKE 'pg_%'");
        t.commit();
      }
      srv = new PostgresMCPServer(url);
      available = true;
    } catch (const std::exception&) {
      // Most likely pg_stat_statements is not in shared_preload_libraries.
      available = false;
    }
  }

  static void TearDownTestSuite() {
    delete srv; srv = nullptr;
    if (admin) {
      try {
        pqxx::nontransaction n(*admin);
        n.exec("DROP DATABASE IF EXISTS \"" + dbname + "\" WITH (FORCE)");
      } catch (const std::exception&) {}
      delete admin; admin = nullptr;
    }
  }

  void SetUp() override {
    if (!available)
      GTEST_SKIP() << "pg_stat_statements unavailable (not in shared_preload_libraries)";
  }

  // The queryid of the seeded statement, as a decimal string.
  static std::string seeded_queryid() {
    pqxx::connection c(url);
    pqxx::work t(c);
    pqxx::result r = t.exec(
      "SELECT queryid::text FROM extensions.pg_stat_statements "
      "WHERE query ILIKE 'SELECT count(*) FROM pg_class%' "
      "AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) "
      "ORDER BY total_exec_time DESC LIMIT 1");
    return r.empty() ? std::string{} : r[0][0].as<std::string>();
  }
};

pqxx::connection* PgssMCPServerTest::admin = nullptr;
PostgresMCPServer* PgssMCPServerTest::srv = nullptr;
std::string PgssMCPServerTest::dbname;
std::string PgssMCPServerTest::url;
bool PgssMCPServerTest::available = false;

TEST_F(PgssMCPServerTest, RecoversStatementByQueryIdAndPlansItGenerically) {
  std::string qid = seeded_queryid();
  ASSERT_FALSE(qid.empty());

  json r = srv->call_explain_query(qid, "", json::array(), false, 0);
  if (pg_server_version_num(url) >= 160000) {
    ASSERT_TRUE(r.contains("plan")) << r.dump(2);
    EXPECT_EQ(r["source"].get<std::string>(), "pg_stat_statements");
    // The recovered text is normalised, so it can only be planned generically.
    EXPECT_TRUE(r["generic"].get<bool>());
    EXPECT_NE(r["sql"].get<std::string>().find("$1"), std::string::npos);
  } else {
    // GENERIC_PLAN is PG16+; the normalized statement can't be planned without
    // params, but the recovered stats still come back.
    ASSERT_TRUE(r.contains("error")) << r.dump(2);
    EXPECT_TRUE(r.contains("hint"));
  }
  ASSERT_TRUE(r.contains("statement"));
  EXPECT_GT(r["statement"]["calls"].get<long long>(), 0);
}

TEST_F(PgssMCPServerTest, ParamsTurnARecoveredStatementIntoARealAnalyzedPlan) {
  std::string qid = seeded_queryid();
  ASSERT_FALSE(qid.empty());

  json r = srv->call_explain_query(qid, "", json::array({"pg_%"}), true, 5000);
  ASSERT_TRUE(r.contains("plan")) << r.dump(2);
  EXPECT_FALSE(r["generic"].get<bool>());
  EXPECT_TRUE(r["analyzed"].get<bool>());
  EXPECT_TRUE(r["plan"][0].contains("Execution Time"));
}

TEST_F(PgssMCPServerTest, StatementStatsCarriesEvictionInfoAlongsideTheRows) {
  json r = srv->call_statement_stats(5);
  ASSERT_TRUE(r.is_object()) << r.dump(2);
  ASSERT_TRUE(r.contains("statements"));
  ASSERT_TRUE(r["statements"].is_array());
  ASSERT_FALSE(r["statements"].empty());

  // dealloc is what says whether this list is "the slowest queries" or only
  // "the slowest of those not yet evicted" -- a distinction that cannot be
  // drawn from the rows themselves.
  ASSERT_TRUE(r.contains("info")) << r.dump(2);
  EXPECT_TRUE(r["info"].contains("dealloc"));
  EXPECT_TRUE(r["info"].contains("stats_reset"));
  EXPECT_TRUE(r.contains("max"));
}

TEST_F(PgssMCPServerTest, StatementStatsRanksByTheRequestedColumn) {
  json by_calls = srv->call_statement_stats(50, "", "calls", 0);
  ASSERT_TRUE(by_calls.contains("statements")) << by_calls.dump(2);
  EXPECT_EQ(by_calls["order_by"].get<std::string>(), "calls");

  long long prev = -1;
  for (const auto& s : by_calls["statements"]) {
    long long c = s["calls"].get<long long>();
    if (prev >= 0) { EXPECT_LE(c, prev) << by_calls.dump(2); }
    prev = c;
  }

  // Ranking by total time buries a statement called twice at 40s under one
  // called ten million times at 2ms; mean_exec_time is the other question,
  // and it must produce a genuinely different ordering key.
  json by_mean = srv->call_statement_stats(50, "", "mean_exec_time", 0);
  EXPECT_EQ(by_mean["order_by"].get<std::string>(), "mean_exec_time");
  double prev_mean = -1;
  for (const auto& s : by_mean["statements"]) {
    double m = s["mean_exec_ms"].get<double>();
    if (prev_mean >= 0) { EXPECT_LE(m, prev_mean + 1e-9); }
    prev_mean = m;
  }
}

TEST_F(PgssMCPServerTest, StatementStatsRejectsAnUnknownOrderByColumn) {
  // The sort column cannot be a bind parameter, so it is resolved through a
  // fixed table. Anything else must be refused rather than reach the SQL.
  try {
    srv->call_statement_stats(5, "", "1; DROP TABLE x", 0);
    FAIL() << "expected an unknown order_by to be rejected";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("unknown order_by"), std::string::npos);
    EXPECT_NE(msg.find("mean_exec_time"), std::string::npos) << msg;
  }
}

TEST_F(PgssMCPServerTest, StatementStatsFiltersByQueryIdAndReturnsFullText) {
  json all = srv->call_statement_stats(200, "", "", 0);
  ASSERT_FALSE(all["statements"].empty());
  std::string qid = all["statements"][0]["query_id"].get<std::string>();

  // Comparing row counts is not the property under test, and it breaks as soon
  // as both fetches hit the limit: what filtering has to do is narrow the
  // result to one queryid, however many (user, database) rows carry it --
  // pg_stat_statements keeps one entry per pair, so one queryid can
  // legitimately match several.
  std::set<std::string> all_ids;
  for (const auto& s : all["statements"]) all_ids.insert(s["query_id"].get<std::string>());
  ASSERT_GT(all_ids.size(), 1u) << "nothing to narrow from";

  json one = srv->call_statement_stats(200, qid, "", 0);
  ASSERT_FALSE(one["statements"].empty()) << one.dump(2);
  std::set<std::string> one_ids;
  for (const auto& s : one["statements"]) one_ids.insert(s["query_id"].get<std::string>());
  EXPECT_EQ(one_ids.size(), 1u) << one.dump(2);
  EXPECT_EQ(*one_ids.begin(), qid);
  EXPECT_LE(one["statements"].size(), all["statements"].size());
}

TEST_F(PgssMCPServerTest, StatementStatsMinCallsExcludesOneOffStatements) {
  json r = srv->call_statement_stats(50, "", "mean_exec_time", 1000000000LL);
  // Nothing in a fresh test database has been called a billion times.
  EXPECT_TRUE(r["statements"].empty()) << r.dump(2);
}

TEST_F(PgssMCPServerTest, StatementStatsQueryIdIsTextAndFeedsExplainQuery) {
  json r = srv->call_statement_stats(5);
  ASSERT_FALSE(r["statements"].empty());
  const json& first = r["statements"][0];

  // A queryid is 64-bit. Emitted as a JSON number it would be rounded by any
  // client that parses numbers as doubles, and the round-tripped value would
  // then not be found by explainQuery.
  ASSERT_TRUE(first["query_id"].is_string()) << first.dump(2);
  std::string qid = first["query_id"].get<std::string>();

  json e = srv->call_explain_query(qid, "", json::array(), false, 0);
  // Whatever the outcome, it must not be "no entry for that queryid": the id
  // came straight out of the same view a moment ago.
  if (e.contains("error")) {
    EXPECT_EQ(e["error"].get<std::string>().find("no pg_stat_statements entry"),
              std::string::npos) << e.dump(2);
  }
}

TEST_F(PgssMCPServerTest, UnknownQueryIdReturnsHint) {
  json r = srv->call_explain_query("987654321987654321", "", json::array(), false, 0);
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("no pg_stat_statements entry"),
            std::string::npos);
  EXPECT_TRUE(r.contains("hint"));
}

// pg_stat_statements keeps entries for databases that have since been dropped,
// so the LEFT JOIN to pg_database yields a null name. Reading that with
// json::value() throws; this is the regression test for that.
TEST_F(PgssMCPServerTest, QueryIdFromADroppedDatabaseIsReportedNotCrashed) {
  std::string victim = dbname + "_victim";
  {
    pqxx::nontransaction n(*admin);
    n.exec("CREATE DATABASE \"" + victim + "\"");
  }
  std::string victim_url = std::regex_replace(
    url, std::regex(R"(\bdbname\s*=\s*\S+)"), "dbname=" + victim);
  {
    pqxx::connection c(victim_url);
    pqxx::work t(c);
    // The marker must be an identifier, not a literal: pg_stat_statements
    // normalises literals away, so a distinctive string constant would not
    // survive to be searched for.
    t.exec("SELECT count(*) AS zzz_victim_marker FROM pg_class");
    t.commit();
  }
  std::string qid;
  {
    pqxx::connection c(url);
    pqxx::work t(c);
    pqxx::result r = t.exec(
      "SELECT queryid::text FROM extensions.pg_stat_statements "
      "WHERE query ILIKE '%zzz_victim_marker%' ORDER BY total_exec_time DESC LIMIT 1");
    if (!r.empty()) qid = r[0][0].as<std::string>();
  }
  {
    pqxx::nontransaction n(*admin);
    n.exec("DROP DATABASE \"" + victim + "\" WITH (FORCE)");
  }
  if (qid.empty()) GTEST_SKIP() << "could not seed a foreign-database statement";

  json r = srv->call_explain_query(qid, "", json::array(), false, 0);
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_NE(r["error"].get<std::string>().find("no longer exists"), std::string::npos);
  EXPECT_TRUE(r.contains("hint"));
}

// --- connection config ---

namespace {

// Writes an INI to a unique temp path with the given mode and removes it on
// destruction, so permission-sensitive tests cannot leak state between runs.
class TempIni {
public:
  TempIni(const std::string& body, mode_t mode = 0600) {
    std::ostringstream p;
    p << "/tmp/pg_licht_test_" << ::getpid() << "_" << (counter_++) << ".ini";
    path_ = p.str();
    std::ofstream out(path_);
    out << body;
    out.close();
    ::chmod(path_.c_str(), mode);
  }
  ~TempIni() { ::unlink(path_.c_str()); }
  const std::string& path() const { return path_; }
private:
  std::string path_;
  static int counter_;
};
int TempIni::counter_ = 0;

pglicht::ConnectionRegistry load(const std::string& body, mode_t mode = 0600) {
  TempIni ini(body, mode);
  return pglicht::ConnectionRegistry::from_ini(ini.path(), "pg-licht-test");
}

}  // namespace

TEST(ConnectionConfigTest, ParsesNamedSections) {
  auto reg = load("[default]\nhost = localhost\nport = 5432\ndbname = one\n"
                  "\n[other]\ndbname = two\nuser = bob\n");
  EXPECT_EQ(reg.default_name(), "default");
  ASSERT_EQ(reg.names().size(), 2u);
  EXPECT_EQ(reg.get("other").dbname, "two");
  EXPECT_EQ(reg.get("other").user, "bob");
  EXPECT_NE(reg.get("default").conninfo.find("dbname=one"), std::string::npos);
}

TEST(ConnectionConfigTest, AppendsApplicationName) {
  auto reg = load("[default]\ndbname = one\n");
  EXPECT_NE(reg.get("default").conninfo.find("application_name=pg-licht-test"),
            std::string::npos);
}

TEST(ConnectionConfigTest, RespectsExplicitApplicationName) {
  auto reg = load("[default]\ndbname = one\napplication_name = custom\n");
  const std::string& ci = reg.get("default").conninfo;
  EXPECT_NE(ci.find("application_name=custom"), std::string::npos);
  EXPECT_EQ(ci.find("pg-licht-test"), std::string::npos);
}

TEST(ConnectionConfigTest, ServiceAloneIsValid) {
  auto reg = load("[prod]\nservice = mysvc\n");
  EXPECT_EQ(reg.get("prod").service, "mysvc");
  EXPECT_NE(reg.get("prod").conninfo.find("service=mysvc"), std::string::npos);
}

TEST(ConnectionConfigTest, ServiceCombinesWithExplicitKeys) {
  auto reg = load("[staging]\nservice = mysvc\ndbname = override\n");
  EXPECT_EQ(reg.get("staging").service, "mysvc");
  EXPECT_EQ(reg.get("staging").dbname, "override");
}

TEST(ConnectionConfigTest, SectionWithNeitherServiceNorDbnameIsRejected) {
  // The section name must appear in the message, so the operator knows which
  // one to fix rather than getting a bare libpq failure at connect time.
  try {
    load("[broken]\nhost = localhost\n");
    FAIL() << "expected a rejection";
  } catch (const std::exception& e) {
    EXPECT_NE(std::string(e.what()).find("broken"), std::string::npos);
  }
}

TEST(ConnectionConfigTest, GroupReadableFileIsRejected) {
  try {
    load("[default]\ndbname = one\npassword = hunter2\n", 0644);
    FAIL() << "expected a permission rejection";
  } catch (const std::exception& e) {
    EXPECT_NE(std::string(e.what()).find("chmod 600"), std::string::npos);
  }
}

TEST(ConnectionConfigTest, OptionsKeyIsRejectedWithPgBouncerExplanation) {
  // PgBouncer refuses GUCs in the startup packet, so this would fail only at
  // connect time against the pooler this server targets.
  try {
    load("[default]\ndbname = one\noptions = -c statement_timeout=1000\n");
    FAIL() << "expected 'options' to be rejected";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("PgBouncer"), std::string::npos);
    // Rejecting the only obvious workaround obliges the message to name the
    // supported one. Until 3.2.1 it claimed a SET LOCAL statement_timeout that
    // only explainQuery ever issued, which sent the reader looking for a
    // setting that was not being applied.
    EXPECT_NE(msg.find("statement_timeout_ms"), std::string::npos) << msg;
  }
}

TEST(ConnectionConfigTest, UnknownConnectionNameListsConfiguredOnes) {
  auto reg = load("[default]\ndbname = one\n\n[other]\ndbname = two\n");
  try {
    reg.get("nope");
    FAIL() << "expected unknown connection to throw";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("nope"), std::string::npos);
    EXPECT_NE(msg.find("other"), std::string::npos);
  }
}

TEST(ConnectionConfigTest, EmptyNameResolvesToDefault) {
  auto reg = load("[first]\ndbname = one\n\n[second]\ndbname = two\n");
  // No [default] section, so the first in file order becomes the default.
  EXPECT_EQ(reg.default_name(), "first");
  EXPECT_EQ(reg.get("").dbname, "one");
}

TEST(ConnectionConfigTest, CommentsAndQuotedValuesAreHandled) {
  auto reg = load("; leading comment\n[default]\n# another\n"
                  "dbname = one   ; trailing comment\n"
                  "password = 'has spaces; and #hash'\n");
  EXPECT_EQ(reg.get("default").dbname, "one");
  // A quoted value keeps characters that would otherwise start a comment, and
  // is re-quoted for libpq because it contains whitespace.
  EXPECT_NE(reg.get("default").conninfo.find("'has spaces; and #hash'"),
            std::string::npos);
}

TEST(ConnectionConfigTest, DuplicateSectionIsRejected) {
  EXPECT_THROW(load("[a]\ndbname = one\n[a]\ndbname = two\n"), std::exception);
}

TEST(ConnectionConfigTest, PasswordIsNotRetainedForDisplay) {
  auto reg = load("[default]\ndbname = one\npassword = hunter2\n");
  const auto& c = reg.get("default");
  // It must reach libpq via the conninfo, but never be echoed back.
  EXPECT_NE(c.conninfo.find("password=hunter2"), std::string::npos);
  EXPECT_EQ(c.host, "");
  EXPECT_EQ(c.service, "");
  EXPECT_EQ(c.dbname, "one");
}

TEST(ConnectionConfigTest, FromUrlBuildsASingleDefault) {
  auto reg = pglicht::ConnectionRegistry::from_url("port=5555 dbname=x", "app/1");
  EXPECT_EQ(reg.default_name(), "default");
  EXPECT_EQ(reg.names().size(), 1u);
  EXPECT_NE(reg.get("default").conninfo.find("application_name=app/1"),
            std::string::npos);
}

TEST(ConnectionConfigTest, HostCapacityKeysAreParsedAndKeptOutOfTheConninfo) {
  auto reg = load("[default]\ndbname = one\nhost_ram_mb = 65536\nhost_vcpus = 16\n"
                  "host_storage = local nvme\nhost_note = shared with the app server\n");
  const auto& c = reg.get("default");
  EXPECT_EQ(c.capacity.ram_mb, 65536);
  EXPECT_EQ(c.capacity.vcpus, 16);
  EXPECT_EQ(c.capacity.storage, "local nvme");
  EXPECT_EQ(c.capacity.note, "shared with the app server");
  EXPECT_EQ(c.capacity.source, "config file");
  // libpq would reject the whole conninfo over an unknown keyword, so these
  // must be consumed here rather than passed through.
  EXPECT_EQ(c.conninfo.find("host_ram_mb"), std::string::npos);
  EXPECT_EQ(c.conninfo.find("host_vcpus"), std::string::npos);
  EXPECT_EQ(c.conninfo.find("nvme"), std::string::npos);
}

TEST(ConnectionConfigTest, HostCapacityIsUnsetWhenNotDeclared) {
  auto reg = load("[default]\ndbname = one\n");
  const auto& c = reg.get("default");
  EXPECT_FALSE(c.capacity.configured());
  EXPECT_EQ(c.capacity.ram_mb, 0);
  EXPECT_EQ(c.capacity.source, "");
}

TEST(ConnectionConfigTest, NonNumericHostRamIsRejectedNamingTheSection) {
  // "64GB" where megabytes are meant would be off by three orders of
  // magnitude and silently invalidate every ratio derived from it.
  try {
    load("[prod]\ndbname = one\nhost_ram_mb = 64GB\n");
    FAIL() << "expected a rejection";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("prod"), std::string::npos);
    EXPECT_NE(msg.find("host_ram_mb"), std::string::npos);
    EXPECT_NE(msg.find("64GB"), std::string::npos);
  }
}

TEST(ConnectionConfigTest, ZeroOrNegativeHostVcpusIsRejected) {
  EXPECT_THROW(load("[default]\ndbname = one\nhost_vcpus = 0\n"), std::exception);
  EXPECT_THROW(load("[default]\ndbname = one\nhost_vcpus = -4\n"), std::exception);
}

TEST(ConnectionConfigTest, HostCapacityComesFromTheEnvironmentForASingleConnection) {
  ::setenv("PG_LICHT_HOST_RAM_MB", "8192", 1);
  ::setenv("PG_LICHT_HOST_VCPUS", "4", 1);
  ::setenv("PG_LICHT_HOST_STORAGE", "gp3", 1);
  auto reg = pglicht::ConnectionRegistry::from_url("dbname=x", "app/1");
  ::unsetenv("PG_LICHT_HOST_RAM_MB");
  ::unsetenv("PG_LICHT_HOST_VCPUS");
  ::unsetenv("PG_LICHT_HOST_STORAGE");

  const auto& c = reg.get("default");
  EXPECT_EQ(c.capacity.ram_mb, 8192);
  EXPECT_EQ(c.capacity.vcpus, 4);
  EXPECT_EQ(c.capacity.storage, "gp3");
  EXPECT_EQ(c.capacity.source, "environment");
}

TEST(ConnectionConfigTest, EnvironmentHostCapacityDoesNotLeakIntoTheConninfo) {
  ::setenv("PG_LICHT_HOST_RAM_MB", "8192", 1);
  auto reg = pglicht::ConnectionRegistry::from_url("dbname=x", "app/1");
  ::unsetenv("PG_LICHT_HOST_RAM_MB");
  EXPECT_EQ(reg.get("default").conninfo.find("8192"), std::string::npos);
}

TEST(ConnectionConfigTest, FromUrlLeavesUriFormUntouched) {
  // libpq parses URI forms itself; appending keywords would corrupt them.
  auto reg = pglicht::ConnectionRegistry::from_url("postgresql://h/db", "app/1");
  EXPECT_EQ(reg.get("default").conninfo, "postgresql://h/db");
}

// --- the statement ceiling ---

TEST(ConnectionConfigTest, StatementTimeoutDefaultsWhenNotDeclared) {
  auto reg = load("[default]\ndbname = one\n");
  EXPECT_EQ(reg.get("default").statement_timeout_ms,
            pglicht::kDefaultStatementTimeoutMs);
}

TEST(ConnectionConfigTest, StatementTimeoutIsPerConnectionAndKeptOutOfTheConninfo) {
  // The ceiling that suits a catalog browsed over a WAN link is not the one
  // that suits a deliberate pgstattuple scan, so it is declared per section.
  auto reg = load("[fast]\ndbname = one\nstatement_timeout_ms = 5000\n"
                  "[slow]\ndbname = two\nstatement_timeout_ms = 900000\n");
  EXPECT_EQ(reg.get("fast").statement_timeout_ms, 5000);
  EXPECT_EQ(reg.get("slow").statement_timeout_ms, 900000);
  // statement_timeout is a GUC, not a libpq keyword: passed through, it would
  // make libpq reject the entire conninfo.
  EXPECT_EQ(reg.get("fast").conninfo.find("statement_timeout"), std::string::npos);
  EXPECT_EQ(reg.get("slow").conninfo.find("900000"), std::string::npos);
}

TEST(ConnectionConfigTest, ZeroStatementTimeoutDisablesTheCeiling) {
  // Unlike the capacity keys, 0 is a value here rather than a typo: it is how
  // an operator restores the unbounded behaviour of 3.2.0 and earlier.
  auto reg = load("[default]\ndbname = one\nstatement_timeout_ms = 0\n");
  EXPECT_EQ(reg.get("default").statement_timeout_ms, 0);
}

TEST(ConnectionConfigTest, NonNumericStatementTimeoutIsRejectedNamingTheSection) {
  try {
    load("[prod]\ndbname = one\nstatement_timeout_ms = 30s\n");
    FAIL() << "expected a rejection";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("prod"), std::string::npos);
    EXPECT_NE(msg.find("statement_timeout_ms"), std::string::npos);
    EXPECT_NE(msg.find("30s"), std::string::npos);
  }
}

TEST(ConnectionConfigTest, AnOversizedStatementTimeoutIsRejectedRatherThanNarrowed) {
  // statement_timeout is an int GUC. Narrowing past INT_MAX would turn "wait
  // essentially forever" into a few seconds, which is the one direction an
  // operator must never be surprised in.
  try {
    load("[prod]\ndbname = one\nstatement_timeout_ms = 99999999999\n");
    FAIL() << "expected a rejection";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("2147483647"), std::string::npos) << msg;
    EXPECT_NE(msg.find("prod"), std::string::npos) << msg;
  }
}

TEST(ConnectionConfigTest, StatementTimeoutComesFromTheEnvironmentForASingleConnection) {
  ::setenv("PG_LICHT_STATEMENT_TIMEOUT_MS", "7500", 1);
  auto reg = pglicht::ConnectionRegistry::from_url("dbname=x", "app/1");
  ::unsetenv("PG_LICHT_STATEMENT_TIMEOUT_MS");
  EXPECT_EQ(reg.get("default").statement_timeout_ms, 7500);
  EXPECT_EQ(reg.get("default").conninfo.find("7500"), std::string::npos);
}

TEST(ConnectionConfigTest, StatementTimeoutFallsBackToTheDefaultWithoutTheEnvironment) {
  ::unsetenv("PG_LICHT_STATEMENT_TIMEOUT_MS");
  auto reg = pglicht::ConnectionRegistry::from_url("dbname=x", "app/1");
  EXPECT_EQ(reg.get("default").statement_timeout_ms,
            pglicht::kDefaultStatementTimeoutMs);
}

// --- connection topology ---

TEST(ConnectionTopologyTest, TheShippedExampleFileParses) {
  // The example is documentation that runs: if the parser gains a rule the
  // example violates, this fails rather than shipping a file that would be
  // rejected on the reader's first attempt.
  //
  // Parsed through a copy at mode 0600, not in place. Git records only the
  // executable bit, so a checkout gets whatever the umask gives -- 0644 on a
  // CI runner -- and from_ini refuses a group-readable config on the ~/.pgpass
  // rule. That refusal is correct and has its own test; what this one is about
  // is the content.
  std::ifstream in(PGLICHT_EXAMPLE_INI);
  ASSERT_TRUE(in) << "cannot read " << PGLICHT_EXAMPLE_INI;
  std::stringstream body;
  body << in.rdbuf();
  ASSERT_FALSE(body.str().empty());

  TempIni copy(body.str());
  auto reg = pglicht::ConnectionRegistry::from_ini(copy.path(), "pg-licht-test");

  EXPECT_EQ(reg.names().size(), 5u);
  // Two declared instances plus one inferred from a shared host and port.
  EXPECT_EQ(reg.instances().size(), 3u);
  EXPECT_EQ(reg.replication_groups().size(), 2u);

  EXPECT_EQ(reg.get("app_dev").instance_source, "inferred");
  EXPECT_EQ(reg.get("app_prod").instance_source, "declared");
  // Capacity declared once on the instance reaches its members.
  EXPECT_EQ(reg.get("app_prod").capacity.ram_mb, 65536);
  EXPECT_EQ(reg.get("app_prod").capacity.source, "instance section");
  // A replica on a deliberately smaller box: proof that inheritance follows
  // the instance and not the replication group.
  EXPECT_EQ(reg.get("app_ro").capacity.ram_mb, 16384);

  // A group spans instances, which is what groups are for.
  EXPECT_EQ(reg.members("group", "app").size(), 3u);
}



TEST(ConnectionTopologyTest, ParsesTheThreeAxesAndKeepsThemOutOfTheConninfo) {
  auto reg = load("[billing_prod]\ndbname = billing\ninstance = pg-prod-01\n"
                  "replication_group = billing-ha\ngroup = prod, billing\n");
  const auto& c = reg.get("billing_prod");
  EXPECT_EQ(c.instance, "pg-prod-01");
  EXPECT_EQ(c.instance_source, "declared");
  EXPECT_EQ(c.replication_group, "billing-ha");
  ASSERT_EQ(c.groups.size(), 2u);
  EXPECT_EQ(c.groups[0], "prod");
  EXPECT_EQ(c.groups[1], "billing");
  // libpq rejects the whole conninfo over an unknown keyword, so all three
  // must be consumed rather than passed through.
  EXPECT_EQ(c.conninfo.find("instance"), std::string::npos);
  EXPECT_EQ(c.conninfo.find("replication_group"), std::string::npos);
  EXPECT_EQ(c.conninfo.find("group"), std::string::npos);
}

TEST(ConnectionTopologyTest, IndexesMembersInFileOrder) {
  auto reg = load("[c]\ndbname = c\ninstance = i1\n"
                  "[a]\ndbname = a\ninstance = i1\n"
                  "[b]\ndbname = b\ninstance = i2\n");
  const auto& m = reg.members("instance", "i1");
  ASSERT_EQ(m.size(), 2u);
  EXPECT_EQ(m[0], "c");   // file order, not alphabetical
  EXPECT_EQ(m[1], "a");
  EXPECT_EQ(reg.instances().size(), 2u);
}

TEST(ConnectionTopologyTest, UnknownTopologyNameListsConfiguredOnes) {
  auto reg = load("[a]\ndbname = a\ngroup = prod\n");
  try {
    reg.members("group", "dev");
    FAIL() << "expected unknown group to throw";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("dev"), std::string::npos);
    EXPECT_NE(msg.find("prod"), std::string::npos);
  }
}

TEST(ConnectionTopologyTest, TrailingCommaInAGroupListIsRejected) {
  // Silently dropping it would leave a group the operator believes exists but
  // that no connection is a member of.
  EXPECT_THROW(load("[a]\ndbname = a\ngroup = prod,\n"), std::exception);
  EXPECT_THROW(load("[a]\ndbname = a\ngroup = prod,,dev\n"), std::exception);
}

TEST(ConnectionTopologyTest, RepeatedGroupKeysAccumulateAndDeduplicate) {
  auto reg = load("[a]\ndbname = a\ngroup = prod\ngroup = billing, prod\n");
  const auto& g = reg.get("a").groups;
  ASSERT_EQ(g.size(), 2u);
  EXPECT_EQ(g[0], "prod");
  EXPECT_EQ(g[1], "billing");
}

TEST(ConnectionTopologyTest, RepeatedInstanceKeyIsRejected) {
  EXPECT_THROW(load("[a]\ndbname = a\ninstance = i1\ninstance = i2\n"), std::exception);
  EXPECT_THROW(load("[a]\ndbname = a\nreplication_group = r\nreplication_group = s\n"),
               std::exception);
}

TEST(ConnectionTopologyTest, EmptyTopologyValueIsRejected) {
  EXPECT_THROW(load("[a]\ndbname = a\ninstance =\n"), std::exception);
}

TEST(ConnectionTopologyTest, OneNameMayNotLabelTwoAxes) {
  // An agent passing the right name to the wrong argument would otherwise
  // sweep a different set of databases with nothing in the payload to say so.
  try {
    load("[a]\ndbname = a\ninstance = shared\n"
         "[b]\ndbname = b\ngroup = shared\n");
    FAIL() << "expected a cross-axis rejection";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("shared"), std::string::npos);
    EXPECT_NE(msg.find("instance"), std::string::npos);
    EXPECT_NE(msg.find("group"), std::string::npos);
  }
}

TEST(ConnectionTopologyTest, InstanceSectionIsNotAConnectionEvenWhenWrittenFirst) {
  // order_ used to take every section, so a reserved section placed first
  // would have become the default connection.
  auto reg = load("[instance:pg-01]\nhost_ram_mb = 65536\n"
                  "[app]\ndbname = app\ninstance = pg-01\n");
  EXPECT_EQ(reg.default_name(), "app");
  ASSERT_EQ(reg.names().size(), 1u);
  EXPECT_EQ(reg.names()[0], "app");
  EXPECT_THROW(reg.get("instance:pg-01"), std::exception);
}

TEST(ConnectionTopologyTest, InstanceCapacityIsInheritedByMembers) {
  auto reg = load("[instance:pg-01]\nhost_ram_mb = 65536\nhost_vcpus = 16\n"
                  "host_storage = local nvme\n"
                  "[a]\ndbname = a\ninstance = pg-01\n"
                  "[b]\ndbname = b\ninstance = pg-01\n");
  for (const char* n : {"a", "b"}) {
    const auto& c = reg.get(n);
    EXPECT_EQ(c.capacity.ram_mb, 65536) << n;
    EXPECT_EQ(c.capacity.vcpus, 16) << n;
    EXPECT_EQ(c.capacity.storage, "local nvme") << n;
    EXPECT_EQ(c.capacity.source, "instance section") << n;
  }
  EXPECT_EQ(reg.instance_capacity("pg-01").ram_mb, 65536);
}

TEST(ConnectionTopologyTest, ExplicitCapacityWinsOverTheInstanceSection) {
  auto reg = load("[instance:pg-01]\nhost_ram_mb = 65536\nhost_vcpus = 16\n"
                  "[a]\ndbname = a\ninstance = pg-01\nhost_ram_mb = 1024\n");
  const auto& c = reg.get("a");
  EXPECT_EQ(c.capacity.ram_mb, 1024);          // the connection's own value
  EXPECT_EQ(c.capacity.vcpus, 16);             // still inherited
  EXPECT_EQ(c.capacity.source, "config file + instance section");
}

TEST(ConnectionTopologyTest, InstanceSectionRejectsConnectionKeys) {
  try {
    load("[instance:pg-01]\ndbname = oops\n[a]\ndbname = a\ninstance = pg-01\n");
    FAIL() << "expected a rejection";
  } catch (const std::exception& e) {
    std::string msg = e.what();
    EXPECT_NE(msg.find("pg-01"), std::string::npos);
    EXPECT_NE(msg.find("dbname"), std::string::npos);
  }
}

TEST(ConnectionTopologyTest, UnclaimedInstanceSectionIsRejected) {
  // The symptom otherwise is capacity silently missing from every ratio.
  try {
    load("[instance:typo]\nhost_ram_mb = 65536\n"
         "[a]\ndbname = a\ninstance = pg-01\n");
    FAIL() << "expected a rejection";
  } catch (const std::exception& e) {
    EXPECT_NE(std::string(e.what()).find("typo"), std::string::npos);
  }
}

TEST(ConnectionTopologyTest, CapacityIsNotInheritedThroughAnInferredInstance) {
  // Inheritance follows a declared instance only. Here the two connections
  // share an endpoint and would be inferred into "h:5432", but the section
  // naming that endpoint is claimed by nobody -- and is rejected as such.
  EXPECT_THROW(load("[instance:h:5432]\nhost_ram_mb = 65536\n"
                    "[a]\nhost = h\nport = 5432\ndbname = a\n"
                    "[b]\nhost = h\nport = 5432\ndbname = b\n"),
               std::exception);
}

TEST(ConnectionTopologyTest, InstanceIsInferredFromAnIdenticalHostAndPort) {
  auto reg = load("[a]\nhost = h1\nport = 5432\ndbname = a\n"
                  "[b]\nhost = h1\nport = 5432\ndbname = b\n");
  EXPECT_EQ(reg.get("a").instance, "h1:5432");
  EXPECT_EQ(reg.get("a").instance_source, "inferred");
  EXPECT_EQ(reg.members("instance", "h1:5432").size(), 2u);
}

TEST(ConnectionTopologyTest, InferenceNeedsBothHostAndPortAndMoreThanOneMember) {
  // A single connection on an endpoint is not evidence of anything.
  auto lone = load("[a]\nhost = h1\nport = 5432\ndbname = a\n");
  EXPECT_EQ(lone.get("a").instance, "");

  // Many instances can run on one host as long as their ports differ, so the
  // host alone must not group them.
  auto ports = load("[a]\nhost = h1\nport = 5432\ndbname = a\n"
                    "[b]\nhost = h1\nport = 5433\ndbname = b\n");
  EXPECT_EQ(ports.get("a").instance, "");
  EXPECT_EQ(ports.get("b").instance, "");

  // No port at all is not an endpoint.
  auto noport = load("[a]\nhost = h1\ndbname = a\n[b]\nhost = h1\ndbname = b\n");
  EXPECT_EQ(noport.get("a").instance, "");
}

TEST(ConnectionTopologyTest, ServiceConnectionsAreNeverInferred) {
  // The service file is deliberately not expanded, so host and port are
  // unknown here and any grouping would be a guess.
  auto reg = load("[a]\nservice = svc\nhost = h1\nport = 5432\n"
                  "[b]\nservice = svc\nhost = h1\nport = 5432\n");
  EXPECT_EQ(reg.get("a").instance, "");
  EXPECT_EQ(reg.get("b").instance, "");
}

TEST(ConnectionTopologyTest, ADeclaredInstanceIsNeverOverwrittenByInference) {
  auto reg = load("[a]\nhost = h1\nport = 5432\ndbname = a\ninstance = declared\n"
                  "[b]\nhost = h1\nport = 5432\ndbname = b\n");
  EXPECT_EQ(reg.get("a").instance, "declared");
  EXPECT_EQ(reg.get("a").instance_source, "declared");
  EXPECT_EQ(reg.get("b").instance, "");   // one remaining member is not evidence
}

TEST(ConnectionTopologyTest, ConnectionsWithoutTopologyKeepEmptyIndexes) {
  auto reg = load("[a]\ndbname = a\n");
  EXPECT_TRUE(reg.instances().empty());
  EXPECT_TRUE(reg.replication_groups().empty());
  EXPECT_TRUE(reg.groups().empty());
  EXPECT_EQ(reg.get("a").instance_source, "");
}

// --- tool classification and annotations ---

TEST_F(PostgresMCPServerTest, EveryToolIsClassified) {
  // The fan-out rules read the scope table. A tool missing from it would be
  // silently ineligible for every sweep, which is invisible from the outside.
  json tools = srv->call_tools_list();
  ASSERT_FALSE(tools.empty());
  for (const auto& t : tools) {
    const std::string name = t.value("name", "");
    // scope_note() returns "" only for the registry tools; every other tool
    // must have said something about where its answer varies.
    const bool registry = (name == "listConnections" || name == "listTopology" ||
                           name == "verifyTopology");
    const std::string d = t.value("description", "");
    if (!registry) {
      const bool classified =
          d.find("instance-wide") != std::string::npos ||
          d.find("byte-identical here") != std::string::npos ||
          d.find("each server's own") != std::string::npos ||
          d.find("Never runs across more than one connection") != std::string::npos;
      EXPECT_TRUE(classified) << name << " carries no scope note: " << d;
    }
  }
}

TEST_F(PostgresMCPServerTest, LegacyClientsSeeNoAnnotationsOrTitles) {
  // annotations arrived in 2025-03-26 and title in 2025-06-18. Sending either
  // to a 2024-11-05 client would change bytes 3.1.1 promised.
  json tools = srv->call_tools_list("2024-11-05");
  ASSERT_FALSE(tools.empty());
  for (const auto& t : tools) {
    EXPECT_FALSE(t.contains("annotations")) << t.value("name", "");
    EXPECT_FALSE(t.contains("title")) << t.value("name", "");
  }
}

TEST_F(PostgresMCPServerTest, AnnotationsAppearForClientsThatUnderstandThem) {
  json tools = srv->call_tools_list("2025-03-26");
  ASSERT_FALSE(tools.empty());
  int explain_seen = 0;
  for (const auto& t : tools) {
    const std::string name = t.value("name", "");
    ASSERT_TRUE(t.contains("annotations")) << name;
    const json& a = t["annotations"];
    // Honest because every statement runs in a read-only transaction.
    EXPECT_TRUE(a["readOnlyHint"].get<bool>()) << name;
    EXPECT_FALSE(a["destructiveHint"].get<bool>()) << name;
    EXPECT_FALSE(a["openWorldHint"].get<bool>()) << name;
    // title belongs to a later revision and must not appear yet.
    EXPECT_FALSE(t.contains("title")) << name;

    if (name == "explainQuery") {
      explain_seen++;
      // With analyze:true it really executes; the plan is proven free of any
      // ModifyTable node first, so it cannot mutate, but it is not idempotent.
      ASSERT_TRUE(a.contains("idempotentHint")) << t.dump(2);
      EXPECT_FALSE(a["idempotentHint"].get<bool>());
    } else {
      EXPECT_FALSE(a.contains("idempotentHint")) << name;
    }
  }
  EXPECT_EQ(explain_seen, 1);
}

// --- 4.0.0 outputSchema ---

namespace {

// A validator for exactly the JSON Schema subset these declarations use:
// "type" (a name or a list of names) and "properties". Nothing here uses
// "required" and every schema sets additionalProperties true, so conformance
// reduces to "the top level is an object, and every declared property that is
// present has one of its declared types". That is the whole drift risk: a
// field whose type changes under a declared schema turns a working call into a
// client-side validation error, which is what the roadmap warns about.
bool json_type_matches(const json& v, const std::string& t) {
  if (t == "null")    return v.is_null();
  if (t == "object")  return v.is_object();
  if (t == "array")   return v.is_array();
  if (t == "string")  return v.is_string();
  if (t == "boolean") return v.is_boolean();
  if (t == "integer") return v.is_number_integer();
  if (t == "number")  return v.is_number();
  return false;
}

bool conforms(const json& value, const json& schema, std::string& why) {
  if (schema.contains("type")) {
    const json& t = schema["type"];
    bool ok = false;
    if (t.is_string()) ok = json_type_matches(value, t.get<std::string>());
    else for (const auto& one : t) if (json_type_matches(value, one.get<std::string>())) ok = true;
    if (!ok) { why = std::string("expected ") + t.dump() + ", got " + value.type_name(); return false; }
  }
  if (schema.contains("properties") && value.is_object()) {
    for (auto& [key, sub] : schema["properties"].items()) {
      if (!value.contains(key)) continue;   // nothing is required, by design
      std::string inner;
      if (!conforms(value[key], sub, inner)) { why = key + ": " + inner; return false; }
    }
  }
  return true;
}

}  // namespace

TEST_F(PostgresMCPServerTest, EveryToolDeclaresAnOutputSchema) {
  // Declaring one for 55 tools and not the 56th reads to a client as "that one
  // is unstructured", which is worse than declaring none at all.
  json tools = srv->call_tools_list("2025-06-18");
  ASSERT_FALSE(tools.empty());
  for (const auto& t : tools) {
    const std::string name = t.value("name", "");
    ASSERT_TRUE(t.contains("outputSchema")) << name << " declares no outputSchema";
    EXPECT_EQ(t["outputSchema"]["type"], "object") << name;
    // structuredContent may only be an object, so no schema may say otherwise.
    EXPECT_TRUE(t["outputSchema"].value("additionalProperties", false))
        << name << " forbids additional properties, which a catalog will grow";
    EXPECT_FALSE(t["outputSchema"].contains("required"))
        << name << " marks a field required; a version-conditional payload cannot";
  }
}

TEST_F(PostgresMCPServerTest, OutputSchemaStartsAtTheRevisionThatDefinedIt) {
  for (const char* proto : {"2024-11-05", "2025-03-26"})
    for (const auto& t : srv->call_tools_list(proto))
      EXPECT_FALSE(t.contains("outputSchema")) << proto << " " << t.value("name", "");
  for (const auto& t : srv->call_tools_list("2025-06-18"))
    EXPECT_TRUE(t.contains("outputSchema")) << t.value("name", "");
}

TEST_F(PostgresMCPServerTest, RealPayloadsConformToTheirDeclaredSchema) {
  // The test that makes the declarations safe to ship. A schema that drifts
  // from the payload turns a previously-working call into a validation error
  // on the client, and that failure would be invisible from here otherwise.
  json tools = srv->call_tools_list("2025-06-18");
  std::map<std::string, json> schema;
  for (const auto& t : tools) schema[t.value("name", "")] = t["outputSchema"];

  // Negotiate the revision through initialize rather than per-request _meta,
  // so this exercises the same path a real 2025-06-18 client takes.
  PostgresMCPServer modern(test_url);
  modern.call_rpc({{"jsonrpc", "2.0"}, {"id", 0}, {"method", "initialize"},
                   {"params", {{"protocolVersion", "2025-06-18"},
                               {"capabilities", json::object()},
                               {"clientInfo", {{"name", "t"}, {"version", "0"}}}}}});

  const std::vector<std::pair<std::string, json>> calls = {
    {"listSchemas",      json::object()},
    {"listTables",       {{"schema", "grocery"}}},
    {"tableDetails",     {{"schema", "grocery"}, {"table", "users"}}},
    {"tableStats",       {{"schema", "grocery"}, {"table", "users"}}},
    {"tableSize",        {{"schema", "grocery"}, {"table", "users"}}},
    {"listTableStats",   {{"schema", "grocery"}}},
    {"listTableSizes",   {{"schema", "grocery"}}},
    {"searchTables",     {{"web_search", "users"}}},
    {"listFunctions",    {{"schema", "grocery"}}},
    {"listEnums",        {{"schema", "grocery"}}},
    {"listTypes",        {{"schema", "grocery"}}},
    {"listSequences",    {{"schema", "grocery"}}},
    {"listRoles",        json::object()},
    {"listExtensions",   json::object()},
    {"listTablespaces",  json::object()},
    {"listCollations",   json::object()},
    {"listAccessMethods",json::object()},
    {"listLanguages",    json::object()},
    {"listConnections",  json::object()},
    {"listTopology",     json::object()},
    {"currentLocks",     json::object()},
    {"currentActivity",  json::object()},
    {"databaseSize",     json::object()},
    {"databaseStats",    json::object()},
    {"progressStats",    json::object()},
    {"wraparoundStatus", json::object()},
    {"checkpointStats",  json::object()},
    {"hostCapacity",     json::object()},
    {"ioStats",          json::object()},
    {"tableIOStats",     {{"schema", "grocery"}}},
    {"duplicateIndexes", {{"schema", "grocery"}}},
    {"serverSettings",   json::object()},
    {"checkKey",         {{"schema", "grocery"}, {"table", "users"}, {"values", json::array({1})}}},
    {"explainQuery",     {{"sql", "SELECT 1"}}},
  };
  int checked = 0;
  for (const auto& [tool, args] : calls) {
    json r = modern.call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                              {"params", {{"name", tool}, {"arguments", args}}}});
    if (!r["result"].contains("structuredContent")) continue;  // an isError path
    const json& payload = r["result"]["structuredContent"];
    ASSERT_TRUE(schema.count(tool)) << tool;
    std::string why;
    EXPECT_TRUE(conforms(payload, schema[tool], why))
        << tool << " payload violates its declared outputSchema: " << why;
    checked++;
  }
  EXPECT_GE(checked, 30) << "too few tools actually exercised to prove anything";
}

namespace {

const char* kProtoVersion  = "io.modelcontextprotocol/protocolVersion";
const char* kProtoCaps     = "io.modelcontextprotocol/clientCapabilities";
const char* kProtoSrvInfo  = "io.modelcontextprotocol/serverInfo";

json modern_meta(const std::string& version = "2026-07-28") {
  return {{kProtoVersion, version}, {kProtoCaps, json::object()}};
}

}  // namespace

// --- 4.0.0 caching hints and pagination (revision 2026-07-28) ---

TEST_F(PostgresMCPServerTest, CacheableResultsCarryTheirHints) {
  // The spec says servers MUST include caching hints on every result with
  // resultType "complete" from these six operations. tools/list is ~91 kB, and
  // without a hint a client SHOULD treat it as immediately stale and re-fetch
  // it whenever it needs the list.
  const std::vector<std::pair<std::string, json>> cacheable = {
    {"server/discover",          json::object()},
    {"tools/list",               json::object()},
    {"prompts/list",             json::object()},
    {"resources/list",           json::object()},
    {"resources/templates/list", json::object()},
    {"resources/read",           {{"uri", "pglicht://default/server/extensions"}}},
  };
  for (const auto& [method, extra] : cacheable) {
    json params = extra;
    params["_meta"] = modern_meta();
    json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", method},
                            {"params", params}});
    ASSERT_TRUE(r.contains("result")) << method << " " << r.dump(2);
    const json& res = r["result"];
    ASSERT_EQ(res.value("resultType", ""), "complete") << method;
    ASSERT_TRUE(res.contains("ttlMs")) << method << " carries no ttlMs";
    EXPECT_TRUE(res["ttlMs"].is_number_integer()) << method;
    EXPECT_GE(res["ttlMs"].get<long long>(), 0) << method << ": ttlMs MUST be >= 0";
    ASSERT_TRUE(res.contains("cacheScope")) << method << " carries no cacheScope";
    const std::string scope = res["cacheScope"].get<std::string>();
    EXPECT_TRUE(scope == "public" || scope == "private") << method << ": " << scope;
  }
}

TEST_F(PostgresMCPServerTest, CacheScopeIsPrivateWhereTheResultIsNotShareable) {
  auto scope_of = [&](const std::string& method) {
    json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", method},
                            {"params", {{"_meta", modern_meta()}}}});
    return r["result"].value("cacheScope", "");
  };
  // tools/list varies by negotiated revision (annotations, title and
  // outputSchema are each gated) while the cache key is method plus params,
  // and it embeds the configured default connection name. resources/list
  // enumerates the operator's schemas. Neither may be served to another caller.
  EXPECT_EQ(scope_of("tools/list"), "private");
  EXPECT_EQ(scope_of("resources/list"), "private");
  // These are compiled in and identical for everyone.
  EXPECT_EQ(scope_of("prompts/list"), "public");
  EXPECT_EQ(scope_of("resources/templates/list"), "public");
  EXPECT_EQ(scope_of("server/discover"), "public");
}

TEST_F(PostgresMCPServerTest, LegacyResultsCarryNoCachingHints) {
  // The hints are defined by the same revision that defines resultType, so
  // they are gated the same way: a legacy result must not grow fields its era
  // never had.
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"},
                          {"params", json::object()}});
  EXPECT_FALSE(r["result"].contains("ttlMs"));
  EXPECT_FALSE(r["result"].contains("cacheScope"));
  EXPECT_FALSE(r["result"].contains("resultType"));
}

TEST_F(PostgresMCPServerTest, PaginationWalksEveryItemExactlyOnce) {
  json items = json::array();
  for (int i = 0; i < 250; i++) items.push_back({{"n", i}});

  std::vector<int> seen;
  json params = json::object();
  int pages = 0;
  for (;;) {
    json page = json::object();
    ASSERT_TRUE(PostgresMCPServer::call_paginate(items, params, "items", page)) << params.dump();
    pages++;
    for (const auto& e : page["items"]) seen.push_back(e["n"].get<int>());
    if (!page.contains("nextCursor")) break;
    params["cursor"] = page["nextCursor"];
    ASSERT_LE(pages, 10) << "pagination did not terminate";
  }
  EXPECT_GT(pages, 1) << "250 items should not fit in one page";
  ASSERT_EQ(seen.size(), 250u);
  for (int i = 0; i < 250; i++) EXPECT_EQ(seen[static_cast<size_t>(i)], i);
}

TEST_F(PostgresMCPServerTest, CursorsAreOpaqueAndStable) {
  json items = json::array();
  for (int i = 0; i < 250; i++) items.push_back(i);

  json first = json::object();
  ASSERT_TRUE(PostgresMCPServer::call_paginate(items, json::object(), "items", first));
  ASSERT_TRUE(first.contains("nextCursor"));
  const std::string cursor = first["nextCursor"].get<std::string>();
  // Opaque by contract: it must not read as a number a client could do
  // arithmetic on.
  EXPECT_TRUE(cursor.find_first_not_of("0123456789") != std::string::npos) << cursor;

  // Stable: the same request yields the same cursor.
  json again = json::object();
  ASSERT_TRUE(PostgresMCPServer::call_paginate(items, json::object(), "items", again));
  EXPECT_EQ(again["nextCursor"].get<std::string>(), cursor);
}

TEST_F(PostgresMCPServerTest, AnInvalidCursorIsInvalidParams) {
  for (const char* bad : {"not-base64!!", "", "cGdsaWNodDo5OTk5OTk="}) {
    json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"},
                            {"params", {{"_meta", modern_meta()}, {"cursor", bad}}}});
    ASSERT_TRUE(r.contains("error")) << "cursor '" << bad << "' was accepted";
    EXPECT_EQ(r["error"]["code"], -32602) << bad;
  }
  // An empty string is a valid *cursor value* per the spec -- a client must not
  // read it as end-of-results -- but it is not one this server ever issues, so
  // it is refused rather than silently treated as "start from the beginning".
}

TEST_F(PostgresMCPServerTest, NoCurrentListIsTruncatedForAClientThatIgnoresCursors) {
  // The page size is deliberately larger than anything this server lists
  // today, so a client that never sends a cursor still sees every tool. A
  // smaller page would silently hide 44 of the 56 tools from such a client.
  for (const char* method : {"tools/list", "prompts/list", "resources/templates/list",
                             "resources/list"}) {
    json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", method},
                            {"params", {{"_meta", modern_meta()}}}});
    EXPECT_FALSE(r["result"].contains("nextCursor"))
        << method << " paginates today, which would truncate a client that ignores it";
  }
  json tools = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"},
                              {"params", {{"_meta", modern_meta()}}}});
  EXPECT_EQ(tools["result"]["tools"].size(), srv->call_tools_list().size());
}

// --- 4.0.0 resources, prompts and completions ---

namespace {
json rpc1(PostgresMCPServer& s, const std::string& method, const json& params) {
  return s.call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", method}, {"params", params}});
}
}  // namespace

TEST_F(PostgresMCPServerTest, CapabilitiesDeclareTheThreeNewSurfaces) {
  // A private server: initialize sets the negotiated revision for the rest of
  // that server's life, and the fixture's is shared by every test in the suite.
  PostgresMCPServer own(test_url);
  json init = rpc1(own, "initialize",
                   {{"protocolVersion", "2025-06-18"},
                    {"capabilities", json::object()},
                    {"clientInfo", {{"name", "t"}, {"version", "0"}}}});
  const json& caps = init["result"]["capabilities"];
  for (const char* c : {"tools", "resources", "prompts", "completions"})
    EXPECT_TRUE(caps.contains(c)) << c << " missing from initialize";

  // The stateless era advertises the same set, or a modern client would see a
  // smaller server than a legacy one.
  json disc = rpc1(own, "server/discover", json::object());
  const json& dcaps = disc["result"]["capabilities"];
  for (const char* c : {"tools", "resources", "prompts", "completions"})
    EXPECT_TRUE(dcaps.contains(c)) << c << " missing from server/discover";
}

TEST_F(PostgresMCPServerTest, ResourcesListIsBoundedAndDoesNotEnumerateTables) {
  json r = rpc1(*srv, "resources/list", json::object());
  const json& res = r["result"]["resources"];
  ASSERT_TRUE(res.is_array()) << r.dump(2);

  bool saw_schemas = false;
  for (const auto& e : res) {
    const std::string uri = e.value("uri", "");
    EXPECT_EQ(e.value("mimeType", ""), "application/json") << uri;
    if (uri.find("/schemas") != std::string::npos) saw_schemas = true;
    // Nothing per-object may be enumerated here. A 10k-table database would
    // otherwise produce a 10k-entry response on a call clients make eagerly,
    // and enumerating schemas costs a connection per configured connection --
    // which 4.0.0 did, and which is what made a registry of remote databases
    // slow to start.
    EXPECT_EQ(uri.find("/table/"), std::string::npos)
        << "resources/list enumerated a table: " << uri;
    EXPECT_EQ(uri.find("/schema/"), std::string::npos)
        << "resources/list enumerated a schema, which costs a connection: " << uri;
  }
  EXPECT_TRUE(saw_schemas) << "the schema list itself must still be addressable";
}

TEST_F(PostgresMCPServerTest, StartingUpOpensNoConnection) {
  // Constructing the server must do no network I/O. Connecting eagerly cost a
  // full round trip to a remote database before the client could do anything,
  // and it was fatal: an unreachable default aborted the constructor, so a
  // registry of twenty databases was unusable in its entirety because one of
  // them was behind a VPN that happened to be down.
  //
  // 192.0.2.0/24 is TEST-NET-1: reserved, and routed nowhere. If the
  // constructor tried to reach it this would block until the operating
  // system's connect timeout rather than returning at once.
  const auto t0 = std::chrono::steady_clock::now();
  EXPECT_NO_THROW({
    PostgresMCPServer unreachable("host=192.0.2.1 port=5432 dbname=x user=y");
  });
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - t0).count();
  EXPECT_LT(ms, 2000) << "the constructor appears to have tried to connect";
}

TEST_F(PostgresMCPServerTest, ResourcesListDoesNotConnectEither) {
  // The same property for the call clients make eagerly at startup. One
  // unreachable connection in the registry must not delay or truncate it.
  const std::string ini =
      "[live]\nhost = " + std::string("127.0.0.1") + "\n"
      "[dead]\nhost = 192.0.2.1\nport = 5432\ndbname = x\nuser = y\n";
  (void)ini;  // built below through the fixture's own helper

  PostgresMCPServer two(pglicht::ConnectionRegistry::from_url(
      "host=192.0.2.1 port=5432 dbname=x user=y", "pg-licht-test"));
  const auto t0 = std::chrono::steady_clock::now();
  json r = rpc1(two, "resources/list", json::object());
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - t0).count();
  EXPECT_LT(ms, 2000) << "resources/list appears to have tried to connect";
  // And it still describes that connection, rather than dropping it.
  ASSERT_TRUE(r["result"]["resources"].is_array()) << r.dump(2);
  EXPECT_GE(r["result"]["resources"].size(), 4u);
}

TEST_F(PostgresMCPServerTest, ResourceTemplatesCoverWhatTheListDoesNot) {
  json r = rpc1(*srv, "resources/templates/list", json::object());
  const json& t = r["result"]["resourceTemplates"];
  ASSERT_TRUE(t.is_array());
  bool saw_table = false;
  for (const auto& e : t)
    if (e.value("uriTemplate", "") == "pglicht://{conn}/schema/{schema}/table/{table}")
      saw_table = true;
  EXPECT_TRUE(saw_table);
}

TEST_F(PostgresMCPServerTest, AResourceServesTheSamePayloadAsItsTool) {
  // Resources are the existing query methods behind a URI, not a second
  // implementation that can drift from the first.
  json r = rpc1(*srv, "resources/read",
                {{"uri", "pglicht://default/schema/grocery/table/users"}});
  ASSERT_TRUE(r["result"].contains("contents")) << r.dump(2);
  const json& c = r["result"]["contents"][0];
  EXPECT_EQ(c.value("mimeType", ""), "application/json");
  json body = json::parse(c["text"].get<std::string>());
  EXPECT_EQ(body, srv->call_table("grocery", "users"));
}

TEST_F(PostgresMCPServerTest, ResourcesCarryStructureAndNoReadings) {
  // The volatility principle, and the reason resources had to wait for the
  // statistics split: a document a client pins into context and re-reads later
  // must not carry a counter that moves under it.
  json r = rpc1(*srv, "resources/read",
                {{"uri", "pglicht://default/schema/grocery/table/users"}});
  json body = json::parse(r["result"]["contents"][0]["text"].get<std::string>());
  for (const char* f : {"n_dead_tup", "seq_scan", "idx_scan", "last_vacuum",
                        "rows", "size", "size_estimate"})
    EXPECT_FALSE(body.contains(f)) << f << " is a reading, and this is a document";
  EXPECT_TRUE(body.contains("columns"));
}

TEST_F(PostgresMCPServerTest, AnUnknownResourceFailsWithInvalidParams) {
  for (const char* uri : {"pglicht://default/nope",
                          "pglicht://default/schema/grocery/table",
                          "https://example.com/x"}) {
    json r = rpc1(*srv, "resources/read", {{"uri", uri}});
    ASSERT_TRUE(r.contains("error")) << uri << " " << r.dump(2);
    EXPECT_EQ(r["error"]["code"], -32602) << uri;
  }
}

TEST_F(PostgresMCPServerTest, AResourceNamingAnUnknownConnectionSaysWhichExist) {
  json r = rpc1(*srv, "resources/read", {{"uri", "pglicht://nosuchconn/schemas"}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  // The registry throws a message listing the configured names, so a typo is
  // answered rather than turned into a connect failure.
  EXPECT_NE(r["error"]["message"].get<std::string>().find("default"), std::string::npos)
      << r["error"].dump(2);
}

TEST_F(PostgresMCPServerTest, PromptsAreListedWithTheirArguments) {
  json r = rpc1(*srv, "prompts/list", json::object());
  const json& p = r["result"]["prompts"];
  ASSERT_TRUE(p.is_array());
  std::set<std::string> names;
  for (const auto& e : p) {
    names.insert(e.value("name", ""));
    EXPECT_FALSE(e.value("description", "").empty()) << e.value("name", "");
    EXPECT_TRUE(e["arguments"].is_array()) << e.value("name", "");
  }
  for (const char* n : {"diagnose-slow-query", "triage-lock-contention",
                        "bloat-and-vacuum-review", "buffer-cache-review",
                        "capacity-check", "replication-slot-review",
                        "plan-schema-change", "explain-and-fix"})
    EXPECT_TRUE(names.count(n)) << n << " is missing";
}

TEST_F(PostgresMCPServerTest, PromptsSendTheModelToCheckPrivilegesFirst) {
  // Every prompt whose core tools are privilege-gated opens by checking. A
  // restricted role gets null statistics rather than an error, so a plan built
  // on tools that will not answer looks like it worked.
  for (const char* n : {"diagnose-slow-query", "triage-lock-contention",
                        "bloat-and-vacuum-review", "buffer-cache-review",
                        "replication-slot-review", "plan-schema-change",
                        "explain-and-fix"}) {
    json r = rpc1(*srv, "prompts/get", {{"name", n},
                                        {"arguments", {{"sql", "SELECT 1"},
                                                       {"change", "add a column"}}}});
    const std::string text = r["result"]["messages"][0]["content"]["text"].get<std::string>();
    EXPECT_NE(text.find("checkPrivileges"), std::string::npos) << n;
  }
}

TEST_F(PostgresMCPServerTest, TheSlowQueryPromptBranchesIntoBloatAndVacuum) {
  // The richest path: a slow statement can end in a plan fix, a vacuum change,
  // an index, or a schema change, and the prompt has to name all four routes.
  json r = rpc1(*srv, "prompts/get", {{"name", "diagnose-slow-query"}});
  const std::string t = r["result"]["messages"][0]["content"]["text"].get<std::string>();
  for (const char* tool : {"statementStats", "explainQuery", "tableStats",
                           "tableBloat", "indexBloat", "duplicateIndexes",
                           "bloat-and-vacuum-review", "plan-schema-change"})
    EXPECT_NE(t.find(tool), std::string::npos) << tool << " missing from the slow-query path";
}

TEST_F(PostgresMCPServerTest, TheDiagnosticPromptsChooseBetweenARewriteAndDDL) {
  // Both prompts can end in four different kinds of fix, and which one it is
  // has to be a decision rather than a default. Ending at "propose the fix as
  // DDL" reaches for an index when an ANALYZE or a rewrite would have done --
  // and an index is a write cost paid forever to buy one read pattern.
  for (const char* n : {"diagnose-slow-query", "explain-and-fix"}) {
    json r = rpc1(*srv, "prompts/get",
                  {{"name", n}, {"arguments", {{"sql", "SELECT 1"}}}});
    const std::string t = r["result"]["messages"][0]["content"]["text"].get<std::string>();
    // The four kinds, and the instruction to pick one deliberately.
    for (const char* k : {"the fix is a rewrite", "the fix is statistics",
                          "the fix is configuration", "only then is the fix DDL",
                          "Prefer them in that order"})
      EXPECT_NE(t.find(k), std::string::npos) << n << " missing: " << k;
    // The cost argument is what makes the ordering more than a preference.
    EXPECT_NE(t.find("every INSERT and UPDATE"), std::string::npos) << n;
    EXPECT_NE(t.find("why the cheaper options were rejected"), std::string::npos) << n;

    // The asymmetry this server can actually exploit: a rewrite is checkable
    // here, an index is not.
    EXPECT_NE(t.find("call explainQuery on the rewritten statement"), std::string::npos) << n;
    // With hypopg an index is checkable too, and the prompt must say so and
    // say how to find out whether it is available.
    EXPECT_NE(t.find("evaluateIndex"), std::string::npos) << n;
    EXPECT_NE(t.find("checkPrivileges says whether hypopg is there"), std::string::npos) << n;
    EXPECT_NE(t.find("an index is a prediction"), std::string::npos) << n;
    EXPECT_NE(t.find("plan-schema-change"), std::string::npos)
        << n << " must hand index creation to the prompt that measures the table";
  }
}

TEST_F(PostgresMCPServerTest, TheSlotPromptLeadsWithWhatASlotHoldsBack) {
  json r = rpc1(*srv, "prompts/get", {{"name", "replication-slot-review"}});
  const std::string t = r["result"]["messages"][0]["content"]["text"].get<std::string>();
  // The two consequences that get misdiagnosed as something else.
  EXPECT_NE(t.find("xmin horizon"), std::string::npos);
  EXPECT_NE(t.find("wal_status"), std::string::npos);
  // Dropping a slot is irreversible for its consumer; the prompt must say so.
  EXPECT_NE(t.find("irreversible"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, TheSchemaChangePromptRequiresTheChangeAndStatesTheLocks) {
  json missing = rpc1(*srv, "prompts/get", {{"name", "plan-schema-change"},
                                            {"arguments", json::object()}});
  ASSERT_TRUE(missing.contains("error")) << missing.dump(2);
  EXPECT_EQ(missing["error"]["code"], -32602);

  json r = rpc1(*srv, "prompts/get",
                {{"name", "plan-schema-change"},
                 {"arguments", {{"change", "add a NOT NULL enum column"},
                                {"schema", "shop"}, {"table", "orders"}}}});
  const std::string t = r["result"]["messages"][0]["content"]["text"].get<std::string>();
  EXPECT_NE(t.find("add a NOT NULL enum column"), std::string::npos);
  EXPECT_NE(t.find("shop.orders"), std::string::npos);

  // It must send the model to measure, because the readings are the only thing
  // the model cannot already know.
  for (const char* k : {"checkPrivileges", "tableStats", "tableSize",
                        "tableDetails", "currentActivity", "currentLocks",
                        "most_common_vals", "oldest running transaction",
                        "replicationSlots"})
    EXPECT_NE(t.find(k), std::string::npos) << k << " missing from the measurement step";

  // A statement naming one table routinely locks several, and the unnamed one
  // is often the busier: a foreign key locks what it references, a partitioned
  // table means every partition. Measuring only the target misses the lock
  // that actually hurts.
  for (const char* k : {"every object the change touches", "not only the one named",
                        "the table it references", "every partition"})
    EXPECT_NE(t.find(k), std::string::npos) << k << " missing; the prompt measures one object only";

  // ...and it must ask for a classification and a threshold rather than an
  // answer, so the reasoning is checkable against a different table.
  for (const char* k : {"metadata only", "full rewrite", "would flip",
                        "irreversible", "read-only"})
    EXPECT_NE(t.find(k), std::string::npos) << k << " missing";
}

TEST_F(PostgresMCPServerTest, TheSchemaChangePromptTeachesAMethodNotARecipe) {
  // The point of the prompt is the readings, not the DDL. A model already
  // knows what CREATE INDEX CONCURRENTLY is; what it cannot know is this
  // table's size, its indexes and what is holding a lock right now. Baking the
  // recipe in also makes the prompt wrong at one end of the scale -- always
  // reaching for the safest-at-scale option is machinery nobody needs on an
  // empty table -- and version-specific rules go stale silently.
  json r = rpc1(*srv, "prompts/get",
                {{"name", "plan-schema-change"}, {"arguments", {{"change", "add an index"}}}});
  const std::string t = r["result"]["messages"][0]["content"]["text"].get<std::string>();
  for (const char* recipe : {"CONCURRENTLY", "NOT VALID", "VALIDATE CONSTRAINT",
                             "ALTER COLUMN TYPE", "PostgreSQL 11"})
    EXPECT_EQ(t.find(recipe), std::string::npos)
        << recipe << " is a prescribed recipe; this prompt must derive from measurements";
  // The principle itself has to be stated, or nothing stops it drifting back.
  EXPECT_NE(t.find("Do not answer from a recipe"), std::string::npos);
  EXPECT_NE(t.find("billion"), std::string::npos) << "the scale contrast is the argument";
  EXPECT_NE(t.find("Partitioning"), std::string::npos) << "the extreme case is the clearest one";
}

TEST_F(PostgresMCPServerTest, APromptSubstitutesItsArguments) {
  json r = rpc1(*srv, "prompts/get",
                {{"name", "bloat-and-vacuum-review"}, {"arguments", {{"schema", "grocery"}}}});
  ASSERT_TRUE(r["result"].contains("messages")) << r.dump(2);
  const std::string text =
      r["result"]["messages"][0]["content"]["text"].get<std::string>();
  EXPECT_NE(text.find("grocery"), std::string::npos);
  // The template exists to encode an order of investigation, so it must name
  // the step that is most often skipped.
  EXPECT_NE(text.find("replicationSlots"), std::string::npos)
      << "the bloat prompt must send the model to check the slot first";
}

TEST_F(PostgresMCPServerTest, APromptFallsBackWhenAnOptionalArgumentIsOmitted) {
  json r = rpc1(*srv, "prompts/get", {{"name", "bloat-and-vacuum-review"}});
  ASSERT_TRUE(r["result"].contains("messages")) << r.dump(2);
  EXPECT_NE(r["result"]["messages"][0]["content"]["text"].get<std::string>().find("public"),
            std::string::npos);
}

TEST_F(PostgresMCPServerTest, APromptMissingARequiredArgumentIsRefused) {
  json r = rpc1(*srv, "prompts/get", {{"name", "explain-and-fix"}, {"arguments", json::object()}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32602);
  EXPECT_NE(r["error"]["message"].get<std::string>().find("sql"), std::string::npos);

  json unknown = rpc1(*srv, "prompts/get", {{"name", "no-such-prompt"}});
  ASSERT_TRUE(unknown.contains("error"));
  EXPECT_EQ(unknown["error"]["code"], -32602);
}

TEST_F(PostgresMCPServerTest, CompletionOffersConnectionsAndSchemas) {
  json r = rpc1(*srv, "completion/complete",
                {{"ref", {{"type", "ref/resource"}, {"uri", "pglicht://{conn}/schemas"}}},
                 {"argument", {{"name", "conn"}, {"value", ""}}}});
  const json& c = r["result"]["completion"];
  ASSERT_TRUE(c["values"].is_array()) << r.dump(2);
  EXPECT_EQ(c["values"][0], "default");
  EXPECT_FALSE(c["hasMore"].get<bool>());

  json sch = rpc1(*srv, "completion/complete",
                  {{"ref", {{"type", "ref/resource"}, {"uri", "pglicht://{conn}/schema/{schema}"}}},
                   {"argument", {{"name", "schema"}, {"value", "groc"}}}});
  bool saw = false;
  for (const auto& v : sch["result"]["completion"]["values"])
    if (v == "grocery") saw = true;
  EXPECT_TRUE(saw) << sch.dump(2);
}

TEST_F(PostgresMCPServerTest, CompletionNeverFailsTheSession) {
  // A completion is a convenience. An argument it knows nothing about, or a
  // catalog it cannot read, returns an empty list rather than an error.
  json r = rpc1(*srv, "completion/complete",
                {{"ref", {{"type", "ref/prompt"}, {"name", "capacity-check"}}},
                 {"argument", {{"name", "something_unknown"}, {"value", "x"}}}});
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  EXPECT_TRUE(r["result"]["completion"]["values"].is_array());
  EXPECT_EQ(r["result"]["completion"]["values"].size(), 0u);
}

TEST_F(PostgresMCPServerTest, TitlesAppearOnlyFromTheRevisionThatDefinedThem) {
  json tools = srv->call_tools_list("2025-06-18");
  bool checked = false;
  for (const auto& t : tools) {
    ASSERT_TRUE(t.contains("title")) << t.value("name", "");
    if (t.value("name", "") == "bufferCacheSummary") {
      EXPECT_EQ(t["title"], "Buffer cache summary");
      checked = true;
    }
  }
  EXPECT_TRUE(checked);
}

TEST_F(PostgresMCPServerTest, InstanceWideToolsSayThatTheyAre) {
  json tools = srv->call_tools_list();
  std::map<std::string, std::string> d;
  for (const auto& t : tools) d[t.value("name", "")] = t.value("description", "");

  // pg_stat_activity and pg_stat_statements report the whole instance from any
  // one database, so a sweep across databases repeats one answer.
  for (const char* n : {"currentActivity", "currentLocks", "statementStats",
                        "bufferCacheSummary", "serverSettings"})
    EXPECT_NE(d[n].find("instance-wide"), std::string::npos) << n;

  // ...while the per-database catalogs must not claim to be.
  for (const char* n : {"listTables", "tableBloat", "listFunctions"})
    EXPECT_EQ(d[n].find("instance-wide"), std::string::npos) << n;
}

TEST_F(PostgresMCPServerTest, IndexToolsSayTheirScanCountsArePerServer) {
  // The reason this matters: an index dead on the primary may be carrying a
  // replica's whole reporting workload, and only idx_scan on that replica
  // shows it. duplicateIndexes and indexBloat both report idx_scan.
  json tools = srv->call_tools_list();
  std::map<std::string, std::string> d;
  for (const auto& t : tools) d[t.value("name", "")] = t.value("description", "");
  for (const char* n : {"duplicateIndexes", "indexBloat", "tableIOStats"})
    EXPECT_NE(d[n].find("each server's own"), std::string::npos) << n;
  // Bloat of a table is physical and replicated verbatim.
  EXPECT_NE(d["tableBloat"].find("byte-identical here"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, InitializeHonoursTheRequestedProtocolVersion) {
  // 3.1.1 ignored it and always replied 2024-11-05.
  EXPECT_EQ(srv->call_initialize_version("2025-06-18"), "2025-06-18");
  EXPECT_EQ(srv->call_initialize_version("2024-11-05"), "2024-11-05");
  // An unknown or absent version falls back to the revision this server has
  // always spoken rather than echoing something untested.
  EXPECT_EQ(srv->call_initialize_version("2099-01-01"), "2024-11-05");
  EXPECT_EQ(srv->call_initialize_version(""), "2024-11-05");
}

// --- buffer cache ---

TEST_F(PostgresMCPServerTest, BufferCacheSummaryReportsTotalsAndTheUsageHistogram) {
  json r = srv->call_buffer_cache_summary();

  // pg_buffercache 1.4 shipped with PostgreSQL 16, so on 14 and 15 the summary
  // functions genuinely do not exist. Assert the gate says so honestly rather
  // than skipping: pointing the operator at ALTER EXTENSION on a server with no
  // 1.4 to update to would send them in a circle.
  if (r.contains("error")) {
    const std::string e = r["error"].get<std::string>();
    ASSERT_NE(e.find("pg_buffercache_summary()"), std::string::npos) << r.dump(2);
    EXPECT_NE(e.find("PostgreSQL 16"), std::string::npos) << r.dump(2);
    EXPECT_NE(r["hint"].get<std::string>().find("bufferCacheContents"),
              std::string::npos) << r.dump(2);
    GTEST_SKIP() << "pg_buffercache predates 1.4 on this server: " << e;
  }

  // shared_buffers is the denominator for everything else here, so the used,
  // unused and total figures have to agree or none of the ratios mean anything.
  ASSERT_TRUE(r.contains("shared_buffers_blocks")) << r.dump(2);
  const long long total = r["shared_buffers_blocks"].get<long long>();
  const long long used = r["buffers_used"].get<long long>();
  const long long unused = r["buffers_unused"].get<long long>();
  EXPECT_GT(total, 0);
  EXPECT_EQ(used + unused, total);
  EXPECT_EQ(r["shared_buffers_bytes"].get<long long>(),
            total * r["block_size_bytes"].get<long long>());

  // The histogram is the reason this tool exists: a hit ratio cannot tell a
  // stable working set from clock-sweep churn, and the usage counts can.
  ASSERT_TRUE(r["usage_counts"].is_array()) << r.dump(2);
  ASSERT_FALSE(r["usage_counts"].empty());
  long long histogram_total = 0;
  int previous = -1;
  for (const auto& b : r["usage_counts"]) {
    EXPECT_GT(b["usage_count"].get<int>(), previous) << "not ordered";
    previous = b["usage_count"].get<int>();
    histogram_total += b["buffers"].get<long long>();
  }
  EXPECT_EQ(histogram_total, total) << "histogram must account for every buffer";
}

TEST_F(PostgresMCPServerTest, BufferCacheContentsRanksRelationsAndResolvesMappedCatalogs) {
  // A mapped catalog has relfilenode = 0 in pg_class, so joining the view on
  // oid instead of pg_relation_filenode(oid) silently drops every one of them
  // -- and they are among the most heavily cached relations in any database.
  // Ask the catalog which relations are mapped rather than hardcoding names.
  std::set<std::string> mapped;
  {
    pqxx::connection c(test_url);
    pqxx::work t(c);
    for (const auto& row : t.exec("SELECT relname FROM pg_class WHERE relfilenode = 0"))
      mapped.insert(row[0].as<std::string>());
  }
  ASSERT_FALSE(mapped.empty()) << "no mapped relations: the premise is wrong";

  json r = srv->call_buffer_cache_contents(200);
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  ASSERT_TRUE(r["relations"].is_array()) << r.dump(2);
  ASSERT_FALSE(r["relations"].empty());

  long long previous = -1;
  bool saw_mapped = false;
  for (const auto& row : r["relations"]) {
    // Ranked by buffers held, descending.
    const long long buffers = row["buffers"].get<long long>();
    if (previous >= 0) { EXPECT_LE(buffers, previous); }
    previous = buffers;

    // An unresolved row would mean the join dropped a relation rather than
    // naming it, which is the failure this tool must never produce silently.
    const std::string relation = row["relation"].get<std::string>();
    EXPECT_FALSE(relation.empty()) << row.dump(2);
    EXPECT_FALSE(row["schema"].get<std::string>().empty()) << row.dump(2);

    const std::string fork = row["fork"].get<std::string>();
    EXPECT_TRUE(fork == "main" || fork == "fsm" || fork == "vm" || fork == "init")
      << "unexpected fork: " << fork;

    if (mapped.count(relation)) saw_mapped = true;
  }
  EXPECT_TRUE(saw_mapped)
    << "no mapped catalog resolved; the relfilenode join is wrong: " << r.dump(2);
}

TEST_F(PostgresMCPServerTest, BufferCacheContentsStillMeasuresTheForksItReturns) {
  // 3.2.1 moved pg_relation_size() below the limit, so it is called for the
  // rows returned instead of once per cached relation. The measurement must
  // still be there: dropping it would turn a cost fix into a payload change.
  json r = srv->call_buffer_cache_contents(5);
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  ASSERT_FALSE(r["relations"].empty()) << r.dump(2);

  bool measured = false;
  for (const auto& row : r["relations"]) {
    ASSERT_TRUE(row.contains("fork_bytes")) << row.dump(2);
    ASSERT_TRUE(row.contains("cached_bytes")) << row.dump(2);
    ASSERT_TRUE(row.contains("percent_of_shared_buffers")) << row.dump(2);
    if (!row["fork_bytes"].is_null() && row["fork_bytes"].get<long long>() > 0) {
      measured = true;
      // A fork that holds buffers has a size, and the ratio derived from it.
      EXPECT_FALSE(row["percent_of_fork_cached"].is_null()) << row.dump(2);
    }
  }
  EXPECT_TRUE(measured) << "no fork was measured at all: " << r.dump(2);
}

TEST_F(PostgresMCPServerTest, BufferCacheContentsResolvesSharedCatalogs) {
  // reldatabase 0 is the shared catalogs. Filtering them out along with other
  // databases' buffers would drop pg_database and pg_authid entirely.
  { pqxx::connection c(test_url); pqxx::work t(c);
    t.exec("SELECT count(*) FROM pg_database"); }

  json r = srv->call_buffer_cache_contents(200);
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  bool saw_shared = false;
  for (const auto& row : r["relations"]) {
    if (row["relation"] == "pg_database") saw_shared = true;
  }
  EXPECT_TRUE(saw_shared) << "shared catalogs must survive the reldatabase filter";
}

TEST_F(PostgresMCPServerTest, BufferCacheContentsCapsAndDefaultsTheLimit) {
  EXPECT_EQ(srv->call_buffer_cache_contents(5)["limit"].get<int>(), 5);
  EXPECT_EQ(srv->call_buffer_cache_contents(0)["limit"].get<int>(), 20);
  EXPECT_EQ(srv->call_buffer_cache_contents(-1)["limit"].get<int>(), 20);
  EXPECT_EQ(srv->call_buffer_cache_contents(100000)["limit"].get<int>(), 200);
  EXPECT_LE(srv->call_buffer_cache_contents(3)["relations"].size(), 3u);
}

TEST_F(PostgresMCPServerTest, BufferCacheToolsNameTheExtensionWhenItIsAbsent) {
  // A database without the extension must produce the CREATE EXTENSION hint,
  // not an empty result. This is also the extension_schema() miss path: the
  // lookup returns nothing rather than resolving to public.
  const std::string other = test_dbname + "_nobc";
  {
    pqxx::nontransaction n(*admin_conn);
    n.exec("CREATE DATABASE \"" + other + "\"");
  }
  const std::string other_url = std::regex_replace(
      test_url, std::regex(R"(\bdbname\s*=\s*\S+)"), "dbname=" + other);
  {
    PostgresMCPServer bare{other_url};
    for (const json& r : {bare.call_buffer_cache_summary(),
                          bare.call_buffer_cache_contents(10)}) {
      ASSERT_TRUE(r.contains("error")) << r.dump(2);
      EXPECT_NE(r["error"].get<std::string>().find("pg_buffercache is not installed"),
                std::string::npos);
      EXPECT_NE(r["hint"].get<std::string>().find("CREATE EXTENSION pg_buffercache"),
                std::string::npos);
    }
  }
  pqxx::nontransaction n(*admin_conn);
  n.exec("DROP DATABASE \"" + other + "\" WITH (FORCE)");
}

TEST_F(PostgresMCPServerTest, EverySessionEndsInRollbackAndLeavesNothing) {
  // Everything here is read-only, so there is nothing a commit could preserve,
  // and ending in ROLLBACK makes "the server is as you found it" provable
  // rather than incidental. Asserted through a temporary table, which is the
  // one thing a committed transaction would leave visible to the next
  // connection.
  pqxx::connection probe(test_url);
  {
    pqxx::work w(probe);
    w.exec("CREATE TABLE grocery.rollback_probe (x int)");
    w.commit();
  }
  const size_t before = srv->call_tables("grocery").size();
  // Exercise a spread of tools, including the ones that set transaction-scoped
  // state of their own.
  srv->call_table("grocery", "users");
  srv->call_table_stats("grocery", "users");
  srv->call_explain_query("", "SELECT 1", json::array(), false, 0);
  srv->call_check_privileges();
  EXPECT_EQ(srv->call_tables("grocery").size(), before);

  pqxx::work w(probe);
  // Still exactly one row in pg_class for it: nothing this server did created
  // or dropped anything.
  EXPECT_EQ(w.exec("SELECT count(*) FROM pg_class WHERE relname = 'rollback_probe'")[0][0]
              .as<int>(), 1);
  w.exec("DROP TABLE grocery.rollback_probe");
  w.commit();
}

// --- connection reuse and the reaper ---

TEST_F(PostgresMCPServerTest, ConnectionsAreReusedAcrossCalls) {
  // Against a local socket this is invisible; against a remote database the
  // connect, TLS handshake and authentication were the majority of what a call
  // spent, repeated for the next call to the same database milliseconds later.
  pqxx::connection watch(test_url);
  auto backends = [&] {
    pqxx::nontransaction n(watch);
    return n.exec("SELECT count(*) FROM pg_stat_activity"
                  " WHERE application_name LIKE 'pg-licht%'"
                  "   AND pid <> pg_backend_pid()")[0][0].as<int>();
  };
  srv->call_schemas();
  const int after_first = backends();
  for (int i = 0; i < 5; i++) srv->call_schemas();
  EXPECT_EQ(backends(), after_first)
      << "a sixth call opened a new backend; the connection was not reused";
  EXPECT_GE(after_first, 1) << "the connection should still be held between calls";
}

TEST_F(PostgresMCPServerTest, TheReaperClosesAnIdleConnection) {
  // The cache holds only idle connections, so the reaper can never see one
  // that is in use. reap_now() is the test hook for the TTL having elapsed --
  // waiting 60 s in a unit test would be its own kind of bug.
  ConnectionCache cache{std::chrono::seconds{60}};
  pglicht::ConnConfig cfg;
  cfg.name = "t";
  cfg.conninfo = test_url;
  {
    Session s{cfg, std::nullopt, &cache, "t"};
    s.txn().exec("SELECT 1");
    EXPECT_EQ(cache.idle_count(), 0u) << "a connection in use must not be in the cache";
  }
  EXPECT_EQ(cache.idle_count(), 1u) << "released connections are cached";
  cache.reap_now();
  EXPECT_EQ(cache.idle_count(), 0u);
}

TEST_F(PostgresMCPServerTest, AConnectionKilledUnderneathIsReplaced) {
  // idle_session_timeout, a server restart, a NAT table that forgot us: a
  // cached socket can be dead by the time the next call wants it, and there is
  // no way to know but to use it. The retry is safe because nothing of the
  // caller's has run at that point -- only BEGIN and the setup batch.
  ConnectionCache cache{std::chrono::seconds{60}};
  pglicht::ConnConfig cfg;
  cfg.name = "t";
  cfg.conninfo = test_url;

  int victim = 0;
  {
    Session s{cfg, std::nullopt, &cache, "t"};
    victim = s.txn().exec("SELECT pg_backend_pid()")[0][0].as<int>();
  }
  ASSERT_EQ(cache.idle_count(), 1u);

  // Kill the cached backend from outside.
  //
  // This can only be staged on a direct connection. Behind a transaction-mode
  // pooler the Session has already handed its server backend back to the pool,
  // so the killer is liable to be assigned that very backend and terminate
  // itself -- and more fundamentally, what pg_licht holds there is a
  // connection to the pooler, not a PostgreSQL backend, so "the connection was
  // killed underneath us" is not a thing SQL can arrange. The retry path this
  // test covers is unchanged either way; only the way to provoke it is not
  // available.
  try {
    pqxx::connection killer(test_url);
    pqxx::nontransaction n(killer);
    n.exec("SELECT pg_terminate_backend(" + std::to_string(victim) + ")");
  } catch (const std::exception& e) {
    GTEST_SKIP() << "cannot terminate a specific backend here (pooled?): " << e.what();
  }

  // The next session must succeed anyway, on a different backend.
  Session s{cfg, std::nullopt, &cache, "t"};
  const int replacement = s.txn().exec("SELECT pg_backend_pid()")[0][0].as<int>();
  EXPECT_NE(replacement, victim) << "reused a terminated backend";
}

TEST_F(PostgresMCPServerTest, ReuseSurvivesWhateverTheTransportIs) {
  // The property that has to hold everywhere, pooled or not: consecutive calls
  // on a reused connection keep answering correctly. Stated separately from
  // the backend-killing test above, which can only be staged directly.
  for (int i = 0; i < 8; i++) {
    json r = srv->call_schemas();
    ASSERT_TRUE(r.is_object()) << "call " << i;
  }
  EXPECT_TRUE(srv->call_table("grocery", "users").contains("columns"));
  EXPECT_TRUE(srv->call_table_stats("grocery", "users").contains("rows"));
}

// --- evaluateIndex (hypopg) ---

namespace {
bool hypopg_available(PostgresMCPServer& s) {
  json r = s.call_evaluate_index("SELECT 1", json::array({"CREATE INDEX ON grocery.users (name)"}),
                                 json::array());
  const bool present = !(r.contains("error") &&
                         r["error"].get<std::string>().find("not installed") != std::string::npos);
  // On a developer machine a missing hypopg is a reason to skip. In CI it is
  // installed on purpose -- the pooled job is the only place the reset bracket
  // can be proved -- so there a skip would mean something broke while looking
  // exactly like something merely absent. PGLICHT_REQUIRE_HYPOPG turns that
  // silence into a failure.
  if (!present && std::getenv("PGLICHT_REQUIRE_HYPOPG"))
    ADD_FAILURE() << "PGLICHT_REQUIRE_HYPOPG is set but hypopg is not installed: "
                  << r.dump();
  return present;
}
}  // namespace

TEST_F(PostgresMCPServerTest, EvaluateIndexShowsWhetherThePlannerWouldUseIt) {
  if (!hypopg_available(*srv)) GTEST_SKIP() << "hypopg is not installed here";
  json r = srv->call_evaluate_index(
      "SELECT id FROM grocery.orders WHERE amount = 10",
      json::array({"CREATE INDEX ON grocery.orders (amount)"}), json::array());
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  ASSERT_EQ(r["indexes"].size(), 1u);
  // "used" is the point of the tool: a proposed index the planner ignores is
  // the common case, and a cost figure alone hides it.
  EXPECT_TRUE(r["indexes"][0].contains("used"));
  EXPECT_TRUE(r["indexes"][0]["used"].is_boolean());
  EXPECT_TRUE(r["indexes"][0].contains("estimated_size"));
  EXPECT_TRUE(r["baseline"].contains("plan"));
  EXPECT_TRUE(r["hypothetical"].contains("plan"));
  EXPECT_GT(r["baseline"]["total_cost"].get<double>(), 0.0);
}

TEST_F(PostgresMCPServerTest, EvaluateIndexBuildsNothingAndKeepsTheGuard) {
  if (!hypopg_available(*srv)) GTEST_SKIP() << "hypopg is not installed here";
  const std::string before = srv->call_table("grocery", "orders")["indexes"].dump();
  srv->call_evaluate_index("SELECT id FROM grocery.orders WHERE amount = 10",
                           json::array({"CREATE INDEX ON grocery.orders (amount)"}),
                           json::array());
  // Nothing was built: a hypothetical index exists only in backend memory.
  EXPECT_EQ(srv->call_table("grocery", "orders")["indexes"].dump(), before);
  // And the read-only guard is untouched by having planned against one.
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "explainQuery"},
                                      {"arguments", {{"sql", "SELECT 1"}}}}}});
  EXPECT_FALSE(r["result"]["isError"].get<bool>()) << r.dump(2);
}

TEST_F(PostgresMCPServerTest, AHypotheticalIndexDoesNotLeakToTheNextCall) {
  // The reason the reset bracket exists. Measured against hypopg 1.4.3: a
  // hypothetical index survives ROLLBACK, survives into a new transaction, and
  // survives DISCARD ALL -- which is exactly what PgBouncer issues as
  // server_reset_query. Behind a transaction pooler that means one caller's
  // hypothetical index would reshape the next caller's plans, silently.
  //
  // A direct connection is fresh every call, so this can only ever fail behind
  // the pooler; it passes trivially otherwise rather than failing spuriously.
  if (!hypopg_available(*srv)) GTEST_SKIP() << "hypopg is not installed here";
  const char* sql = "SELECT id FROM grocery.orders WHERE amount = 10";

  json first = srv->call_evaluate_index(
      sql, json::array({"CREATE INDEX ON grocery.orders (amount)"}), json::array());
  ASSERT_FALSE(first.contains("error")) << first.dump(2);
  const double baseline = first["baseline"]["total_cost"].get<double>();

  // A second call must see the same baseline. If the first call's index had
  // leaked onto the backend, this baseline would already be planning with it.
  for (int i = 0; i < 3; i++) {
    json again = srv->call_evaluate_index(
        sql, json::array({"CREATE INDEX ON grocery.orders (id)"}), json::array());
    ASSERT_FALSE(again.contains("error")) << again.dump(2);
    EXPECT_DOUBLE_EQ(again["baseline"]["total_cost"].get<double>(), baseline)
        << "a hypothetical index leaked onto a pooled backend";
    EXPECT_EQ(again["indexes"].size(), 1u) << "a previous call's index is still present";
  }
  // And an ordinary explain must not be planning against one either.
  json plain = srv->call_explain_query("", sql, json::array(), false, 0);
  EXPECT_EQ(plain.dump().find("btree_orders"), std::string::npos)
      << "explainQuery is planning against a leaked hypothetical index";
}

TEST_F(PostgresMCPServerTest, EvaluateIndexAnswersWhetherAnIndexIsSafeToDrop) {
  if (!hypopg_available(*srv)) GTEST_SKIP() << "hypopg is not installed here";
  // The fixture tables are a handful of rows, where a sequential scan costs the
  // same as an index scan and hiding the index changes nothing. The signal only
  // exists once the table is big enough for the planner to care.
  {
    pqxx::connection c(test_url);
    pqxx::nontransaction n(c);
    n.exec("DROP TABLE IF EXISTS grocery.hypo_probe");
    n.exec("CREATE TABLE grocery.hypo_probe (id bigint PRIMARY KEY, pad text)");
    n.exec("INSERT INTO grocery.hypo_probe "
           "SELECT g, repeat('x', 80) FROM generate_series(1, 20000) g");
    n.exec("ANALYZE grocery.hypo_probe");
  }
  json r = srv->call_evaluate_index("SELECT pad FROM grocery.hypo_probe WHERE id = 4242",
                                    json::array(), json::array({"grocery.hypo_probe_pkey"}));
  if (r.contains("error") &&
      r["error"].get<std::string>().find("1.4.0") != std::string::npos) {
    pqxx::connection c(test_url);
    pqxx::nontransaction n(c);
    n.exec("DROP TABLE IF EXISTS grocery.hypo_probe");
    GTEST_SKIP() << "hypopg predates hypopg_hide_index";
  }
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  ASSERT_TRUE(r.contains("hidden"));
  // Hiding the primary key must make the plan worse; that is the whole signal
  // that the index is not safe to drop.
  EXPECT_GT(r["hypothetical"]["total_cost"].get<double>(),
            r["baseline"]["total_cost"].get<double>()) << r["baseline"].dump();
  {
    pqxx::connection c(test_url);
    pqxx::nontransaction n(c);
    n.exec("DROP TABLE IF EXISTS grocery.hypo_probe");
  }
}

TEST_F(PostgresMCPServerTest, EvaluateIndexRefusesAnUnknownIndexClearly) {
  if (!hypopg_available(*srv)) GTEST_SKIP() << "hypopg is not installed here";
  json r = srv->call_evaluate_index("SELECT 1", json::array(),
                                    json::array({"no_such_index_xyz"}));
  if (r.value("error", "").find("1.4.0") != std::string::npos) GTEST_SKIP();
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  // Named, not a raw server error leaked through.
  EXPECT_NE(r["error"].get<std::string>().find("no index named"), std::string::npos)
      << r.dump(2);
}

TEST_F(PostgresMCPServerTest, EvaluateIndexNeedsSomethingToEvaluate) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "evaluateIndex"},
                                      {"arguments", {{"sql", "SELECT 1"}}}}}});
  EXPECT_TRUE(r["result"]["isError"].get<bool>()) << r.dump(2);
}

TEST_F(PostgresMCPServerTest, EvaluateIndexIsNeverSwept) {
  // Same reasoning as explainQuery: the statement is rarely valid elsewhere.
  json tools = srv->call_tools_list();
  for (const auto& t : tools) {
    if (t.value("name", "") != "evaluateIndex") continue;
    EXPECT_FALSE(t["inputSchema"]["properties"].contains("instance"));
    EXPECT_FALSE(t["inputSchema"]["properties"].contains("replication_group"));
    EXPECT_FALSE(t["inputSchema"]["properties"].contains("group"));
  }
}

// --- checkPrivileges ---

TEST_F(PostgresMCPServerTest, CheckPrivilegesCountsEveryToolExactlyOnce) {
  json r = srv->call_check_privileges();
  ASSERT_TRUE(r.contains("tools")) << r.dump(2);
  const size_t total    = r["tools"].get<size_t>();
  const size_t avail    = r["available"].get<size_t>();
  const size_t degraded = r.contains("degraded") ? r["degraded"].size() : 0;
  const size_t denied   = r.contains("denied")   ? r["denied"].size()   : 0;
  // Tools absent from both lists are fully available, so the arithmetic has to
  // close or the count is telling the caller something untrue.
  EXPECT_EQ(avail + degraded + denied, total);
  EXPECT_EQ(total, srv->call_tools_list().size());
  EXPECT_EQ(r["connection"].get<std::string>(), "default");
  EXPECT_FALSE(r["role"].get<std::string>().empty());
}

TEST_F(PostgresMCPServerTest, CheckPrivilegesNamesNoRolesAndNoGrants) {
  // Which predefined role gates a tool is PostgreSQL's business; the caller
  // needs to know what works. And emitting DDL would contradict what this
  // server tells every client about itself -- plans verbatim, no heuristics,
  // no generated DDL -- so an unavailable tool is described, never prescribed.
  const std::string dump = srv->call_check_privileges().dump();
  EXPECT_EQ(dump.find("GRANT"), std::string::npos) << dump;
  EXPECT_EQ(dump.find("memberships"), std::string::npos);
  for (const char* role : {"pg_monitor", "pg_read_all_stats", "pg_read_all_data",
                           "pg_stat_scan_tables", "pg_read_all_settings"})
    EXPECT_EQ(dump.find(role), std::string::npos) << role << " leaked into the payload";
}

TEST_F(PostgresMCPServerTest, CheckPrivilegesSeparatesNotInstalledFromNotPermitted) {
  // Two different fixes for the operator: CREATE EXTENSION versus a grant.
  // Conflating them is the same defect the 42501 handling fixed in 3.2.0.
  json r = srv->call_check_privileges();
  for (const auto& d : r.value("denied", json::array())) {
    const std::string reason = d["reason"].get<std::string>();
    const bool classified = reason.find("not installed") != std::string::npos ||
                            reason.find("restricted to") != std::string::npos ||
                            reason.find("readable only by") != std::string::npos;
    EXPECT_TRUE(classified) << d["tool"] << ": " << reason;
  }
}

TEST_F(PostgresMCPServerTest, CheckPrivilegesReportsARestrictedRoleAccurately) {
  const std::string role = "licht_cp_" + std::to_string(getpid());
  {
    pqxx::nontransaction n(*admin_conn);
    n.exec("DROP ROLE IF EXISTS \"" + role + "\"");
    n.exec("CREATE ROLE \"" + role + "\" LOGIN");
  }
  const std::string url = std::regex_replace(
      test_url, std::regex(R"(\buser\s*=\s*\S+)"), "") + " user=" + role;

  // Same two ways this cannot be exercised as the buffer-cache test: peer auth
  // refuses the login, and a pooler with a forced user hands back the
  // superuser's session whichever role was asked for.
  bool usable = false;
  try {
    pqxx::connection probe(url);
    pqxx::work t(probe);
    usable = (t.exec("SELECT current_user")[0][0].as<std::string>() == role);
  } catch (const std::exception&) {
  }

  if (usable) {
    PostgresMCPServer unpriv{url};
    json r = unpriv.call_check_privileges();
    EXPECT_EQ(r["role"].get<std::string>(), role);

    std::set<std::string> degraded, denied;
    for (const auto& d : r.value("degraded", json::array())) degraded.insert(d["tool"]);
    for (const auto& d : r.value("denied", json::array()))   denied.insert(d["tool"]);

    // The three that read row data are degraded, never denied: privilege is
    // per object, so this role may still hold SELECT on some tables. Calling
    // them unavailable would be as wrong as calling them available.
    for (const char* t : {"tableStats", "checkKey", "explainQuery"}) {
      EXPECT_TRUE(degraded.count(t)) << t << " should be degraded for a bare role";
      EXPECT_FALSE(denied.count(t)) << t << " is per-object, so it cannot be denied outright";
    }
    // pgstattuple is installed by the fixture, so this is a privilege denial
    // rather than an absent extension.
    EXPECT_TRUE(denied.count("tableBloat"));
    EXPECT_NE(r["available"].get<size_t>(), r["tools"].get<size_t>());
    // The catalog is world-readable, so the great majority still works.
    EXPECT_GT(r["available"].get<size_t>(), r["tools"].get<size_t>() * 3 / 4);
  } else {
    GTEST_SKIP() << "cannot log in as an unprivileged role here";
  }

  pqxx::nontransaction n(*admin_conn);
  n.exec("DROP ROLE IF EXISTS \"" + role + "\"");
}

TEST_F(PostgresMCPServerTest, BufferCacheDeniedNamesTheGrantRatherThanTheExtension) {
  // A valid read-only role missing a monitoring grant is a different problem
  // from an absent extension. Reporting it as "not installed" would send the
  // operator to CREATE EXTENSION for something already installed.
  const std::string role = "licht_unpriv_" + std::to_string(getpid());
  {
    pqxx::nontransaction n(*admin_conn);
    n.exec("DROP ROLE IF EXISTS \"" + role + "\"");
    n.exec("CREATE ROLE \"" + role + "\" LOGIN");
  }
  const std::string url = std::regex_replace(
      test_url, std::regex(R"(\buser\s*=\s*\S+)"), "") + " user=" + role;

  // Two ways this cannot be exercised, both of which must skip rather than
  // silently pass: peer/ident auth refuses the login outright, and a pooler
  // configured with a forced user (as the local rig's PgBouncer is) hands back
  // the superuser's session no matter which role was asked for.
  bool usable = false;
  std::string actual;
  try {
    pqxx::connection probe(url);
    pqxx::work t(probe);
    actual = t.exec("SELECT current_user")[0][0].as<std::string>();
    usable = (actual == role);
  } catch (const std::exception&) {
    actual = "(could not connect)";
  }

  if (usable) {
    PostgresMCPServer unpriv{url};
    // bufferCacheContents reads the view, which every version of the extension
    // has -- so this reaches the permission check on PostgreSQL 14 and 15 too,
    // where the summary functions do not exist and the version gate would
    // answer first.
    json r = unpriv.call_buffer_cache_contents(5);
    ASSERT_TRUE(r.contains("error")) << r.dump(2);
    EXPECT_NE(r["error"].get<std::string>().find("permission denied"), std::string::npos);
    EXPECT_NE(r["hint"].get<std::string>().find("GRANT pg_monitor"), std::string::npos);
    EXPECT_EQ(r["error"].get<std::string>().find("not installed"), std::string::npos);
  }

  {
    pqxx::nontransaction n(*admin_conn);
    n.exec("DROP ROLE IF EXISTS \"" + role + "\"");
  }
  if (!usable)
    GTEST_SKIP() << "cannot run as an unprivileged role here (got " << actual
                 << "); needs a direct connection with password or trust auth";
}

TEST_F(PostgresMCPServerTest, BufferCacheToolsAreRegistered) {
  json tools = srv->call_tools_list();
  int found = 0;
  for (const auto& t : tools) {
    const std::string n = t.value("name", "");
    if (n == "bufferCacheSummary" || n == "bufferCacheContents") found++;
  }
  EXPECT_EQ(found, 2);
}

// --- observed role ---

TEST_F(PostgresMCPServerTest, SessionObservesRoleOnTheSameRoundTripAsTheGuard) {
  auto reg = pglicht::ConnectionRegistry::from_url(test_url, "pg-licht-test");
  Session s{reg.get("default")};

  // The development database is a primary. The standby side of this is covered
  // by the pooled rig, which stands up a replica of its own.
  EXPECT_STREQ(s.role(), "primary");
  EXPECT_FALSE(s.in_recovery());

  // The property batching the probe onto the guard's statement could have
  // broken. If SET TRANSACTION READ ONLY ever stopped taking effect, every
  // tool would silently lose its write backstop, so assert it here rather
  // than trust that PQexec keeps running the first statement.
  EXPECT_THROW(s.txn().exec("CREATE TEMP TABLE role_probe_guard(i int)"),
               std::exception);
}

// A physical standby, when the pooled rig provided one. Plain ctest runs have
// no standby and skip: the replica side is genuinely untestable without one,
// and a silently-passing test would be worse than a skipped one.
namespace {
std::string standby_url() {
  const char* u = std::getenv("STANDBY_URL");
  return u ? std::string(u) : std::string();
}
}  // namespace

TEST(SessionRoleTest, ObservesTheReplicaSideOnAStandby) {
  if (standby_url().empty())
    GTEST_SKIP() << "no STANDBY_URL; run cpp/test/run-pooled-tests.sh";
  auto reg = pglicht::ConnectionRegistry::from_url(standby_url(), "pg-licht-test");
  Session s{reg.get("default")};
  EXPECT_STREQ(s.role(), "replica");
  EXPECT_TRUE(s.in_recovery());
}

// --- the statement ceiling ---

TEST_F(PostgresMCPServerTest, SessionAppliesTheConfiguredStatementTimeout) {
  // Transaction-scoped, like the read-only guard and for the same reason: a
  // transaction pooler discards session state but preserves this.
  auto reg = pglicht::ConnectionRegistry::from_url(test_url, "pg-licht-test");
  ASSERT_EQ(reg.get("default").statement_timeout_ms,
            pglicht::kDefaultStatementTimeoutMs);

  Session s{reg.get("default")};
  EXPECT_EQ(s.statement_timeout_ms(), pglicht::kDefaultStatementTimeoutMs);
  pqxx::result r = s.txn().exec("SHOW statement_timeout");
  EXPECT_EQ(r[0][0].as<std::string>(), "2min");
}

TEST_F(PostgresMCPServerTest, AZeroStatementTimeoutLeavesTheServerDefaultAlone) {
  // 0 must not be spelled as "0ms": setting it explicitly would override a
  // server that deliberately configures its own ceiling.
  auto reg = pglicht::ConnectionRegistry::from_url(test_url, "pg-licht-test");
  pglicht::ConnConfig cfg = reg.get("default");
  cfg.statement_timeout_ms = 0;

  Session s{cfg};
  EXPECT_EQ(s.statement_timeout_ms(), 0);
  pqxx::result r = s.txn().exec(
    "SELECT current_setting('statement_timeout') = boot_val "
    "  FROM pg_settings WHERE name = 'statement_timeout'");
  EXPECT_TRUE(r[0][0].as<bool>()) << "the session set a timeout it should not have";
}

TEST_F(PostgresMCPServerTest, AnExceededCeilingArrivesAsSqlstate57014) {
  // The whole reporting path turns on this code being readable. It is the
  // mirror image of the insufficient_privilege case fixed in 3.2.0, where
  // libpqxx leaves sqlstate() empty: here there is no dedicated exception
  // class, so it arrives as a plain sql_error that does carry its code.
  auto reg = pglicht::ConnectionRegistry::from_url(test_url, "pg-licht-test");
  Session s{reg.get("default"), 100};
  ASSERT_EQ(s.statement_timeout_ms(), 100);
  try {
    s.txn().exec("SELECT pg_sleep(3)");
    FAIL() << "expected the statement to be cancelled";
  } catch (const pqxx::sql_error& e) {
    EXPECT_EQ(e.sqlstate(), "57014") << e.what();
  }
}

TEST_F(PostgresMCPServerTest, AnExplicitTimeoutOverridesTheConnectionCeiling) {
  // explainQuery is the one caller that states how long it will wait.
  auto reg = pglicht::ConnectionRegistry::from_url(test_url, "pg-licht-test");
  Session s{reg.get("default"), 250};
  EXPECT_EQ(s.statement_timeout_ms(), 250);
  pqxx::result r = s.txn().exec("SHOW statement_timeout");
  EXPECT_EQ(r[0][0].as<std::string>(), "250ms");
}

TEST_F(PostgresMCPServerTest, RoleIsObservedFreshlyForEverySession) {
  // Nothing caches it: two sessions each ask. A cached role would go stale at
  // a failover, which is exactly when it would be read.
  auto reg = pglicht::ConnectionRegistry::from_url(test_url, "pg-licht-test");
  Session a{reg.get("default")};
  Session b{reg.get("default")};
  EXPECT_STREQ(a.role(), b.role());
  EXPECT_STREQ(a.role(), "primary");
}

// --- topology tools ---

namespace {

// A server built from an INI, so the topology tools run against a real
// registry. probe() only touches the *default* connection, so the extra
// sections may name anything: nothing ever connects to them.
class TopologyFixture : public PostgresMCPServerTest {
protected:
  // One INI section addressing `url`, plus whatever topology keys are wanted.
  static std::string section(const std::string& name, const std::string& url,
                             const std::string& extra) {
    std::string out = "[" + name + "]\n";
    std::istringstream in(url);
    std::string tok;
    while (in >> tok) {
      size_t eq = tok.find('=');
      if (eq != std::string::npos)
        out += tok.substr(0, eq) + " = " + tok.substr(eq + 1) + "\n";
    }
    return out + extra;
  }
  static std::string ini_with(const std::string& extra) {
    return section("default", test_url, extra);
  }
  // A connection that cannot be reached: port 1 refuses immediately.
  //
  // Immediately is the point -- this exercises the error path, not the timeout
  // path. For a host that never answers at all, use blackholed() below; the two
  // fail in different ways and only the second is bounded by connect_timeout.
  static std::string unreachable(const std::string& name) {
    return "[" + name + "]\nhost = 127.0.0.1\nport = 1\ndbname = nowhere\n";
  }
  // A connection whose packets go nowhere. 192.0.2.0/24 is TEST-NET-1
  // (RFC 5737): reserved for documentation, routed nowhere, so a SYN to it is
  // dropped rather than refused. That is the case a connect timeout exists
  // for, and the one a decommissioned or firewalled database presents.
  static std::string blackholed(const std::string& name, const std::string& extra) {
    return "[" + name + "]\nhost = 192.0.2.1\nport = 5432\ndbname = nowhere\n" + extra;
  }
  static std::string standby_url_or_empty() {
    const char* u = std::getenv("STANDBY_URL");
    return u ? std::string(u) : std::string();
  }
  static int count_severity(const json& findings, const char* sev) {
    int n = 0;
    for (const auto& f : findings) { if (f["severity"] == sev) n++; }
    return n;
  }
  static bool has_topic(const json& findings, const char* topic) {
    for (const auto& f : findings) { if (f["topic"] == topic) return true; }
    return false;
  }
};

std::unique_ptr<PostgresMCPServer> server_from(const std::string& body) {
  TempIni ini(body);
  return std::make_unique<PostgresMCPServer>(
      pglicht::ConnectionRegistry::from_ini(ini.path(), "pg-licht-test"));
}

}  // namespace

TEST_F(TopologyFixture, ListTopologyIndexesTheThreeAxes) {
  auto s = server_from(ini_with(
      "instance = pg-01\ngroup = prod\n"
      "[ro]\nhost = h9\nport = 5432\ndbname = ro\n"
      "instance = pg-02\nreplication_group = ha\ngroup = prod, reporting\n"));
  json t = s->call_topology();

  ASSERT_EQ(t["instances"].size(), 2u);
  EXPECT_EQ(t["instances"][0]["name"], "pg-01");
  EXPECT_EQ(t["instances"][0]["source"], "declared");
  EXPECT_EQ(t["instances"][0]["connections"][0], "default");

  ASSERT_EQ(t["replication_groups"].size(), 1u);
  EXPECT_EQ(t["replication_groups"][0]["name"], "ha");
  EXPECT_EQ(t["replication_groups"][0]["connections"].size(), 1u);

  // A group spans instances; "prod" holds both, in file order.
  ASSERT_EQ(t["groups"].size(), 2u);
  json prod;
  for (const auto& g : t["groups"]) if (g["name"] == "prod") prod = g;
  ASSERT_FALSE(prod.is_null());
  ASSERT_EQ(prod["connections"].size(), 2u);
  EXPECT_EQ(prod["connections"][0], "default");
  EXPECT_EQ(prod["connections"][1], "ro");

  EXPECT_TRUE(t["unlabelled"].empty());
}

TEST_F(TopologyFixture, ListTopologyNamesConnectionsWithNoLabelsAtAll) {
  auto s = server_from(ini_with("[lonely]\ndbname = lonely\n"));
  json t = s->call_topology();
  ASSERT_EQ(t["unlabelled"].size(), 2u);
  EXPECT_EQ(t["unlabelled"][0], "default");
  EXPECT_EQ(t["unlabelled"][1], "lonely");
  EXPECT_TRUE(t["instances"].empty());
}

TEST_F(TopologyFixture, ListTopologyReportsInferredInstancesAsSuch) {
  auto s = server_from(ini_with(
      "[a]\nhost = h1\nport = 5432\ndbname = a\n"
      "[b]\nhost = h1\nport = 5432\ndbname = b\n"));
  json t = s->call_topology();
  ASSERT_EQ(t["instances"].size(), 1u);
  EXPECT_EQ(t["instances"][0]["name"], "h1:5432");
  // Inferred is a hint for grouping output, not evidence of shared memory, so
  // the caller has to be able to tell the two apart.
  EXPECT_EQ(t["instances"][0]["source"], "inferred");
}

TEST_F(TopologyFixture, ListTopologyCarriesInstanceCapacity) {
  auto s = server_from(ini_with(
      "instance = pg-01\n"
      "[instance:pg-01]\nhost_ram_mb = 65536\nhost_vcpus = 16\n"));
  json t = s->call_topology();
  ASSERT_EQ(t["instances"].size(), 1u);
  EXPECT_EQ(t["instances"][0]["host_ram_mb"], 65536);
  EXPECT_EQ(t["instances"][0]["host_vcpus"], 16);
}

TEST_F(TopologyFixture, ListConnectionsCarriesTopologyLabels) {
  auto s = server_from(ini_with(
      "instance = pg-01\nreplication_group = ha\ngroup = prod, billing\n"));
  json c = s->call_connections()["connections"];
  ASSERT_EQ(c.size(), 1u);
  EXPECT_EQ(c[0]["instance"], "pg-01");
  EXPECT_EQ(c[0]["instance_source"], "declared");
  EXPECT_EQ(c[0]["replication_group"], "ha");
  ASSERT_EQ(c[0]["groups"].size(), 2u);
  EXPECT_EQ(c[0]["groups"][0], "prod");
  // Role is observed, never configured, so it must not appear here.
  EXPECT_FALSE(c[0].contains("role"));
}

TEST_F(TopologyFixture, VerifyTopologyReportsWhatEachServerActuallyIs) {
  auto s = server_from(ini_with(""));
  json v = s->call_verify_topology();

  ASSERT_EQ(v["connections"].size(), 1u);
  const json& c = v["connections"][0];
  EXPECT_EQ(c["connection"], "default");
  EXPECT_EQ(c["role"], "primary");
  EXPECT_EQ(c["database"], test_dbname);
  // 64-bit: a system identifier does not survive JSON number precision, so it
  // must arrive as a string, like query_id.
  ASSERT_TRUE(c.contains("system_identifier")) << v.dump(2);
  EXPECT_TRUE(c["system_identifier"].is_string()) << v.dump(2);
  EXPECT_FALSE(c["system_identifier"].get<std::string>().empty());

  // Nothing is declared, so there is nothing to contradict.
  EXPECT_EQ(count_severity(v["findings"], "error"), 0) << v.dump(2);
}

TEST_F(TopologyFixture, VerifyTopologyAcceptsATrueInstanceDeclaration) {
  // Two names for the same server, declared as one instance: true by
  // construction, so no error may be raised.
  auto s = server_from(ini_with("instance = pg-01\n") +
                       section("twin", test_url, "instance = pg-01\n"));
  json v = s->call_verify_topology();
  EXPECT_EQ(count_severity(v["findings"], "error"), 0) << v.dump(2);
  EXPECT_EQ(v["connections"][0]["instance"], "pg-01");
}

TEST_F(TopologyFixture, VerifyTopologyFlagsALineageTheConfigDoesNotDeclare) {
  // Same server under two names, declared as two separate instances. They hold
  // the same data and neither declaration ties them together -- the case where
  // a sweep silently misses a server that carries real workload.
  auto s = server_from(ini_with("instance = pg-01\n") +
                       section("twin", test_url, "instance = pg-02\n"));
  json v = s->call_verify_topology();
  ASSERT_TRUE(has_topic(v["findings"], "system_identifier")) << v.dump(2);
}

TEST_F(TopologyFixture, VerifyTopologyCallsTwoPrimariesSplitBrain) {
  // Two names for the same primary, declared as a replication group. Exactly
  // the shape of split brain, and the tool must report both rather than pick.
  auto s = server_from(ini_with("replication_group = ha\n") +
                       section("twin", test_url, "replication_group = ha\n"));
  json v = s->call_verify_topology();

  bool split = false;
  for (const auto& f : v["findings"]) {
    if (f["topic"] == "replication_group" &&
        f["detail"].get<std::string>().find("split") != std::string::npos) {
      split = true;
      EXPECT_NE(f["detail"].get<std::string>().find("default"), std::string::npos);
      EXPECT_NE(f["detail"].get<std::string>().find("twin"), std::string::npos);
    }
  }
  EXPECT_TRUE(split) << v.dump(2);
}

TEST_F(TopologyFixture, VerifyTopologySurvivesAnUnreachableMember) {
  // A partial answer during an incident beats an exception, so the sweep must
  // finish and say plainly what it could not reach.
  auto s = server_from(ini_with("") + unreachable("dead"));
  json v = s->call_verify_topology();

  ASSERT_EQ(v["connections"].size(), 2u);
  const json& dead = v["connections"][1];
  EXPECT_EQ(dead["connection"], "dead");
  ASSERT_TRUE(dead.contains("error")) << v.dump(2);
  EXPECT_FALSE(dead.contains("role"));
  // The reachable one is still fully reported.
  EXPECT_EQ(v["connections"][0]["role"], "primary");
  EXPECT_TRUE(has_topic(v["findings"], "reachability")) << v.dump(2);
}

TEST_F(TopologyFixture, VerifyTopologyReadsARealStandbyAsAReplica) {
  const std::string sb = standby_url_or_empty();
  if (sb.empty()) GTEST_SKIP() << "no STANDBY_URL; run cpp/test/run-pooled-tests.sh";

  auto s = server_from(ini_with("replication_group = ha\n") +
                       section("replica", sb, "replication_group = ha\n"));
  json v = s->call_verify_topology();

  ASSERT_EQ(v["connections"].size(), 2u);
  EXPECT_EQ(v["connections"][0]["role"], "primary");
  EXPECT_EQ(v["connections"][1]["role"], "replica");
  // A physical replica carries its primary's identifier: that is what makes
  // the identifier a lineage rather than a postmaster.
  EXPECT_EQ(v["connections"][0]["system_identifier"],
            v["connections"][1]["system_identifier"]);
  // One primary, one lineage, correctly declared: nothing to report.
  EXPECT_EQ(count_severity(v["findings"], "error"), 0) << v.dump(2);
}

TEST_F(TopologyFixture, VerifyTopologyRejectsAStandbyDeclaredAsTheSameInstance) {
  const std::string sb = standby_url_or_empty();
  if (sb.empty()) GTEST_SKIP() << "no STANDBY_URL; run cpp/test/run-pooled-tests.sh";

  // Same identifier, different servers. They share WAL lineage, not memory, so
  // calling them one instance would license a contention claim that is false.
  auto s = server_from(ini_with("instance = wrong\n") +
                       section("replica", sb, "instance = wrong\n"));
  json v = s->call_verify_topology();

  bool flagged = false;
  for (const auto& f : v["findings"]) {
    if (f["topic"] == "instance" && f["severity"] == "error" &&
        f["detail"].get<std::string>().find("replication_group") != std::string::npos)
      flagged = true;
  }
  EXPECT_TRUE(flagged) << v.dump(2);
}

// --- dual-era protocol ---


TEST_F(PostgresMCPServerTest, TheSameCallDrivenBothWaysReturnsIdenticalContent) {
  // Through 3.2.1 this asserted that the two eras returned identical `content`.
  // 4.0.0 changes the carrier deliberately -- a client that negotiated
  // 2025-06-18 receives structuredContent and no text block -- so what has to
  // hold now is that the *payload* is the same either way, which is the claim
  // that actually mattered.
  json legacy = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                               {"params", {{"name", "listSchemas"},
                                           {"arguments", json::object()}}}});
  json modern = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/call"},
                               {"params", {{"name", "listSchemas"},
                                           {"arguments", json::object()},
                                           {"_meta", modern_meta()}}}});
  auto payload_of = [](const json& r) {
    const json& res = r["result"];
    if (res.contains("structuredContent")) return res["structuredContent"];
    return json::parse(res["content"][0]["text"].get<std::string>());
  };
  EXPECT_EQ(payload_of(legacy), payload_of(modern));
}

TEST_F(PostgresMCPServerTest, EveryClientGetsATextBlockByDefault) {
  // 4.0.0 sent structuredContent *instead of* the text block to a client that
  // negotiated 2025-06-18, and a first-party client that advertises that
  // revision while reading `content` was left with nothing -- reported as "the
  // content was missing from the response object". A client can advertise a
  // revision it does not fully implement, and this server cannot tell.
  //
  // So the default is what the spec advises: both.
  json legacy = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                               {"params", {{"name", "listSchemas"},
                                           {"arguments", json::object()}}}});
  EXPECT_TRUE(legacy["result"].contains("content"));
  EXPECT_FALSE(legacy["result"].contains("structuredContent"))
      << "a pre-2025-06-18 client must not be sent a field its revision lacks";

  json modern = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/call"},
                               {"params", {{"name", "listSchemas"},
                                           {"arguments", json::object()},
                                           {"_meta", modern_meta()}}}});
  EXPECT_TRUE(modern["result"].contains("structuredContent"));
  EXPECT_TRUE(modern["result"].contains("content"))
      << "the text block is what a client reading `content` needs to see";

  // Whichever it reads, it reads the same thing.
  EXPECT_EQ(modern["result"]["structuredContent"],
            json::parse(modern["result"]["content"][0]["text"].get<std::string>()));
}

TEST_F(PostgresMCPServerTest, StructuredContentStartsAtTheRevisionThatDefinedIt) {
  // 2025-03-26 brought annotations but not structuredContent, so a client
  // there must still get the text block. The boundary is the revision that
  // defined the feature, not "modern versus legacy".
  for (const auto& [proto, structured] :
       std::vector<std::pair<std::string, bool>>{{"2024-11-05", false},
                                                 {"2025-03-26", false},
                                                 {"2025-06-18", true},
                                                 {"2025-11-25", true}}) {
    PostgresMCPServer s(test_url);
    s.call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "initialize"},
                {"params", {{"protocolVersion", proto},
                            {"capabilities", json::object()},
                            {"clientInfo", {{"name", "t"}, {"version", "0"}}}}}});
    json r = s.call_rpc({{"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/call"},
                         {"params", {{"name", "listSchemas"},
                                     {"arguments", json::object()}}}});
    EXPECT_EQ(r["result"].contains("structuredContent"), structured) << proto;
    // The text block goes to everyone by default, whatever the revision.
    EXPECT_TRUE(r["result"].contains("content")) << proto;
  }
}

TEST_F(PostgresMCPServerTest, TheTextBlockIsSerialisedCompactly) {
  // Indentation was 18-39% of the payload across real catalog reads, and no
  // consumer reads it -- they all parse the text as JSON.
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "listTables"},
                                      {"arguments", {{"schema", "grocery"}}}}}});
  const std::string text = r["result"]["content"][0]["text"].get<std::string>();
  EXPECT_EQ(text.find("\n"), std::string::npos) << "text block is still pretty-printed";
  // Still valid JSON, and still the same payload.
  json reparsed;
  ASSERT_NO_THROW(reparsed = json::parse(text));
  EXPECT_TRUE(reparsed.is_object());
}

TEST_F(PostgresMCPServerTest, EveryToolPayloadIsAnObject) {
  // structuredContent may only be a JSON object. A tool returning a top-level
  // array or null is unrepresentable in the format modern clients receive, so
  // this is an invariant now rather than a coincidence. currentLocks and
  // listConnections were the two arrays; an empty result set was the null.
  for (const auto& [tool, args] : std::vector<std::pair<std::string, json>>{
           {"listConnections", json::object()},
           {"currentLocks", json::object()},
           {"listSchemas", json::object()},
           {"listEnums", {{"schema", "pg_catalog"}}},   // empty result set
           {"listTables", {{"schema", "grocery"}}}}) {
    json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                            {"params", {{"name", tool},
                                        {"arguments", args},
                                        {"_meta", modern_meta()}}}});
    ASSERT_TRUE(r["result"].contains("structuredContent")) << tool << r.dump(2);
    EXPECT_TRUE(r["result"]["structuredContent"].is_object())
        << tool << " returns a " << r["result"]["structuredContent"].type_name();
  }
}

TEST_F(PostgresMCPServerTest, ALegacyResponseIsUnchangedFrom311) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "listSchemas"},
                                      {"arguments", json::object()}}}});
  // A legacy client keeps the text-block carrier and gains none of the
  // modern-only result fields. It does *not* get 3.1.1's bytes any more:
  // 4.0.0 compacted the serialisation and changed three payload shapes. That
  // is the release being a major, not a regression -- but the envelope claim
  // still has to hold, because a legacy client parses it.
  EXPECT_TRUE(r["result"].contains("content"));
  EXPECT_FALSE(r["result"].contains("resultType"));
  EXPECT_FALSE(r["result"].contains("_meta"));
}

TEST_F(PostgresMCPServerTest, AModernResponseCarriesResultTypeAndServerInfo) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "listSchemas"},
                                      {"arguments", json::object()},
                                      {"_meta", modern_meta()}}}});
  EXPECT_EQ(r["result"]["resultType"], "complete");
  ASSERT_TRUE(r["result"]["_meta"].contains(kProtoSrvInfo)) << r.dump(2);
  EXPECT_EQ(r["result"]["_meta"][kProtoSrvInfo]["name"], "pg-licht-cpp");
}

TEST_F(PostgresMCPServerTest, ALegacyProgressTokenIsNotMistakenForAModernRequest) {
  // _meta is not new: progressToken has lived there since the legacy
  // revisions. Discriminating on _meta itself rather than on the specific
  // version key would reject a 3.1.1 client for using a legacy feature
  // correctly.
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "listSchemas"},
                                      {"arguments", json::object()},
                                      {"_meta", {{"progressToken", 7}}}}}});
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  EXPECT_FALSE(r["result"]["isError"].get<bool>());
  EXPECT_FALSE(r["result"].contains("resultType"));   // still legacy
}

TEST_F(PostgresMCPServerTest, AModernRequestMissingClientCapabilitiesIsMalformed) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "listSchemas"},
                                      {"arguments", json::object()},
                                      {"_meta", {{kProtoVersion, "2026-07-28"}}}}}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32602);
  EXPECT_NE(r["error"]["message"].get<std::string>().find("clientCapabilities"),
            std::string::npos);
}

TEST_F(PostgresMCPServerTest, AnUnsupportedProtocolVersionIsRejectedWithWhatIsSupported) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                          {"params", {{"name", "listSchemas"},
                                      {"arguments", json::object()},
                                      {"_meta", modern_meta("1999-01-01")}}}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32022);
  ASSERT_TRUE(r["error"]["data"].contains("supported")) << r.dump(2);
  EXPECT_EQ(r["error"]["data"]["requested"], "1999-01-01");
  bool has_modern = false;
  for (const auto& v : r["error"]["data"]["supported"]) {
    if (v == "2026-07-28") has_modern = true;
  }
  EXPECT_TRUE(has_modern) << r.dump(2);
}

TEST_F(PostgresMCPServerTest, ServerDiscoverDescribesTheServerWithoutAHandshake) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "server/discover"},
                          {"params", {{"_meta", modern_meta()}}}});
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  const json& res = r["result"];
  EXPECT_EQ(res["resultType"], "complete");
  EXPECT_TRUE(res["capabilities"].contains("tools"));
  ASSERT_TRUE(res["supportedVersions"].is_array());
  EXPECT_GE(res["supportedVersions"].size(), 2u);

  // The instructions exist to say the things that otherwise live only inside
  // individual tool descriptions.
  const std::string ins = res["instructions"].get<std::string>();
  EXPECT_NE(ins.find("READ ONLY"), std::string::npos);
  EXPECT_NE(ins.find("explainQuery"), std::string::npos);
  EXPECT_NE(ins.find("connection"), std::string::npos);
  EXPECT_NE(ins.find("replication_group"), std::string::npos);
}

TEST_F(PostgresMCPServerTest, ModernClientsGetAnnotationsWithoutAHandshake) {
  // The stateless revision has no initialize, so annotations must be driven by
  // the per-request version rather than by anything negotiated.
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/list"},
                          {"params", {{"_meta", modern_meta()}}}});
  ASSERT_FALSE(r["result"]["tools"].empty()) << r.dump(2);
  EXPECT_TRUE(r["result"]["tools"][0].contains("annotations")) << r.dump(2);
  EXPECT_TRUE(r["result"]["tools"][0].contains("title"));
}

TEST_F(PostgresMCPServerTest, NotificationsInitializedRemainsALegacyNoOp) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"method", "notifications/initialized"},
                          {"params", json::object()}});
  EXPECT_TRUE(r.empty()) << r.dump(2);
}

TEST_F(PostgresMCPServerTest, InitializeEchoesTheNegotiatedVersionOverTheWire) {
  json r = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "initialize"},
                          {"params", {{"protocolVersion", "2025-06-18"}}}});
  EXPECT_EQ(r["result"]["protocolVersion"], "2025-06-18");
  // ...and from then on tools/list carries what that revision defines.
  json tools = srv->call_rpc({{"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"},
                              {"params", json::object()}});
  EXPECT_TRUE(tools["result"]["tools"][0].contains("title")) << tools.dump(2);
}

// --- fan-out dispatch ---

namespace {

json rpc_call(PostgresMCPServer& srv, const std::string& tool, json args) {
  return srv.call_rpc({{"jsonrpc", "2.0"}, {"id", 1}, {"method", "tools/call"},
                       {"params", {{"name", tool}, {"arguments", args}}}});
}

// The tool payload, unwrapped from the MCP text content block.
// Era-aware: a modern client receives structuredContent and no text block, a
// legacy one receives the text block and nothing else. Tests that only care
// about the payload should not have to know which.
json rpc_payload(const json& response) {
  const json& r = response["result"];
  if (r.contains("structuredContent")) return r["structuredContent"];
  return json::parse(r["content"][0]["text"].get<std::string>());
}

}  // namespace

TEST_F(TopologyFixture, ASingleConnectionCallCarriesNoSweepEnvelope) {
  // The whole release rests on this: naming one connection must behave exactly
  // as it did in 3.1.1, envelope and all.
  auto s = server_from(ini_with("instance = pg-01\n"));
  json r = rpc_call(*s, "listSchemas", {{"connection", "default"}});
  ASSERT_FALSE(r["result"]["isError"].get<bool>()) << r.dump(2);
  json p = rpc_payload(r);
  EXPECT_FALSE(p.contains("members"));
  EXPECT_FALSE(p.contains("axis"));
}

TEST_F(TopologyFixture, InstanceSweepReturnsOneResultPerMember) {
  auto s = server_from(ini_with("instance = pg-01\n") +
                       section("twin", test_url, "instance = pg-01\n"));
  json p = rpc_payload(rpc_call(*s, "listSchemas", {{"instance", "pg-01"}}));

  EXPECT_EQ(p["axis"], "instance");
  EXPECT_EQ(p["name"], "pg-01");
  ASSERT_EQ(p["members"].size(), 2u);
  // File order, so a sweep is reproducible.
  EXPECT_EQ(p["members"][0]["connection"], "default");
  EXPECT_EQ(p["members"][1]["connection"], "twin");
  for (const auto& m : p["members"]) {
    EXPECT_EQ(m["role"], "primary") << m.dump(2);
    EXPECT_EQ(m["instance"], "pg-01");
    ASSERT_TRUE(m.contains("result")) << m.dump(2);
    EXPECT_FALSE(m.contains("error"));
  }
}

TEST_F(TopologyFixture, SweepingAnInstanceWideReadingAcrossDatabasesIsRefused) {
  // Eight databases on one postmaster would return the same rows eight times
  // with nothing in the payload to say so.
  auto s = server_from(ini_with("instance = pg-01\n") +
                       section("twin", test_url, "instance = pg-01\n"));
  json r = rpc_call(*s, "currentActivity", {{"instance", "pg-01"}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32602);
  EXPECT_NE(r["error"]["message"].get<std::string>().find("instance-wide"),
            std::string::npos);
}

TEST_F(TopologyFixture, SweepingAByteIdenticalReadingAcrossReplicasIsRefused) {
  auto s = server_from(ini_with("replication_group = ha\n") +
                       section("twin", test_url, "replication_group = ha\n"));
  json r = rpc_call(*s, "tableBloat", {{"replication_group", "ha"}, {"schema", "grocery"}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32602);
  EXPECT_NE(r["error"]["message"].get<std::string>().find("byte-identical"),
            std::string::npos);
}

TEST_F(TopologyFixture, IndexToolsMaySweepAReplicationGroup) {
  // duplicateIndexes reports idx_scan, which is each server's own. This is the
  // case the release exists for, so it must not be refused.
  auto s = server_from(ini_with("replication_group = ha\n") +
                       section("twin", test_url, "replication_group = ha\n"));
  json r = rpc_call(*s, "duplicateIndexes", {{"replication_group", "ha"}, {"schema", "grocery"}});
  ASSERT_FALSE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(rpc_payload(r)["members"].size(), 2u);
}

TEST_F(TopologyFixture, AGroupSweepCollapsesMembersThatWouldAnswerIdentically) {
  // A group may span instances, so it cannot be refused outright -- but two
  // databases on one postmaster answering an instance-wide question would
  // duplicate, and a caller reading two identical answers concludes agreement.
  auto s = server_from(ini_with("instance = pg-01\ngroup = prod\n") +
                       section("twin", test_url, "instance = pg-01\ngroup = prod\n"));
  json p = rpc_payload(rpc_call(*s, "currentActivity", {{"group", "prod"}}));

  EXPECT_EQ(p["members"].size(), 1u);
  ASSERT_TRUE(p.contains("skipped")) << p.dump(2);
  ASSERT_EQ(p["skipped"].size(), 1u);
  EXPECT_EQ(p["skipped"][0]["connection"], "twin");
  EXPECT_NE(p["skipped"][0]["reason"].get<std::string>().find("instance-wide"),
            std::string::npos);
}

TEST_F(TopologyFixture, AnUnreachableMemberDoesNotFailTheSweep) {
  auto s = server_from(ini_with("group = prod\n") +
                       "[dead]\nhost = 127.0.0.1\nport = 1\ndbname = nowhere\ngroup = prod\n");
  json p = rpc_payload(rpc_call(*s, "listSchemas", {{"group", "prod"}}));

  ASSERT_EQ(p["members"].size(), 2u);
  EXPECT_TRUE(p["members"][0].contains("result"));
  ASSERT_TRUE(p["members"][1].contains("error")) << p.dump(2);
  // A member that never connected must not inherit the previous member's role.
  EXPECT_EQ(p["members"][1]["role"], "unknown");
}

TEST_F(TopologyFixture, ASweepBoundsTheConnectThatDoesTheWork) {
  // Until 3.2.1 the bounded connect reached only verifyTopology and the
  // optional role probe. The connection each member's tool actually opened
  // went through active_cfg() unbounded, so a blackholed member cost the
  // kernel's SYN-retry budget -- six retries, about 127 seconds, on a Linux
  // default -- and sweeps are sequential, so every such member added that to
  // the whole call.
  //
  // The bound is 5 seconds. The assertion is deliberately loose: what it has
  // to separate is "bounded" from "waiting on the kernel", and any figure
  // between the two settles that without turning a slow CI runner into a
  // failure.
  auto s = server_from(ini_with("group = prod\n") + blackholed("gone", "group = prod\n"));

  const auto start = std::chrono::steady_clock::now();
  json p = rpc_payload(rpc_call(*s, "listSchemas", {{"group", "prod"}}));
  const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - start).count();

  ASSERT_EQ(p["members"].size(), 2u) << p.dump(2);
  EXPECT_TRUE(p["members"][0].contains("result")) << p.dump(2);
  ASSERT_TRUE(p["members"][1].contains("error")) << p.dump(2);
  EXPECT_LT(elapsed, 60) << "the sweep waited on the kernel rather than on "
                            "connect_timeout: " << elapsed << "s";
}

TEST_F(TopologyFixture, AnExceededCeilingIsReportedAsTheCeilingNotAnExecutionError) {
  // A ceiling reached and a query that failed call for opposite responses, and
  // PostgreSQL's own message does not distinguish them for the caller. 1ms is
  // below what any catalog read takes, so the tool's own query trips it -- the
  // read-only guard and set_config both run before it takes effect.
  auto s = server_from(ini_with("statement_timeout_ms = 1\n"));
  json r = rpc_call(*s, "listTables", {{"schema", "grocery"}});

  const std::string text = r["result"]["content"][0]["text"].get<std::string>();
  if (!r["result"]["isError"].get<bool>())
    GTEST_SKIP() << "the catalog read finished inside 1ms: " << text;

  json p = json::parse(text);
  ASSERT_TRUE(p.is_object()) << text;
  EXPECT_NE(p.value("error", "").find("statement_timeout"), std::string::npos) << text;
  EXPECT_EQ(p.value("timeout_ms", 0), 1) << text;
  // The hint has to name the knob, or the caller has no way to act on it.
  EXPECT_NE(p.value("hint", "").find("statement_timeout_ms"), std::string::npos) << text;
}

TEST_F(TopologyFixture, ASweepLeavesNoBoundedConfigBehindIt) {
  // The sweep swaps in a connection config carrying the bounded connect. If it
  // survived the call, the next single-connection call would silently inherit
  // it -- a bug that only shows up long after the call that caused it.
  auto s = server_from(ini_with("group = prod\n") +
                       section("twin", test_url, "group = prod\n"));
  rpc_payload(rpc_call(*s, "listSchemas", {{"group", "prod"}}));

  json r = rpc_call(*s, "listConnections", json::object());
  ASSERT_FALSE(r["result"]["isError"].get<bool>()) << r.dump(2);
  json p = rpc_payload(r);
  EXPECT_FALSE(p.contains("members")) << p.dump(2);
}

TEST_F(TopologyFixture, MoreThanOneTargetIsRejected) {
  auto s = server_from(ini_with("group = prod\n"));
  json r = rpc_call(*s, "listSchemas", {{"connection", "default"}, {"group", "prod"}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32602);
  EXPECT_NE(r["error"]["message"].get<std::string>().find("connection"), std::string::npos);
  EXPECT_NE(r["error"]["message"].get<std::string>().find("group"), std::string::npos);
}

TEST_F(TopologyFixture, RoleAppliesOnlyAcrossServers) {
  auto s = server_from(ini_with("instance = pg-01\n") +
                       section("twin", test_url, "instance = pg-01\n"));
  json r = rpc_call(*s, "listSchemas", {{"instance", "pg-01"}, {"role", "primary"}});
  ASSERT_TRUE(r.contains("error")) << r.dump(2);
  EXPECT_EQ(r["error"]["code"], -32602);

  json bad = rpc_call(*s, "listSchemas", {{"group", "nope"}, {"role", "leader"}});
  ASSERT_TRUE(bad.contains("error")) << bad.dump(2);
  EXPECT_EQ(bad["error"]["code"], -32602);
}

TEST_F(TopologyFixture, TwoPrimariesAreSweptAndNamedRatherThanChosen) {
  auto s = server_from(ini_with("replication_group = ha\n") +
                       section("twin", test_url, "replication_group = ha\n"));
  json p = rpc_payload(rpc_call(*s, "duplicateIndexes",
                                {{"replication_group", "ha"}, {"role", "primary"}}));
  EXPECT_EQ(p["members"].size(), 2u);
  ASSERT_TRUE(p.contains("notes")) << p.dump(2);
  bool split = false;
  for (const auto& n : p["notes"]) {
    if (n.get<std::string>().find("split brain") != std::string::npos) split = true;
  }
  EXPECT_TRUE(split) << p.dump(2);
}

TEST_F(TopologyFixture, NoReplicaIsAFindingNotAnEmptyResult) {
  auto s = server_from(ini_with("replication_group = ha\n"));
  json p = rpc_payload(rpc_call(*s, "duplicateIndexes",
                                {{"replication_group", "ha"}, {"role", "replica"}}));
  EXPECT_TRUE(p["members"].empty());
  ASSERT_TRUE(p.contains("notes")) << p.dump(2);
  EXPECT_NE(p["notes"][0].get<std::string>().find("no member"), std::string::npos);
}

TEST_F(TopologyFixture, ExplainQueryAndRegistryToolsNeverSweep) {
  auto s = server_from(ini_with("group = prod\n"));
  for (const char* tool : {"explainQuery", "listTopology", "listConnections"}) {
    json r = rpc_call(*s, tool, {{"group", "prod"}});
    ASSERT_TRUE(r.contains("error")) << tool << ": " << r.dump(2);
    EXPECT_EQ(r["error"]["code"], -32602) << tool;
  }
}

TEST_F(TopologyFixture, AWideSweepNamesWhatItDropped) {
  // Silent truncation would read as "nothing else is affected".
  std::string ini = ini_with("group = wide\n");
  for (int i = 0; i < 40; i++)
    ini += section("c" + std::to_string(i), test_url, "group = wide\n");
  auto s = server_from(ini);

  json p = rpc_payload(rpc_call(*s, "listSchemas", {{"group", "wide"}}));
  EXPECT_EQ(p["members"].size(), 32u);
  ASSERT_TRUE(p.contains("skipped")) << p.dump(2);
  EXPECT_EQ(p["skipped"].size(), 41u - 32u);
  EXPECT_NE(p["skipped"][0]["reason"].get<std::string>().find("cap"), std::string::npos);
}

TEST_F(TopologyFixture, AParallelSweepKeepsConfigurationOrder) {
  // Members run concurrently now, so the order they finish in is arbitrary.
  // The payload must still be in configuration order, or a reader comparing
  // two sweeps -- which is the entire point of sweeping -- would be comparing
  // different rows. Results go to fixed slots rather than being appended.
  std::string ini = ini_with("group = ordered\n");
  std::vector<std::string> expected{"default"};
  for (int i = 0; i < 12; i++) {
    const std::string n = "m" + std::to_string(i);
    ini += section(n, test_url, "group = ordered\n");
    expected.push_back(n);
  }
  auto s = server_from(ini);

  // Twice, because a race that reorders would not do it every time.
  for (int run = 0; run < 2; run++) {
    json p = rpc_payload(rpc_call(*s, "listSchemas", {{"group", "ordered"}}));
    ASSERT_EQ(p["members"].size(), expected.size()) << p.dump(2);
    std::vector<std::string> got;
    for (const auto& m : p["members"]) got.push_back(m["connection"].get<std::string>());
    EXPECT_EQ(got, expected) << "run " << run << ": " << p.dump(2);
    // And every member actually answered, rather than one thread's result
    // landing in another's slot.
    for (const auto& m : p["members"])
      EXPECT_TRUE(m.contains("result")) << m.dump(2);
  }
}

TEST_F(TopologyFixture, AParallelSweepSurvivesAnUnreachableMember) {
  // One member that cannot be reached must not fail the sweep, and must not
  // take another member's slot with it.
  std::string ini = ini_with("group = mixed\n");
  ini += section("alive", test_url, "group = mixed\n");
  ini += "[dead]\nhost = 192.0.2.1\nport = 5432\ndbname = x\nuser = y\n"
         "group = mixed\nconnect_timeout = 1\n";
  ini += section("alive2", test_url, "group = mixed\n");
  auto s = server_from(ini);

  json p = rpc_payload(rpc_call(*s, "listSchemas", {{"group", "mixed"}}));
  ASSERT_EQ(p["members"].size(), 4u) << p.dump(2);
  EXPECT_EQ(p["members"][0]["connection"], "default");
  EXPECT_EQ(p["members"][2]["connection"], "dead");
  EXPECT_TRUE(p["members"][2].contains("error")) << p.dump(2);
  // The reachable members still answered.
  EXPECT_TRUE(p["members"][1].contains("result"));
  EXPECT_TRUE(p["members"][3].contains("result"));
}

TEST_F(TopologyFixture, UnknownTopologyNameFailsAtTheCallRatherThanSweepingNothing) {
  auto s = server_from(ini_with("group = prod\n"));
  json r = rpc_call(*s, "listSchemas", {{"group", "typo"}});
  ASSERT_FALSE(r["result"].is_null());
  EXPECT_TRUE(r["result"]["isError"].get<bool>()) << r.dump(2);
  EXPECT_NE(r["result"]["content"][0]["text"].get<std::string>().find("typo"),
            std::string::npos);
}

TEST_F(TopologyFixture, SweepArgumentsAreAdvertisedOnlyWhereTheyApply) {
  auto s = server_from(ini_with(""));
  json tools = s->call_tools_list();
  std::map<std::string, json> props;
  for (const auto& t : tools) props[t.value("name", "")] = t["inputSchema"]["properties"];

  // Instance-wide: no instance sweep offered, but a group may still span hosts.
  EXPECT_FALSE(props["currentActivity"].contains("instance"));
  EXPECT_TRUE(props["currentActivity"].contains("group"));
  // Byte-identical across replicas: no replication_group sweep, no role filter.
  EXPECT_FALSE(props["tableBloat"].contains("replication_group"));
  EXPECT_FALSE(props["tableBloat"].contains("role"));
  EXPECT_TRUE(props["tableBloat"].contains("instance"));
  // Per-server counters: both offered.
  EXPECT_TRUE(props["duplicateIndexes"].contains("replication_group"));
  EXPECT_TRUE(props["duplicateIndexes"].contains("role"));
  // Never sweeps.
  EXPECT_FALSE(props["explainQuery"].contains("group"));
  EXPECT_FALSE(props.count("listTopology") && props["listTopology"].contains("group"));
}

TEST_F(PostgresMCPServerTest, ListConnectionsOmitsTopologyWhenNoneIsConfigured) {
  // A DATABASE_URL deployment has one connection and no topology; the fields
  // must be absent rather than empty, so 3.1.1 output is unchanged.
  json c = srv->call_connections()["connections"];
  ASSERT_EQ(c.size(), 1u);
  EXPECT_FALSE(c[0].contains("instance"));
  EXPECT_FALSE(c[0].contains("replication_group"));
  EXPECT_FALSE(c[0].contains("groups"));
}

TEST_F(PostgresMCPServerTest, RegistryToolsTakeNoConnectionArgument) {
  // Neither reads a database, so neither has a target to be pointed at.
  json tools = srv->call_tools_list();
  int seen = 0;
  for (const auto& t : tools) {
    const std::string name = t.value("name", "");
    if (name != "listConnections" && name != "listTopology" &&
        name != "verifyTopology") continue;
    seen++;
    EXPECT_FALSE(t["inputSchema"]["properties"].contains("connection")) << name;
  }
  EXPECT_EQ(seen, 3);
}

TEST_F(PostgresMCPServerTest, EveryOtherToolTakesAConnectionArgument) {
  json tools = srv->call_tools_list();
  for (const auto& t : tools) {
    const std::string name = t.value("name", "");
    if (name == "listConnections" || name == "listTopology" ||
        name == "verifyTopology") continue;
    EXPECT_TRUE(t["inputSchema"]["properties"].contains("connection")) << name;
  }
}

TEST_F(PostgresMCPServerTest, ListConnectionsReportsDefaultWithoutSecrets) {
  json envelope = srv->call_connections();
  // 4.0.0: wrapped, for the same reason as currentLocks.
  ASSERT_TRUE(envelope.is_object());
  json result = envelope["connections"];
  ASSERT_TRUE(result.is_array());
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0]["name"].get<std::string>(), "default");
  EXPECT_TRUE(result[0]["default"].get<bool>());
  EXPECT_FALSE(result[0].contains("password"));
}

int main(int argc, char **argv) {
  if (!std::getenv("DATABASE_URL")) {
    std::cerr << "ERROR: DATABASE_URL environment variable is required to run tests.\n"
              << "  Example: DATABASE_URL=\"port=5555 dbname=pglitch\" " << argv[0] << "\n";
    return 1;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
