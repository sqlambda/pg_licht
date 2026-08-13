#include <gtest/gtest.h>
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
  EXPECT_TRUE(result["users"].contains("description"));
  EXPECT_TRUE(result["users"].contains("rows"));
  EXPECT_TRUE(result["users"].contains("size"));
  EXPECT_TRUE(result["users"].contains("seq_scan"));
  EXPECT_TRUE(result["users"].contains("idx_scan"));
  EXPECT_TRUE(result["users"].contains("n_live_tup"));
  EXPECT_TRUE(result["users"].contains("n_dead_tup"));
  EXPECT_TRUE(result["users"].contains("last_vacuum"));
  EXPECT_TRUE(result["users"].contains("last_analyze"));
  EXPECT_TRUE(result["users"].contains("columns"));
  EXPECT_TRUE(result["users"].contains("index_count"));
  EXPECT_TRUE(result["users"].contains("constraint_count"));
  EXPECT_GT(result["users"]["index_count"].get<int>(), 0);
  EXPECT_GT(result["users"]["constraint_count"].get<int>(), 0);
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
  EXPECT_TRUE(result.contains("rows"));
  EXPECT_TRUE(result.contains("size"));
  EXPECT_TRUE(result.contains("indexes_size"));
  EXPECT_TRUE(result.contains("description"));
  EXPECT_TRUE(result.contains("seq_scan"));
  EXPECT_TRUE(result.contains("idx_scan"));
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("indexes"));
  EXPECT_TRUE(result.contains("constraints"));
  EXPECT_TRUE(result.contains("foreign_keys"));
  EXPECT_TRUE(result.contains("referenced_by"));
  EXPECT_TRUE(result.contains("primary_key"));
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

TEST_F(PostgresMCPServerTest, TableIndexesHaveSizeField) {
  json result = srv->call_table("grocery", "users");
  EXPECT_FALSE(result["indexes"].empty());
  for (auto& [idx_name, idx_obj] : result["indexes"].items()) {
    EXPECT_TRUE(idx_obj.contains("size")) << "Index " << idx_name << " missing size";
    EXPECT_TRUE(idx_obj.contains("definition"));
    EXPECT_TRUE(idx_obj.contains("index_uses"));
  }
}

TEST_F(PostgresMCPServerTest, TableColumnsHaveTypeInfo) {
  json result = srv->call_table("grocery", "users");
  EXPECT_TRUE(result["columns"].contains("name"));
  EXPECT_TRUE(result["columns"]["name"].contains("type"));
  EXPECT_TRUE(result["columns"]["name"].contains("format_type"));
  EXPECT_TRUE(result["columns"]["name"].contains("not_null"));
}

TEST_F(PostgresMCPServerTest, TableColumnsIncludeStatsForAnalyzedTable) {
  json result = srv->call_table("grocery", "users");
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
  EXPECT_TRUE(result["toast"]["name"].get<std::string>().rfind("pg_toast", 0) == 0);
  EXPECT_TRUE(result["toast"].contains("size"));
  EXPECT_TRUE(result["toast"].contains("index_size"));
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
  ASSERT_TRUE(result.is_array());

  bool found = false;
  for (auto& lock : result) {
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
    if (!r.is_array()) continue;
    for (const auto& l : r) {
      if (l.contains("chain_depth") && l["chain_depth"].get<int>() > 0) chain = r;
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

// --- listTables / tableDetails version-gated columns ---

TEST_F(PostgresMCPServerTest, TableStatsIncludeFailedHotUpdatesOnPg16AndLater) {
  const bool pg16 = pg_server_version_num(test_url) >= 160000;

  json tables = srv->call_tables("grocery");
  ASSERT_TRUE(tables.contains("users"));
  // n_tup_newpage_upd counts updates that had to move the row to another
  // page: the direct measure of failed HOT updates.
  EXPECT_EQ(tables["users"].contains("n_tup_newpage_upd"), pg16);
  EXPECT_EQ(tables["users"].contains("last_seq_scan"), pg16);

  json detail = srv->call_table("grocery", "users");
  EXPECT_EQ(detail.contains("n_tup_newpage_upd"), pg16);
  EXPECT_EQ(detail.contains("last_seq_scan"), pg16);
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
  json all = srv->call_statement_stats(20, "", "", 0);
  ASSERT_FALSE(all["statements"].empty());
  std::string qid = all["statements"][0]["query_id"].get<std::string>();

  json one = srv->call_statement_stats(20, qid, "", 0);
  // Not necessarily a single row: pg_stat_statements keeps one entry per
  // (user, database) pair, so one queryid can legitimately match several.
  ASSERT_FALSE(one["statements"].empty()) << one.dump(2);
  EXPECT_LT(one["statements"].size(), all["statements"].size());
  for (const auto& s : one["statements"])
    EXPECT_EQ(s["query_id"].get<std::string>(), qid) << one.dump(2);
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
    EXPECT_NE(std::string(e.what()).find("PgBouncer"), std::string::npos);
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

TEST_F(PostgresMCPServerTest, ListConnectionsReportsDefaultWithoutSecrets) {
  json result = srv->call_connections();
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
