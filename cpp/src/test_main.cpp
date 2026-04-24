#include <gtest/gtest.h>
#include <cstdlib>
#include <unistd.h>
#include <sstream>
#include <regex>
#include "server.h"

class PostgresMCPServerTest : public ::testing::Test {
protected:
  static pqxx::connection* admin_conn;
  static PostgresMCPServer* srv;
  static std::string test_dbname;
  static std::string base_url;

  static void SetUpTestSuite() {
    const char* env_url = std::getenv("DATABASE_URL");
    if (!env_url) {
      GTEST_SKIP_("DATABASE_URL environment variable not set");
      return;
    }

    base_url = env_url;

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

      std::string test_url;
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

      txn.exec(
        "CREATE TABLE public.users ("
        "  id    SERIAL PRIMARY KEY,"
        "  name  VARCHAR(100) NOT NULL,"
        "  email VARCHAR(200) UNIQUE"
        ")"
      );
      txn.exec("COMMENT ON TABLE public.users IS 'user account records'");
      txn.exec("COMMENT ON COLUMN public.users.email IS 'unique email address'");
      txn.exec("INSERT INTO public.users(name, email) VALUES ('Alice', 'alice@example.com')");
      txn.exec("INSERT INTO public.users(name, email) VALUES ('Bob', 'bob@example.com')");
      txn.exec("ANALYZE public.users");

      txn.exec(
        "CREATE TABLE public.orders ("
        "  id      SERIAL PRIMARY KEY,"
        "  user_id INT NOT NULL REFERENCES public.users(id),"
        "  amount  NUMERIC(10,2) CHECK (amount > 0)"
        ")"
      );
      txn.exec("INSERT INTO public.orders(user_id, amount) VALUES (1, 99.99)");
      txn.exec("ANALYZE public.orders");

      txn.exec(
        "CREATE TABLE public.user_account_log ("
        "  id         SERIAL PRIMARY KEY,"
        "  user_id    INT REFERENCES public.users(id),"
        "  action     TEXT,"
        "  logged_at  TIMESTAMPTZ DEFAULT now()"
        ")"
      );

      txn.exec("CREATE TABLE public.bare_notes (note TEXT)");

      txn.exec(
        "CREATE FUNCTION public.log_user_action() RETURNS trigger LANGUAGE plpgsql AS $$\n"
        "BEGIN\n"
        "  INSERT INTO public.user_account_log(user_id, action) VALUES (NEW.id, TG_OP);\n"
        "  RETURN NEW;\n"
        "END;\n"
        "$$"
      );
      txn.exec("COMMENT ON FUNCTION public.log_user_action() IS 'audit trigger for user table'");
      txn.exec(
        "CREATE TRIGGER trg_user_audit"
        " AFTER INSERT OR UPDATE ON public.users"
        " FOR EACH ROW EXECUTE FUNCTION public.log_user_action()"
      );
      txn.exec(
        "CREATE FUNCTION public.get_user_count() RETURNS bigint LANGUAGE sql AS $$"
        " SELECT COUNT(*) FROM public.users; $$"
      );

      txn.exec(
        "CREATE VIEW public.active_users AS "
        "  SELECT id, name FROM public.users WHERE name IS NOT NULL"
      );

      txn.exec(
        "CREATE MATERIALIZED VIEW public.user_stats AS "
        "  SELECT COUNT(*) AS user_count FROM public.users"
      );

      txn.exec("CREATE UNIQUE INDEX ON public.user_stats(user_count)");
      txn.exec("REFRESH MATERIALIZED VIEW public.user_stats");
      txn.exec("ANALYZE public.user_stats");

      txn.exec("CREATE TYPE public.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled')");
      txn.exec("COMMENT ON TYPE public.order_status IS 'status of a customer order'");
      txn.exec("ALTER TABLE public.orders ADD COLUMN status public.order_status DEFAULT 'pending'");
      txn.exec("CREATE TYPE public.user_role AS ENUM ('admin', 'user', 'guest')");

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
        ntxn.exec("DROP DATABASE IF EXISTS \"" + test_dbname + "\"");
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

TEST_F(PostgresMCPServerTest, SchemasReturnsObject) {
  json result = srv->call_schemas();
  EXPECT_TRUE(result.is_object());
  EXPECT_GT(result.size(), 0);
}

TEST_F(PostgresMCPServerTest, SchemasContainsPublic) {
  json result = srv->call_schemas();
  EXPECT_TRUE(result.contains("public"));
  EXPECT_TRUE(result["public"].contains("tables"));
  EXPECT_TRUE(result["public"]["tables"].is_array());

  auto tables = result["public"]["tables"];
  std::vector<std::string> table_names(tables.begin(), tables.end());
  EXPECT_NE(std::find(table_names.begin(), table_names.end(), "users"), table_names.end());
  EXPECT_NE(std::find(table_names.begin(), table_names.end(), "orders"), table_names.end());
}

TEST_F(PostgresMCPServerTest, SchemasExcludesSystemSchemas) {
  json result = srv->call_schemas();
  EXPECT_FALSE(result.contains("pg_catalog"));
  EXPECT_FALSE(result.contains("information_schema"));
}

TEST_F(PostgresMCPServerTest, TablesReturnsKnownTables) {
  json result = srv->call_tables("public");
  EXPECT_TRUE(result.contains("users"));
  EXPECT_TRUE(result.contains("orders"));
}

TEST_F(PostgresMCPServerTest, TablesHasExpectedFields) {
  json result = srv->call_tables("public");
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
  EXPECT_TRUE(result["users"].contains("indexes"));
  EXPECT_TRUE(result["users"].contains("constraints"));
}

TEST_F(PostgresMCPServerTest, TableWithNoIndexesIsIncluded) {
  json result = srv->call_tables("public");
  EXPECT_TRUE(result.contains("bare_notes"));
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

TEST_F(PostgresMCPServerTest, SearchDoesNotMatchColumnComment) {
  json result = srv->call_search("unique email");
  for (auto& [key, value] : result.items()) {
    EXPECT_NE(key.find(".users"), 0) << "Should not find users by column comment";
  }
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
  json result = srv->call_table("public", "users");
  EXPECT_TRUE(result.contains("table"));
  EXPECT_TRUE(result.contains("rows"));
  EXPECT_TRUE(result.contains("size"));
  EXPECT_TRUE(result.contains("description"));
  EXPECT_TRUE(result.contains("seq_scan"));
  EXPECT_TRUE(result.contains("idx_scan"));
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("indexes"));
  EXPECT_TRUE(result.contains("constraints"));
  EXPECT_TRUE(result.contains("foreign_keys"));
}

TEST_F(PostgresMCPServerTest, TableColumnsHaveTypeInfo) {
  json result = srv->call_table("public", "users");
  EXPECT_TRUE(result["columns"].contains("name"));
  EXPECT_TRUE(result["columns"]["name"].contains("type"));
  EXPECT_TRUE(result["columns"]["name"].contains("format_type"));
  EXPECT_TRUE(result["columns"]["name"].contains("not_null"));
}

TEST_F(PostgresMCPServerTest, TableColumnsIncludeStatsForAnalyzedTable) {
  json result = srv->call_table("public", "users");
  EXPECT_TRUE(result["columns"]["name"].contains("null_frac"));
  EXPECT_TRUE(result["columns"]["name"].contains("avg_width"));
  EXPECT_TRUE(result["columns"]["name"].contains("n_distinct"));
}

TEST_F(PostgresMCPServerTest, TableWorksWhenUnanalyzed) {
  json result = srv->call_table("public", "bare_notes");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result["columns"].contains("note"));
}

TEST_F(PostgresMCPServerTest, TableForeignKeyIsSeparateFromConstraints) {
  json result = srv->call_table("public", "orders");
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

// --- listFunctions tests ---

TEST_F(PostgresMCPServerTest, FunctionsReturnsBothFunctions) {
  json result = srv->call_functions("public");
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
  json result = srv->call_functions("public");
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("kind"));
    std::string kind = value["kind"].get<std::string>();
    EXPECT_TRUE(kind == "function" || kind == "procedure");
  }
}

TEST_F(PostgresMCPServerTest, FunctionHasExpectedFields) {
  json result = srv->call_functions("public");
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
  json result = srv->call_function_detail("public", "get_user_count");
  EXPECT_TRUE(result.is_object());
  EXPECT_FALSE(result.empty());
  for (auto& [key, value] : result.items()) {
    EXPECT_TRUE(value.contains("source"));
    EXPECT_TRUE(value.contains("definition"));
    EXPECT_FALSE(value["source"].get<std::string>().empty());
  }
}

TEST_F(PostgresMCPServerTest, FunctionDetailsShowsTriggerUsage) {
  json result = srv->call_function_detail("public", "log_user_action");
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
  json result = srv->call_table("public", "users");
  EXPECT_TRUE(result.contains("triggers"));
  EXPECT_TRUE(result["triggers"].is_object());
  EXPECT_TRUE(result["triggers"].contains("trg_user_audit"));
  auto& trig = result["triggers"]["trg_user_audit"];
  EXPECT_TRUE(trig.contains("timing"));
  EXPECT_TRUE(trig.contains("events"));
  EXPECT_TRUE(trig.contains("function"));
  EXPECT_EQ(trig["timing"].get<std::string>(), "AFTER");
  EXPECT_NE(trig["events"].get<std::string>().find("INSERT"), std::string::npos);
}

// --- view / materialized view tests ---

TEST_F(PostgresMCPServerTest, SchemasContainsView) {
  json result = srv->call_schemas();
  auto tables = result["public"]["tables"];
  std::vector<std::string> names(tables.begin(), tables.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "active_users"), names.end());
}

TEST_F(PostgresMCPServerTest, SchemasContainsMaterializedView) {
  json result = srv->call_schemas();
  auto tables = result["public"]["tables"];
  std::vector<std::string> names(tables.begin(), tables.end());
  EXPECT_NE(std::find(names.begin(), names.end(), "user_stats"), names.end());
}

TEST_F(PostgresMCPServerTest, TablesContainsViewAndMV) {
  json result = srv->call_tables("public");
  EXPECT_TRUE(result.contains("active_users"));
  EXPECT_TRUE(result.contains("user_stats"));
}

TEST_F(PostgresMCPServerTest, TablesViewHasKindField) {
  json result = srv->call_tables("public");
  ASSERT_TRUE(result.contains("active_users"));
  EXPECT_EQ(result["active_users"]["kind"].get<std::string>(), "view");
}

TEST_F(PostgresMCPServerTest, TablesMVHasKindField) {
  json result = srv->call_tables("public");
  ASSERT_TRUE(result.contains("user_stats"));
  EXPECT_EQ(result["user_stats"]["kind"].get<std::string>(), "materialized view");
}

TEST_F(PostgresMCPServerTest, TableDetailsWorksForView) {
  json result = srv->call_table("public", "active_users");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("definition"));
  EXPECT_FALSE(result["definition"].is_null());
  EXPECT_FALSE(result["definition"].get<std::string>().empty());
}

TEST_F(PostgresMCPServerTest, TableDetailsWorksForMV) {
  json result = srv->call_table("public", "user_stats");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("columns"));
  EXPECT_TRUE(result.contains("definition"));
  EXPECT_FALSE(result["definition"].is_null());
  EXPECT_FALSE(result["definition"].get<std::string>().empty());
  EXPECT_FALSE(result["indexes"].empty());
}

// --- listEnums tests ---

TEST_F(PostgresMCPServerTest, EnumsReturnsKnownEnum) {
  json result = srv->call_enums("public");
  EXPECT_TRUE(result.is_object());
  EXPECT_TRUE(result.contains("order_status"));
  EXPECT_TRUE(result.contains("user_role"));
}

TEST_F(PostgresMCPServerTest, EnumsHasExpectedFields) {
  json result = srv->call_enums("public");
  ASSERT_TRUE(result.contains("order_status"));
  EXPECT_TRUE(result["order_status"].contains("description"));
  EXPECT_TRUE(result["order_status"].contains("values"));
  EXPECT_TRUE(result["order_status"]["values"].is_array());
  EXPECT_EQ(result["order_status"]["description"].get<std::string>(), "status of a customer order");
}

TEST_F(PostgresMCPServerTest, EnumsValuesAreOrdered) {
  json result = srv->call_enums("public");
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
  json result = srv->call_enum_detail("public", "order_status");
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.contains("description"));
  EXPECT_TRUE(result.contains("values"));
  EXPECT_TRUE(result["values"].is_array());
  EXPECT_EQ(result["description"].get<std::string>(), "status of a customer order");
}

TEST_F(PostgresMCPServerTest, EnumDetailsHasUsedByColumns) {
  json result = srv->call_enum_detail("public", "order_status");
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
  json result = srv->call_enum_detail("public", "user_role");
  EXPECT_TRUE(result.contains("used_by_columns"));
  EXPECT_TRUE(result["used_by_columns"].is_array());
  EXPECT_EQ(result["used_by_columns"].size(), 0u);
}

TEST_F(PostgresMCPServerTest, EnumDetailsNotFound) {
  json result = srv->call_enum_detail("public", "nonexistent_enum");
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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
