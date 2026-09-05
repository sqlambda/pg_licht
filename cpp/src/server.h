#pragma once

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>
#include <cctype>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>
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
// Live connections, keyed by the configured connection name, reused across
// calls and closed once idle.
//
// Why this exists: every tool call used to open a connection and close it
// again. Against a local socket that is free; against a remote database it is
// the majority of what a call spends -- TCP, TLS and authentication, repeated
// for the next call to the same database milliseconds later. Holding the
// connection between calls removes all of it.
//
// The cache holds ONLY idle connections. Acquiring removes the entry;
// releasing puts it back with a fresh timestamp. That is what makes the reaper
// thread safe without an in-use flag and without a race to get wrong: a
// connection being used by a call is not in the map, so the reaper cannot see
// it, let alone close it. The mutex guards a small map and is held for
// microseconds.
//
// Keyed on the connection *name*, and every registry entry carries its own
// user, so a reused connection is never shared across identities.
class ConnectionCache {
public:
  // How many idle connections may be held at once.
  //
  // The TTL alone does not bound this. A sweep releases one connection per
  // member, so an operator with 400 configured databases holds 400 open
  // sockets for the full minute after a single group sweep -- and macOS ships
  // a 256 file-descriptor limit, so the sweep exhausts descriptors partway
  // through and the remaining members report connect failures that read as a
  // network outage rather than a local limit. A cap makes the worst case
  // arithmetic instead of a function of the registry: at most kMaxIdle held
  // plus kSweepConcurrency in flight.
  //
  // 32 covers an ordinary registry entirely -- every connection stays warm and
  // no eviction ever runs -- while staying far below any platform's limit. Past
  // that the cache degrades to reconnecting, which is exactly what it did
  // before 4.1.0 and is never worse than that.
  static constexpr size_t kMaxIdle = 32;

  explicit ConnectionCache(std::chrono::seconds idle_ttl = std::chrono::seconds{60},
                           size_t max_idle = kMaxIdle)
    : idle_ttl_(idle_ttl), max_idle_(max_idle == 0 ? 1 : max_idle) {
    reaper_ = std::thread([this] { reap_loop(); });
  }

  // Joined rather than detached. A detached thread racing PQfinish against
  // process exit is the classic source of an intermittent crash at shutdown,
  // and it would show up in CI long before anyone reproduced it by hand.
  ~ConnectionCache() {
    {
      std::lock_guard<std::mutex> lk(m_);
      stop_ = true;
    }
    cv_.notify_all();
    if (reaper_.joinable()) reaper_.join();
  }

  ConnectionCache(const ConnectionCache&) = delete;
  ConnectionCache& operator=(const ConnectionCache&) = delete;

  // Takes the cached connection for `name` if one is idle, else nothing. The
  // caller owns it until it calls release(); it is out of the map meanwhile.
  std::unique_ptr<pqxx::connection> take(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = idle_.find(name);
    if (it == idle_.end()) return nullptr;
    auto conn = std::move(it->second.conn);
    idle_.erase(it);
    return conn;
  }

  void release(const std::string& name, std::unique_ptr<pqxx::connection> conn) {
    if (!conn) return;
    // A connection the server closed under us is worth nothing to the next
    // caller; dropping it here means the next take() misses and reconnects,
    // rather than handing out a corpse.
    if (!conn->is_open()) return;
    std::lock_guard<std::mutex> lk(m_);
    idle_[name] = Entry{std::move(conn), Clock::now()};
    evict_over_cap();
  }

  // Test hook: how many connections are being held right now.
  size_t idle_count() {
    std::lock_guard<std::mutex> lk(m_);
    return idle_.size();
  }

  // Test hook: close everything now, without waiting for the TTL.
  void reap_now() {
    std::lock_guard<std::mutex> lk(m_);
    idle_.clear();
  }

private:
  using Clock = std::chrono::steady_clock;
  struct Entry {
    std::unique_ptr<pqxx::connection> conn;
    Clock::time_point idle_since;
  };

  // Drops the least recently released entries until the cap holds. Called with
  // m_ held, from release() only -- the map grows nowhere else.
  //
  // A linear scan per eviction rather than a maintained LRU list: the map is
  // capped at kMaxIdle, so this is a scan of at most 32 entries on the one path
  // that has already paid for a network round trip. An intrusive list would be
  // faster and would be the third place in this class that has to stay in sync
  // with the map.
  void evict_over_cap() {
    while (idle_.size() > max_idle_) {
      auto oldest = idle_.begin();
      for (auto it = std::next(idle_.begin()); it != idle_.end(); ++it)
        if (it->second.idle_since < oldest->second.idle_since) oldest = it;
      idle_.erase(oldest);
    }
  }

  void reap_loop() {
    std::unique_lock<std::mutex> lk(m_);
    while (!stop_) {
      // Waking on the TTL rather than polling a short interval: the point of
      // the thread is to not hold a socket open to a remote database while
      // nobody is working, and a wakeup a minute costs nothing.
      cv_.wait_for(lk, idle_ttl_, [this] { return stop_; });
      if (stop_) break;
      const auto now = Clock::now();
      for (auto it = idle_.begin(); it != idle_.end();) {
        if (now - it->second.idle_since >= idle_ttl_) it = idle_.erase(it);
        else ++it;
      }
    }
  }

  std::chrono::seconds idle_ttl_;
  size_t max_idle_;
  std::mutex m_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::map<std::string, Entry> idle_;
  std::thread reaper_;
};

class Session {
public:
  // timeout_ms overrides the connection's configured statement ceiling; omit it
  // and the connection's own value applies. Only explainQuery passes one, since
  // it is the one tool whose caller states how long it is willing to wait.
  // cache may be null, in which case the connection is opened and closed for
  // this session alone -- which is what the tests and any single-shot use get.
  explicit Session(const pglicht::ConnConfig& cfg,
                   std::optional<int> timeout_ms = std::nullopt,
                   ConnectionCache* cache = nullptr,
                   const std::string& name = std::string())
    : cache_(cache), name_(name) {
    // A cached connection can have been closed by the server under us --
    // idle_session_timeout, a restart, a NAT table that forgot us. There is no
    // way to know but to use it, so a failure here discards it and tries once
    // more with a fresh one. That retry is safe precisely because nothing of
    // the caller's has run yet: the only statements attempted are BEGIN and
    // the setup batch below, and neither is anything to replay.
    for (int attempt = 0; attempt < 2; attempt++) {
      const bool reused = (attempt == 0 && cache_ != nullptr);
      conn_ = reused ? cache_->take(name_) : nullptr;
      if (!conn_) conn_ = std::make_unique<pqxx::connection>(cfg.conninfo);
      try {
        txn_.emplace(*conn_);
        break;
      } catch (const std::exception&) {
        conn_.reset();
        if (attempt == 1) throw;   // a fresh connection failed too: real error
      }
    }
    // One round trip sets up the whole session. PQexec takes several
    // statements separated by semicolons and returns the last result, so the
    // guard, the ceiling and the role probe cost what any one of them would.
    //
    // SET TRANSACTION READ ONLY must be first, and is the real write backstop:
    // it catches writes a query plan cannot reveal, such as a VOLATILE function
    // that modifies data, aborting with SQLSTATE 25006. The property this could
    // break is the guard itself, and a test asserts a later write is still
    // rejected with 25006, because the failure mode of getting it wrong is
    // silent.
    //
    // The ceiling used to cost a round trip of its own. It was written as
    // set_config(..., is_local => true) with the value bound as a parameter,
    // and a parameterised statement cannot be batched with others in one
    // PQexec. But the value is an int this server computes -- from the config
    // file or from explainQuery's own validated argument -- never a string a
    // caller supplies, so SET LOCAL with the number written out is exactly as
    // safe and rides along here instead. On a remote database that is one
    // round trip saved on every single call, which is a fifth of what a call
    // spends once a pooler has removed the connect.
    //
    // The role rides along too, which is why every session observes it rather
    // than only the calls that think they need it: during a failover, a stale
    // role is the worst answer available.
    statement_timeout_ms_ = timeout_ms.value_or(cfg.statement_timeout_ms);
    last_statement_timeout_ms() = statement_timeout_ms_;

    std::string setup = "SET TRANSACTION READ ONLY";
    if (statement_timeout_ms_ > 0) {
      // std::to_string of an int: nothing here can carry a quote or a
      // semicolon, so there is no statement to inject into.
      setup += "; SET LOCAL statement_timeout = " +
               std::to_string(statement_timeout_ms_);
    }
    setup += "; SELECT pg_is_in_recovery()";

    pqxx::result role = txn_->exec(setup);
    if (!role.empty() && !role[0][0].is_null())
      in_recovery_ = role[0][0].as<bool>();
    last_observed_role() = this->role();
  }

  // The ceiling this session applied, so a cancellation can be reported as the
  // limit being reached rather than as an unexplained error. 0 means none.
  int statement_timeout_ms() const { return statement_timeout_ms_; }

  pqxx::work& txn() { return *txn_; }

  // Whether this server is a standby, observed on connect rather than declared.
  //
  // Role is not configuration: failover swaps it, and failover is exactly when
  // this server gets used. Nothing caches it -- a stale "this one is the
  // primary" is the worst answer available, and it would be given at the
  // moment it costs the most.
  //
  // "unknown" is reachable only if the probe returned nothing, which should
  // not happen; it is spelled out rather than defaulted to "primary" because
  // defaulting would be a claim.
  const char* role() const {
    return !in_recovery_.has_value() ? "unknown"
         : *in_recovery_             ? "replica"
                                     : "primary";
  }
  bool in_recovery() const { return in_recovery_.value_or(false); }

  // The role the most recently constructed Session observed.
  //
  // This exists so a fan-out sweep can label a member's result without opening
  // a second connection purely to ask. It is safe for the same reason active_
  // is: the server reads stdin line by line, so exactly one tool call is ever
  // in flight, and each call opens one Session. The sweep resets it before
  // every member so a member that never connects cannot inherit the previous
  // one's role.
  // thread_local, not merely static: a parallel sweep runs members on
  // different threads, and a shared static would attribute one member's role
  // to another -- silently, and precisely during a failover, which is when
  // this tool gets used.
  static const char*& last_observed_role() {
    static thread_local const char* r = "unknown";
    return r;
  }

  // The ceiling the most recently constructed Session applied.
  //
  // Exists for the same reason and on the same terms as last_observed_role():
  // exactly one tool call is ever in flight, and each opens one Session. The
  // reader is the handler that turns a cancellation into a message, which runs
  // after the Session has been destroyed and so cannot ask it directly.
  static int& last_statement_timeout_ms() {
    static thread_local int ms = 0;
    return ms;
  }

  // Server version as an integer (e.g. 160004 for 16.4), from the startup
  // handshake -- no round trip. Used to gate catalog columns and SQL features
  // that don't exist on older majors; correct per connection, which matters
  // when different configured connections point at different-version servers.
  int server_version() const { return conn_->server_version(); }

  // txn_ is destroyed before conn_, rolling back; conn_ then disconnects.
  // Every session ends in ROLLBACK, explicitly. pqxx::work already aborts an
  // uncommitted transaction on destruction, so this changes no behaviour -- it
  // states the intent, and makes it something a test can assert rather than
  // something that holds by accident. Nothing here ever commits: every
  // statement runs under SET TRANSACTION READ ONLY, so there is nothing a
  // commit could preserve, and rolling back leaves the server provably as it
  // was found.
  //
  // One thing rollback does NOT undo, and it is worth naming because the
  // assumption is natural: backend-local state set by an extension. A hypopg
  // hypothetical index survives ROLLBACK, survives into the next transaction,
  // and survives DISCARD ALL. evaluate_index resets it explicitly for exactly
  // that reason; see the bracket there.
  ~Session() {
    // A destructor must not throw, and a connection already gone is not an
    // error worth reporting: the transaction dies with it either way.
    try { if (txn_) txn_->abort(); } catch (...) {}
    // End the transaction before handing the connection back, so what returns
    // to the cache is idle and clean. txn_ is destroyed here rather than left
    // to member order, because it holds a reference to *conn_.
    txn_.reset();
    if (cache_ && conn_) cache_->release(name_, std::move(conn_));
  }

  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;

private:
  ConnectionCache* cache_ = nullptr;
  std::string name_;
  std::unique_ptr<pqxx::connection> conn_;
  std::optional<pqxx::work> txn_;
  std::optional<bool> in_recovery_;
  int statement_timeout_ms_ = 0;
};

class PostgresMCPServer {
public:
  // Single-connection form: DATABASE_URL, an explicit conninfo, or argv[1].
  // Deliberately does not connect. The registry is parsed and validated here,
  // so a malformed conninfo still fails at startup; only *reachability* is left
  // to the first call that needs it.
  //
  // Connecting eagerly cost more than it bought. It is one full round trip to a
  // remote database before the client can do anything -- and worse, it was
  // fatal: an unreachable default connection aborted the constructor, main
  // reported "Fatal DB Error", and the server did not start at all. A registry
  // of twenty databases was therefore unusable in its entirety because one of
  // them was behind a VPN that happened to be down. Reachability is a property
  // of the moment a call is made, not of startup.
  explicit PostgresMCPServer(const std::string& conn_str)
    : registry_(pglicht::ConnectionRegistry::from_url(conn_str, app_name())) {}

  // Multi-connection form, from an INI file of named sections.
  explicit PostgresMCPServer(pglicht::ConnectionRegistry registry)
    : registry_(std::move(registry)) {}

  // A worker for one member of a parallel sweep. Shares the registry (read
  // only after construction) and the connection cache, and owns its own
  // active_, sweep_cfg_ and ext_schemas_ -- which is the whole reason the 58
  // query methods need no change at all to run concurrently.
  PostgresMCPServer(const pglicht::ConnectionRegistry& registry,
                    std::shared_ptr<ConnectionCache> cache)
    : registry_(registry), cache_(std::move(cache)) {}

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
  const json call_table_stats(const std::string& schema, const std::string& table_name) {
    return table_stats(schema, table_name);
  }
  const json call_list_table_stats(const std::string& schema) { return list_table_stats(schema); }
  const json call_table_size(const std::string& schema, const std::string& table_name) {
    return table_size(schema, table_name);
  }
  const json call_list_table_sizes(const std::string& schema) { return list_table_sizes(schema); }
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
  const json call_activity() { return activity(0, "", 0, ""); }
  const json call_activity(int pid, const std::string& query_id,
                           double min_duration_s, const std::string& state) {
    return activity(pid, query_id, min_duration_s, state);
  }
  const json call_locks() { return locks(0); }
  const json call_locks(int pid) { return locks(pid); }
  const json call_replication_slots() { return replication_slots(); }
  const json call_database_stats() { return database_stats(); }
  const json call_statement_stats(int limit) {
    return statement_stats(limit, "", "", 0);
  }
  const json call_statement_stats(int limit, const std::string& query_id,
                                  const std::string& order_by, long long min_calls) {
    return statement_stats(limit, query_id, order_by, min_calls);
  }
  const json call_table_bloat(const std::string& schema, const std::string& table_name, bool exact) {
    return table_bloat(schema, table_name, exact);
  }
  const json call_index_bloat(const std::string& schema, const std::string& index_name) {
    return index_bloat(schema, index_name);
  }
  // The tools array as tools/list advertises it, so a query method that exists
  // but was never registered -- and is therefore unreachable by any client --
  // fails the suite rather than passing it.
  const json call_tools_list() { return get_tools_list(client_protocol_)["tools"]; }
  // Test hook. Nothing this server lists today exceeds one page, which is
  // deliberate -- see kListPageSize -- so the multi-page path would otherwise
  // ship untested until the first operator with a hundred schemas found it.
  static bool call_paginate(const json& items, const json& params,
                            const char* key, json& out) {
    return paginate(items, params, key, out);
  }
  const json call_tools_list(const std::string& protocol) {
    return get_tools_list(protocol)["tools"];
  }
  // Which protocol version initialize would settle on, without the transport.
  std::string call_initialize_version(const std::string& requested) {
    for (const auto& v : supported_protocols())
      if (v == requested) return v;
    return "2024-11-05";
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
  const json call_progress_stats() { return progress_stats(0, ""); }
  const json call_progress_stats(int pid, const std::string& relation) {
    return progress_stats(pid, relation);
  }
  const json call_io_stats() { return io_stats(0, "", "", ""); }
  const json call_io_stats(int pid, const std::string& backend_type,
                           const std::string& object, const std::string& context) {
    return io_stats(pid, backend_type, object, context);
  }
  const json call_table_io_stats(const std::string& schema, const std::string& table_name, int limit) {
    return table_io_stats(schema, table_name, limit);
  }
  const json call_host_capacity(long long ram_mb, int vcpus, const std::string& storage) {
    return host_capacity(ram_mb, vcpus, storage);
  }

  // Drive one JSON-RPC request and return the response, for tests that need the
  // transport layer rather than a query method -- argument validation, error
  // codes and the fan-out envelope all live there.
  json call_rpc(const json& request) {
    std::ostringstream buf;
    std::streambuf* saved = std::cout.rdbuf(buf.rdbuf());
    try {
      handle_request(request);
    } catch (...) {
      std::cout.rdbuf(saved);
      throw;
    }
    std::cout.rdbuf(saved);
    const std::string line = buf.str();
    return line.empty() ? json::object() : json::parse(line);
  }

  const json call_connections() { return connections(); }
  const json call_topology() { return topology(); }
  const json call_verify_topology() { return verify_topology(); }
  const json call_check_privileges() { return check_privileges(); }
  const json call_evaluate_index(const std::string& sql, const json& create_defs,
                                 const json& hide_names) {
    return evaluate_index(sql, create_defs, hide_names);
  }
  const json call_buffer_cache_summary() { return buffer_cache_summary(); }
  const json call_buffer_cache_contents(int limit) { return buffer_cache_contents(limit); }
  const json call_explain_query(const std::string& queryid, const std::string& sql,
                                const json& params, bool analyze, int timeout_ms) {
    return explain_query(queryid, sql, params, analyze, timeout_ms);
  }

private:
  pglicht::ConnectionRegistry registry_;
  // shared_ptr because parallel fan-out gives each worker its own server
  // object; they must share one cache or each would open its own connection.
  std::shared_ptr<ConnectionCache> cache_ = std::make_shared<ConnectionCache>();
  // Which configured connection the in-flight tool call targets. Resolved once
  // per request by handle_request so the ~40 query methods keep their existing
  // signatures. The server reads stdin line by line, so only one call is ever
  // in flight and a member is safe.
  std::string active_;
  unsigned long long explain_seq_ = 0;
  // What the client negotiated. Defaults to the revision 3.1.1 spoke, so a
  // client that never sends initialize sees exactly 3.1.1's tools/list.
  std::string client_protocol_ = "2024-11-05";
  // Per-request: whether this one came from a stateless (modern) client, and
  // which revision it speaks. Members rather than parameters for the same
  // reason as active_ -- the server reads stdin line by line, so exactly one
  // request is ever in flight.
  bool modern_ = false;
  std::string request_protocol_ = "2024-11-05";

  // Bounded connect for the tools that sweep every configured connection.
  static constexpr int kSweepConnectTimeoutSeconds = 5;

  // The current sweep member's config, carrying the bounded connect.
  //
  // Set only while fan_out is running a member, so every query method goes on
  // reading active_cfg() without learning that a sweep exists -- the same
  // reason the sweep loop itself lives above dispatch. Without this the bound
  // reached only the optional role probe, and the connection that did the
  // actual work waited out the kernel's TCP timeout instead: sweeps are
  // sequential, so each unreachable member cost the whole sweep about two
  // minutes on a Linux default of six SYN retries.
  std::optional<pglicht::ConnConfig> sweep_cfg_;

  static std::string app_name() {
    return std::string("pg-licht-cpp/") + PGLICHT_VERSION;
  }

  const pglicht::ConnConfig& active_cfg() const {
    return sweep_cfg_ ? *sweep_cfg_ : registry_.get(active_);
  }

  // Every session goes through here, so reuse is a property of the server
  // rather than something ~50 query methods each have to remember. The cache
  // is keyed on the connection name, and each registry entry carries its own
  // user, so a reused connection never crosses identities.
  Session open_session(std::optional<int> timeout = std::nullopt) {
    const pglicht::ConnConfig& cfg = active_cfg();
    return Session{cfg, timeout, cache_.get(), cfg.name};
  }

  // Where each extension actually lives, per connection.
  //
  // An extension does not have to be in public: CREATE EXTENSION ... SCHEMA
  // ext is normal practice, and a hardened cluster usually keeps extensions
  // out of public entirely. Nothing here sets a search_path -- it cannot,
  // since PgBouncer discards session state and rejects `options` in the
  // conninfo -- so an unqualified pgstattuple() or pg_stat_statements
  // resolves only against the *default* search_path of the connecting role,
  // and a tool that relied on that reported "not installed" for an extension
  // that was plainly installed. Every extension object is therefore
  // schema-qualified from pg_extension instead.
  //
  // Keyed by connection name because one name is one database for the life of
  // the process, and the lookup is otherwise an extra round trip per call.
  std::map<std::string, std::map<std::string, std::string>> ext_schemas_;

  // The schema of an installed extension, already quoted for interpolation
  // into SQL, or "" when the extension is not installed on this connection.
  //
  // Runs inside the caller's transaction, so the answer is consistent with the
  // query it is about to qualify.
  std::string extension_schema(pqxx::work& txn, const std::string& extname) {
    auto& per_conn = ext_schemas_[active_cfg().name];
    auto it = per_conn.find(extname);
    if (it != per_conn.end()) return it->second;

    pqxx::result r = pqxx_exec(
      txn,
      "SELECT QUOTE_IDENT(n.nspname)"
      " FROM pg_extension AS e"
      " JOIN pg_namespace AS n ON n.oid = e.extnamespace"
      " WHERE e.extname = $1",
      pqxx::params{extname});

    std::string schema = r.empty() || r[0][0].is_null()
      ? std::string{} : r[0][0].as<std::string>();

    // Only a positive answer is remembered. Caching "not installed" would pin
    // that verdict for the life of the process, so a CREATE EXTENSION during
    // an incident would never be picked up -- exactly when it is most likely
    // to happen, since the tool's own hint is what prompts it.
    if (!schema.empty()) per_conn[extname] = schema;
    return schema;
  }

  // Drop a remembered schema after a query that used it failed to find the
  // object. ALTER EXTENSION ... SET SCHEMA is rare, but a cached location that
  // has moved would otherwise keep failing until a restart; forgetting it here
  // makes the next call re-resolve. The failed statement has already aborted
  // this transaction, so the retry cannot happen before then.
  void forget_extension_schema(const std::string& extname) {
    auto conn = ext_schemas_.find(active_cfg().name);
    if (conn != ext_schemas_.end()) conn->second.erase(extname);
  }

  // The two error shapes for a missing extension, kept next to each other so
  // the hints stay consistent between the tools that raise them.
  static json pgss_missing() {
    return {
      {"error", "pg_stat_statements is not installed"},
      {"hint", "Add 'pg_stat_statements' to shared_preload_libraries in "
               "postgresql.conf, restart PostgreSQL, then run: "
               "CREATE EXTENSION pg_stat_statements;"}
    };
  }

  static json pgstattuple_missing() {
    return {
      {"error", "pgstattuple is not installed"},
      {"hint", "Run: CREATE EXTENSION pgstattuple;"}
    };
  }

  // Every pgstattuple function is installed with EXECUTE revoked from PUBLIC
  // and granted to pg_stat_scan_tables, so a perfectly valid read-only role
  // gets SQLSTATE 42501 rather than an answer. Left as a raw exception that
  // reads as an internal failure, it sends the caller looking for a bug
  // instead of asking for the one grant that fixes it.
  // Why every privilege branch below catches pqxx::insufficient_privilege by
  // type rather than testing e.sqlstate() == "42501":
  //
  // libpqxx maps the SQLSTATE to the right exception class but leaves
  // sqlstate() *empty* on that class -- verified against libpqxx 7.10, where
  // undefined_table and undefined_function both carry their codes and
  // insufficient_privilege does not. A code comparison therefore never matches,
  // the branch never runs, and the operator gets a raw exception instead of the
  // grant they are missing. The type is reliable where the string is not.
  //
  // Note that libpqxx also raises insufficient_privilege for SQLSTATE 25006
  // (write attempted in a read-only transaction). None of these queries write,
  // so the two cannot be confused here -- but a future caller that does write
  // must not reuse this pattern to report a grant.

  // A statement the server cancelled because it hit statement_timeout.
  //
  // Reported as the ceiling being reached rather than as a raw error, because
  // the two call for opposite responses: an error means something is wrong,
  // while this means the answer needs longer than the caller allowed -- and the
  // caller cannot tell which from the message PostgreSQL sends.
  //
  // Unlike insufficient_privilege (see above), a cancellation really does carry
  // its SQLSTATE. libpqxx has no dedicated class for 57014, so it arrives as a
  // plain pqxx::sql_error with sqlstate() == "57014" -- verified against
  // libpqxx 7.10, the same version whose empty sqlstate() on
  // insufficient_privilege made the code comparison there unusable. Testing the
  // code is right here for exactly the reason it was wrong there, so the two
  // must not be made to look alike.
  static bool is_statement_timeout(const pqxx::sql_error& e) {
    return e.sqlstate() == "57014";
  }

  static json statement_timeout_error(int timeout_ms, const std::string& detail) {
    return {
      {"error", "statement cancelled after " + std::to_string(timeout_ms) +
                "ms (statement_timeout)"},
      {"hint", "This is a ceiling, not a failure: the statement was still "
               "running when it was reached. Raise it for this connection with "
               "'statement_timeout_ms' in the connections file (or "
               "PG_LICHT_STATEMENT_TIMEOUT_MS with a single DATABASE_URL), or "
               "set 0 to remove it. tableBloat with exact=true, indexBloat on a "
               "btree or hash index, and bufferCacheContents all scale with the "
               "relation or with shared_buffers rather than with the query, so "
               "they are the calls that reach it"},
      {"timeout_ms", timeout_ms},
      {"detail", detail}
    };
  }

  // pg_buffercache's two failure shapes. The grant is the interesting one: a
  // valid read-only role missing a monitoring grant is a different problem
  // from an absent extension, and conflating them sends the operator to
  // CREATE EXTENSION for something that is plainly already installed.
  static json pg_buffercache_missing() {
    return {
      {"error", "pg_buffercache is not installed"},
      {"hint", "Run: CREATE EXTENSION pg_buffercache;"}
    };
  }

  static json pg_buffercache_denied(const std::string& what,
                                    const std::string& detail) {
    return {
      {"error", "permission denied for " + what},
      {"hint", "pg_buffercache is restricted to superusers and roles with "
               "pg_monitor. Run: GRANT pg_monitor TO <role>;"},
      {"detail", detail}
    };
  }

  static json pgstattuple_denied(const std::string& func, const std::string& detail) {
    return {
      {"error", "permission denied for " + func},
      {"hint", "pgstattuple's functions have EXECUTE revoked from PUBLIC and "
               "granted to pg_stat_scan_tables. Run: GRANT pg_stat_scan_tables "
               "TO <role>; (or grant EXECUTE on the function directly)"},
      {"detail", detail}
    };
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
      // Topology, so a caller can see which connections share a postmaster and
      // which hold the same data, without a second call. Role is deliberately
      // absent: it is observed, not configured (see verifyTopology).
      if (!c.instance.empty()) {
        entry["instance"] = c.instance;
        entry["instance_source"] = c.instance_source;
      }
      if (!c.replication_group.empty()) entry["replication_group"] = c.replication_group;
      if (!c.groups.empty())            entry["groups"] = c.groups;
      // Host capacity, so a caller can see at a glance which connections still
      // need it injected before hostCapacity can compute anything.
      if (c.capacity.ram_mb > 0)        entry["host_ram_mb"]  = c.capacity.ram_mb;
      if (c.capacity.vcpus > 0)         entry["host_vcpus"]   = c.capacity.vcpus;
      if (!c.capacity.storage.empty())  entry["host_storage"] = c.capacity.storage;
      if (!c.capacity.note.empty())     entry["host_note"]    = c.capacity.note;
      out.push_back(entry);
    }
    // Wrapped for the same reason as currentLocks: a top-level array cannot be
    // a `structuredContent` payload.
    return json{{"connections", out}};
  }

  // The configured topology, as three indexes plus whatever belongs to none of
  // them. Pure registry data: this opens no connection, which is what makes it
  // cheap enough to call before deciding how wide a sweep to run.
  const json topology() {
    auto axis = [&](const pglicht::ConnectionRegistry::Members& idx) {
      json out = json::array();
      for (const auto& [name, members] : idx)
        out.push_back({{"name", name}, {"connections", members}});
      return out;
    };

    json instances = json::array();
    for (const auto& [name, members] : registry_.instances()) {
      json entry = {{"name", name}, {"connections", members}};
      // Members of one instance share a source by construction: inference only
      // ever fills in connections that declared nothing.
      entry["source"] = registry_.get(members.front()).instance_source;
      const auto cap = registry_.instance_capacity(name);
      if (cap.ram_mb > 0)       entry["host_ram_mb"]  = cap.ram_mb;
      if (cap.vcpus > 0)        entry["host_vcpus"]   = cap.vcpus;
      if (!cap.storage.empty()) entry["host_storage"] = cap.storage;
      if (!cap.note.empty())    entry["host_note"]    = cap.note;
      instances.push_back(entry);
    }

    json unlabelled = json::array();
    for (const auto& name : registry_.names()) {
      const auto& c = registry_.get(name);
      if (c.instance.empty() && c.replication_group.empty() && c.groups.empty())
        unlabelled.push_back(name);
    }

    return {
      {"instances", instances},
      {"replication_groups", axis(registry_.replication_groups())},
      {"groups", axis(registry_.groups())},
      {"unlabelled", unlabelled}
    };
  }

  // A copy of a connection's config with a bounded connect, for the tools that
  // sweep every configured connection. One unreachable host must cost seconds,
  // not whatever the kernel's TCP timeout happens to be.
  static pglicht::ConnConfig with_connect_timeout(pglicht::ConnConfig cfg,
                                                  int seconds) {
    if (cfg.conninfo.find("connect_timeout") != std::string::npos) return cfg;
    const bool uri = cfg.conninfo.rfind("postgres://", 0) == 0 ||
                     cfg.conninfo.rfind("postgresql://", 0) == 0;
    if (uri)
      cfg.conninfo += (cfg.conninfo.find('?') == std::string::npos ? "?" : "&") +
                      ("connect_timeout=" + std::to_string(seconds));
    else
      cfg.conninfo += " connect_timeout=" + std::to_string(seconds);
    return cfg;
  }

  // Whether the declared topology is true.
  //
  // The whole tool turns on one fact: system_identifier names a replication
  // *lineage*, not a postmaster. A physical replica began life as a copy of its
  // primary and carries the same value forever. So the identifier alone cannot
  // tell the two axes apart, and the endpoint has to be read alongside it:
  //
  //   same identifier, same host and port  -> one instance. Shared buffers,
  //                                           shared autovacuum workers.
  //   same identifier, different host      -> one replication group. Shared
  //                                           data and WAL lineage, not memory.
  //   different identifiers where the config claims either -> the config is
  //                                           wrong, and says so.
  //
  // Logical replication is outside this entirely: a logical replica has its own
  // identifier and its own schema, so disagreement there is "cannot verify",
  // not "mismatch". Saying "mismatch" would send an operator to fix a config
  // that is correct.
  const json verify_topology() {
    struct Observed {
      std::string name, role, sysid, database, addr, version, error;
      long long port = 0;
      bool ok = false, endpoint_known = false;
    };
    // Every conclusion below is drawn by comparing members against each other,
    // so all of them have to be observed before any of it runs. That made the
    // observation loop serial for three releases: one connect at a time across
    // the whole registry, which is the widest walk in the server. Measured at
    // 57s for twelve unreachable hosts on a 5s connect timeout, and it scales
    // with the registry -- 400 configured databases is half an hour.
    //
    // Only the observing is parallel. Results land in fixed slots, so the
    // comparison passes read the registry in configuration order exactly as
    // before and the payload is byte-identical.
    const std::vector<std::string> names = registry_.names();
    std::vector<Observed> seen(names.size());

    const std::string q = R"(
      SELECT JSONB_BUILD_OBJECT(
               'system_identifier', (SELECT c.system_identifier::text
                                     FROM pg_control_system() AS c),
               'database',          current_database(),
               'server_addr',       HOST(inet_server_addr()),
               'server_port',       inet_server_port(),
               'server_version',    current_setting('server_version')
             );
    )";

    parallel_for(names.size(), [&](size_t i) {
      Observed& o = seen[i];
      o.name = names[i];
      try {
        const auto& cfg = registry_.get(names[i]);
        Session sess{with_connect_timeout(cfg, kSweepConnectTimeoutSeconds)};
        o.role = sess.role();
        pqxx::result r = pqxx_exec(sess.txn(), q, pqxx::params{});
        json row = json::parse(r[0][0].as<std::string>());
        // As text: a system identifier is a 64-bit value and does not survive
        // JSON number precision, the same reason query_id is a string.
        if (!row["system_identifier"].is_null())
          o.sysid = row["system_identifier"].get<std::string>();
        o.database = row.value("database", "");
        o.version  = row.value("server_version", "");
        if (!row["server_addr"].is_null() && !row["server_port"].is_null()) {
          o.addr = row["server_addr"].get<std::string>();
          o.port = row["server_port"].get<long long>();
          o.endpoint_known = true;
        }
        o.ok = true;
      } catch (const std::exception& e) {
        o.error = e.what();
      } catch (...) {
        // parallel_for runs this on a worker thread, where an escaping
        // exception is std::terminate rather than a failed member.
        o.error = "unknown error";
      }
    });

    json findings = json::array();
    auto finding = [&](const char* topic, const std::string& name,
                       const char* severity, const std::string& detail) {
      findings.push_back({{"topic", topic}, {"name", name},
                          {"severity", severity}, {"detail", detail}});
    };
    auto by_name = [&](const std::string& n) -> const Observed* {
      for (const auto& o : seen) if (o.name == n) return &o;
      return nullptr;
    };
    auto join = [](const std::vector<std::string>& v) {
      std::string out;
      for (const auto& x : v) out += (out.empty() ? "" : ", ") + x;
      return out;
    };

    // --- declared instances ---
    for (const auto& [iname, members] : registry_.instances()) {
      if (registry_.get(members.front()).instance_source != "declared") continue;
      std::vector<std::string> reachable;
      std::set<std::string> ids, endpoints;
      bool endpoints_known = true;
      for (const auto& m : members) {
        const Observed* o = by_name(m);
        if (!o || !o->ok) continue;
        reachable.push_back(m);
        if (!o->sysid.empty()) ids.insert(o->sysid);
        if (o->endpoint_known) endpoints.insert(o->addr + ":" + std::to_string(o->port));
        else endpoints_known = false;
      }
      if (reachable.size() < 2) continue;
      if (ids.size() > 1) {
        finding("instance", iname, "error",
                "members report different system identifiers, so they are not one "
                "postmaster and share no buffers: " + join(reachable));
      } else if (!endpoints_known) {
        finding("instance", iname, "info",
                "members agree on the system identifier, but at least one is "
                "connected over a Unix socket, where inet_server_addr() is null "
                "-- an instance and a replication group cannot be told apart "
                "without an endpoint");
      } else if (endpoints.size() > 1) {
        finding("instance", iname, "error",
                "members share a system identifier but sit on different servers. "
                "That is a replication lineage, not one postmaster: declare them "
                "as a replication_group instead");
      }
    }

    // --- declared replication groups ---
    for (const auto& [rname, members] : registry_.replication_groups()) {
      std::vector<std::string> reachable, primaries;
      std::set<std::string> ids;
      for (const auto& m : members) {
        const Observed* o = by_name(m);
        if (!o || !o->ok) continue;
        reachable.push_back(m);
        if (!o->sysid.empty()) ids.insert(o->sysid);
        if (o->role == std::string("primary")) primaries.push_back(m);
      }
      if (reachable.empty()) {
        finding("replication_group", rname, "error",
                "no member could be reached, so nothing about this group was "
                "verified");
        continue;
      }
      if (ids.size() > 1) {
        finding("replication_group", rname, "warning",
                "cannot verify: physical replication shares one system "
                "identifier and these members do not. If this is logical "
                "replication that is expected -- membership can then only be "
                "declared, and listPublications and listSubscriptions are where "
                "it is visible");
      }
      if (primaries.empty()) {
        finding("replication_group", rname, "error",
                "every reachable member is in recovery: there is no primary. "
                "During a failover this is the finding, not an empty result");
      } else if (primaries.size() > 1) {
        finding("replication_group", rname, "error",
                "more than one member reports itself a primary, which is split "
                "brain: " + join(primaries) + ". Both are reported; neither is "
                "chosen");
      }
    }

    // --- lineages the config never mentions ---
    // An undeclared replica is the case where "is this index used?" quietly
    // gets the wrong answer: the workload is on a server nothing sweeps.
    std::map<std::string, std::vector<std::string>> by_sysid;
    for (const auto& o : seen)
      if (o.ok && !o.sysid.empty()) by_sysid[o.sysid].push_back(o.name);

    for (const auto& [sysid, members] : by_sysid) {
      if (members.size() < 2) continue;
      const auto& first = registry_.get(members.front());
      bool one_instance = !first.instance.empty();
      bool one_group = !first.replication_group.empty();
      for (const auto& m : members) {
        const auto& c = registry_.get(m);
        if (c.instance != first.instance) one_instance = false;
        if (c.replication_group != first.replication_group) one_group = false;
      }
      if (one_instance || one_group) continue;
      finding("system_identifier", sysid, "warning",
              "these connections share a system identifier but the config does "
              "not tie them together: " + join(members) +
              ". They hold the same data; statistics counters on each are that "
              "server's own, so a sweep that misses one misses that workload");
    }

    // --- what could not be answered ---
    std::vector<std::string> failed;
    for (const auto& o : seen) if (!o.ok) failed.push_back(o.name);
    if (!failed.empty())
      finding("reachability", "", "warning",
              "not reached, so every finding above covers only what answered: " +
              join(failed));

    json conns = json::array();
    for (const auto& o : seen) {
      json e = {{"connection", o.name}};
      const auto& cfg = registry_.get(o.name);
      if (!cfg.instance.empty()) {
        e["instance"] = cfg.instance;
        e["instance_source"] = cfg.instance_source;
      }
      if (!cfg.replication_group.empty()) e["replication_group"] = cfg.replication_group;
      if (!o.ok) { e["error"] = o.error; conns.push_back(e); continue; }
      e["role"] = o.role;
      e["database"] = o.database;
      e["server_version"] = o.version;
      if (!o.sysid.empty()) e["system_identifier"] = o.sysid;
      if (o.endpoint_known) {
        e["server_addr"] = o.addr;
        e["server_port"] = o.port;
      }
      conns.push_back(e);
    }

    return {{"connections", conns}, {"findings", findings}};
  }

  // Where a tool's answer actually varies.
  //
  // The two fan-out axes are close to inverses of each other, and getting this
  // wrong produces sweeps that are either duplicated or misleading:
  //
  //   per_database  differs between the databases of one instance. False for
  //                 the instance-wide views -- pg_stat_activity, pg_locks,
  //                 pg_stat_statements, pg_buffercache and the shared catalogs
  //                 all report the whole instance from any one database, so a
  //                 sweep would return the same rows once per database with
  //                 nothing in the payload to say so.
  //   per_server    differs between members of a replication group. A physical
  //                 replica is byte-identical in its catalogs and its physical
  //                 layout, so DDL and bloat do not vary -- but every statistics
  //                 counter does, because each server accumulates its own.
  //   primary_authoritative
  //                 carries vacuum-side counters. Vacuum never runs on a
  //                 replica, so its values there are noise rather than a second
  //                 opinion.
  //   registry      touches no database at all.
  //
  // Note which tools are per_server for a reason that is easy to miss:
  // duplicateIndexes and indexBloat report idx_scan alongside their physical
  // measurements. The bloat figures are identical across a replication group;
  // the scan counts are not, and they are the whole answer to "is this index
  // safe to drop?" -- an index dead on the primary may be carrying a replica's
  // entire reporting workload.
  struct ToolScope {
    bool per_database = false;
    bool per_server = false;
    bool primary_authoritative = false;
    bool registry = false;
  };

  static const std::map<std::string, ToolScope>& tool_scopes() {
    // {per_database, per_server, primary_authoritative, registry}
    static const std::map<std::string, ToolScope> m = {
      // Registry only: no connection is opened.
      {"listConnections",       {false, false, false, true}},
      {"listTopology",          {false, false, false, true}},
      {"verifyTopology",        {false, false, false, true}},

      // Instance-wide readings: one answer per postmaster.
      {"bufferCacheContents",   {false, true,  false, false}},
      {"bufferCacheSummary",    {false, true,  false, false}},
      {"checkpointStats",       {false, true,  false, false}},
      {"currentActivity",       {false, true,  false, false}},
      {"currentLocks",          {false, true,  false, false}},
      {"databaseStats",         {false, true,  false, false}},
      {"hostCapacity",          {false, true,  false, false}},
      {"ioStats",               {false, true,  false, false}},
      {"progressStats",         {false, true,  false, false}},
      {"replicationSlots",      {false, true,  false, false}},
      {"serverSettings",        {false, true,  false, false}},
      {"statementStats",        {false, true,  false, false}},
      // Shared catalogs, and replicated verbatim.
      {"listRoles",             {false, false, false, false}},
      {"listTablespaces",       {false, false, false, false}},

      // Per-database catalogs, identical on a physical replica.
      {"checkKey",              {true,  false, false, false}},
      {"databaseSize",          {true,  false, false, false}},
      {"enumDetails",           {true,  false, false, false}},
      {"explainQuery",          {true,  false, false, false}},
      {"functionDetails",       {true,  false, false, false}},
      {"listAccessMethods",     {true,  false, false, false}},
      {"listCasts",             {true,  false, false, false}},
      {"listCollations",        {true,  false, false, false}},
      {"listEnums",             {true,  false, false, false}},
      {"listEventTriggers",     {true,  false, false, false}},
      {"listExtendedStatistics",{true,  false, false, false}},
      {"listExtensions",        {true,  false, false, false}},
      {"listForeignServers",    {true,  false, false, false}},
      {"listForeignTables",     {true,  false, false, false}},
      {"listFunctions",         {true,  false, false, false}},
      {"listLanguages",         {true,  false, false, false}},
      {"listOperatorClasses",   {true,  false, false, false}},
      {"listOperators",         {true,  false, false, false}},
      {"listPublications",      {true,  false, false, false}},
      {"listSchemas",           {true,  false, false, false}},
      {"listSequences",         {true,  false, false, false}},
      {"listSubscriptions",     {true,  false, false, false}},
      {"listTextSearchConfigs", {true,  false, false, false}},
      {"listTypes",             {true,  false, false, false}},
      {"searchEnums",           {true,  false, false, false}},
      {"searchFunctions",       {true,  false, false, false}},
      // Predefined-role membership is cluster-wide, but which extensions are
      // installed is per database, and so are object grants -- so the answer
      // legitimately differs between two databases of one instance. A physical
      // replica returns it verbatim.
      {"checkPrivileges",       {true,  false, false, false}},
      // Planning is per database, and a physical replica plans identically off
      // the same statistics. Like explainQuery it is never swept: the same
      // statement is rarely valid in another database.
      {"evaluateIndex",         {true,  false, false, false}},
      {"tableBloat",            {true,  false, false, false}},
      {"typeDetails",           {true,  false, false, false}},
      // 4.0.0 moved these three off the per_server row below. They were only
      // ever there because they carried statistics counters; structure is
      // byte-identical on a physical replica by definition, so a
      // replication_group sweep used to re-run the whole structural payload --
      // columns, constraints, triggers, policies, view definitions -- once per
      // replica to get identical bytes back. Correcting this row is the point
      // of the split, not a side effect of it.
      {"listTables",            {true,  false, false, false}},
      {"searchTables",          {true,  false, false, false}},
      {"tableDetails",          {true,  false, false, false}},
      // Measured sizes read the same files on every member of a replication
      // group, so they belong here too rather than beside the counters.
      {"listTableSizes",        {true,  false, false, false}},
      {"tableSize",             {true,  false, false, false}},

      // Per-database *and* per-server: they carry statistics counters.
      {"duplicateIndexes",      {true,  true,  false, false}},
      {"indexBloat",            {true,  true,  false, false}},
      {"tableIOStats",          {true,  true,  false, false}},
      // The other half of the split, and the half that is worth sweeping:
      // scan counts and dead tuples are each server's own, and vacuum only
      // runs on the primary.
      {"listTableStats",        {true,  true,  true,  false}},
      {"tableStats",            {true,  true,  true,  false}},
      // Frozen xids are replicated, but the vacuum counters beside them are not
      // meaningful on a server where vacuum never runs.
      {"wraparoundStatus",      {true,  false, true,  false}},
    };
    return m;
  }

  // What to add to a tool's description so the model can tell which of the
  // three classes it is in. The payload cannot say it, and a sweep that
  // silently repeated one answer would read as agreement between databases.
  static std::string scope_note(const std::string& name, const ToolScope& sc) {
    if (sc.registry) return "";
    if (name == "explainQuery")
      return " Never runs across more than one connection: the same statement is"
             " rarely valid in another database, and with analyze it would"
             " execute once per member.";
    std::string note;
    if (!sc.per_database)
      note += " This reading is instance-wide -- every database on the same"
              " postmaster returns it identically, so asking each of them in turn"
              " repeats one answer.";
    if (!sc.per_server)
      note += " A physical replica is byte-identical here, so asking each member"
              " of a replication group adds nothing.";
    else if (!sc.registry)
      note += " The counters here are each server's own, so members of a"
              " replication group legitimately disagree and the answer is their"
              " sum, not the primary's copy.";
    if (sc.primary_authoritative)
      note += " Dead tuples and the last vacuum and analyze times describe work"
              " that only happens on a primary; on a replica they are noise, not"
              " a second opinion.";
    return note;
  }

  // ======================= Resources ======================================
  //
  // The organising principle is the spec's control model read through
  // volatility: a resource is a document a client may pin into context and
  // re-read later, so only structure belongs here. Everything that changes
  // without a DDL statement -- counters, sizes, activity, locks, plans --
  // stays a tool, because the model has to decide *when* to take a reading.
  //
  // This is what the 4.0.0 statistics split bought. Before it, listTables and
  // tableDetails carried n_dead_tup, last_vacuum and idx_scan, and serving
  // them as documents would have invited a client to cache a number that moves
  // under it. Now the structure half is genuinely stable and the readings have
  // their own tools.
  //
  // URI scheme, namespaced per connection so multi-database stays coherent. A
  // resource names exactly one object in exactly one database; the topology
  // axes are deliberately absent, because partial failure across members can
  // only be reported per member, and that belongs to tools.
  //
  //   pglicht://{conn}/schemas
  //   pglicht://{conn}/schema/{schema}
  //   pglicht://{conn}/schema/{schema}/table/{table}
  //   pglicht://{conn}/schema/{schema}/functions
  //   pglicht://{conn}/schema/{schema}/enums
  //   pglicht://{conn}/schema/{schema}/types
  //   pglicht://{conn}/server/roles
  //   pglicht://{conn}/server/extensions
  //   pglicht://{conn}/server/settings

  static std::string uri_encode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (char raw : s) {
      const unsigned char c = static_cast<unsigned char>(raw);
      if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') out += raw;
      else { out += '%'; out += hex[c >> 4]; out += hex[c & 0x0F]; }
    }
    return out;
  }

  static std::string uri_decode(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size(); i++) {
      if (s[i] == '%' && i + 2 < s.size()) {
        out += static_cast<char>(std::stoi(s.substr(i + 1, 2), nullptr, 16));
        i += 2;
      } else out += s[i];
    }
    return out;
  }

  static json resource_entry(const std::string& uri, const std::string& name,
                             const std::string& desc) {
    return {{"uri", uri}, {"name", name}, {"description", desc},
            {"mimeType", "application/json"}};
  }

  // Bounded on purpose, and it opens no connection at all. Everything here is
  // read from the registry; schemas, tables, functions, enums and types are all
  // reached through resources/templates/list instead.
  //
  // 4.0.0 enumerated schemas here, which meant one connection per configured
  // connection, sequentially, on a call clients make eagerly at startup. On a
  // registry of remote databases that is the single most expensive thing this
  // server does: twenty databases at 80 ms round-trip is upwards of ten seconds
  // before the client is usable, and one host that blackholes packets adds the
  // operating system's full connect timeout on top. The schema list is still
  // available -- pglicht://{conn}/schemas returns it -- but now only when
  // somebody actually reads it.
  json get_resources_list() {
    json out = json::array();
    for (const auto& conn : registry_.names()) {
      const std::string c = uri_encode(conn);
      const std::string base = "pglicht://" + c;
      out.push_back(resource_entry(base + "/schemas", conn + " schemas",
                                   "Every schema in " + conn + ", with its tables and role grants."));
      out.push_back(resource_entry(base + "/server/roles", conn + " roles",
                                   "Cluster-wide roles, attributes and memberships."));
      out.push_back(resource_entry(base + "/server/extensions", conn + " extensions",
                                   "Installed extensions, their versions and schemas."));
      out.push_back(resource_entry(base + "/server/settings", conn + " settings",
                                   "Server configuration, grouped by category."));
    }
    return {{"resources", out}};
  }

  json get_resource_templates_list() {
    json t = json::array();
    auto tpl = [&](const char* uri, const char* name, const char* desc) {
      t.push_back({{"uriTemplate", uri}, {"name", name}, {"description", desc},
                   {"mimeType", "application/json"}});
    };
    tpl("pglicht://{conn}/schema/{schema}/table/{table}", "table structure",
        "Columns, indexes, constraints, foreign keys in both directions, triggers, "
        "policies and grants for one table. Structure only -- for counters call "
        "the tableStats tool, for measured size the tableSize tool.");
    tpl("pglicht://{conn}/schema/{schema}/functions", "schema functions",
        "Functions and procedures in one schema.");
    tpl("pglicht://{conn}/schema/{schema}/enums", "schema enums",
        "Enum types in one schema, with their ordered values.");
    tpl("pglicht://{conn}/schema/{schema}/types", "schema types",
        "Composite types, domains and range types in one schema.");
    tpl("pglicht://{conn}/schema/{schema}", "schema tables",
        "Structure of every table and view in one schema.");
    return {{"resourceTemplates", t}};
  }

  // Splits a pglicht:// URI and answers it with the query method that already
  // exists. Throws std::invalid_argument for anything unrecognised, which the
  // caller turns into -32602 rather than a connect error.
  json read_resource(const std::string& uri) {
    const std::string prefix = "pglicht://";
    if (uri.rfind(prefix, 0) != 0)
      throw std::invalid_argument("unknown resource scheme: " + uri);

    std::vector<std::string> seg;
    {
      std::string rest = uri.substr(prefix.size()), cur;
      for (char ch : rest) {
        if (ch == '/') { seg.push_back(uri_decode(cur)); cur.clear(); }
        else cur += ch;
      }
      seg.push_back(uri_decode(cur));
    }
    if (seg.size() < 2) throw std::invalid_argument("incomplete resource URI: " + uri);

    const std::string conn = seg[0];
    // Fails here, naming the configured connections, rather than as a
    // confusing connect error later.
    (void)registry_.get(conn);

    struct Restore {
      std::string& slot; std::string prev;
      ~Restore() { slot = prev; }
    } restore{active_, active_};
    active_ = conn;

    if (seg.size() == 2 && seg[1] == "schemas")               return schemas();
    if (seg.size() == 3 && seg[1] == "server") {
      if (seg[2] == "roles")      return roles();
      if (seg[2] == "extensions") return extensions();
      if (seg[2] == "settings")   return server_settings();
    }
    if (seg[1] == "schema" && seg.size() >= 3) {
      const std::string& sch = seg[2];
      if (seg.size() == 3)                              return tables(sch);
      if (seg.size() == 4 && seg[3] == "functions")     return functions(sch);
      if (seg.size() == 4 && seg[3] == "enums")         return enums(sch);
      if (seg.size() == 4 && seg[3] == "types")         return types(sch);
      if (seg.size() == 5 && seg[3] == "table")         return table(sch, seg[4]);
    }
    throw std::invalid_argument("unknown resource: " + uri);
  }

  // ======================= Prompts ========================================
  //
  // Static templates with argument substitution: no database is touched until
  // the model acts on the message and calls a tool. Each one encodes an order
  // of investigation that is easy to get wrong -- read before you write, check
  // the replication slot before blaming bloat, observe the role before
  // trusting a reading -- and names the tools that answer each step.

  struct PromptArg { const char* name; const char* desc; bool required; };
  struct PromptDef {
    const char* name;
    const char* description;
    std::vector<PromptArg> args;
    std::string (*render)(const json&);
  };

  static std::string arg_or(const json& a, const char* key, const std::string& fallback) {
    if (!a.contains(key) || a[key].is_null()) return fallback;
    if (a[key].is_string()) return a[key].get<std::string>();
    return a[key].dump();
  }

  static const std::vector<PromptDef>& prompt_defs() {
    static const std::vector<PromptDef> v = {
      {"diagnose-slow-query",
       "Trace a slow statement from pg_stat_statements to a plan and a fix.",
       {{"query_id", "queryid from statementStats; omit to start from the worst offender", false},
        {"min_duration_s", "ignore statements faster than this mean duration", false}},
       [](const json& a) -> std::string {
         const std::string qid = arg_or(a, "query_id", "");
         const std::string mind = arg_or(a, "min_duration_s", "");
         std::string s =
           "Diagnose a slow statement on this PostgreSQL server.\n\n"
           "0. Call checkPrivileges first. statementStats hides the query text "
           "of other roles without stats access, explainQuery needs SELECT on "
           "the tables referenced, and tableBloat needs a grant of its own -- "
           "a plan built on tools that will not answer wastes the incident.\n"
           "1. Call statementStats to rank statements by total execution time. "
           "Read info.dealloc first: if it is non-zero the extension has been "
           "evicting entries, so the list is the slowest of what survived, not "
           "the slowest overall -- say so before drawing conclusions.\n";
         if (!qid.empty())
           s += "2. The statement of interest is query_id " + qid +
                ". Call explainQuery with that queryid.\n";
         else
           s += "2. Pick the statement with the largest total_exec_time"
                + (mind.empty() ? std::string()
                                : " whose mean duration is at least " + mind + "s")
                + " and call explainQuery with its query_id.\n";
         s +=
           "3. Read the plan for the usual causes in this order: a sequential "
           "scan on a large table, an estimate that is orders of magnitude off "
           "the actual row count, a sort or hash that spilled to disk, and a "
           "nested loop driven by a bad estimate.\n"
           "4. If the estimates are wrong, call tableStats on the tables "
           "involved and compare n_mod_since_analyze and last_analyze -- stale "
           "statistics explain more bad plans than missing indexes do.\n"
           "5. If the plan shows a sequential scan where a usable index exists, "
           "or an index scan fetching far more heap pages than it returns rows, "
           "suspect bloat rather than the plan. tableBloat measures dead space "
           "in the table, indexBloat in one index; both cost a scan, so name "
           "the object rather than sweeping the schema.\n"
           "6. If bloat is confirmed, the fix is usually vacuum reaching the "
           "table more often rather than a REINDEX. Read n_dead_tup and the "
           "last vacuum times from tableStats and the per-table autovacuum "
           "settings from tableDetails.reloptions, and follow "
           "bloat-and-vacuum-review before changing anything cluster-wide -- "
           "an unconsumed replication slot makes every autovacuum setting "
           "irrelevant, and that prompt checks for one.\n"
           "7. Before proposing an index, call duplicateIndexes to check that "
           "one does not already exist, and tableDetails to see what is there. "
           "If the change is DDL, follow plan-schema-change: an index that is "
           "correct and an index that is safe to create on a live server are "
           "different questions.\n"
           "8. Decide what kind of fix this is before writing one, and say "
           "which. Either the statement asks for something the planner cannot "
           "use -- a predicate that is not sargable, a function or a cast over "
           "an indexed column, NOT IN against a nullable subquery, OFFSET deep "
           "into a large result -- and the fix is a rewrite. Or the planner was "
           "misinformed, and the fix is statistics: an ANALYZE, a higher "
           "statistics target, or extended statistics for correlated columns. "
           "Or the plan is right but under-resourced, and the fix is "
           "configuration, such as work_mem for a node that spilled. Or the "
           "statement is already well formed and nothing supports the access "
           "path it needs -- and only then is the fix DDL.\n"
           "   Prefer them in that order, because that is the order of what "
           "they cost. An ANALYZE is free and instant. A rewrite costs a deploy "
           "and nothing in the database. Configuration changes the behaviour of "
           "every other query too. An index is a write cost paid by every "
           "INSERT and UPDATE for as long as it exists, to buy speed for one "
           "read pattern. Say why the cheaper options were rejected rather than "
           "passing over them.\n"           "9. Verify what can be verified. A rewrite can be checked here and "
           "now: call explainQuery on the rewritten statement and show that the "
           "plan actually changed, and how. An index is checkable too "
           "wherever hypopg is installed: call evaluateIndex with the CREATE "
           "INDEX statement and report whether the planner actually took it, "
           "because a proposed index the planner ignores is the common case "
           "and a cost figure alone hides it. checkPrivileges says whether "
           "hypopg is there; where it is not, an index is a prediction and has "
           "to be presented as one rather than as a result. Either way hand "
           "the creation to plan-schema-change: whether an index is correct "
           "and whether it is safe to build on this server are different "
           "questions, and evaluateIndex answers only the first.";
         return s;
       }},

      {"triage-lock-contention",
       "Find what is blocking what, and who to look at first.",
       {{"connection", "connection to investigate; defaults to the configured default", false}},
       [](const json& a) -> std::string {
         const std::string c = arg_or(a, "connection", "");
         const std::string on = c.empty() ? std::string() : " on connection " + c;
         return
           "Triage lock contention" + on + ".\n\n"
           "0. Call checkPrivileges. Without stats access, currentActivity hides "
           "the query text of backends belonging to other roles, which is most "
           "of what this investigation reads.\n\n"
           "Read before you write. Do not kill anything, and do not recommend "
           "killing anything until the last step.\n\n"
           "1. Call currentLocks. The rows carry a chain_depth: 0 is the "
           "backend asked about, and the largest depth is the backend at the "
           "root of the wait chain. That is the one to look at first.\n"
           "2. Call currentActivity for the pids in the chain. For each, read "
           "state, wait_event_type, query and how long the transaction has "
           "been open.\n"
           "3. An idle in transaction backend at the root is the usual answer, "
           "and the fix is in the application that left it open, not in the "
           "database.\n"
           "4. Report the chain root, what it is doing, how long it has held "
           "the lock, and what is queued behind it. Only then discuss whether "
           "terminating it is safe, and say what would be lost.";
       }},

      {"bloat-and-vacuum-review",
       "Decide whether bloat is real, and whether autovacuum is keeping up.",
       {{"schema", "schema to review; defaults to public", false}},
       [](const json& a) -> std::string {
         const std::string sch = arg_or(a, "schema", "public");
         return
           "Review bloat and autovacuum health for schema " + sch + ".\n\n"
           "0. Call checkPrivileges. tableBloat and indexBloat are denied outright "
           "to a role without the scanning grant, and tableStats returns null "
           "statistics rather than an error when it cannot read a column -- "
           "which looks exactly like a table nobody has analyzed.\n"
           "1. Call listTableStats for " + sch + ". Rank by n_dead_tup, and "
           "read last_vacuum and last_autovacuum beside it: a large dead-tuple "
           "count on a table vacuumed minutes ago is a busy table, not a "
           "neglected one.\n"
           "2. Call replicationSlots before concluding anything. An inactive "
           "or lagging slot holds back the xmin horizon, which stops vacuum "
           "from removing dead tuples cluster-wide -- and no amount of "
           "autovacuum tuning fixes it. This is the single most common wrong "
           "diagnosis in this area.\n"
           "3. Call wraparoundStatus. If any database or table is approaching "
           "autovacuum_freeze_max_age, that outranks ordinary bloat.\n"
           "4. For the worst few tables, call tableBloat to measure rather than "
           "estimate. Leave exact at its default first; the approximation is "
           "usually enough to rank them.\n"
           "5. For an index that looks redundant, duplicateIndexes says it is "
           "covered and idx_scan says nobody used it on this server -- but "
           "neither proves the planner would not miss it. Where hypopg is "
           "installed, evaluateIndex with 'hide' plans the query without the "
           "index and settles it. Dropping an index is easy; rebuilding one on "
           "a large table is not.\n"
           "6. Only where bloat is confirmed, look at per-table autovacuum "
           "storage parameters via tableDetails.reloptions and propose "
           "changes. Say which reading justifies each one.";
       }},

      {"buffer-cache-review",
       "See what is occupying shared buffers and whether it is the right thing.",
       {{"connection", "connection to review; defaults to the configured default", false}},
       [](const json& a) -> std::string {
         const std::string c = arg_or(a, "connection", "");
         const std::string on = c.empty() ? std::string() : " on connection " + c;
         return
           "Review shared buffer usage" + on + ".\n\n"
           "0. Call checkPrivileges. Both buffer-cache tools need the monitoring "
           "role, and they are the whole of this review.\n"
           "1. Call bufferCacheSummary first. It is cheap; bufferCacheContents "
           "aggregates every buffer and is not.\n"
           "2. Call hostCapacity and compare shared_buffers against the host's "
           "RAM, and effective_cache_size against what the OS can plausibly "
           "cache.\n"
           "3. Call tableIOStats. A low hit ratio on a small, frequently read "
           "table is the actionable case; a low ratio on a large table scanned "
           "once a day is not a problem.\n"
           "4. Only if the summary suggests something is wrong, call "
           "bufferCacheContents to see which relations hold the buffers.\n"
           "5. Report whether the cache is undersized, mis-sized relative to "
           "the host, or being churned by one workload, and say which reading "
           "supports the conclusion.";
       }},

      {"capacity-check",
       "Check memory and parallelism settings against the machine.",
       {{"connection", "connection to check; defaults to the configured default", false}},
       [](const json& a) -> std::string {
         const std::string c = arg_or(a, "connection", "");
         const std::string on = c.empty() ? std::string() : " for connection " + c;
         return
           "Check server capacity settings" + on + ".\n\n"
           "1. Call hostCapacity. If it reports that host RAM and vCPU count "
           "are unknown, say so and stop with a partial answer: PostgreSQL "
           "cannot see the machine it runs on, and those values have to be "
           "declared per connection in the config file. Do not guess them.\n"
           "2. Read derived.committed_worst_case_percent_of_ram. This is "
           "work_mem times max_connections plus maintenance_work_mem times "
           "autovacuum_max_workers, and it is the number that decides whether "
           "the OOM killer is a risk.\n"
           "3. Compare shared_buffers and effective_cache_size against RAM.\n"
           "4. Check parallelism: max_parallel_workers_per_vcpu above 1 means "
           "a single query can oversubscribe the machine.\n"
           "5. Call databaseStats and read numbackends against max_connections "
           "to see how much of the worst case is actually reached.\n"
           "6. Report each finding with the reading behind it, and name the "
           "setting to change.";
       }},

      {"replication-slot-review",
       "Find what a replication slot is holding back, and what it costs to release it.",
       {{"connection", "connection to review; defaults to the configured default", false}},
       [](const json& a) -> std::string {
         const std::string c = arg_or(a, "connection", "");
         const std::string on = c.empty() ? std::string() : " on connection " + c;
         return
           "Review replication slots" + on + " and what they are holding back.\n\n"
           "An unconsumed slot is the failure mode that presents as something "
           "else. It holds back the xmin horizon, so vacuum cannot remove dead "
           "tuples anywhere in the cluster; and it pins WAL until the disk "
           "fills. Both symptoms get diagnosed as bloat, or as a disk problem, "
           "and neither diagnosis leads anywhere.\n\n"
           "0. Call checkPrivileges: a role without stats access sees less of "
           "replicationSlots and of currentActivity than this review needs.\n"
           "1. Call replicationSlots. For each slot read three things: whether "
           "it is active, how far its restart_lsn trails the current WAL "
           "position, and its wal_status. reserved is healthy. extended means "
           "it has gone past max_wal_size. unreserved means the slot is now the "
           "only thing keeping that WAL on disk. lost means the WAL it needs is "
           "already gone and the consumer cannot resume -- that slot is dead, "
           "and only rebuilding its consumer fixes it.\n"
           "2. An inactive slot with growing lag is the finding. Before "
           "anything else, establish whose it is: listTopology and "
           "verifyTopology for physical replicas, listSubscriptions and "
           "listPublications for logical ones. A slot with no owner anybody "
           "recognises is the common case and the easy one.\n"
           "3. Measure how fast it grows before deciding how urgent it is. "
           "checkpointStats reports WAL volume, and serverSettings carries "
           "max_slot_wal_keep_size -- if that is set, PostgreSQL will "
           "invalidate the slot rather than fill the disk, which trades a "
           "broken replica for a live primary. If it is unset, nothing bounds "
           "the growth.\n"
           "4. Call wraparoundStatus. The same held xmin horizon also stops "
           "freezing, and if wraparound headroom is shrinking that outranks the "
           "disk: one ends in a slow server, the other in a cluster that stops "
           "accepting writes.\n"
           "5. Report the slot, its owner, what it is holding, and how long the "
           "disk has at the current rate. Dropping a slot is irreversible for "
           "its consumer: say exactly what would have to be rebuilt, and prefer "
           "fixing or decommissioning the consumer over dropping the slot "
           "underneath it.";
       }},

      {"plan-schema-change",
       "Choose how to apply a DDL change by measuring the table it targets.",
       {{"change", "the change you intend to make, in your own words", true},
        {"schema", "schema of the table being changed", false},
        {"table", "table being changed", false}},
       [](const json& a) -> std::string {
         const std::string change = arg_or(a, "change", "");
         const std::string sch = arg_or(a, "schema", "");
         const std::string tbl = arg_or(a, "table", "");
         std::string target;
         if (!tbl.empty()) target = "Target: " + (sch.empty() ? "" : sch + ".") + tbl + "\n";
         return
           "Plan this schema change against what this database actually is.\n\n"
           "The change: " + change + "\n" + target + "\n"
           "This server is read-only and will run none of it. What you produce "
           "is a plan for an operator to apply.\n\n"
           "Do not answer from a recipe. The safe method for a change is a "
           "property of the table in front of you, not of the statement: the "
           "same DDL that is instant on one table is an outage on another. A "
           "table with no rows can be rewritten in place and nobody notices; "
           "the same rewrite at a billion rows is hours under an exclusive "
           "lock. Partitioning is the extreme case -- trivial before there is "
           "data, a migration project with a cutover after it. Reaching for the "
           "safest-at-scale approach on a small table is complexity nobody "
           "needs, and reaching for the simple one on a large table is the "
           "outage. Measure first, then choose, and say which reading decided "
           "it.\n\n"
           "1. Call checkPrivileges. tableStats and tableSize are what this "
           "whole plan rests on; if either is degraded here, say so rather than "
           "estimating, because they are what separates a metadata change from "
           "a full rewrite.\n"
           "2. Identify every object the change touches, then measure each of "
           "them -- not only the one named as the target. A statement that "
           "names one table routinely locks more than one: a foreign key locks "
           "the table it references as well as the table it is added to, a "
           "partitioned table means the parent and every partition, and a "
           "column that other tables reference cannot be considered alone. The "
           "object you did not name is often the busier one, and its lock is "
           "the one that surprises people. Read the change itself for the "
           "objects it names, and tableDetails for the ones the catalog knows "
           "about -- foreign keys in both directions, and inheritance or "
           "partition parents.\n"
           "   For each of them:\n"
           "   - tableStats: the row count, and n_distinct and most_common_vals "
           "when the change involves a default, an index or a partition key -- "
           "most_common_vals is the empirical answer to what value the rows "
           "already hold\n"
           "   - tableSize: what a rewrite would actually move. Use the "
           "measured size, not the estimate; this is the number that turns "
           "\"instant\" into \"an hour\"\n"
           "   - tableDetails: the indexes, constraints, inbound foreign keys "
           "and triggers already there, and whether it is already partitioned. "
           "Each one multiplies the cost of a rewrite, and some rule out "
           "approaches outright\n"
           "   - the scan and tuple counters in tableStats: how busy it is. A "
           "lock window costs nothing on a table nothing is touching\n"
           "   - currentActivity and currentLocks: the oldest running "
           "transaction, and whether anything already holds or waits for a lock "
           "on any of these objects. A brief exclusive lock request waits "
           "behind a long transaction, and everything arriving after it waits "
           "behind that -- which is how a millisecond operation becomes an "
           "outage. Check this for every object the change touches, since one "
           "busy parent is enough to stall the whole statement\n"
           "   - serverSettings for the server version, since what is possible "
           "and what it costs both moved across majors\n"
           "   - listTopology and replicationSlots if the change rewrites: the "
           "WAL a rewrite generates has to reach every replica and pass through "
           "every slot\n"
           "3. From those numbers, classify the change before writing any DDL. "
           "Say which of the three it is -- metadata only, a scan without a "
           "rewrite, or a full rewrite of the table and its indexes -- and "
           "separately what lock it needs and for how long. The lock level "
           "alone settles nothing: a strong lock held for a millisecond is "
           "safe, and a weak one held for an hour may not be. Duration is the "
           "number that matters, and duration comes from the measurements.\n"
           "4. Choose the method by matching that cost against what this table "
           "can tolerate. Above some size the incremental approach is the only "
           "one available; below it, it is machinery for nothing. State the "
           "size or rate at which your answer would flip, so the reasoning can "
           "be checked against a different table later.\n"
           "5. Give the plan as ordered DDL. Against each statement say the "
           "lock it takes, whether it rewrites, what must not be running "
           "alongside it, and roughly how long at this table's measured size.\n"
           "6. Say what the revert looks like, and flag anything irreversible "
           "as irreversible whatever the size.";
       }},

      {"explain-and-fix",
       "Explain one statement and propose a concrete fix.",
       {{"sql", "the statement to explain", true},
        {"params", "JSON array of parameter values, if the statement is parameterised", false}},
       [](const json& a) -> std::string {
         const std::string sql = arg_or(a, "sql", "");
         const std::string prm = arg_or(a, "params", "");
         return
           "Explain this statement and propose a fix.\n\n"
           "SQL:\n" + sql + "\n\n" +
           (prm.empty() ? std::string()
                        : "Parameters: " + prm + "\n\n") +
           "0. Call checkPrivileges: EXPLAIN needs SELECT on every table the "
           "statement touches, so a restricted role fails here rather than "
           "returning a worse plan.\n"
           "1. Call explainQuery with this sql" +
           (prm.empty() ? std::string() : " and these params") +
           ". Leave analyze false on the first call: the plan alone usually "
           "shows the problem, and analyze executes the statement.\n"
           "2. If the plan is not conclusive and the statement is a SELECT, "
           "call again with analyze true to get actual row counts and timing. "
           "The server proves the plan has no ModifyTable node before running "
           "it, so this cannot mutate -- but say that you are about to run it.\n"
           "3. Compare estimated against actual rows at each node. A ratio "
           "worse than about 100x is a statistics problem, not an index "
           "problem.\n"
           "4. Call tableDetails and tableStats on the tables involved before "
           "proposing an index: check what indexes exist, and whether "
           "last_analyze is recent.\n"
           "5. Decide what kind of fix this is before writing one, and say "
           "which. Either the statement asks for something the planner cannot "
           "use -- a predicate that is not sargable, a function or a cast over "
           "an indexed column, NOT IN against a nullable subquery, OFFSET deep "
           "into a large result -- and the fix is a rewrite. Or the planner was "
           "misinformed, and the fix is statistics: an ANALYZE, a higher "
           "statistics target, or extended statistics for correlated columns. "
           "Or the plan is right but under-resourced, and the fix is "
           "configuration, such as work_mem for a node that spilled. Or the "
           "statement is already well formed and nothing supports the access "
           "path it needs -- and only then is the fix DDL.\n"
           "   Prefer them in that order, because that is the order of what "
           "they cost. An ANALYZE is free and instant. A rewrite costs a deploy "
           "and nothing in the database. Configuration changes the behaviour of "
           "every other query too. An index is a write cost paid by every "
           "INSERT and UPDATE for as long as it exists, to buy speed for one "
           "read pattern. Say why the cheaper options were rejected rather than "
           "passing over them.\n"           "6. Verify what can be verified. A rewrite can be checked here and "
           "now: call explainQuery on the rewritten statement and show that the "
           "plan actually changed, and how. An index is checkable too "
           "wherever hypopg is installed: call evaluateIndex with the CREATE "
           "INDEX statement and report whether the planner actually took it, "
           "because a proposed index the planner ignores is the common case "
           "and a cost figure alone hides it. checkPrivileges says whether "
           "hypopg is there; where it is not, an index is a prediction and has "
           "to be presented as one rather than as a result. Either way hand "
           "the creation to plan-schema-change: whether an index is correct "
           "and whether it is safe to build on this server are different "
           "questions, and evaluateIndex answers only the first.";
       }},
    };
    return v;
  }

  json get_prompts_list() {
    json out = json::array();
    for (const auto& p : prompt_defs()) {
      json args = json::array();
      for (const auto& a : p.args)
        args.push_back({{"name", a.name}, {"description", a.desc}, {"required", a.required}});
      out.push_back({{"name", p.name}, {"description", p.description}, {"arguments", args}});
    }
    return {{"prompts", out}};
  }

  json get_prompt(const std::string& name, const json& arguments) {
    for (const auto& p : prompt_defs()) {
      if (name != p.name) continue;
      for (const auto& a : p.args)
        if (a.required && (!arguments.contains(a.name) || arguments[a.name].is_null()))
          throw std::invalid_argument(std::string("prompt ") + p.name +
                                      " requires argument '" + a.name + "'");
      return {{"description", p.description},
              {"messages", {{{"role", "user"},
                             {"content", {{"type", "text"}, {"text", p.render(arguments)}}}}}}};
    }
    throw std::invalid_argument("unknown prompt: " + name);
  }

  // ======================= Completions ====================================
  //
  // completion/complete is defined for prompt arguments and resource template
  // variables -- there is no ref/tool in the spec -- so this covers the schema,
  // table and connection variables that appear in both. The backing queries
  // already exist; nothing new touches the database.
  //
  // A completion is a convenience, so it must never be the thing that fails a
  // session: an unreachable database or an unreadable catalog returns an empty
  // list rather than an error.
  json complete(const json& ref, const json& argument) {
    const std::string arg_name = argument.value("name", "");
    const std::string typed    = argument.value("value", "");
    std::vector<std::string> values;

    auto starts_with = [&](const std::string& s) {
      return typed.empty() || s.rfind(typed, 0) == 0;
    };

    try {
      if (arg_name == "conn" || arg_name == "connection") {
        for (const auto& n : registry_.names()) if (starts_with(n)) values.push_back(n);
      } else if (arg_name == "schema") {
        const std::string prev = active_;
        struct R { std::string& s; std::string p; ~R(){ s = p; } } r{active_, prev};
        // A resource template carries the connection in the same URI, but the
        // completion request does not pass sibling variables, so this can only
        // complete against the default connection. Better than nothing, and it
        // never guesses at a connection the caller did not name.
        const json all = schemas();
        if (all.is_object())
          for (const auto& it : all.items())
            if (starts_with(it.key())) values.push_back(it.key());
      } else if (arg_name == "table") {
        const std::string prev = active_;
        struct R { std::string& s; std::string p; ~R(){ s = p; } } r{active_, prev};
        const json all = tables("public");
        if (all.is_object())
          for (const auto& it : all.items())
            if (starts_with(it.key())) values.push_back(it.key());
      }
    } catch (const std::exception&) {
      values.clear();
    }
    (void)ref;

    // The spec caps a completion response at 100 values and asks the server to
    // say whether more exist.
    const bool has_more = values.size() > 100;
    if (has_more) values.resize(100);
    return {{"completion", {{"values", values},
                            {"total", values.size()},
                            {"hasMore", has_more}}}};
  }

  // --- outputSchema -------------------------------------------------------
  //
  // Emitted only to a client that negotiated 2025-06-18: that revision defined
  // both `outputSchema` and `structuredContent`, and declaring a schema for a
  // payload the client will never receive in structured form is noise.
  //
  // These are deliberately permissive, and that is the design rather than a
  // shortcut. The roadmap's own warning is the real risk: a client that
  // validates results against a declared schema turns any drift between the
  // schema and the payload into a failed call, on a call that worked before.
  // Three things make leaf-level schemas unsafe here:
  //
  //   1. Payloads are version-conditional. tableStats gains
  //      n_tup_newpage_upd and last_seq_scan on PostgreSQL 16, checkpointStats
  //      is normalised across three different sets of underlying views, and a
  //      schema would have to be re-proved on five majors for 58 tools.
  //   2. Every tool carries the same escape hatch: when an extension is absent
  //      or a grant is missing, the payload is {error, hint} instead of the
  //      documented shape. A schema that enumerated the documented shape would
  //      reject exactly the answer an operator most needs to read.
  //   3. A catalog is open-ended. New PostgreSQL releases add columns.
  //
  // So each schema names the shape it can actually promise, types the keys
  // that are unconditional, marks nothing `required`, and allows additional
  // properties. It can describe a payload; it can never reject one this server
  // produces. A test walks real payloads against these to keep that true.

  static json schema_map(const std::string& keyed_by, const std::string& entry) {
    return {{"type", "object"},
            {"description", "Keyed by " + keyed_by + "; each value is " + entry +
                            ". An absent extension or a missing grant is reported "
                            "as {error, hint} in place of the map."},
            {"additionalProperties", true}};
  }

  static json schema_fixed(const std::string& desc,
                           std::initializer_list<std::pair<const char*, const char*>> props) {
    json p = json::object();
    // Every declared type admits null. Almost all of these come out of a LEFT
    // JOIN or a nullable catalog column -- tableDetails.definition is null for
    // an ordinary table, reloptions is null unless storage parameters were set,
    // last_vacuum is null on a table never vacuumed -- and a schema that said
    // "integer" where the server can legitimately answer null would reject a
    // correct payload. That is the one failure this whole table exists to
    // avoid, so it is spelled out rather than left to chance.
    for (const auto& kv : props)
      p[kv.first] = json{{"type", json::array({kv.second, "null"})}};
    return {{"type", "object"},
            {"description", desc + " An absent extension or a missing grant is "
                            "reported as {error, hint} instead."},
            {"properties", p},
            {"additionalProperties", true}};
  }

  static const std::map<std::string, json>& tool_output_schemas() {
    static const std::map<std::string, json> m = {
      // --- schema exploration: name -> object ---
      {"listSchemas",            schema_map("schema name", "that schema's tables, views and role grants")},
      {"listTables",             schema_map("table name", "that table's structure")},
      {"searchTables",           schema_map("schema-qualified table name", "that table's structure")},
      {"listFunctions",          schema_map("function signature", "that routine's signature and properties")},
      {"searchFunctions",        schema_map("schema-qualified function signature", "that routine's signature and properties")},
      {"functionDetails",        schema_map("function signature", "that routine's source, definition and grants")},
      {"listEnums",              schema_map("enum type name", "that enum's ordered values")},
      {"searchEnums",            schema_map("enum type name", "that enum's ordered values")},
      {"enumDetails",            schema_map("enum type name", "its values and the columns that reference it")},
      {"listTypes",              schema_map("type name", "that composite, domain or range type")},
      {"typeDetails",            schema_map("type name", "its attributes or subtype and the columns using it")},
      {"listSequences",          schema_map("sequence name", "that sequence's range, increment and owning column")},
      {"listExtendedStatistics", schema_map("statistics object name", "its target table, columns and kinds")},
      {"listCollations",         schema_map("collation name", "its provider, ctype and determinism")},
      {"listCasts",              schema_map("source->target type pair", "that cast's function and context")},
      {"listOperators",          schema_map("operator signature", "its operand and result types")},
      {"listOperatorClasses",    schema_map("opclass name and access method", "its input type and default flag")},
      {"listAccessMethods",      schema_map("access method name", "its kind and handler")},
      {"listLanguages",          schema_map("language name", "its handler, trust and ownership")},
      {"listTextSearchConfigs",  schema_map("configuration name", "its parser and token mappings")},
      {"listEventTriggers",      schema_map("event trigger name", "its event, function and enabled state")},
      {"listExtensions",         schema_map("extension name", "its version and installation schema")},
      {"listRoles",              schema_map("role name", "its attributes and group memberships")},
      {"listTablespaces",        schema_map("tablespace name", "its location, owner and options")},
      {"listForeignServers",     schema_map("server name", "its wrapper, options and user mappings")},
      {"listForeignTables",      schema_map("foreign table name", "its server and column list")},
      {"listPublications",       schema_map("publication name", "its tables and replicated operations")},
      {"listSubscriptions",      schema_map("subscription name", "its publication, slot and state")},
      {"replicationSlots",       schema_map("slot name", "its type, activity and retained WAL")},

      // --- statistics keyed by object ---
      {"listTableStats",         schema_map("table name", "that table's statistics counters and size estimate")},
      {"listTableSizes",         schema_map("table name", "that table's measured size, index size and total")},
      {"tableIOStats",           schema_map("schema-qualified table name", "its buffer cache hit ratios and scan counts")},
      {"databaseStats",          schema_map("database name", "that database's pg_stat_database counters")},
      {"currentActivity",        schema_map("backend pid", "that backend's state, query and wait event")},
      {"serverSettings",         schema_map("settings category", "a map of setting name to its value and metadata")},
      {"bufferCacheContents",    schema_map("schema-qualified relation name", "its buffered pages and usage counts")},

      // --- fixed shapes ---
      {"tableDetails",           schema_fixed("One table's structure.",
                                   {{"table", "string"}, {"kind", "string"}, {"description", "string"},
                                    {"columns", "object"}, {"primary_key", "array"}, {"indexes", "object"},
                                    {"constraints", "object"}, {"foreign_keys", "object"},
                                    {"referenced_by", "object"}, {"triggers", "object"}, {"rules", "object"},
                                    {"row_level_security", "object"}, {"policies", "object"},
                                    {"roles", "object"}})},
      {"tableStats",             schema_fixed("One table's statistics.",
                                   {{"table", "string"}, {"rows", "number"}, {"size_estimate", "integer"},
                                    {"seq_scan", "integer"}, {"idx_scan", "integer"},
                                    {"n_live_tup", "integer"}, {"n_dead_tup", "integer"},
                                    {"n_mod_since_analyze", "integer"}, {"n_ins_since_vacuum", "integer"},
                                    {"columns", "object"}, {"indexes", "object"}})},
      {"tableSize",              schema_fixed("One table's measured size.",
                                   {{"table", "string"}, {"kind", "string"}, {"main_size", "integer"},
                                    {"size", "integer"}, {"indexes_size", "integer"},
                                    {"total_size", "integer"}, {"indexes", "object"}})},
      {"databaseSize",           schema_fixed("Size of the connected database.",
                                   {{"database", "string"}, {"size", "integer"}})},
      {"checkKey",               schema_fixed("Whether a row with the given key exists.",
                                   {{"exists", "boolean"}})},
      {"evaluateIndex",          schema_fixed("How a statement would plan with different indexes.",
                                   {{"statement", "string"}, {"hypopg_version", "string"},
                                    {"baseline", "object"}, {"hypothetical", "object"},
                                    {"cost_ratio", "number"}, {"indexes", "array"},
                                    {"hidden", "array"}, {"note", "string"}})},
      {"checkPrivileges",        schema_fixed("Which tools this role can use on this connection.",
                                   {{"connection", "string"}, {"role", "string"},
                                    {"tools", "integer"}, {"available", "integer"},
                                    {"degraded", "array"}, {"denied", "array"}})},
      {"currentLocks",           schema_fixed("Lock rows, newest blocking chain first.",
                                   {{"locks", "array"}})},
      {"listConnections",        schema_fixed("The configured connection registry.",
                                   {{"connections", "array"}})},
      {"listTopology",           schema_fixed("The three configured topology axes.",
                                   {{"instances", "array"}, {"replication_groups", "array"},
                                    {"groups", "array"}, {"unlabelled", "array"}})},
      {"verifyTopology",         schema_fixed("Declared topology checked against each server.",
                                   {{"connections", "array"}, {"findings", "array"}})},
      {"ioStats",                schema_fixed("pg_stat_io rows per backend type and context.",
                                   {{"io", "array"}})},
      {"duplicateIndexes",       schema_fixed("Indexes that duplicate or cover another.",
                                   {{"identical", "array"}, {"redundant", "array"}})},
      {"progressStats",          schema_fixed("Running commands, one array per category; always all six.",
                                   {{"vacuum", "array"}, {"analyze", "array"}, {"create_index", "array"},
                                    {"cluster", "array"}, {"copy", "array"}, {"basebackup", "array"}})},
      {"wraparoundStatus",       schema_fixed("Transaction id and multixact headroom.",
                                   {{"databases", "object"}, {"tables", "array"}, {"limits", "object"}})},
      {"checkpointStats",        schema_fixed("Checkpoint, WAL and background writer activity, normalised across versions.",
                                   {{"checkpointer", "object"}, {"bgwriter", "object"}, {"backend_io", "object"},
                                    {"wal", "object"}, {"settings", "object"}, {"source", "string"}})},
      {"hostCapacity",           schema_fixed("Memory and parallelism settings against the host.",
                                   {{"host", "object"}, {"server", "object"}, {"settings", "object"},
                                    {"derived", "object"}})},
      {"statementStats",         schema_fixed("pg_stat_statements rows with the extension's own counters.",
                                   {{"statements", "array"}, {"info", "object"}})},
      {"explainQuery",           schema_fixed("An EXPLAIN plan and what produced it.",
                                   {{"plan", "array"}, {"sql", "string"}, {"source", "string"},
                                    {"analyzed", "boolean"}, {"generic", "boolean"},
                                    {"read_only", "boolean"}})},
      {"tableBloat",             schema_fixed("Physical storage bloat for one table.", {})},
      {"indexBloat",             schema_fixed("Physical statistics for one index, per access method.", {})},
      {"bufferCacheSummary",     schema_fixed("Shared buffer occupancy across the instance.", {})},
    };
    return m;
  }

  const json get_tools_list(const std::string& protocol) {
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
	    {"description", "return the structure of every table, view and materialised view in a schema: kind, comment, storage options, columns and their per-column index counts, index count and constraint count. Structure only -- it changes when someone issues DDL and not otherwise. For row counts, scan counters, dead tuples and vacuum times call listTableStats; for measured on-disk sizes call listTableSizes"},
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
	    {"description", "return the structure of one table: columns with types, defaults, storage and compression, primary key, indexes, constraints, foreign keys, inbound foreign keys (referenced_by), triggers, rules, row-level security, policies and privileges. Structure only -- it changes when someone issues DDL and not otherwise, and it returns no sample column values. For row counts, scan counters, dead tuples, vacuum times and the pg_stats column histograms call tableStats; for measured on-disk sizes call tableSize"},
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
	    {"description", "find tables across every non-system schema by full-text search over table names, comments, column names and comments, enum labels and grantee names, and return their structure. Structure only -- there is no statistics counterpart, because a text search is how you find a table, not how you read a counter: name a match to tableStats or tableSize for those"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"web_search", {{"type", "string"}}}
		  }},
		{"required", {"web_search"}}
	      }}
	  },
	  {
	    {"name", "evaluateIndex"},
	    {"description", "plan a statement as if the indexes were different, using hypopg. 'create' takes CREATE INDEX statements to plan against without building them; 'hide' takes the names of existing indexes to plan without, which is how to ask whether an index is safe to drop. Nothing is built, no lock is taken and no catalog row is written, and the statement is never executed -- hypopg cannot serve EXPLAIN ANALYZE, so this is plan-only and safer than explainQuery with analyze. Returns the plan and total cost before and after, and for each index whether the planner actually used it, which is the answer that matters: a proposed index the planner ignores is the common case and a cost figure alone hides it. The cost is the planner's estimate, not a measurement. For a statement recovered from pg_stat_statements, call explainQuery with its queryid first and pass the sql it echoes back. Reports a clear error with setup instructions if hypopg is not installed"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"sql", {{"type", "string"}}},
		    {"create", {{"type", "array"}, {"items", {{"type", "string"}}},
		                {"description", "CREATE INDEX statements to plan against"}}},
		    {"hide", {{"type", "array"}, {"items", {{"type", "string"}}},
		              {"description", "names of existing indexes to plan without"}}}
		  }},
		{"required", {"sql"}}
	      }}
	  },
	  {
	    {"name", "checkPrivileges"},
	    {"description", "report which tools the current role can actually use on this connection, and how the rest fall short. Most of this server works for any role that can connect, because the catalog is world-readable; what varies is the monitoring extras and whether the role can read table data. Call this first when working against an unfamiliar connection or a restricted role -- the alternative is discovering the limits tool by tool, and a privilege-filtered answer is easy to mistake for an empty one. Names no role memberships and no GRANT statements: what a caller needs is which tools work. Tools absent from both lists are fully available"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "tableStats"},
	    {"description", "return the statistics PostgreSQL keeps for one table: estimated row count, seq_scan and idx_scan counts, live and dead tuples, rows modified since the last analyze, rows inserted since the last vacuum, the last vacuum and analyze times, per-index scan counts, and the per-column pg_stats histograms (null_frac, avg_width, n_distinct, physical order correlation, most_common_vals and their frequencies). Reads the catalog and the statistics collector only -- no relation is opened and no file is measured. size_estimate is relpages*8192 and is only as fresh as estimated_from says: for a measured size call tableSize. Note that most_common_vals contains literal values sampled from the column. Not to be confused with tableIOStats, which reports pg_statio_all_tables -- whether reads came from the buffer cache or the disk"},
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
	    {"name", "listTableStats"},
	    {"description", "return the statistics PostgreSQL keeps for every table in a schema: estimated row count, seq_scan and idx_scan counts, live and dead tuples, rows modified since the last analyze, rows inserted since the last vacuum, and the last vacuum and analyze times. Reads the catalog and the statistics collector only -- no relation is opened and no file is measured. Carries no per-column histograms; name one table to tableStats for those. size_estimate is relpages*8192 and is only as fresh as estimated_from says: for measured sizes call listTableSizes"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
	      }}
	  },
	  {
	    {"name", "tableSize"},
	    {"description", "measure one table on disk: main fork, total table size including TOAST and the free space and visibility maps, index size, grand total, the TOAST relation and each index individually. COSTS MORE THAN IT LOOKS: these functions open the relation with AccessShareLock, so on a table an ALTER TABLE is rewriting the call waits behind AccessExclusiveLock until statement_timeout fires. Prefer size_estimate from tableStats, which is free, and call this when the estimate is too stale to act on. A partitioned table reports its own storage, which is zero -- measure the partitions"},
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
	    {"name", "listTableSizes"},
	    {"description", "measure every table in a schema on disk: table size, index size and grand total per relation. COSTS MORE THAN IT LOOKS, and more here than in tableSize: one relation is opened per table, each taking AccessShareLock, so a single table held under AccessExclusiveLock by an ALTER TABLE blocks the whole call rather than one row of it, and on a large schema this is thousands of file-metadata calls. Prefer size_estimate from listTableStats, which is free, and call this when the estimates are too stale to act on. Partitioned tables report their own storage, which is zero"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}}
		  }},
		{"required", {"schema"}}
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
	    {"description", "return current server connections and running queries (pg_stat_activity) across all databases: pid, database, user, application_name, backend_type, state, wait event, query text, transaction and query duration, leader_pid for parallel workers, and the backend's xid and xmin. query_id is returned as a decimal string and is the join key to statementStats and explainQuery, so a statement seen running here can be looked up and planned. All filters are optional and combine; with none the whole view is returned, which on a busy server is mostly idle connections and internal processes"},
	    {"inputSchema", {
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
	      }}
	  },
	  {
	    {"name", "currentLocks"},
	    {"description", "return current locks (pg_locks) joined with the holding backend's query and user, plus which pids are blocking each waiting lock; use to diagnose lock contention. Given a pid, returns that backend's locks together with every backend blocking it transitively, each tagged with chain_depth: 0 is the pid asked about, and the largest depth is the backend at the root of the pile-up, which is the one to look at first"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"pid", {
			{"type", "integer"},
			{"description", "restrict to this backend and its transitive blockers, resolved through pg_blocking_pids. Omit for every lock in the cluster"}
		      }}
		  }}
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
	    {"description", "return tracked queries from pg_stat_statements under 'statements', with calls, timing, row counts, buffer usage, temporary block I/O and WAL volume, alongside an 'info' block from pg_stat_statements_info whose dealloc counter says whether entries are being evicted -- if it is climbing, this is not the slowest queries in the cluster but the slowest of those that survived eviction. query_id is a decimal string, ready to pass to explainQuery. Returns a clear error with setup instructions if the extension is not installed"},
	    {"inputSchema", {
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
	    {"name", "progressStats"},
	    {"description", "return every long-running maintenance command currently reporting progress (pg_stat_progress_vacuum, _analyze, _create_index, _cluster, _copy, _basebackup), with the phase, the blocks or tuples done against the total, a completion percentage, and how long it has been running. Use it to decide whether a VACUUM will finish before wraparound, or whether a CREATE INDEX is stuck waiting on a locker. The PostgreSQL 17 rename of the vacuum dead-tuple columns is normalized, and 'dead_tuple_unit' says whether the server counts tuples or bytes"},
	    {"inputSchema", {
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
	      }}
	  },
	  {
	    {"name", "ioStats"},
	    {"description", "return cumulative I/O statistics under 'io', per backend type, object and context (pg_stat_io, PostgreSQL 16+): reads, writes, extends, hits, evictions, reuses, fsyncs and their timings, with a hit percentage. This is where backend-written buffers, vacuum's ring-buffer reuse, and bulk read/write I/O become visible separately from the aggregate counters in checkpointStats. Rows with no activity are omitted unless a filter was given. Given a pid, reports that one backend instead, via pg_stat_get_backend_io, plus its WAL volume under 'wal' -- a backend can be quiet in I/O and still be generating WAL heavily; that requires PostgreSQL 18, since pg_stat_io itself has no pid column. Returns a clear error on PostgreSQL 15 and older, where the view does not exist"},
	    {"inputSchema", {
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
	    {"description", "return per-object buffer cache hit ratios (pg_statio_all_tables): heap_blks_read versus heap_blks_hit, idx_blks_read versus idx_blks_hit, the TOAST and TOAST-index pairs, and a combined ratio, with relation size and scan counts. Naming a single table adds a per-index breakdown from pg_statio_all_indexes. Ratios are null, not zero, for an object that has seen no reads at all. Not to be confused with tableStats, which reports pg_stat_user_tables -- scans, tuples and vacuum state; this tool answers only whether those reads came from the buffer cache or the disk"},
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
	    {"description", "return physical storage bloat for a table (pgstattuple/pgstattuple_approx): table size, live/dead tuple counts and percentages, free space and percentage. Defaults to the cheap visibility-map-based approximation; set exact=true for a precise but I/O-heavy full table scan. More accurate than the ANALYZE-time estimates in listTableStats/tableStats. Returns a clear error with setup instructions if the pgstattuple extension is not installed"},
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
	    {"name", "indexBloat"},
	    {"description", "return physical statistics for one index, from whichever pgstattuple function matches its access method: pgstatindex for btree (tree level, leaf/internal/empty/deleted pages, average leaf density, leaf fragmentation), pgstatginindex for GIN (pending list pages and tuples, alongside the fastupdate setting and pending list limit that bound them), pgstathashindex for hash (bucket/overflow/bitmap/unused pages, live and dead items, free percent). The access method is resolved from the catalog, so the caller does not need to know it; gist, spgist and brin are reported as unsupported by name, since pgstattuple has no function for them. Metrics are deliberately NOT normalized across access methods -- 'access_method' says which set came back. Index size and idx_scan travel with the metrics, because a fragmented index nothing has scanned is a candidate for dropping rather than REINDEX. btree and hash read the whole index; GIN reads only the metapage and is always cheap"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"schema", {{"type", "string"}}},
		    {"index",  {{"type", "string"},
				{"description", "the index's own name, not the name of the table it is on"}}}
		  }},
		{"required", {"schema", "index"}}
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
	    {"name", "verifyTopology"},
	    {"description", "connect to every configured connection and report what each server actually is: its role (primary or replica, from pg_is_in_recovery(), observed now rather than configured), its system identifier, database, address, port and version -- then check the declared topology against them. A physical replica carries the same system identifier as its primary forever, so the identifier alone cannot separate the two axes: same identifier with the same host and port is one instance, same identifier on different hosts is a replication group. Reports declarations the servers contradict, connections that share an identifier but are not declared together (an undeclared replica is where 'is this index used?' quietly gets the wrong answer), a replication group with no primary, and split brain. Logical replication cannot be verified this way and is reported as such rather than as a mismatch. Connects once per configured connection, sequentially, with a short connect timeout; a connection that fails is reported and does not abort the rest"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "bufferCacheSummary"},
	    {"description", "return how much of shared_buffers is used, dirty and pinned, with the usage-count histogram from pg_buffercache. Cheap enough for a routine health sweep beside checkpointStats. tableIOStats counts only what shared_buffers served, so a miss there may still have come from the OS page cache at RAM speed; this is the only in-core view of that split. Read the histogram rather than a hit ratio: mass at usage_count 2-5 is a stable working set, everything at 0-1 with no unused buffers is clock-sweep churn, and those are the same ratio with opposite diagnoses. One sample is weak evidence -- two samples minutes apart are the method. The readings cover the whole instance, not this database alone. Requires the pg_buffercache extension at version 1.4 or later, and a role with pg_monitor"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "bufferCacheContents"},
	    {"description", "return which relations own shared_buffers, aggregated per relation and fork and ranked by buffers held: cached bytes, percent of that fork resident, percent of shared_buffers consumed, dirty buffers, average usagecount and pins. Never raw per-buffer rows. Answers which relation is driving checkpoint writeback (pair with checkpointStats), whether the visibility-map fork is resident enough for index-only scans to pay off, and -- on a multi-tenant instance -- which database's working set is displacing the others. Only buffers belonging to this database and the shared catalogs can be resolved to names; buffers held by other databases on the same instance are visible to PostgreSQL but deliberately not reported here. Cost is O(shared_buffers) and does not vary with the limit or with anything else asked: pg_buffercache materialises one row per buffer before any filter applies, so narrowing the question does not narrow the scan. Around 0.5s per 16GB of shared_buffers, and it is subject to the connection's statement_timeout like every other call. bufferCacheSummary reads the same memory through a function that returns one row and is roughly a hundred times cheaper, so prefer it for anything routine. Requires the pg_buffercache extension and a role with pg_monitor"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", {
		    {"limit", {{"type", "integer"}, {"description", "how many relation/fork rows to return, ranked by buffers held. Defaults to 20, capped at 200"}}}
		  }}
	      }}
	  },
	  {
	    {"name", "listTopology"},
	    {"description", "return the configured topology: which connections share an instance (one postmaster, so they share shared_buffers, WAL, autovacuum workers and disk), which belong to the same replication_group (a primary and its replicas, holding the same data on different servers), and which carry each operator group label. Reads the config file only and opens no database connection, so it is cheap to call before deciding how wide a sweep to run. An instance whose source is \"inferred\" was derived from an identical host and port rather than declared, and is a hint for grouping output, not evidence of shared memory. Roles are not here: primary or replica is observed per call, never configured -- use verifyTopology"},
	    {"inputSchema", {
		{"type", "object"},
		{"properties", json::object()}
	      }}
	  },
	  {
	    {"name", "listConnections"},
	    {"description", "return the configured database connections by name, with the libpq service name or host/port/dbname/user for each, its instance, replication_group and group labels where configured, and which is the default; passwords are never returned and a service file is never expanded. Pass a name as the 'connection' argument of any other tool to run that tool against that database. See listTopology for the same labels indexed the other way round, by topology name rather than by connection"},
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
    // `annotations` arrived in MCP revision 2025-03-26 and tool `title` in
    // 2025-06-18. Emitting them to a client that negotiated 2024-11-05 would
    // change bytes that release 3.1.1 promised, so both are gated on the
    // version the client actually asked for rather than sent unconditionally.
    // Revision strings are ISO dates, so comparing them as strings is ordering
    // them by date.
    const bool wants_annotations = protocol >= "2025-03-26";
    const bool wants_title       = protocol >= "2025-06-18";
    // Same revision that defined structuredContent, and gated together on
    // purpose: outputSchema describes the structured payload, so a client that
    // will only ever be sent a text block has no use for it.
    const bool wants_output_schema = protocol >= kStructuredContentRevision;

    for (auto& tool : list["tools"]) {
      const std::string name = tool["name"].get<std::string>();
      auto it = tool_scopes().find(name);
      // A tool with no entry is a bug, not a default: the fan-out rules read
      // this table, and a missing row would quietly make a tool ineligible.
      // A test asserts the table covers every tool.
      const ToolScope sc = it == tool_scopes().end() ? ToolScope{} : it->second;

      if (!sc.registry) {
        tool["inputSchema"]["properties"]["connection"] = conn_prop;

        // The sweep arguments are advertised only where they are eligible, so
        // the schema itself teaches the rule and the -32602 is only a backstop.
        //
        // These are this server's own surface rather than a protocol-revision
        // feature, so unlike annotations they are not gated on the negotiated
        // version: a new optional input property cannot break a caller, and
        // gating it would hide the feature from exactly the clients that exist.
        if (tool_name_is_sweepable(name)) {
          if (sc.per_database)
            tool["inputSchema"]["properties"]["instance"] = json{
              {"type", "string"},
              {"description", "run against every database of this instance (see "
                              "listTopology) and return one result per member"}};
          if (sc.per_server)
            tool["inputSchema"]["properties"]["replication_group"] = json{
              {"type", "string"},
              {"description", "run against every member of this replication group "
                              "and return one result per member. The counters here "
                              "are each server's own, so the answer is their sum"}};
          tool["inputSchema"]["properties"]["group"] = json{
            {"type", "string"},
            {"description", "run against every connection carrying this group "
                            "label. Members that would answer identically for "
                            "this tool are collapsed and reported under "
                            "'skipped'"}};
          if (sc.per_server)
            tool["inputSchema"]["properties"]["role"] = json{
              {"type", "string"},
              {"description", "with replication_group or group, sweep only "
                              "members whose observed role is \"primary\" or "
                              "\"replica\". Observed per call, never configured"}};
        }
      }

      const std::string note = scope_note(name, sc);
      if (!note.empty())
        tool["description"] = tool["description"].get<std::string>() + note;

      if (wants_annotations) {
        // Every tool runs inside SET TRANSACTION READ ONLY, which is what makes
        // the claim honest rather than aspirational.
        json ann = {
          {"readOnlyHint", true},
          {"destructiveHint", false},
          {"openWorldHint", false}
        };
        // explainQuery is the carve-out. With analyze:true it really executes
        // the statement -- the plan is proven free of any ModifyTable node
        // first, so it still cannot mutate, but running it twice is not the
        // same as running it once.
        if (name == "explainQuery") ann["idempotentHint"] = false;
        tool["annotations"] = ann;
      }
      if (wants_title) tool["title"] = tool_title(name);
      if (wants_output_schema) {
        auto os = tool_output_schemas().find(name);
        // A tool with no schema is a bug, not a default: declaring none for one
        // tool while declaring them for the other 55 reads to a client as "this
        // one is unstructured". A test asserts the table covers every tool.
        if (os != tool_output_schemas().end()) tool["outputSchema"] = os->second;
      }
    }
    return list;
  }

  // explainQuery aside, any tool that touches a database can be swept.
  static bool tool_name_is_sweepable(const std::string& name) {
    return name != "explainQuery" && name != "evaluateIndex";
  }

  // A human-readable label, derived from the tool name rather than stored
  // twice: "bufferCacheSummary" -> "Buffer cache summary". Keeping it derived
  // means a renamed tool cannot end up with a stale title.
  static std::string tool_title(const std::string& name) {
    std::string out;
    for (size_t i = 0; i < name.size(); i++) {
      const unsigned char c = static_cast<unsigned char>(name[i]);
      if (i == 0) { out += static_cast<char>(std::toupper(c)); continue; }
      if (std::isupper(c) && !std::isupper(static_cast<unsigned char>(name[i - 1]))) {
        out += ' ';
        out += static_cast<char>(std::tolower(c));
      } else {
        out += name[i];
      }
    }
    return out;
  }

  const json schemas() {
    Session sess = open_session();
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

  // Structure only. Every reading this used to carry -- reltuples, the
  // relpages size estimate and the whole pg_stat_user_tables row -- moved to
  // listTableStats in 4.0.0, and the measured sizes to listTableSizes. What is
  // left changes only when someone issues DDL, which is what lets the tool be
  // classified per_database and refuse a replication-group sweep: a physical
  // replica would return these bytes verbatim.
  const json tables(const std::string& schema) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(c.relname,
              JSONB_BUILD_OBJECT(
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'reloptions', c.reloptions,
               'columns', columns, 'index_count', COALESCE(index_count, 0), 'constraint_count', COALESCE(constraint_count, 0)))
      FROM pg_class AS c
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

  // Structure only, for the same reason as tables() above. searchTables is the
  // one tool of the three with no statistics counterpart: text search is a
  // discovery path, and nobody runs a full-text query to read a counter. A
  // caller who wants readings on a match names it to tableStats.
  const json search(const std::string& web_search) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(c.relnamespace::regnamespace::name || '.' || c.relname,
              JSONB_BUILD_OBJECT(
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'reloptions', c.reloptions,
               'columns', columns, 'index_count', COALESCE(index_count, 0), 'constraint_count', COALESCE(constraint_count, 0),
               'roles', COALESCE(roles, '{}'::jsonb)))
      FROM pg_class AS c
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
    Session sess = open_session();
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
    Session sess = open_session();
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

    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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

  // All four filters are optional and independent; omitting every one returns
  // the whole of pg_stat_activity, as before. They matter because the
  // unfiltered view on a busy server is mostly idle connections and internal
  // processes, and the entry the caller wants is one row in several hundred.
  const json activity(int pid, const std::string& query_id,
                      double min_duration_s, const std::string& state) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // pg_wait_events (PostgreSQL 17+) carries a prose description of every
    // wait event, which is what turns an opaque name like "BufFileRead" into
    // something actionable without leaving the tool output.
    const std::string wait_desc = sess.server_version() >= 170000
      ? ", 'wait_event_description', we.description" : "";
    const std::string wait_join = sess.server_version() >= 170000
      ? R"(LEFT JOIN pg_wait_events AS we
             ON we.type = a.wait_event_type AND we.name = a.wait_event)"
      : "";

    // query_id is the join key to pg_stat_statements and explainQuery: without
    // it there is no route from "this is running now" to "this is its plan".
    // It is emitted as text because it is a 64-bit value that does not survive
    // JSON number precision, matching explainQuery's queryid argument.
    //
    // It is null unless compute_query_id is on (the default, 'auto', enables it
    // when pg_stat_statements is loaded); serverSettings reports that GUC.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(a.pid::text,
               JSONB_BUILD_OBJECT(
                 'database',         a.datname,
                 'user',             a.usename,
                 'application_name', a.application_name,
                 'client_addr',      a.client_addr::text,
                 'backend_type',     a.backend_type,
                 'state',            a.state,
                 'wait_event_type',  a.wait_event_type,
                 'wait_event',       a.wait_event,
                 'backend_start',    a.backend_start,
                 'xact_start',       a.xact_start,
                 'query_start',      a.query_start,
                 'state_change',     a.state_change,
                 'xact_duration_s',  round(EXTRACT(EPOCH FROM now() - a.xact_start)::numeric, 3),
                 'query_duration_s', round(EXTRACT(EPOCH FROM now() - a.query_start)::numeric, 3),
                 'query_id',         a.query_id::text,
                 'leader_pid',       a.leader_pid,
                 'backend_xid',      a.backend_xid::text,
                 'backend_xmin',     a.backend_xmin::text,
                 'query',            a.query
               )" + wait_desc + R"(
               ))
      FROM pg_stat_activity AS a
      )" + wait_join + R"(
      WHERE a.pid != pg_backend_pid()
        -- A pid brings its parallel workers with it. Asking about a leader and
        -- being shown only the leader hides where the work is actually
        -- happening, which is the reason for asking in the first place.
        AND ($1 = '' OR a.pid = $1::int OR a.leader_pid = $1::int)
        AND ($2 = '' OR a.query_id = $2::bigint)
        -- Duration is measured on the current query, and plain idle backends
        -- are excluded: an idle connection's query_start dates its last
        -- statement, so including them would report every long-idle session as
        -- a long-running query.
        AND ($3 = '' OR (a.state IS DISTINCT FROM 'idle'
                         AND a.query_start IS NOT NULL
                         AND now() - a.query_start
                             >= make_interval(secs => $3::double precision)))
        AND ($4 = '' OR a.state = $4);
    )";

    pqxx::result res = pqxx_exec(
      txn, query,
      pqxx::params{pid > 0 ? std::to_string(pid) : std::string{},
                   query_id,
                   min_duration_s > 0 ? std::to_string(min_duration_s) : std::string{},
                   state});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      // An empty object, not null: with filters applied, "no backend matched"
      // is a normal answer and should read as an empty set rather than as a
      // missing result.
      return json::object();
    }
  }

  // With a pid, this returns that backend's locks together with every backend
  // blocking it, transitively. A flat list of locks is the wrong shape during
  // a pile-up: what you need is the pid at the root of the chain, and
  // reconstructing that graph by hand from the full dump is exactly the work
  // the tool should be doing.
  const json locks(int pid) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // chain_depth is 0 for the pid asked about, 1 for what blocks it, and so
    // on; the largest depth is the backend to look at first. The path array is
    // a cycle guard -- a deadlock is a cycle in this graph, and without it the
    // recursion would not terminate.
    const std::string chain_cte = pid > 0 ? R"(
      WITH RECURSIVE chain(pid, depth, path) AS (
          SELECT $1::int, 0, ARRAY[$1::int]
        UNION ALL
          SELECT b.pid, c.depth + 1, c.path || b.pid
          FROM chain AS c,
               LATERAL unnest(pg_blocking_pids(c.pid)) AS b(pid)
          WHERE NOT b.pid = ANY(c.path)
            AND c.depth < 16
      ),
      chain_min AS (
          SELECT pid, MIN(depth) AS depth FROM chain GROUP BY pid
      )
    )" : "";

    const std::string chain_field = pid > 0 ? ", 'chain_depth', ch.depth" : "";
    const std::string chain_join  = pid > 0
      ? "JOIN chain_min AS ch ON ch.pid = l.pid" : "";
    const std::string order_by = pid > 0
      ? "ORDER BY ch.depth, l.granted ASC, l.pid"
      : "ORDER BY l.granted ASC, l.pid";

    std::string query = chain_cte + R"(
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
               )" + chain_field + R"(
               ) )" + order_by + R"(
             )
      FROM pg_locks AS l
      )" + chain_join + R"(
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

    pqxx::result res = pid > 0
      ? pqxx_exec(txn, query, pqxx::params{pid})
      : txn.exec(query);

    // 4.0.0 wraps the rows in an object. `structuredContent` may only be a
    // JSON object, so a top-level array is unrepresentable in the format
    // modern clients receive -- the same reason, and the same fix, that 3.0.0
    // applied to statementStats.
    if (!res.empty() && !res[0][0].is_null()) {
      return json{{"locks", json::parse(res[0][0].as<std::string>())}};
    } else {
      return json{{"locks", json::array()}};
    }
  }

  const json replication_slots() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // retained_wal_bytes is the key diagnostic: a lagging or unused slot holds
    // back WAL cleanup indefinitely, a common cause of disk bloat incidents.
    //
    // wal_status is the verdict retained_wal_bytes only hints at: 'extended'
    // means the slot is already past max_wal_size, and 'lost' means the WAL it
    // needs is gone and the slot is unusable. safe_wal_size is how much more
    // WAL can be written before that happens, and goes negative once it has.
    //
    // The spill and stream counters come from pg_stat_replication_slots
    // (PostgreSQL 14+), a different view from pg_replication_slots: they show
    // logical decoding spilling large transactions to disk, which is invisible
    // in the slot's own row and is a common, silent throughput cliff.
    const std::string conflicting = sess.server_version() >= 160000
      ? ", 'conflicting', s.conflicting" : "";
    const std::string invalidation = sess.server_version() >= 170000
      ? ", 'invalidation_reason', s.invalidation_reason, 'inactive_since', s.inactive_since"
      : "";

    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(s.slot_name,
               JSONB_BUILD_OBJECT(
                 'plugin',              s.plugin,
                 'slot_type',           s.slot_type,
                 'database',            s.database,
                 'temporary',           s.temporary,
                 'two_phase',           s.two_phase,
                 'active',              s.active,
                 'active_pid',          s.active_pid,
                 'restart_lsn',         s.restart_lsn::text,
                 'confirmed_flush_lsn', s.confirmed_flush_lsn::text,
                 'wal_status',          s.wal_status,
                 'safe_wal_size',       s.safe_wal_size,
                 'retained_wal_bytes',  CASE WHEN s.restart_lsn IS NOT NULL
                                           THEN pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn) END,
                 'spill_txns',          st.spill_txns,
                 'spill_count',         st.spill_count,
                 'spill_bytes',         st.spill_bytes,
                 'stream_txns',         st.stream_txns,
                 'stream_count',        st.stream_count,
                 'stream_bytes',        st.stream_bytes,
                 'total_txns',          st.total_txns,
                 'total_bytes',         st.total_bytes,
                 'stats_reset',         st.stats_reset
               )" + conflicting + invalidation + R"(
               ))
      FROM pg_replication_slots AS s
      LEFT JOIN pg_stat_replication_slots AS st ON st.slot_name = s.slot_name;
    )";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json database_stats() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // The session counters (PostgreSQL 14+) are what distinguish a database
    // that is busy from one that is merely holding transactions open:
    // idle_in_transaction_time and sessions_abandoned name the two failure
    // modes that the commit/rollback counts alone cannot tell apart.
    //
    // parallel_workers_launched falling short of parallel_workers_to_launch
    // (PostgreSQL 18+) means queries planned for parallelism ran without it,
    // because max_parallel_workers was exhausted.
    const std::string parallel = sess.server_version() >= 180000
      ? R"(, 'parallel_workers_to_launch', parallel_workers_to_launch,
            'parallel_workers_launched',  parallel_workers_launched)"
      : "";

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
                 'session_time',              session_time,
                 'active_time',               active_time,
                 'idle_in_transaction_time',  idle_in_transaction_time,
                 'sessions',                  sessions,
                 'sessions_abandoned',        sessions_abandoned,
                 'sessions_fatal',            sessions_fatal,
                 'sessions_killed',           sessions_killed,
                 'stats_reset',       stats_reset
               )" + parallel + R"(
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

  const json statement_stats(int limit, const std::string& query_id,
                             const std::string& order_by, long long min_calls) {
    // The sort column cannot be a bind parameter, so it is resolved through a
    // fixed table rather than interpolated. Nothing caller-supplied reaches the
    // SQL text: an unknown name is rejected here, listing what is accepted.
    static const std::map<std::string, std::string> ORDERINGS = {
      {"total_exec_time",   "pss.total_exec_time DESC"},
      {"mean_exec_time",    "pss.mean_exec_time DESC"},
      {"max_exec_time",     "pss.max_exec_time DESC"},
      {"calls",             "pss.calls DESC"},
      {"rows",              "pss.rows DESC"},
      {"shared_blks_read",  "pss.shared_blks_read DESC"},
      {"temp_blks_written", "pss.temp_blks_written DESC"},
      {"wal_bytes",         "pss.wal_bytes DESC"},
    };
    std::string key = order_by.empty() ? "total_exec_time" : order_by;
    auto ord = ORDERINGS.find(key);
    if (ord == ORDERINGS.end()) {
      std::string valid;
      for (const auto& [k, v] : ORDERINGS) { (void)v; valid += (valid.empty() ? "" : ", ") + k; }
      throw std::runtime_error("unknown order_by \"" + order_by +
                               "\"; valid values are: " + valid);
    }

    if (!query_id.empty()) {
      bool ok = query_id.size() <= 20;
      for (size_t i = 0; ok && i < query_id.size(); i++) {
        if (i == 0 && query_id[i] == '-') { ok = query_id.size() > 1; continue; }
        if (!std::isdigit(static_cast<unsigned char>(query_id[i]))) ok = false;
      }
      if (!ok) throw std::runtime_error("query_id must be a decimal integer string");
    }

    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // Qualified with the schema the extension was installed into; see
    // extension_schema. An empty answer is "not installed", which is the same
    // outcome the 42P01 catch below reports -- it is checked up front so the
    // caller gets that answer whatever their search_path looks like.
    const std::string pgss = extension_schema(txn, "pg_stat_statements");
    if (pgss.empty()) return pgss_missing();

    // pg_stat_statements_info.dealloc counts how many times the extension has
    // evicted its least-used entries because pg_stat_statements.max was
    // exceeded. A climbing dealloc means this list is not the slowest queries
    // in the cluster but the slowest of those that survived eviction -- a
    // difference that cannot be inferred from the rows themselves, which is
    // why it is returned alongside them.
    const std::string since = sess.server_version() >= 170000
      ? ", 'stats_since', pss.stats_since, 'minmax_stats_since', pss.minmax_stats_since"
      : "";
    const std::string pg18 = sess.server_version() >= 180000
      ? R"(, 'wal_buffers_full', pss.wal_buffers_full,
            'parallel_workers_to_launch', pss.parallel_workers_to_launch,
            'parallel_workers_launched', pss.parallel_workers_launched)"
      : "";

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
        'statements', COALESCE((
          SELECT JSONB_AGG(row_json)
          FROM (
              SELECT JSONB_BUILD_OBJECT(
                       -- As text, not a number: a queryid is 64-bit and does
                       -- not survive JSON number precision in a client that
                       -- parses numbers as doubles. explainQuery already takes
                       -- it as a string for that reason, and a value that
                       -- silently changed between the two tools would be
                       -- looked up and not found.
                       'query_id',         pss.queryid::text,
                       -- Truncated in a list, whole when one statement was
                       -- asked for by id: the point of naming a queryid is to
                       -- get at the statement, and truncation is what makes
                       -- the list readable, not something the caller wants.
                       'query',            CASE WHEN $2 = '' THEN LEFT(pss.query, 500)
                                                ELSE pss.query END,
                       'calls',            pss.calls,
                       'total_exec_ms',    pss.total_exec_time,
                       'mean_exec_ms',     pss.mean_exec_time,
                       'min_exec_ms',      pss.min_exec_time,
                       'max_exec_ms',      pss.max_exec_time,
                       'rows',             pss.rows,
                       'shared_blks_hit',  pss.shared_blks_hit,
                       'shared_blks_read', pss.shared_blks_read,
                       'temp_blks_read',   pss.temp_blks_read,
                       'temp_blks_written', pss.temp_blks_written,
                       'wal_records',      pss.wal_records,
                       'wal_fpi',          pss.wal_fpi,
                       'wal_bytes',        pss.wal_bytes,
                       'user',             r.rolname,
                       'database',         d.datname
                     )" + since + pg18 + R"(
                     ) AS row_json
              FROM )" + pgss + R"(.pg_stat_statements AS pss
              LEFT JOIN pg_roles AS r ON r.oid = pss.userid
              LEFT JOIN pg_database AS d ON d.oid = pss.dbid
              WHERE ($2 = '' OR pss.queryid = $2::bigint)
                AND ($3 = '' OR pss.calls >= $3::bigint)
              ORDER BY )" + ord->second + R"(
              LIMIT $1::bigint
          ) sub), '[]'::jsonb),
        'info', (SELECT JSONB_BUILD_OBJECT(
                   'dealloc', i.dealloc, 'stats_reset', i.stats_reset)
                 FROM )" + pgss + R"(.pg_stat_statements_info AS i),
        -- missing_ok, so a server where the GUC is absent yields null rather
        -- than an error that would mask the real result.
        'max', current_setting('pg_stat_statements.max', true)
      );
    )";

    try {
      pqxx::result res = pqxx_exec(
        txn, query,
        pqxx::params{std::to_string(limit), query_id,
                     min_calls > 0 ? std::to_string(min_calls) : std::string{}});

      if (!res.empty() && !res[0][0].is_null()) {
        json out = json::parse(res[0][0].as<std::string>());
        out["order_by"] = key;
        return out;
      } else {
        return {{"statements", json::array()}, {"order_by", key}};
      }
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42P01") { // undefined_table
        // The view was there when its schema was resolved, so it has since
        // moved or been dropped; forget the location and report it missing.
        forget_extension_schema("pg_stat_statements");
        return pgss_missing();
      }
      throw;
    }
  }

  const json progress_stats(int pid, const std::string& relation) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // The relation is matched by name against pg_class rather than by casting
    // the argument to regclass: a regclass cast of a name that does not exist
    // raises an error, and "no such table" should come back as an empty result
    // from a diagnostic tool, not as a failure. Both the bare and the
    // schema-qualified form are accepted.
    // pc/pn rather than c/n: the COPY subquery already binds c, and an alias
    // collision here would silently resolve to the wrong relation.
    // Deliberately not a raw string. This fragment ends in two parentheses,
    // and a raw string's terminator is )delimiter" -- so both R"(...))" and
    // R"SQL(...))SQL" swallow one of them and produce SQL that fails to parse
    // only when the filter is actually used. Plain literals have no such trap.
    auto rel_filter = [](const std::string& alias) {
      return " AND ($2 = '' OR " + alias + ".relid IN ("
             " SELECT pc.oid FROM pg_class AS pc"
             " JOIN pg_namespace AS pn ON pn.oid = pc.relnamespace"
             " WHERE pc.relname = $2 OR pn.nspname || '.' || pc.relname = $2))";
    };
    const std::string pid_v  = " AND ($1 = '' OR v.pid = $1::int)";
    const std::string pid_an = " AND ($1 = '' OR an.pid = $1::int)";
    const std::string pid_ci = " AND ($1 = '' OR ci.pid = $1::int)";
    const std::string pid_cl = " AND ($1 = '' OR cl.pid = $1::int)";
    const std::string pid_c  = " AND ($1 = '' OR c.pid = $1::int)";
    // A base backup has no relation at all, so a relation filter excludes it
    // entirely rather than matching everything.
    const std::string bb_filter = " AND ($1 = '' OR b.pid = $1::int) AND $2 = ''";

    // PostgreSQL 17 renamed three pg_stat_progress_vacuum columns:
    // max_dead_tuples -> max_dead_tuple_bytes, num_dead_tuples ->
    // num_dead_item_ids, and added dead_tuple_bytes. The old and new names
    // measure different things (tuple counts against bytes), so they are
    // reported under their own names rather than pretended to be one field,
    // with 'dead_tuple_unit' saying which the server produced.
    const bool v17 = sess.server_version() >= 170000;
    const std::string vacuum_dead = v17
      ? R"('dead_tuple_unit',     'bytes',
           'max_dead_tuple_bytes', v.max_dead_tuple_bytes,
           'dead_tuple_bytes',     v.dead_tuple_bytes,
           'num_dead_item_ids',    v.num_dead_item_ids,
           'indexes_total',        v.indexes_total,
           'indexes_processed',    v.indexes_processed)"
      : R"('dead_tuple_unit',  'tuples',
           'max_dead_tuples',  v.max_dead_tuples,
           'num_dead_tuples',  v.num_dead_tuples)";

    // Percentages are the point of a progress view: "1.2 million of 4 million
    // blocks" is only useful once it is 30%.
    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
        'vacuum', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'pid',               v.pid,
                   'database',          v.datname,
                   'relation',          v.relid::regclass::text,
                   'phase',             v.phase,
                   'heap_blks_total',   v.heap_blks_total,
                   'heap_blks_scanned', v.heap_blks_scanned,
                   'heap_blks_vacuumed', v.heap_blks_vacuumed,
                   'scanned_percent',
                     round(100.0 * v.heap_blks_scanned / NULLIF(v.heap_blks_total, 0), 1),
                   'vacuumed_percent',
                     round(100.0 * v.heap_blks_vacuumed / NULLIF(v.heap_blks_total, 0), 1),
                   'index_vacuum_count', v.index_vacuum_count,
                   'query',             a.query,
                   'started',           a.query_start,
                   'elapsed_s',         round(EXTRACT(EPOCH FROM now() - a.query_start)::numeric, 1),
                   )" + vacuum_dead + R"())
          FROM pg_stat_progress_vacuum AS v
          LEFT JOIN pg_stat_activity AS a ON a.pid = v.pid
          WHERE true)" + pid_v + rel_filter("v") + R"(), '[]'::jsonb),
        'analyze', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'pid',                   an.pid,
                   'database',              an.datname,
                   'relation',              an.relid::regclass::text,
                   'phase',                 an.phase,
                   'sample_blks_total',     an.sample_blks_total,
                   'sample_blks_scanned',   an.sample_blks_scanned,
                   'scanned_percent',
                     round(100.0 * an.sample_blks_scanned / NULLIF(an.sample_blks_total, 0), 1),
                   'ext_stats_total',       an.ext_stats_total,
                   'ext_stats_computed',    an.ext_stats_computed,
                   'child_tables_total',    an.child_tables_total,
                   'child_tables_done',     an.child_tables_done,
                   'elapsed_s',             round(EXTRACT(EPOCH FROM now() - a.query_start)::numeric, 1)))
          FROM pg_stat_progress_analyze AS an
          LEFT JOIN pg_stat_activity AS a ON a.pid = an.pid
          WHERE true)" + pid_an + rel_filter("an") + R"(), '[]'::jsonb),
        'create_index', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'pid',              ci.pid,
                   'database',         ci.datname,
                   'relation',         ci.relid::regclass::text,
                   'index',            NULLIF(ci.index_relid, 0)::regclass::text,
                   'command',          ci.command,
                   'phase',            ci.phase,
                   'blocks_total',     ci.blocks_total,
                   'blocks_done',      ci.blocks_done,
                   'blocks_percent',
                     round(100.0 * ci.blocks_done / NULLIF(ci.blocks_total, 0), 1),
                   'tuples_total',     ci.tuples_total,
                   'tuples_done',      ci.tuples_done,
                   'lockers_total',    ci.lockers_total,
                   'lockers_done',     ci.lockers_done,
                   'current_locker_pid', ci.current_locker_pid,
                   'elapsed_s',        round(EXTRACT(EPOCH FROM now() - a.query_start)::numeric, 1)))
          FROM pg_stat_progress_create_index AS ci
          LEFT JOIN pg_stat_activity AS a ON a.pid = ci.pid
          WHERE true)" + pid_ci + rel_filter("ci") + R"(), '[]'::jsonb),
        'cluster', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'pid',                cl.pid,
                   'database',           cl.datname,
                   'relation',           cl.relid::regclass::text,
                   'command',            cl.command,
                   'phase',              cl.phase,
                   'heap_tuples_scanned', cl.heap_tuples_scanned,
                   'heap_tuples_written', cl.heap_tuples_written,
                   'heap_blks_total',    cl.heap_blks_total,
                   'heap_blks_scanned',  cl.heap_blks_scanned,
                   'scanned_percent',
                     round(100.0 * cl.heap_blks_scanned / NULLIF(cl.heap_blks_total, 0), 1),
                   'index_rebuild_count', cl.index_rebuild_count,
                   'elapsed_s',          round(EXTRACT(EPOCH FROM now() - a.query_start)::numeric, 1)))
          FROM pg_stat_progress_cluster AS cl
          LEFT JOIN pg_stat_activity AS a ON a.pid = cl.pid
          WHERE true)" + pid_cl + rel_filter("cl") + R"(), '[]'::jsonb),
        'copy', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'pid',            c.pid,
                   'database',       c.datname,
                   'relation',       NULLIF(c.relid, 0)::regclass::text,
                   'command',        c.command,
                   'type',           c.type,
                   'bytes_processed', c.bytes_processed,
                   'bytes_total',    c.bytes_total,
                   'bytes_percent',
                     round(100.0 * c.bytes_processed / NULLIF(c.bytes_total, 0), 1),
                   'tuples_processed', c.tuples_processed,
                   'tuples_excluded',  c.tuples_excluded,
                   'elapsed_s',      round(EXTRACT(EPOCH FROM now() - a.query_start)::numeric, 1)))
          FROM pg_stat_progress_copy AS c
          LEFT JOIN pg_stat_activity AS a ON a.pid = c.pid
          WHERE true)" + pid_c + rel_filter("c") + R"(), '[]'::jsonb),
        'basebackup', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'pid',                b.pid,
                   'phase',              b.phase,
                   'backup_total',       b.backup_total,
                   'backup_streamed',    b.backup_streamed,
                   'streamed_percent',
                     round(100.0 * b.backup_streamed / NULLIF(b.backup_total, 0), 1),
                   'tablespaces_total',  b.tablespaces_total,
                   'tablespaces_streamed', b.tablespaces_streamed))
          FROM pg_stat_progress_basebackup AS b
          WHERE true)" + bb_filter + R"(), '[]'::jsonb)
      );
    )";

    pqxx::result res = pqxx_exec(
      txn, query,
      pqxx::params{pid > 0 ? std::to_string(pid) : std::string{}, relation});

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  const json io_stats(int pid, const std::string& backend_type,
                      const std::string& object, const std::string& context) {
    Session sess = open_session();

    // pg_stat_io arrived in PostgreSQL 16. Saying so plainly beats an
    // undefined-table error, and matches how a missing extension is reported.
    if (sess.server_version() < 160000) {
      return {
        {"error", "pg_stat_io requires PostgreSQL 16 or newer"},
        {"hint", "this server is older; use checkpointStats for the "
                 "pg_stat_bgwriter counters, which include buffers_backend "
                 "and buffers_backend_fsync on these versions, and "
                 "tableIOStats for per-object cache hit ratios"}
      };
    }
    // Per-backend I/O is not a filter over pg_stat_io -- the view has no pid
    // column at all. It is a separate function, added in PostgreSQL 18.
    if (pid > 0 && sess.server_version() < 180000) {
      return {
        {"error", "per-backend I/O statistics require PostgreSQL 18 or newer"},
        {"hint", "pg_stat_io is aggregated across backends and has no pid "
                 "column; pg_stat_get_backend_io(), which reports one "
                 "backend, was added in 18. Omit pid for the cluster-wide "
                 "view, or use currentActivity to see what the backend is "
                 "doing"}
      };
    }

    pqxx::work& txn = sess.txn();

    // Byte counters replaced the single op_bytes column in PostgreSQL 18;
    // before that, a block count times op_bytes was the only way to get bytes,
    // and op_bytes itself is gone in 18. Reporting the byte columns only where
    // they exist avoids inventing a number on older servers.
    const std::string bytes = sess.server_version() >= 180000
      ? R"(, 'read_bytes', read_bytes, 'write_bytes', write_bytes,
            'extend_bytes', extend_bytes)"
      : "";

    // One backend, or the whole cluster: the row shape is identical either
    // way, so only the source relation changes.
    //
    // Both branches reference $1, which keeps one parameter list across them.
    // PostgreSQL rejects a bind that supplies more parameters than the
    // statement uses, so the cluster-wide branch cannot simply leave it out;
    // the predicate it carries is the invariant that this branch is the one
    // taken when no pid was given.
    const std::string source = pid > 0
      ? "pg_stat_get_backend_io($1::int)" : "pg_stat_io";
    const std::string pid_guard = pid > 0 ? "" : " AND $1 = ''";

    // Rows where nothing has happened at all are dropped: pg_stat_io is a
    // dense matrix of backend_type x object x context, and most combinations
    // are structurally impossible, so an unfiltered dump is mostly nulls.
    // With an explicit filter the rows are kept regardless, since "this
    // context did nothing" is then the answer to the question asked.
    const std::string activity_filter =
      (backend_type.empty() && object.empty() && context.empty())
        ? R"(AND (COALESCE(reads, 0) > 0 OR COALESCE(writes, 0) > 0
                  OR COALESCE(extends, 0) > 0 OR COALESCE(hits, 0) > 0
                  OR COALESCE(evictions, 0) > 0 OR COALESCE(fsyncs, 0) > 0))"
        : "";

    std::string query = R"(
      SELECT COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT(
               'backend_type', backend_type,
               'object',       object,
               'context',      context,
               'reads',        reads,
               'read_time_ms', read_time,
               'writes',       writes,
               'write_time_ms', write_time,
               'writebacks',   writebacks,
               'writeback_time_ms', writeback_time,
               'extends',      extends,
               'extend_time_ms', extend_time,
               'hits',         hits,
               'evictions',    evictions,
               'reuses',       reuses,
               'fsyncs',       fsyncs,
               'fsync_time_ms', fsync_time,
               'hit_percent',  round(100.0 * hits / NULLIF(hits + reads, 0), 2),
               'stats_reset',  stats_reset
             )" + bytes + R"(
             ) ORDER BY backend_type, object, context), '[]'::jsonb)
      FROM )" + source + R"(
      WHERE ($2 = '' OR backend_type = $2)
        AND ($3 = '' OR object = $3)
        AND ($4 = '' OR context = $4)
      )" + pid_guard + activity_filter + ";";

    pqxx::result res = pqxx_exec(
      txn, query,
      pqxx::params{pid > 0 ? std::to_string(pid) : std::string{},
                   backend_type, object, context});

    json out = json::object();
    if (pid > 0) out["pid"] = pid;
    out["io"] = (!res.empty() && !res[0][0].is_null())
      ? json::parse(res[0][0].as<std::string>()) : json::array();

    // WAL is a separate per-backend function, and is the other half of "what
    // is this backend doing to the disk": a backend can be quiet in pg_stat_io
    // and still be generating WAL heavily.
    if (pid > 0) {
      pqxx::result w = pqxx_exec(
        txn,
        R"(SELECT JSONB_BUILD_OBJECT(
             'wal_records',      wal_records,
             'wal_fpi',          wal_fpi,
             'wal_bytes',        wal_bytes,
             'wal_buffers_full', wal_buffers_full,
             'stats_reset',      stats_reset)
           FROM pg_stat_get_backend_wal($1::int))",
        pqxx::params{std::to_string(pid)});
      if (!w.empty() && !w[0][0].is_null())
        out["wal"] = json::parse(w[0][0].as<std::string>());
    }
    return out;
  }

  const json wraparound_status(const std::string& schema, int limit) {
    Session sess = open_session();
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
    //
    // relallfrozen (PostgreSQL 18+) turns an age into an estimate of the work
    // left to do: an old relfrozenxid on a table that is already 99% frozen is
    // a very different problem from the same age with nothing frozen.
    // total_autovacuum_time answers the next question -- whether autovacuum
    // has been trying and failing to keep up, or has simply never run.
    const std::string pg18_cols = sess.server_version() >= 180000
      ? R"(, 'frozen_percent',
              round(100.0 * r.relallfrozen / NULLIF(r.relpages, 0), 1),
            'relallfrozen', r.relallfrozen,
            'total_vacuum_time_ms', r.total_vacuum_time,
            'total_autovacuum_time_ms', r.total_autovacuum_time)"
      : "";
    const std::string pg18_sel = sess.server_version() >= 180000
      ? R"(, c.relallfrozen, c.relpages,
            s.total_vacuum_time, s.total_autovacuum_time)"
      : "";

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
                   'n_dead_tup', r.n_dead_tup)" + pg18_cols + R"(
                   ) ORDER BY r.xid_age DESC)
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
                   s.last_vacuum, s.last_autovacuum, s.n_dead_tup)" + pg18_sel + R"(
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
    Session sess = open_session();
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
    Session sess = open_session();
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

    // num_done and slru_written are PostgreSQL 18 additions to
    // pg_stat_checkpointer; the rest of the view is unchanged since 17.
    const std::string ckpt_pg18 = sess.server_version() >= 180000
      ? "'checkpoints_done', c.num_done, 'slru_written', c.slru_written,"
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
             )" + ckpt_pg18 + R"(
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
    Session sess = open_session();
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

    Session sess = open_session();
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

    Session sess = open_session(tmo);
    pqxx::work& txn = sess.txn();

    // --- resolve the statement text ---
    std::string sql = sql_in;
    json stats = nullptr;

    if (!queryid.empty()) {
      // Schema-qualified from pg_extension rather than left to search_path;
      // see extension_schema.
      const std::string pgss = extension_schema(txn, "pg_stat_statements");
      if (pgss.empty()) return pgss_missing();

      // Full, untruncated text: recovering it is the whole point of the
      // queryid path, so this deliberately omits statementStats' LEFT(query,500).
      std::string q = R"(
        SELECT JSONB_BUILD_OBJECT(
                 'query',            pss.query,
                 -- text for the same reason as in statementStats
                 'query_id',         pss.queryid::text,
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
        FROM )" + pgss + R"(.pg_stat_statements AS pss
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
          forget_extension_schema("pg_stat_statements");
          return pgss_missing();
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
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // Schema-qualified from pg_extension rather than left to search_path; see
    // extension_schema. pgstattuple is the extension most often installed
    // somewhere other than public, since it is an operator's tool rather than
    // part of any application's schema.
    const std::string pgst = extension_schema(txn, "pgstattuple");
    if (pgst.empty()) return pgstattuple_missing();

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
      FROM )" + pgst + R"(.pgstattuple(
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
      FROM )" + pgst + R"(.pgstattuple_approx(
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
    } catch (const pqxx::insufficient_privilege& e) {
      return pgstattuple_denied("pgstattuple", e.what());
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42883") { // undefined_function
        // The extension was there when its schema was resolved, so it has
        // since moved or been dropped; forget the location so the next call
        // looks it up again.
        forget_extension_schema("pgstattuple");
        return pgstattuple_missing();
      }
      throw;
    }
  }

  // Compare a pg_extension.extversion against a major.minor floor. Only the
  // first two components are considered, which is all pgstattuple has ever
  // used, and an unparsable version is treated as too old rather than assumed
  // current -- guessing high turns a clear "upgrade the extension" answer into
  // an undefined-function error.
  static bool extversion_at_least(const std::string& v, int want_major, int want_minor) {
    int major = 0, minor = 0;
    size_t i = 0;
    if (i >= v.size() || !std::isdigit(static_cast<unsigned char>(v[i]))) return false;
    while (i < v.size() && std::isdigit(static_cast<unsigned char>(v[i])))
      major = major * 10 + (v[i++] - '0');
    if (i < v.size() && v[i] == '.') {
      i++;
      while (i < v.size() && std::isdigit(static_cast<unsigned char>(v[i])))
        minor = minor * 10 + (v[i++] - '0');
    }
    return major > want_major || (major == want_major && minor >= want_minor);
  }

  // Physical statistics for one index, from whichever pgstattuple function
  // fits its access method.
  //
  // The access method is resolved here rather than asked of the caller. The
  // three functions have nothing in common -- different names, disjoint
  // column sets, and one of them (pgstatindex) raises a bare "is not a btree
  // index" when pointed at the wrong kind -- so a caller made to choose would
  // have to look the index up first, which is the work this tool exists to do.
  //
  // The returned metrics are deliberately NOT normalized across access
  // methods, unlike tableBloat's exact/approx pair: leaf_fragmentation and
  // pending_tuples are not two spellings of one quantity, and flattening them
  // into shared field names would invent a comparison that does not exist.
  // 'access_method' names which set came back.
  const json index_bloat(const std::string& schema, const std::string& index_name) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string pgst = extension_schema(txn, "pgstattuple");
    if (pgst.empty()) return pgstattuple_missing();

    // Matched through pg_namespace by name rather than by casting to
    // regnamespace: the cast raises on a schema that does not exist, and
    // "no such index" should come back from a diagnostic tool as a stated
    // result, not as an exception.
    //
    // The size and scan count travel with the metrics because they are what
    // the metrics get weighed against -- a fragmented index nobody has
    // scanned since the last statistics reset is a candidate for dropping,
    // not for REINDEX, and that call cannot be made from density alone.
    const std::string meta_q = R"(
      SELECT JSONB_BUILD_OBJECT(
               'oid',           c.oid::text,
               'relkind',       c.relkind::text,
               'access_method', COALESCE(am.amname, ''),
               'schema',        n.nspname,
               'index',         c.relname,
               'table',         COALESCE(t.relname, ''),
               'index_size',    pg_relation_size(c.oid),
               'idx_scan',      s.idx_scan,
               -- GIN's pending list is only populated when fastupdate is on,
               -- and its size is bounded by the reloption if set and by the
               -- GUC otherwise. pending_pages without them is a number with
               -- no threshold to read it against.
               'fastupdate',    (SELECT o.option_value
                                 FROM pg_options_to_table(c.reloptions) AS o
                                 WHERE o.option_name = 'fastupdate'),
               'pending_list_limit_kb',
                                COALESCE((SELECT o.option_value
                                          FROM pg_options_to_table(c.reloptions) AS o
                                          WHERE o.option_name = 'gin_pending_list_limit'),
                                         current_setting('gin_pending_list_limit', true)),
               'extension_version',
                                (SELECT e.extversion FROM pg_extension AS e
                                 WHERE e.extname = 'pgstattuple')
             )
      FROM pg_class AS c
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
      LEFT JOIN pg_am AS am ON am.oid = c.relam
      LEFT JOIN pg_index AS i ON i.indexrelid = c.oid
      LEFT JOIN pg_class AS t ON t.oid = i.indrelid
      LEFT JOIN pg_stat_all_indexes AS s ON s.indexrelid = c.oid
      WHERE n.nspname = $1 AND c.relname = $2;
    )";

    pqxx::result mres = pqxx_exec(txn, meta_q, pqxx::params{schema, index_name});
    if (mres.empty() || mres[0][0].is_null()) {
      return {
        {"error", "no relation named \"" + schema + "\".\"" + index_name + "\""},
        {"hint", "indexes are listed per table by tableDetails, and across a "
                 "schema by duplicateIndexes"}
      };
    }

    json meta = json::parse(mres[0][0].as<std::string>());
    const std::string relkind = json_str(meta, "relkind");
    const std::string am      = json_str(meta, "access_method");

    // A partitioned index is a catalog entry with no storage of its own; the
    // pages belong to the per-partition indexes underneath it.
    if (relkind == "I") {
      return {
        {"error", "\"" + schema + "\".\"" + index_name +
                  "\" is a partitioned index and has no storage of its own"},
        {"hint", "inspect the index on an individual partition instead; "
                 "tableDetails on a partition lists them"},
        {"access_method", am}
      };
    }
    if (relkind != "i") {
      return {
        {"error", "\"" + schema + "\".\"" + index_name + "\" is not an index"},
        {"hint", relkind == "r" || relkind == "m" || relkind == "p"
                   ? "it looks like a table or materialized view; use tableBloat"
                   : "indexes are listed per table by tableDetails"}
      };
    }

    // gist, spgist and brin have no pgstattuple support at all. Saying so,
    // and naming what is supported, is the whole difference between a dead
    // end and a caller who knows to reach for pageinspect.
    static const std::map<std::string, std::string> FUNCS = {
      {"btree", "pgstatindex"},
      {"gin",   "pgstatginindex"},
      {"hash",  "pgstathashindex"},
    };
    auto fn = FUNCS.find(am);
    if (fn == FUNCS.end()) {
      return {
        {"error", "pgstattuple has no statistics function for a " +
                  (am.empty() ? std::string("(unknown)") : am) + " index"},
        {"hint", "supported access methods are btree, gin and hash; for gist, "
                 "spgist and brin the page-level detail is in the pageinspect "
                 "extension instead"},
        {"access_method", am}
      };
    }

    // pgstathashindex arrived in pgstattuple 1.5. An extension created before
    // that and never ALTER EXTENSION ... UPDATE'd is a live catalog entry
    // missing exactly this function, which would otherwise surface as the
    // extension being absent -- it is not, it is out of date.
    const std::string extver = json_str(meta, "extension_version");
    if (fn->second == "pgstathashindex" && !extversion_at_least(extver, 1, 5)) {
      return {
        {"error", "pgstathashindex requires pgstattuple 1.5, but version " +
                  (extver.empty() ? std::string("(unknown)") : extver) +
                  " is installed"},
        {"hint", "Run: ALTER EXTENSION pgstattuple UPDATE;"},
        {"access_method", am}
      };
    }

    // Column lists per access method, spelled out rather than SELECT *: the
    // shape is part of this tool's contract, and a column added by a future
    // extension version should not silently change it.
    static const std::map<std::string, std::string> COLUMNS = {
      {"btree",
       "'version', s.version, 'tree_level', s.tree_level,"
       " 'root_block_no', s.root_block_no, 'internal_pages', s.internal_pages,"
       " 'leaf_pages', s.leaf_pages, 'empty_pages', s.empty_pages,"
       " 'deleted_pages', s.deleted_pages, 'avg_leaf_density', s.avg_leaf_density,"
       " 'leaf_fragmentation', s.leaf_fragmentation"},
      {"gin",
       "'version', s.version, 'pending_pages', s.pending_pages,"
       " 'pending_tuples', s.pending_tuples"},
      {"hash",
       "'version', s.version, 'bucket_pages', s.bucket_pages,"
       " 'overflow_pages', s.overflow_pages, 'bitmap_pages', s.bitmap_pages,"
       " 'unused_pages', s.unused_pages, 'live_items', s.live_items,"
       " 'dead_items', s.dead_items, 'free_percent', s.free_percent"},
    };

    // The oid is resolved above and passed back as text, so the function
    // argument is a bind parameter and not the caller's identifier.
    const std::string stat_q =
      "SELECT JSONB_BUILD_OBJECT(" + COLUMNS.at(am) + ")"
      " FROM " + pgst + "." + fn->second + "($1::oid::regclass) AS s;";

    try {
      pqxx::result res = pqxx_exec(txn, stat_q, pqxx::params{json_str(meta, "oid")});
      if (res.empty() || res[0][0].is_null()) return {};

      json out = json::parse(res[0][0].as<std::string>());
      out["schema"]        = meta["schema"];
      out["index"]         = meta["index"];
      out["table"]         = meta["table"];
      out["access_method"] = am;
      out["index_size"]    = meta["index_size"];
      out["idx_scan"]      = meta["idx_scan"];
      // Only meaningful for GIN, and misleading anywhere else.
      if (am == "gin") {
        // fastupdate defaults to on, and the reloption is absent until it is
        // set explicitly, so a null here is "on" rather than "unknown".
        out["fastupdate"] = meta["fastupdate"].is_null()
          ? json("on") : meta["fastupdate"];
        out["pending_list_limit_kb"] = meta["pending_list_limit_kb"];
      }
      return out;
    } catch (const pqxx::insufficient_privilege& e) {
      return pgstattuple_denied(fn->second, e.what());
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42883") { // undefined_function
        forget_extension_schema("pgstattuple");
        return pgstattuple_missing();
      }
      throw;
    }
  }

  // How much of shared_buffers is in use, and the usage-count histogram.
  //
  // Why this is not answerable from tableIOStats: that counts only what
  // shared_buffers served, so a "miss" there may still have been served from
  // the OS page cache at RAM speed. pg_buffercache is the only in-core view of
  // the split.
  //
  // The histogram is the part worth reading. A hit ratio says almost nothing
  // on its own; mass at usage_count 2-5 is a stable working set, while
  // everything sitting at 0-1 with no unused buffers is clock-sweep churn --
  // the same ratio, the opposite diagnosis.
  const json buffer_cache_summary() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string ext = extension_schema(txn, "pg_buffercache");
    if (ext.empty()) return pg_buffercache_missing();

    // Gate on the *extension* version, not the server version. An extension
    // created before 1.4 and never ALTER EXTENSION ... UPDATE'd is a live
    // catalog entry missing exactly these two functions, which would otherwise
    // surface as the extension being absent -- it is not, it is out of date.
    pqxx::result ver = pqxx_exec(
      txn, "SELECT extversion FROM pg_extension WHERE extname = 'pg_buffercache'",
      pqxx::params{});
    const std::string extver = ver.empty() || ver[0][0].is_null()
      ? std::string() : ver[0][0].as<std::string>();
    if (!extversion_at_least(extver, 1, 4)) {
      const std::string have = extver.empty() ? std::string("(unknown)") : extver;
      // Two different problems that look alike. pg_buffercache 1.4 shipped with
      // PostgreSQL 16, so on 14 and 15 there is no 1.4 to update to and
      // "ALTER EXTENSION ... UPDATE" would send the operator in a circle.
      if (sess.server_version() < 160000) {
        return {
          {"error", "pg_buffercache_summary() was added in pg_buffercache 1.4, "
                    "which ships with PostgreSQL 16. This server has "
                    "pg_buffercache " + have + ", the newest available here"},
          {"hint", "Use bufferCacheContents instead: it reads the pg_buffercache "
                   "view, which exists in every version of the extension"}
        };
      }
      return {
        {"error", "pg_buffercache_summary() requires pg_buffercache 1.4, but "
                  "version " + have + " is installed"},
        {"hint", "Run: ALTER EXTENSION pg_buffercache UPDATE;"}
      };
    }

    const std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'buffers_used',        s.buffers_used,
               'buffers_unused',      s.buffers_unused,
               'buffers_dirty',       s.buffers_dirty,
               'buffers_pinned',      s.buffers_pinned,
               'usagecount_avg',      ROUND(s.usagecount_avg::numeric, 2),
               'shared_buffers_blocks',
                 (SELECT setting::bigint FROM pg_settings WHERE name = 'shared_buffers'),
               'block_size_bytes',    current_setting('block_size')::bigint,
               'shared_buffers_bytes',
                 (SELECT setting::bigint FROM pg_settings WHERE name = 'shared_buffers')
                 * current_setting('block_size')::bigint,
               'usage_counts',
                 (SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                           'usage_count', u.usage_count,
                           'buffers',     u.buffers,
                           'dirty',       u.dirty,
                           'pinned',      u.pinned)
                         ORDER BY u.usage_count)
                  FROM )" + ext + R"(.pg_buffercache_usage_counts() AS u),
               'extension_version',   $1::text
             )
      FROM )" + ext + R"(.pg_buffercache_summary() AS s;
    )";

    try {
      pqxx::result res = pqxx_exec(txn, query, pqxx::params{extver});
      if (res.empty() || res[0][0].is_null()) return {};
      return json::parse(res[0][0].as<std::string>());
    } catch (const pqxx::insufficient_privilege& e) {
      return pg_buffercache_denied("pg_buffercache_summary()", e.what());
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42883") {   // undefined_function
        forget_extension_schema("pg_buffercache");
        return pg_buffercache_missing();
      }
      throw;
    }
  }

  // Which relations own the cache, aggregated per relation and fork.
  //
  // Never raw per-buffer rows: the view has one row per buffer, so a machine
  // with 16GB of shared_buffers has two million of them.
  //
  // The aggregation happens before the join for the same reason -- it collapses
  // millions of buffer rows to a few thousand groups, instead of hash-joining
  // pg_class against the whole pool.
  //
  // Two joins that most published examples get wrong:
  //
  //   * Join on relfilenode, not oid. A mapped catalog has relfilenode = 0 in
  //     pg_class, so pg_relation_filenode(oid) is what matches the view.
  //   * Filter reldatabase to this database's oid *or* 0. Zero is the shared
  //     catalogs; buffers belonging to other databases in the instance are
  //     visible here but cannot be resolved to names locally, and reporting
  //     them as unknown rows would invite them to be read as this database's.
  const json buffer_cache_contents(int limit) {
    if (limit <= 0) limit = 20;
    if (limit > 200) limit = 200;

    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string ext = extension_schema(txn, "pg_buffercache");
    if (ext.empty()) return pg_buffercache_missing();

    const std::string query = R"(
      WITH buf AS (
        SELECT relfilenode,
               relforknumber,
               COUNT(*)                                  AS buffers,
               COUNT(*) FILTER (WHERE isdirty)           AS dirty,
               ROUND(AVG(usagecount)::numeric, 2)        AS avg_usagecount,
               SUM(pinning_backends)                     AS pins
          FROM )" + ext + R"(.pg_buffercache
         WHERE reldatabase IN (0, (SELECT oid FROM pg_database
                                    WHERE datname = current_database()))
           AND relfilenode IS NOT NULL
         GROUP BY relfilenode, relforknumber
      ), named AS (
        SELECT n.nspname                                 AS schema,
               c.relname                                 AS relation,
               c.relkind                                 AS relkind,
               c.oid                                     AS reloid,
               buf.relforknumber,
               CASE buf.relforknumber WHEN 0 THEN 'main' WHEN 1 THEN 'fsm'
                                      WHEN 2 THEN 'vm'   WHEN 3 THEN 'init' END AS fork,
               buf.buffers, buf.dirty, buf.avg_usagecount, buf.pins,
               buf.buffers * current_setting('block_size')::bigint AS cached_bytes
          FROM buf
          JOIN pg_class     AS c ON pg_relation_filenode(c.oid) = buf.relfilenode
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
      -- Ranking needs only the buffer counts, so the limit is applied before
      -- any fork is measured. pg_relation_size() is a stat() per call, and
      -- computing it inside `named` meant one syscall per *cached relation* to
      -- return `limit` rows -- on a large instance thousands of them, and on
      -- network storage with a cold dentry cache that is the dominant cost of
      -- the whole tool. The join stays above the limit so the ranking is still
      -- over relations that actually resolve, which keeps the result identical
      -- to what a caller got before.
      ), top AS (
        SELECT * FROM named ORDER BY buffers DESC LIMIT $1
      )
      SELECT JSONB_AGG(x ORDER BY x.buffers DESC) FROM (
        SELECT schema, relation, relkind, fork, buffers, dirty, pins,
               avg_usagecount, cached_bytes, fork_bytes,
               CASE WHEN fork_bytes > 0
                    THEN ROUND(cached_bytes * 100.0 / fork_bytes, 2) END
                 AS percent_of_fork_cached,
               ROUND(buffers * 100.0 /
                     NULLIF((SELECT setting::bigint FROM pg_settings
                              WHERE name = 'shared_buffers'), 0), 2)
                 AS percent_of_shared_buffers
          FROM top
          CROSS JOIN LATERAL (
            SELECT CASE top.relforknumber
                     WHEN 0 THEN pg_relation_size(top.reloid, 'main')
                     WHEN 1 THEN pg_relation_size(top.reloid, 'fsm')
                     WHEN 2 THEN pg_relation_size(top.reloid, 'vm')
                     WHEN 3 THEN pg_relation_size(top.reloid, 'init')
                   END AS fork_bytes
          ) AS sz
      ) AS x;
    )";

    try {
      pqxx::result res = pqxx_exec(txn, query, pqxx::params{limit});
      json rows = (res.empty() || res[0][0].is_null())
        ? json::array() : json::parse(res[0][0].as<std::string>());
      return {
        {"limit", limit},
        {"relations", rows}
      };
    } catch (const pqxx::insufficient_privilege& e) {
      return pg_buffercache_denied("the pg_buffercache view", e.what());
    } catch (const pqxx::sql_error& e) {
      if (e.sqlstate() == "42P01") {   // undefined_table
        forget_extension_schema("pg_buffercache");
        return pg_buffercache_missing();
      }
      throw;
    }
  }

  const json database_size() {
    Session sess = open_session();
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

    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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

    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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
    Session sess = open_session();
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

  // Structure only. In 4.0.0 this shed reltuples, the measured sizes, the
  // pg_stat_user_tables row, the per-column pg_stats histograms and the
  // per-index scan counters; they are in tableStats and tableSize now.
  //
  // Two consequences worth naming. The tool no longer returns
  // most_common_vals, which was literal column values -- on a customers table,
  // customer names -- from the tool an agent calls most often to inspect
  // structure. And nothing here is version-conditional any more, so the
  // server_version() probe and the string-erase fixup that used to drop
  // last_idx_scan on PostgreSQL 14 and 15 are both gone; that gate lives in
  // table_stats() now.
  const json table(const std::string& schema, const std::string& table) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'table', c.relname,
               'description', COALESCE(obj_description(c.oid, 'pg_class'), ''),
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'definition', CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END,
               'reloptions', c.reloptions,
               'columns', columns,
               'toast', CASE WHEN c.reltoastrelid != 0 THEN
                          JSONB_BUILD_OBJECT('name', tc.relname)
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
                         'compression', CASE a.attcompression WHEN 'p' THEN 'pglz' WHEN 'l' THEN 'lz4' ELSE 'default' END))) AS columns
                       FROM pg_attribute AS a
                       JOIN pg_type AS t ON t.oid = a.atttypid
                       LEFT JOIN pg_attrdef AS ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                       WHERE attnum > 0
                         AND attrelid = c.oid
                         AND NOT attisdropped) _lat29 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(indexname,
                          JSONB_BUILD_OBJECT('definition', indexdef)) AS indexes
                         FROM pg_indexes AS i
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

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table});

    if (!res.empty() && !res[0][0].is_null()) {
      std::string pgsql_table = res[0][0].as<std::string>();
      return json::parse(pgsql_table);
    } else {
      return {};
    }
  }

  // Plan a statement as if the indexes were different, using hypopg.
  //
  // This is what closes the asymmetry the diagnostic prompts otherwise have to
  // admit to: a proposed rewrite can be explained on the spot, while a proposed
  // index used to be a prediction. A hypothetical index is planned against but
  // never built -- no lock, no catalog row, no file -- so the planner's verdict
  // is available before anyone commits to the write cost an index carries for
  // the rest of its life.
  //
  // It never executes the statement. hypopg cannot serve EXPLAIN ANALYZE (there
  // is no index to scan), so this tool is plan-only and therefore strictly
  // safer than explainQuery with analyze.
  //
  // THE RESET BRACKET IS NOT OPTIONAL. Measured against hypopg 1.4.3: a
  // hypothetical index lives in backend-local memory for the whole session and
  // is cleared by none of the things that would be expected to clear it --
  // not ROLLBACK, not a new transaction, and not DISCARD ALL, which is exactly
  // what PgBouncer issues as server_reset_query. Only hypopg_reset() removes
  // it. Against a transaction-mode pooler that means one caller's hypothetical
  // index would otherwise stay on the backend and silently reshape the next
  // caller's plans -- a wrong answer with nothing to indicate it. So the reset
  // runs on the way in, which protects this call from whatever a previous one
  // left, and again on the way out through a scope guard that survives an
  // exception, which protects the next call from this one. Either alone would
  // do most of the job; both is cheap and neither depends on the other.
  const json evaluate_index(const std::string& sql, const json& create_defs,
                            const json& hide_names) {
    if (sql.empty()) throw std::runtime_error("sql is required");
    if (!create_defs.is_array() || !hide_names.is_array())
      throw std::runtime_error("create and hide must be arrays");
    if (create_defs.empty() && hide_names.empty())
      throw std::runtime_error(
        "name at least one index: 'create' takes CREATE INDEX statements to "
        "plan against, 'hide' the names of existing indexes to plan without");
    if (create_defs.size() + hide_names.size() > 16)
      throw std::runtime_error("at most 16 indexes may be evaluated in one call");

    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string hypo = extension_schema(txn, "hypopg");
    if (hypo.empty())
      return {{"error", "hypopg is not installed"},
              {"hint", "Run: CREATE EXTENSION hypopg; -- it plans against "
                       "indexes without building them, and creates nothing"}};

    // hypopg_hide_index arrived in 1.4.0. Gated on the extension version rather
    // than the server version, for the reason recorded when pg_buffercache
    // needed the same treatment: the two move independently.
    std::string ver;
    {
      pqxx::result r = pqxx_exec(
        txn, "SELECT extversion FROM pg_extension WHERE extname = 'hypopg'", pqxx::params{});
      if (!r.empty() && !r[0][0].is_null()) ver = r[0][0].as<std::string>();
    }
    const bool can_hide = ver >= "1.4";
    if (!hide_names.empty() && !can_hide)
      return {{"error", "hiding an existing index needs hypopg 1.4.0 or later"},
              {"hint", "this server has hypopg " + ver +
                       "; run ALTER EXTENSION hypopg UPDATE, or use 'create' "
                       "alone, which every version supports"},
              {"hypopg_version", ver}};

    auto reset = [&]() {
      try { txn.exec("SELECT " + hypo + ".hypopg_reset()"); } catch (...) {}
      if (can_hide)
        try { txn.exec("SELECT " + hypo + ".hypopg_unhide_all_indexes()"); } catch (...) {}
    };
    reset();
    struct Guard {
      std::function<void()> f;
      ~Guard() { f(); }
    } guard{reset};

    // Each EXPLAIN runs inside a savepoint so that a failing statement leaves
    // the transaction usable -- the reset on the way out needs it alive.
    const bool pg16 = sess.server_version() >= 160000;
    auto plan_of = [&]() -> json {
      try {
        pqxx::subtransaction sub{txn};
        pqxx::result r = sub.exec("EXPLAIN (FORMAT JSON) " + sql);
        json p = json::parse(r[0][0].as<std::string>());
        sub.commit();
        return p;
      } catch (const pqxx::sql_error& e) {
        // 42P02: the statement carries $n placeholders. GENERIC_PLAN is the
        // same fallback explainQuery uses, and it is PostgreSQL 16+.
        if (e.sqlstate() != "42P02" || !pg16) throw;
        pqxx::subtransaction sub{txn};
        pqxx::result r = sub.exec("EXPLAIN (FORMAT JSON, GENERIC_PLAN) " + sql);
        json p = json::parse(r[0][0].as<std::string>());
        sub.commit();
        return p;
      }
    };
    auto cost_of = [](const json& p) -> double {
      if (p.is_array() && !p.empty() && p[0].contains("Plan") &&
          p[0]["Plan"].contains("Total Cost"))
        return p[0]["Plan"]["Total Cost"].get<double>();
      return 0.0;
    };

    const json baseline = plan_of();

    json indexes = json::array();
    for (const auto& d : create_defs) {
      if (!d.is_string())
        throw std::runtime_error("every entry of create must be a CREATE INDEX statement");
      const std::string def = d.get<std::string>();
      try {
        pqxx::result r = pqxx_exec(
          txn, "SELECT indexrelid::text, indexname FROM " + hypo + ".hypopg_create_index($1)",
          pqxx::params{def});
        if (r.empty()) continue;
        const std::string oid = r[0][0].as<std::string>();
        const std::string nm  = r[0][1].as<std::string>();
        pqxx::result sz = pqxx_exec(
          txn, "SELECT " + hypo + ".hypopg_relation_size($1::oid)", pqxx::params{oid});
        indexes.push_back({{"definition", def},
                           {"name", nm},
                           {"estimated_size", sz.empty() || sz[0][0].is_null()
                              ? json(nullptr) : json(sz[0][0].as<long long>())}});
      } catch (const pqxx::sql_error& e) {
        return {{"error", "hypopg rejected an index definition"},
                {"definition", def},
                {"detail", e.what()}};
      }
    }

    json hidden = json::array();
    for (const auto& h : hide_names) {
      if (!h.is_string())
        throw std::runtime_error("every entry of hide must be an index name");
      const std::string nm = h.get<std::string>();
      try {
        pqxx::result r = pqxx_exec(
          txn,
          // to_regclass rather than a ::regclass cast: the cast raises on an
          // unknown name, which would surface a raw server error instead of
          // the message below naming what to do about it.
          "SELECT c.oid::text FROM pg_class AS c"
          " WHERE c.oid = to_regclass($1) AND c.relkind IN ('i', 'I')",
          pqxx::params{nm});
        if (r.empty())
          return {{"error", "no index named " + nm},
                  {"hint", "name an existing index, schema-qualified if it is "
                           "not on the search path; tableDetails lists them "
                           "for one table"}};
        txn.exec("SELECT " + hypo + ".hypopg_hide_index(" +
                 txn.quote(r[0][0].as<std::string>()) + "::oid)");
        hidden.push_back({{"index", nm}});
      } catch (const pqxx::sql_error& e) {
        return {{"error", "could not hide " + nm}, {"detail", e.what()}};
      }
    }

    const json after = plan_of();
    const std::string after_text = after.dump();
    // Whether the planner took the index is the answer people actually want,
    // and a cost figure alone hides it: hypopg's most useful verdict is often
    // that a proposed index was ignored.
    for (auto& e : indexes)
      e["used"] = after_text.find(e["name"].get<std::string>()) != std::string::npos;

    const double before_cost = cost_of(baseline), after_cost = cost_of(after);
    json out = {
      {"statement", sql},
      {"hypopg_version", ver},
      {"baseline", {{"total_cost", before_cost}, {"plan", baseline}}},
      {"hypothetical", {{"total_cost", after_cost}, {"plan", after}}},
      {"cost_ratio", before_cost > 0 ? json(after_cost / before_cost) : json(nullptr)},
      {"indexes", indexes}
    };
    if (!hidden.empty()) out["hidden"] = hidden;
    out["note"] = "Hypothetical indexes are planned against and never built. "
                  "The cost is the planner's estimate, not a measurement: it "
                  "says the plan would change, not how long it would take.";
    return out;
  }

  // Which tools this role can actually use on this connection.
  //
  // The catalog is world-readable, so most of the tool set works for any role
  // that can connect. What varies is a small, knowable set: the predefined
  // roles that gate the monitoring extras, and whether the role can read table
  // data at all. Measured against PostgreSQL 18: a bare login role runs 47 of
  // the 58 tools at full fidelity, pg_monitor takes that to 54, and the ones
  // that remain are exactly the three that touch row data.
  //
  // The point of asking once, up front, is that the alternative is discovering
  // it tool by tool -- and the discovery is misleading, because a filtered
  // answer looks like an empty one. tableStats on a role without SELECT
  // returns every column with null statistics, which is byte-for-byte what a
  // never-analyzed table looks like.
  //
  // Deliberately reports no role memberships and no GRANT statements. Which
  // predefined role gates a tool is PostgreSQL's business, not the caller's --
  // the caller needs to know what works. And emitting DDL would contradict what
  // this server tells every client about itself: plans verbatim, no heuristics,
  // no generated DDL. Naming a grant in the hint of a tool that *failed* is
  // diagnosis; listing grants beside every unavailable tool is a standing
  // recommendation to escalate privilege, which is not this server's to make.
  const json check_privileges() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // One round trip. pg_has_role is looked up through pg_roles rather than by
    // name so that a predefined role missing on some version yields false
    // instead of raising -- pg_read_all_data is PostgreSQL 14+, which is the
    // floor this server supports, but the guard costs nothing and the next
    // predefined role added upstream will not need this remembered.
    auto has_role = [](const char* name) {
      return std::string(
        "COALESCE((SELECT pg_has_role(current_user, oid, 'USAGE')"
        " FROM pg_roles WHERE rolname = '") + name + "'), false)";
    };
    const std::string query =
      "SELECT JSONB_BUILD_OBJECT("
      " 'role', current_user,"
      " 'superuser', COALESCE((SELECT rolsuper FROM pg_roles"
      "                        WHERE rolname = current_user), false),"
      " 'monitor',      " + has_role("pg_monitor")           + ","
      " 'read_stats',   " + has_role("pg_read_all_stats")    + ","
      " 'read_settings'," + has_role("pg_read_all_settings") + ","
      " 'scan_tables',  " + has_role("pg_stat_scan_tables")  + ","
      " 'read_data',    " + has_role("pg_read_all_data")     + ")";

    pqxx::result res = txn.exec(query);
    const json p = json::parse(res[0][0].as<std::string>());

    const bool super     = p.value("superuser", false);
    const bool monitor   = super || p.value("monitor", false);
    const bool stats     = monitor || p.value("read_stats", false);
    const bool settings  = monitor || p.value("read_settings", false);
    const bool scan      = monitor || p.value("scan_tables", false);
    const bool data      = super  || p.value("read_data", false);

    // Extension presence is a different failure from missing privilege, and
    // conflating them would send an operator to the wrong fix. Checked here so
    // the answer distinguishes "not installed" from "not permitted".
    const bool has_pgstattuple  = !extension_schema(txn, "pgstattuple").empty();
    const bool has_buffercache  = !extension_schema(txn, "pg_buffercache").empty();
    const bool has_pgss         = !extension_schema(txn, "pg_stat_statements").empty();
    const bool has_hypopg       = !extension_schema(txn, "hypopg").empty();

    json denied = json::array(), degraded = json::array();
    auto deny = [&](const char* tool, const std::string& why) {
      denied.push_back({{"tool", tool}, {"reason", why}});
    };
    auto degrade = [&](const char* tool, const std::string& what) {
      degraded.push_back({{"tool", tool}, {"what", what}});
    };

    // Denied means the tool cannot produce an answer for this role at all.
    for (const char* t : {"tableBloat", "indexBloat"}) {
      if (!has_pgstattuple) deny(t, "the pgstattuple extension is not installed");
      else if (!scan)       deny(t, "the pgstattuple functions are restricted to "
                                    "roles permitted to run table-scanning "
                                    "monitoring functions");
    }
    for (const char* t : {"bufferCacheSummary", "bufferCacheContents"}) {
      if (!has_buffercache) deny(t, "the pg_buffercache extension is not installed");
      else if (!monitor)    deny(t, "pg_buffercache is readable only by roles "
                                    "granted the monitoring role");
    }
    if (!has_hypopg)
      deny("evaluateIndex", "the hypopg extension is not installed");
    else if (!data)
      degrade("evaluateIndex", "planning a statement needs SELECT on the tables "
                               "it references, so this fails on any statement "
                               "reaching a table this role cannot read");
    if (!has_pgss)
      deny("statementStats", "the pg_stat_statements extension is not installed");
    else if (!stats)
      degrade("statementStats", "the query text of statements run by other roles "
                                "is replaced with <insufficient privilege>; the "
                                "counters beside it are complete");

    if (!settings)
      degrade("serverSettings", "settings marked superuser-only are absent "
                                "entirely rather than masked, so the category "
                                "count is lower than a privileged role sees");
    if (!stats)
      degrade("currentActivity", "the query text and some columns of backends "
                                 "belonging to other roles are hidden");

    // The three that read row data. Not "denied": privilege here is per object,
    // so a role without blanket read access may still hold SELECT on some
    // tables and none on others. Reporting these as unavailable would be as
    // wrong as reporting them as available.
    if (!data) {
      degrade("tableStats", "per-column statistics are omitted for any column "
                            "this role cannot SELECT, and the columns still "
                            "appear with null values -- indistinguishable from "
                            "a table that was never analyzed");
      degrade("checkKey", "fails on any table this role cannot SELECT");
      degrade("explainQuery", "fails on any statement referencing a table this "
                              "role cannot SELECT");
    }

    // Everything not named is fully available. Counting rather than listing:
    // the exceptions are the answer, and enumerating 53 working tool names
    // would be most of the payload.
    const size_t total = tool_scopes().size();
    json out = {
      {"connection", active_cfg().name},
      {"role", p.value("role", "")},
      {"tools", total},
      {"available", total - denied.size() - degraded.size()}
    };
    if (!degraded.empty()) out["degraded"] = degraded;
    if (!denied.empty())   out["denied"]   = denied;
    return out;
  }

  // --- Catalog statistics: readings, but free ones ------------------------
  //
  // Everything here comes out of pg_stat_user_tables, pg_stats and the
  // pg_class counters that ANALYZE maintains. No relation is opened and no
  // file is stat()ed, so the cost is a catalog scan and nothing else. That is
  // the line these two tools sit on: they are volatile, which is why they are
  // not in tableDetails, but they are cheap, which is why they are not behind
  // the gate that tableSize is.
  //
  // size_estimate is relpages * 8192 and is deliberately not called `size`.
  // relpages is set by VACUUM and ANALYZE, so between runs it can be arbitrarily
  // stale -- on a table that has doubled since the last analyze it is half the
  // truth. estimated_from carries the timestamp that produced it so a caller can
  // judge the staleness instead of guessing at it, and tableSize is where an
  // actually-measured number comes from.

  // The PostgreSQL 16 additions, shared by both statistics tools.
  // n_tup_newpage_upd counts updates that had to move the row to another page:
  // the direct measure of failed HOT updates, whose usual cause is a too-high
  // fillfactor or an index on a frequently updated column. last_seq_scan dates
  // the last sequential scan, which turns a large seq_scan count into something
  // actionable.
  static std::string stats_pg16_fragment(int server_version) {
    return server_version >= 160000
      ? R"(, 'n_tup_newpage_upd', s.n_tup_newpage_upd,
            'last_seq_scan', s.last_seq_scan)"
      : "";
  }

  // The pg_stat_user_tables columns both tools return, in one place so the
  // single-table and schema-wide forms cannot drift apart.
  static constexpr const char* kTableStatsCommon = R"(
               'rows', c.reltuples,
               'size_estimate', c.relpages::bigint * 8192,
               'estimated_from', GREATEST(s.last_vacuum, s.last_autovacuum,
                                          s.last_analyze, s.last_autoanalyze),
               'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
               'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
               'n_mod_since_analyze', s.n_mod_since_analyze,
               'n_ins_since_vacuum', s.n_ins_since_vacuum,
               'last_vacuum', GREATEST(s.last_vacuum, s.last_autovacuum),
               'last_analyze', GREATEST(s.last_analyze, s.last_autoanalyze))";

  const json table_stats(const std::string& schema, const std::string& table) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string pg16 = stats_pg16_fragment(sess.server_version());
    // pg_stat_user_indexes.last_idx_scan is PostgreSQL 16+. Built the same way
    // as the table fragment rather than by erasing a literal out of the
    // finished query, which is what the pre-4.0.0 tableDetails did: if the two
    // ever drifted the erase silently did nothing and the pg14/pg15 jobs failed
    // on an undefined column.
    const std::string idx_pg16 = sess.server_version() >= 160000
      ? R"(, 'last_use', si.last_idx_scan)" : "";

    std::string query = std::string(R"(
      SELECT JSONB_BUILD_OBJECT(
               'table', c.relname,)") + kTableStatsCommon + pg16 + R"(,
               'columns', COALESCE(columns, '{}'::jsonb),
               'indexes', COALESCE(indexes, '{}'::jsonb))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(a.attname,
                          JSONB_BUILD_OBJECT(
                           'null_frac', ps.null_frac,
                           'avg_width', ps.avg_width,
                           'n_distinct', ps.n_distinct,
                           'physical_order_correlation', ps.correlation,
                           'most_common_vals', ps.most_common_vals,
                           'most_common_freqs', ps.most_common_freqs)) AS columns
                         FROM pg_attribute AS a
                         LEFT JOIN pg_stats AS ps ON ps.schemaname = $1
                                                  AND ps.tablename = $2
                                                  AND ps.attname = a.attname
                         WHERE a.attnum > 0
                           AND a.attrelid = c.oid
                           AND NOT a.attisdropped) _lat40 ON true
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(si.indexrelname,
                          JSONB_BUILD_OBJECT(
                           'index_uses', si.idx_scan)" + idx_pg16 + R"()) AS indexes
                         FROM pg_stat_user_indexes AS si
                         WHERE si.relid = c.oid) _lat41 ON true
      WHERE c.relnamespace = $1::regnamespace
        AND c.relname = $2;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table});

    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    return {};
  }

  const json list_table_stats(const std::string& schema) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string pg16 = stats_pg16_fragment(sess.server_version());

    // No per-column pg_stats here, deliberately. tableDetails never carried
    // them in the schema-wide form either, and default_statistics_target
    // sample values for every column of every table in a schema is a payload
    // nobody asked for. Name a table to tableStats to get them.
    std::string query = std::string(R"(
      SELECT JSONB_OBJECT_AGG(c.relname,
              JSONB_BUILD_OBJECT()") + kTableStatsCommon + pg16 + R"(
               ))
      FROM pg_class AS c
      LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
      WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND c.relkind IN ('r', 'p', 'm', 'v');
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    return {};
  }

  // --- Measured size: the gated tier --------------------------------------
  //
  // pg_table_size() and friends are not physical reads -- they stat() one file
  // per 1 GB segment and read no blocks. What makes them worth gating is the
  // lock: each opens the relation with AccessShareLock, so a size call on a
  // table that an ALTER TABLE is rewriting waits behind AccessExclusiveLock.
  // Across a schema that means the call stalls on precisely the table an
  // incident is about. Since 3.2.1 every statement carries a statement_timeout,
  // so it fails rather than hanging -- but it still fails, and it fails at the
  // worst moment.
  //
  // Hence a separate tool rather than a flag: the cost note is in the
  // description the model reads while choosing which tool to call, not in an
  // argument it reads after it has already chosen.
  const json table_size(const std::string& schema, const std::string& table) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
               'table', c.relname,
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'main_size', pg_relation_size(c.oid, 'main'),
               'size', pg_table_size(c.oid),
               'indexes_size', pg_indexes_size(c.oid),
               'total_size', pg_total_relation_size(c.oid),
               'toast', CASE WHEN c.reltoastrelid != 0 THEN
                          JSONB_BUILD_OBJECT(
                            'name',       tc.relname,
                            'size',       pg_relation_size(c.reltoastrelid),
                            'index_size', pg_indexes_size(c.reltoastrelid))
                        END,
               'indexes', COALESCE(indexes, '{}'::jsonb))
      FROM pg_class AS c
      LEFT JOIN pg_class AS tc ON tc.oid = c.reltoastrelid
      LEFT JOIN LATERAL (SELECT JSONB_OBJECT_AGG(i.relname, pg_relation_size(i.oid)) AS indexes
                         FROM pg_index AS ix
                         JOIN pg_class AS i ON i.oid = ix.indexrelid
                         WHERE ix.indrelid = c.oid) _lat42 ON true
      WHERE c.relnamespace = $1::regnamespace
        AND c.relname = $2;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table});

    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    return {};
  }

  const json list_table_sizes(const std::string& schema) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // This is the form the lock caveat is really about: one relation_open per
    // table in the schema, so a single table under AccessExclusiveLock blocks
    // the whole call rather than one row of it.
    std::string query = R"(
      SELECT JSONB_OBJECT_AGG(c.relname,
              JSONB_BUILD_OBJECT(
               'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                      WHEN 'm' THEN 'materialized view' WHEN 'v' THEN 'view' END,
               'size', pg_table_size(c.oid),
               'indexes_size', pg_indexes_size(c.oid),
               'total_size', pg_total_relation_size(c.oid)))
      FROM pg_class AS c
      WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
        AND c.relkind IN ('r', 'p', 'm', 'v');
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});

    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    return {};
  }

  // The three _meta keys the stateless revision defines, and the revision
  // itself. Spelled out once: a typo in one of these strings would silently
  // route every modern request down the legacy path.
  static constexpr const char* kMetaProtocolVersion =
    "io.modelcontextprotocol/protocolVersion";
  static constexpr const char* kMetaClientCapabilities =
    "io.modelcontextprotocol/clientCapabilities";
  static constexpr const char* kMetaServerInfo =
    "io.modelcontextprotocol/serverInfo";
  static constexpr const char* kModernProtocol = "2026-07-28";

  // Legacy revisions this server's surface is genuinely the same across: tools
  // only, text content, no resources or prompts. Listed rather than
  // open-ended, because echoing a version nobody exercises asserts support for
  // features that may not exist.
  static const std::vector<std::string>& supported_protocols() {
    static const std::vector<std::string> v = [] {
      std::vector<std::string> all = {
        "2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"
      };
      // PG_LICHT_MAX_PROTOCOL caps what this server will agree to in the
      // handshake. initialize replies with the client's requested revision only
      // if it appears here, and falls back to 2024-11-05 otherwise -- so
      // trimming this list is how an operator forces an older, text-only
      // response shape without the client offering any way to ask for one.
      //
      // It exists because a client can advertise a revision it does not fully
      // implement. Claude Code negotiates 2025-06-18 and reads `content`; a
      // server that answers such a client with structuredContent alone leaves
      // it with nothing, reported as "the content was missing from the response
      // object". There is no client-side setting for this: `claude mcp add`
      // offers transport, env, headers and scope, and no protocol version.
      //
      // Revision strings are ISO dates, so a string comparison orders them.
      const char* cap = std::getenv("PG_LICHT_MAX_PROTOCOL");
      if (cap == nullptr || *cap == '\0') return all;
      std::vector<std::string> kept;
      for (const auto& p : all) if (p <= cap) kept.push_back(p);
      // A cap below every known revision would leave nothing to negotiate,
      // which is worse than ignoring it.
      return kept.empty() ? all : kept;
    }();
    return v;
  }

  // Everything the server answers to, legacy and modern.
  static const std::vector<std::string>& all_protocols() {
    static const std::vector<std::string> v = [] {
      std::vector<std::string> a = supported_protocols();
      a.push_back(kModernProtocol);
      return a;
    }();
    return v;
  }

  // What a client should know before its first call, and what currently lives
  // only inside individual tool descriptions where a client browsing the server
  // never sees it.
  static std::string instructions() {
    return
      "Every statement runs inside a transaction opened with SET TRANSACTION "
      "READ ONLY. Nothing here writes: the one tool that executes anything is "
      "explainQuery with analyze:true, and it does so only after the plan is "
      "proven free of any ModifyTable node.\n\n"
      "explainQuery returns the plan verbatim. There are no heuristics and no "
      "generated DDL -- reading the plan is yours to do.\n\n"
      "Every tool that reads a database takes an optional 'connection' naming "
      "one configured target; listConnections and listTopology enumerate them "
      "without connecting. Tools may instead take 'instance' (the databases of "
      "one postmaster), 'replication_group' (a primary and its replicas) or "
      "'group' (an operator label), and then return one result per member. "
      "Which of those a tool accepts depends on where its answer actually "
      "varies, and its input schema says which.\n\n"
      "Role -- primary or replica -- is observed on every connection rather "
      "than configured, because failover swaps it. verifyTopology checks the "
      "declared topology against what the servers report.";
  }

  void initialize(const json& id, const json& params) {
    // 3.1.1 ignored params.protocolVersion and always replied 2024-11-05.
    // Honouring it is what lets tool annotations reach a client that
    // understands them while a 2024-11-05 client keeps byte-identical output.
    //
    // When the requested revision is one this server will not speak, the spec
    // says to answer with another it does support. Answering with the *highest*
    // rather than the oldest is what makes PG_LICHT_MAX_PROTOCOL behave as an
    // operator would expect: capping at 2025-03-26 and being asked for
    // 2025-06-18 should yield 2025-03-26, not drop all the way to 2024-11-05
    // and silently cost the client annotations and titles it can use.
    const std::string want = params.value("protocolVersion", std::string());
    const auto& supported = supported_protocols();
    std::string reply = supported.back();   // highest this server will speak
    for (const auto& v : supported)
      if (v == want) { reply = v; break; }
    client_protocol_ = reply;

    send_response(id, {
        {"protocolVersion", reply},
        // 4.0.0 adds resources, prompts and completions. Declared together
        // because they were authored together against the finished tool
        // surface, which is what the statistics split was blocking.
        {"capabilities", {
            {"tools", json::object()},
            {"resources", json::object()},
            {"prompts", json::object()},
            {"completions", json::object()}
	  }},
        {"serverInfo", {{"name", "pg-licht-cpp"}, {"version", PGLICHT_VERSION}}}
      });
  }

  // How many members one sweep will visit. Fan-out is sequential -- the server
  // is a single-threaded getline loop with no stdout write mutex -- so width is
  // wall-clock, and a sweep that quietly covered 10 of 40 databases would read
  // as "nothing else is affected".
  static constexpr size_t kMaxSweepMembers = 32;

  // How many members a sweep visits at once.
  //
  // Bounded, but not as tightly as it first looks like it should be. The worry
  // with concurrency is piling connections onto one server -- and a sweep
  // cannot do that: members of a replication group are distinct servers by
  // definition, and a group sweep already collapses members that would answer
  // identically. So the width is spread across machines, one connection each,
  // which is the same load a single-connection call places on any one of them.
  //
  // Sized so an ordinary production replication group -- a primary and a dozen
  // replicas -- finishes in one round rather than two.
  static constexpr size_t kSweepConcurrency = 16;

  // Runs body(0..n-1) on at most kSweepConcurrency threads and joins them all.
  //
  // Every place this server talks to more than one database at a time goes
  // through here, so "how wide does pg_licht ever go" has one answer in one
  // place. 4.1.0 built this pool inline in fan_out() and left two other paths
  // walking the registry one server at a time: verify_topology(), and the role
  // probe in fan_out() itself, which sat directly in front of the pool it did
  // not use. Measured on twelve unreachable hosts at a 5s connect timeout, both
  // took 60s against 5s for the same call swept without a role filter.
  //
  // body must not throw. Each caller wraps its own work in try/catch, because
  // an exception crossing a std::thread boundary calls std::terminate and one
  // unreachable host must never take the process with it.
  static void parallel_for(size_t n, const std::function<void(size_t)>& body) {
    if (n == 0) return;
    const size_t width = std::min<size_t>(n, kSweepConcurrency);
    // One member is the overwhelmingly common case, and spawning a thread to
    // wait on a single connect is pure latency.
    if (width == 1) {
      for (size_t i = 0; i < n; i++) body(i);
      return;
    }
    std::atomic<size_t> next{0};
    std::vector<std::thread> pool;
    pool.reserve(width);
    for (size_t w = 0; w < width; w++)
      pool.emplace_back([&] {
        for (size_t i = next.fetch_add(1); i < n; i = next.fetch_add(1)) body(i);
      });
    for (auto& t : pool) t.join();
  }

  // Why a sweep on this axis is refused, or "" when it is allowed.
  //
  // These are -32602 rather than a shrug because the failure they prevent is
  // invisible in the payload: sweeping an instance-wide reading across eight
  // databases returns the same rows eight times, and nothing in the result says
  // so. A caller reading eight identical answers concludes the databases agree.
  std::string sweep_rejection(const std::string& axis,
                              const std::string& tool_name,
                              const std::vector<std::string>& members) const {
    auto it = tool_scopes().find(tool_name);
    if (it == tool_scopes().end()) return "";
    const ToolScope& sc = it->second;
    const std::string first = members.empty() ? std::string("<connection>") : members.front();

    if (sc.registry)
      return tool_name + " reads no database, so it has no target to sweep";
    if (tool_name == "explainQuery")
      return "explainQuery never sweeps: the same statement is rarely valid in "
             "another database, and with analyze:true it would execute once per "
             "member";
    if (axis == "instance" && !sc.per_database)
      return tool_name + " is instance-wide -- every database on one postmaster "
             "returns it identically, so sweeping them would repeat one answer. "
             "Call it once with connection=" + first;
    if (axis == "replication_group" && !sc.per_server)
      return tool_name + " is byte-identical across a replication group, because "
             "physical replication copies it verbatim. Call it once with "
             "connection=" + first;
    return "";
  }

  // Run one tool across a set of connections.
  //
  // The loop lives here, above dispatch, which is what lets every query method
  // go on reading active_cfg() without learning that a sweep exists -- and what
  // keeps a call that names a single connection byte-identical to 3.1.1.
  json fan_out(const std::string& axis, const std::string& name,
               const std::string& role_filter, const std::string& tool_name,
               const json& arguments) {
    std::vector<std::string> members = registry_.members(axis, name);
    auto scope_it = tool_scopes().find(tool_name);
    const ToolScope sc = scope_it == tool_scopes().end() ? ToolScope{} : scope_it->second;

    json skipped = json::array();
    auto skip = [&](const std::string& conn, const std::string& why) {
      skipped.push_back({{"connection", conn}, {"reason", why}});
    };

    // A group may span instances and replication groups, so it cannot be
    // refused the way the two topology axes are -- sweeping listTables across
    // two databases on different hosts is exactly what a group is for. Instead
    // collapse the members that would answer identically, and say which and why.
    if (axis == "group") {
      std::set<std::string> seen_instance, seen_group;
      std::vector<std::string> kept;
      for (const auto& m : members) {
        const auto& c = registry_.get(m);
        if (!sc.per_database && !c.instance.empty() && !seen_instance.insert(c.instance).second) {
          skip(m, "another member of instance " + c.instance + " already answered, "
                  "and this reading is instance-wide");
          continue;
        }
        if (!sc.per_server && !c.replication_group.empty() &&
            !seen_group.insert(c.replication_group).second) {
          skip(m, "another member of replication_group " + c.replication_group +
                  " already answered, and this reading is byte-identical across it");
          continue;
        }
        kept.push_back(m);
      }
      members = kept;
    }

    // Role filtering has to observe, never assume -- so it costs one connect
    // per member before the tool's own. Every member is probed even when only
    // the primary is wanted: stopping at the first would hide split brain, and
    // "never pick one of two primaries" outranks saving a connect.
    json notes = json::array();
    if (!role_filter.empty()) {
      // The probe is one connect per member and nothing else, so it runs
      // through the same pool the sweep below uses. Only the waiting is
      // parallel: the classification that follows stays a sequential pass over
      // the results in configuration order, so `matched`, `primaries` and the
      // skip reasons come out in the order they always did.
      std::vector<std::string> observed(members.size());
      parallel_for(members.size(), [&](size_t i) {
        try {
          Session probe{with_connect_timeout(registry_.get(members[i]),
                                             kSweepConnectTimeoutSeconds)};
          observed[i] = probe.role();
        } catch (const std::exception&) {
          observed[i] = "unknown";
        } catch (...) {
          // See verify_topology(): terminate, not a failed member.
          observed[i] = "unknown";
        }
      });

      std::vector<std::string> matched, primaries;
      for (size_t i = 0; i < members.size(); i++) {
        const std::string& m = members[i];
        if (observed[i] == "primary") primaries.push_back(m);
        if (observed[i] == role_filter) matched.push_back(m);
        else skip(m, "role is " + observed[i] + ", not " + role_filter);
      }
      if (role_filter == "primary" && primaries.size() > 1)
        notes.push_back("more than one member reports itself a primary (" +
                        std::to_string(primaries.size()) + "): this is split "
                        "brain. Every one of them was swept; none was chosen");
      if (matched.empty())
        notes.push_back("no member is currently a " + role_filter +
                        ". During a failover that is the finding, not an empty "
                        "result");
      members = matched;
    }

    if (members.size() > kMaxSweepMembers) {
      for (size_t i = kMaxSweepMembers; i < members.size(); i++)
        skip(members[i], "beyond the " + std::to_string(kMaxSweepMembers) +
                         "-member cap for one sweep");
      members.resize(kMaxSweepMembers);
    }

    json out = json::array();

    // Clearing on the way out has to survive an exception escaping the loop:
    // a sweep config left behind would silently redirect the next
    // single-connection call, which is the kind of bug that only shows up
    // after the call that caused it has been forgotten.
    struct SweepScope {
      std::optional<pglicht::ConnConfig>& slot;
      ~SweepScope() { slot.reset(); }
    } sweep_scope{sweep_cfg_};

    // Members run concurrently, one connection each -- which is the same rule
    // a single-connection call obeys, applied to several connections at once.
    // A thirteen-member replication group used to cost thirteen round trips
    // end to end; it now costs the slowest member.
    //
    // Each worker is its own server object sharing this one's registry and
    // connection cache, so active_, sweep_cfg_ and ext_schemas_ are per worker
    // and none of the 58 query methods had to learn about threads.
    //
    // Results are written to a fixed slot rather than appended, so the payload
    // stays in configuration order however the members finish. The output is
    // byte-identical to the sequential version.
    std::vector<json> slots(members.size());
    {
      const size_t width = std::min<size_t>(members.size(), kSweepConcurrency);
      std::atomic<size_t> next{0};
      auto worker = [&]() {
        PostgresMCPServer w(registry_, cache_);
        for (size_t i = next.fetch_add(1); i < members.size(); i = next.fetch_add(1)) {
          const std::string& m = members[i];
          // Reset first: a member that never connects must not inherit the
          // previous member's role. thread_local, so this is per worker.
          Session::last_observed_role() = "unknown";
          w.active_ = m;
          const auto& cfg = registry_.get(m);
          // Every connect this member makes is bounded, not just the role probe.
          w.sweep_cfg_ = with_connect_timeout(cfg, kSweepConnectTimeoutSeconds);

          json entry = {{"connection", m}};
          if (!cfg.instance.empty())          entry["instance"] = cfg.instance;
          if (!cfg.replication_group.empty()) entry["replication_group"] = cfg.replication_group;

          json result;
          try {
            if (!w.dispatch_tool(tool_name, arguments, result)) {
              entry["error"] = "Tool not found: " + tool_name;
            } else {
              entry["result"] = result;
            }
          } catch (const pqxx::sql_error& e) {
            // A member that ran out of time is a finding about that member, not
            // a failure of the sweep -- and it is the member most worth looking
            // at, so it must not be flattened into the same string as a refused
            // connection.
            if (is_statement_timeout(e))
              entry["error"] = statement_timeout_error(
                Session::last_statement_timeout_ms(), e.what());
            else
              entry["error"] = e.what();
          } catch (const std::exception& e) {
            // One unreachable host must not fail the sweep: a partial answer
            // during an incident beats an exception.
            entry["error"] = e.what();
          }
          entry["role"] = Session::last_observed_role();
          slots[i] = std::move(entry);
        }
      };
      std::vector<std::thread> pool;
      pool.reserve(width);
      for (size_t t = 0; t < width; t++) pool.emplace_back(worker);
      for (auto& th : pool) th.join();
    }
    for (auto& e : slots) out.push_back(std::move(e));
    active_.clear();

    json payload = {
      {"axis", axis},
      {"name", name},
      {"members", out}
    };
    if (!role_filter.empty()) payload["role"] = role_filter;
    if (!skipped.empty())     payload["skipped"] = skipped;
    if (!notes.empty())       payload["notes"] = notes;
    return payload;
  }

  // The tool dispatch chain, lifted out of handle_request so a fan-out sweep
  // can run it once per member. Returns false when the name is unknown.
  //
  // Every branch reads active_, set by the caller. That indirection is the
  // whole reason the ~50 query methods needed no changes to gain fan-out.
  bool dispatch_tool(const std::string& tool_name, const json& arguments,
                     json& result_content) {
    if (tool_name == "listConnections") {
      result_content = connections();
    }
    else if (tool_name == "listTopology") {
      result_content = topology();
    }
    else if (tool_name == "verifyTopology") {
      result_content = verify_topology();
    }
    else if (tool_name == "bufferCacheSummary") {
      result_content = buffer_cache_summary();
    }
    else if (tool_name == "bufferCacheContents") {
      int limit = arguments.contains("limit") && arguments["limit"].is_number_integer()
        ? arguments["limit"].get<int>() : 20;
      result_content = buffer_cache_contents(limit);
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
    else if (tool_name == "evaluateIndex") {
      std::string sql = arguments.contains("sql") ? arguments["sql"].get<std::string>() : "";
      json creates = arguments.contains("create") ? arguments["create"] : json::array();
      json hides   = arguments.contains("hide")   ? arguments["hide"]   : json::array();
      result_content = evaluate_index(sql, creates, hides);
    }
    else if (tool_name == "checkPrivileges") {
      result_content = check_privileges();
    }
    else if (tool_name == "tableStats") {
      std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
      std::string target_table = arguments.contains("table") ? arguments["table"].get<std::string>() : "";
      result_content = table_stats(target_schema, target_table);
    }
    else if (tool_name == "listTableStats") {
      std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
      result_content = list_table_stats(target_schema);
    }
    else if (tool_name == "tableSize") {
      std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
      std::string target_table = arguments.contains("table") ? arguments["table"].get<std::string>() : "";
      result_content = table_size(target_schema, target_table);
    }
    else if (tool_name == "listTableSizes") {
      std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
      result_content = list_table_sizes(target_schema);
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
      int pid = arguments.contains("pid") && arguments["pid"].is_number_integer()
        ? arguments["pid"].get<int>() : 0;
      std::string qid = arguments.contains("query_id") && arguments["query_id"].is_string()
        ? arguments["query_id"].get<std::string>() : "";
      double min_dur = arguments.contains("min_duration_s") && arguments["min_duration_s"].is_number()
        ? arguments["min_duration_s"].get<double>() : 0;
      std::string st = arguments.contains("state") && arguments["state"].is_string()
        ? arguments["state"].get<std::string>() : "";
      result_content = activity(pid, qid, min_dur, st);
    }
    else if (tool_name == "currentLocks") {
      int pid = arguments.contains("pid") && arguments["pid"].is_number_integer()
        ? arguments["pid"].get<int>() : 0;
      result_content = locks(pid);
    }
    else if (tool_name == "replicationSlots") {
      result_content = replication_slots();
    }
    else if (tool_name == "databaseStats") {
      result_content = database_stats();
    }
    else if (tool_name == "statementStats") {
      int limit = arguments.contains("limit") ? arguments["limit"].get<int>() : 20;
      std::string qid;
      if (arguments.contains("query_id")) {
        if (arguments["query_id"].is_string()) qid = arguments["query_id"].get<std::string>();
        else if (arguments["query_id"].is_number_integer())
          qid = std::to_string(arguments["query_id"].get<long long>());
      }
      std::string ord = arguments.contains("order_by") && arguments["order_by"].is_string()
        ? arguments["order_by"].get<std::string>() : "";
      long long min_calls = arguments.contains("min_calls") && arguments["min_calls"].is_number_integer()
        ? arguments["min_calls"].get<long long>() : 0;
      result_content = statement_stats(limit, qid, ord, min_calls);
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
    else if (tool_name == "progressStats") {
      int pid = arguments.contains("pid") && arguments["pid"].is_number_integer()
        ? arguments["pid"].get<int>() : 0;
      std::string rel = arguments.contains("relation") && arguments["relation"].is_string()
        ? arguments["relation"].get<std::string>() : "";
      result_content = progress_stats(pid, rel);
    }
    else if (tool_name == "ioStats") {
      int pid = arguments.contains("pid") && arguments["pid"].is_number_integer()
        ? arguments["pid"].get<int>() : 0;
      std::string bt = arguments.contains("backend_type") && arguments["backend_type"].is_string()
        ? arguments["backend_type"].get<std::string>() : "";
      std::string ob = arguments.contains("object") && arguments["object"].is_string()
        ? arguments["object"].get<std::string>() : "";
      std::string cx = arguments.contains("context") && arguments["context"].is_string()
        ? arguments["context"].get<std::string>() : "";
      result_content = io_stats(pid, bt, ob, cx);
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
    else if (tool_name == "indexBloat") {
      std::string target_schema = arguments.contains("schema") ? arguments["schema"].get<std::string>() : "public";
      std::string target_index  = arguments.contains("index")  ? arguments["index"].get<std::string>()  : "";
      result_content = index_bloat(target_schema, target_index);
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
      return false;
    }
    // Every tool payload is a JSON object, enforced here rather than trusted
    // of ~50 query methods. `structuredContent` may only be an object, so from
    // 4.0.0 a null payload is not merely untidy -- it is unrepresentable in the
    // format modern clients receive. The methods return a value-initialised
    // `json` (which is null, not `{}`) when a query matches no rows, so that is
    // the case this catches. Reading "no rows" as an empty map is also the
    // better answer: null invites "the tool failed", `{}` says "nothing there".
    if (result_content.is_null()) result_content = json::object();
    return true;
  }

  void handle_request(const json& req) {
    std::string method = req.value("method", "");
    const json params = req.contains("params") && req["params"].is_object()
      ? req["params"] : json::object();

    // Era discrimination.
    //
    // The discriminator is the presence of that one _meta key, never the
    // presence of _meta itself. progressToken has lived in _meta since the
    // legacy revisions, so keying on _meta would route any legacy client using
    // progress tokens into the modern path and reject it with -32602 -- a
    // client that worked in 3.1.1 breaking on upgrade for using a legacy
    // feature correctly.
    const json meta = params.contains("_meta") && params["_meta"].is_object()
      ? params["_meta"] : json::object();
    modern_ = meta.contains(kMetaProtocolVersion);

    if (modern_) {
      // Both keys are required on every modern request; a request missing one
      // is malformed rather than defaulted.
      if (!meta[kMetaProtocolVersion].is_string()) {
        send_error(req.value("id", json()), -32602,
                   std::string(kMetaProtocolVersion) + " must be a string");
        return;
      }
      if (!meta.contains(kMetaClientCapabilities)) {
        send_error(req.value("id", json()), -32602,
                   std::string("modern requests must carry ") +
                   kMetaClientCapabilities + " in params._meta");
        return;
      }
      request_protocol_ = meta[kMetaProtocolVersion].get<std::string>();
      bool known = false;
      for (const auto& v : all_protocols()) if (v == request_protocol_) known = true;
      if (!known) {
        send_error_with_data(
          req.value("id", json()), -32022,
          "unsupported protocol version: " + request_protocol_,
          {{"supported", all_protocols()}, {"requested", request_protocol_}});
        return;
      }
    } else {
      // Legacy: whatever the handshake settled on, defaulting to the revision
      // 3.1.1 spoke.
      request_protocol_ = client_protocol_;
    }

    if (method == "server/discover") {
      send_response(req.value("id", json()), cacheable({
          {"resultType", "complete"},
          {"supportedVersions", all_protocols()},
          {"capabilities", {{"tools", json::object()},
                            {"resources", json::object()},
                            {"prompts", json::object()},
                            {"completions", json::object()}}},
          {"instructions", instructions()}
        // Versions, capabilities and instructions are all compiled in, and
        // none of it is specific to a caller.
        }, kTtlStatic, "public"));
      return;
    }

    if (method == "initialize") {
      initialize(req["id"], params);
    }
    else if (method == "notifications/initialized") {
      return;
    }
    else if (method == "tools/list") {
      json out = json::object();
      // "private" rather than "public", for two reasons that both bite. The
      // list varies by negotiated revision -- annotations, title and
      // outputSchema are each gated -- while the cache key is the method and
      // its params, and the revision travels in _meta. And the `connection`
      // property description carries the operator's configured default
      // connection name. Neither belongs in a cache a gateway may serve to
      // another caller. A private cache still gives this client the full
      // benefit, which is where the ~93 kB saving actually lands.
      if (!paginate(get_tools_list(request_protocol_)["tools"], params, "tools", out)) {
        send_error(req["id"], -32602, "invalid cursor");
        return;
      }
      send_response(req["id"], cacheable(out, kTtlStatic, "private"));
    }
    else if (method == "resources/list") {
      json out = json::object();
      // Connection names and a live schema enumeration: deployment-specific,
      // and short-lived because CREATE SCHEMA changes it.
      if (!paginate(get_resources_list()["resources"], params, "resources", out)) {
        send_error(req["id"], -32602, "invalid cursor");
        return;
      }
      send_response(req["id"], cacheable(out, kTtlCatalog, "private"));
    }
    else if (method == "resources/templates/list") {
      json out = json::object();
      if (!paginate(get_resource_templates_list()["resourceTemplates"], params,
                    "resourceTemplates", out)) {
        send_error(req["id"], -32602, "invalid cursor");
        return;
      }
      // Static URI templates, identical for every caller.
      send_response(req["id"], cacheable(out, kTtlStatic, "public"));
    }
    else if (method == "resources/read") {
      const std::string uri = params.value("uri", "");
      try {
        json body = read_resource(uri);
        if (body.is_null()) body = json::object();
        // A resource is a document, so it is served as one: the JSON text of
        // the object, with its mimeType. There is no structuredContent form
        // for resource contents in the spec, and the text is compact for the
        // same reason a tool result is.
        // Structure changes only on DDL, but it does change, so the hint is
        // short. Private: this is the content of the operator's database.
        send_response(req["id"], cacheable({
            {"contents", {{{"uri", uri},
                           {"mimeType", "application/json"},
                           {"text", body.dump()}}}}
          }, kTtlCatalog, "private"));
      } catch (const std::invalid_argument& e) {
        send_error(req["id"], -32602, e.what());
      } catch (const std::exception& e) {
        send_error(req["id"], -32603, std::string("resource read failed: ") + e.what());
      }
    }
    else if (method == "prompts/list") {
      json out = json::object();
      if (!paginate(get_prompts_list()["prompts"], params, "prompts", out)) {
        send_error(req["id"], -32602, "invalid cursor");
        return;
      }
      // Compiled-in templates with no configuration in them.
      send_response(req["id"], cacheable(out, kTtlStatic, "public"));
    }
    else if (method == "prompts/get") {
      try {
        send_response(req["id"], get_prompt(params.value("name", ""),
                                            params.contains("arguments")
                                              ? params["arguments"] : json::object()));
      } catch (const std::invalid_argument& e) {
        send_error(req["id"], -32602, e.what());
      }
    }
    else if (method == "completion/complete") {
      send_response(req["id"], complete(params.contains("ref") ? params["ref"] : json::object(),
                                        params.contains("argument") ? params["argument"]
                                                                    : json::object()));
    }
    else if (method == "tools/call") {
      std::string tool_name = params.value("name", "");
      auto arguments = params.value("arguments", json::object());

      try {
	json result_content;

	// At most one target selector. More than one is a caller error worth
	// naming: silently preferring one would run the sweep the caller did
	// not ask for.
	auto str_arg = [&](const char* k) {
	  return arguments.contains(k) && arguments[k].is_string()
	    ? arguments[k].get<std::string>() : std::string{};
	};
	const std::string want_conn     = str_arg("connection");
	const std::string want_instance = str_arg("instance");
	const std::string want_repl     = str_arg("replication_group");
	const std::string want_group    = str_arg("group");
	const std::string want_role     = str_arg("role");

	std::vector<std::string> given;
	if (!want_conn.empty())     given.push_back("connection");
	if (!want_instance.empty()) given.push_back("instance");
	if (!want_repl.empty())     given.push_back("replication_group");
	if (!want_group.empty())    given.push_back("group");
	if (given.size() > 1) {
	  std::string names;
	  for (const auto& g : given) names += (names.empty() ? "" : ", ") + g;
	  send_error(req["id"], -32602,
		     "at most one target may be given, but got: " + names);
	  return;
	}

	const std::string axis = !want_instance.empty() ? "instance"
			       : !want_repl.empty()     ? "replication_group"
			       : !want_group.empty()    ? "group"
						        : std::string{};

	if (!want_role.empty()) {
	  if (want_role != "primary" && want_role != "replica") {
	    send_error(req["id"], -32602,
		       "role must be \"primary\" or \"replica\", got \"" + want_role + "\"");
	    return;
	  }
	  // Filtering by role only means something across servers. Within one
	  // instance every database has the same role by definition.
	  if (axis != "replication_group" && axis != "group") {
	    send_error(req["id"], -32602,
		       "role applies only to a replication_group or group sweep; "
		       "every database of one instance has the same role");
	    return;
	  }
	}

	if (!axis.empty()) {
	  // members() throws a message listing the configured names, so a typo
	  // fails here rather than as a silently empty sweep.
	  const auto& members = registry_.members(axis, axis == "instance" ? want_instance
					       : axis == "replication_group" ? want_repl
									     : want_group);
	  const std::string why = sweep_rejection(axis, tool_name, members);
	  if (!why.empty()) { send_error(req["id"], -32602, why); return; }

	  result_content = fan_out(axis,
				   axis == "instance" ? want_instance
				 : axis == "replication_group" ? want_repl : want_group,
				   want_role, tool_name, arguments);
	  send_response(req["id"], tool_result(result_content));
	  return;
	}

	// Resolve the target connection once, before dispatch. get() throws a
	// message listing the configured names if this one is unknown, so a
	// typo fails here rather than as a confusing connect error later.
	active_ = want_conn;
	(void)registry_.get(active_);

	if (!dispatch_tool(tool_name, arguments, result_content)) {
	  send_error(req["id"], -32601, "Tool not found: " + tool_name);
	  return;
	}

	send_response(req["id"], tool_result(result_content));

      } catch (const pqxx::sql_error& e) {
	// Hitting the ceiling is reported as a result rather than an execution
	// error: nothing went wrong, the answer just needs longer than this
	// connection allows, and the caller can act on that.
	if (is_statement_timeout(e)) {
	  send_response(req["id"], {
	      // Errors stay a text block in both eras. `structuredContent` is the
	      // format for a tool's answer; an error is a message about why there
	      // is no answer, and the spec pairs isError with content.
	      {"content", {{{"type", "text"},
			    {"text", statement_timeout_error(
			       Session::last_statement_timeout_ms(), e.what()).dump()}}}},
	      {"isError", true}
	    });
	} else {
	  send_response(req["id"], {
	      {"content", {{{"type", "text"}, {"text", std::string("Execution error: ") + e.what()}}}},
	      {"isError", true}
	    });
	}
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

  // ============ Caching hints and pagination (revision 2026-07-28) =========
  //
  // The caching utility requires `ttlMs` and `cacheScope` on every result with
  // resultType "complete" from server/discover, tools/list, prompts/list,
  // resources/list, resources/templates/list and resources/read. Both are
  // therefore gated on modern_, exactly as resultType is: they are defined by
  // the same revision, and a legacy result must not grow fields its era never
  // had.
  //
  // This matters more here than it looks. tools/list is ~93 kB once 58 tools
  // carry descriptions, annotations, titles and outputSchemas, and without a
  // freshness hint a client SHOULD treat it as immediately stale and re-fetch
  // it whenever it needs the list. That is the largest single payload the
  // server produces and it is entirely static.

  // Everything compiled into the binary or read from the config file at
  // startup. None of it can change while the process lives: the registry is
  // built once, the tool table is a static, and no `listChanged` capability is
  // declared, so there is no mechanism by which a client could be told
  // otherwise even if it could.
  static constexpr int kTtlStatic = 3600000;   // 1 hour
  // Anything derived from a live catalog. Structure changes only when someone
  // issues DDL -- that is the whole premise of serving it as a resource -- but
  // DDL does happen, so this is short enough that a CREATE SCHEMA or an ALTER
  // TABLE is picked up on the next access rather than an hour later.
  static constexpr int kTtlCatalog = 60000;    // 1 minute

  // Page size for every paginated list. Deliberately larger than any list this
  // server currently produces, so nothing is truncated for a client that
  // ignores nextCursor: 58 tools, 8 prompts and 5 templates are each one page.
  // The list that can genuinely outgrow it is resources/list, which is
  // connections x schemas -- and that is the case pagination exists for.
  static constexpr size_t kListPageSize = 100;

  static std::string b64_encode(const std::string& in) {
    static const char* t =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    unsigned val = 0;
    int valb = -6;
    for (char raw : in) {
      val = (val << 8) + static_cast<unsigned char>(raw);
      valb += 8;
      while (valb >= 0) { out += t[(val >> static_cast<unsigned>(valb)) & 0x3Fu]; valb -= 6; }
    }
    if (valb > -6) out += t[((val << 8) >> static_cast<unsigned>(valb + 8)) & 0x3Fu];
    while (out.size() % 4) out += '=';
    return out;
  }

  static bool b64_decode(const std::string& in, std::string& out) {
    static const char* t =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::vector<int> rev(256, -1);
    for (int i = 0; i < 64; i++) rev[static_cast<unsigned char>(t[i])] = i;
    unsigned val = 0;
    int valb = -8;
    out.clear();
    for (char raw : in) {
      const unsigned char c = static_cast<unsigned char>(raw);
      if (c == '=') break;
      if (rev[c] == -1) return false;
      val = (val << 6) + static_cast<unsigned>(rev[c]);
      valb += 6;
      if (valb >= 0) {
        out += static_cast<char>((val >> static_cast<unsigned>(valb)) & 0xFFu);
        valb -= 8;
      }
    }
    return true;
  }

  // Opaque by contract: clients MUST NOT parse these. Base64 rather than a
  // bare offset so that a client which ignores that rule fails visibly instead
  // of quietly depending on the encoding.
  static std::string encode_cursor(size_t offset) {
    return b64_encode("pglicht:" + std::to_string(offset));
  }

  static bool decode_cursor(const std::string& cursor, size_t& offset) {
    std::string plain;
    if (!b64_decode(cursor, plain)) return false;
    const std::string prefix = "pglicht:";
    if (plain.rfind(prefix, 0) != 0) return false;
    const std::string digits = plain.substr(prefix.size());
    if (digits.empty()) return false;
    for (char c : digits) if (c < '0' || c > '9') return false;
    try { offset = static_cast<size_t>(std::stoull(digits)); }
    catch (const std::exception&) { return false; }
    return true;
  }

  json cacheable(json result, int ttl_ms, const char* scope) const {
    if (modern_) {
      result["ttlMs"] = ttl_ms;
      result["cacheScope"] = scope;
    }
    return result;
  }

  // Slices one page out of `items` under `key`, attaching nextCursor when more
  // remain. Returns false for a cursor this server did not issue, which the
  // caller turns into -32602 as the spec requires.
  //
  // An absent cursor and an empty-string cursor are deliberately not the same
  // thing: the spec says an empty string is a valid cursor value and must not
  // read as end-of-results, so only `contains` decides whether one was sent.
  static bool paginate(const json& items, const json& params,
                       const char* key, json& out) {
    size_t offset = 0;
    if (params.contains("cursor")) {
      if (!params["cursor"].is_string()) return false;
      if (!decode_cursor(params["cursor"].get<std::string>(), offset)) return false;
      if (offset > items.size()) return false;
    }
    json page = json::array();
    const size_t end = std::min(offset + kListPageSize, items.size());
    for (size_t i = offset; i < end; i++) page.push_back(items[i]);
    out[key] = page;
    if (end < items.size()) out["nextCursor"] = encode_cursor(end);
    return true;
  }

  // The revision that defined `outputSchema` and `structuredContent`.
  //
  // 4.0.0 sent the structured payload *instead of* the text block to any client
  // that negotiated this revision, on the reasoning that sending both
  // serialises the same payload twice and a client feeding results into a
  // model's context may inject both, doubling the tokens. The compatibility the
  // spec's "send both" advice protects was assumed to be covered by the
  // negotiated revision: a client that cannot read structuredContent would not
  // ask for a revision that has it.
  //
  // That assumption was wrong, and a first-party client disproved it on the
  // first day: Claude Code negotiates 2025-06-18 and reads `content`, so it
  // received a result it reported as having no content at all. A client can
  // advertise a revision it does not fully implement, and there is no way for
  // this server to tell.
  //
  // So the default is now what the spec advises -- both -- and the single
  // format is opt-in through PG_LICHT_STRUCTURED_ONLY, for operators who have
  // confirmed their client reads structured content and want the bytes back.
  // The token saving is real, but it is not worth a silently empty answer, and
  // an operator who has verified their client is the only one who can know.
  static constexpr const char* kStructuredContentRevision = "2025-06-18";

  // Opt in to the single-format behaviour 4.0.0 and 4.1.0 had by default.
  static bool structured_only() {
    static const bool v = [] {
      const char* e = std::getenv("PG_LICHT_STRUCTURED_ONLY");
      return e != nullptr && *e != '\0' && std::string(e) != "0";
    }();
    return v;
  }

  bool wants_structured_content() const {
    return request_protocol_ >= kStructuredContentRevision;
  }

  // One success result, in whichever of the two formats the client negotiated.
  //
  // The text form is serialised compactly rather than with dump(2). The
  // indentation was 18-39% of the payload measured across real catalog reads,
  // and it buys a caller nothing: every consumer parses the text as JSON
  // rather than reading it. This is the only remaining path that carries it,
  // since a modern client no longer receives a text block at all.
  json tool_result(const json& payload) const {
    json out;
    const bool structured = wants_structured_content();
    if (structured) out["structuredContent"] = payload;
    // The text block goes to everyone unless an operator has opted out of it.
    // It is still compact: the indentation was 18-39% of the payload and no
    // consumer reads it, since every one parses the text as JSON.
    if (!structured || !structured_only())
      out["content"] = {{{"type", "text"}, {"text", payload.dump()}}};
    out["isError"] = false;
    return out;
  }

  void send_response(const json& id, const json& result) {
    json body = result;
    // Modern results must carry resultType, and should carry serverInfo in
    // _meta. Both are gated: a legacy response has to stay byte-identical to
    // 3.1.1, which is what makes that compatibility provable rather than
    // assumed.
    if (modern_) {
      if (!body.contains("resultType")) body["resultType"] = "complete";
      body["_meta"][kMetaServerInfo] = {
        {"name", "pg-licht-cpp"}, {"version", PGLICHT_VERSION}
      };
    }
    json res = {{"jsonrpc", "2.0"}, {"id", id}, {"result", body}};
    std::cout << res.dump() << std::endl;
  }

  void send_error_with_data(const json& id, int code, const std::string& msg,
                            const json& data) {
    json res = {{"jsonrpc", "2.0"}, {"id", id},
                {"error", {{"code", code}, {"message", msg}, {"data", data}}}};
    std::cout << res.dump() << std::endl;
  }

  void send_error(const json& id, int code, const std::string& msg) {
    json err = {{"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", msg}}}};
    std::cout << err.dump() << std::endl;
  }
};
