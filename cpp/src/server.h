#pragma once

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>
#include <limits>
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
// Templated on the transaction type so a pqxx::subtransaction can use it too;
// both overloads live on transaction_base.
template<typename T, typename P>
inline pqxx::result pqxx_exec(T& txn, const std::string& sql, P&& params) {
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
// is PgBouncer in transaction mode (pool_mode=transaction). There the server
// connection is handed back to the pool at COMMIT and the next transaction may
// land on a different one, so *session* state cannot be relied on between
// transactions -- whether or not the pooler also resets the connection it
// takes back (server_reset_query, which transaction mode skips by default).
// An earlier version set `default_transaction_read_only` once at startup and
// committed; behind such a pooler that setting was silently absent from every
// later call that landed elsewhere, and the read-only guard with it.
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

// Every PostgreSQL feature this server's SQL depends on, named, with the
// release that introduced it.
//
// Before 4.3.0 these were twenty-nine bare comparisons against 150000,
// 160000, 170000 and 180000, scattered through five thousand lines of query
// methods. Supporting a new major meant grepping for magic numbers and
// trusting that the grep was complete; and the numbers said which release
// without ever saying which *feature*, so a reader had to reconstruct the
// reason from the SQL beside them.
//
// Adding PostgreSQL 19 is now reading one table. So is answering "what does
// this server lack", which nothing could previously ask.
enum class Feature {
  // 15
  SubTwoPhase,              // pg_subscription.subtwophasestate
  SubscriptionStatsView,    // pg_stat_subscription_stats
  ExtendedStatsInherit,     // pg_statistic_ext_data.stxdinherit
  PublicationSchemas,       // pg_publication_namespace (FOR TABLES IN SCHEMA)
  ParameterAcl,             // pg_parameter_acl (GRANT SET ON PARAMETER)
  // 16
  PgStatIo,                 // the pg_stat_io view
  GenericPlan,              // EXPLAIN (GENERIC_PLAN)
  BufferCacheSummary,       // pg_buffercache 1.4's summary functions
  SlotConflicting,          // pg_replication_slots.conflicting
  SubOrigin,                // pg_subscription.suborigin
  SubLeaderPid,             // pg_stat_subscription.leader_pid
  TableStatsSeqScanDetail,  // n_tup_newpage_upd, last_seq_scan
  IndexLastScan,            // pg_stat_all_indexes.last_idx_scan
  // 17
  WaitEventDescriptions,    // pg_wait_events
  SlotInvalidationReason,   // pg_replication_slots.invalidation_reason
  StatementStatsSince,      // pg_stat_statements.stats_since
  VacuumProgressBytes,      // dead tuple counts became byte counts
  Checkpointer,             // pg_stat_checkpointer split from pg_stat_bgwriter
  SubFailover,              // pg_subscription.subfailover
  SubWorkerType,            // pg_stat_subscription.worker_type
  MaintainPrivilege,        // the MAINTAIN table privilege
  // 18
  ActivityParallelWorkers,  // parallel_workers_to_launch / _launched
  StatementStatsWalBuffers, // wal_buffers_full and the parallel columns
  BackendIo,                // pg_stat_get_backend_io
  IoByteCounters,           // read_bytes / write_bytes / extend_bytes
  RelAllFrozen,             // pg_class.relallfrozen, total_vacuum_time
  WalIoMovedToPgStatIo,     // pg_stat_wal lost wal_write/wal_sync here
  CheckpointerNumDone,      // pg_stat_checkpointer.num_done, slru_written
  VacuumDelayTime,          // pg_stat_progress_vacuum.delay_time
  SubConflictCounters,      // the seven confl_* counters
};

constexpr int feature_since(Feature f) {
  switch (f) {
    case Feature::SubTwoPhase:
    case Feature::SubscriptionStatsView:
    case Feature::ExtendedStatsInherit:
    case Feature::PublicationSchemas:
    case Feature::ParameterAcl:            return 150000;

    case Feature::PgStatIo:
    case Feature::GenericPlan:
    case Feature::BufferCacheSummary:
    case Feature::SlotConflicting:
    case Feature::SubOrigin:
    case Feature::SubLeaderPid:
    case Feature::TableStatsSeqScanDetail:
    case Feature::IndexLastScan:            return 160000;

    case Feature::WaitEventDescriptions:
    case Feature::SlotInvalidationReason:
    case Feature::StatementStatsSince:
    case Feature::VacuumProgressBytes:
    case Feature::Checkpointer:
    case Feature::SubFailover:
    case Feature::SubWorkerType:
    case Feature::MaintainPrivilege:        return 170000;

    case Feature::ActivityParallelWorkers:
    case Feature::StatementStatsWalBuffers:
    case Feature::BackendIo:
    case Feature::IoByteCounters:
    case Feature::RelAllFrozen:
    case Feature::WalIoMovedToPgStatIo:
    case Feature::CheckpointerNumDone:
    case Feature::VacuumDelayTime:
    case Feature::SubConflictCounters:      return 180000;
  }
  return 0;  // unreachable; every enumerator is listed above
}

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

  // Does this server have the feature, rather than "is this server new
  // enough" -- the call site then says why it is branching.
  bool has(Feature f) const { return server_version() >= feature_since(f); }

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

// Typed access to a `tools/call` arguments object.
//
// Before 4.3.0 every tool re-typed its own extraction, and the schema default
// was written out longhand once per tool -- twenty-four identical copies of
// `arguments.contains("schema") ? arguments["schema"].get<std::string>() :
// "public"`. One of them differing from the others would have been invisible.
//
// The semantics are deliberately the ones that were already there rather than
// stricter ones: a missing key yields the default, and a key of the wrong type
// still throws, because handle_request already turns that into -32602 and
// silently substituting a default would hide a caller's bug.
class Args {
public:
  explicit Args(const json& a) : a_(a) {}

  bool contains(const char* k) const { return a_.contains(k); }
  const json& operator[](const char* k) const { return a_[k]; }
  const json& raw() const { return a_; }

  std::string str(const char* k, const char* d = "") const {
    return a_.contains(k) ? a_[k].get<std::string>() : std::string(d);
  }
  // Integers check their type rather than throwing: `limit` is the argument a
  // model is most likely to send as a string, and a default is a better answer
  // there than an error about JSON types.
  int num(const char* k, int d) const {
    return a_.contains(k) && a_[k].is_number_integer() ? a_[k].get<int>() : d;
  }
  long long bignum(const char* k, long long d) const {
    return a_.contains(k) && a_[k].is_number_integer() ? a_[k].get<long long>() : d;
  }
  bool flag(const char* k, bool d) const {
    return a_.contains(k) ? a_[k].get<bool>() : d;
  }
  json arr(const char* k) const {
    return a_.contains(k) ? a_[k] : json::array();
  }
  json obj(const char* k) const {
    return a_.contains(k) ? a_[k] : json::object();
  }

private:
  const json& a_;
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
  const json call_list_partitions(const std::string& schema) { return list_partitions(schema); }
  const json call_role_dependencies(const std::string& r) { return role_dependencies(r); }
  const json call_default_privileges() { return default_privileges(""); }
  const json call_default_privileges(const std::string& sc) { return default_privileges(sc); }
  const json call_large_objects() { return large_objects(); }
  const json call_replication_stats() { return replication_stats(); }
  const json call_partition_details(const std::string& sc, const std::string& t) {
    return partition_details(sc, t);
  }
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
  const json call_roles() { return roles(""); }
  const json call_roles(const std::string& pattern) { return roles(pattern); }
  const json call_foreign_tables(const std::string& schema) { return foreign_tables(schema); }
  const json call_foreign_servers() { return foreign_servers(); }
  const json call_tablespaces() { return tablespaces(); }
  const json call_collations(const std::string& schema) { return collations(schema); }
  const json call_event_triggers() { return event_triggers(); }
  const json call_publications() { return publications(); }
  const json call_subscriptions() { return subscriptions(); }
  const json call_subscription_stats() { return subscription_stats(); }
  const json call_disk_usage() { return disk_usage(); }
  const json call_column_histogram(const std::string& sc, const std::string& t,
                                   const std::string& c) { return column_histogram(sc, t, c); }
  const json call_check_role_access(const std::string& r, const std::string& sc,
                                    const std::string& o) { return check_role_access(r, sc, o); }
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
  const json call_server_settings() { return server_settings("", false); }
  const json call_server_settings(const std::string& p, bool all) { return server_settings(p, all); }
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
    return evaluate_index(sql, create_defs, hide_names, json::object(), "");
  }
  const json call_buffer_cache_summary() { return buffer_cache_summary(); }
  const json call_buffer_cache_contents(int limit) { return buffer_cache_contents(limit); }
  const json call_explain_query(const std::string& queryid, const std::string& sql,
                                const json& params, bool analyze, int timeout_ms) {
    return explain_query(queryid, sql, params, analyze, timeout_ms, json::object(), "");
  }
  const json call_explain_query(const std::string& queryid, const std::string& sql,
                                const json& params, bool analyze, int timeout_ms,
                                const json& settings, const std::string& as_role) {
    return explain_query(queryid, sql, params, analyze, timeout_ms, settings, as_role);
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

  // How many role or database names travel with a collapsed override group
  // before the count stands in for them. Small on purpose: the names are
  // useful for the handful of roles that differ, and the reason this exists at
  // all is that an unbounded list of them exceeded the client's payload limit.
  static constexpr int kOverrideNames = 5;

  // How many relation names listSchemas carries per schema before table_count
  // stands in for them. listTables is the tool that names every table in one
  // schema; this one summarises every schema, and a full name list per schema
  // is quadratic in exactly the direction that breaks.
  static constexpr int kSchemaTableNames = 25;

  // How many roles listRoles returns without a pattern. Ordered so the ones
  // carrying an attribute or a membership come first, because those are the
  // ones a cap must not drop.
  static constexpr int kRoleLimit = 200;

  // How many member tables listPublications names per publication. A
  // publication FOR ALL TABLES expands to the whole database, so the list is
  // unbounded by construction and the count is the answer that scales.
  static constexpr int kPublicationTableNames = 50;

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
      // The directories belong to the postmaster, not to a database.
      {"diskUsage",             {false, true,  false, false}},
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
      // Roles are cluster-wide but the grants asked about are per object,
      // so the answer varies by database and not across a replication group.
      {"checkRoleAccess",       {true,  false, false, false}},
      // pg_statistic is an ordinary catalog and is replicated, so a physical
      // replica holds the primary's histogram byte for byte.
      {"columnHistogram",       {true,  false, false, false}},
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
      // partitionDetails carries per-partition vacuum state and scan
      // counters, so it splits the way tableStats does: the structure is
      // replicated, the counters beside it are not, and vacuum only runs on
      // the primary. listPartitions reads reltuples and relpages, which are
      // catalog columns and therefore identical on a physical replica.
      {"partitionDetails",      {true,  true,  true,  false}},
      {"listPartitions",        {true,  false, false, false}},
      // pg_shdepend and pg_default_acl are catalogs, replicated byte for
      // byte, so a replication_group sweep would return the same answer per
      // member. pg_shdepend is additionally shared across the cluster, so
      // the count it reports is already cluster-wide from any database --
      // but the NAMES are per database, which is why it is per_database.
      {"roleDependencies",      {true,  false, false, false}},
      {"defaultPrivileges",     {true,  false, false, false}},
      {"largeObjects",          {true,  false, false, false}},
      // Every member of a replication group has its own WAL senders, and a
      // replica that is itself a sender is exactly what this finds, so this
      // is worth asking of each server rather than one: per_server. Both
      // pg_stat_replication and the replication origins are instance-wide --
      // pg_replication_origin is a shared catalog -- so every database on one
      // postmaster returns the same rows: not per_database. And a cascading
      // standby's senders are real, not vacuum-side noise: not
      // primary_authoritative. The first version had all three the other way
      // round, which refused the replication-group sweep this tool exists
      // for and let an instance sweep repeat one answer per database.
      {"replicationStats",      {false, true,  false, false}},
      {"tableStats",            {true,  true,  true,  false}},
      // Same row, and for the same reason. pg_subscription is a shared catalog
      // scoped by subdbid, so the answer is per database; the workers, their
      // message ages and the error counters are each server's own; and apply
      // only runs where the subscription does, which is the primary. A replica
      // of a subscriber has no apply worker of its own to report.
      {"subscriptionStats",     {true,  true,  true,  false}},
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
      if (seg[2] == "roles")      return roles("");
      if (seg[2] == "extensions") return extensions();
      if (seg[2] == "settings")   return server_settings("", false);
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

  static const std::vector<PromptDef>& prompt_defs();

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
  //      schema would have to be re-proved on five majors for 62 tools.
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
      {"diskUsage",              schema_fixed("What PostgreSQL is holding on disk, by directory and by "
                                              "tablespace. Never how much room is left.",
                                              {{"wal", "object"}, {"archive_status", "object"},
                                               {"temp_files", "object"}, {"log_files", "object"},
                                               {"tablespaces", "object"}, {"databases", "object"},
                                               {"note", "string"}})},
      {"columnHistogram",        schema_fixed("The full value distribution of one column: the "
                                              "most common values and the histogram of what is left.",
                                              {{"schema", "string"}, {"table", "string"}, {"column", "string"},
                                               {"null_frac", "number"}, {"avg_width", "integer"},
                                               {"n_distinct", "number"},
                                               {"physical_order_correlation", "number"},
                                               {"most_common_vals", "array"}, {"most_common_freqs", "array"},
                                               {"histogram_bounds", "array"}, {"histogram_buckets", "integer"},
                                               {"statistics_target", "object"}, {"inherited", "boolean"},
                                               {"note", "string"}})},
      {"checkRoleAccess",        schema_fixed("Whether one role holds privileges on one object, "
                                              "as PostgreSQL itself evaluates it.",
                                              {{"role", "object"}, {"object", "object"},
                                               {"database_access", "object"}, {"schema_access", "object"},
                                               {"privileges", "object"}, {"columns", "object"},
                                               {"row_level_security", "object"},
                                               {"overloads", "array"}, {"note", "string"}})},
      {"listSubscriptions",      schema_map("subscription name", "its publication, slot and state")},
      {"subscriptionStats",      schema_map("subscription name", "its worker state, table sync progress and error counters")},
      {"replicationSlots",       schema_map("slot name", "its type, activity and retained WAL")},

      // --- statistics keyed by object ---
      {"listTableStats",         schema_map("table name", "that table's statistics counters and size estimate")},
      {"listPartitions",         schema_map("partitioned table name", "its strategy, key, partition count and default partition")},
      {"defaultPrivileges",      schema_fixed("Default privileges: what grants the NEXT object gets.",
                                   {{"default_privileges", "array"}})},
      {"largeObjects",           schema_fixed("Large objects, which no size tool can see.",
                                   {{"total", "integer"}, {"by_owner", "object"}, {"note", "string"}})},
      {"roleDependencies",       schema_fixed("What depends on one role, cluster-wide.",
                                   {{"role", "string"}, {"exists", "boolean"}, {"total", "integer"},
                                    {"by_kind", "object"}, {"by_database", "object"},
                                    {"resolvable_in_this_database", "integer"}, {"objects", "array"}})},
      {"replicationStats",       schema_fixed("WAL senders and replication origins.",
                                   {{"replication", "object"}, {"origins", "object"}, {"note", "string"}})},
      {"listTableSizes",         schema_map("table name", "that table's measured size, index size and total")},
      {"tableIOStats",           schema_map("schema-qualified table name", "its buffer cache hit ratios and scan counts")},
      {"databaseStats",          schema_map("database name", "that database's pg_stat_database counters")},
      {"currentActivity",        schema_map("backend pid", "that backend's state, query and wait event")},
      {"serverSettings",         schema_map("settings category", "a map of setting name to its value and metadata")},
      {"bufferCacheContents",    schema_map("schema-qualified relation name", "its buffered pages and usage counts")},

      // --- fixed shapes ---
      {"partitionDetails",       schema_fixed("One partitioned table and every partition it has.",
                                   {{"table", "string"}, {"strategy", "string"}, {"key", "string"},
                                    {"is_partition_of", "string"}, {"counters_since", "string"},
                                    {"partitions", "array"}})},
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

  // ------------------------------------------------------------------
  // The tool registry.
  //
  // One row per tool, carrying the four facts that must agree: the name, the
  // description the model reads, the schema of what it accepts, and the code
  // that runs it. Until 4.3.0 those lived in two tables five thousand lines
  // apart -- a JSON literal in get_tools_list and an if/else chain in
  // dispatch_tool -- with nothing making them agree. They did agree, as it
  // happens: the split was verified 62/62 with no orphan on either side when
  // this table was generated from them. Nothing had guaranteed that, and a
  // tool advertised but not dispatchable would have been a -32601 at runtime
  // with a clean build behind it.
  //
  // Everything else about a tool is still derived rather than stored here:
  // the fan-out scope from tool_scopes(), the output schema from
  // structured_schemas(), the title from the name, and the connection and
  // sweep arguments injected by get_tools_list. Those are computed per
  // protocol revision, so they cannot be constants in this table.
  struct ToolDef {
    const char* name;
    const char* description;
    json (*input_schema)();
    json (*run)(PostgresMCPServer&, const Args&);
  };

  static const std::vector<ToolDef>& tool_defs();

  static const std::unordered_map<std::string, const ToolDef*>& tool_index() {
    static const std::unordered_map<std::string, const ToolDef*> ix = [] {
      std::unordered_map<std::string, const ToolDef*> m;
      for (const ToolDef& d : tool_defs()) m.emplace(d.name, &d);
      return m;
    }();
    return ix;
  }

  const json get_tools_list(const std::string& protocol) {
    json list = {{"tools", json::array()}};
    for (const ToolDef& d : tool_defs())
      list["tools"].push_back({{"name",        d.name},
                               {"description", d.description},
                               {"inputSchema", d.input_schema()}});

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

  // An empty result from a schema-scoped tool has two causes that look
  // identical and mean opposite things: the schema is empty, or it does not
  // exist. bloat-and-vacuum-review defaults to "public", and on a database
  // that has no public schema a caller following that default got {} and would
  // reasonably conclude the schema is clean. Silence that reads as a healthy
  // answer is the defect this release keeps finding, so it is named instead.
  json no_such_schema(pqxx::work& txn, const std::string& schema) {
    pqxx::result r = pqxx_exec(
      txn, "SELECT 1 FROM pg_namespace WHERE nspname = $1", pqxx::params{schema});
    if (!r.empty()) return {};
    return {
      {"error", "no such schema: \"" + schema + "\""},
      {"hint", "listSchemas names every schema in this database. Names are case "
               "sensitive here exactly as they are in the catalog, and a "
               "database need not have a schema called \"public\" -- several "
               "tools default to it, so an unexpected empty answer is worth "
               "checking against listSchemas first."}
    };
  }

  const json schemas() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // `tables` was every relation name in every schema. On a 24-schema
    // database where one schema holds 1,816 tables that is an 89 kB payload
    // over the client's limit, and the tool returns nothing -- which is the
    // unbounded-payload defect §12 already had recorded against this exact
    // tool. The count is what the summary question needs ("how big is this
    // schema"); listTables is the tool that names them, and it takes one
    // schema at a time precisely so its size is bounded by the caller.
    std::string query = std::string(R"(
      SELECT JSONB_OBJECT_AGG(nspname,
              JSONB_BUILD_OBJECT(
               'table_count', table_count,
               'tables', relnames,
               'tables_truncated', table_count > )") + std::to_string(kSchemaTableNames) + R"(,
               'roles', COALESCE(roles, '{}'::jsonb)))
      FROM pg_namespace
      LEFT JOIN LATERAL (SELECT count(*) AS table_count,
                                to_jsonb((array_agg(relname ORDER BY relname))[1:)"
                              + std::to_string(kSchemaTableNames) + R"(]) AS relnames
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
        AND table_count > 0;
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
      json missing = no_such_schema(txn, schema);
      return missing.is_null() ? json::object() : missing;
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

  // By default only settings that differ from their built-in default, which is
  // the set that describes THIS server rather than PostgreSQL. all=true
  // restores the full dump.
  //
  // Measured on a real cluster: 456 settings, 99 kB, past the client's payload
  // limit -- so the tool returned nothing, and the ~40 settings that actually
  // describe the machine went with the 400 that describe the software. This is
  // the same set EXPLAIN (SETTINGS) reports, and for the same reason.
  const json server_settings(const std::string& pattern, bool all) {
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
        WHERE ($1 = '' OR name ILIKE '%' || $1 || '%' OR category ILIKE '%' || $1 || '%')
          AND ($2 OR source <> 'default' OR setting IS DISTINCT FROM boot_val)
        GROUP BY category
      ) s;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{pattern, all});

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
    const std::string wait_desc = sess.has(Feature::WaitEventDescriptions)
      ? ", 'wait_event_description', we.description" : "";
    const std::string wait_join = sess.has(Feature::WaitEventDescriptions)
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
    const std::string conflicting = sess.has(Feature::SlotConflicting)
      ? ", 'conflicting', s.conflicting" : "";
    const std::string invalidation = sess.has(Feature::SlotInvalidationReason)
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
    const std::string parallel = sess.has(Feature::ActivityParallelWorkers)
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
    const std::string since = sess.has(Feature::StatementStatsSince)
      ? ", 'stats_since', pss.stats_since, 'minmax_stats_since', pss.minmax_stats_since"
      : "";
    const std::string pg18 = sess.has(Feature::StatementStatsWalBuffers)
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
    const bool v17 = sess.has(Feature::VacuumProgressBytes);
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

    // delay_time (PostgreSQL 18) is the total time this vacuum has spent
    // sleeping on the cost-based delay. It answers "is autovacuum being
    // throttled" directly, where bloat-and-vacuum-review previously had to
    // infer it from timestamps and total_autovacuum_time -- and a vacuum that
    // is 90% asleep looks identical, in blocks scanned per second, to one on a
    // slow disk. Reported beside elapsed_s so the ratio is available without a
    // second call; the two together are what separate a throttle from a
    // bottleneck.
    const std::string vacuum_delay = sess.has(Feature::VacuumDelayTime)
      ? R"(, 'delay_time_ms', round(v.delay_time::numeric, 1),
            'delay_percent',
              round((100.0 * v.delay_time
                     / NULLIF(EXTRACT(EPOCH FROM now() - a.query_start) * 1000, 0))::numeric, 1))"
      : "";

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
                   )" + vacuum_dead + vacuum_delay + R"())
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
    if (!sess.has(Feature::PgStatIo)) {
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
    if (pid > 0 && !sess.has(Feature::BackendIo)) {
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
    const std::string bytes = sess.has(Feature::IoByteCounters)
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

    // The two thresholds PostgreSQL itself uses, from varsup.c's
    // SetTransactionIdLimit():
    //
    //   xidWrapLimit = oldest_datfrozenxid + (MaxTransactionId >> 1)  // 2^31-1
    //   xidStopLimit = xidWrapLimit - 3000000
    //   xidWarnLimit = xidWrapLimit - 40000000
    //
    // so the age at which the server refuses to assign new transaction ids is
    // 2147483647 - 3000000 = 2144483647, and the age at which it starts
    // warning in the log is 2147483647 - 40000000 = 2107483647. Both are
    // outage thresholds and both are distinct from autovacuum_freeze_max_age,
    // which is merely where an anti-wraparound autovacuum is forced.
    //
    // Through 4.2.0 this used 2146483647 -- a 1,000,000 delta PostgreSQL has
    // not used for many releases -- which reported two million transactions of
    // headroom that did not exist, in the direction that reads as safe. The
    // warn limit was not reported at all, so the one threshold an operator has
    // already seen fire in the log was the one this tool could not show them.
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
    const std::string pg18_cols = sess.has(Feature::RelAllFrozen)
      ? R"(, 'frozen_percent',
              round(100.0 * r.relallfrozen / NULLIF(r.relpages, 0), 1),
            'relallfrozen', r.relallfrozen,
            'total_vacuum_time_ms', r.total_vacuum_time,
            'total_autovacuum_time_ms', r.total_autovacuum_time)"
      : "";
    const std::string pg18_sel = sess.has(Feature::RelAllFrozen)
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
          || JSONB_BUILD_OBJECT('wraparound_limit', 2144483647::bigint,
                                'wraparound_warn_limit', 2107483647::bigint),
        'databases',
          (SELECT JSONB_OBJECT_AGG(d.datname, JSONB_BUILD_OBJECT(
                    'xid_age', age(d.datfrozenxid),
                    'xid_percent_of_freeze_max_age',
                      round(100.0 * age(d.datfrozenxid)
                            / NULLIF(current_setting('autovacuum_freeze_max_age')::bigint, 0), 1),
                    'xid_percent_of_wraparound_limit',
                      round(100.0 * age(d.datfrozenxid) / 2144483647, 3),
                    'xids_until_wraparound_limit', 2144483647 - age(d.datfrozenxid),
                    'xids_until_warn_limit', 2107483647 - age(d.datfrozenxid),
                    'mxid_age', mxid_age(d.datminmxid),
                    'mxid_percent_of_freeze_max_age',
                      round(100.0 * mxid_age(d.datminmxid)
                            / NULLIF(current_setting('autovacuum_multixact_freeze_max_age')::bigint, 0), 1),
                    -- Multixacts have the same 3,000,000 stop limit as xids and
                    -- the same 40,000,000 warning, and members-space exhaustion
                    -- is its own incident with its own ceiling. Both documents
                    -- said "each age as a percentage of ... the hard limit"
                    -- while only the xid half was reported.
                    'mxid_percent_of_wraparound_limit',
                      round(100.0 * mxid_age(d.datminmxid) / 2144483647, 3),
                    'mxids_until_wraparound_limit', 2144483647 - mxid_age(d.datminmxid),
                    'mxids_until_warn_limit', 2107483647 - mxid_age(d.datminmxid),
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
                     round(100.0 * r.xid_age / 2144483647, 3),
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
    std::string query = std::string(R"(
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
      SELECT JSONB_BUILD_OBJECT()" + std::string(kCountersSince) + R"(,
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
    )");

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
    const bool split = sess.has(Feature::Checkpointer);

    // pg_stat_wal lost wal_write/wal_sync and their timings in PostgreSQL 18,
    // where they moved to pg_stat_io. The remaining four columns exist on
    // every supported major.
    const std::string wal_timing = !sess.has(Feature::WalIoMovedToPgStatIo)
      ? R"(, 'wal_write', w.wal_write,
            'wal_sync', w.wal_sync,
            'wal_write_time_ms', w.wal_write_time,
            'wal_sync_time_ms', w.wal_sync_time)"
      : "";

    // num_done and slru_written are PostgreSQL 18 additions to
    // pg_stat_checkpointer; the rest of the view is unchanged since 17.
    const std::string ckpt_pg18 = sess.has(Feature::CheckpointerNumDone)
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
    std::string query = std::string(R"(
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
      ),
      -- pg_db_role_setting is a whole configuration layer pg_settings cannot
      -- show: pg_settings reports the value for THIS session, so an
      -- `ALTER ROLE app SET work_mem` or `ALTER DATABASE reporting SET ...` is
      -- invisible to anyone connected as someone else. Through 4.2.0 the
      -- worst case below was computed from the global work_mem alone, so on
      -- any server where the application role carries a larger one the figure
      -- was understated -- wrong in the direction that reads as safe.
      --
      -- setdatabase = 0 means "all databases", setrole = 0 means "all roles",
      -- so the four combinations are the four scopes.
      ovr AS (
        SELECT COALESCE(d.datname, '')                       AS database,
               COALESCE(r.rolname, '')                       AS role,
               split_part(cfg, '=', 1)                       AS name,
               substr(cfg, strpos(cfg, '=') + 1)             AS value
          FROM pg_db_role_setting AS s
          LEFT JOIN pg_database AS d ON d.oid = s.setdatabase
          LEFT JOIN pg_roles    AS r ON r.oid = s.setrole
          CROSS JOIN LATERAL unnest(s.setconfig) AS cfg
      ),
      -- work_mem's unit is kB, so a bare number is kilobytes; anything with a
      -- suffix is what pg_size_bytes understands. An unparseable value is
      -- skipped rather than guessed at.
      wm AS (
        SELECT max(CASE WHEN value ~ '^[0-9]+$' THEN value::bigint * 1024
                        WHEN value ~ '^[0-9]+\s*[kMGT]B$' THEN pg_size_bytes(value)
                   END) AS max_bytes
          FROM ovr WHERE name = 'work_mem'
      )
      SELECT JSONB_BUILD_OBJECT(
        'server', JSONB_BUILD_OBJECT(
          'version', current_setting('server_version'),
          'database', current_database()),
        -- Every per-role and per-database override, not only work_mem: a
        -- statement_timeout of 0 on one role explains as much as a memory
        -- setting does, and none of it is visible in `settings` above.
        --
        -- Collapsed by (scope, name, value), which is the only grouping that
        -- loses nothing: two roles with DIFFERENT values stay separate rows,
        -- and the whole reason to read this is to find a value that differs
        -- from the global one. What collapses is repetition -- 4.2.1 returned
        -- one row per role and on a multi-tenant cluster that is one row per
        -- tenant. Measured on a real 723-role database: 701 rows, 698 of them
        -- the same search_path, 105 kB, over the client's payload limit, so
        -- the tool returned nothing at all and took capacity-check and
        -- triage-active-sessions down with it. The three overrides that
        -- mattered were in the other 3 rows.
        --
        -- roles/databases carry the names up to kOverrideNames, then the count
        -- stands in for them. A count is what the question actually needs
        -- ("does anything override work_mem, and how much of the fleet"), and
        -- an unbounded name list is how this broke in the first place.
        'overrides', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'scope',     g.scope,
                   'name',      g.name,
                   'value',     g.value,
                   'count',     g.n,
                   'roles',     g.roles,
                   'databases', g.databases,
                   'names_truncated', g.n > )" + std::to_string(kOverrideNames) + R"()
                 ORDER BY g.name, g.value)
            FROM (
              SELECT CASE WHEN database = '' AND role = '' THEN 'cluster'
                          WHEN database = '' THEN 'role'
                          WHEN role = ''     THEN 'database'
                          ELSE 'role_in_database' END               AS scope,
                     name,
                     value,
                     count(*)                                       AS n,
                     to_jsonb((array_remove(array_agg(NULLIF(role, '')
                                            ORDER BY role), NULL)
                              )[1:)" + std::to_string(kOverrideNames) + R"(])     AS roles,
                     to_jsonb((array_remove(array_agg(DISTINCT NULLIF(database, '')), NULL)
                              )[1:)" + std::to_string(kOverrideNames) + R"(])     AS databases
                FROM ovr
               GROUP BY 1, 2, 3) AS g), '[]'::jsonb),
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
          -- GREATEST of the global work_mem and any override, because the
          -- worst case is what the largest-configured role can do, not what
          -- the current session happens to be set to.
          'work_mem_effective_max_bytes', GREATEST(v.work_mem, wm.max_bytes),
          'work_mem_is_overridden', wm.max_bytes IS NOT NULL
                                    AND wm.max_bytes > v.work_mem,
          'committed_worst_case_bytes',
            v.shared_buffers + GREATEST(v.work_mem, wm.max_bytes) * v.max_connections
              + v.maint_work_mem * v.av_workers,
          'committed_worst_case_percent_of_ram',
            round(100.0 * (v.shared_buffers
                           + GREATEST(v.work_mem, wm.max_bytes) * v.max_connections
                           + v.maint_work_mem * v.av_workers)
                  / NULLIF(host.ram_bytes, 0), 1),
          'max_parallel_workers_per_vcpu',
            round((SELECT setting::numeric FROM g WHERE name = 'max_parallel_workers')
                  / NULLIF(host.vcpus, 0), 2),
          'max_worker_processes_per_vcpu',
            round((SELECT setting::numeric FROM g WHERE name = 'max_worker_processes')
                  / NULLIF(host.vcpus, 0), 2))
          FROM v, host, wm),
        'notes', JSONB_BUILD_ARRAY(
          'work_mem is a per-node limit, not a per-connection one: a single query '
          'with several sorts or hash joins can use a multiple of it, and parallel '
          'workers each get their own. work_mem_times_max_connections is therefore a '
          'floor on the worst case, not a ceiling.',
          'shared_buffers is counted once here; the operating system page cache is '
          'not, which is what effective_cache_size is meant to describe.',
          'settings shows this session''s values. overrides carries what '
          'pg_db_role_setting holds for other roles and databases, which pg_settings '
          'cannot show and which is the usual answer to "slow only from the '
          'application". committed_worst_case uses the largest work_mem any role is '
          'configured with, not this session''s.')
      );
    )");

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

  // --- the execution budget for EXPLAIN ANALYZE under caller settings ---
  //
  // settings exists to change a PLAN, and for planning that is safe. analyze
  // EXECUTES, and then work_mem, hash_mem_multiplier and the parallelism knobs
  // set the resource footprint of a statement that really runs. The read-only
  // guard and the 30s timeout bound what it writes and how long it runs, and
  // neither bounds memory: work_mem goes to 2TB per sort or hash node and
  // hash_mem_multiplier to 1000. A hash join at those values can exhaust RAM
  // in seconds, and an OOM kill of one backend restarts every connection on
  // the instance. That is an outage caused by a read-only tool call.
  //
  // So caller-supplied settings may be EXECUTED only within a budget taken
  // from the host capacity the operator declared for this connection --
  // host_ram_mb and host_vcpus, per connection, inherited from its
  // [instance:...] section, or from the environment. Never from a tool
  // argument: a caller must not be able to raise its own limit. With either
  // figure absent there is nothing to bound against, and no settings change is
  // executed at all; the plan built under the settings is still returned.
  //
  // plan_as_role alone is not budgeted. It applies what that role already runs
  // with in production, so it cannot exceed production's own footprint.
  static constexpr long long kAnalyzeRamShareDivisor = 10;  // a tenth of RAM
  static constexpr int kAnalyzeVcpusPerWorker = 4;          // 1 worker / 4 vCPUs

  struct ExecFootprint {
    double worst_bytes = 0;   // double: work_mem x 1000 x participants overflows int64
    int memory_nodes = 0;
    int workers = 0;
  };

  // The worst case the executor may allocate for a plan, from the plan's own
  // shape. Each memory-using node is limited to work_mem, or to hash_mem
  // (work_mem x hash_mem_multiplier) for a hash table, and every process that
  // runs the node gets its own allowance: under a Gather that is the planned
  // workers plus the leader. A Parallel Hash shares one table, but its size
  // limit is also hash_mem x participants, so the same product holds.
  //
  // An upper bound on what the limits permit, not a prediction -- a node that
  // never reaches its limit uses less. Deliberately conservative where the
  // plan cannot say: every CTE Scan is counted, though scans of one CTE share
  // a tuplestore.
  static void plan_footprint(const json& node, double work_mem, double hash_mem,
                             double participants, ExecFootprint& fp) {
    if (!node.is_object()) return;
    const std::string type  = node.value("Node Type", "");
    const std::string strat = node.value("Strategy", "");
    const bool hashed =
        type == "Hash" || type == "Memoize" || type == "Recursive Union" ||
        ((type == "Aggregate" || type == "SetOp") &&
         (strat == "Hashed" || strat == "Mixed"));
    const bool bounded_by_work_mem =
        type == "Sort" || type == "Incremental Sort" || type == "Materialize" ||
        type == "WindowAgg" || type == "Function Scan" ||
        type == "Table Function Scan" || type == "CTE Scan" ||
        type == "Bitmap Heap Scan";
    if (hashed)                   { fp.worst_bytes += hash_mem * participants; fp.memory_nodes++; }
    else if (bounded_by_work_mem) { fp.worst_bytes += work_mem * participants; fp.memory_nodes++; }

    double below = participants;
    if (type == "Gather" || type == "Gather Merge") {
      const int w = node.value("Workers Planned", 0);
      fp.workers += w;
      below = participants * (w + 1);
    }
    if (node.contains("Plans") && node["Plans"].is_array())
      for (const auto& child : node["Plans"])
        plan_footprint(child, work_mem, hash_mem, below, fp);
  }

  // Decides whether a plan built under caller settings may be executed, and
  // says why in either case. `allowed` is the decision; everything else is the
  // arithmetic, so a refusal can be checked rather than taken on trust.
  json analyze_budget(pqxx::work& txn, const json& plan) {
    const pglicht::HostCapacity& cap = active_cfg().capacity;
    json out = json::object();
    json host = json::object();
    if (cap.ram_mb > 0) host["ram_mb"] = cap.ram_mb;
    if (cap.vcpus > 0)  host["vcpus"]  = cap.vcpus;
    if (!cap.source.empty()) host["source"] = cap.source;
    out["host"] = host;

    if (cap.ram_mb <= 0 || cap.vcpus <= 0) {
      std::string missing = cap.ram_mb <= 0 && cap.vcpus <= 0 ? "host_ram_mb and host_vcpus"
                          : cap.ram_mb <= 0 ? "host_ram_mb" : "host_vcpus";
      out["allowed"] = false;
      out["reason"] =
          "not analyzed: executing under explicit settings is bounded by the host "
          "capacity declared for this connection, and " + missing + " is not "
          "declared, so no settings change may be executed. Declare host_ram_mb and "
          "host_vcpus on the connection or its [instance:...] section (or "
          "PG_LICHT_HOST_RAM_MB and PG_LICHT_HOST_VCPUS for a single DATABASE_URL). "
          "Omit settings to analyze under the connection's own environment, or use "
          "plan_as_role, which is production's own. The plan below was built under "
          "the settings and not executed.";
      return out;
    }

    pqxx::result r = txn.exec(
        "SELECT pg_size_bytes(current_setting('work_mem'))::float8, "
        "       current_setting('hash_mem_multiplier')::float8");
    const double work_mem = r[0][0].as<double>();
    const double hash_mem = work_mem * r[0][1].as<double>();

    ExecFootprint fp;
    if (plan.is_array() && !plan.empty() && plan[0].contains("Plan"))
      plan_footprint(plan[0]["Plan"], work_mem, hash_mem, 1.0, fp);

    const long long mem_budget = cap.ram_mb * 1048576LL / kAnalyzeRamShareDivisor;
    const int worker_budget = cap.vcpus / kAnalyzeVcpusPerWorker;
    const long long worst =
        fp.worst_bytes >= 9.0e18 ? std::numeric_limits<long long>::max()
                                 : static_cast<long long>(fp.worst_bytes);

    out["memory_budget_bytes"]     = mem_budget;
    out["worst_case_memory_bytes"] = worst;
    out["memory_nodes"]            = fp.memory_nodes;
    out["workers_budget"]          = worker_budget;
    out["workers_planned"]         = fp.workers;
    out["rule"] =
        "at most a tenth of declared RAM for the worst case the plan's memory "
        "limits permit (each sort-like node at work_mem, each hash node at "
        "work_mem x hash_mem_multiplier, times the processes running it), and at "
        "most one parallel worker per four declared vCPUs";

    std::string why;
    if (fp.worst_bytes > static_cast<double>(mem_budget))
      why = "the plan's worst-case memory, " + std::to_string(worst) + " bytes over " +
            std::to_string(fp.memory_nodes) + " memory-using node(s), exceeds the "
            "budget of " + std::to_string(mem_budget) + " bytes (a tenth of the "
            "declared " + std::to_string(cap.ram_mb) + " MB)";
    if (fp.workers > worker_budget)
      why += std::string(why.empty() ? "" : ", and ") + "the plan asks for " +
             std::to_string(fp.workers) + " parallel worker(s) against a budget of " +
             std::to_string(worker_budget) + " (one per four of the declared " +
             std::to_string(cap.vcpus) + " vCPUs)";
    out["allowed"] = why.empty();
    if (!why.empty())
      out["reason"] = "not analyzed: " + why + ". Lower the settings and call again; "
                      "the plan below was built under them and not executed.";
    return out;
  }

  // The settings a caller may apply before a plan is produced.
  //
  // An allowlist, not a denylist, and that is the whole safety argument. An
  // arbitrary SET passthrough would let a caller turn off
  // default_transaction_read_only, remove statement_timeout, or change role /
  // session_authorization -- three of this server's four safety properties,
  // handed away through a convenience argument. The set that changes a PLAN is
  // bounded and known, and every entry is USERSET, so none of it needs a
  // privilege this server does not already have.
  //
  // search_path is deliberately absent even though it changes plans, because
  // it does so by changing WHICH OBJECTS the statement resolves to. That is a
  // different question from how they are joined, and quietly planning against
  // a different table than the caller meant is worse than refusing.
  static bool is_planner_setting(const std::string& name) {
    // Every enable_* GUC is a planner method toggle by convention, and new
    // ones arrive most releases -- enable_group_by_reordering in 17,
    // enable_distinct_reordering and enable_self_join_elimination in 18. The
    // prefix rule is what keeps this table from needing an edit per release.
    if (name.rfind("enable_", 0) == 0) return true;
    static const std::set<std::string> kAllowed = {
      // memory
      "work_mem", "hash_mem_multiplier", "maintenance_work_mem",
      // costs
      "seq_page_cost", "random_page_cost", "cpu_tuple_cost",
      "cpu_index_tuple_cost", "cpu_operator_cost", "effective_cache_size",
      "effective_io_concurrency",
      // parallelism
      "max_parallel_workers_per_gather", "parallel_setup_cost",
      "parallel_tuple_cost", "min_parallel_table_scan_size",
      "min_parallel_index_scan_size",
      // join search
      "from_collapse_limit", "join_collapse_limit", "geqo", "geqo_threshold",
      // partitioning and caching
      "constraint_exclusion", "plan_cache_mode",
      // jit
      "jit", "jit_above_cost", "jit_inline_above_cost", "jit_optimize_above_cost",
    };
    return kAllowed.count(name) > 0;
  }

  // Applies planner settings for the remainder of THIS transaction.
  //
  // set_config(name, value, is_local := true) rather than a built SET LOCAL
  // string: it is a function call with bound parameters, so a value cannot
  // escape into SQL text at all. is_local means the value reverts at commit,
  // so it cannot leak across a connection a pooler hands to somebody else --
  // the same property that made the read-only guard transaction-scoped rather
  // than session-scoped, and the same reason.
  //
  // Returns an error object when a name is not allowlisted. Silently dropping
  // it would return a plan the caller believes was built under an environment
  // that was never applied, which is the exact failure class this release
  // keeps correcting -- reintroduced through the fix for it.
  json apply_planner_settings(pqxx::work& txn, const json& settings, json& applied) {
    if (!settings.is_object()) return {};
    for (auto it = settings.begin(); it != settings.end(); ++it) {
      const std::string name = it.key();
      if (!is_planner_setting(name))
        return {{"error", "\"" + name + "\" is not a planner setting and will not be applied"},
                {"hint", "only settings that change a PLAN are accepted: work_mem, "
                         "hash_mem_multiplier, the cost knobs, the parallelism knobs, "
                         "every enable_*, the join-search limits, constraint_exclusion, "
                         "plan_cache_mode and the jit knobs. search_path is excluded on "
                         "purpose: it changes which objects the statement resolves to "
                         "rather than how they are joined, and planning against a "
                         "different table than you meant is worse than this refusal. "
                         "Nothing was applied and no plan was produced."}};
      const std::string value = it->is_string() ? it->get<std::string>() : it->dump();
      try {
        pqxx_exec(txn, "SELECT set_config($1, $2, true)", pqxx::params{name, value});
        applied[name] = value;
      } catch (const pqxx::sql_error& e) {
        return {{"error", "could not apply \"" + name + "\" = \"" + value + "\""},
                {"hint", "the setting exists in this server's allowlist but the value "
                         "was refused; check the unit and the range. Nothing was "
                         "applied and no plan was produced."},
                {"detail", e.what()}};
      }
    }
    return {};
  }

  // The same, taken from what a role is actually configured with.
  //
  // This is the form the question is asked in -- "why is it slow for the
  // application" -- and it removes the step where a human copies values out of
  // hostCapacity.overrides by hand and gets one wrong.
  //
  // Skipped entries are listed rather than dropped. A role carrying a
  // search_path or a statement_timeout gets neither applied, and a plan that
  // silently ignored half the role's environment while claiming to be that
  // role's plan would be worse than one that never claimed it.
  json apply_role_settings(pqxx::work& txn, const std::string& role,
                           json& applied, json& skipped, bool& role_exists) {
    // ORDER BY s.setdatabase, cfg: role-wide entries (setdatabase = 0) first,
    // this database's entries after them. They are applied in this order and
    // the last one wins, which is the precedence PostgreSQL itself gives them
    // -- ALTER ROLE r IN DATABASE d SET overrides ALTER ROLE r SET. Ordered by
    // text alone, "work_mem=1GB" sorts before "work_mem=64MB" and the role-wide
    // 64MB would be what got planned under, in the one configuration where the
    // tool's promise of "the plan the application gets" matters most.
    pqxx::result r = pqxx_exec(txn,
      "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1),"
      "       COALESCE((SELECT JSONB_AGG(cfg ORDER BY s.setdatabase, cfg)"
      "                   FROM pg_db_role_setting s"
      "                   JOIN pg_roles rr ON rr.oid = s.setrole"
      "                   CROSS JOIN LATERAL unnest(s.setconfig) AS cfg"
      "                  WHERE rr.rolname = $1"
      "                    AND s.setdatabase IN (0, (SELECT oid FROM pg_database"
      "                                               WHERE datname = current_database()))),"
      "                '[]'::jsonb)::text",
      pqxx::params{role});
    role_exists = r[0][0].as<bool>();
    if (!role_exists)
      return {{"error", "no such role: \"" + role + "\""},
              {"hint", "listRoles names them; pass 'pattern' there if the role is "
                       "outside the cap. Nothing was applied and no plan was produced."}};

    for (const auto& e : json::parse(r[0][1].as<std::string>())) {
      const std::string cfg = e.get<std::string>();
      const auto eq = cfg.find('=');
      if (eq == std::string::npos) continue;
      const std::string name  = cfg.substr(0, eq);
      const std::string value = cfg.substr(eq + 1);
      if (!is_planner_setting(name)) {
        skipped.push_back({{"name", name}, {"value", value},
                           {"reason", "not a planner setting"}});
        continue;
      }
      // One savepoint per setting. A refused set_config is an SQL error, and an
      // SQL error aborts the transaction: without the savepoint every later
      // set_config and the EXPLAIN itself would fail with "current transaction
      // is aborted", each reported here as a refusal it was not. RELEASE
      // SAVEPOINT keeps a set_config(is_local) value in the parent, so a value
      // applied inside the savepoint is still in force when the plan is built.
      try {
        pqxx::subtransaction sub{txn};
        pqxx_exec(sub, "SELECT set_config($1, $2, true)", pqxx::params{name, value});
        sub.commit();
        applied[name] = value;
      } catch (const pqxx::sql_error&) {
        skipped.push_back({{"name", name}, {"value", value},
                           {"reason", "the server refused the value"}});
      }
    }
    return {};
  }

  const json explain_query(const std::string& queryid, const std::string& sql_in,
                           const json& params, bool analyze, int timeout_ms,
                           const json& settings, const std::string& plan_as_role) {
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

    // The planning environment, applied before anything is planned and only
    // for this transaction. Refusals return here rather than planning under a
    // half-applied environment and labelling the result as though it were
    // whole.
    json applied = json::object(), skipped = json::array();
    bool role_exists = true;
    if (!plan_as_role.empty()) {
      json err = apply_role_settings(txn, plan_as_role, applied, skipped, role_exists);
      if (!err.is_null() && !err.empty()) return err;
    }
    // Explicit settings win, because they are the caller's deliberate
    // override of what the role happens to carry.
    {
      json err = apply_planner_settings(txn, settings, applied);
      if (!err.is_null() && !err.empty()) return err;
    }
    json plan_env;
    if (!applied.empty() || !plan_as_role.empty()) {
      plan_env = json::object();
      plan_env["applied"] = applied;
      if (!plan_as_role.empty()) {
        plan_env["from_role"] = plan_as_role;
        plan_env["skipped_from_role"] = skipped;
      }
      plan_env["note"] =
        "Applied with set_config(..., is_local := true), so these revert when "
        "this transaction ends and cannot leak to another session. The plan "
        "below was built under them; the Settings block reports what the "
        "server saw.";
    }

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
      // Every EXPLAIN here carries SETTINGS, which reports the settings that
      // differ from the built-in default -- and so names the environment this
      // plan was built in.
      //
      // That environment is this server's connection, not the one the
      // statement runs in. work_mem alone is enough to change the algorithm
      // rather than the cost: measured on PostgreSQL 18 over 400k rows with
      // identical statistics, one statement planned as GroupAggregate over a
      // Sort at work_mem 64kB and as HashAggregate at 512MB. Anything read off
      // node types, Sort Method, or whether a node spilled is then read off a
      // plan production never runs.
      //
      // pg_db_role_setting is where a per-role work_mem lives, hostCapacity
      // reports it under `overrides`, and the two together let a caller see
      // the mismatch. SETTINGS is the half that says what planned it; without
      // it there is nothing to compare against and nothing to notice.
      //
      // SETTINGS is PostgreSQL 12 and later, so it needs no gate on any
      // supported major.
      //
      // --- Phase A: produce a plan without executing anything ---
      if (params.empty()) {
        try {
          // A savepoint, so that the expected failure below leaves the
          // transaction usable rather than aborted.
          pqxx::subtransaction sub{txn};
          pqxx::result r = sub.exec("EXPLAIN (SETTINGS, FORMAT JSON) " + sql);
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
          if (!sess.has(Feature::GenericPlan)) {
            json out = {
              {"error", "the statement has $n placeholders and no params were supplied"},
              {"hint", "supply values via the params argument so the statement can be "
                       "prepared and planned; planning a normalized statement without "
                       "params needs EXPLAIN (GENERIC_PLAN), which requires PostgreSQL 16+"}
            };
            // On the queryid path the statement was already recovered; return it
            // so the caller still gets the stats and can retry with params.
            if (!stats.is_null()) out["statement"] = stats;
            if (!plan_env.is_null()) out["planning_environment"] = plan_env;
            return out;
          }
          pqxx::result r = txn.exec("EXPLAIN (SETTINGS, GENERIC_PLAN, FORMAT JSON) " + sql);
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
          "EXPLAIN (SETTINGS, FORMAT JSON) EXECUTE " + prepared + "(" + lits + ")");
        plan = json::parse(r[0][0].as<std::string>());
      }

      // --- the write gate ---
      bool read_only = !plan_has_modify(plan);
      bool analyzed = false;
      std::string note;
      // Caller settings that would shape a real execution. plan_as_role alone
      // does not count: see analyze_budget.
      const bool explicit_settings = settings.is_object() && !settings.empty();
      json exec_budget;

      // --- Phase B: optionally execute, only once proven safe ---
      if (analyze) {
        if (!read_only) {
          note = "not analyzed: the plan contains a ModifyTable node, so the "
                 "statement modifies data; only the plan is returned";
        } else if (generic) {
          note = "not analyzed: the statement has $n placeholders and no params "
                 "were supplied, so only a generic plan could be produced; supply "
                 "params to get a real plan and enable ANALYZE";
        } else if (explicit_settings &&
                   !(exec_budget = analyze_budget(txn, plan)).value("allowed", false)) {
          // Refused the way the other two are: the plan is still returned,
          // analyzed stays false, and the note says why. The arithmetic goes
          // into planning_environment so a refusal can be checked.
          note = exec_budget.value("reason", std::string("not analyzed"));
        } else {
          std::string target = prepared.empty()
            ? sql : ("EXECUTE " + prepared + "(" + lits + ")");
          pqxx::result r = txn.exec(
            "EXPLAIN (SETTINGS, ANALYZE, BUFFERS, FORMAT JSON) " + target);
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
      if (!plan_env.is_null()) {
        if (!exec_budget.is_null()) plan_env["execution_budget"] = exec_budget;
        out["planning_environment"] = plan_env;
      }
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
      // 42883 on a statement recovered from pg_stat_statements is almost always
      // the normalization, not the statement. Normalizing replaces every
      // literal with $n and strips its type, so a placeholder in a position
      // where nothing constrains it -- CASE WHEN ... THEN $6 ELSE $7 is the
      // usual one -- is assigned text at PARSE time and the operator lookup
      // fails before any value is bound. Supplying params cannot fix that,
      // which is the part worth saying: without it a caller retries with
      // params, gets the identical error, and has no way to tell whether the
      // params were even applied.
      if (ss == "42883")
        return {{"error", "the statement references an operator or function that "
                          "does not exist for the types PostgreSQL inferred"},
                {"params_supplied", !params.empty()},
                {"hint", std::string(
                   params.empty()
                     ? "supply values via the params argument if the types are "
                       "inferable. "
                     : "params WERE applied and did not help, which is the "
                       "expected outcome here. ") +
                   "If this statement came from pg_stat_statements, the cause is "
                   "usually normalization rather than the statement: replacing a "
                   "literal with $n strips its type, and a placeholder nothing "
                   "constrains (typically CASE WHEN ... THEN $n ELSE $n) is "
                   "assigned text when the statement is parsed -- before any "
                   "value is bound, so no params argument can change it. Pass the "
                   "statement to 'sql' with the literals written back in, or add "
                   "explicit casts such as $1::numeric, and it will plan."},
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
      if (!sess.has(Feature::BufferCacheSummary)) {
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

  // `pattern` narrows by name; without it the list is capped, ordered so the
  // roles worth reading survive the cap.
  //
  // A multi-tenant cluster has one login role per tenant. Measured on a real
  // one: 723 roles, 689 of them client_* with default attributes and no
  // memberships, 165 kB -- past the client's payload limit, so the tool
  // returned nothing and the interesting 34 went with it. Ordering by
  // "carries a non-default attribute or a membership" first means the cap
  // drops the identical crowd rather than the superusers.
  const json roles(const std::string& pattern) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    std::string query = std::string(R"(
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
      FROM (SELECT * FROM pg_roles AS r0
             LEFT JOIN LATERAL (
                 SELECT count(*) AS n_memberships
                   FROM pg_auth_members AS m0 WHERE m0.member = r0.oid) _m ON true
             WHERE ($1 = '' OR r0.rolname ILIKE '%' || $1 || '%')
             ORDER BY (r0.rolsuper OR r0.rolcreaterole OR r0.rolcreatedb
                       OR r0.rolreplication OR r0.rolbypassrls
                       OR NOT r0.rolinherit OR r0.rolconnlimit <> -1
                       OR r0.rolvaliduntil IS NOT NULL
                       OR _m.n_memberships > 0) DESC, r0.rolname
             LIMIT )") + std::to_string(kRoleLimit) + R"() AS r
      LEFT JOIN LATERAL (
          SELECT JSONB_AGG(g.rolname ORDER BY g.rolname) AS member_of
          FROM pg_auth_members AS m
          JOIN pg_roles AS g ON g.oid = m.roleid
          WHERE m.member = r.oid
      ) _lat24 ON true;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{pattern});

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
                 -- shobj_description, not obj_description. A tablespace is a
                 -- SHARED object, so COMMENT ON TABLESPACE writes to
                 -- pg_shdescription and obj_description() -- which reads
                 -- pg_description -- returns null for every one of them.
                 -- Verified on PostgreSQL 18: a commented tablespace gave
                 -- NULL through obj_description and the comment through
                 -- shobj_description. Every tablespace comment has been
                 -- invisible here, reported as '' rather than as missing.
                 'description', COALESCE(shobj_description(oid, 'pg_tablespace'), '')
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

    // FOR TABLES IN SCHEMA is PostgreSQL 15. pg_publication_tables resolves
    // the members either way, so the MEMBERS were never missing -- what was
    // lost is the DECLARATION, and that decides what happens next: a table
    // created later in a published schema joins the publication by itself,
    // while one added to a table-list publication does not. Two publications
    // with identical members today can behave differently tomorrow.
    const std::string sch_pub = sess.has(Feature::PublicationSchemas)
      ? ", 'schemas', COALESCE((SELECT JSONB_AGG(n.nspname ORDER BY n.nspname)"
        "                         FROM pg_publication_namespace pn"
        "                         JOIN pg_namespace n ON n.oid = pn.pnnspid"
        "                        WHERE pn.pnpubid = p.oid), '[]'::jsonb)"
      : "";

    // The same unbounded expansion listSchemas had, and it is worse here:
    // FOR ALL TABLES resolves through pg_publication_tables to every table in
    // the database, so a publication declared in one line expands to thousands
    // of names. Past the client's payload limit the tool returns nothing, so a
    // cluster with one big publication reports no publications at all.
    //
    // table_count is what the question needs -- "is this publication carrying
    // what I think" -- and all_tables already says the list is the catalog.
    std::string query = std::string(R"(
      SELECT JSONB_OBJECT_AGG(
               p.pubname,
               JSONB_BUILD_OBJECT(
                 'owner',      p.pubowner::regrole::text,
                 'all_tables', p.puballtables,
                 'insert',     p.pubinsert,
                 'update',     p.pubupdate,
                 'delete',     p.pubdelete,
                 'truncate',   p.pubtruncate,
                 'table_count', COALESCE(table_count, 0),
                 'tables',     COALESCE(tables, '[]'::jsonb),
                 'tables_truncated', COALESCE(table_count, 0) > )") + std::to_string(kPublicationTableNames) + sch_pub + R"(
               )
             )
      FROM pg_publication AS p
      LEFT JOIN LATERAL (
          SELECT count(*) AS table_count,
                 to_jsonb((array_agg(pt.schemaname || '.' || pt.tablename
                                     ORDER BY pt.schemaname, pt.tablename)
                          )[1:)" + std::to_string(kPublicationTableNames) + R"(]) AS tables
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
    // password. PostgreSQL agrees strongly enough to revoke it: the catalog
    // carries REVOKE ALL FROM public and then a column-level GRANT SELECT on
    // every column except that one, so 17 of its 18 columns are world-readable
    // and selecting the eighteenth would turn a working tool into a permission
    // error for every non-superuser rather than leaking anything.
    //
    // Scoped to the current database via subdbid to avoid leaking subscriptions
    // that belong to other databases in the same cluster.
    //
    // The rest are version gates, omitted rather than returned as null on a
    // server that has no such column: absent means "this server cannot answer",
    // which is not what a null would say. subbinary and substream are PG14, the
    // supported floor.
    const std::string opt =
      std::string(sess.has(Feature::SubTwoPhase) ? ", 'two_phase', subtwophasestate"
                                ", 'disable_on_error', subdisableonerr" : "") +
                 (sess.has(Feature::SubOrigin) ? ", 'origin', suborigin"
                                ", 'run_as_owner', subrunasowner" : "") +
                 (sess.has(Feature::SubFailover) ? ", 'failover', subfailover" : "");
    std::string query =
      "SELECT JSONB_OBJECT_AGG(subname, JSONB_BUILD_OBJECT("
      "  'owner',              subowner::regrole::text"
      ", 'enabled',            subenabled"
      ", 'publications',       subpublications"
      ", 'slot_name',          subslotname"
      ", 'synchronous_commit', subsynccommit"
      ", 'binary',             subbinary"
      ", 'stream',             substream" + opt +
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

  // The runtime half of listSubscriptions: worker identity, per-table sync
  // state, and the error and conflict counters. listSubscriptions reads
  // pg_subscription, which is what CREATE SUBSCRIPTION declared and changes
  // only on DDL; everything here is a reading. The two compose by name.
  //
  // Scoped by subdbid, and it has to be. pg_subscription is a shared catalog
  // and neither statistics view filters by database -- both are defined over
  // pg_subscription with no WHERE clause -- so an unfiltered query would report
  // the workers and counters of subscriptions belonging to other databases in
  // the cluster, which is exactly the leak subscriptions() guards against.
  // pg_subscription_rel is a local catalog by contrast, so joining it through
  // the already-filtered set scopes it for free.
  //
  // There is deliberately no byte-lag field. received_lsn and latest_end_lsn
  // track each other rather than the publisher: measured against a 19 MB
  // backlog, both sat at the same LSN and their difference was zero in either
  // direction, so any figure derived from them reads as "no lag" at exactly the
  // moment there is some. Byte lag is a publisher-side quantity -- the slot's
  // confirmed_flush_lsn against pg_current_wal_lsn() -- and replicationSlots
  // already reports it as retained_wal_bytes. msg_age_s is the honest
  // subscriber-side signal, and it needs no clock agreement with the caller.
  const json subscription_stats() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // worker_type is PG17. Below it the type is inferred from whether the
    // worker is bound to a relation, which is what the pre-17 idiom was. That
    // collapses 'parallel apply' into 'apply' -- both are apply workers with a
    // null relid -- and leader_pid still tells them apart on 16.
    const std::string worker_type =
      sess.has(Feature::SubWorkerType) ? "st.worker_type"
                  : "CASE WHEN st.relid IS NOT NULL THEN 'table synchronization' "
                    "ELSE 'apply' END";
    const std::string leader_pid = sess.has(Feature::SubLeaderPid) ? "st.leader_pid" : "NULL::int";

    // pg_stat_subscription_stats is PG15. On 14, the supported floor, the view
    // does not exist at all, so the whole errors key is omitted rather than
    // returned as nulls: absent means "this server cannot answer", which is
    // not the same as zero errors.
    const bool has_stats = sess.has(Feature::SubscriptionStatsView);
    // The seven conflict counters are PG18.
    const std::string conflicts = sess.has(Feature::SubConflictCounters) ?
      ", 'conflicts', JSONB_BUILD_OBJECT("
      "   'insert_exists',             ss.confl_insert_exists"
      " , 'update_origin_differs',     ss.confl_update_origin_differs"
      " , 'update_exists',             ss.confl_update_exists"
      " , 'update_missing',            ss.confl_update_missing"
      " , 'delete_origin_differs',     ss.confl_delete_origin_differs"
      " , 'delete_missing',            ss.confl_delete_missing"
      " , 'multiple_unique_conflicts', ss.confl_multiple_unique_conflicts)" : "";

    const std::string errors_cte = has_stats ?
      ", e AS ("
      "  SELECT ss.subid, JSONB_BUILD_OBJECT("
      "           'apply_error_count', ss.apply_error_count"
      "         , 'sync_error_count',  ss.sync_error_count"
      "         , 'stats_reset',       ss.stats_reset" + conflicts + ") AS errors"
      "    FROM pg_stat_subscription_stats AS ss"
      "    JOIN sub ON sub.oid = ss.subid)" : "";
    const std::string errors_key  = has_stats ? ", 'errors', e.errors" : "";
    const std::string errors_join = has_stats ? " LEFT JOIN e ON e.subid = sub.oid" : "";

    const std::string query =
      "WITH sub AS ("
      "  SELECT oid, subname FROM pg_subscription"
      "   WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
      "), w AS ("
      "  SELECT st.subid, JSONB_AGG(JSONB_BUILD_OBJECT("
      "           'worker_type',           " + worker_type +
      "         , 'pid',                   st.pid"
      "         , 'leader_pid',            " + leader_pid +
      "         , 'relation',              st.relid::regclass::text"
      "         , 'received_lsn',          st.received_lsn::text"
      "         , 'latest_end_lsn',        st.latest_end_lsn::text"
      "         , 'latest_end_time',       st.latest_end_time"
      "         , 'last_msg_send_time',    st.last_msg_send_time"
      "         , 'last_msg_receipt_time', st.last_msg_receipt_time"
      "         , 'msg_age_s', round(EXTRACT(EPOCH FROM now() - st.last_msg_receipt_time)::numeric, 1)"
      "         ) ORDER BY st.pid) AS workers"
      "    FROM pg_stat_subscription AS st"
      "    JOIN sub ON sub.oid = st.subid"
      "   GROUP BY st.subid"
      "), r AS ("
      "  SELECT sr.srsubid"
      "       , count(*)                                        AS total"
      "       , count(*) FILTER (WHERE sr.srsubstate = 'r')      AS ready"
      "       , count(*) FILTER (WHERE sr.srsubstate = 'i')      AS initializing"
      "       , count(*) FILTER (WHERE sr.srsubstate = 'd')      AS copying"
      "       , count(*) FILTER (WHERE sr.srsubstate = 'f')      AS copy_finished"
      "       , count(*) FILTER (WHERE sr.srsubstate = 's')      AS synchronized"
      // Only the exceptional set is listed. On a subscription of two thousand
      // tables the ready ones are the whole catalog and say nothing; total and
      // ready already carry that. An empty not_ready with total = ready is
      // unambiguous, which is the property a bare count would lose.
      "       , COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT("
      "             'relation', sr.srrelid::regclass::text"
      "           , 'state', CASE sr.srsubstate WHEN 'i' THEN 'initializing'"
      "                                         WHEN 'd' THEN 'copying'"
      "                                         WHEN 'f' THEN 'copy_finished'"
      "                                         WHEN 's' THEN 'synchronized'"
      "                                         ELSE sr.srsubstate::text END)"
      "           ORDER BY sr.srrelid::regclass::text)"
      "           FILTER (WHERE sr.srsubstate <> 'r'), '[]'::jsonb) AS not_ready"
      "    FROM pg_subscription_rel AS sr"
      "    JOIN sub ON sub.oid = sr.srsubid"
      "   GROUP BY sr.srsubid"
      ")" + errors_cte +
      " SELECT JSONB_OBJECT_AGG(sub.subname, JSONB_BUILD_OBJECT("
      "          'workers', COALESCE(w.workers, '[]'::jsonb)"
      "        , 'tables',  JSONB_BUILD_OBJECT("
      "              'total',         COALESCE(r.total, 0)"
      "            , 'ready',         COALESCE(r.ready, 0)"
      "            , 'initializing',  COALESCE(r.initializing, 0)"
      "            , 'copying',       COALESCE(r.copying, 0)"
      "            , 'copy_finished', COALESCE(r.copy_finished, 0)"
      "            , 'synchronized',  COALESCE(r.synchronized, 0)"
      "            , 'not_ready',     COALESCE(r.not_ready, '[]'::jsonb))" + errors_key +
      "        ))"
      "   FROM sub LEFT JOIN w ON w.subid   = sub.oid"
      "            LEFT JOIN r ON r.srsubid = sub.oid" + errors_join + ";";

    pqxx::result res = txn.exec(query);

    if (!res.empty() && !res[0][0].is_null()) {
      return json::parse(res[0][0].as<std::string>());
    } else {
      return {};
    }
  }

  // Does `role` hold privileges on one object, answered by PostgreSQL rather
  // than reconstructed from ACLs.
  //
  // The catalog is world-readable and so are the has_*_privilege functions, so
  // this needs no grant of its own: any role that can connect can ask about any
  // other. What it saves is not access but correctness -- inheritance, PUBLIC,
  // ownership and superuser all fold into the answer the way the server itself
  // folds them, which is laborious and easy to get subtly wrong by hand.
  //
  // Three things this deliberately reports rather than resolves:
  //
  // 1. has_table_privilege does NOT consider row-level security. A role can
  //    hold SELECT and see no rows at all. The rls block carries what decides
  //    that, including whether this particular role is subject to it, because
  //    the owner is exempt unless FORCE ROW LEVEL SECURITY is set and that is
  //    the single most common wrong test.
  // 2. Schema USAGE is returned beside the object privileges rather than folded
  //    into them. SELECT on a table is inert without it, and the error names the
  //    table, which sends people to the wrong object.
  // 3. inherits false cuts the other way from what one might expect, and it was
  //    measured rather than assumed: the has_* functions DO honour rolinherit,
  //    so a NOINHERIT member of a granted group gets false here even though the
  //    privilege is one SET ROLE away. A false is therefore "not right now"
  //    rather than "never", and the role block carries inherits and member_of
  //    so a caller can tell the two apart.
  const json check_role_access(const std::string& role,
                               const std::string& schema,
                               const std::string& object) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // A role that does not exist makes every has_*_privilege call raise, so it
    // is established first and reported as a fact rather than an error: "no
    // such role" is an answer to the question that was asked.
    pqxx::result who = pqxx_exec(txn,
      "SELECT JSONB_BUILD_OBJECT("
      "  'name', r.rolname, 'exists', true"
      ", 'can_login', r.rolcanlogin, 'superuser', r.rolsuper"
      ", 'inherits', r.rolinherit, 'bypass_rls', r.rolbypassrls"
      ", 'member_of', COALESCE((SELECT JSONB_AGG(g.rolname ORDER BY g.rolname)"
      "                          FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid"
      "                         WHERE m.member = r.oid), '[]'::jsonb))"
      " FROM pg_roles AS r WHERE r.rolname = $1",
      pqxx::params{role});

    if (who.empty() || who[0][0].is_null())
      return {{"role", {{"name", role}, {"exists", false}}},
              {"note", "no such role on this server; nothing else was evaluated"}};

    json out;
    out["role"] = json::parse(who[0][0].as<std::string>());

    // MAINTAIN is PostgreSQL 17. Asking for it on an older server raises
    // rather than returning false, so it is gated rather than probed.
    const std::string maintain =
      sess.has(Feature::MaintainPrivilege) ? ", 'MAINTAIN', has_table_privilege($1, c.oid, 'MAINTAIN')" : "";

    pqxx::result rel = pqxx_exec(txn,
      "SELECT JSONB_BUILD_OBJECT("
      "  'object', JSONB_BUILD_OBJECT("
      "      'schema', n.nspname, 'name', c.relname"
      "    , 'kind', CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'"
      "                             WHEN 'v' THEN 'view'  WHEN 'm' THEN 'materialized view'"
      "                             WHEN 'f' THEN 'foreign table' WHEN 'S' THEN 'sequence'"
      "                             ELSE c.relkind::text END"
      "    , 'owner', pg_get_userbyid(c.relowner)"
      "    , 'is_owner', pg_get_userbyid(c.relowner) = $1)"
      // Schema USAGE is a gate of its own, and the one most often missed.
      ", 'schema_access', JSONB_BUILD_OBJECT("
      "      'usage',  has_schema_privilege($1, n.oid, 'USAGE')"
      "    , 'create', has_schema_privilege($1, n.oid, 'CREATE'))"
      ", 'database_access', JSONB_BUILD_OBJECT("
      "      'connect', has_database_privilege($1, current_database(), 'CONNECT'))"
      ", 'privileges', CASE WHEN c.relkind = 'S' THEN JSONB_BUILD_OBJECT("
      "        'USAGE',  has_sequence_privilege($1, c.oid, 'USAGE')"
      "      , 'SELECT', has_sequence_privilege($1, c.oid, 'SELECT')"
      "      , 'UPDATE', has_sequence_privilege($1, c.oid, 'UPDATE'))"
      "    ELSE JSONB_BUILD_OBJECT("
      "        'SELECT',     has_table_privilege($1, c.oid, 'SELECT')"
      "      , 'INSERT',     has_table_privilege($1, c.oid, 'INSERT')"
      "      , 'UPDATE',     has_table_privilege($1, c.oid, 'UPDATE')"
      "      , 'DELETE',     has_table_privilege($1, c.oid, 'DELETE')"
      "      , 'TRUNCATE',   has_table_privilege($1, c.oid, 'TRUNCATE')"
      "      , 'REFERENCES', has_table_privilege($1, c.oid, 'REFERENCES')"
      "      , 'TRIGGER',    has_table_privilege($1, c.oid, 'TRIGGER')" + maintain +
      "      ) END"
      // Only the columns that carry a grant the table itself does not: a
      // table-level yes makes the per-column answer noise, and the interesting
      // case is a table-level no that is really a partial yes.
      ", 'columns', CASE WHEN c.relkind = 'S' THEN NULL ELSE ("
      "     SELECT NULLIF(JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(p.priv, cols)), '{}'::jsonb)"
      "       FROM (VALUES ('SELECT'),('INSERT'),('UPDATE'),('REFERENCES')) AS p(priv)"
      "       CROSS JOIN LATERAL ("
      "         SELECT CASE WHEN has_table_privilege($1, c.oid, p.priv) THEN NULL ELSE ("
      "                  SELECT JSONB_AGG(a.attname ORDER BY a.attnum)"
      "                    FROM pg_attribute AS a"
      "                   WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped"
      "                     AND has_column_privilege($1, c.oid, a.attnum, p.priv)) END AS cols) _c"
      "   ) END"
      // has_table_privilege knows nothing about RLS, so what decides the rows
      // is reported separately and in full.
      ", 'row_level_security', JSONB_BUILD_OBJECT("
      "      'enabled', c.relrowsecurity, 'forced', c.relforcerowsecurity"
      "    , 'applies_to_role', c.relrowsecurity"
      "        AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = $1)"
      "        AND NOT (pg_get_userbyid(c.relowner) = $1 AND NOT c.relforcerowsecurity)"
      "    , 'policies', COALESCE((SELECT JSONB_OBJECT_AGG(pol.polname, JSONB_BUILD_OBJECT("
      "          'command', CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT'"
      "                                     WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE'"
      "                                     ELSE 'ALL' END"
      "        , 'permissive', pol.polpermissive"
      "        , 'roles', COALESCE((SELECT JSONB_AGG(rolname ORDER BY rolname)"
      "                              FROM pg_roles WHERE oid = ANY(pol.polroles)), '[\"PUBLIC\"]'::jsonb)"
      "        , 'applies_to_role', pol.polroles IS NULL"
      "             OR 0 = ANY(pol.polroles)"
      "             OR EXISTS (SELECT 1 FROM pg_roles pr WHERE pr.oid = ANY(pol.polroles)"
      "                          AND pg_has_role($1, pr.oid, 'USAGE'))"
      "        , 'using', pg_get_expr(pol.polqual, pol.polrelid)"
      "        , 'with_check', pg_get_expr(pol.polwithcheck, pol.polrelid)))"
      "        FROM pg_policy AS pol WHERE pol.polrelid = c.oid), '{}'::jsonb)))"
      "  FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace"
      " WHERE c.oid = to_regclass($2)",
      pqxx::params{role, schema + "." + object});

    if (!rel.empty() && !rel[0][0].is_null()) {
      out.update(json::parse(rel[0][0].as<std::string>()));
      return out;
    }

    // Not a relation. Functions and procedures overload, so every signature of
    // that name is reported rather than one guessed at.
    pqxx::result fn = pqxx_exec(txn,
      "SELECT JSONB_BUILD_OBJECT("
      "  'schema_access', JSONB_BUILD_OBJECT("
      "      'usage', has_schema_privilege($1, n.oid, 'USAGE'))"
      ", 'database_access', JSONB_BUILD_OBJECT("
      "      'connect', has_database_privilege($1, current_database(), 'CONNECT'))"
      ", 'overloads', JSONB_AGG(JSONB_BUILD_OBJECT("
      "      'signature', p.oid::regprocedure::text"
      "    , 'kind', CASE p.prokind WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate'"
      "                             WHEN 'w' THEN 'window' ELSE 'function' END"
      "    , 'owner', pg_get_userbyid(p.proowner)"
      "    , 'is_owner', pg_get_userbyid(p.proowner) = $1"
      "    , 'security_definer', p.prosecdef"
      "    , 'privileges', JSONB_BUILD_OBJECT("
      "          'EXECUTE', has_function_privilege($1, p.oid, 'EXECUTE')))"
      "    ORDER BY p.oid::regprocedure::text))"
      "  FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace"
      " WHERE n.nspname = $2 AND p.proname = $3"
      " GROUP BY n.oid",
      pqxx::params{role, schema, object});

    if (!fn.empty() && !fn[0][0].is_null()) {
      out.update(json::parse(fn[0][0].as<std::string>()));
      out["object"] = {{"schema", schema}, {"name", object}, {"kind", "routine"}};
      return out;
    }

    out["object"] = {{"schema", schema}, {"name", object}};
    out["note"] = "no table, view, sequence, function or procedure by that name "
                  "in that schema; nothing was evaluated. Names are case "
                  "sensitive here exactly as they are in the catalog.";
    return out;
  }

  // The whole distribution of one column: the MCV list and the full histogram,
  // rather than the three points tableStats carries.
  //
  // Two tools rather than a flag on tableStats, on the reasoning locked in
  // decision 3 and already shipped for the buffer cache: a summary asked on
  // every table and a detail asked about one column have different call
  // frequencies and different shapes. histogram_bounds is statistics_target
  // wide -- 101 entries by default and up to 10001 -- so inlining it per column
  // of a wide table is the unbounded payload this project has one of already.
  // Here it is one column, named deliberately, and the caller has asked for it.
  //
  // MCVs and the histogram are complements, not alternatives, and returning one
  // without the other invites a wrong reading: ANALYZE puts the most frequent
  // values in the MCV list and builds the histogram from what is LEFT OVER. So
  // the histogram describes the tail, and a value in the MCV list will not
  // appear in it however common it is.
  //
  // The bounds are equal-frequency: consecutive entries delimit buckets holding
  // roughly the same number of rows each, so bounds bunched close together are a
  // dense region of the distribution and a wide gap is a sparse one. That is
  // what makes them useful as parameters -- picking across the buckets samples
  // the distribution rather than one corner of it.
  const json column_histogram(const std::string& schema, const std::string& table,
                              const std::string& column) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    pqxx::result res = pqxx_exec(txn,
      "SELECT JSONB_BUILD_OBJECT("
      "  'schema', $1::text, 'table', $2::text, 'column', $3::text"
      ", 'null_frac', ps.null_frac"
      ", 'avg_width', ps.avg_width"
      ", 'n_distinct', ps.n_distinct"
      ", 'physical_order_correlation', ps.correlation"
      ", 'most_common_vals',  ps.most_common_vals"
      ", 'most_common_freqs', ps.most_common_freqs"
      ", 'histogram_bounds',  ps.histogram_bounds"
      ", 'histogram_buckets', CASE WHEN ps.histogram_bounds IS NULL THEN 0"
      "     ELSE ARRAY_LENGTH(ps.histogram_bounds::text::text[], 1) - 1 END"
      // Why the histogram is the width it is, and the knob that changes it.
      // -1 means the column inherits default_statistics_target.
      ", 'statistics_target', JSONB_BUILD_OBJECT("
      "      'column', NULLIF(a.attstattarget, -1)"
      "    , 'default', current_setting('default_statistics_target')::int"
      "    , 'effective', COALESCE(NULLIF(a.attstattarget, -1),"
      "                            current_setting('default_statistics_target')::int))"
      ", 'inherited', ps.inherited"
      // pg_stats is defined WITH (security_barrier) and carries
      //   AND (c.relrowsecurity = false OR NOT row_security_active(c.oid))
      // so on a table with RLS active for this role the view returns no row at
      // all -- not a filtered row, no row. Every statistic then reads as null,
      // which is indistinguishable from a table nobody has analyzed. This is
      // the common case rather than the corner one wherever RLS is the norm,
      // so the condition is detected and reported instead of being left for
      // the caller to deduce.
      ", 'stats_hidden_by_rls',"
      "    (c.relrowsecurity AND row_security_active(c.oid)))"
      "  FROM pg_attribute AS a"
      "  JOIN pg_class AS c ON c.oid = a.attrelid"
      "  JOIN pg_namespace AS n ON n.oid = c.relnamespace"
      "  LEFT JOIN pg_stats AS ps ON ps.schemaname = n.nspname"
      "                          AND ps.tablename  = c.relname"
      "                          AND ps.attname    = a.attname"
      " WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3"
      "   AND a.attnum > 0 AND NOT a.attisdropped"
      " ORDER BY ps.inherited NULLS LAST LIMIT 1",
      pqxx::params{schema, table, column});

    if (res.empty() || res[0][0].is_null())
      return {{"error", "no such column"},
              {"hint", "checked " + schema + "." + table + "." + column +
                       ". Names are case sensitive here exactly as they are in "
                       "the catalog; listTables names the columns of a table."}};

    json out = json::parse(res[0][0].as<std::string>());
    // A column with no row in pg_stats at all is a different statement from one
    // whose values are all common, and both come back as nulls otherwise.
    if (out["n_distinct"].is_null()) {
      if (out.value("stats_hidden_by_rls", false))
        out["note"] = "row-level security is enabled on this table and active "
                      "for the current role, and pg_stats returns no row at all "
                      "in that case -- so these nulls say nothing about whether "
                      "the table has been analyzed. The statistics exist and the "
                      "planner uses them; this view cannot show them to this "
                      "role. Ask as the table owner (who is exempt unless FORCE "
                      "ROW LEVEL SECURITY is set) or as a role RLS does not "
                      "apply to. checkRoleAccess reports which of those this "
                      "role is.";
      else
        out["note"] = "no pg_stats row for this column: either nothing has "
                      "analyzed the table, or the current role cannot read its "
                      "statistics -- pg_stats filters on has_column_privilege. "
                      "tableStats carries the analyze timestamps that tell those "
                      "apart, and checkRoleAccess says whether the role can read "
                      "the column.";
    }
    else if (out["histogram_bounds"].is_null())
      out["note"] = "no histogram for this column. ANALYZE builds one only from "
                    "the values left after the most common ones are taken into "
                    "most_common_vals, so a column with few distinct values has "
                    "none at all and the MCV list is the whole distribution.";
    return out;
  }

  // What PostgreSQL is holding on disk, without a shell on the box.
  //
  // This exists because triage-disk-space is otherwise unexecutable: pg_licht
  // speaks SQL and nothing else, and "the disk is filling" is answered by
  // looking at directories. It turns out almost all of it is reachable --
  // pg_ls_waldir, pg_ls_archive_statusdir, pg_ls_tmpdir and pg_ls_logdir each
  // return names and sizes, and pg_tablespace_size and pg_database_size cover
  // the data. What is NOT reachable is the volume itself: PostgreSQL has no
  // function for total or free space, so this says what PostgreSQL is
  // responsible for and never what is left.
  //
  // Every section is its own savepoint. They fail independently and for
  // ordinary reasons -- pg_ls_logdir RAISES rather than returning empty when
  // logging_collector is off, a role short of pg_monitor is refused the
  // pg_ls_* family outright, pg_tablespace_size needs privileges of its own --
  // and one refusal must not cost the caller the sections that would have
  // answered. An aborted transaction would do exactly that.
  //
  // What this costs, measured rather than assumed. The four directory reads are
  // one opendir and a stat per entry: 0.7 ms together on a small cluster, and
  // they stay cheap because pg_wal holds segments rather than relations. The
  // two size sections are the whole cost -- 18 ms against 373 segment files
  // locally, and 314 ms for the whole call against a cluster of roughly a
  // terabyte where a bare databaseSize is already 163 ms.
  //
  // They scale with FILE COUNT, not with bytes: pg_tablespace_size and
  // pg_database_size walk the tree and stat every segment, so a cluster with
  // many small relations costs more than a larger one with few. Nothing is
  // read, no relation is opened and no lock is taken -- this is metadata, not
  // I/O -- but those numbers were measured with the directory entries warm in
  // the page cache, and a disk that is filling is exactly when they are not.
  // statement_timeout bounds it either way, so the failure is a timeout naming
  // itself rather than a hang.
  //
  // The two size sections also overlap: pg_tablespace_size(pg_default) walks
  // the same tree pg_database_size walks for every database inside it, so that
  // subtree is traversed twice. They are kept apart anyway because they answer
  // different questions -- which VOLUME is full, and which DATABASE grew -- and
  // deriving one from the other is wrong wherever a relation lives in a
  // non-default tablespace. The duplicated walk is the price of both answers
  // and is most of the difference between 163 ms and 314 ms.
  const json disk_usage() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();
    json out;

    auto section = [&](const char* key, const std::string& sql) {
      try {
        pqxx::subtransaction sub{txn};
        pqxx::result r = sub.exec(sql);
        sub.commit();
        if (!r.empty() && !r[0][0].is_null())
          out[key] = json::parse(r[0][0].as<std::string>());
      } catch (const std::exception& e) {
        // The reason is the finding often enough to be worth keeping: "not a
        // member of pg_monitor" and "logging_collector is off" are different
        // answers and both are useful.
        out[key] = {{"error", std::string(e.what()).substr(0, 300)}};
      }
    };

    // WAL is the most common answer to a filling disk, and the one a slot pins.
    section("wal",
      "SELECT JSONB_BUILD_OBJECT('files', COUNT(*), 'bytes', COALESCE(SUM(size), 0))"
      "  FROM pg_ls_waldir()");

    // A .ready backlog is a failing archive_command, which retains every
    // segment it has not archived. Indistinguishable from an abandoned slot by
    // size alone, and this is what tells them apart.
    section("archive_status",
      "SELECT JSONB_BUILD_OBJECT("
      "  'ready', COUNT(*) FILTER (WHERE name LIKE '%.ready')"
      ", 'done',  COUNT(*) FILTER (WHERE name LIKE '%.done')"
      ", 'archive_mode', current_setting('archive_mode'))"
      "  FROM pg_ls_archive_statusdir()");

    // Temp files are the fastest thing to free: every byte returns when the
    // statement that spilled them ends.
    section("temp_files",
      "SELECT JSONB_BUILD_OBJECT('files', COUNT(*), 'bytes', COALESCE(SUM(size), 0))"
      "  FROM pg_ls_tmpdir()");

    // Logs are inside the data directory by default, so they compete for the
    // same volume, and a stuck log rotation fills it with nobody looking.
    section("log_files",
      "SELECT JSONB_BUILD_OBJECT('files', COUNT(*), 'bytes', COALESCE(SUM(size), 0)"
      ", 'logging_collector', current_setting('logging_collector')"
      ", 'directory', current_setting('log_directory'))"
      "  FROM pg_ls_logdir()");

    // A tablespace sits on whatever volume it was created on, so the database
    // that is growing and the disk that is full need not be the same device.
    section("tablespaces",
      "SELECT JSONB_OBJECT_AGG(spcname, JSONB_BUILD_OBJECT("
      "  'bytes', pg_tablespace_size(oid)"
      ", 'location', NULLIF(pg_tablespace_location(oid), '')))"
      "  FROM pg_tablespace");

    // Cluster-wide, not just the connected database: the one filling the disk
    // is frequently not the one anybody is looking at.
    section("databases",
      "SELECT JSONB_OBJECT_AGG(datname, pg_database_size(oid))"
      "  FROM pg_database WHERE datallowconn");

    out["note"] =
      "Sizes are what PostgreSQL accounts for, not the volume. Total and free "
      "space are not exposed to SQL by any PostgreSQL function, so this cannot "
      "say how much room is left -- only what is consuming it and how that is "
      "divided. The pg_ls_* sections need pg_monitor; a section that reports an "
      "error rather than a size is usually that.";
    return out;
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

    // pg_statistic_ext is the DEFINITION. A CREATE STATISTICS that has never
    // been ANALYZEd has a row there, no data anywhere, and changes no plan --
    // and through 4.2.0 this tool reported it identically to a working one.
    // The built data lives in pg_statistic_ext_data. 4.2.0's prompt audit
    // added "check whether extended statistics exist" to explain-and-fix and
    // diagnose-slow-query precisely so neither would recommend creating what
    // was already there, so existence had to stop meaning the catalog row and
    // start meaning the statistics.
    //
    // stxdinherit is PostgreSQL 15 and later, where it also became part of the
    // key: an object on a partitioned parent can carry separate data for the
    // parent alone and for the whole inheritance tree. Aggregated rather than
    // picked, so `built` is true if either exists and built_for_inherited says
    // which. On 14 there is one row per object and the question cannot arise,
    // so the key is null there rather than a guess.
    const std::string inherit_sel = sess.has(Feature::ExtendedStatsInherit)
      ? ", COALESCE(JSONB_AGG(DISTINCT d.stxdinherit), '[]'::jsonb) AS built_for"
      : ", NULL::jsonb AS built_for";

    const std::string query =
      "SELECT JSONB_OBJECT_AGG("
      "         s.stxname,"
      "         JSONB_BUILD_OBJECT("
      "           'table',       s.stxrelid::regclass::text,"
      "           'columns',     COALESCE(_cols.cols, '[]'::jsonb),"
      "           'kinds',       COALESCE(_kinds.kinds, '[]'::jsonb),"
      "           'built',       COALESCE(built.any_data, false),"
      "           'built_kinds', COALESCE(built.kinds, '[]'::jsonb),"
      "           'built_for_inherited', built.built_for,"
      "           'description', COALESCE(obj_description(s.oid, 'pg_statistic_ext'), '')"
      "         ))"
      "  FROM pg_statistic_ext AS s"
      "  LEFT JOIN LATERAL ("
      "      SELECT JSONB_AGG(attname ORDER BY attnum) AS cols"
      "        FROM pg_attribute"
      "       WHERE attrelid = s.stxrelid AND attnum = ANY(s.stxkeys)"
      "  ) _cols ON true"
      "  LEFT JOIN LATERAL ("
      "      SELECT JSONB_AGG(CASE k WHEN 'd' THEN 'ndistinct' WHEN 'f' THEN 'dependencies'"
      "                              WHEN 'm' THEN 'mcv' WHEN 'e' THEN 'expressions' END) AS kinds"
      "        FROM unnest(s.stxkind) AS k"
      "  ) _kinds ON true"
      "  LEFT JOIN LATERAL ("
      "      SELECT bool_or(d.stxdndistinct IS NOT NULL"
      "                  OR d.stxddependencies IS NOT NULL"
      "                  OR d.stxdmcv IS NOT NULL"
      "                  OR d.stxdexpr IS NOT NULL) AS any_data"
      "           , COALESCE(JSONB_AGG(DISTINCT bk) FILTER (WHERE bk IS NOT NULL),"
      "                      '[]'::jsonb) AS kinds"
      + inherit_sel +
      "        FROM pg_statistic_ext_data AS d"
      "        LEFT JOIN LATERAL unnest(ARRAY["
      "               CASE WHEN d.stxdndistinct    IS NOT NULL THEN 'ndistinct'    END,"
      "               CASE WHEN d.stxddependencies IS NOT NULL THEN 'dependencies' END,"
      "               CASE WHEN d.stxdmcv          IS NOT NULL THEN 'mcv'          END,"
      "               CASE WHEN d.stxdexpr         IS NOT NULL THEN 'expressions'  END])"
      "             AS bk ON true"
      "       WHERE d.stxoid = s.oid"
      "  ) built ON true"
      " WHERE s.stxnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1);";

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
  // not ROLLBACK, not a new transaction, and not DISCARD ALL, PgBouncer's
  // default server_reset_query. Only hypopg_reset() removes it. Against a
  // transaction-mode pooler that means one caller's hypothetical index would
  // otherwise stay on the backend and silently reshape the next caller's
  // plans -- a wrong answer with nothing to indicate it. The pooler cannot be
  // asked to help: in transaction mode it runs no reset query at all unless
  // server_reset_query_always is set (its manual's reasoning is that
  // transaction-pooled connections "should not have any need for a reset
  // query" -- true of core session state, false of memory an extension owns),
  // and forcing that on does not clear hypopg either. Both measured through a
  // real PgBouncer on 2026-09-03: two independent failures to catch it. So the reset
  // runs on the way in, which protects this call from whatever a previous one
  // left, and again on the way out through a scope guard that survives an
  // exception, which protects the next call from this one. Either alone would
  // do most of the job; both is cheap and neither depends on the other.
  const json evaluate_index(const std::string& sql, const json& create_defs,
                            const json& hide_names, const json& settings,
                            const std::string& plan_as_role) {
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

    // The same defect as explainQuery had, and worse here: the whole output is
    // a before/after cost comparison, so an environment that does not match
    // production makes BOTH halves answer a different question than the one
    // asked. Applied for this transaction only, from the same allowlist.
    json applied = json::object(), skipped = json::array();
    bool role_exists = true;
    if (!plan_as_role.empty()) {
      json err = apply_role_settings(txn, plan_as_role, applied, skipped, role_exists);
      if (!err.is_null() && !err.empty()) return err;
    }
    {
      json err = apply_planner_settings(txn, settings, applied);
      if (!err.is_null() && !err.empty()) return err;
    }
    json plan_env;
    if (!applied.empty() || !plan_as_role.empty()) {
      plan_env = json::object();
      plan_env["applied"] = applied;
      if (!plan_as_role.empty()) {
        plan_env["from_role"] = plan_as_role;
        plan_env["skipped_from_role"] = skipped;
      }
    }

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
    const bool pg16 = sess.has(Feature::GenericPlan);
    auto plan_of = [&]() -> json {
      try {
        pqxx::subtransaction sub{txn};
        pqxx::result r = sub.exec("EXPLAIN (SETTINGS, FORMAT JSON) " + sql);
        json p = json::parse(r[0][0].as<std::string>());
        sub.commit();
        return p;
      } catch (const pqxx::sql_error& e) {
        // 42P02: the statement carries $n placeholders. GENERIC_PLAN is the
        // same fallback explainQuery uses, and it is PostgreSQL 16+.
        if (e.sqlstate() != "42P02" || !pg16) throw;
        pqxx::subtransaction sub{txn};
        pqxx::result r = sub.exec("EXPLAIN (SETTINGS, FORMAT JSON, GENERIC_PLAN) " + sql);
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
    if (!plan_env.is_null()) out["planning_environment"] = plan_env;
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
  // data at all. Measured against PostgreSQL 18: a bare login role runs 50 of
  // the 62 tools at full fidelity, pg_monitor takes that to 58, and the ones
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
      " 'read_data',    " + has_role("pg_read_all_data")     + ","
      // Asked of the view itself rather than inferred from a role: no
      // predefined role grants it -- pg_monitor and pg_read_all_stats both
      // lack it, verified on 18.6 -- so only the superuser or an explicit
      // GRANT reads it, and only has_table_privilege sees an explicit GRANT.
      " 'origin_status', has_table_privilege("
      "     'pg_catalog.pg_replication_origin_status', 'SELECT'))";

    pqxx::result res = txn.exec(query);
    const json p = json::parse(res[0][0].as<std::string>());

    const bool super     = p.value("superuser", false);
    const bool monitor   = super || p.value("monitor", false);
    const bool stats     = monitor || p.value("read_stats", false);
    const bool settings  = monitor || p.value("read_settings", false);
    const bool scan      = monitor || p.value("scan_tables", false);
    const bool data      = super  || p.value("read_data", false);
    const bool origins   = super  || p.value("origin_status", false);

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

    // replicationStats has two halves that fail differently, so the entry says
    // which. pg_stat_replication restricts per ROW: without the stats role the
    // senders are listed with most columns null, which reads like an idle
    // replica. pg_replication_origin_status is refused outright to everyone but
    // the superuser unless granted, and pg_monitor does not grant it -- so this
    // is degraded for a monitoring role too, which the 4.3.0 counts missed.
    if (!stats || !origins) {
      std::string what;
      if (!stats)
        what = "the WAL senders are listed with most of their columns null -- "
               "which reads like an idle replica rather than a permission "
               "answer -- because pg_stat_replication restricts per row";
      if (!origins)
        what += std::string(what.empty() ? "" : "; and ") +
                "replication origin progress is refused: "
                "pg_replication_origin_status is readable only by the superuser "
                "or a role granted SELECT on it, and pg_monitor does not include "
                "it" + (stats ? ", so origins is an error while the senders are "
                                "complete" : "");
      degrade("replicationStats", what);
    }

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

    // roleDependencies has no entry, deliberately: since it reads pg_roles
    // rather than pg_authid, everything it touches is world-readable and
    // pg_identify_object checks no privilege, so a bare login role gets the
    // whole answer.

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
  // size_estimate is relpages * block_size and is deliberately not called
  // `size`. It multiplied by a literal 8192 through 4.2.0, which is only the
  // default: pg_class.relpages is documented as a count of pages "of size
  // BLCKSZ", and BLCKSZ is a compile-time option between 1kB and 32kB. On a
  // server built with anything else every estimate was wrong by the ratio, and
  // silently. current_setting('block_size') is a preset GUC, readable by any
  // role and constant for the life of the server, so this costs nothing.
  //
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
    return server_version >= feature_since(Feature::TableStatsSeqScanDetail)
      ? R"(, 'n_tup_newpage_upd', s.n_tup_newpage_upd,
            'last_seq_scan', s.last_seq_scan)"
      : "";
  }

  // The pg_stat_user_tables columns both tools return, in one place so the
  // single-table and schema-wide forms cannot drift apart.
  //
  // The four timestamps are returned separately, under the catalog's own names.
  // Through 4.1.1 they were two, each a GREATEST() of the manual and automatic
  // column, which was wrong in the one direction that does not look wrong: a
  // table kept alive by a cron job reported "vacuumed five minutes ago" and
  // read as healthy, when the finding was that autovacuum is not reaching it at
  // all. "Is autovacuum keeping up" is the question these tools exist to answer
  // and the merge made it unanswerable -- bloat-and-vacuum-review had to be
  // pointed at wraparoundStatus, which had never merged them, to recover a
  // reading its natural tool already held.
  //
  // estimated_from stays a GREATEST of all four, and that merge is correct:
  // it dates relpages and reltuples, which any of the four refreshes equally.
  // A scan counter is meaningless without the window it covers, and
  // pg_stat_reset() zeroes idx_scan and seq_scan along with everything else.
  // An index reported with idx_scan = 0 four days after a reset is an index
  // nothing has used FOR FOUR DAYS -- which for a month-end report, a
  // quarterly job or a failover path is indistinguishable from one nothing has
  // ever used, and the difference decides whether dropping it is safe. This
  // has already produced a wrong recommendation on a real database: a 1.9 GB
  // index proposed for dropping on the strength of a zero that was four days
  // old.
  //
  // Reported as a lower bound rather than a guarantee:
  // pg_stat_reset_single_table_counters() zeroes one relation without touching
  // pg_stat_database.stats_reset, so a null or old value here does not prove
  // the counters beside it are that old. It does prove they are no older.
  static constexpr const char* kCountersSince =
    "'counters_since', (SELECT stats_reset FROM pg_stat_database"
    "                    WHERE datname = current_database())";

  static constexpr const char* kTableStatsCommon = R"(
               'rows', c.reltuples,
               'size_estimate', c.relpages::bigint * current_setting('block_size')::bigint,
               'estimated_from', GREATEST(s.last_vacuum, s.last_autovacuum,
                                          s.last_analyze, s.last_autoanalyze),
               'seq_scan', s.seq_scan, 'idx_scan', s.idx_scan,
               'n_live_tup', s.n_live_tup, 'n_dead_tup', s.n_dead_tup,
               'n_mod_since_analyze', s.n_mod_since_analyze,
               'n_ins_since_vacuum', s.n_ins_since_vacuum,
               'last_vacuum', s.last_vacuum,
               'last_autovacuum', s.last_autovacuum,
               'last_analyze', s.last_analyze,
               'last_autoanalyze', s.last_autoanalyze)";

  const json table_stats(const std::string& schema, const std::string& table) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string pg16 = stats_pg16_fragment(sess.server_version());
    // pg_stat_user_indexes.last_idx_scan is PostgreSQL 16+. Built the same way
    // as the table fragment rather than by erasing a literal out of the
    // finished query, which is what the pre-4.0.0 tableDetails did: if the two
    // ever drifted the erase silently did nothing and the pg14/pg15 jobs failed
    // on an undefined column.
    const std::string idx_pg16 = sess.has(Feature::IndexLastScan)
      ? R"(, 'last_use', si.last_idx_scan)" : "";

    std::string query = std::string(R"(
      SELECT JSONB_BUILD_OBJECT(
               'table', c.relname,)") + kCountersSince + "," + kTableStatsCommon + pg16 + R"(,
               'columns', COALESCE(columns, '{}'::jsonb),
               -- pg_stats returns NO ROW for a table whose RLS is active for
               -- this role, so every per-column statistic below comes back null
               -- and reads exactly like a table nobody has analyzed. Reported
               -- rather than left to be deduced: the analyze timestamps beside
               -- it prove the statistics exist, and the two together are the
               -- only way to tell "not collected" from "not visible to you".
               'stats_hidden_by_rls',
                 (c.relrowsecurity AND row_security_active(c.oid)),
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
                           'most_common_freqs', ps.most_common_freqs,
                           -- Three points off the histogram rather than the
                           -- histogram itself. The array is statistics_target
                           -- entries wide -- 101 by default, up to 10001 -- and
                           -- inlining it for every column of a wide table is the
                           -- unbounded-payload mistake this project has already
                           -- made once elsewhere.
                           --
                           -- What these three are FOR is picking real parameter
                           -- values to re-plan a statement with: low and high are
                           -- the observed extremes of the distribution and mid is
                           -- its median, all of them values that actually occur in
                           -- the column, so a plan built for them is a plan for
                           -- real data rather than for an invented constant. Pair
                           -- them with most_common_vals, which gives the other end
                           -- of the same question: the plan for a frequent value
                           -- against the plan for a rare one is where parameter
                           -- sensitivity shows itself.
                           --
                           -- Null when the column has no histogram at all, which
                           -- means every value is in the MCV list or the column
                           -- was never analyzed. count says which by being 0.
                           -- Selected through to_jsonb rather than a text[] cast
                           -- so the three points carry the same JSON types the
                           -- full array does in columnHistogram: an integer
                           -- column gives numbers here and numbers there. A
                           -- summary whose values do not compare equal to the
                           -- detail it summarises is worse than either alone.
                           'histogram_bounds',
                             CASE WHEN ps.histogram_bounds IS NULL THEN NULL
                             ELSE (SELECT JSONB_BUILD_OBJECT(
                                     'low',   hb -> 0,
                                     'mid',   hb -> (JSONB_ARRAY_LENGTH(hb) / 2),
                                     'high',  hb -> (JSONB_ARRAY_LENGTH(hb) - 1),
                                     'count', JSONB_ARRAY_LENGTH(hb))
                                   FROM TO_JSONB(ps.histogram_bounds) AS _h(hb))
                             END)) AS columns
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

  // Partitioning, which nothing here read until 4.3.0. relkind 'p' was used in
  // five places and only ever to print the words "partitioned table", so a
  // parent's children, its key, its bounds and its default partition were all
  // invisible -- while tableSize's own description told the caller to "measure
  // the partitions", an instruction this server gave no way to follow.
  //
  // Cheap by construction: reltuples and relpages are catalog columns set by
  // VACUUM and ANALYZE, so this opens no relation and takes no lock. The
  // measured counterpart is listTableSizes on the schema holding the
  // partitions, which is deliberately not folded in here -- a parent with
  // three hundred children would be three hundred relation opens behind one
  // innocent-looking call.
  // What depends on a role, cluster-wide. checkRoleAccess answers "may this
  // role use this object"; nothing answered the inverse, and the inverse is
  // the whole of
  //
  //   ERROR:  role "x" cannot be dropped because some objects depend on it
  //   DETAIL: 4 objects in database app
  //
  // -- a message that reports a count and refuses to name anything.
  //
  // pg_shdepend is shared across the cluster: one copy, not one per database.
  // That is what makes the count cross-database and the names not. objid is
  // only resolvable from the database it lives in, so entries for other
  // databases are counted and named by database, never by object. Saying so is
  // the point -- a tool that silently reported only the current database would
  // answer "nothing depends on this role" to somebody about to DROP it.
  const json role_dependencies(const std::string& role) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string param_acl = sess.has(Feature::ParameterAcl)
      ? "WHEN d.classid = 'pg_parameter_acl'::regclass "
        "THEN (SELECT parname FROM pg_parameter_acl WHERE oid = d.objid)"
      : "";

    const std::string query = std::string(R"(
      WITH d AS (
        SELECT s.dbid, s.classid, s.objid, s.objsubid, s.deptype
          FROM pg_shdepend AS s
          -- pg_roles, never pg_authid. pg_authid has no public SELECT, so a
          -- bare login role reading it gets "permission denied" and no answer
          -- at all -- and that is the role most likely to be asking before a
          -- DROP ROLE it does not itself have the privilege to run. The
          -- refclassid comparison is a regclass literal and reads nothing.
          JOIN pg_roles AS a ON a.oid = s.refobjid
         WHERE a.rolname = $1
           AND s.refclassid = 'pg_authid'::regclass
      )
      SELECT JSONB_BUILD_OBJECT(
        'role', $1,
        'exists', EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1),
        'total', (SELECT count(*) FROM d),
        -- deptype, spelled out. 'o' is the one that blocks DROP ROLE outright;
        -- 'a' and 'r' are cleared by REASSIGN OWNED / DROP OWNED, and knowing
        -- which is which is the difference between reassigning and hunting.
        'by_kind', COALESCE((
          SELECT JSONB_OBJECT_AGG(k, n) FROM (
            SELECT CASE deptype WHEN 'o' THEN 'owner'
                                WHEN 'a' THEN 'acl'
                                WHEN 'i' THEN 'init_acl'
                                WHEN 'r' THEN 'policy'
                                WHEN 't' THEN 'tablespace'
                                ELSE deptype::text END AS k,
                   count(*) AS n
              FROM d GROUP BY 1) AS x), '{}'::jsonb),
        -- dbid 0 is a shared object (a database, a tablespace, another role).
        'by_database', COALESCE((
          SELECT JSONB_OBJECT_AGG(name, n) FROM (
            SELECT CASE WHEN d.dbid = 0 THEN '(shared objects)'
                        ELSE COALESCE(db.datname, '(dropped database ' || d.dbid || ')')
                   END AS name,
                   count(*) AS n
              FROM d LEFT JOIN pg_database AS db ON db.oid = d.dbid
             GROUP BY 1) AS y), '{}'::jsonb),
        'resolvable_in_this_database',
          (SELECT count(*) FROM d WHERE d.dbid IN (0, (SELECT oid FROM pg_database
                                                        WHERE datname = current_database()))),
        -- Names, for the rows this database can resolve. A row in another
        -- database is a count above and nothing here, which is honest rather
        -- than empty: connect there and ask again.
        'objects', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'kind', c.relname,
                   'name', CASE
                             WHEN d.classid = 'pg_class'::regclass
                               THEN (SELECT n.nspname || '.' || r.relname
                                       FROM pg_class r JOIN pg_namespace n
                                         ON n.oid = r.relnamespace WHERE r.oid = d.objid)
                             WHEN d.classid = 'pg_proc'::regclass
                               THEN (SELECT n.nspname || '.' || p.proname
                                       FROM pg_proc p JOIN pg_namespace n
                                         ON n.oid = p.pronamespace WHERE p.oid = d.objid)
                             WHEN d.classid = 'pg_namespace'::regclass
                               THEN (SELECT nspname FROM pg_namespace WHERE oid = d.objid)
                             WHEN d.classid = 'pg_database'::regclass
                               THEN (SELECT datname FROM pg_database WHERE oid = d.objid)
                             WHEN d.classid = 'pg_tablespace'::regclass
                               THEN (SELECT spcname FROM pg_tablespace WHERE oid = d.objid)
                             -- pg_roles again: this branch is unreachable for
                             -- most rows, and that does not help. Its condition
                             -- is a column, not a constant, so the subselect
                             -- stays in the range table and the permission
                             -- check fires whether or not a row ever takes it.
                             WHEN d.classid = 'pg_authid'::regclass
                               THEN (SELECT rolname FROM pg_roles WHERE oid = d.objid)
                             WHEN d.classid = 'pg_type'::regclass
                               THEN (SELECT n.nspname || '.' || t.typname
                                       FROM pg_type t JOIN pg_namespace n
                                         ON n.oid = t.typnamespace WHERE t.oid = d.objid)
                             -- GRANT SET ON PARAMETER records a shared
                             -- dependency like any other grant, so these rows
                             -- were already being COUNTED here and only the
                             -- name was missing. pg_parameter_acl is
                             -- PostgreSQL 15 and later.
                             --
                             -- Gated rather than guarded. to_regclass would
                             -- keep the comparison safe on 14, but the branch
                             -- also SELECTs from the catalog by name, and
                             -- PostgreSQL parses the whole statement -- an
                             -- unreachable branch still has to resolve. So the
                             -- text is only emitted where the catalog exists.
                             )" + param_acl + R"(
                             -- Everything else through pg_identify_object, which
                             -- names any class this database can resolve:
                             -- 'p on public.t' for a policy, 'for role x in
                             -- schema s on tables' for a default privilege, the
                             -- oid for a large object, and the language, foreign
                             -- server, subscription, extension or event trigger
                             -- by name. The first version returned NULL for all
                             -- of them -- including 'policy', the kind by_kind
                             -- promotes as the difference between REASSIGN OWNED
                             -- and DROP OWNED. It reads through the syscache and
                             -- checks no privilege, so a bare role gets names
                             -- too (verified on 18.6). The explicit branches
                             -- above stay because their unquoted schema.name
                             -- format is what callers already read.
                             ELSE (pg_identify_object(d.classid, d.objid, 0)).identity
                             END,
                   'column', NULLIF(d.objsubid, 0),
                   'dependency', CASE d.deptype WHEN 'o' THEN 'owner'
                                                WHEN 'a' THEN 'acl'
                                                WHEN 'i' THEN 'init_acl'
                                                WHEN 'r' THEN 'policy'
                                                WHEN 't' THEN 'tablespace'
                                                ELSE d.deptype::text END)
                 ORDER BY c.relname, d.objid)
            FROM d JOIN pg_class AS c ON c.oid = d.classid
           WHERE d.dbid IN (0, (SELECT oid FROM pg_database
                                 WHERE datname = current_database()))), '[]'::jsonb));
    )");

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{role});
    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    return {};
  }

  // ALTER DEFAULT PRIVILEGES, which decides what grants the NEXT object gets.
  // checkRoleAccess answers about the objects that exist; this is the only
  // thing that answers about the ones that do not yet, and it is the standing
  // cause of "the new table isn't readable and every old one is" -- which
  // presents as a broken grant and is a missing default.
  const json default_privileges(const std::string& schema) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    // A named schema that does not exist is an error, as it is in every other
    // schema-taking tool. An empty list here would read as "no defaults are
    // set", which is a real and different answer.
    if (!schema.empty()) {
      json missing = no_such_schema(txn, schema);
      if (!missing.is_null()) return missing;
    }

    const std::string query = R"(
      SELECT COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT(
               -- defaclnamespace = 0 is a "global" entry that overrides the
               -- hard-wired defaults for the type; a non-zero one is per-schema
               -- and its privileges are ADDED to the global ones. Two entries
               -- for the same type are therefore cumulative, not conflicting.
               'scope', CASE WHEN d.defaclnamespace = 0 THEN 'global' ELSE 'schema' END,
               'schema', n.nspname,
               'granted_by', pg_get_userbyid(d.defaclrole),
               'object_type', CASE d.defaclobjtype
                                WHEN 'r' THEN 'table'    WHEN 'S' THEN 'sequence'
                                WHEN 'f' THEN 'function' WHEN 'T' THEN 'type'
                                WHEN 'n' THEN 'schema'   WHEN 'L' THEN 'large object'
                                ELSE d.defaclobjtype::text END,
               'grants', COALESCE(g.grants, '{}'::jsonb))
             ORDER BY d.defaclnamespace <> 0, n.nspname, d.defaclobjtype), '[]'::jsonb)
        FROM pg_default_acl AS d
        LEFT JOIN pg_namespace AS n ON n.oid = d.defaclnamespace
        LEFT JOIN LATERAL (
            SELECT JSONB_OBJECT_AGG(grantee, privs) AS grants
              FROM (SELECT COALESCE(r.rolname, 'PUBLIC') AS grantee,
                           JSONB_AGG(a.privilege_type ORDER BY a.privilege_type) AS privs
                      FROM aclexplode(d.defaclacl) AS a
                      LEFT JOIN pg_roles AS r ON r.oid = a.grantee
                     GROUP BY COALESCE(r.rolname, 'PUBLIC')) AS s) AS g ON true
       -- A named schema keeps the global entries too. Per-schema entries
       -- are ADDED to the global ones, so "what will the next table in this
       -- schema get" is both sets together; filtering the global ones out
       -- answered half the question and presented it as the whole.
       WHERE $1 = '' OR n.nspname = $1 OR d.defaclnamespace = 0;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});
    if (!res.empty() && !res[0][0].is_null())
      return {{"default_privileges", json::parse(res[0][0].as<std::string>())}};
    return {{"default_privileges", json::array()}};
  }

  // Large objects live in a catalog rather than in any user relation, so
  // tableSize, listTableSizes and tableStats are all blind to them while
  // diskUsage.databases counts their bytes. The signature is "the database
  // grew and no table did", which triage-disk-space could not resolve: it
  // would rank tables, find nothing, and stop.
  //
  // Counted and owned, never sized. The bytes live in pg_largeobject, which
  // the documentation says is no longer publicly readable and directs callers
  // here instead -- so a size sum is not reliably available to this server and
  // is not attempted. Saying that is better than a number that is null on
  // every server with a permission structure.
  const json large_objects() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string query = R"(
      SELECT JSONB_BUILD_OBJECT(
        'total', (SELECT count(*) FROM pg_largeobject_metadata),
        'by_owner', COALESCE((
          SELECT JSONB_OBJECT_AGG(owner, n) FROM (
            SELECT pg_get_userbyid(lomowner) AS owner, count(*) AS n
              FROM pg_largeobject_metadata GROUP BY 1) AS s), '{}'::jsonb),
        -- An orphan is a large object no column references. Detecting that
        -- exhaustively means knowing every oid/lo column in the schema, which
        -- this server does not, so the count is reported and the method is
        -- named rather than guessed at.
        'note', 'Sizes are not reported: the bytes live in pg_largeobject, '
                'which is not publicly readable, and the documentation directs '
                'callers to pg_largeobject_metadata for the list instead. '
                'A large object is unreferenced only if no column holds its '
                'oid, which cannot be determined from the catalog alone -- '
                'lo_unlink on a live oid loses data, so confirm against the '
                'application before deleting anything.');
    )";

    pqxx::result res = txn.exec(query);
    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    return {};
  }

  // The publisher side, which nothing here read. replicationSlots reports what
  // a slot RETAINS, in bytes; subscriptionStats cannot measure lag at all by
  // construction (neither of its LSN columns references the publisher). So
  // between the three tools this server had, "how far behind is this replica,
  // in seconds" had no answer anywhere.
  //
  // pg_stat_replication is the only source of write_lag, flush_lag and
  // replay_lag -- as INTERVALS, for physical standbys and logical subscribers
  // alike -- beside sent_lsn against write/flush/replay_lsn.
  //
  // Runs on its own savepoint alongside the origin status, because the two
  // fail independently: pg_stat_replication is security-restricted per row
  // rather than refused, while the origin function can be refused outright.
  const json replication_stats() {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();
    json out = json::object();

    // Documented behaviour worth carrying, because it is the reading most
    // likely to be misread: "If the standby server has entirely caught up with
    // the sending server and there is no more WAL activity, the most recently
    // measured lag times will continue to be displayed for a short time and
    // then show NULL." A null lag on an idle replica is caught up, not broken,
    // and a stale non-null one is the last measurement rather than the current
    // state.
    try {
      pqxx::subtransaction sub{txn};
      pqxx::result r = sub.exec(R"(
        SELECT COALESCE(JSONB_OBJECT_AGG(key, obj), '{}'::jsonb) FROM (
          -- Keyed by application_name AND pid, never by name alone. A
          -- walreceiver's default application_name is the standby's
          -- cluster_name, and Debian packaging sets that per major rather than
          -- per host ('18/main' on every install); unpackaged builds all default
          -- to 'walreceiver'. Two such standbys share a key, and
          -- JSONB_OBJECT_AGG keeps one value per key -- so the tool whose
          -- reason to exist is "which replica is behind" would silently lose
          -- one of them. The pid is what pg_stat_replication itself is keyed by.
          SELECT CASE WHEN COALESCE(application_name, '') = ''
                      THEN 'pid ' || pid::text
                      ELSE application_name || ' (pid ' || pid::text || ')' END AS key,
                 JSONB_BUILD_OBJECT(
                   'pid',              pid,
                   'user',             usename,
                   'application_name', application_name,
                   'client_addr',      host(client_addr),
                   'backend_start',    backend_start,
                   'backend_xmin',     backend_xmin::text,
                   'state',            state,
                   'sync_state',       sync_state,
                   'sync_priority',    sync_priority,
                   'sent_lsn',         sent_lsn::text,
                   'write_lsn',        write_lsn::text,
                   'flush_lsn',        flush_lsn::text,
                   'replay_lsn',       replay_lsn::text,
                   'write_lag_s',      round(EXTRACT(EPOCH FROM write_lag)::numeric, 3),
                   'flush_lag_s',      round(EXTRACT(EPOCH FROM flush_lag)::numeric, 3),
                   'replay_lag_s',     round(EXTRACT(EPOCH FROM replay_lag)::numeric, 3),
                   'reply_time',       reply_time,
                   -- The byte gap between what this server can send and what
                   -- this consumer has replayed. Complements the lag
                   -- intervals: bytes say how much, seconds say how long.
                   --
                   -- On a primary that is pg_current_wal_lsn(). On a
                   -- cascading standby pg_current_wal_lsn() raises, and the
                   -- first version returned NULL there -- for exactly the
                   -- server whose downstream replicas have no other byte
                   -- reading. A cascading walsender sends up to the later of
                   -- what it has received and what it has replayed (its
                   -- GetStandbyFlushRecPtr), so that is the minuend. Verified
                   -- on an 18 cascade with the leaf's replay paused: NULL
                   -- before, 12970272 after, equal to sent_lsn - replay_lsn.
                   'replay_behind_bytes',
                     pg_wal_lsn_diff(
                       CASE WHEN pg_is_in_recovery()
                            THEN GREATEST(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
                            ELSE pg_current_wal_lsn() END,
                       replay_lsn)
                 ) AS obj
            FROM pg_stat_replication) AS s)");
      out["replication"] = json::parse(r[0][0].as<std::string>());
      sub.commit();
    } catch (const pqxx::sql_error& e) {
      out["replication"] = json{{"error", "could not read pg_stat_replication"},
                                {"detail", e.what()}};
    }

    try {
      pqxx::subtransaction sub{txn};
      pqxx::result r = sub.exec(R"(
        SELECT COALESCE(JSONB_OBJECT_AGG(external_id, JSONB_BUILD_OBJECT(
                 'local_id',   local_id,
                 'remote_lsn', remote_lsn::text,
                 'local_lsn',  local_lsn::text)), '{}'::jsonb)
          FROM pg_replication_origin_status)");
      out["origins"] = json::parse(r[0][0].as<std::string>());
      sub.commit();
    } catch (const pqxx::sql_error& e) {
      out["origins"] = json{{"error", "could not read pg_replication_origin_status"},
                            {"hint", "reading replication origin progress needs the "
                                     "superuser or a role granted SELECT on "
                                     "pg_replication_origin_status. No predefined "
                                     "role includes it -- pg_monitor and "
                                     "pg_read_all_stats do not -- so a monitoring "
                                     "role gets this error too. The senders above "
                                     "are unaffected"},
                            {"detail", e.what()}};
    }

    out["note"] =
      "pg_stat_replication is security-restricted per row rather than refused: "
      "a role without pg_read_all_stats or pg_monitor sees the sessions exist "
      "and finds many columns null, which reads like an idle replica rather "
      "than a permission answer -- call checkPrivileges. Lag columns revert to "
      "NULL a short time after a standby has entirely caught up and WAL "
      "activity stops, so a null lag on an idle replica means caught up, and a "
      "non-null one on an idle replica is the last measurement rather than the "
      "current state.";
    return out;
  }

  const json list_partitions(const std::string& schema) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string query = R"(
      SELECT JSONB_OBJECT_AGG(parent, obj)
        FROM (
          SELECT c.relname AS parent,
                 JSONB_BUILD_OBJECT(
                   'strategy', CASE p.partstrat WHEN 'r' THEN 'range'
                                                WHEN 'l' THEN 'list'
                                                WHEN 'h' THEN 'hash'
                                                ELSE p.partstrat::text END,
                   'key', pg_get_partkeydef(c.oid),
                   'partitions', count(ch.oid),
                   -- A default partition catches every row that matched no
                   -- bound, so it is the difference between an insert that
                   -- fails loudly and one that silently lands in the wrong
                   -- place. Its row count is the finding, not its existence.
                   'has_default',
                     COALESCE(bool_or(pg_get_expr(ch.relpartbound, ch.oid) = 'DEFAULT'), false),
                   'default_partition',
                     max(ch.relname) FILTER (
                       WHERE pg_get_expr(ch.relpartbound, ch.oid) = 'DEFAULT'),
                   -- NULL when the default has never been analyzed, never 0
                   -- and never -1. reltuples is -1 until the first VACUUM or
                   -- ANALYZE, and stays -1 after rows arrive (verified on
                   -- 18.6: two rows inserted, still -1). Clamping to 0 would
                   -- report a default partition that is filling up as empty,
                   -- and that is the one reading this field exists to catch.
                   -- has_default separates "no default" from "not measured".
                   'default_rows',
                     (max(ch.reltuples) FILTER (
                        WHERE pg_get_expr(ch.relpartbound, ch.oid) = 'DEFAULT'
                          AND ch.reltuples >= 0))::bigint,
                   'rows', COALESCE(sum(GREATEST(ch.reltuples, 0))::bigint, 0),
                   'size_estimate',
                     COALESCE(sum(ch.relpages)::bigint, 0)
                       * current_setting('block_size')::bigint,
                   -- A partition may itself be partitioned. Reporting the
                   -- count rather than recursing keeps this one query, and
                   -- partitionDetails names them.
                   'sub_partitioned',
                     count(*) FILTER (WHERE ch.relkind = 'p')
                 ) AS obj
            FROM pg_class AS c
            JOIN pg_partitioned_table AS p ON p.partrelid = c.oid
            LEFT JOIN pg_inherits AS i  ON i.inhparent = c.oid
            LEFT JOIN pg_class    AS ch ON ch.oid = i.inhrelid
           WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
             -- Only top-level parents: a sub-partitioned child is reported
             -- under its own parent rather than twice.
             AND NOT c.relispartition
           GROUP BY c.oid, c.relname, p.partstrat) AS s;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema});
    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());
    json missing = no_such_schema(txn, schema);
    return missing.is_null() ? json::object() : missing;
  }

  // One parent, every partition, with the readings that decide which one is
  // the problem. The per-partition statistics already existed in
  // pg_stat_user_tables -- autovacuum runs per partition, so a parent has no
  // vacuum state of its own and bloat-and-vacuum-review was ranking a relation
  // whose counters are always zero while the child that is behind went
  // unlisted.
  const json partition_details(const std::string& schema, const std::string& table) {
    Session sess = open_session();
    pqxx::work& txn = sess.txn();

    const std::string query = std::string(R"(
      SELECT JSONB_BUILD_OBJECT(
        'table',    c.relname,
        'strategy', CASE p.partstrat WHEN 'r' THEN 'range'
                                     WHEN 'l' THEN 'list'
                                     WHEN 'h' THEN 'hash'
                                     ELSE p.partstrat::text END,
        'key',      pg_get_partkeydef(c.oid),
        'is_partition_of',
          (SELECT pn.nspname || '.' || pc.relname
             FROM pg_inherits pi
             JOIN pg_class pc ON pc.oid = pi.inhparent
             JOIN pg_namespace pn ON pn.oid = pc.relnamespace
            WHERE pi.inhrelid = c.oid),
        )") + kCountersSince + R"(,
        'partitions', COALESCE((
          SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                   'name',    ch.relname,
                   'schema',  chn.nspname,
                   -- Verbatim. Parsing a bound generically is not possible --
                   -- it carries whatever types the key columns have -- and a
                   -- misparsed boundary is worse than an unparsed one. For a
                   -- RANGE parent, comparing the highest upper bound here
                   -- against now() is how to see that next period's partition
                   -- was never created, which is the classic overnight failure.
                   'bound',   pg_get_expr(ch.relpartbound, ch.oid),
                   'is_default', pg_get_expr(ch.relpartbound, ch.oid) = 'DEFAULT',
                   'is_partitioned', ch.relkind = 'p',
                   -- NULL rather than 0 for a partition never analyzed, for
                   -- the reason default_rows gives in listPartitions: -1 means
                   -- "not measured" and survives inserts, so a clamp would
                   -- call a filling partition empty. relpages is set by the
                   -- same VACUUM or ANALYZE, so its 0 means the same thing and
                   -- the size estimate goes null with it.
                   'rows', CASE WHEN ch.reltuples < 0 THEN NULL
                                ELSE ch.reltuples::bigint END,
                   'size_estimate',
                     CASE WHEN ch.reltuples < 0 THEN NULL
                          ELSE ch.relpages::bigint * current_setting('block_size')::bigint END,
                   'n_live_tup', s.n_live_tup,
                   'n_dead_tup', s.n_dead_tup,
                   'n_mod_since_analyze', s.n_mod_since_analyze,
                   'n_ins_since_vacuum',  s.n_ins_since_vacuum,
                   'seq_scan', s.seq_scan,
                   'idx_scan', s.idx_scan,
                   'last_vacuum',      s.last_vacuum,
                   'last_autovacuum',  s.last_autovacuum,
                   'last_analyze',     s.last_analyze,
                   'last_autoanalyze', s.last_autoanalyze)
                 ORDER BY pg_get_expr(ch.relpartbound, ch.oid) = 'DEFAULT', ch.relname)
            FROM pg_inherits AS i
            JOIN pg_class AS ch ON ch.oid = i.inhrelid
            JOIN pg_namespace AS chn ON chn.oid = ch.relnamespace
            LEFT JOIN pg_stat_user_tables AS s ON s.relid = ch.oid
           WHERE i.inhparent = c.oid), '[]'::jsonb))
        FROM pg_class AS c
        JOIN pg_partitioned_table AS p ON p.partrelid = c.oid
       WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
         AND c.relname = $2;
    )";

    pqxx::result res = pqxx_exec(txn, query, pqxx::params{schema, table});
    if (!res.empty() && !res[0][0].is_null())
      return json::parse(res[0][0].as<std::string>());

    // Not partitioned and does not exist are different answers, and the first
    // is the one a caller reaches by habit after listTables named the relation.
    pqxx::result k = pqxx_exec(
      txn, "SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n "
           "ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2",
      pqxx::params{schema, table});
    if (!k.empty())
      return {{"error", "\"" + schema + "." + table + "\" is not a partitioned table"},
              {"hint", "relkind is '" + k[0][0].as<std::string>() + "'; only a "
                       "partitioned table (relkind 'p') has partitions. "
                       "listPartitions names every partitioned table in a schema, "
                       "and tableDetails describes an ordinary one."}};
    return {{"error", "no such table: \"" + schema + "." + table + "\""},
            {"hint", "listTables names every relation in a schema. Names are "
                     "case sensitive here exactly as they are in the catalog."}};
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
    json missing = no_such_schema(txn, schema);
    return missing.is_null() ? json::object() : missing;
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
    json missing = no_such_schema(txn, schema);
    return missing.is_null() ? json::object() : missing;
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
    const auto it = tool_index().find(tool_name);
    if (it == tool_index().end()) return false;
    result_content = it->second->run(*this, Args{arguments});
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
  // This matters more here than it looks. tools/list is ~93 kB once 62 tools
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
  // ignores nextCursor: 62 tools, 12 prompts and 5 templates are each one page.
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
