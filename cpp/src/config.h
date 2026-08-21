#pragma once

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/stat.h>

// Connection configuration for pg-licht.
//
// A connection is either a libpq service name or an explicit set of libpq
// keywords. Both come from an INI file of named sections; DATABASE_URL remains
// supported and becomes the "default" connection when no file is present.
//
// Nothing here opens a connection: this is pure parsing and conninfo assembly.
// The server connects per tool call and closes afterwards (see Session in
// server.h), because the target deployment is PgBouncer in transaction mode,
// where session state does not survive between transactions.

namespace pglicht {

// Capacity of the machine the server runs on.
//
// Total RAM and vCPU count are properties of the host, not of the cluster:
// PostgreSQL has no catalog for them, and a backend cannot read them portably
// (and would read the *client's* host anyway, which is the wrong machine).
// They therefore have to be injected by whoever knows -- the operator, via the
// connections file or the environment, or an agent that inspects the host over
// SSH and passes them as arguments to hostCapacity.
//
// Zero means "not configured" and is reported as such. Nothing here is ever
// guessed: a wrong RAM figure would silently invalidate every ratio derived
// from it.
struct HostCapacity {
  long long ram_mb = 0;
  int vcpus = 0;
  std::string storage;   // free text, e.g. "nvme", "gp3", "spinning rust"
  std::string note;      // free text, e.g. "shared with the app server"
  std::string source;    // where the values came from, for the caller to judge

  bool configured() const {
    return ram_mb > 0 || vcpus > 0 || !storage.empty() || !note.empty();
  }
};

// Topology labels attached to a connection.
//
// Three axes, deliberately separate:
//
//   instance           one postmaster. Members share shared_buffers, WAL,
//                      autovacuum workers, max_connections and disk. This is
//                      what PostgreSQL's glossary calls a *database cluster*:
//                      the databases a single instance manages.
//   replication_group  a primary and its replicas. Members hold the same data
//                      on different servers.
//   group              an arbitrary operator label -- environment, tier,
//                      region, team. Any number per connection.
//
// The word "cluster" is deliberately absent. PostgreSQL uses it for the first
// axis and RDS/Aurora for the second, so it means opposite things to the two
// people most likely to read this file.
//
// Only the first two license a claim: on one instance, one database's
// checkpoint storm is another's latency; on one replication group, two members
// disagreeing about the same object are both telling the truth. A group
// implies nothing at all.
//
// Role -- primary or replica -- is deliberately *not* here. It is observed per
// call via pg_is_in_recovery(), never declared: failover swaps it, and failover
// is exactly when this server gets used.

// One named connection.
struct ConnConfig {
  std::string name;
  std::string conninfo;   // assembled libpq conninfo string

  // Non-secret fields echoed by listConnections. A password, if any, is parsed
  // into the conninfo but deliberately never retained here.
  std::string service;
  std::string host;
  std::string port;
  std::string dbname;
  std::string user;

  // Topology. Empty means "not a member of any".
  std::string instance;
  std::string instance_source;   // "declared" or "inferred"
  std::string replication_group;
  std::vector<std::string> groups;

  HostCapacity capacity;
};

namespace detail {

inline std::string trim(const std::string& s) {
  size_t b = 0, e = s.size();
  while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) b++;
  while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) e--;
  return s.substr(b, e - b);
}

// Quote a value for a libpq conninfo string. libpq splits on unquoted
// whitespace, so any value that is empty or contains whitespace must be
// single-quoted, and embedded quotes/backslashes escaped.
inline std::string quote_conninfo(const std::string& v) {
  bool needs_quotes = v.empty();
  for (char c : v) {
    if (std::isspace(static_cast<unsigned char>(c)) || c == '\'' || c == '\\') {
      needs_quotes = true;
      break;
    }
  }
  if (!needs_quotes) return v;

  std::string out = "'";
  for (char c : v) {
    if (c == '\'' || c == '\\') out += '\\';
    out += c;
  }
  return out + "'";
}

// Split a comma-separated list, rejecting an empty entry rather than dropping
// it. A trailing comma is a typo, and silently ignoring it would mean a group
// the operator believes exists but that no connection is a member of.
inline std::vector<std::string> split_list(const std::string& v,
                                           const std::string& where) {
  std::vector<std::string> out;
  size_t b = 0;
  while (true) {
    size_t c = v.find(',', b);
    std::string item = trim(c == std::string::npos ? v.substr(b)
                                                   : v.substr(b, c - b));
    if (item.empty())
      throw std::runtime_error(where + ": empty entry in the list \"" + v + "\"");
    if (std::find(out.begin(), out.end(), item) == out.end())
      out.push_back(item);
    if (c == std::string::npos) break;
    b = c + 1;
  }
  return out;
}

// Parse a strictly positive integer, or throw with the caller's context. Used
// for the host capacity keys, where a typo ("64GB" where megabytes are meant)
// must fail loudly at startup rather than silently produce ratios that are off
// by three orders of magnitude.
inline long long positive_int(const std::string& v, const std::string& where) {
  if (v.empty())
    throw std::runtime_error(where + ": expected a positive integer, got an empty value");
  for (char c : v) {
    if (!std::isdigit(static_cast<unsigned char>(c)))
      throw std::runtime_error(where + ": expected a positive integer, got \"" + v + "\"");
  }
  try {
    long long n = std::stoll(v);
    if (n <= 0)
      throw std::runtime_error(where + ": expected a positive integer, got \"" + v + "\"");
    return n;
  } catch (const std::out_of_range&) {
    throw std::runtime_error(where + ": value out of range: \"" + v + "\"");
  }
}

}  // namespace detail

// A set of named connections, plus which one is the default.
class ConnectionRegistry {
public:
  // Build a single "default" connection from a DATABASE_URL / conninfo string.
  // Used for the DATABASE_URL fallback and by the test fixture.
  static ConnectionRegistry from_url(const std::string& url,
                                     const std::string& app_name) {
    ConnectionRegistry reg;
    ConnConfig cfg;
    cfg.name = "default";
    // A URL (postgresql://...) is passed through untouched; libpq parses it.
    // application_name is appended only for keyword-style strings, where
    // appending is unambiguous.
    cfg.conninfo = url;
    if (url.rfind("postgres://", 0) != 0 && url.rfind("postgresql://", 0) != 0 &&
        url.find("application_name") == std::string::npos) {
      cfg.conninfo += " application_name=" + detail::quote_conninfo(app_name);
    }
    // Host capacity from the environment. Deliberately only on this path:
    // there is exactly one connection here, so there is no question which host
    // the variables describe. With a connections file each section declares
    // its own, since the sections may well live on different machines.
    cfg.capacity = capacity_from_env();

    reg.order_.push_back("default");
    reg.conns_["default"] = cfg;
    reg.default_name_ = "default";
    return reg;
  }

  // Parse an INI file of named connection sections.
  static ConnectionRegistry from_ini(const std::string& path,
                                     const std::string& app_name) {
    // Sections may carry passwords, so apply the ~/.pgpass rule: refuse a file
    // that anyone other than the owner can read.
    struct stat st {};
    if (::stat(path.c_str(), &st) != 0)
      throw std::runtime_error("cannot read config file: " + path);
    if (st.st_mode & 0077)
      throw std::runtime_error(
        "config file " + path + " is group/world accessible; "
        "it may contain credentials. Run: chmod 600 " + path);

    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open config file: " + path);

    ConnectionRegistry reg;
    std::string line, section;
    std::map<std::string, std::vector<std::pair<std::string, std::string>>> raw;
    std::vector<std::string> sections;   // every section, in file order
    size_t lineno = 0;

    while (std::getline(in, line)) {
      lineno++;
      std::string t = detail::trim(line);
      if (t.empty() || t[0] == ';' || t[0] == '#') continue;

      if (t[0] == '[') {
        size_t close = t.find(']');
        if (close == std::string::npos)
          throw std::runtime_error(path + ":" + std::to_string(lineno) +
                                   ": unterminated section header");
        section = detail::trim(t.substr(1, close - 1));
        if (section.empty())
          throw std::runtime_error(path + ":" + std::to_string(lineno) +
                                   ": empty section name");
        if (raw.find(section) != raw.end())
          throw std::runtime_error(path + ":" + std::to_string(lineno) +
                                   ": duplicate section [" + section + "]");
        raw[section];
        sections.push_back(section);
        continue;
      }

      size_t eq = t.find('=');
      if (eq == std::string::npos)
        throw std::runtime_error(path + ":" + std::to_string(lineno) +
                                 ": expected key = value");
      if (section.empty())
        throw std::runtime_error(path + ":" + std::to_string(lineno) +
                                 ": key outside of any [section]");

      std::string key = detail::trim(t.substr(0, eq));
      std::string val = detail::trim(t.substr(eq + 1));

      // Strip an inline comment, but only when unquoted -- a password or an
      // options value may legitimately contain ; or #.
      if (!val.empty() && val.front() != '\'' && val.front() != '"') {
        size_t c = val.find_first_of(";#");
        if (c != std::string::npos) val = detail::trim(val.substr(0, c));
      }
      if (val.size() >= 2 && (val.front() == '\'' || val.front() == '"') &&
          val.back() == val.front()) {
        val = val.substr(1, val.size() - 2);
      }

      std::transform(key.begin(), key.end(), key.begin(),
                     [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      raw[section].emplace_back(key, val);
    }

    // Split the sections in two. A [instance:<name>] section is not a
    // connection: it declares capacity that the instance's members inherit.
    // It must stay out of order_, out of conns_ and out of default selection,
    // or an instance section written first in the file would silently become
    // the default connection.
    const std::string prefix = "instance:";
    for (const auto& sec : sections) {
      if (sec.rfind(prefix, 0) != 0) {
        reg.order_.push_back(sec);
        continue;
      }
      std::string iname = detail::trim(sec.substr(prefix.size()));
      if (iname.empty())
        throw std::runtime_error(path + ": [" + sec + "] has an empty instance name");
      reg.instance_caps_[iname] = build_instance_capacity(iname, raw[sec], path);
    }

    if (reg.order_.empty())
      throw std::runtime_error(path + ": no connection sections defined");

    for (const auto& name : reg.order_) {
      reg.conns_[name] = build(name, raw[name], app_name, path);
    }

    // "default" if present, else the first connection section in file order.
    reg.default_name_ =
      (reg.conns_.count("default") ? std::string("default") : reg.order_.front());

    reg.resolve_topology(path);
    return reg;
  }

  const ConnConfig& get(const std::string& name) const {
    auto it = conns_.find(name.empty() ? default_name_ : name);
    if (it == conns_.end()) {
      std::string known;
      for (const auto& n : order_) known += (known.empty() ? "" : ", ") + n;
      throw std::runtime_error("unknown connection \"" + name +
                               "\"; configured connections: " + known);
    }
    return it->second;
  }

  const std::vector<std::string>& names() const { return order_; }
  const std::string& default_name() const { return default_name_; }

  // Topology indexes. Each maps a name to its members in config file order,
  // which is what makes a fan-out sweep deterministic.
  using Members = std::map<std::string, std::vector<std::string>>;
  const Members& instances() const { return instances_; }
  const Members& replication_groups() const { return replication_groups_; }
  const Members& groups() const { return groups_; }

  // Declared capacity for an instance, or an unconfigured value when the file
  // has no [instance:<name>] section for it.
  HostCapacity instance_capacity(const std::string& name) const {
    auto it = instance_caps_.find(name);
    return it == instance_caps_.end() ? HostCapacity{} : it->second;
  }

  // Members of one topology name, or a throw listing the configured ones --
  // the same treatment get() gives an unknown connection, so a typo fails at
  // the call rather than as a silently empty sweep.
  const std::vector<std::string>& members(const std::string& axis,
                                          const std::string& name) const {
    const Members& idx = axis == "instance"          ? instances_
                       : axis == "replication_group" ? replication_groups_
                                                     : groups_;
    auto it = idx.find(name);
    if (it == idx.end()) {
      std::string known;
      for (const auto& [k, _] : idx) known += (known.empty() ? "" : ", ") + k;
      throw std::runtime_error(
        "unknown " + axis + " \"" + name + "\"; configured " + axis + "s: " +
        (known.empty() ? "(none)" : known));
    }
    return it->second;
  }

  // PG_LICHT_HOST_RAM_MB / _VCPUS / _STORAGE / _NOTE, for the single-connection
  // form. Exposed for the test suite; from_url is its only caller.
  static HostCapacity capacity_from_env() {
    HostCapacity cap;
    auto env = [](const char* n) -> std::string {
      const char* v = std::getenv(n);
      return v ? detail::trim(v) : std::string{};
    };
    std::string ram = env("PG_LICHT_HOST_RAM_MB");
    std::string cpu = env("PG_LICHT_HOST_VCPUS");
    if (!ram.empty()) cap.ram_mb = detail::positive_int(ram, "PG_LICHT_HOST_RAM_MB");
    if (!cpu.empty()) cap.vcpus = static_cast<int>(
                        detail::positive_int(cpu, "PG_LICHT_HOST_VCPUS"));
    cap.storage = env("PG_LICHT_HOST_STORAGE");
    cap.note    = env("PG_LICHT_HOST_NOTE");
    if (cap.configured()) cap.source = "environment";
    return cap;
  }

private:
  std::map<std::string, ConnConfig> conns_;
  std::vector<std::string> order_;
  std::string default_name_;
  Members instances_, replication_groups_, groups_;
  std::map<std::string, HostCapacity> instance_caps_;

  // A reserved [instance:<name>] section carries capacity keys and nothing
  // else. Rejecting anything else here catches a connection section that was
  // accidentally given the reserved prefix, which would otherwise vanish from
  // the registry without a word.
  static HostCapacity build_instance_capacity(
      const std::string& iname,
      const std::vector<std::pair<std::string, std::string>>& kvs,
      const std::string& path) {
    HostCapacity cap;
    const std::string where = path + ": [instance:" + iname + "]";
    for (const auto& [key, val] : kvs) {
      if (key == "host_ram_mb")      cap.ram_mb = detail::positive_int(val, where + " host_ram_mb");
      else if (key == "host_vcpus")  cap.vcpus = static_cast<int>(
                                       detail::positive_int(val, where + " host_vcpus"));
      else if (key == "host_storage") cap.storage = val;
      else if (key == "host_note")    cap.note = val;
      else
        throw std::runtime_error(
          where + ": unexpected key '" + key + "'. An instance section carries "
          "host capacity only (host_ram_mb, host_vcpus, host_storage, "
          "host_note); connection keys belong in a connection section");
    }
    if (cap.configured()) cap.source = "instance section";
    return cap;
  }

  // Everything that can only be decided once every connection is parsed:
  // capacity inheritance, instance inference, the membership indexes, and the
  // cross-axis name check.
  void resolve_topology(const std::string& path) {
    inherit_instance_capacity(path);
    infer_instances();
    build_indexes();
    reject_cross_axis_names(path);
  }

  void inherit_instance_capacity(const std::string& path) {
    for (const auto& name : order_) {
      ConnConfig& c = conns_[name];
      if (c.instance.empty()) continue;
      auto it = instance_caps_.find(c.instance);
      if (it == instance_caps_.end()) continue;
      const HostCapacity& from = it->second;

      const bool declared_own = c.capacity.configured();
      bool inherited = false;
      if (c.capacity.ram_mb == 0 && from.ram_mb > 0) { c.capacity.ram_mb = from.ram_mb; inherited = true; }
      if (c.capacity.vcpus == 0 && from.vcpus > 0)   { c.capacity.vcpus = from.vcpus;   inherited = true; }
      if (c.capacity.storage.empty() && !from.storage.empty()) { c.capacity.storage = from.storage; inherited = true; }
      if (c.capacity.note.empty() && !from.note.empty())       { c.capacity.note = from.note;       inherited = true; }
      if (inherited)
        c.capacity.source = declared_own ? "config file + instance section"
                                         : "instance section";
    }

    // An instance section nobody claims is a typo in one name or the other,
    // and the symptom -- capacity silently missing from every ratio -- is
    // exactly what the loud parsing elsewhere in this file exists to prevent.
    for (const auto& [iname, cap] : instance_caps_) {
      (void)cap;
      bool claimed = false;
      for (const auto& name : order_)
        if (conns_.at(name).instance == iname) { claimed = true; break; }
      if (!claimed)
        throw std::runtime_error(
          path + ": [instance:" + iname + "] is declared but no connection sets "
          "instance = " + iname);
    }
  }

  // Infer an instance from an identical host *and* port. Port matters: many
  // instances can run on one server as long as their ports differ, so a host
  // alone is not evidence.
  //
  // Never inferred for a service= connection: the service file is deliberately
  // not expanded (it may hold a password), so host and port are unknown here.
  // Never inferred from a single connection either -- one endpoint shared with
  // nobody says nothing about anything.
  //
  // The result is advisory. Behind a pooler one host:port can front several
  // instances and one instance can be reachable through several pooler ports,
  // so an inferred instance groups output but never licenses a contention
  // claim; only verifyTopology settles that.
  void infer_instances() {
    std::map<std::string, std::vector<std::string>> by_endpoint;
    for (const auto& name : order_) {
      const ConnConfig& c = conns_.at(name);
      if (!c.instance.empty() || !c.service.empty()) continue;
      if (c.host.empty() || c.port.empty()) continue;
      by_endpoint[c.host + ":" + c.port].push_back(name);
    }
    for (const auto& [endpoint, members] : by_endpoint) {
      if (members.size() < 2) continue;
      // A declared instance that happens to be named "host:port" is not this
      // one. Joining them would assert shared memory on no evidence.
      bool taken = false;
      for (const auto& name : order_)
        if (conns_.at(name).instance == endpoint) { taken = true; break; }
      if (taken) continue;
      for (const auto& n : members) {
        conns_[n].instance = endpoint;
        conns_[n].instance_source = "inferred";
      }
    }
  }

  void build_indexes() {
    for (const auto& name : order_) {
      const ConnConfig& c = conns_.at(name);
      if (!c.instance.empty())          instances_[c.instance].push_back(name);
      if (!c.replication_group.empty()) replication_groups_[c.replication_group].push_back(name);
      for (const auto& g : c.groups)    groups_[g].push_back(name);
    }
  }

  // One name may not label two different axes. An agent that passed the right
  // name to the wrong argument would otherwise get a different set of
  // databases back with nothing in the payload to say so.
  void reject_cross_axis_names(const std::string& path) const {
    const std::pair<const Members*, const char*> axes[] = {
      {&instances_, "instance"},
      {&replication_groups_, "replication_group"},
      {&groups_, "group"},
    };
    for (size_t i = 0; i < 3; i++) {
      for (size_t j = i + 1; j < 3; j++) {
        for (const auto& [name, members] : *axes[i].first) {
          (void)members;
          if (axes[j].first->count(name))
            throw std::runtime_error(
              path + ": \"" + name + "\" names both a " + axes[i].second +
              " and a " + axes[j].second + "; one name must not label two axes");
        }
      }
    }
  }

  static ConnConfig build(const std::string& name,
                          const std::vector<std::pair<std::string, std::string>>& kvs,
                          const std::string& app_name,
                          const std::string& path) {
    ConnConfig cfg;
    cfg.name = name;
    std::string conninfo;
    bool has_app_name = false;

    for (const auto& [key, val] : kvs) {
      // GUCs in `options` are rejected by PgBouncer at startup
      // ("unsupported startup parameter in options: ..."), so a config that
      // relied on them would fail only at connect time, against the pooler
      // this server is built for. Reject it here with an explanation instead.
      if (key == "options")
        throw std::runtime_error(
          path + ": [" + name + "] sets 'options', which PgBouncer rejects as an "
          "unsupported startup parameter. pg-licht sets what it needs per "
          "transaction (BEGIN READ ONLY, SET LOCAL statement_timeout) instead");

      // Host capacity keys describe the machine, not the connection. They are
      // consumed here and never reach the conninfo -- libpq would reject the
      // whole string as an invalid option otherwise.
      if (key == "host_ram_mb") {
        cfg.capacity.ram_mb = detail::positive_int(val, path + ": [" + name + "] host_ram_mb");
        continue;
      }
      if (key == "host_vcpus") {
        cfg.capacity.vcpus = static_cast<int>(
          detail::positive_int(val, path + ": [" + name + "] host_vcpus"));
        continue;
      }
      if (key == "host_storage") { cfg.capacity.storage = val; continue; }
      if (key == "host_note")    { cfg.capacity.note = val;    continue; }

      // Topology keys describe where the connection sits, not how to reach it.
      // Like the capacity keys they must be consumed here: an unrecognised key
      // falls through into the conninfo below, and libpq rejects the whole
      // string with "invalid connection option" at connect time -- a failure
      // that surfaces on the first tool call rather than at startup.
      if (key == "instance" || key == "replication_group") {
        std::string& slot = (key == "instance") ? cfg.instance
                                                : cfg.replication_group;
        if (!slot.empty())
          throw std::runtime_error(path + ": [" + name + "] declares '" + key +
                                   "' more than once");
        if (val.empty())
          throw std::runtime_error(path + ": [" + name + "] has an empty '" +
                                   key + "'");
        slot = val;
        if (key == "instance") cfg.instance_source = "declared";
        continue;
      }
      if (key == "group") {
        for (const auto& g : detail::split_list(val, path + ": [" + name + "] group")) {
          if (std::find(cfg.groups.begin(), cfg.groups.end(), g) == cfg.groups.end())
            cfg.groups.push_back(g);
        }
        continue;
      }

      if (key == "service") cfg.service = val;
      else if (key == "host")   cfg.host   = val;
      else if (key == "port")   cfg.port   = val;
      else if (key == "dbname") cfg.dbname = val;
      else if (key == "user")   cfg.user   = val;
      else if (key == "application_name") has_app_name = true;

      if (!conninfo.empty()) conninfo += " ";
      conninfo += key + "=" + detail::quote_conninfo(val);
    }

    // Either a service (which supplies the rest from the service file) or at
    // least a dbname. Catching this here names the offending section, instead
    // of letting libpq fail later with "definition of service ... not found"
    // or a connect to an unintended default database.
    if (cfg.service.empty() && cfg.dbname.empty())
      throw std::runtime_error(
        path + ": [" + name + "] must set either 'service' or at least 'dbname'");

    if (!has_app_name) {
      if (!conninfo.empty()) conninfo += " ";
      conninfo += "application_name=" + detail::quote_conninfo(app_name);
    }

    if (cfg.capacity.configured()) cfg.capacity.source = "config file";

    cfg.conninfo = conninfo;
    return cfg;
  }
};

}  // namespace pglicht
