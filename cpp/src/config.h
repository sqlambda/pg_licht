#pragma once

#include <algorithm>
#include <cctype>
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
        reg.order_.push_back(section);
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

    if (raw.empty())
      throw std::runtime_error(path + ": no connection sections defined");

    for (const auto& name : reg.order_) {
      reg.conns_[name] = build(name, raw[name], app_name, path);
    }

    // "default" if present, else the first section in file order.
    reg.default_name_ =
      (reg.conns_.count("default") ? std::string("default") : reg.order_.front());
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

private:
  std::map<std::string, ConnConfig> conns_;
  std::vector<std::string> order_;
  std::string default_name_;

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

    cfg.conninfo = conninfo;
    return cfg;
  }
};

}  // namespace pglicht
