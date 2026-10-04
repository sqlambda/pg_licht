// libFuzzer target: one JSON-RPC message from the client, through the real
// dispatch -- the input that comes from outside the operator's control.
//
// The server's only connection is a Unix socket in a directory that does not
// exist, so anything that would reach a database fails at connect, at once,
// and what is exercised is everything before that: JSON parsing, method and
// era dispatch, argument validation, sweep selection, the payload guard, and
// the error paths. run() catches what handle_request throws and answers a
// parse error, so this does the same; an exception is the server refusing,
// and only a crash or a sanitizer report is a finding.
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include "../src/server.h"

namespace {
PostgresMCPServer& server() {
  static PostgresMCPServer s(pglicht::ConnectionRegistry::from_ini_text(
      "[default]\nhost = /nonexistent-pglicht-fuzz\ndbname = none\nconnect_timeout = 1\n"
      "[other]\nhost = /nonexistent-pglicht-fuzz\ndbname = other\ngroup = g\ninstance = i\n"
      "[pool]\nkind = pgbouncer\nhost = /nonexistent-pglicht-fuzz\nport = 6432\ngroup = g\n",
      "fuzz.ini", "pg-licht-fuzz"));
  return s;
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const std::string line(reinterpret_cast<const char*>(data), size);
  json req = json::parse(line, nullptr, /*allow_exceptions=*/false);
  if (req.is_discarded()) return 0;
  // Responses go to std::cout; keep them off the fuzzer's terminal.
  std::ostringstream sink;
  std::streambuf* saved = std::cout.rdbuf(sink.rdbuf());
  try {
    server().call_rpc(req);
  } catch (const std::exception&) {
  }
  std::cout.rdbuf(saved);
  return 0;
}
