#include <cstdlib>
#include <string>
#include "server.h"

namespace {

std::string default_config_path() {
  if (const char* home = std::getenv("HOME"))
    return std::string(home) + "/.config/pg_licht/connections.ini";
  return "";
}

bool file_exists(const std::string& path) {
  if (path.empty()) return false;
  struct stat st {};
  return ::stat(path.c_str(), &st) == 0;
}

void usage(const char* argv0) {
  std::cerr
    << "Usage: " << argv0 << " [--config <file.ini>] [database_url]" << std::endl
    << std::endl
    << "Resolution order:" << std::endl
    << "  1. --config <file>" << std::endl
    << "  2. $PG_LICHT_CONFIG" << std::endl
    << "  3. ~/.config/pg_licht/connections.ini (if present)" << std::endl
    << "  4. $DATABASE_URL" << std::endl
    << "  5. argv[1]" << std::endl
    << std::endl
    << "See pg_licht_mcp(1) for the full manual." << std::endl;
}

}  // namespace

int main(int argc, char *argv[]) {
  std::string config_path;
  std::string db_url;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--config" || arg == "-c") {
      if (i + 1 >= argc) {
        std::cerr << "--config requires a file path." << std::endl;
        return 1;
      }
      config_path = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      usage(argv[0]);
      return 0;
    } else {
      db_url = arg;
    }
  }

  if (config_path.empty()) {
    if (const char* env_cfg = std::getenv("PG_LICHT_CONFIG")) config_path = env_cfg;
  }
  // The default path is honoured only when it exists, so DATABASE_URL keeps
  // working on machines that have never created a config file.
  if (config_path.empty() && file_exists(default_config_path()))
    config_path = default_config_path();

  if (db_url.empty()) {
    if (const char* env_url = std::getenv("DATABASE_URL")) db_url = env_url;
  }

  if (config_path.empty() && db_url.empty()) {
    usage(argv[0]);
    return 1;
  }

  try {
    if (!config_path.empty()) {
      auto registry = pglicht::ConnectionRegistry::from_ini(
        config_path, std::string("pg-licht-cpp/") + PGLICHT_VERSION);
      PostgresMCPServer server(std::move(registry));
      server.run();
    } else {
      PostgresMCPServer server(db_url);
      server.run();
    }
  } catch (const std::exception& e) {
    std::cerr << "Fatal DB Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
