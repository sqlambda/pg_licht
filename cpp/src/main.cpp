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
    << "       " << argv0 << " --version | --help" << std::endl
    << std::endl
    << "Resolution order:" << std::endl
    << "  1. --config <file>" << std::endl
    << "  2. $PG_LICHT_CONFIG" << std::endl
    << "  3. ~/.config/pg_licht/connections.ini (if present)" << std::endl
    << "  4. $DATABASE_URL" << std::endl
    << "  5. argv[1]" << std::endl
    << std::endl
    << "Budgets (budgets.ini), first found:" << std::endl
    << "  1. $PG_LICHT_BUDGETS" << std::endl
    << "  2. budgets.ini beside the connections file" << std::endl
    << "  3. ~/.config/pg_licht/budgets.ini" << std::endl
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
    } else if (arg == "--version" || arg == "-V") {
      // To stdout, since it is the answer rather than a diagnostic. A package
      // manager knows the version; a binary installed from the Homebrew tap or
      // built from source has nothing else to ask.
      std::cout << "pg_licht_mcp " << PGLICHT_VERSION << std::endl;
      return 0;
    } else if (arg.size() > 1 && arg[0] == '-') {
      // Through 4.3.3 any argument that was not --config or --help became the
      // connection string, so --version started a server that read stdin and
      // exited silently -- which looks like success. No connection string
      // starts with '-': a URL starts with postgres:// or postgresql://, a
      // conninfo with a keyword.
      std::cerr << "unknown option: " << arg << std::endl << std::endl;
      usage(argv[0]);
      return 2;
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

  // Everything before run() is reading configuration: the budgets file, the
  // connections file, a connection string. Nothing has connected yet --
  // connections open on the first call that needs one -- so a failure here was
  // reported as "Fatal DB Error" about a database nobody had reached.
  bool configured = false;
  try {
    // Loaded here and only here, so a server built any other way -- the test
    // fixture above all -- keeps the built-in limits and never reads a
    // developer's own file. A bad file is fatal at startup, like a bad
    // connections file.
    const char* env_budgets = std::getenv("PG_LICHT_BUDGETS");
    const char* home = std::getenv("HOME");
    const pglicht::Budgets budgets = pglicht::Budgets::load(
      pglicht::Budgets::resolve_path(env_budgets ? env_budgets : "", config_path,
                                     home ? home : ""));

    if (!config_path.empty()) {
      auto registry = pglicht::ConnectionRegistry::from_ini(
        config_path, std::string("pg-licht-cpp/") + PGLICHT_VERSION);
      // A skipped section is reported where the operator looks first: the
      // client's MCP server log, which is where stderr goes.
      for (const auto& [name, why] : registry.invalid())
        std::cerr << "pg_licht_mcp: skipped [" << name << "]: " << why << std::endl;
      PostgresMCPServer server(std::move(registry));
      server.set_budgets(budgets);
      configured = true;
      server.run();
    } else {
      PostgresMCPServer server(db_url);
      server.set_budgets(budgets);
      configured = true;
      server.run();
    }
  } catch (const std::exception& e) {
    std::cerr << (configured ? "Fatal error: " : "Configuration error: ") << e.what()
              << std::endl;
    return 1;
  }

  return 0;
}
