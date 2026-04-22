#include <cstdlib>
#include "server.h"

int main(int argc, char *argv[]) {
  std::string db_url;

  if (const char* env_url = std::getenv("DATABASE_URL")) {
    db_url = env_url;
  }
  else if (argc == 2) {
    db_url = argv[1];
  }
  else {
    std::cerr << "Uso: " << argv[0] << " <database_url>" << std::endl;
    std::cerr << "Ou defina a variavel de ambiente DATABASE_URL." << std::endl;
    return 1;
  }

  try {
    PostgresMCPServer server(db_url);
    server.run();
  } catch (const std::exception& e) {
    std::cerr << "Fatal DB Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
