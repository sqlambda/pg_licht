// libFuzzer target: the budgets file parser, from bytes. See
// fuzz_connections.cpp: a refusal is an exception; a crash is a finding.
#include <cstddef>
#include <cstdint>
#include <string>
#include "../src/config.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const std::string text(reinterpret_cast<const char*>(data), size);
  try {
    (void)pglicht::Budgets::parse_text(text, "fuzz-budgets.ini");
  } catch (const std::exception&) {
  }
  return 0;
}
