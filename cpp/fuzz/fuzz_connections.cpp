// libFuzzer target: the connections file parser, from bytes.
//
// The file is the operator's own, so this is robustness rather than an attack
// surface -- but a parser that crashes on a typo takes every connection down,
// which 4.5 just stopped one bad section from doing. A refusal is an
// exception, which is the parser working; only a crash or a sanitizer report
// is a finding.
#include <cstddef>
#include <cstdint>
#include <string>
#include "../src/config.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const std::string text(reinterpret_cast<const char*>(data), size);
  try {
    auto reg = pglicht::ConnectionRegistry::from_ini_text(text, "fuzz.ini", "pg-licht-fuzz");
    for (const auto& n : reg.names()) (void)reg.get(n);
    (void)reg.get("");
  } catch (const std::exception&) {
  }
  return 0;
}
