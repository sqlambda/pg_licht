#!/usr/bin/env bash
#
# The response-shape contract, exercised end to end against the real binary.
#
# Both switches it covers are read once per process from the environment, so
# they cannot be toggled from inside the gtest suite -- the values are cached in
# function-local statics by design, since a server that changed its wire shape
# mid-session would be worse than one that cannot. Hence a process-level test.
#
# No database is needed: listConnections answers from the registry without
# connecting, so a deliberately unroutable host still exercises the full
# request/response path.
#
#   cpp/test/protocol-compat.sh /path/to/pg_licht_mcp
set -euo pipefail

BIN="${1:-${TEST_BIN:-cpp/build/pg_licht_mcp}}"
[ -x "$BIN" ] || { echo "not executable: $BIN" >&2; exit 1; }
URL="host=192.0.2.1 port=5432 dbname=x user=y connect_timeout=1"

fail=0
check() {  # check <label> <expected> <actual>
  if [ "$2" = "$3" ]; then printf '  ok    %-46s %s\n' "$1" "$3"
  else printf '  FAIL  %-46s expected %s, got %s\n' "$1" "$2" "$3"; fail=1; fi
}

# Emits: "<negotiated>|<sorted result keys of the tools/call>"
probe() {  # probe <requested protocol>
  printf '%s\n%s\n' \
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"$1\",\"capabilities\":{},\"clientInfo\":{\"name\":\"t\",\"version\":\"0\"}}}" \
    '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"listConnections","arguments":{}}}' \
  | "$BIN" "$URL" 2>/dev/null | python3 -c '
import json,sys
neg=""; keys=""
for line in sys.stdin:
    line=line.strip()
    if not line.startswith("{"): continue
    o=json.loads(line)
    if o.get("id")==1: neg=o["result"]["protocolVersion"]
    if o.get("id")==2: keys=",".join(sorted(k for k in o["result"] if k!="isError"))
print(f"{neg}|{keys}")'
}

echo "default -- every client gets a text block"
r=$(probe 2024-11-05); check "2024-11-05 negotiated"    "2024-11-05" "${r%%|*}"
                       check "2024-11-05 result"        "content"    "${r##*|}"
r=$(probe 2025-06-18); check "2025-06-18 negotiated"    "2025-06-18" "${r%%|*}"
                       check "2025-06-18 result"        "content,structuredContent" "${r##*|}"

echo
echo "PG_LICHT_STRUCTURED_ONLY=1 -- opt in to the single format"
export PG_LICHT_STRUCTURED_ONLY=1
r=$(probe 2025-06-18); check "2025-06-18 result"        "structuredContent" "${r##*|}"
r=$(probe 2024-11-05); check "2024-11-05 unaffected"    "content"    "${r##*|}"
unset PG_LICHT_STRUCTURED_ONLY

echo
echo "PG_LICHT_MAX_PROTOCOL=2025-03-26 -- refuse to negotiate any higher"
export PG_LICHT_MAX_PROTOCOL=2025-03-26
r=$(probe 2025-06-18); check "2025-06-18 downgraded to" "2025-03-26" "${r%%|*}"
                       check "and the result carries"   "content"    "${r##*|}"
r=$(probe 2024-11-05); check "2024-11-05 still fine"    "2024-11-05" "${r%%|*}"
unset PG_LICHT_MAX_PROTOCOL

echo
echo "a nonsense cap is ignored rather than leaving nothing to negotiate"
export PG_LICHT_MAX_PROTOCOL=1999-01-01
r=$(probe 2025-06-18); check "2025-06-18 still negotiated" "2025-06-18" "${r%%|*}"
unset PG_LICHT_MAX_PROTOCOL

echo
[ "$fail" -eq 0 ] && echo "OK: response-shape contract holds" || { echo "protocol-compat FAILED"; exit 1; }
