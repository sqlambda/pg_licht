#!/usr/bin/env python3
"""Write the seed corpora for the libFuzzer targets in cpp/fuzz/.

Seeds are where a fuzzer starts mutating from, so they should be the inputs
that reach the most code: for the INI parsers, the example files the project
ships; for JSON-RPC, every method the server answers, in both protocol eras,
and one tools/call per tool with the arguments its reference page shows
(tools/reference/examples.json) -- each a real, valid request, so mutations
start deep inside argument validation rather than at the JSON parser.

Usage: fuzz-seeds.py <output directory>
  writes <out>/connections, <out>/budgets and <out>/jsonrpc
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
META = {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {}}


def write(directory, name, data):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, name), "wb") as f:
        f.write(data if isinstance(data, bytes) else data.encode())


def main(out):
    for kind, src in (("connections", "cpp/test/connections.example.ini"),
                      ("budgets", "cpp/test/budgets.example.ini")):
        with open(os.path.join(ROOT, src), "rb") as f:
            write(os.path.join(out, kind), os.path.basename(src), f.read())

    rpc = os.path.join(out, "jsonrpc")
    n = 0

    def req(method, params=None, modern=False):
        nonlocal n
        p = dict(params or {})
        if modern:
            p["_meta"] = META
        msg = {"jsonrpc": "2.0", "id": n + 1, "method": method, "params": p}
        write(rpc, f"{n:03d}-{method.replace('/', '_')}{'-modern' if modern else ''}",
              json.dumps(msg))
        n += 1

    req("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                       "clientInfo": {"name": "fuzz", "version": "1"}})
    for method in ("tools/list", "prompts/list", "resources/list",
                   "resources/templates/list", "server/discover"):
        req(method)
        req(method, modern=True)
    req("resources/read", {"uri": "pglicht://default/schemas"})
    req("completion/complete", {"ref": {"type": "ref/prompt", "name": "explain-slow-query"},
                                "argument": {"name": "query_id", "value": ""}})

    with open(os.path.join(ROOT, "tools/reference/examples.json")) as f:
        examples = json.load(f)
    for name, ex in sorted(examples["tools"].items()):
        args = ex.get("arguments", {})
        req("tools/call", {"name": name, "arguments": args})
        # The same call as a sweep, which takes the selection path.
        req("tools/call", {"name": name, "arguments": {**args, "group": "g"}})
    for name, ex in sorted(examples.get("prompts", {}).items()):
        req("prompts/get", {"name": name, "arguments": ex.get("arguments", {})})
    print(f"seeds: {n} JSON-RPC requests, 2 INI files -> {out}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
