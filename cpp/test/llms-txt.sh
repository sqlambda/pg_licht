#!/usr/bin/env bash
#
# Generate llms.txt from the built binary exactly as the release workflow does,
# and check what it wrote. Needs no database: listing tools and prompts answers
# from the binary alone.
#
# The generator already fails when a tool or prompt has no anchor in the
# rendered man page, so a pass here means every entry links to real
# documentation. This adds the checks the generator cannot make about itself:
# that every tool and prompt the binary lists appears exactly once, and that
# the file has the shape llmstxt.org describes.
#
# Usage: llms-txt.sh <path to pg_licht_mcp>
set -euo pipefail

bin=$1
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

mandoc -T html "$root/cpp/man/pg_licht_mcp.1" > "$tmp/pg_licht_mcp.1.html"
python3 "$root/tools/generate-llms-txt.py" --binary "$bin" \
  --man-html "$tmp/pg_licht_mcp.1.html" --output "$tmp/llms.txt"

python3 - "$bin" "$tmp/llms.txt" <<'PY'
import json, os, re, subprocess, sys, tempfile

binary, path = sys.argv[1], sys.argv[2]
text = open(path, encoding="utf-8").read()
lines = text.splitlines()

# The spec's shape: an H1 first, then a blockquote summary.
assert lines[0] == "# pg-licht", lines[0]
assert lines[2].startswith("> "), lines[2]
for section in ("## Docs", "## Tools", "## Prompts"):
    assert section in lines, f"missing {section}"

# Ask the binary independently of the generator.
with tempfile.TemporaryDirectory() as tmp:
    ini = os.path.join(tmp, "c.ini")
    open(ini, "w").write("[none]\nhost = /nonexistent\ndbname = none\n")
    os.chmod(ini, 0o600)
    reqs = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "llms-txt-test", "version": "1"}}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {"jsonrpc": "2.0", "id": 3, "method": "prompts/list", "params": {}},
    ]
    out = subprocess.run([binary, "--config", ini], input="\n".join(json.dumps(r) for r in reqs) + "\n",
                         capture_output=True, text=True, env={**os.environ, "HOME": tmp}, timeout=30).stdout
by_id = {m["id"]: m["result"] for m in map(json.loads, out.splitlines()) if "id" in m}
names = [t["name"] for t in by_id[2]["tools"]] + [p["name"] for p in by_id[3]["prompts"]]

entries = re.findall(r"^- \[([^\]]+)\]\(", text, re.M)
for n in names:
    count = entries.count(n)
    assert count == 1, f"{n} appears {count} times in llms.txt"
version = by_id[1]["serverInfo"]["version"]
assert f"Version {version}." in lines[2], f"summary does not name version {version}"
print(f"llms.txt: {len(by_id[2]['tools'])} tools, {len(by_id[3]['prompts'])} prompts, version {version}")
PY
