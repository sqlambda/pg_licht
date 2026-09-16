#!/usr/bin/env bash
#
# Build the whole site exactly as the release workflow does -- the rendered man
# page, the HTML reference with its landing page, and llms.txt -- and check it.
# Needs no database: tools and prompts are listed by the binary alone.
#
# gen-reference.py already fails when a mocked example no longer matches its
# tool's schema, when a tool has no man page category, or when the landing page
# uses a placeholder it does not know. This adds what a generator cannot say
# about its own output: that every tool and prompt the binary lists has a page,
# that no link between the site's own pages points at nothing, and that the
# landing page has no placeholder left unfilled.
#
# Usage: reference.sh <path to pg_licht_mcp>
set -euo pipefail

bin=$1
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
site=$(mktemp -d)
trap 'rm -rf "$site"' EXIT

mandoc -T html "$root/cpp/man/pg_licht_mcp.1" > "$site/pg_licht_mcp.1.html"
python3 "$root/tools/gen-reference.py" --binary "$bin" --out "$site/reference" \
  --landing "$root/site/index.html" --landing-out "$site/index.html"
python3 "$root/tools/generate-llms-txt.py" --binary "$bin" \
  --man-html "$site/pg_licht_mcp.1.html" --output "$site/llms.txt"

python3 - "$bin" "$site" <<'PY'
import json, os, re, subprocess, sys, tempfile
from urllib.parse import urlparse

binary, site = sys.argv[1], sys.argv[2]

with tempfile.TemporaryDirectory() as tmp:
    ini = os.path.join(tmp, "c.ini")
    open(ini, "w").write("[none]\nhost = /nonexistent\ndbname = none\n")
    os.chmod(ini, 0o600)
    reqs = [{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "reference-test", "version": "1"}}},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            {"jsonrpc": "2.0", "id": 3, "method": "prompts/list", "params": {}}]
    out = subprocess.run([binary, "--config", ini], capture_output=True, text=True, timeout=30,
                         input="\n".join(map(json.dumps, reqs)) + "\n",
                         env={**os.environ, "HOME": tmp}).stdout
res = {m["id"]: m["result"] for m in map(json.loads, out.splitlines()) if "id" in m}
names = [t["name"] for t in res[2]["tools"]] + [p["name"] for p in res[3]["prompts"]]

ref = os.path.join(site, "reference")
missing = [n for n in names if not os.path.isfile(os.path.join(ref, f"{n}.html"))]
assert not missing, f"no reference page for: {missing}"

# Every required argument must be documented on its tool's own page. The
# generator files the shared arguments (connection, the sweep selectors) under
# one heading on the index, and a tool whose own argument happens to share one
# of those names had it filed there too -- so its page said "None of its own"
# while the schema marked it required. That is the third place the name `role`
# collided; this invariant catches the next one whatever it is called.
undocumented = []
for t in res[2]["tools"]:
    page = open(os.path.join(ref, f"{t['name']}.html"), encoding="utf-8").read()
    for arg in t["inputSchema"].get("required", []):
        if not re.search(r"<dt>%s[ <]" % re.escape(arg), page):
            undocumented.append(f"{t['name']}.{arg}")
assert not undocumented, ("required arguments missing from their tool's page: "
                          + ", ".join(undocumented))

# Every file in the site, by its path relative to the site root.
files = set()
for d, _, fs in os.walk(site):
    for f in fs:
        files.add(os.path.relpath(os.path.join(d, f), site))

anchors = {}
def ids(rel):
    if rel not in anchors:
        anchors[rel] = set(re.findall(r'\bid="([^"]+)"', open(os.path.join(site, rel), encoding="utf-8").read()))
    return anchors[rel]

broken = []
for rel in sorted(f for f in files if f.endswith(".html")):
    text = open(os.path.join(site, rel), encoding="utf-8").read()
    for href in re.findall(r'href="([^"]+)"', text):
        if urlparse(href).scheme or href.startswith("mailto:"):
            continue
        path, _, frag = href.partition("#")
        target = os.path.normpath(os.path.join(os.path.dirname(rel), path)) if path else rel
        if target not in files:
            broken.append(f"{rel} -> {href}")
        elif frag and target.endswith(".html") and frag not in ids(target):
            broken.append(f"{rel} -> {href} (no such anchor)")
assert not broken, "broken links:\n  " + "\n  ".join(broken)

landing = open(os.path.join(site, "index.html"), encoding="utf-8").read()
left = re.findall(r"\{\{[A-Z_]+\}\}", landing)
assert not left, f"landing page placeholders left unfilled: {left}"

print(f"site: {len(names)} reference pages, {len([f for f in files if f.endswith('.html')])} "
      f"html files, every internal link resolves")
PY
