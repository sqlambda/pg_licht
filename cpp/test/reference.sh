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
# landing page has no placeholder left unfilled -- and that every count of the
# operations written by hand in the docs is the number the binary lists.
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

python3 - "$bin" "$site" "$root" <<'PY'
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
listing_bytes = next(len(l.encode()) for l in out.splitlines() if '"id":2' in l)
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
#
# Every argument, not only the required ones: a new optional argument -- a
# limit, a pattern -- is the kind a release adds, and a page that omits it
# tells the reader the tool cannot be narrowed. The shared selectors are the
# exception, filed once on the index; `role` is one only where
# replication_group is beside it, the rule gen-reference.py applies.
undocumented = []
for t in res[2]["tools"]:
    page = open(os.path.join(ref, f"{t['name']}.html"), encoding="utf-8").read()
    props = t["inputSchema"].get("properties", {})
    shared = {"connection", "instance", "replication_group", "group"}
    if "replication_group" in props:
        shared.add("role")
    for arg in set(props) - shared | set(t["inputSchema"].get("required", [])):
        if not re.search(r"<dt>%s[ <]" % re.escape(arg), page):
            undocumented.append(f"{t['name']}.{arg}")
assert not undocumented, ("arguments missing from their tool's page: "
                          + ", ".join(sorted(undocumented)))

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

# Every count of the operations written by hand must be the number the binary
# lists. The manual said "50 of the 62 operations" for four releases after it
# stopped being true, because nothing compared it with anything. Comments are
# skipped: the landing page records earlier measurements there on purpose.
import html as htmllib
total = len(res[2]["tools"])
def text_of(path, markup):
    t = open(path, encoding="utf-8").read()
    if markup:
        t = re.sub(r"<!--.*?-->", " ", t, flags=re.S)
        t = htmllib.unescape(re.sub(r"<[^>]+>", " ", t))
    return re.sub(r"\s+", " ", t)
repo = sys.argv[3]
docs = {"README.md": (os.path.join(repo, "README.md"), False),
        "INSTALL.md": (os.path.join(repo, "INSTALL.md"), False),
        "the manual": (os.path.join(site, "pg_licht_mcp.1.html"), True),
        "the landing page": (os.path.join(site, "index.html"), True)}
# "N operations" and "N of M" are the forms a total is written in by hand;
# "N tools" is a subset -- "(10 tools, each marked)" -- or a generated count.
counts = [r"(?<![\d.,])(\d{2,3}) (?:read-only )?operations\b",
          r"\b\d{2} of (?:the )?(\d{2,3})\b"]
wrong = []
for label, (path, markup) in docs.items():
    if not os.path.isfile(path):
        continue
    t = text_of(path, markup)
    for p in counts:
        for m in re.finditer(p, t):
            if int(m.group(1)) != total:
                wrong.append(f"{label}: \"{t[max(0, m.start() - 40):m.end() + 10].strip()}\"")
assert not wrong, (f"the binary lists {total} tools, but these say otherwise:\n  "
                   + "\n  ".join(wrong))

# The manual states the size of tools/list, which grows with every tool and
# argument added: it said 93 kB at 62 operations when the listing was 146. Held
# to within a tenth of the measured answer, on the revision that carries
# output schemas, which is the largest.
man_text = text_of(os.path.join(site, "pg_licht_mcp.1.html"), True)
m = re.search(r"roughly (\d+) kB once (\d+) operations", man_text)
assert m, "the manual no longer states the size of tools/list; update this check"
stated = int(m.group(1)) * 1000
assert abs(stated - listing_bytes) <= listing_bytes / 10, (
    f"the manual says tools/list is roughly {m.group(1)} kB; it is "
    f"{listing_bytes} bytes (about {round(listing_bytes / 1000)} kB)")

# `pattern` and `web_search` both narrow by a string and match differently --
# one a literal substring, one full-text -- and the difference is invisible
# from the name. Every tool that takes either must say which in its own
# description, and be listed under that argument in the manual's Matching
# section and in the README's table. `pattern` was an ILIKE on two tools
# through 4.3.3 while documented as a substring, and the three web_search
# arguments had no description at all.
by_arg = {"pattern": [], "web_search": []}
says = {"pattern": "literal, case-insensitive substring",
        "web_search": "full-text search, not a substring"}
silent = []
for t in res[2]["tools"]:
    for arg, phrase in says.items():
        p = t["inputSchema"].get("properties", {}).get(arg)
        if p is None:
            continue
        by_arg[arg].append(t["name"])
        if phrase not in p.get("description", ""):
            silent.append(f"{t['name']}.{arg} does not say \"{phrase}\"")
assert not silent, "arguments that do not say how they match:\n  " + "\n  ".join(silent)

man_text = text_of(os.path.join(site, "pg_licht_mcp.1.html"), True)
start = man_text.find("Matching: pattern and web_search")
end = man_text.find("Privileges", start)
assert start >= 0, "the manual has no Matching: pattern and web_search section"
section = man_text[start:end]
readme = open(os.path.join(repo, "README.md"), encoding="utf-8").read()
rows = {arg: next((l for l in readme.splitlines() if l.startswith(f"| `{arg}` |")), "")
        for arg in by_arg}
unlisted = []
for arg, tools in by_arg.items():
    for name in tools:
        if name not in section:
            unlisted.append(f"{name} ({arg}) is not in the manual's Matching section")
        if f"`{name}`" not in rows[arg]:
            unlisted.append(f"{name} ({arg}) is not in the README's {arg} row")
assert not unlisted, "\n  ".join(["matching is undocumented for:"] + unlisted)

print(f"site: {len(names)} reference pages, {len([f for f in files if f.endswith('.html')])} "
      f"html files, every internal link resolves")
PY
