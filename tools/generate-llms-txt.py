#!/usr/bin/env python3
"""Generate llms.txt for pg_licht from the binary itself.

llms.txt (https://llmstxt.org) is how an agent that does not have this server
installed yet can learn what it is. Every fact in the file comes from the binary
the release ships -- its version, and every tool and prompt with its own
description -- by asking it over stdio, the same way an MCP client does. Nothing
is hand-maintained: a hand-written copy would be a fifth place the tool count
lives, beside README, INSTALL, the man page and the binary, and those have
disagreed before.

The binary is driven with a private, throwaway connections file passed by
--config, which outranks every other source. Without it, a run on a developer's
machine would load ~/.config/pg_licht/connections.ini and could open a real
cluster. Listing tools and prompts needs no database, so the throwaway file
points nowhere.

Usage:
  generate-llms-txt.py --binary PATH [--man-html PATH] [--base-url URL]
                       [--ref REF] [--output PATH]

--man-html is the man page rendered by `mandoc -T html`. When given, every
tool and prompt links to its own entry there, and the run FAILS if an entry has
no anchor, so no link in the file can point at nothing.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

REPO = "https://github.com/sqlambda/pg_licht"
PROTOCOL = "2025-06-18"
MAX_NOTE = 280


def first_sentence(text):
    """The first sentence of a description, capped at MAX_NOTE characters.

    Several descriptions run to a paragraph, and the point of the format is
    that it is cheap to read. A sentence ends at a period followed by
    whitespace, whatever comes next -- these descriptions routinely start a
    sentence with an identifier such as tables_truncated or checkRoleAccess --
    except after "e.g.", "i.e.", "vs." and "etc.".
    """
    text = " ".join(text.split())
    m = re.search(r"(?<!e\.g)(?<!i\.e)(?<!\bvs)(?<!\betc)\.\s+(?=\S)", text)
    s = text[: m.start() + 1] if m else text
    if len(s) > MAX_NOTE:
        cut = s.rfind(" ", 0, MAX_NOTE - 1)
        s = s[: cut if cut > 0 else MAX_NOTE - 1].rstrip(",;:-") + "…"
    return s[:1].upper() + s[1:]


class Server:
    """The binary over stdio, one JSON-RPC line each way."""

    def __init__(self, binary, config):
        env = {k: v for k, v in os.environ.items()
               if not (k.startswith("PG_LICHT_") or k == "DATABASE_URL")}
        # Belt and braces: --config already outranks the per-user file, and
        # a HOME with no .config in it means there is nothing to fall back to.
        env["HOME"] = os.path.dirname(config)
        self.proc = subprocess.Popen(
            [binary, "--config", config], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        self.next_id = 0

    def call(self, method, params=None):
        self.next_id += 1
        req = {"jsonrpc": "2.0", "id": self.next_id, "method": method,
               "params": params or {}}
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError(f"{method}: the server exited: "
                                   f"{self.proc.stderr.read().strip()}")
            msg = json.loads(line)
            if msg.get("id") == self.next_id:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg["result"]

    def notify(self, method):
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method}) + "\n")
        self.proc.stdin.flush()

    def list_all(self, method, key):
        items, cursor = [], None
        while True:
            res = self.call(method, {"cursor": cursor} if cursor else {})
            items.extend(res.get(key, []))
            cursor = res.get("nextCursor")
            if not cursor:
                return items

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=10)


def describe(binary):
    with tempfile.TemporaryDirectory() as tmp:
        config = os.path.join(tmp, "connections.ini")
        with open(config, "w") as f:
            f.write("[none]\nhost = /nonexistent\ndbname = none\n")
        os.chmod(config, 0o600)  # the server refuses a group-readable file
        srv = Server(binary, config)
        try:
            init = srv.call("initialize", {
                "protocolVersion": PROTOCOL, "capabilities": {},
                "clientInfo": {"name": "generate-llms-txt", "version": "1"}})
            srv.notify("notifications/initialized")
            tools = srv.list_all("tools/list", "tools")
            prompts = srv.list_all("prompts/list", "prompts")
        finally:
            srv.close()
    return init["serverInfo"]["version"], tools, prompts


def anchors(man_html):
    with open(man_html, encoding="utf-8") as f:
        return set(re.findall(r'id="([^"]+)"', f.read()))


def render(version, tools, prompts, base_url, ref, man_anchors):
    manual = f"{base_url}/pg_licht_mcp.1.html"

    def link(name):
        return f"{manual}#{name}" if man_anchors is not None else manual

    if man_anchors is not None:
        missing = [x["name"] for x in tools + prompts if x["name"] not in man_anchors]
        if missing:
            raise RuntimeError("no man page anchor for: " + ", ".join(missing) +
                               " -- document them in cpp/man/pg_licht_mcp.1")

    out = [
        "# pg-licht",
        "",
        f"> A read-only PostgreSQL server for the Model Context Protocol: "
        f"{len(tools)} tools and {len(prompts)} guided prompts for exploring "
        f"schemas, reading statistics and diagnosing a live server. Version {version}.",
        "",
        "Every tool call runs in its own READ ONLY transaction bounded by "
        "statement_timeout, and every catalog query is parameterized, so no "
        "argument is ever concatenated into SQL. The one operation that can "
        "execute a statement, explainQuery with analyze, does so only after "
        "the plan is proven free of any data-modifying node. It speaks MCP over "
        "stdio and runs as a single binary; connections are libpq connection "
        "strings or a connections file.",
        "",
        "Install with `brew tap sqlambda/pg-licht && brew install pg-licht`, "
        f"or from the Debian, Rocky Linux and tarball packages on the releases page: {REPO}/releases",
        "",
        "## Docs",
        "",
        f"- [README]({REPO}/blob/{ref}/README.md): what it is, the safety model, "
        "topology, host capacity",
        f"- [INSTALL]({REPO}/blob/{ref}/INSTALL.md): Homebrew, deb, rpm, tarball, "
        "verifying, uninstalling",
        f"- [Manual]({manual}): configuration, connection strings, every tool and "
        "prompt in full",
        "",
        "## Tools",
        "",
    ]
    for t in tools:
        out.append(f"- [{t['name']}]({link(t['name'])}): "
                   f"{first_sentence(t.get('description', ''))}")
    out += ["", "## Prompts", ""]
    for p in prompts:
        out.append(f"- [{p['name']}]({link(p['name'])}): "
                   f"{first_sentence(p.get('description', ''))}")
    out += ["", "## Optional", "",
            f"- [Changelog]({REPO}/blob/{ref}/CHANGES.md): every release, "
            "with the reasoning behind each change", ""]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--binary", required=True)
    ap.add_argument("--man-html")
    ap.add_argument("--base-url", default="https://sqlambda.github.io/pg_licht")
    ap.add_argument("--ref", default="main",
                    help="git ref the README, INSTALL and CHANGES links point at")
    ap.add_argument("--output", help="write here instead of stdout")
    a = ap.parse_args()

    version, tools, prompts = describe(a.binary)
    if not tools or not prompts:
        raise RuntimeError(f"the binary listed {len(tools)} tools and "
                           f"{len(prompts)} prompts; refusing to write an empty file")
    text = render(version, tools, prompts, a.base_url.rstrip("/"), a.ref,
                  anchors(a.man_html) if a.man_html else None)
    if a.output:
        with open(a.output, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as e:
        print(f"generate-llms-txt: {e}", file=sys.stderr)
        sys.exit(1)
