#!/usr/bin/env python3
"""Generate the HTML reference for every pg_licht tool and prompt.

    tools/gen-reference.py --binary PATH --out DIR [--man-html NAME]

Writes DIR/index.html, one page per tool and one per prompt, in the shape of
the PostgreSQL manual's per-command pages (the format pg_laswell's reference
uses): Synopsis, Description, Parameters, Output, Scope, Example, See also.

Nothing about a tool is written by hand. Names, descriptions, parameters,
output shapes, annotations and sweep eligibility come from the binary, asked
over stdio exactly as an MCP client asks; prompt text comes from prompts/get.
Categories come from the man page's OPERATIONS subsections, and the list of
tools that can return values from your data from its "What reaches the caller"
subsection -- so moving an entry in the man page moves it here.

The one hand-written input is tools/reference/examples.json: an invented
example call for every tool and prompt. It is checked, not trusted. Every
example's arguments must satisfy the tool's inputSchema and its result the
tool's outputSchema, and every tool and prompt the binary lists must have one.
A mock that no longer matches the tool it illustrates fails the run.
"""

import argparse
import html
import importlib.util
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
E = html.escape

# The llms.txt generator already drives the binary safely -- a throwaway
# --config and a bare HOME, so a developer's own connections are never read.
_spec = importlib.util.spec_from_file_location(
    "generate_llms_txt", os.path.join(HERE, "generate-llms-txt.py"))
LLMS = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(LLMS)

# Arguments every connected tool accepts; described once on the index page
# rather than on all sixty-five pages.
COMMON_ARGS = ("connection", "instance", "replication_group", "group", "role")


def common_args(props):
    """Which of the shared arguments this tool actually carries.

    `role` is the sweep filter only for a tool the server publishes
    `replication_group` for -- it adds the two together, for per_server tools.
    Anywhere else a `role` property is the tool's own argument, as
    roleDependencies has, and belongs in Parameters rather than being filed
    under "arguments every tool takes". Same rule the server applies in
    dispatch and when building the schema: a tool's own argument wins.
    """
    shared = [k for k in COMMON_ARGS if k in props]
    if "replication_group" not in props and "role" in shared:
        shared.remove("role")
    return shared

# The sentence tools/list appends to a description saying where the answer
# varies. Split off into its own section on each page.
SCOPE_OPENERS = ("This reading is instance-wide", "A physical replica is byte-identical",
                 "The counters here are each server's own", "Never runs across more than one",
                 "Dead tuples and the last vacuum")

CSS = """
:root{--fg:#2b2d30;--bg:#fff;--muted:#62666d;--rule:#dfe1e4;--link:#0b5cad;
      --code-bg:#f6f7f8;--code-rule:#e3e5e8;--accent:#0b5cad;--req:#a3312b;
      --req-bg:#fbecea;--req-rule:#f1cdc9;--opt-bg:#edf3f9;--opt-rule:#cfe0ef;
      --note-bg:#f4f8fb;--warn-bg:#fdf6ee;--warn-rule:#c47f2c;--mock:#7a5c00;--mock-bg:#fff7da}
@media (prefers-color-scheme:dark){:root{--fg:#e3e4e6;--bg:#15171a;--muted:#9a9fa6;
      --rule:#2b2f34;--link:#79b0ee;--code-bg:#1d2024;--code-rule:#2c3035;--accent:#79b0ee;
      --req:#f0a39c;--req-bg:#3a2220;--req-rule:#5a302c;--opt-bg:#1d2a37;--opt-rule:#2d4256;
      --note-bg:#1a232c;--warn-bg:#2d2419;--warn-rule:#c9914a;--mock:#e9c96a;--mock-bg:#2f2a17}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
     font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif}
.wrap{max-width:54rem;margin:0 auto;padding:1.5rem 1.25rem 5rem}
a{color:var(--link);text-decoration:none}
a:hover{text-decoration:underline}
a:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:2px}
nav.crumb{display:flex;justify-content:space-between;gap:1rem;flex-wrap:wrap;
          border-bottom:1px solid var(--rule);padding-bottom:.6rem;margin-bottom:1.5rem;
          font-size:.85rem;color:var(--muted)}
nav.crumb .l,nav.crumb .r{display:flex;gap:1rem;flex-wrap:wrap}
h1{font-size:1.9rem;margin:.2rem 0 .1rem;font-weight:600;letter-spacing:-.01em;overflow-wrap:anywhere}
.purpose{color:var(--muted);margin:0 0 1rem;font-size:1.02rem}
.badges{margin:0 0 1.6rem}
h2{font-size:1.15rem;margin:2.2rem 0 .7rem;padding-bottom:.3rem;border-bottom:1px solid var(--rule);font-weight:600}
h3{font-size:1rem;margin:1.6rem 0 .5rem}
pre{background:var(--code-bg);border:1px solid var(--code-rule);border-radius:4px;
    padding:.85rem 1rem;overflow-x:auto;font-size:.84rem;line-height:1.5;margin:0 0 1rem}
pre.prose{white-space:pre-wrap;font-family:inherit;font-size:.9rem}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
p code,li code,dd code,td code{background:var(--code-bg);border:1px solid var(--code-rule);
    border-radius:3px;padding:.05rem .3rem;font-size:.86em}
dl.params{margin:0}
dl.params>dt{margin:1.1rem 0 .35rem;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.92rem;font-weight:600}
dl.params>dd{margin:0 0 0 1.4rem}
.tag{display:inline-block;margin:0 .4rem .3rem 0;padding:.02rem .45rem;border-radius:3px;
     font:600 .68rem/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
     text-transform:uppercase;letter-spacing:.04em;vertical-align:.08em;
     background:var(--opt-bg);color:var(--accent);border:1px solid var(--opt-rule)}
.tag.req{background:var(--req-bg);color:var(--req);border-color:var(--req-rule)}
.tag.type{text-transform:none;letter-spacing:0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.tag.mock{background:var(--mock-bg);color:var(--mock);border-color:var(--mock)}
.note{background:var(--note-bg);border-left:3px solid var(--accent);padding:.7rem .9rem;margin:0 0 1rem;font-size:.92rem}
.note.warn{background:var(--warn-bg);border-left-color:var(--warn-rule)}
.table-wrap{overflow-x:auto;margin:0 0 1rem}
table{border-collapse:collapse;width:100%;font-size:.88rem}
th,td{text-align:left;padding:.4rem .6rem;border-bottom:1px solid var(--rule);vertical-align:top}
th{font-weight:600;color:var(--muted)}
td.k{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:nowrap}
.idx{display:grid;grid-template-columns:repeat(auto-fill,minmax(15rem,1fr));gap:.4rem 2rem}
.idx section h3{margin:1.2rem 0 .3rem;font-size:.95rem}
.idx ul{list-style:none;margin:0;padding:0}
.idx li{margin:.15rem 0;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.88rem}
footer{border-top:1px solid var(--rule);margin-top:3rem;padding-top:1rem;color:var(--muted);font-size:.85rem}
"""


# --- the binary ---------------------------------------------------------------

class Binary:
    """One session with the binary, opened with the llms.txt generator's safe config."""

    def __enter__(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        config = os.path.join(self._tmp.name, "connections.ini")
        with open(config, "w") as f:
            f.write("[none]\nhost = /nonexistent\ndbname = none\n")
        os.chmod(config, 0o600)
        self.srv = LLMS.Server(self.binary, config)
        init = self.srv.call("initialize", {
            "protocolVersion": LLMS.PROTOCOL, "capabilities": {},
            "clientInfo": {"name": "gen-reference", "version": "1"}})
        self.srv.notify("notifications/initialized")
        self.version = init["serverInfo"]["version"]
        return self

    def __init__(self, binary):
        self.binary = binary

    def __exit__(self, *exc):
        try:
            self.srv.close()
        finally:
            self._tmp.cleanup()

    def listings(self):
        return (self.srv.list_all("tools/list", "tools"),
                self.srv.list_all("prompts/list", "prompts"))

    def prompt_texts(self, prompts, prompt_examples):
        """prompts/get for each prompt with its example arguments. Called only
        after the examples validated, so a bad mock is reported with every other
        problem rather than stopping the run here first."""
        texts = {}
        for p in prompts:
            args = prompt_examples[p["name"]].get("arguments", {})
            try:
                res = self.srv.call("prompts/get", {"name": p["name"], "arguments": args})
            except RuntimeError as e:
                raise RuntimeError(f"prompts/get {p['name']} with the example arguments: {e}")
            text = "\n\n".join(m["content"]["text"] for m in res.get("messages", [])
                               if m.get("content", {}).get("type") == "text")
            if not text.strip():
                raise RuntimeError(f"prompts/get {p['name']} returned no text")
            texts[p["name"]] = text
        return texts


# --- the man page ---------------------------------------------------------------

def read_man(path):
    """Tool categories in man page order, the prompt order, and the tools named in
    "What reaches the caller"."""
    categories, cat_of, prompts, exposure = [], {}, [], set()
    section = sub = None
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if line.startswith(".Sh "):
            section, sub = line[4:].strip(), None
            continue
        if line.startswith(".Ss "):
            sub = line[4:].strip()
            continue
        m = re.match(r"\.It Ic (\S+)(.*)", line)
        if section == "OPERATIONS" and sub and m:
            if sub not in categories:
                categories.append(sub)
            cat_of[m.group(1)] = sub
        elif section == "PROMPTS" and m:
            prompts.append(m.group(1))
        elif section == "SECURITY CONSIDERATIONS" and sub == "What reaches the caller" and m:
            # ".It Ic currentActivity , currentLocks"
            names = [m.group(1)] + re.findall(r",\s*(\w+)", m.group(2))
            exposure.update(n for n in names if n)
    return categories, cat_of, prompts, exposure


# --- validation -----------------------------------------------------------------

def _is(value, t):
    return {"object": isinstance(value, dict), "array": isinstance(value, list),
            "string": isinstance(value, str), "null": value is None,
            "boolean": isinstance(value, bool),
            "integer": isinstance(value, int) and not isinstance(value, bool),
            "number": isinstance(value, (int, float)) and not isinstance(value, bool),
            }.get(t, True)


def validate(value, schema, path, errors, strict_keys=False):
    """The JSON Schema subset these schemas use: type (with nullable unions),
    properties, required, items, additionalProperties."""
    if not isinstance(schema, dict):
        return
    t = schema.get("type")
    types = t if isinstance(t, list) else ([t] if t else [])
    if types and not any(_is(value, x) for x in types):
        errors.append(f"{path}: expected {'|'.join(types)}, got {type(value).__name__}")
        return
    if isinstance(value, dict):
        props = schema.get("properties", {})
        for k in schema.get("required", []):
            if k not in value:
                errors.append(f"{path}: missing required '{k}'")
        for k, v in value.items():
            if k in props:
                validate(v, props[k], f"{path}.{k}", errors)
            elif schema.get("additionalProperties") is False or (strict_keys and props):
                errors.append(f"{path}: '{k}' is not a property of this schema")
    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for i, v in enumerate(value):
            validate(v, schema["items"], f"{path}[{i}]", errors)


def check_examples(tools, prompts, examples):
    errors = []
    for t in tools:
        ex = examples["tools"].get(t["name"])
        if ex is None:
            errors.append(f"{t['name']}: no example in tools/reference/examples.json")
            continue
        validate(ex.get("arguments", {}), t["inputSchema"], f"{t['name']} arguments",
                 errors, strict_keys=True)
        if "outputSchema" in t:
            validate(ex.get("result"), t["outputSchema"], f"{t['name']} result", errors)
    for p in prompts:
        ex = examples["prompts"].get(p["name"])
        if ex is None:
            errors.append(f"{p['name']}: no example in tools/reference/examples.json")
            continue
        known = {a["name"] for a in p.get("arguments", [])}
        for a in p.get("arguments", []):
            if a.get("required") and a["name"] not in ex.get("arguments", {}):
                errors.append(f"{p['name']} arguments: missing required '{a['name']}'")
        for k in ex.get("arguments", {}):
            if k not in known:
                errors.append(f"{p['name']} arguments: '{k}' is not an argument of this prompt")
    names = {t["name"] for t in tools} | {p["name"] for p in prompts}
    stale = sorted((set(examples["tools"]) | set(examples["prompts"])) - names)
    if stale:
        errors.append("examples for tools or prompts the binary does not list: " + ", ".join(stale))
    return errors


# --- rendering ------------------------------------------------------------------

def split_scope(description):
    cut = min((description.find(" " + o) for o in SCOPE_OPENERS if " " + o in description),
              default=-1)
    if cut < 0:
        return description, ""
    return description[:cut].rstrip(), description[cut:].strip()


def paragraphs(text):
    """A description is one long string. Break it every few sentences so it reads
    as prose, never inside one."""
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z`\"'(])", " ".join(text.split()))
    out, cur = [], []
    for s in sentences:
        cur.append(s)
        if len(" ".join(cur)) > 420:
            out.append(" ".join(cur))
            cur = []
    if cur:
        out.append(" ".join(cur))
    return out


def inline(text):
    """Escape, then mark identifiers a reader would type as code."""
    t = E(text)
    return re.sub(r"(?<![\w/&#])([a-z]+(?:_[a-z0-9]+)+|[a-z]+[A-Z]\w+)(?![\w/])",
                  r"<code>\1</code>", t)


def type_label(schema):
    t = schema.get("type", "any")
    return " | ".join(t) if isinstance(t, list) else t


def head(title):
    return ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width,initial-scale=1">',
            f'<title>{E(title)}</title>',
            f'<style>{CSS}</style></head><body><div class="wrap">']


def foot():
    return ['<footer>Generated by <code>tools/gen-reference.py</code> from the '
            'pg_licht binary and its man page. Examples are mocked: every value in '
            'them is invented, and each is checked against the tool\'s own schema.'
            '</footer></div></body></html>']


def crumb(category, prev_name, next_name, man_html):
    left = f'<a href="index.html">Reference</a><span>{E(category)}</span>'
    right = []
    if prev_name:
        right.append(f'<a href="{E(prev_name)}.html">← {E(prev_name)}</a>')
    if next_name:
        right.append(f'<a href="{E(next_name)}.html">{E(next_name)} →</a>')
    right.append(f'<a href="{man_html}">Manual</a>')
    return (f'<nav class="crumb"><div class="l">{left}</div>'
            f'<div class="r">{"".join(right)}</div></nav>')


def tool_page(t, category, siblings, prev_name, next_name, example, exposed, man_html):
    props = t["inputSchema"].get("properties", {})
    required = set(t["inputSchema"].get("required", []))
    shared = common_args(props)
    own = [k for k in props if k not in shared]
    own.sort(key=lambda k: (k not in required, k))
    sweeps = [k for k in ("instance", "replication_group", "group") if k in props]
    body, scope = split_scope(t.get("description", ""))

    h = head(f"{t['name']} — pg_licht")
    h.append(crumb(category, prev_name, next_name, f"{man_html}#{t['name']}"))
    h.append(f'<h1>{E(t["name"])}</h1>')
    h.append(f'<p class="purpose">{inline(LLMS.first_sentence(body))}</p>')
    badges = []
    if t.get("annotations", {}).get("readOnlyHint"):
        badges.append('<span class="tag">read-only</span>')
    if "connection" in props:
        badges.append('<span class="tag">any connection</span>')
    for s in sweeps:
        badges.append(f'<span class="tag">sweeps {E(s.replace("_", " "))}</span>')
    if t["name"] in exposed:
        badges.append('<span class="tag req">can return data values</span>')
    h.append(f'<p class="badges">{"".join(badges)}</p>')

    sig = ", ".join(k if k in required else f"[{k}]" for k in own)
    h.append('<h2>Synopsis</h2>')
    h.append(f'<pre><code>{E(t["name"])}({E(sig)})</code></pre>')

    h.append('<h2>Description</h2>')
    for i, para in enumerate(paragraphs(body)):
        if i == 0:
            para = para[:1].upper() + para[1:]
        h.append(f'<p>{inline(para)}</p>')
    if t["name"] in exposed:
        h.append('<div class="note warn">This operation can return values that came from '
                 'your data. It never changes anything and never returns rows, but read '
                 '<a href="../index.html#data">what reaches the caller</a> before connecting '
                 'it to a database whose contents are sensitive.</div>')

    h.append('<h2>Parameters</h2>')
    if own:
        h.append('<dl class="params">')
        for k in own:
            s = props[k]
            tag = '<span class="tag req">required</span>' if k in required else '<span class="tag">optional</span>'
            h.append(f'<dt>{E(k)} {tag}<span class="tag type">{E(type_label(s))}</span></dt>')
            h.append(f'<dd>{inline(s.get("description", ""))}</dd>')
        h.append('</dl>')
    else:
        h.append('<p>None of its own.</p>')
    common = shared
    if common:
        h.append('<p class="note">Also accepts '
                 + ", ".join(f"<code>{E(k)}</code>" for k in common)
                 + ', described once under <a href="index.html#common">arguments every tool takes</a>.</p>')

    h.append('<h2>Output</h2>')
    out = t.get("outputSchema", {})
    if out.get("properties"):
        h.append(f'<p>{inline(out.get("description", ""))}</p>')
        h.append('<div class="table-wrap"><table><tr><th>Field</th><th>Type</th></tr>')
        for k, s in out["properties"].items():
            h.append(f'<tr><td class="k">{E(k)}</td><td>{E(type_label(s))}</td></tr>')
        h.append('</table></div>')
    else:
        h.append(f'<p>{inline(out.get("description", "A JSON object."))}</p>')

    if scope:
        h.append('<h2>Scope</h2>')
        h.append(f'<p>{inline(scope)}</p>')

    h.append('<h2>Example <span class="tag mock">mocked data</span></h2>')
    call = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": t["name"], "arguments": example.get("arguments", {})}}
    h.append('<h3>Request</h3>')
    h.append(f'<pre><code>{E(json.dumps(call, indent=2, ensure_ascii=False))}</code></pre>')
    h.append('<h3>Result</h3>')
    h.append(f'<pre><code>{E(json.dumps(example.get("result"), indent=2, ensure_ascii=False))}</code></pre>')
    h.append('<p class="note">Invented values on a fictional <code>shop</code> database, '
             'shaped by and checked against this tool\'s output schema. Real output is '
             'returned as <code>structuredContent</code> to clients that negotiate MCP '
             '2025-06-18 or later.</p>')

    others = [s for s in siblings if s != t["name"]]
    if others:
        h.append('<h2>See also</h2>')
        h.append('<p>' + ", ".join(f'<a href="{E(s)}.html"><code>{E(s)}</code></a>' for s in others) + '</p>')
    h += foot()
    return "\n".join(h)


def prompt_page(p, prev_name, next_name, example, text, man_html):
    h = head(f"{p['name']} — pg_licht")
    h.append(crumb("Prompts", prev_name, next_name, f"{man_html}#{p['name']}"))
    h.append(f'<h1>{E(p["name"])}</h1>')
    h.append(f'<p class="purpose">{inline(p.get("description", ""))}</p>')
    h.append('<p class="badges"><span class="tag">prompt</span><span class="tag">read-only tools</span></p>')
    args = p.get("arguments", [])
    sig = ", ".join(a["name"] if a.get("required") else f"[{a['name']}]" for a in args)
    h.append('<h2>Synopsis</h2>')
    h.append(f'<pre><code>{E(p["name"])}({E(sig)})</code></pre>')
    h.append('<h2>Arguments</h2>')
    if args:
        h.append('<dl class="params">')
        for a in args:
            tag = '<span class="tag req">required</span>' if a.get("required") else '<span class="tag">optional</span>'
            h.append(f'<dt>{E(a["name"])} {tag}</dt><dd>{inline(a.get("description", ""))}</dd>')
        h.append('</dl>')
    else:
        h.append('<p>None.</p>')
    h.append('<h2>What it asks the model to do</h2>')
    h.append('<p>The text below is exactly what <code>prompts/get</code> returns for the '
             'example arguments. A client inserts it as the opening message of a '
             'conversation, and the model then calls the tools it names.</p>')
    h.append(f'<pre class="prose">{E(text)}</pre>')
    h.append('<h2>Example <span class="tag mock">mocked arguments</span></h2>')
    call = {"jsonrpc": "2.0", "id": 1, "method": "prompts/get",
            "params": {"name": p["name"], "arguments": example.get("arguments", {})}}
    h.append(f'<pre><code>{E(json.dumps(call, indent=2, ensure_ascii=False))}</code></pre>')
    h += foot()
    return "\n".join(h)


def index_page(version, tools, prompts, categories, cat_of, prompt_order, exposed, man_html):
    by_cat = {c: [t for t in tools if cat_of.get(t["name"]) == c] for c in categories}
    by_cat = {c: v for c, v in by_cat.items() if v}
    h = head("pg_licht reference")
    h.append(f'<nav class="crumb"><div class="l"><a href="../index.html">pg_licht</a>'
             f'<span>Reference</span></div><div class="r"><a href="{man_html}">Manual</a>'
             f'<a href="../llms.txt">llms.txt</a></div></nav>')
    h.append('<h1>Reference</h1>')
    h.append(f'<p class="purpose">Every tool and prompt pg_licht {E(version)} offers: '
             f'{len(tools)} read-only tools in {len(by_cat)} groups, and {len(prompts)} '
             'guided prompts. Each page is generated from the binary itself.</p>')
    h.append('<div class="idx">')
    for c, ts in by_cat.items():
        h.append(f'<section><h3><a href="#{E(slug(c))}">{E(c)}</a></h3><ul>')
        for t in ts:
            h.append(f'<li><a href="{E(t["name"])}.html">{E(t["name"])}</a></li>')
        h.append('</ul></section>')
    h.append('<section><h3><a href="#prompts">Prompts</a></h3><ul>')
    for name in prompt_order:
        h.append(f'<li><a href="{E(name)}.html">{E(name)}</a></li>')
    h.append('</ul></section></div>')

    h.append('<h2 id="common">Arguments every tool takes</h2>')
    h.append('<dl class="params">'
             '<dt>connection <span class="tag">optional</span></dt><dd>Which configured '
             'connection to ask. Omitted, the default one. One server holds any number of '
             'connections, so this is how a single pg_licht answers for a whole fleet.</dd>'
             '<dt>instance, replication_group, group <span class="tag">optional</span></dt>'
             '<dd>Ask every member of a topology label instead, and get one result per '
             'member in configuration order. Offered only by tools whose answer really '
             'differs across that set: an instance-wide reading is not swept across the '
             'databases of one postmaster, and a catalog read is not swept across replicas '
             'that are byte-identical.</dd>'
             '<dt>role <span class="tag">optional</span></dt><dd>With a sweep, only '
             'members observed right now to be <code>primary</code> or <code>replica</code>.</dd>'
             '</dl>'
             '<p class="note">These five names are read by the server itself, out of the '
             'arguments of every call. A tool that declares an argument of its own by one '
             'of those names keeps it &mdash; <code>roleDependencies</code> takes a '
             '<code>role</code>, naming the role to ask about &mdash; and cannot be '
             'targeted by that selector. The tool\'s argument wins, in its published '
             'schema and in the call alike.</p>')

    # Which tools take each is read from their schemas, not written here, so
    # the lists cannot fall behind a tool that gains one.
    def takers(arg):
        names = sorted(t["name"] for t in tools
                       if arg in t["inputSchema"].get("properties", {}))
        return ", ".join(f'<a href="{E(n)}.html"><code>{E(n)}</code></a>' for n in names)
    h.append('<h2 id="matching">Arguments that match by a string</h2>')
    h.append('<p>Two arguments narrow an answer by a string, and they match differently.</p>')
    h.append('<dl class="params">'
             '<dt>pattern</dt><dd>A literal, case-insensitive substring of a name. There are '
             'no wildcards: <code>_</code> and <code>%</code> match themselves, so '
             '<code>user_</code> finds <code>user_x</code> and not <code>users</code>, and '
             'nothing is stemmed. Taken by ' + takers("pattern") + '.</dd>'
             '<dt>web_search</dt><dd>Full-text search, not a substring: PostgreSQL\'s '
             '<code>websearch_to_tsquery</code> in English. Words are stemmed, so '
             '<code>order</code> finds <code>orders</code>; names are split into words at '
             'underscores and capitals, so <code>customer_id</code> contributes '
             '<code>customer</code>; <code>"a phrase"</code>, <code>or</code> and '
             '<code>-word</code> work; a fragment of a word matches nothing &mdash; use a '
             'listing\'s <code>pattern</code> for that. Taken by ' + takers("web_search") +
             ', each of which says what it searches.</dd>'
             '</dl>'
             f'<p class="note">The full rules are under <a href="{E(man_html)}'
             '#Matching:_pattern_and_web_search">Matching: pattern and web_search</a> '
             'in the manual.</p>')

    for c, ts in by_cat.items():
        h.append(f'<h2 id="{E(slug(c))}">{E(c)}</h2>')
        h.append('<div class="table-wrap"><table><tr><th>Tool</th><th>What it answers</th></tr>')
        for t in ts:
            body, _ = split_scope(t.get("description", ""))
            flag = ' <span class="tag req">data values</span>' if t["name"] in exposed else ""
            h.append(f'<tr><td class="k"><a href="{E(t["name"])}.html">{E(t["name"])}</a></td>'
                     f'<td>{inline(LLMS.first_sentence(body))}{flag}</td></tr>')
        h.append('</table></div>')

    h.append('<h2 id="prompts">Prompts</h2>')
    h.append('<p>A prompt is a ready-made investigation: the client inserts its text as '
             'the opening message, and the model works through the tools it names.</p>')
    h.append('<div class="table-wrap"><table><tr><th>Prompt</th><th>What it works out</th></tr>')
    pd = {p["name"]: p for p in prompts}
    for name in prompt_order:
        h.append(f'<tr><td class="k"><a href="{E(name)}.html">{E(name)}</a></td>'
                 f'<td>{inline(pd[name].get("description", ""))}</td></tr>')
    h.append('</table></div>')
    h += foot()
    return "\n".join(h)


def slug(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# --- main -----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--binary", required=True)
    ap.add_argument("--out", required=True, help="directory to write the reference into")
    ap.add_argument("--man", default=os.path.join(ROOT, "cpp", "man", "pg_licht_mcp.1"))
    ap.add_argument("--examples", default=os.path.join(HERE, "reference", "examples.json"))
    ap.add_argument("--man-html", default="../pg_licht_mcp.1.html",
                    help="link to the rendered man page, relative to the reference pages")
    ap.add_argument("--landing", help="landing page template with {{PLACEHOLDERS}} to fill")
    ap.add_argument("--landing-out", help="where to write the filled landing page")
    a = ap.parse_args()
    if bool(a.landing) != bool(a.landing_out):
        ap.error("--landing and --landing-out go together")

    examples = json.load(open(a.examples, encoding="utf-8"))
    with Binary(a.binary) as binary:
        version = binary.version
        tools, prompts = binary.listings()
        categories, cat_of, prompt_order, exposed = read_man(a.man)
        run(a, binary, version, tools, prompts, examples, categories, cat_of,
            prompt_order, exposed)


def run(a, binary, version, tools, prompts, examples, categories, cat_of,
        prompt_order, exposed):

    errors = check_examples(tools, prompts, examples)
    uncategorized = [t["name"] for t in tools if t["name"] not in cat_of]
    if uncategorized:
        errors.append("no man page OPERATIONS entry, so no category, for: " + ", ".join(uncategorized))
    listed = {p["name"] for p in prompts}
    if set(prompt_order) != listed:
        errors.append(f"man page PROMPTS and prompts/list disagree: {sorted(set(prompt_order) ^ listed)}")
    unknown = sorted(exposed - {t["name"] for t in tools})
    if unknown:
        errors.append("'What reaches the caller' names tools the binary does not list: " + ", ".join(unknown))
    if errors:
        raise RuntimeError("the reference cannot be generated:\n  " + "\n  ".join(errors))
    texts = binary.prompt_texts(prompts, examples["prompts"])

    os.makedirs(a.out, exist_ok=True)
    ordered = [t for c in categories for t in tools if cat_of.get(t["name"]) == c]
    names = [t["name"] for t in ordered]
    for i, t in enumerate(ordered):
        c = cat_of[t["name"]]
        siblings = [x["name"] for x in ordered if cat_of[x["name"]] == c]
        page = tool_page(t, c, siblings, names[i - 1] if i else None,
                         names[i + 1] if i + 1 < len(names) else None,
                         examples["tools"][t["name"]], exposed, a.man_html)
        with open(os.path.join(a.out, f"{t['name']}.html"), "w", encoding="utf-8") as f:
            f.write(page)
    pd = {p["name"]: p for p in prompts}
    for i, name in enumerate(prompt_order):
        page = prompt_page(pd[name], prompt_order[i - 1] if i else None,
                           prompt_order[i + 1] if i + 1 < len(prompt_order) else None,
                           examples["prompts"][name], texts[name], a.man_html)
        with open(os.path.join(a.out, f"{name}.html"), "w", encoding="utf-8") as f:
            f.write(page)
    with open(os.path.join(a.out, "index.html"), "w", encoding="utf-8") as f:
        f.write(index_page(version, ordered, prompts, categories, cat_of, prompt_order,
                           exposed, a.man_html))
    if a.landing:
        by_cat = {}
        for t in ordered:
            by_cat.setdefault(cat_of[t["name"]], []).append(t["name"])
        template = open(a.landing, encoding="utf-8").read()
        values = {
            "VERSION": E(version), "TOOLS": str(len(ordered)), "PROMPTS": str(len(prompt_order)),
            "CATEGORIES": str(len(by_cat)),
            "CATEGORY_LIST": "\n".join(
                f'<li><a href="reference/index.html#{E(slug(c))}">{E(c)}</a>'
                f'<span>{len(ts)}</span></li>' for c, ts in by_cat.items()),
            "EXPOSED_COUNT": str(len(exposed)),
        }
        missing = sorted(set(re.findall(r"\{\{([A-Z_]+)\}\}", template)) - set(values))
        if missing:
            raise RuntimeError(f"{a.landing} uses unknown placeholders: {missing}")
        for k, v in values.items():
            template = template.replace("{{" + k + "}}", v)
        with open(a.landing_out, "w", encoding="utf-8") as f:
            f.write(template)
    print(f"reference: {len(ordered)} tools, {len(prompt_order)} prompts, "
          f"{len({cat_of[t['name']] for t in ordered})} categories, version {version} -> {a.out}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, json.JSONDecodeError) as e:
        print(f"gen-reference: {e}", file=sys.stderr)
        sys.exit(1)
