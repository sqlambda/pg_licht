# pg-licht

A PostgreSQL MCP (Model Context Protocol) server that exposes schema exploration tools over JSON-RPC 2.0. Available in C++ and TypeScript.

## Tools

| Tool | Description |
|------|-------------|
| `listSchemas` | All schemas with their table names |
| `listTables` | Tables in a schema with row counts, sizes, and scan statistics |
| `tableDetails` | Full table detail: columns (with pg_stats histograms), indexes (with usage counts), constraints, foreign keys, triggers |
| `searchTables` | Full-text search across table names and descriptions |
| `listFunctions` | Functions and procedures in a schema with metadata |
| `functionDetails` | Full function detail: source code, definition, trigger usage |
| `searchFunctions` | Full-text search across function names, source code, language, trigger names, and descriptions |

## C++

### Requirements

- CMake 3.31+
- clang++
- libpqxx
- nlohmann_json
- GTest (for tests)

### Build

```bash
cd cpp
cmake .
make
```

### Run

```bash
DATABASE_URL="postgresql://user:pass@host/dbname" ./pg_licht_mcp

# libpq key-value format also works
DATABASE_URL="host=localhost port=5432 dbname=mydb" ./pg_licht_mcp

# or as an argument
./pg_licht_mcp "postgresql://user:pass@host/dbname"
```

### Test

```bash
DATABASE_URL="port=5432 dbname=mydb" ./pg_licht_mcp_test
# or
DATABASE_URL="port=5432 dbname=mydb" ctest --output-on-failure
```

Tests create and destroy a temporary database named `pg_licht_test_<PID>` automatically.

## TypeScript

### Requirements

- Node.js 20+
- npm

### Build

```bash
cd typescript
npm install
npm run build
```

### Run

```bash
DATABASE_URL="postgresql://user:pass@host/dbname" node dist/index.js

# libpq key-value format also works
DATABASE_URL="port=5432 dbname=mydb" node dist/index.js
```

## MCP Configuration

### Claude Code (`claude mcp add`)

The `--scope` flag controls where the configuration is saved:

| Scope | Flag | Config file | Use when |
|-------|------|-------------|----------|
| Project (default) | `--scope project` | `.claude.json` in project root | shared with the repo |
| User | `--scope user` | `~/.claude.json` | available across all projects |
| Local | `--scope local` | `.claude.json.local` (git-ignored) | personal overrides, not committed |

**TypeScript:**
```bash
claude mcp add --scope project pg-licht \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  -- node /path/to/pg-licht/typescript/dist/index.js
```

**C++:**
```bash
claude mcp add --scope project pg-licht \
  -e DATABASE_URL="postgresql://user:pass@host/dbname" \
  -- /path/to/pg-licht/cpp/pg_licht_mcp
```

Replace `--scope project` with `--scope user` or `--scope local` as needed.

### Manual — `.claude.json`

You can also edit `.claude.json` (project root), `~/.claude.json` (user), or `.claude.json.local` (local) directly:

**TypeScript:**
```json
{
  "mcpServers": {
    "pg-licht": {
      "command": "node",
      "args": ["/path/to/pg-licht/typescript/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@host/dbname"
      }
    }
  }
}
```

**C++:**
```json
{
  "mcpServers": {
    "pg-licht": {
      "command": "/path/to/pg-licht/cpp/pg_licht_mcp",
      "env": {
        "DATABASE_URL": "postgresql://user:pass@host/dbname"
      }
    }
  }
}
```

### Claude Desktop

Add the same JSON block above under `mcpServers` in `claude_desktop_config.json`.

## License

Apache 2.0 — see [LICENSE](LICENSE).
