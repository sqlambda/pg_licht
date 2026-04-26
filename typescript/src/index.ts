#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";
import {
  parseConnStr,
  listSchemas,
  listTables,
  tableDetails,
  searchTables,
  listFunctions,
  functionDetails,
  searchFunctions,
  listEnums,
  enumDetails,
  searchEnums,
} from "./queries.js";

const connStr = process.env.DATABASE_URL ?? process.argv[2];
if (!connStr) {
  process.stderr.write(
    "Usage: pg-licht-mcp <connection-string>\n" +
    "Or set the DATABASE_URL environment variable.\n"
  );
  process.exit(1);
}

const client = new pg.Client(parseConnStr(connStr));
await client.connect();

// ---------------------------------------------------------------------------
// MCP server
// ---------------------------------------------------------------------------

const server = new Server(
  { name: "pg-licht-mcp", version: "1.0.0" },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "listSchemas",
      description: "return schema list with basic summaries",
      inputSchema: { type: "object", properties: {} },
    },
    {
      name: "listTables",
      description: "return table list with basic statistics",
      inputSchema: {
        type: "object",
        properties: { schema: { type: "string" } },
        required: ["schema"],
      },
    },
    {
      name: "tableDetails",
      description: "return table details like columns, foreign keys, indexes, triggers, data histograms",
      inputSchema: {
        type: "object",
        properties: {
          schema: { type: "string" },
          table: { type: "string" },
        },
        required: ["schema", "table"],
      },
    },
    {
      name: "searchTables",
      description: "return table list with basic statistics based on text search",
      inputSchema: {
        type: "object",
        properties: { web_search: { type: "string" } },
        required: ["web_search"],
      },
    },
    {
      name: "listFunctions",
      description: "return function and procedure list for a schema",
      inputSchema: {
        type: "object",
        properties: { schema: { type: "string" } },
        required: ["schema"],
      },
    },
    {
      name: "functionDetails",
      description: "return detailed function or procedure info including source and trigger usage",
      inputSchema: {
        type: "object",
        properties: {
          schema: { type: "string" },
          function: { type: "string" },
        },
        required: ["schema", "function"],
      },
    },
    {
      name: "searchFunctions",
      description: "search functions and procedures by name, source, language, trigger name, or description",
      inputSchema: {
        type: "object",
        properties: { web_search: { type: "string" } },
        required: ["web_search"],
      },
    },
    {
      name: "listEnums",
      description: "return enum type list for a schema with their values and descriptions",
      inputSchema: {
        type: "object",
        properties: { schema: { type: "string" } },
        required: ["schema"],
      },
    },
    {
      name: "enumDetails",
      description: "return enum type details including values and which columns use it",
      inputSchema: {
        type: "object",
        properties: {
          schema: { type: "string" },
          enum: { type: "string" },
        },
        required: ["schema", "enum"],
      },
    },
    {
      name: "searchEnums",
      description: "search enum types by name, values, or description",
      inputSchema: {
        type: "object",
        properties: { web_search: { type: "string" } },
        required: ["web_search"],
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args = {} } = req.params;

  try {
    let result: unknown;

    switch (name) {
      case "listSchemas":
        result = await listSchemas(client);
        break;

      case "listTables":
        result = await listTables(client, (args.schema as string) ?? "public");
        break;

      case "tableDetails":
        result = await tableDetails(
          client,
          (args.schema as string) ?? "public",
          (args.table as string) ?? ""
        );
        break;

      case "searchTables":
        result = await searchTables(client, (args.web_search as string) ?? "");
        break;

      case "listFunctions":
        result = await listFunctions(client, (args.schema as string) ?? "public");
        break;

      case "functionDetails":
        result = await functionDetails(
          client,
          (args.schema as string) ?? "public",
          (args.function as string) ?? ""
        );
        break;

      case "searchFunctions":
        result = await searchFunctions(client, (args.web_search as string) ?? "");
        break;

      case "listEnums":
        result = await listEnums(client, (args.schema as string) ?? "public");
        break;

      case "enumDetails":
        result = await enumDetails(
          client,
          (args.schema as string) ?? "public",
          (args.enum as string) ?? ""
        );
        break;

      case "searchEnums":
        result = await searchEnums(client, (args.web_search as string) ?? "");
        break;

      default:
        return {
          content: [{ type: "text", text: `Unknown tool: ${name}` }],
          isError: true,
        };
    }

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
      isError: false,
    };
  } catch (err) {
    return {
      content: [{ type: "text", text: `Error: ${(err as Error).message}` }],
      isError: true,
    };
  }
});

const transport = new StdioServerTransport();
await server.connect(transport);
