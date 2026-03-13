#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { allTools, getToolByName, AUTH_TOOL_NAMES } from "./tools/index.js";
import { requireAuth } from "./auth/fabricAuth.js";

const server = new McpServer({
  name: "fabric-optimization-mcp-server",
  version: "1.0.0",
});

// Register all tools dynamically
for (const tool of allTools) {
  // Build a Zod schema from the JSON schema properties
  const properties = tool.inputSchema.properties ?? {};
  const required = tool.inputSchema.required ?? [];

  const zodShape: Record<string, z.ZodTypeAny> = {};

  for (const [key, prop] of Object.entries(properties)) {
    const p = prop as Record<string, unknown>;
    let zodType: z.ZodTypeAny;

    switch (p.type) {
      case "string":
        zodType = z.string().describe((p.description as string) ?? "");
        break;
      case "boolean":
        zodType = z.boolean().describe((p.description as string) ?? "");
        break;
      case "number":
        zodType = z.number().describe((p.description as string) ?? "");
        break;
      case "array": {
        const items = (p.items as Record<string, unknown> | undefined);
        const itemType = items?.type;
        const inner = itemType === "number" ? z.number() : z.string();
        zodType = z.array(inner).describe((p.description as string) ?? "");
        break;
      }
      case "object":
        zodType = z.record(z.string(), z.unknown()).describe((p.description as string) ?? "");
        break;
      default:
        zodType = z.unknown();
    }

    if (!required.includes(key)) {
      zodType = zodType.optional();
    }

    zodShape[key] = zodType;
  }

  server.tool(
    tool.name,
    tool.description,
    zodShape,
    async (args) => {
      try {
        // Enforce authentication for non-auth tools
        if (!AUTH_TOOL_NAMES.has(tool.name)) {
          requireAuth();
        }

        const handler = getToolByName(tool.name)?.handler;
        if (!handler) {
          return {
            content: [{ type: "text" as const, text: `Tool "${tool.name}" not found.` }],
          };
        }

        const result = await handler(args as Record<string, unknown>);
        return {
          content: [{ type: "text" as const, text: result }],
        };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return {
          content: [{ type: "text" as const, text: `Error: ${message}` }],
          isError: true,
        };
      }
    }
  );
}

// Start the server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Fabric Optimization MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
