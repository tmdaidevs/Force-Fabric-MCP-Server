import { authTools } from "./auth.js";
import { lakehouseTools } from "./lakehouse.js";
import { warehouseTools } from "./warehouse.js";
import { eventhouseTools } from "./eventhouse.js";
import { semanticModelTools } from "./semanticModel.js";
import { workspaceTools } from "./workspace.js";

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, unknown>;
    required?: string[];
  };
  handler: (args: Record<string, unknown>) => Promise<string>;
}

// Auth tools don't require authentication
export const AUTH_TOOL_NAMES = new Set(authTools.map((t) => t.name));

export const allTools: ToolDefinition[] = [
  ...authTools,
  ...workspaceTools,
  ...lakehouseTools,
  ...warehouseTools,
  ...eventhouseTools,
  ...semanticModelTools,
] as ToolDefinition[];

export function getToolByName(name: string): ToolDefinition | undefined {
  return allTools.find((t) => t.name === name);
}
