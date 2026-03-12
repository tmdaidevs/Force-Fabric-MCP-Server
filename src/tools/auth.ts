import { login, logout, getAuthStatus } from "../auth/fabricAuth.js";
import type { AuthMethod } from "../auth/fabricAuth.js";

// ──────────────────────────────────────────────
// Tool: auth_login
// ──────────────────────────────────────────────

export async function authLogin(args: {
  method: string;
  tenantId?: string;
  clientId?: string;
  clientSecret?: string;
}): Promise<string> {
  const result = await login(args.method as AuthMethod, {
    tenantId: args.tenantId,
    clientId: args.clientId,
    clientSecret: args.clientSecret,
  });
  return result;
}

// ──────────────────────────────────────────────
// Tool: auth_status
// ──────────────────────────────────────────────

export async function authStatus(): Promise<string> {
  const status = getAuthStatus();

  if (!status.authenticated) {
    return [
      "## ❌ Not Authenticated",
      "",
      "You are not logged in to Fabric. Use `auth_login` to connect.",
      "",
      "### Available Login Methods",
      "",
      "| Method | Description |",
      "|--------|-------------|",
      "| `azure_cli` | Use existing Azure CLI session (`az login`) — **recommended for development** |",
      "| `interactive_browser` | Opens a browser window for interactive login |",
      "| `device_code` | Login via device code (useful for headless/remote environments) |",
      "| `vscode` | Use VS Code's Azure account |",
      "| `service_principal` | Use a service principal (requires tenantId, clientId, clientSecret) |",
      "| `default` | Auto-detect (tries CLI, managed identity, env vars, VS Code, etc.) |",
    ].join("\n");
  }

  return [
    "## ✅ Authenticated",
    "",
    `- **Method**: ${status.method}`,
    "",
    "You are connected to Fabric and ready to use optimization tools.",
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool: auth_logout
// ──────────────────────────────────────────────

export async function authLogout(): Promise<string> {
  return logout();
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const authTools = [
  {
    name: "auth_login",
    description:
      "Login to Microsoft Fabric. MUST be called before using any other tool. " +
      "Choose a login method: azure_cli (recommended), interactive_browser, device_code, vscode, service_principal, or default.",
    inputSchema: {
      type: "object" as const,
      properties: {
        method: {
          type: "string",
          description:
            "Authentication method. Options: " +
            "'azure_cli' (use existing az login session — recommended), " +
            "'interactive_browser' (opens browser for login), " +
            "'device_code' (device code flow for headless environments), " +
            "'vscode' (use VS Code Azure account), " +
            "'service_principal' (requires tenantId, clientId, clientSecret), " +
            "'default' (auto-detect best available method).",
        },
        tenantId: {
          type: "string",
          description: "Azure Tenant ID (optional, needed for interactive_browser, device_code, service_principal)",
        },
        clientId: {
          type: "string",
          description: "Azure App Registration Client ID (optional, needed for interactive_browser, service_principal)",
        },
        clientSecret: {
          type: "string",
          description: "Client secret (only for service_principal method)",
        },
      },
      required: ["method"],
    },
    handler: authLogin,
  },
  {
    name: "auth_status",
    description:
      "Check if you are currently authenticated to Microsoft Fabric. " +
      "Shows the login method and available authentication options.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
    handler: authStatus,
  },
  {
    name: "auth_logout",
    description: "Logout from Microsoft Fabric and clear cached credentials.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
    handler: authLogout,
  },
];
