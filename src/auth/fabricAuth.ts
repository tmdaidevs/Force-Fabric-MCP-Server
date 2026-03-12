import {
  DefaultAzureCredential,
  InteractiveBrowserCredential,
  AzureCliCredential,
  VisualStudioCodeCredential,
  DeviceCodeCredential,
} from "@azure/identity";
import type { TokenCredential, AccessToken } from "@azure/identity";

const FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default";
export const SQL_SCOPE = "https://database.windows.net/.default";
export const KUSTO_SCOPE = "https://kusto.kusto.windows.net/.default";

export type AuthMethod =
  | "azure_cli"
  | "interactive_browser"
  | "device_code"
  | "vscode"
  | "default"
  | "service_principal";

let credential: TokenCredential | null = null;
let cachedToken: AccessToken | null = null;
let currentAuthMethod: AuthMethod | null = null;
let isAuthenticated = false;

export function getAuthStatus(): { authenticated: boolean; method: AuthMethod | null } {
  return { authenticated: isAuthenticated, method: currentAuthMethod };
}

export function requireAuth(): void {
  if (!isAuthenticated || !credential) {
    throw new Error(
      "Not authenticated. Please use the `auth_login` tool first to connect to Fabric. " +
      "Available methods: azure_cli, interactive_browser, device_code, vscode, default, service_principal"
    );
  }
}

export async function login(method: AuthMethod, options?: {
  tenantId?: string;
  clientId?: string;
  clientSecret?: string;
}): Promise<string> {
  // Reset state
  credential = null;
  cachedToken = null;
  isAuthenticated = false;
  currentAuthMethod = null;

  switch (method) {
    case "azure_cli":
      credential = new AzureCliCredential();
      break;

    case "interactive_browser":
      credential = new InteractiveBrowserCredential({
        tenantId: options?.tenantId,
        clientId: options?.clientId,
      });
      break;

    case "device_code":
      credential = new DeviceCodeCredential({
        tenantId: options?.tenantId,
        clientId: options?.clientId,
        userPromptCallback: (info) => {
          // This message will be returned to the user via the tool response
          console.error(`\n🔑 Device Code Auth: ${info.message}\n`);
        },
      });
      break;

    case "vscode":
      credential = new VisualStudioCodeCredential({
        tenantId: options?.tenantId,
      });
      break;

    case "service_principal":
      if (!options?.clientId || !options?.clientSecret || !options?.tenantId) {
        throw new Error(
          "Service Principal login requires tenantId, clientId, and clientSecret."
        );
      }
      credential = new DefaultAzureCredential();
      // Set env vars for DefaultAzureCredential to pick up
      process.env.AZURE_TENANT_ID = options.tenantId;
      process.env.AZURE_CLIENT_ID = options.clientId;
      process.env.AZURE_CLIENT_SECRET = options.clientSecret;
      break;

    case "default":
      credential = new DefaultAzureCredential();
      break;

    default:
      throw new Error(
        `Unknown auth method "${method}". Available: azure_cli, interactive_browser, device_code, vscode, default, service_principal`
      );
  }

  // Verify the credential works by acquiring a token
  try {
    cachedToken = await credential.getToken(FABRIC_SCOPE);
    if (!cachedToken) {
      throw new Error("No token received");
    }
    isAuthenticated = true;
    currentAuthMethod = method;
    return `Successfully authenticated via "${method}". Token valid until ${new Date(cachedToken.expiresOnTimestamp).toISOString()}.`;
  } catch (error) {
    credential = null;
    const msg = error instanceof Error ? error.message : String(error);
    throw new Error(`Authentication failed with method "${method}": ${msg}`);
  }
}

export function logout(): string {
  credential = null;
  cachedToken = null;
  isAuthenticated = false;
  const prev = currentAuthMethod;
  currentAuthMethod = null;
  return prev
    ? `Logged out (was authenticated via "${prev}").`
    : "Not currently logged in.";
}

export async function getAccessToken(): Promise<string> {
  requireAuth();

  // Reuse cached token if still valid (with 5 min buffer)
  if (cachedToken && cachedToken.expiresOnTimestamp > Date.now() + 5 * 60 * 1000) {
    return cachedToken.token;
  }

  cachedToken = await credential!.getToken(FABRIC_SCOPE);
  if (!cachedToken) {
    isAuthenticated = false;
    throw new Error(
      "Token refresh failed. Please use `auth_login` to re-authenticate."
    );
  }
  return cachedToken.token;
}

export async function getTokenForScope(scope: string): Promise<string> {
  requireAuth();
  const token = await credential!.getToken(scope);
  if (!token) {
    throw new Error(`Failed to acquire token for scope "${scope}".`);
  }
  return token.token;
}
