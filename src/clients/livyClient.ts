import { getAccessToken } from "../auth/fabricAuth.js";

// ──────────────────────────────────────────────
// Livy API client for running Spark SQL directly
// No notebooks needed — uses Fabric Livy sessions
// ──────────────────────────────────────────────

const FABRIC_API_BASE = "https://api.fabric.microsoft.com";
const LIVY_API_VERSION = "2023-12-01";

export interface LivySessionInfo {
  id: string;
  state: string;
}

export interface LivyStatementResult {
  status: "ok" | "error";
  output?: string;
  error?: string;
  traceback?: string[];
}

async function livyFetch<T>(
  url: string,
  options: { method?: string; body?: unknown } = {}
): Promise<{ status: number; data: T; raw: Response }> {
  const token = await getAccessToken();
  const { method = "GET", body } = options;

  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };

  const fetchOptions: RequestInit = { method, headers };
  if (body) {
    fetchOptions.body = JSON.stringify(body);
  }

  const response = await fetch(url, fetchOptions);
  const status = response.status;

  if (status === 204 || status === 200 || status === 202) {
    const text = await response.text();
    const data = text ? (JSON.parse(text) as T) : ({} as T);
    return { status, data, raw: response };
  }

  const errorText = await response.text();
  throw new Error(`Livy API error (${status}): ${errorText}`);
}

function buildSessionBaseUrl(workspaceId: string, lakehouseId: string): string {
  return (
    `${FABRIC_API_BASE}/v1/workspaces/${encodeURIComponent(workspaceId)}` +
    `/lakehouses/${encodeURIComponent(lakehouseId)}` +
    `/livyapi/versions/${LIVY_API_VERSION}/sessions`
  );
}

/**
 * Create a Livy Spark session with retry/backoff for cold-start scenarios.
 */
async function createSession(
  workspaceId: string,
  lakehouseId: string,
  maxRetries = 3
): Promise<{ sessionId: string; baseUrl: string }> {
  const baseUrl = buildSessionBaseUrl(workspaceId, lakehouseId);

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const { status, data } = await livyFetch<LivySessionInfo>(baseUrl, {
        method: "POST",
        body: {},
      });

      if (status === 202 || status === 200) {
        return { sessionId: String(data.id), baseUrl };
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      // Retry on transient errors
      if (/429|500|502|503|504/.test(msg) && attempt < maxRetries - 1) {
        const wait = 10_000 * (attempt + 1);
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
      throw error;
    }
  }

  throw new Error("Failed to create Livy session after retries.");
}

/**
 * Wait for a Livy session to reach 'idle' state.
 */
async function waitForSession(
  sessionUrl: string,
  timeoutMs = 300_000
): Promise<void> {
  const start = Date.now();
  let pollInterval = 5_000;

  while (Date.now() - start < timeoutMs) {
    await new Promise((r) => setTimeout(r, pollInterval));
    pollInterval = Math.min(pollInterval * 1.3, 15_000);

    const { data } = await livyFetch<{ state: string }>(sessionUrl);
    const state = data.state;

    if (state === "idle") return;
    if (["dead", "error", "killed", "shutting_down"].includes(state)) {
      throw new Error(`Livy session entered terminal state: ${state}`);
    }
  }

  throw new Error(`Livy session did not become idle within ${timeoutMs / 1000}s.`);
}

/**
 * Submit a PySpark statement and wait for its result.
 */
async function executeStatement(
  sessionUrl: string,
  code: string,
  timeoutMs = 300_000
): Promise<LivyStatementResult> {
  const statementsUrl = `${sessionUrl}/statements`;

  const { data: stmtInfo } = await livyFetch<{ id: number }>(statementsUrl, {
    method: "POST",
    body: { code, kind: "pyspark" },
  });

  const stmtUrl = `${statementsUrl}/${stmtInfo.id}`;
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    await new Promise((r) => setTimeout(r, 3_000));

    const { data: stmt } = await livyFetch<{
      state: string;
      output?: {
        status: string;
        data?: { "text/plain"?: string };
        evalue?: string;
        traceback?: string[];
      };
    }>(stmtUrl);

    if (stmt.state === "available") {
      const output = stmt.output;
      if (!output) {
        return { status: "error", error: "No output from statement" };
      }
      if (output.status === "ok") {
        return {
          status: "ok",
          output: output.data?.["text/plain"] ?? "",
        };
      }
      return {
        status: "error",
        error: output.evalue ?? "Unknown error",
        traceback: output.traceback,
      };
    }

    if (["error", "cancelled"].includes(stmt.state)) {
      return { status: "error", error: `Statement ${stmt.state}` };
    }
  }

  return { status: "error", error: "Statement execution timeout" };
}

/**
 * Delete a Livy session (best-effort cleanup).
 */
async function deleteSession(sessionUrl: string): Promise<void> {
  try {
    await livyFetch(sessionUrl, { method: "DELETE" });
  } catch {
    // Ignore cleanup errors
  }
}

// ──────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────

export interface LivyJobResult {
  table: string;
  fixId: string;
  description: string;
  status: "ok" | "error";
  output?: string;
  error?: string;
}

/**
 * Run Spark SQL fixes on lakehouse tables via Livy API.
 * Creates a session, executes one statement per table, cleans up.
 *
 * @returns Per-table results with status and output.
 */
export async function runSparkFixesViaLivy(
  workspaceId: string,
  lakehouseId: string,
  commands: Array<{ table: string; fixId: string; description: string; code: string }>
): Promise<{ results: LivyJobResult[]; sessionCleanedUp: boolean }> {
  const results: LivyJobResult[] = [];

  const { sessionId, baseUrl } = await createSession(workspaceId, lakehouseId);
  const sessionUrl = `${baseUrl}/${sessionId}`;

  try {
    await waitForSession(sessionUrl);

    for (const cmd of commands) {
      const result = await executeStatement(sessionUrl, cmd.code);
      results.push({
        table: cmd.table,
        fixId: cmd.fixId,
        description: cmd.description,
        status: result.status,
        output: result.output,
        error: result.error,
      });
    }
  } finally {
    await deleteSession(sessionUrl);
  }

  return { results, sessionCleanedUp: true };
}
