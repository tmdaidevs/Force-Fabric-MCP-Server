import { getTokenForScope, KUSTO_SCOPE } from "../auth/fabricAuth.js";

export interface KqlColumn {
  ColumnName: string;
  DataType: string;
}

export interface KqlTable {
  TableName: string;
  Columns: KqlColumn[];
  Rows: unknown[][];
}

export interface KqlResult {
  Tables: KqlTable[];
}

export type KqlRow = Record<string, unknown>;

/** Default per-query timeout in milliseconds (2 minutes). */
const DEFAULT_QUERY_TIMEOUT_MS = 120_000;

function parseKqlTable(table: KqlTable): KqlRow[] {
  return table.Rows.map((row) => {
    const obj: KqlRow = {};
    table.Columns.forEach((col, i) => {
      obj[col.ColumnName] = row[i];
    });
    return obj;
  });
}

/**
 * Execute a KQL query against an Eventhouse/Kusto endpoint.
 */
export async function executeKqlQuery(
  clusterUri: string,
  database: string,
  query: string,
  timeoutMs: number = DEFAULT_QUERY_TIMEOUT_MS
): Promise<KqlRow[]> {
  const token = await getTokenForScope(KUSTO_SCOPE);

  // Normalize cluster URI
  const baseUri = clusterUri.replace(/\/+$/, "");
  const url = `${baseUri}/v1/rest/query`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({ db: database, csl: query }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`KQL query failed (${response.status}): ${errorText}`);
    }

    const result = (await response.json()) as KqlResult;
    if (result.Tables && result.Tables.length > 0) {
      return parseKqlTable(result.Tables[0]);
    }
    return [];
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Execute a KQL management command (.show, .alter, etc.)
 */
export async function executeKqlMgmt(
  clusterUri: string,
  database: string,
  command: string,
  timeoutMs: number = DEFAULT_QUERY_TIMEOUT_MS
): Promise<KqlRow[]> {
  const token = await getTokenForScope(KUSTO_SCOPE);

  const baseUri = clusterUri.replace(/\/+$/, "");
  const url = `${baseUri}/v1/rest/mgmt`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({ db: database, csl: command }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`KQL mgmt command failed (${response.status}): ${errorText}`);
    }

    const result = (await response.json()) as KqlResult;
    if (result.Tables && result.Tables.length > 0) {
      return parseKqlTable(result.Tables[0]);
    }
    return [];
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Run multiple KQL diagnostic commands in parallel and return named results.
 * Uses a concurrency limit to avoid overwhelming the cluster.
 */
export async function runKqlDiagnostics(
  clusterUri: string,
  database: string,
  commands: Record<string, { query: string; isMgmt: boolean }>,
  concurrency: number = 5
): Promise<Record<string, { rows?: KqlRow[]; error?: string }>> {
  const entries = Object.entries(commands);
  const results: Record<string, { rows?: KqlRow[]; error?: string }> = {};

  // Process in parallel batches
  for (let i = 0; i < entries.length; i += concurrency) {
    const batch = entries.slice(i, i + concurrency);
    const batchResults = await Promise.allSettled(
      batch.map(async ([name, cmd]) => {
        const rows = cmd.isMgmt
          ? await executeKqlMgmt(clusterUri, database, cmd.query)
          : await executeKqlQuery(clusterUri, database, cmd.query);
        return { name, rows };
      })
    );

    for (let j = 0; j < batchResults.length; j++) {
      const entry = batchResults[j];
      const name = batch[j][0];
      if (entry.status === "fulfilled") {
        results[name] = { rows: entry.value.rows };
      } else {
        const msg = entry.reason instanceof Error ? entry.reason.message : String(entry.reason);
        results[name] = { error: msg };
      }
    }
  }

  return results;
}
