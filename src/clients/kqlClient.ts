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
  query: string
): Promise<KqlRow[]> {
  const token = await getTokenForScope(KUSTO_SCOPE);

  // Normalize cluster URI
  const baseUri = clusterUri.replace(/\/+$/, "");
  const url = `${baseUri}/v1/rest/query`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({ db: database, csl: query }),
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
}

/**
 * Execute a KQL management command (.show, .alter, etc.)
 */
export async function executeKqlMgmt(
  clusterUri: string,
  database: string,
  command: string
): Promise<KqlRow[]> {
  const token = await getTokenForScope(KUSTO_SCOPE);

  const baseUri = clusterUri.replace(/\/+$/, "");
  const url = `${baseUri}/v1/rest/mgmt`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({ db: database, csl: command }),
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
}

/**
 * Run multiple KQL diagnostic commands and return named results.
 */
export async function runKqlDiagnostics(
  clusterUri: string,
  database: string,
  commands: Record<string, { query: string; isMgmt: boolean }>
): Promise<Record<string, { rows?: KqlRow[]; error?: string }>> {
  const results: Record<string, { rows?: KqlRow[]; error?: string }> = {};

  for (const [name, cmd] of Object.entries(commands)) {
    try {
      const rows = cmd.isMgmt
        ? await executeKqlMgmt(clusterUri, database, cmd.query)
        : await executeKqlQuery(clusterUri, database, cmd.query);
      results[name] = { rows };
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      results[name] = { error: msg };
    }
  }

  return results;
}
