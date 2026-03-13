import { getAccessToken } from "../auth/fabricAuth.js";

const FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1";

export interface FabricRequestOptions {
  method?: string;
  body?: unknown;
  params?: Record<string, string>;
}

export interface FabricWorkspace {
  id: string;
  displayName: string;
  description?: string;
  type: string;
  capacityId?: string;
}

export interface FabricItem {
  id: string;
  displayName: string;
  description?: string;
  type: string;
  workspaceId: string;
}

export interface FabricLakehouse extends FabricItem {
  properties?: {
    sqlEndpointProperties?: {
      connectionString?: string;
      id?: string;
      provisioningStatus?: string;
    };
    oneLakeTablesPath?: string;
    oneLakeFilesPath?: string;
  };
}

export interface FabricWarehouse extends FabricItem {
  properties?: {
    connectionString?: string;
    createdDate?: string;
    lastUpdatedTime?: string;
  };
}

export interface FabricEventhouse extends FabricItem {
  properties?: {
    databasesItemIds?: string[];
    ingestionServiceUri?: string;
    queryServiceUri?: string;
  };
}

export interface LakehouseTable {
  name: string;
  type: string;
  location: string;
  format: string;
}

export interface JobInstance {
  id: string;
  itemId: string;
  jobType: string;
  invokeType: string;
  status: string;
  startTimeUtc?: string;
  endTimeUtc?: string;
  failureReason?: {
    message: string;
    errorCode: string;
  };
}

async function fabricFetch<T>(path: string, options: FabricRequestOptions = {}): Promise<T> {
  const token = await getAccessToken();
  const { method = "GET", body, params } = options;

  let url = `${FABRIC_API_BASE}${path}`;
  if (params) {
    const searchParams = new URLSearchParams(params);
    url += `?${searchParams.toString()}`;
  }

  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };

  const fetchOptions: RequestInit = { method, headers };
  if (body) {
    fetchOptions.body = JSON.stringify(body);
  }

  const response = await fetch(url, fetchOptions);

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Fabric API error (${response.status}): ${errorText}`);
  }

  // Handle 202 Accepted (long-running operations)
  if (response.status === 202) {
    const location = response.headers.get("Location");
    const retryAfter = response.headers.get("Retry-After");
    return { location, retryAfter, status: 202 } as T;
  }

  // Handle 204 No Content
  if (response.status === 204) {
    return {} as T;
  }

  return response.json() as Promise<T>;
}

async function fabricFetchPaginated<T>(path: string): Promise<T[]> {
  const items: T[] = [];
  let continuationUri: string | null = `${FABRIC_API_BASE}${path}`;

  while (continuationUri) {
    const token = await getAccessToken();
    const response = await fetch(continuationUri, {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Fabric API error (${response.status}): ${errorText}`);
    }

    const data = await response.json() as { value: T[]; continuationUri?: string };
    items.push(...data.value);
    continuationUri = data.continuationUri ?? null;
  }

  return items;
}

// ──────────────────────────────────────────────
// Workspace operations
// ──────────────────────────────────────────────

export async function listWorkspaces(): Promise<FabricWorkspace[]> {
  return fabricFetchPaginated<FabricWorkspace>("/workspaces");
}

export async function getWorkspace(workspaceId: string): Promise<FabricWorkspace> {
  return fabricFetch<FabricWorkspace>(`/workspaces/${encodeURIComponent(workspaceId)}`);
}

export async function listWorkspaceItems(workspaceId: string, type?: string): Promise<FabricItem[]> {
  const path = type
    ? `/workspaces/${encodeURIComponent(workspaceId)}/items?type=${encodeURIComponent(type)}`
    : `/workspaces/${encodeURIComponent(workspaceId)}/items`;
  return fabricFetchPaginated<FabricItem>(path);
}

// ──────────────────────────────────────────────
// Lakehouse operations
// ──────────────────────────────────────────────

export async function listLakehouses(workspaceId: string): Promise<FabricLakehouse[]> {
  return fabricFetchPaginated<FabricLakehouse>(`/workspaces/${encodeURIComponent(workspaceId)}/lakehouses`);
}

export async function getLakehouse(workspaceId: string, lakehouseId: string): Promise<FabricLakehouse> {
  return fabricFetch<FabricLakehouse>(
    `/workspaces/${encodeURIComponent(workspaceId)}/lakehouses/${encodeURIComponent(lakehouseId)}`
  );
}

export async function listLakehouseTables(workspaceId: string, lakehouseId: string): Promise<LakehouseTable[]> {
  const result = await fabricFetch<{ data: LakehouseTable[] }>(
    `/workspaces/${encodeURIComponent(workspaceId)}/lakehouses/${encodeURIComponent(lakehouseId)}/tables`
  );
  return result.data ?? [];
}

export async function runLakehouseTableMaintenance(
  workspaceId: string,
  lakehouseId: string,
  jobType: string = "TableMaintenance",
  executionData?: Record<string, unknown>
): Promise<JobInstance> {
  const body = executionData ? { executionData: JSON.stringify(executionData) } : undefined;
  return fabricFetch<JobInstance>(
    `/workspaces/${encodeURIComponent(workspaceId)}/lakehouses/${encodeURIComponent(lakehouseId)}/jobs/instances`,
    { method: "POST", params: { jobType }, body }
  );
}

export async function getLakehouseJobStatus(
  workspaceId: string,
  lakehouseId: string,
  jobInstanceId: string
): Promise<JobInstance> {
  return fabricFetch<JobInstance>(
    `/workspaces/${encodeURIComponent(workspaceId)}/lakehouses/${encodeURIComponent(lakehouseId)}/jobs/instances/${encodeURIComponent(jobInstanceId)}`
  );
}

// ──────────────────────────────────────────────
// Warehouse operations
// ──────────────────────────────────────────────

export async function listWarehouses(workspaceId: string): Promise<FabricWarehouse[]> {
  return fabricFetchPaginated<FabricWarehouse>(`/workspaces/${encodeURIComponent(workspaceId)}/warehouses`);
}

export async function getWarehouse(workspaceId: string, warehouseId: string): Promise<FabricWarehouse> {
  return fabricFetch<FabricWarehouse>(
    `/workspaces/${encodeURIComponent(workspaceId)}/warehouses/${encodeURIComponent(warehouseId)}`
  );
}

// ──────────────────────────────────────────────
// Eventhouse operations
// ──────────────────────────────────────────────

export async function listEventhouses(workspaceId: string): Promise<FabricEventhouse[]> {
  return fabricFetchPaginated<FabricEventhouse>(`/workspaces/${encodeURIComponent(workspaceId)}/eventhouses`);
}

export async function getEventhouse(workspaceId: string, eventhouseId: string): Promise<FabricEventhouse> {
  return fabricFetch<FabricEventhouse>(
    `/workspaces/${encodeURIComponent(workspaceId)}/eventhouses/${encodeURIComponent(eventhouseId)}`
  );
}

// ──────────────────────────────────────────────
// KQL Database operations (under Eventhouse)
// ──────────────────────────────────────────────

export async function listKqlDatabases(workspaceId: string): Promise<FabricItem[]> {
  return fabricFetchPaginated<FabricItem>(`/workspaces/${encodeURIComponent(workspaceId)}/kqlDatabases`);
}

// ──────────────────────────────────────────────
// Semantic Model operations
// ──────────────────────────────────────────────

export async function listSemanticModels(workspaceId: string): Promise<FabricItem[]> {
  return fabricFetchPaginated<FabricItem>(`/workspaces/${encodeURIComponent(workspaceId)}/semanticModels`);
}

export interface DaxQueryResponse {
  results: Array<{
    tables: Array<{
      rows: Array<Record<string, unknown>>;
    }>;
  }>;
}

/**
 * Execute a DAX or DMV query against a Semantic Model via the Power BI REST API.
 * Supports both EVALUATE (DAX) and SELECT (DMV) queries.
 */
export async function executeSemanticModelQuery(
  workspaceId: string,
  semanticModelId: string,
  query: string
): Promise<Record<string, unknown>[]> {
  const token = await getAccessToken();
  const url = `https://api.fabric.microsoft.com/v1.0/myorg/groups/${encodeURIComponent(workspaceId)}/datasets/${encodeURIComponent(semanticModelId)}/executeQueries`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      queries: [{ query }],
      serializerSettings: { includeNulls: true },
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Query failed (${response.status}): ${errorText}`);
  }

  const result = await response.json() as DaxQueryResponse;
  if (result.results?.[0]?.tables?.[0]?.rows) {
    return result.results[0].tables[0].rows;
  }
  return [];
}

// Keep backward compat alias
export const executeSemanticModelDaxQuery = executeSemanticModelQuery;

// ──────────────────────────────────────────────
// Semantic Model Definition (model.bim) operations
// ──────────────────────────────────────────────

export interface SemanticModelDefinitionPart {
  path: string;
  payload: string;
  payloadType: "InlineBase64";
}

/**
 * Get the full definition (model.bim) of a Semantic Model.
 */
export async function getSemanticModelDefinition(
  workspaceId: string,
  semanticModelId: string
): Promise<SemanticModelDefinitionPart[]> {
  // This is a long-running operation
  const result = await fabricFetch<{ definition?: { parts: SemanticModelDefinitionPart[] } }>(
    `/workspaces/${encodeURIComponent(workspaceId)}/semanticModels/${encodeURIComponent(semanticModelId)}/getDefinition`,
    { method: "POST" }
  );

  if (result.definition?.parts) {
    return result.definition.parts;
  }

  // If 202, we need to poll — but for now return empty
  return [];
}

/**
 * Update the definition (model.bim) of a Semantic Model.
 */
export async function updateSemanticModelDefinition(
  workspaceId: string,
  semanticModelId: string,
  parts: SemanticModelDefinitionPart[]
): Promise<void> {
  await fabricFetch(
    `/workspaces/${encodeURIComponent(workspaceId)}/semanticModels/${encodeURIComponent(semanticModelId)}/updateDefinition`,
    {
      method: "POST",
      body: {
        definition: { parts },
      },
    }
  );
}

// ──────────────────────────────────────────────
// Notebook operations (create, run, poll, delete)
// ──────────────────────────────────────────────

/**
 * Create a temporary notebook with PySpark code, run it, wait for completion, delete it.
 * Used for Lakehouse table property fixes and Semantic Model sempy fixes.
 */
export async function runTemporaryNotebook(
  workspaceId: string,
  code: string,
  notebookName?: string,
  defaultLakehouseId?: string
): Promise<{ status: string; error?: string }> {
  const name = notebookName ?? `_force_temp_fix_${Date.now()}`;

  // Build notebook content as .py format (Fabric notebook definition)
  const pyContent = `# Fabric notebook source\n\n# METADATA ********************\n\n# META {\n# META   "kernel_info": {\n# META     "name": "synapse_pyspark"\n# META   }\n# META }\n\n# CELL ********************\n\n${code}\n\n# METADATA ********************\n\n# META {\n# META   "language": "python",\n# META   "language_group": "synapse_pyspark"\n# META }\n`;

  const payloadBase64 = Buffer.from(pyContent, "utf-8").toString("base64");

  // 1. Create notebook
  const createBody: Record<string, unknown> = {
    displayName: name,
    definition: {
      format: "ipynb",
      parts: [
        {
          path: "notebook-content.py",
          payload: payloadBase64,
          payloadType: "InlineBase64",
        },
      ],
    },
  };

  // Attach default lakehouse if provided
  if (defaultLakehouseId) {
    (createBody.definition as Record<string, unknown>).parts = [
      {
        path: "notebook-content.py",
        payload: payloadBase64,
        payloadType: "InlineBase64",
      },
    ];
  }

  let notebookId: string;
  try {
    const created = await fabricFetch<{ id: string }>(
      `/workspaces/${encodeURIComponent(workspaceId)}/notebooks`,
      { method: "POST", body: createBody }
    );
    notebookId = created.id;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    return { status: "Failed", error: `Failed to create notebook: ${msg}` };
  }

  // 2. Run notebook job
  let jobInstanceId: string | undefined;
  try {
    const runResult = await fabricFetch<{ id?: string; location?: string; status?: number }>(
      `/workspaces/${encodeURIComponent(workspaceId)}/items/${encodeURIComponent(notebookId)}/jobs/instances`,
      { method: "POST", params: { jobType: "RunNotebook" } }
    );
    jobInstanceId = runResult.id;

    // If 202, extract job ID from location header (handled by fabricFetch)
    if (runResult.location) {
      const match = (runResult.location as string).match(/instances\/([^/?]+)/);
      if (match) jobInstanceId = match[1];
    }
  } catch (error) {
    // Try to clean up the notebook
    try { await fabricFetch(`/workspaces/${encodeURIComponent(workspaceId)}/notebooks/${encodeURIComponent(notebookId)}`, { method: "DELETE" }); } catch { /* ignore */ }
    const msg = error instanceof Error ? error.message : String(error);
    return { status: "Failed", error: `Failed to run notebook: ${msg}` };
  }

  // 3. Poll for completion
  let finalStatus = "Unknown";
  let errorMsg: string | undefined;
  const maxPollTime = 5 * 60 * 1000; // 5 minutes
  const startTime = Date.now();

  while (Date.now() - startTime < maxPollTime) {
    await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5s between polls

    try {
      const status = await fabricFetch<{ status: string; failureReason?: { message: string } }>(
        `/workspaces/${encodeURIComponent(workspaceId)}/items/${encodeURIComponent(notebookId)}/jobs/instances/${encodeURIComponent(jobInstanceId ?? "latest")}`,
      );

      if (status.status === "Completed") {
        finalStatus = "Completed";
        break;
      } else if (status.status === "Failed" || status.status === "Cancelled" || status.status === "Deduped") {
        finalStatus = status.status;
        errorMsg = status.failureReason?.message;
        break;
      }
      // Still running — keep polling
    } catch {
      // Poll failed — try again
    }
  }

  if (finalStatus === "Unknown") {
    finalStatus = "Timeout";
    errorMsg = "Notebook did not complete within 5 minutes.";
  }

  // 4. Delete temp notebook
  try {
    await fabricFetch(
      `/workspaces/${encodeURIComponent(workspaceId)}/notebooks/${encodeURIComponent(notebookId)}`,
      { method: "DELETE" }
    );
  } catch {
    // Ignore delete errors — notebook might already be gone
  }

  return { status: finalStatus, error: errorMsg };
}

// ──────────────────────────────────────────────
// Capacity operations
// ──────────────────────────────────────────────

export async function listCapacities(): Promise<Array<{ id: string; displayName: string; sku: string; state: string; region: string }>> {
  return fabricFetchPaginated(`/capacities`);
}
