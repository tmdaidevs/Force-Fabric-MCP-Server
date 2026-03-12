import { getTokenForScope } from "../auth/fabricAuth.js";

const ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com";
const STORAGE_SCOPE = "https://storage.azure.com/.default";

// ──────────────────────────────────────────────
// OneLake ADLS Gen2 REST API Client
// ──────────────────────────────────────────────

async function onelakeFetch(path: string): Promise<Response> {
  const token = await getTokenForScope(STORAGE_SCOPE);
  const url = `${ONELAKE_DFS}/${path}`;

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OneLake API error (${response.status}): ${errorText}`);
  }

  return response;
}

/**
 * List files in a OneLake directory (ADLS Gen2 path listing).
 */
export async function listOneLakeFiles(
  workspaceName: string,
  lakehouseName: string,
  relativePath: string
): Promise<string[]> {
  const filesystem = `${workspaceName}`;
  const directory = `${lakehouseName}.Lakehouse/${relativePath}`;

  const token = await getTokenForScope(STORAGE_SCOPE);
  const url = `${ONELAKE_DFS}/${encodeURIComponent(filesystem)}?resource=filesystem&directory=${encodeURIComponent(directory)}&recursive=false`;

  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OneLake list error (${response.status}): ${errorText}`);
  }

  const data = await response.json() as { paths?: Array<{ name: string; isDirectory?: string; contentLength?: string }> };
  return (data.paths ?? []).map(p => p.name);
}

/**
 * Read a text file from OneLake.
 */
export async function readOneLakeFile(
  workspaceName: string,
  lakehouseName: string,
  relativePath: string
): Promise<string> {
  const fullPath = `${encodeURIComponent(workspaceName)}/${encodeURIComponent(lakehouseName)}.Lakehouse/${relativePath}`;
  const response = await onelakeFetch(fullPath);
  return response.text();
}

// ──────────────────────────────────────────────
// Delta Log Types
// ──────────────────────────────────────────────

export interface DeltaMetadata {
  id?: string;
  format?: { provider: string };
  schemaString?: string;
  partitionColumns?: string[];
  configuration?: Record<string, string>;
  createdTime?: number;
}

export interface DeltaCommitInfo {
  timestamp?: number;
  operation?: string;
  operationParameters?: Record<string, string>;
  operationMetrics?: Record<string, string>;
  engineInfo?: string;
  isBlindAppend?: boolean;
}

export interface DeltaAddAction {
  path: string;
  size: number;
  modificationTime?: number;
  partitionValues?: Record<string, string>;
  stats?: string;
}

export interface DeltaRemoveAction {
  path: string;
  deletionTimestamp?: number;
  dataChange?: boolean;
}

export interface DeltaLogEntry {
  metaData?: DeltaMetadata;
  commitInfo?: DeltaCommitInfo;
  add?: DeltaAddAction;
  remove?: DeltaRemoveAction;
  protocol?: { minReaderVersion?: number; minWriterVersion?: number };
}

export interface DeltaLogAnalysis {
  metadata: DeltaMetadata | null;
  commits: DeltaCommitInfo[];
  activeFiles: DeltaAddAction[];
  totalVersions: number;
  errors: string[];
}

// ──────────────────────────────────────────────
// Delta Log Reader
// ──────────────────────────────────────────────

/**
 * Read and parse the Delta log for a table.
 * Reads the last checkpoint + subsequent JSON files.
 */
export async function readDeltaLog(
  workspaceName: string,
  lakehouseName: string,
  tableName: string
): Promise<DeltaLogAnalysis> {
  const result: DeltaLogAnalysis = {
    metadata: null,
    commits: [],
    activeFiles: [],
    totalVersions: 0,
    errors: [],
  };

  const logDir = `Tables/${tableName}/_delta_log`;

  try {
    // List delta log files
    const files = await listOneLakeFiles(workspaceName, lakehouseName, logDir);

    // Filter to JSON log files only
    const jsonFiles = files
      .filter(f => f.endsWith(".json"))
      .sort()
      .slice(-50); // Read last 50 versions max for performance

    result.totalVersions = jsonFiles.length;

    // Read each JSON log file
    for (const filePath of jsonFiles) {
      try {
        // Extract just the filename part after the lakehouse path
        const relativePath = filePath.includes("_delta_log")
          ? `Tables/${tableName}/_delta_log/${filePath.split("_delta_log/").pop()}`
          : filePath;

        const content = await readOneLakeFile(workspaceName, lakehouseName, relativePath);

        // Delta log files are newline-delimited JSON
        const lines = content.split("\n").filter(l => l.trim());
        for (const line of lines) {
          try {
            const entry = JSON.parse(line) as DeltaLogEntry;

            if (entry.metaData) {
              result.metadata = entry.metaData;
            }
            if (entry.commitInfo) {
              result.commits.push(entry.commitInfo);
            }
            if (entry.add) {
              result.activeFiles.push(entry.add);
            }
          } catch {
            // Skip malformed lines
          }
        }
      } catch (err) {
        result.errors.push(`Failed to read ${filePath}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  } catch (err) {
    result.errors.push(`Failed to list delta log: ${err instanceof Error ? err.message : String(err)}`);
  }

  return result;
}

// ──────────────────────────────────────────────
// Delta Log Analysis Helpers
// ──────────────────────────────────────────────

export function getPartitionColumns(log: DeltaLogAnalysis): string[] {
  return log.metadata?.partitionColumns ?? [];
}

export function getTableConfig(log: DeltaLogAnalysis): Record<string, string> {
  return log.metadata?.configuration ?? {};
}

export function getLastOperation(log: DeltaLogAnalysis, operation: string): DeltaCommitInfo | null {
  for (let i = log.commits.length - 1; i >= 0; i--) {
    if (log.commits[i].operation === operation) return log.commits[i];
  }
  return null;
}

export function countOperations(log: DeltaLogAnalysis): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const c of log.commits) {
    const op = c.operation ?? "UNKNOWN";
    counts[op] = (counts[op] ?? 0) + 1;
  }
  return counts;
}

export function getFileSizeStats(log: DeltaLogAnalysis): {
  totalFiles: number;
  totalSizeBytes: number;
  avgFileSizeMB: number;
  smallFileCount: number;
  largeFileCount: number;
} {
  const files = log.activeFiles;
  const totalFiles = files.length;
  const totalSizeBytes = files.reduce((s, f) => s + (f.size ?? 0), 0);
  const avgFileSizeMB = totalFiles > 0 ? totalSizeBytes / totalFiles / (1024 * 1024) : 0;
  const smallFileCount = files.filter(f => f.size < 25 * 1024 * 1024).length; // <25MB
  const largeFileCount = files.filter(f => f.size > 1024 * 1024 * 1024).length; // >1GB

  return { totalFiles, totalSizeBytes, avgFileSizeMB, smallFileCount, largeFileCount };
}

export function daysSinceTimestamp(timestampMs: number): number {
  return Math.floor((Date.now() - timestampMs) / (86400 * 1000));
}
