import {
  listEventhouses,
  getEventhouse,
  listKqlDatabases,
} from "../clients/fabricClient.js";
import { runKqlDiagnostics, executeKqlMgmt } from "../clients/kqlClient.js";
import { renderRuleReport } from "./ruleEngine.js";
import type { RuleResult } from "./ruleEngine.js";
import type { FabricEventhouse } from "../clients/fabricClient.js";
import type { KqlRow } from "../clients/kqlClient.js";

// ──────────────────────────────────────────────
// Input validation — prevent KQL injection
// ──────────────────────────────────────────────

const SAFE_KQL_NAME = /^[a-zA-Z0-9_\- .]+$/;

function validateKqlName(value: string, label: string): void {
  if (!SAFE_KQL_NAME.test(value)) {
    throw new Error(`Invalid ${label}: must be alphanumeric/underscore/dash/dot only.`);
  }
}

// ──────────────────────────────────────────────
// Policy parsing helpers — normalise across Kusto API versions
// ──────────────────────────────────────────────

function policyString(row: KqlRow, ...keys: string[]): string {
  for (const k of keys) {
    const v = row[k];
    if (v !== undefined && v !== null && v !== "") {
      return typeof v === "string" ? v : JSON.stringify(v);
    }
  }
  return "";
}

function isTruthy(val: unknown): boolean {
  if (typeof val === "boolean") return val;
  return String(val).toLowerCase() === "true";
}

function isFalsy(val: unknown): boolean {
  if (typeof val === "boolean") return !val;
  return String(val).toLowerCase() === "false";
}

// ──────────────────────────────────────────────
// Tool: eventhouse_list
// ──────────────────────────────────────────────

export async function eventhouseList(args: { workspaceId: string }): Promise<string> {
  const eventhouses = await listEventhouses(args.workspaceId);

  if (eventhouses.length === 0) {
    return "No eventhouses found in this workspace.";
  }

  const lines = eventhouses.map((eh: FabricEventhouse) =>
    [
      `- **${eh.displayName}** (ID: ${eh.id})`,
      eh.properties?.queryServiceUri
        ? `  Query URI: ${eh.properties.queryServiceUri}`
        : null,
      eh.properties?.ingestionServiceUri
        ? `  Ingestion URI: ${eh.properties.ingestionServiceUri}`
        : null,
      eh.properties?.databasesItemIds?.length
        ? `  KQL Databases: ${eh.properties.databasesItemIds.length}`
        : null,
    ].filter(Boolean).join("\n")
  );

  return `## Eventhouses in workspace ${args.workspaceId}\n\n${lines.join("\n\n")}`;
}

// ──────────────────────────────────────────────
// Tool: eventhouse_list_kql_databases
// ──────────────────────────────────────────────

export async function eventhouseListKqlDatabases(args: {
  workspaceId: string;
}): Promise<string> {
  const databases = await listKqlDatabases(args.workspaceId);

  if (databases.length === 0) {
    return "No KQL databases found in this workspace.";
  }

  const lines = databases.map(
    (db) => `- **${db.displayName}** (ID: ${db.id}, Type: ${db.type})`
  );

  return `## KQL Databases in workspace ${args.workspaceId}\n\n${lines.join("\n")}`;
}

// ──────────────────────────────────────────────
// KQL Diagnostic Commands
// ──────────────────────────────────────────────

const KQL_DIAGNOSTICS = {
  tableDetails: {
    query: ".show tables details",
    isMgmt: true,
  },
  cachingPolicy: {
    query: ".show database policy caching",
    isMgmt: true,
  },
  retentionPolicy: {
    query: ".show database policy retention",
    isMgmt: true,
  },
  extentStats: {
    query: `.show database extents | summarize ExtentCount=count(), TotalRows=sum(RowCount), TotalOriginalSizeMB=sum(OriginalSize)/1024/1024, TotalCompressedSizeMB=sum(CompressedSize)/1024/1024 by TableName | order by TotalOriginalSizeMB desc`,
    isMgmt: true,
  },
  materializedViews: {
    query: ".show materialized-views",
    isMgmt: true,
  },
  tableCachingPolicies: {
    query: ".show table * policy caching",
    isMgmt: true,
  },
  tableRetentionPolicies: {
    query: ".show table * policy retention",
    isMgmt: true,
  },
  ingestionBatching: {
    query: ".show table * policy ingestionbatching",
    isMgmt: true,
  },
  streamingIngestion: {
    query: ".show table * policy streamingingestion",
    isMgmt: true,
  },
  // ── Deep Diagnostics ──
  queryPerformance: {
    query: `.show commands-and-queries
      | where StartedOn > ago(7d)
      | where State != "InProgress"
      | summarize QueryCount=count(),
                AvgDurationSec=avg(Duration)/1s,
                MaxDurationSec=max(Duration)/1s,
                P95DurationSec=percentile(Duration, 95)/1s,
                FailedCount=countif(State == "Failed")
        by Database, CommandType
      | order by AvgDurationSec desc`,
    isMgmt: true,
  },
  slowQueries: {
    query: `.show commands-and-queries
      | where StartedOn > ago(7d)
      | where State == "Completed"
      | top 15 by Duration desc
      | project StartedOn, Duration, CommandType,
               QueryText=substring(Text, 0, 200),
               MemoryPeak, TotalCpu, User=ClientRequestProperties["x-ms-user-id"]`,
    isMgmt: true,
  },
  failedCommands: {
    query: `.show commands-and-queries
      | where StartedOn > ago(7d)
      | where State == "Failed"
      | top 10 by StartedOn desc
      | project StartedOn, CommandType,
               FailureReason, QueryText=substring(Text, 0, 200)`,
    isMgmt: true,
  },
  ingestionFailures: {
    query: `.show ingestion failures
      | where FailedOn > ago(7d)
      | summarize FailureCount=count(),
                  LastFailure=max(FailedOn)
        by Table, ErrorCode
      | order by FailureCount desc`,
    isMgmt: true,
  },
  dataFreshness: {
    query: `.show tables details
      | project TableName, TotalRowCount, TotalOriginalSize,
               MinExtentsCreationTime, MaxExtentsCreationTime,
               HotRowCount
      | order by TotalRowCount desc`,
    isMgmt: true,
  },
  updatePolicies: {
    query: ".show table * policy update",
    isMgmt: true,
  },
  partitioningPolicies: {
    query: ".show table * policy partitioning",
    isMgmt: true,
  },
  // ── Schema & Data Quality ──
  tableSchemas: {
    query: `.show database schema as json | project DatabaseSchema`,
    isMgmt: true,
  },
  columnStats: {
    query: `.show database extents
      | summarize
          ExtentCount=count(),
          AvgRowsPerExtent=avg(RowCount),
          MinRows=min(RowCount),
          MaxRows=max(RowCount),
          TotalCompressedMB=sum(CompressedSize)/1024/1024,
          TotalOriginalMB=sum(OriginalSize)/1024/1024
        by TableName
      | extend CompressionRatio=iff(TotalOriginalMB > 0, round((1.0 - TotalCompressedMB/TotalOriginalMB) * 100, 1), 0.0)
      | order by TotalOriginalMB desc`,
    isMgmt: true,
  },
  mergePolicy: {
    query: ".show table * policy merge",
    isMgmt: true,
  },
  encodingPolicy: {
    query: ".show table * policy encoding",
    isMgmt: true,
  },
  rowOrderPolicy: {
    query: ".show table * policy row_order",
    isMgmt: true,
  },
  continuousExports: {
    query: ".show continuous-exports",
    isMgmt: true,
  },
  functions: {
    query: ".show functions",
    isMgmt: true,
  },
  journalEntries: {
    query: `.show journal
      | where EventTimestamp > ago(7d)
      | summarize Count=count() by Event
      | order by Count desc`,
    isMgmt: true,
  },
  // ── Capacity / Usage ──
  storageByTable: {
    query: `.show database extents
      | summarize
          TotalSizeGB=round(sum(OriginalSize)/1024/1024/1024, 2),
          CompressedSizeGB=round(sum(CompressedSize)/1024/1024/1024, 2)
        by TableName
      | order by TotalSizeGB desc
      | limit 20`,
    isMgmt: true,
  },
};

// ──────────────────────────────────────────────
// Analysis helpers
// ──────────────────────────────────────────────

function formatBytes(mb: number): string {
  if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`;
  return `${mb.toFixed(1)} MB`;
}

function analyzeExtentStats(rows: KqlRow[]): string[] {
  if (rows.length === 0) return ["No extent data found."];

  const lines: string[] = [
    "| Table | Extents | Rows | Original Size | Compressed Size | Compression |",
    "|-------|---------|------|--------------|----------------|-------------|",
  ];

  const fragmented: string[] = [];
  let totalOrigMB = 0;
  let totalCompMB = 0;

  for (const r of rows) {
    const extents = r.ExtentCount as number;
    const totalRows = r.TotalRows as number;
    const origMB = r.TotalOriginalSizeMB as number;
    const compMB = r.TotalCompressedSizeMB as number;
    const ratio = origMB > 0 ? ((1 - compMB / origMB) * 100).toFixed(0) : "0";

    totalOrigMB += origMB;
    totalCompMB += compMB;

    lines.push(
      `| ${r.TableName} | ${extents} | ${totalRows.toLocaleString()} | ${formatBytes(origMB)} | ${formatBytes(compMB)} | ${ratio}% |`
    );

    // Flag tables with many small extents (fragmentation)
    if (extents > 100 && totalRows > 0) {
      const avgRowsPerExtent = totalRows / extents;
      if (avgRowsPerExtent < 100000) {
        fragmented.push(`${r.TableName} (${extents} extents, avg ${Math.round(avgRowsPerExtent)} rows/extent)`);
      }
    }
  }

  lines.push("");
  lines.push(`**Total storage**: ${formatBytes(totalOrigMB)} original → ${formatBytes(totalCompMB)} compressed`);

  if (fragmented.length > 0) {
    lines.push(
      "",
      `**🔴 Fragmented tables (${fragmented.length})** — Too many small extents, run merge:`,
      ...fragmented.map(f => `- ${f}`),
      "→ Run `.merge table <name>` to consolidate extents."
    );
  }

  return lines;
}

function analyzeCachingPolicy(dbPolicy: KqlRow[], tablePolicies: KqlRow[]): string[] {
  const lines: string[] = [];

  if (dbPolicy.length > 0) {
    const hotCache = policyString(dbPolicy[0], "Policy", "CachingPolicy", "CachingPolicyObject");
    lines.push(`**Database caching policy**: ${hotCache || JSON.stringify(dbPolicy[0])}`);
  }

  if (tablePolicies.length > 0) {
    lines.push("", "**Per-table caching overrides:**");
    lines.push("| Table | Policy |");
    lines.push("|-------|--------|");
    for (const t of tablePolicies) {
      const policy = policyString(t, "Policy", "CachingPolicy");
      if (policy) {
        const name = t.EntityName ?? t.TableName ?? "?";
        lines.push(`| ${name} | ${policy} |`);
      }
    }
  }

  return lines;
}

function analyzeMaterializedViews(rows: KqlRow[]): string[] {
  if (rows.length === 0) return ["No materialized views configured."];

  const lines: string[] = [
    `Found **${rows.length} materialized view(s)**:`,
    "",
    "| View | Source | Healthy | Auto-Updated |",
    "|------|--------|---------|-------------|",
  ];

  const unhealthy: string[] = [];

  for (const r of rows) {
    const name = r.Name ?? r.MaterializedViewName ?? "?";
    const source = r.SourceTable ?? "?";
    const healthy = r.IsHealthy ?? r.IsEnabled ?? "?";
    const autoUpdate = r.AutoUpdateDefinition ?? "?";
    lines.push(`| ${name} | ${source} | ${healthy} | ${autoUpdate} |`);

    if (isFalsy(healthy)) {
      unhealthy.push(String(name));
    }
  }

  if (unhealthy.length > 0) {
    lines.push(
      "",
      `**🔴 Unhealthy materialized views**: ${unhealthy.join(", ")}`,
      "→ These views are not updating. Check materialization lag and fix underlying issues."
    );
  }

  return lines;
}

// ──────────────────────────────────────────────
// Tool: eventhouse_optimization_recommendations
// ──────────────────────────────────────────────

export async function eventhouseOptimizationRecommendations(args: {
  workspaceId: string;
  eventhouseId: string;
}): Promise<string> {
  const [eventhouse, kqlDatabases] = await Promise.all([
    getEventhouse(args.workspaceId, args.eventhouseId),
    listKqlDatabases(args.workspaceId),
  ]);

  const rules: RuleResult[] = [];
  const header: string[] = [];

  const queryUri = eventhouse.properties?.queryServiceUri;
  const ingestionUri = eventhouse.properties?.ingestionServiceUri;

  header.push(
    "## 🔌 Connection Info",
    "",
    `- **Query URI**: ${queryUri ?? "not available"}`,
    `- **Ingestion URI**: ${ingestionUri ?? "not available"}`,
    `- **KQL Databases**: ${kqlDatabases.length}`,
    ""
  );

  // EH-001: Query URI available
  rules.push({
    id: "EH-001", rule: "Query Endpoint Available", category: "Availability", severity: "HIGH",
    status: queryUri ? "PASS" : "FAIL",
    details: queryUri ? "Query URI is available for KQL analysis." : "No query URI — cannot perform live analysis.",
    recommendation: "Ensure the Eventhouse is properly provisioned.",
  });

  if (!queryUri) {
    return renderRuleReport(`Eventhouse Analysis: ${eventhouse.displayName}`, new Date().toISOString(), header, rules);
  }

  const ehDbIds = new Set(eventhouse.properties?.databasesItemIds ?? []);
  const ehDatabases = kqlDatabases.filter(db => ehDbIds.has(db.id));
  const databasesToAnalyze = ehDatabases.length > 0 ? ehDatabases : kqlDatabases;

  for (const db of databasesToAnalyze) {
    header.push(`---`, "", `## 📊 KQL Database: ${db.displayName}`, "");

    const res = await runKqlDiagnostics(queryUri, db.displayName, KQL_DIAGNOSTICS);
    const pre = `[${db.displayName}] `;

    // Helper
    const cnt = (key: string) => ((res as Record<string, {rows?: KqlRow[]}>)[key]?.rows?.length ?? 0);
    const err = (key: string) => ((res as Record<string, {error?: string}>)[key]?.error);
    const kqlRows = (key: string) => ((res as Record<string, {rows?: KqlRow[]}>)[key]?.rows ?? []);

    // ── Storage header ──
    if (res.extentStats?.rows && res.extentStats.rows.length > 0) {
      header.push(...analyzeExtentStats(res.extentStats.rows), "");
    }

    // EH-002: Fragmentation
    const fragmented = kqlRows("extentStats").filter(x => {
      const extents = x.ExtentCount as number;
      const totalRows = x.TotalRows as number;
      return extents > 100 && totalRows > 0 && (totalRows / extents) < 100000;
    });
    rules.push({
      id: "EH-002", rule: `${pre}No Extent Fragmentation`, category: "Performance", severity: "HIGH",
      status: err("extentStats") ? "ERROR" : fragmented.length === 0 ? "PASS" : "FAIL",
      details: err("extentStats") ?? (fragmented.length === 0 ? "No tables with excessive fragmentation." : `${fragmented.length} table(s) fragmented: ${fragmented.slice(0,3).map(x=>x.TableName).join(", ")}`),
      recommendation: "Run .merge table <name> to consolidate small extents.",
    });

    // EH-003: Compression
    const poorCompression = kqlRows("columnStats").filter(x =>
      typeof x.CompressionRatio === 'number' && x.CompressionRatio < 40 && (x.TotalOriginalMB as number) > 100
    );
    rules.push({
      id: "EH-003", rule: `${pre}Good Compression Ratio`, category: "Performance", severity: "MEDIUM",
      status: err("columnStats") ? "ERROR" : poorCompression.length === 0 ? "PASS" : "WARN",
      details: err("columnStats") ?? (poorCompression.length === 0 ? "All tables have ≥40% compression." : `${poorCompression.length} table(s) with poor compression (<40%).`),
      recommendation: "Review encoding policies and data types for poorly compressed tables.",
    });

    // EH-004: Caching Policy
    rules.push({
      id: "EH-004", rule: `${pre}Caching Policy Configured`, category: "Performance", severity: "MEDIUM",
      status: err("cachingPolicy") ? "ERROR" : cnt("cachingPolicy") > 0 ? "PASS" : "WARN",
      details: err("cachingPolicy") ?? (cnt("cachingPolicy") > 0 ? "Database caching policy defined." : "No explicit caching policy set."),
      recommendation: "Set hot cache period matching your query patterns.",
    });

    // EH-005: Retention Policy
    rules.push({
      id: "EH-005", rule: `${pre}Retention Policy Configured`, category: "Data Management", severity: "MEDIUM",
      status: err("retentionPolicy") ? "ERROR" : cnt("retentionPolicy") > 0 ? "PASS" : "WARN",
      details: err("retentionPolicy") ?? (cnt("retentionPolicy") > 0 ? "Database retention policy defined." : "No retention policy — data retained indefinitely."),
      recommendation: "Set retention policy to manage storage costs.",
    });

    // EH-006: Materialized Views Health
    const matViews = kqlRows("materializedViews");
    const unhealthyViews = matViews.filter(v => isFalsy(v.IsHealthy));
    rules.push({
      id: "EH-006", rule: `${pre}Materialized Views Healthy`, category: "Reliability", severity: "HIGH",
      status: err("materializedViews") ? "ERROR" : matViews.length === 0 ? "N/A" : unhealthyViews.length === 0 ? "PASS" : "FAIL",
      details: err("materializedViews") ?? (matViews.length === 0 ? "No materialized views configured." : unhealthyViews.length === 0 ? `All ${matViews.length} view(s) healthy.` : `${unhealthyViews.length} unhealthy view(s).`),
      recommendation: "Fix unhealthy materialized views — check materialization lag.",
    });

    // EH-007: Data Freshness
    const staleTables = kqlRows("dataFreshness").filter(x => {
      const ts = x.MaxExtentsCreationTime as string | null;
      if (!ts) return false;
      return (Date.now() - new Date(ts).getTime()) / (86400 * 1000) > 7;
    });
    rules.push({
      id: "EH-007", rule: `${pre}Data Is Fresh (<7 days)`, category: "Data Quality", severity: "MEDIUM",
      status: err("dataFreshness") ? "ERROR" : staleTables.length === 0 ? "PASS" : "WARN",
      details: err("dataFreshness") ?? (staleTables.length === 0 ? "All tables have recent data." : `${staleTables.length} table(s) stale (>7 days).`),
      recommendation: "Verify ingestion pipelines are running.",
    });

    // EH-008: Query Performance
    const slowPatterns = kqlRows("queryPerformance").filter(x => typeof x.AvgDurationSec === 'number' && x.AvgDurationSec > 30);
    rules.push({
      id: "EH-008", rule: `${pre}No Slow Query Patterns`, category: "Performance", severity: "HIGH",
      status: err("queryPerformance") ? "ERROR" : slowPatterns.length === 0 ? "PASS" : "FAIL",
      details: err("queryPerformance") ?? (slowPatterns.length === 0 ? "No query patterns averaging >30s." : `${slowPatterns.length} pattern(s) averaging >30s.`),
      recommendation: "Optimize slow KQL queries. Consider materialized views for expensive aggregations.",
    });

    // EH-009: Failed Commands
    rules.push({
      id: "EH-009", rule: `${pre}No Recent Failures`, category: "Reliability", severity: "MEDIUM",
      status: err("failedCommands") ? "ERROR" : cnt("failedCommands") === 0 ? "PASS" : "WARN",
      details: err("failedCommands") ?? (cnt("failedCommands") === 0 ? "No failed commands in last 7 days." : `${cnt("failedCommands")} failed command(s).`),
      recommendation: "Investigate failed commands to fix errors.",
    });

    // EH-010: Ingestion Failures
    const totalIngestionFailures = kqlRows("ingestionFailures").reduce((s, x) => s + ((x.FailureCount as number) ?? 0), 0);
    rules.push({
      id: "EH-010", rule: `${pre}No Ingestion Failures`, category: "Reliability", severity: "HIGH",
      status: err("ingestionFailures") ? (err("ingestionFailures")!.includes("Semantic error") ? "N/A" : "ERROR") : totalIngestionFailures === 0 ? "PASS" : "FAIL",
      details: err("ingestionFailures") ? (err("ingestionFailures")!.includes("Semantic error") ? "Ingestion failures command not available." : err("ingestionFailures")!) : (totalIngestionFailures === 0 ? "No ingestion failures in last 7 days." : `${totalIngestionFailures} ingestion failure(s).`),
      recommendation: "Check ingestion pipeline configuration and data source connectivity.",
    });

    // EH-011: Streaming Ingestion
    const streamingEnabled = kqlRows("streamingIngestion").filter(x => {
      const ps = policyString(x, "Policy", "StreamingIngestionPolicy");
      return ps.includes("true") || ps.includes("Enabled");
    });
    rules.push({
      id: "EH-011", rule: `${pre}Streaming Ingestion Config`, category: "Performance", severity: "INFO",
      status: "PASS",
      details: streamingEnabled.length > 0 ? `${streamingEnabled.length} table(s) have streaming ingestion.` : "No streaming ingestion configured (using batching).",
    });

    // EH-012: Continuous Exports
    const unhealthyExports = kqlRows("continuousExports").filter(x => x.IsRunning === false || x.IsDisabled === true);
    rules.push({
      id: "EH-012", rule: `${pre}Continuous Exports Healthy`, category: "Reliability", severity: "MEDIUM",
      status: err("continuousExports") ? "ERROR" : cnt("continuousExports") === 0 ? "N/A" : unhealthyExports.length === 0 ? "PASS" : "WARN",
      details: err("continuousExports") ?? (cnt("continuousExports") === 0 ? "No continuous exports." : unhealthyExports.length === 0 ? `All ${cnt("continuousExports")} export(s) running.` : `${unhealthyExports.length} export(s) not running.`),
      recommendation: "Re-enable or fix stopped continuous exports.",
    });

    // EH-013: Cold Data (from tableDetails)
    const coldTables = kqlRows("tableDetails").filter(x => {
      const total = x.TotalRowCount as number ?? 0;
      const hot = x.HotRowCount as number ?? 0;
      return total > 0 && hot < total * 0.5;
    });
    rules.push({
      id: "EH-013", rule: `${pre}Hot Cache Coverage`, category: "Performance", severity: "MEDIUM",
      status: err("tableDetails") ? "ERROR" : coldTables.length === 0 ? "PASS" : "WARN",
      details: err("tableDetails") ?? (coldTables.length === 0 ? "All data within hot cache." : `${coldTables.length} table(s) with >50% cold data.`),
      recommendation: "Extend caching policy for frequently queried tables.",
    });

    // EH-014: Ingestion Batching Policy
    const batchingRows = kqlRows("ingestionBatching");
    const noBatching = batchingRows.filter(x => {
      const ps = policyString(x, "Policy", "IngestionBatchingPolicy");
      return !ps || ps === "null" || ps === "{}";
    });
    rules.push({
      id: "EH-014", rule: `${pre}Ingestion Batching Configured`, category: "Performance", severity: "LOW",
      status: err("ingestionBatching") ? "ERROR" : batchingRows.length === 0 ? "N/A" : noBatching.length === 0 ? "PASS" : "WARN",
      details: err("ingestionBatching") ?? (batchingRows.length === 0 ? "No tables found." : noBatching.length === 0 ? "All tables have explicit batching policies." : `${noBatching.length} table(s) using default batching policy.`),
      recommendation: "Configure ingestion batching for high-throughput tables to balance latency vs. efficiency.",
    });

    // EH-015: Update Policies
    const updatePolicyRows = kqlRows("updatePolicies");
    const tablesWithUpdate = updatePolicyRows.filter(x => {
      const ps = policyString(x, "Policy", "UpdatePolicy");
      return ps && ps !== "null" && ps !== "[]" && ps !== "";
    });
    rules.push({
      id: "EH-015", rule: `${pre}Update Policies Configured`, category: "Data Management", severity: "INFO",
      status: err("updatePolicies") ? "ERROR" : "PASS",
      details: err("updatePolicies") ?? (tablesWithUpdate.length > 0 ? `${tablesWithUpdate.length} table(s) with update policies for event-driven transformations.` : "No update policies configured — consider using them for ETL within the database."),
    });

    // EH-016: Partitioning Policies
    const partitionRows = kqlRows("partitioningPolicies");
    const tablesWithPartition = partitionRows.filter(x => {
      const ps = policyString(x, "Policy", "PartitioningPolicy");
      return ps && ps !== "null" && ps !== "{}" && ps !== "";
    });
    const largeTables = kqlRows("extentStats").filter(x => (x.TotalOriginalSizeMB as number) > 1024);
    const largeWithoutPartition = largeTables.filter(lt => {
      const tbl = lt.TableName as string;
      return !tablesWithPartition.some(p => (p.EntityName ?? p.TableName) === tbl);
    });
    rules.push({
      id: "EH-016", rule: `${pre}Partitioning on Large Tables`, category: "Performance", severity: "MEDIUM",
      status: err("partitioningPolicies") ? "ERROR" : largeWithoutPartition.length === 0 ? "PASS" : "WARN",
      details: err("partitioningPolicies") ?? (largeWithoutPartition.length === 0 ? "All large tables (>1 GB) have partitioning policies." : `${largeWithoutPartition.length} large table(s) without partitioning: ${largeWithoutPartition.slice(0, 3).map(x => x.TableName).join(", ")}`),
      recommendation: "Add partitioning policy on large tables to improve query performance on filtered columns.",
    });

    // EH-017: Merge Policy
    const mergeRows = kqlRows("mergePolicy");
    const customMerge = mergeRows.filter(x => {
      const ps = policyString(x, "Policy", "MergePolicy");
      return ps && ps !== "null" && ps !== "{}" && ps !== "";
    });
    rules.push({
      id: "EH-017", rule: `${pre}Merge Policy Configured`, category: "Performance", severity: "LOW",
      status: err("mergePolicy") ? "ERROR" : "PASS",
      details: err("mergePolicy") ?? (customMerge.length > 0 ? `${customMerge.length} table(s) with custom merge policies.` : "All tables using default merge policy."),
      recommendation: "Custom merge policies can optimize compaction for tables with specific ingestion patterns.",
    });

    // EH-018: Encoding Policy
    const encodingRows = kqlRows("encodingPolicy");
    const customEncoding = encodingRows.filter(x => {
      const ps = policyString(x, "Policy", "EncodingPolicy");
      return ps && ps !== "null" && ps !== "{}" && ps !== "";
    });
    const poorCompTables = kqlRows("columnStats").filter(x =>
      typeof x.CompressionRatio === "number" && x.CompressionRatio < 40 && (x.TotalOriginalMB as number) > 100
    );
    const poorCompNoEncoding = poorCompTables.filter(pt => {
      const tbl = pt.TableName as string;
      return !customEncoding.some(e => (e.EntityName ?? e.TableName) === tbl);
    });
    rules.push({
      id: "EH-018", rule: `${pre}Encoding Policy for Poorly Compressed Tables`, category: "Performance", severity: "MEDIUM",
      status: err("encodingPolicy") ? "ERROR" : poorCompNoEncoding.length === 0 ? "PASS" : "WARN",
      details: err("encodingPolicy") ?? (poorCompNoEncoding.length === 0 ? "All poorly compressed tables have encoding policies or compression is adequate." : `${poorCompNoEncoding.length} poorly compressed table(s) without encoding policy: ${poorCompNoEncoding.slice(0, 3).map(x => x.TableName).join(", ")}`),
      recommendation: "Set encoding policy to optimize compression for string-heavy or high-cardinality columns.",
    });

    // EH-019: Row Order Policy
    const rowOrderRows = kqlRows("rowOrderPolicy");
    const tablesWithRowOrder = rowOrderRows.filter(x => {
      const ps = policyString(x, "Policy", "RowOrderPolicy");
      return ps && ps !== "null" && ps !== "[]" && ps !== "";
    });
    rules.push({
      id: "EH-019", rule: `${pre}Row Order Policy`, category: "Performance", severity: "LOW",
      status: err("rowOrderPolicy") ? "ERROR" : "PASS",
      details: err("rowOrderPolicy") ?? (tablesWithRowOrder.length > 0 ? `${tablesWithRowOrder.length} table(s) with row order policies for optimized queries.` : "No row order policies configured — consider for time-series or frequently sorted tables."),
      recommendation: "Set row order policy on tables frequently queried with ORDER BY or time-range filters.",
    });

    // EH-020: Stored Functions Audit
    const functions = kqlRows("functions");
    rules.push({
      id: "EH-020", rule: `${pre}Stored Functions Inventory`, category: "Data Management", severity: "INFO",
      status: err("functions") ? "ERROR" : "PASS",
      details: err("functions") ?? (functions.length > 0 ? `${functions.length} stored function(s) registered.` : "No stored functions."),
      recommendation: "Review stored functions periodically and remove unused ones.",
    });
  }

  return renderRuleReport(
    `Eventhouse Analysis: ${eventhouse.displayName}`,
    new Date().toISOString(),
    header,
    rules
  );
}

// ──────────────────────────────────────────────
// Structured Fix Definitions — like WAREHOUSE_FIXES
// ──────────────────────────────────────────────

interface EventhouseFixDef {
  description: string;
  getCommands: (
    args: { dbName: string; tableName?: string; cachingDays?: number; retentionDays?: number },
    diagnostics: Record<string, { rows?: KqlRow[]; error?: string }>
  ) => string[];
}

const EVENTHOUSE_FIXES: Record<string, EventhouseFixDef> = {
  "EH-002": {
    description: "Merge fragmented tables (>100 extents, <100K rows/extent)",
    getCommands: (args, diag) => {
      if (args.tableName) {
        return [`.merge table ['${args.tableName}']`];
      }
      const fragmented = (diag.extentStats?.rows ?? []).filter(x => {
        const extents = x.ExtentCount as number;
        const totalRows = x.TotalRows as number;
        return extents > 100 && totalRows > 0 && (totalRows / extents) < 100000;
      });
      return fragmented.map(r => `.merge table ['${r.TableName}']`);
    },
  },
  "EH-004": {
    description: "Set hot cache policy",
    getCommands: (args) => {
      const days = args.cachingDays ?? 30;
      if (args.tableName) {
        return [`.alter table ['${args.tableName}'] policy caching hot = ${days}d`];
      }
      return [`.alter database ['${args.dbName}'] policy caching hot = ${days}d`];
    },
  },
  "EH-005": {
    description: "Set retention policy",
    getCommands: (args) => {
      const days = args.retentionDays ?? 365;
      if (args.tableName) {
        return [`.alter table ['${args.tableName}'] policy retention softdelete = ${days}d`];
      }
      return [`.alter database ['${args.dbName}'] policy retention softdelete = ${days}d`];
    },
  },
  "EH-006": {
    description: "Re-enable unhealthy materialized views",
    getCommands: (_args, diag) => {
      const views = (diag.materializedViews?.rows ?? []).filter(v => isFalsy(v.IsHealthy));
      return views.map(v => `.enable materialized-view ['${v.Name ?? v.MaterializedViewName}']`);
    },
  },
  "EH-014": {
    description: "Set ingestion batching policy (MaxItems=500, MaxDelay=00:05:00)",
    getCommands: (args) => {
      if (args.tableName) {
        return [`.alter table ['${args.tableName}'] policy ingestionbatching @'{"MaximumBatchingTimeSpan":"00:05:00","MaximumNumberOfItems":500,"MaximumRawDataSizeMB":1024}'`];
      }
      return [`.alter database ['${args.dbName}'] policy ingestionbatching @'{"MaximumBatchingTimeSpan":"00:05:00","MaximumNumberOfItems":500,"MaximumRawDataSizeMB":1024}'`];
    },
  },
  "EH-016": {
    description: "Set hash partitioning policy on large tables (>1 GB) without one",
    getCommands: (_args, diag) => {
      const largeTables = (diag.extentStats?.rows ?? []).filter(x => (x.TotalOriginalSizeMB as number) > 1024);
      const partitioned = new Set(
        (diag.partitioningPolicies?.rows ?? [])
          .filter(x => {
            const ps = policyString(x, "Policy", "PartitioningPolicy");
            return ps && ps !== "null" && ps !== "{}" && ps !== "";
          })
          .map(x => x.EntityName ?? x.TableName)
      );
      return largeTables
        .filter(t => !partitioned.has(t.TableName as string))
        .map(t => `.alter table ['${t.TableName}'] policy partitioning '{"PartitionKeys": [{"ColumnName": "ingestion_time()", "Kind": "UniformRange", "Properties": {"Reference": "2024-01-01T00:00:00", "RangeSize": "1.00:00:00", "OverrideCreationTime": false}}]}'`);
    },
  },
  "EH-017": {
    description: "Set optimized merge policy on fragmented tables",
    getCommands: (args, diag) => {
      if (args.tableName) {
        return [`.alter table ['${args.tableName}'] policy merge @'{"MaxRangeInHours":24,"RowCountUpperBoundForMerge":16000000}'`];
      }
      const fragmented = (diag.extentStats?.rows ?? []).filter(x => {
        const extents = x.ExtentCount as number;
        const totalRows = x.TotalRows as number;
        return extents > 100 && totalRows > 0 && (totalRows / extents) < 100000;
      });
      return fragmented.map(r => `.alter table ['${r.TableName}'] policy merge @'{"MaxRangeInHours":24,"RowCountUpperBoundForMerge":16000000}'`);
    },
  },
};

const FIXABLE_RULE_IDS = Object.keys(EVENTHOUSE_FIXES);

// ──────────────────────────────────────────────
// Tool: eventhouse_fix — Auto-fix detected issues
// ──────────────────────────────────────────────

export async function eventhouseFix(args: {
  workspaceId: string;
  eventhouseId: string;
  ruleIds?: string[];
  kqlDatabaseName?: string;
  tableName?: string;
  cachingDays?: number;
  retentionDays?: number;
  dryRun?: boolean;
}): Promise<string> {
  // Input validation
  if (args.tableName) validateKqlName(args.tableName, "tableName");
  if (args.kqlDatabaseName) validateKqlName(args.kqlDatabaseName, "kqlDatabaseName");
  if (args.cachingDays !== undefined && (args.cachingDays < 1 || args.cachingDays > 36500)) {
    throw new Error("cachingDays must be between 1 and 36500.");
  }
  if (args.retentionDays !== undefined && (args.retentionDays < 1 || args.retentionDays > 36500)) {
    throw new Error("retentionDays must be between 1 and 36500.");
  }

  const [eventhouse, kqlDatabases] = await Promise.all([
    getEventhouse(args.workspaceId, args.eventhouseId),
    listKqlDatabases(args.workspaceId),
  ]);

  const queryUri = eventhouse.properties?.queryServiceUri;
  if (!queryUri) return "❌ No query URI available. Cannot apply fixes.";

  const ehDbIds = new Set(eventhouse.properties?.databasesItemIds ?? []);
  const ehDatabases = kqlDatabases.filter(db => ehDbIds.has(db.id));
  const databasesToFix = args.kqlDatabaseName
    ? ehDatabases.filter(db => db.displayName === args.kqlDatabaseName)
    : (ehDatabases.length > 0 ? ehDatabases : kqlDatabases);

  const isDryRun = args.dryRun ?? false;
  const results: string[] = [];
  let totalFixed = 0;
  let totalFailed = 0;
  let totalSkipped = 0;

  for (const db of databasesToFix) {
    // Validate db name
    validateKqlName(db.displayName, "database name");

    // Run needed diagnostics for fix generation
    const diagKeys: Record<string, { query: string; isMgmt: boolean }> = {
      extentStats: KQL_DIAGNOSTICS.extentStats,
      materializedViews: KQL_DIAGNOSTICS.materializedViews,
      partitioningPolicies: KQL_DIAGNOSTICS.partitioningPolicies,
    };
    const diagnostics = await runKqlDiagnostics(queryUri, db.displayName, diagKeys);

    const ruleIds = args.ruleIds && args.ruleIds.length > 0
      ? args.ruleIds.filter(id => FIXABLE_RULE_IDS.includes(id))
      : FIXABLE_RULE_IDS;

    for (const ruleId of ruleIds) {
      const fix = EVENTHOUSE_FIXES[ruleId];
      if (!fix) continue;

      const commands = fix.getCommands(
        { dbName: db.displayName, tableName: args.tableName, cachingDays: args.cachingDays, retentionDays: args.retentionDays },
        diagnostics
      );

      if (commands.length === 0) {
        results.push(`| ${ruleId} | ⚪ | ${db.displayName} | No action needed |`);
        totalSkipped++;
        continue;
      }

      for (const cmd of commands) {
        if (isDryRun) {
          results.push(`| ${ruleId} | 🔍 | ${db.displayName} | \`${cmd.substring(0, 80)}\` |`);
          totalSkipped++;
        } else {
          try {
            await executeKqlMgmt(queryUri, db.displayName, cmd);
            results.push(`| ${ruleId} | ✅ | ${db.displayName} | \`${cmd.substring(0, 80)}\` |`);
            totalFixed++;
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            results.push(`| ${ruleId} | ❌ | ${db.displayName}: ${msg.substring(0, 60)} | \`${cmd.substring(0, 60)}\` |`);
            totalFailed++;
          }
        }
      }
    }
  }

  const mode = isDryRun ? "DRY RUN (preview only)" : "Applying fixes";
  return [
    `# 🔧 Eventhouse Fix: ${eventhouse.displayName}`,
    "",
    `_${mode} at ${new Date().toISOString()}_`,
    "",
    isDryRun
      ? `**${totalSkipped} command(s) previewed** — re-run without dryRun to apply.`
      : `**${totalFixed} fixed, ${totalFailed} failed${totalSkipped > 0 ? `, ${totalSkipped} skipped` : ""}**`,
    "",
    "| Rule | Status | Database | Command |",
    "|------|--------|----------|---------|",
    ...results,
    "",
    isDryRun ? "> 💡 Set `dryRun: false` to execute these commands." : "",
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool: eventhouse_auto_optimize — Scan + fix all issues
// ──────────────────────────────────────────────

export async function eventhouseAutoOptimize(args: {
  workspaceId: string;
  eventhouseId: string;
  cachingDays?: number;
  retentionDays?: number;
  dryRun?: boolean;
}): Promise<string> {
  return eventhouseFix({
    workspaceId: args.workspaceId,
    eventhouseId: args.eventhouseId,
    ruleIds: undefined, // all rules
    cachingDays: args.cachingDays,
    retentionDays: args.retentionDays,
    dryRun: args.dryRun,
  });
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const eventhouseTools = [
  {
    name: "eventhouse_list",
    description:
      "List all eventhouses in a Fabric workspace with their query/ingestion URIs and KQL database counts.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
      },
      required: ["workspaceId"],
    },
    handler: eventhouseList,
  },
  {
    name: "eventhouse_list_kql_databases",
    description: "List all KQL databases in a Fabric workspace.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
      },
      required: ["workspaceId"],
    },
    handler: eventhouseListKqlDatabases,
  },
  {
    name: "eventhouse_optimization_recommendations",
    description:
      "LIVE SCAN: Connects to a Fabric Eventhouse KQL endpoint and runs real diagnostic commands. " +
      "Analyzes 20 rules: table storage/fragmentation (extent stats), caching policies, retention policies, " +
      "materialized views health, ingestion batching, streaming ingestion, partitioning, merge/encoding/row_order policies, " +
      "stored functions, and query performance. Returns findings with prioritized action items.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        eventhouseId: {
          type: "string",
          description: "The ID of the eventhouse to analyze",
        },
      },
      required: ["workspaceId", "eventhouseId"],
    },
    handler: eventhouseOptimizationRecommendations,
  },
  {
    name: "eventhouse_fix",
    description:
      "AUTO-FIX: Applies fixes to a Fabric Eventhouse. " +
      "Fixable rules: EH-002 (merge fragmentation), EH-004 (caching), EH-005 (retention), " +
      "EH-006 (re-enable materialized views), EH-014 (ingestion batching), EH-016 (partitioning), EH-017 (merge policy). " +
      "Use dryRun=true to preview commands without executing them.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: { type: "string", description: "The ID of the Fabric workspace" },
        eventhouseId: { type: "string", description: "The ID of the eventhouse to fix" },
        ruleIds: { type: "array", items: { type: "string" }, description: "Rule IDs to fix: EH-002, EH-004, EH-005, EH-006, EH-014, EH-016, EH-017" },
        kqlDatabaseName: { type: "string", description: "Optional: specific KQL database name" },
        tableName: { type: "string", description: "Optional: specific table name" },
        cachingDays: { type: "number", description: "Hot cache days (default: 30)" },
        retentionDays: { type: "number", description: "Retention days (default: 365)" },
        dryRun: { type: "boolean", description: "If true, preview commands without executing them (default: false)" },
      },
      required: ["workspaceId", "eventhouseId"],
    },
    handler: eventhouseFix,
  },
  {
    name: "eventhouse_auto_optimize",
    description:
      "AUTO-OPTIMIZE: Scans a Fabric Eventhouse for all fixable issues across all KQL databases and applies fixes. " +
      "Covers: merge fragmentation, caching policies, retention policies, materialized views, " +
      "ingestion batching, partitioning, merge policy. Use dryRun=true to preview.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: { type: "string", description: "The ID of the Fabric workspace" },
        eventhouseId: { type: "string", description: "The ID of the eventhouse to optimize" },
        cachingDays: { type: "number", description: "Hot cache days (default: 30)" },
        retentionDays: { type: "number", description: "Retention days (default: 365)" },
        dryRun: {
          type: "boolean",
          description: "If true, preview KQL commands without executing (default: false)",
        },
      },
      required: ["workspaceId", "eventhouseId"],
    },
    handler: eventhouseAutoOptimize,
  },
];
