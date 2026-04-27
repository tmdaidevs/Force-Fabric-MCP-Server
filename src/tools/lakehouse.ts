import {
  listLakehouses,
  getLakehouse,
  listLakehouseTables,
  runLakehouseTableMaintenance,
  getLakehouseJobStatus,
  getWorkspace,
  runTemporaryNotebook,
} from "../clients/fabricClient.js";
import { runSparkFixesViaLivy } from "../clients/livyClient.js";
import { runDiagnosticQueries } from "../clients/sqlClient.js";
import type { FabricLakehouse, LakehouseTable } from "../clients/fabricClient.js";
import type { SqlRow } from "../clients/sqlClient.js";
import { renderRuleReport } from "./ruleEngine.js";
import type { RuleResult } from "./ruleEngine.js";
import {
  readDeltaLog,
  getPartitionColumns,
  getTableConfig,
  getLastOperation,
  countOperations,
  getFileSizeStats,
  daysSinceTimestamp,
} from "../clients/onelakeClient.js";
import type { DeltaLogAnalysis } from "../clients/onelakeClient.js";

// ──────────────────────────────────────────────
// Input validation — prevent Spark SQL injection
// ──────────────────────────────────────────────

const SAFE_SPARK_NAME = /^[a-zA-Z0-9_\- .]+$/;

function validateSparkName(value: string, label: string): void {
  if (!SAFE_SPARK_NAME.test(value)) {
    throw new Error(`Invalid ${label}: must be alphanumeric/underscore/dash/dot only.`);
  }
}

// ──────────────────────────────────────────────
// Tool: lakehouse_list
// ──────────────────────────────────────────────

export async function lakehouseList(args: { workspaceId: string }): Promise<string> {
  const lakehouses = await listLakehouses(args.workspaceId);

  if (lakehouses.length === 0) {
    return "No lakehouses found in this workspace.";
  }

  const lines = lakehouses.map((lh: FabricLakehouse) => {
    const sqlStatus = lh.properties?.sqlEndpointProperties?.provisioningStatus ?? "unknown";
    return [
      `- **${lh.displayName}** (ID: ${lh.id})`,
      `  SQL Endpoint: ${sqlStatus}`,
      lh.properties?.oneLakeTablesPath ? `  Tables Path: ${lh.properties.oneLakeTablesPath}` : null,
      lh.properties?.oneLakeFilesPath ? `  Files Path: ${lh.properties.oneLakeFilesPath}` : null,
    ].filter(Boolean).join("\n");
  });

  return `## Lakehouses in workspace ${args.workspaceId}\n\n${lines.join("\n\n")}`;
}

// ──────────────────────────────────────────────
// Tool: lakehouse_list_tables
// ──────────────────────────────────────────────

export async function lakehouseListTables(args: {
  workspaceId: string;
  lakehouseId: string;
}): Promise<string> {
  const [lakehouse, tables] = await Promise.all([
    getLakehouse(args.workspaceId, args.lakehouseId),
    listLakehouseTables(args.workspaceId, args.lakehouseId),
  ]);

  if (tables.length === 0) {
    return `Lakehouse "${lakehouse.displayName}" has no tables.`;
  }

  const lines = tables.map((t: LakehouseTable) =>
    `| ${t.name} | ${t.type} | ${t.format} | ${t.location} |`
  );

  return [
    `## Tables in Lakehouse "${lakehouse.displayName}"`,
    "",
    `Total: ${tables.length} table(s)`,
    "",
    "| Name | Type | Format | Location |",
    "|------|------|--------|----------|",
    ...lines,
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool: lakehouse_run_table_maintenance
// ──────────────────────────────────────────────

export async function lakehouseRunTableMaintenance(args: {
  workspaceId: string;
  lakehouseId: string;
  tableName?: string;
  optimizeSettings?: {
    vOrder?: boolean;
    zOrderColumns?: string[];
  };
  vacuumSettings?: {
    retentionPeriod?: string;
  };
}): Promise<string> {
  const executionData: Record<string, unknown> = {};

  if (args.tableName) {
    // Single table maintenance
    const tableConfig: Record<string, unknown> = {};

    if (args.optimizeSettings) {
      tableConfig.optimizeSettings = {
        vOrder: args.optimizeSettings.vOrder ?? true,
        ...(args.optimizeSettings.zOrderColumns?.length
          ? { zOrderBy: args.optimizeSettings.zOrderColumns }
          : {}),
      };
    }

    if (args.vacuumSettings) {
      tableConfig.vacuumSettings = {
        retentionPeriod: args.vacuumSettings.retentionPeriod ?? "7.00:00:00",
      };
    }

    // Default: both optimize with vOrder and vacuum
    if (!args.optimizeSettings && !args.vacuumSettings) {
      tableConfig.optimizeSettings = { vOrder: true };
      tableConfig.vacuumSettings = { retentionPeriod: "7.00:00:00" };
    }

    executionData.tablesToProcess = [{ tableName: args.tableName, ...tableConfig }];
  }

  const result = await runLakehouseTableMaintenance(
    args.workspaceId,
    args.lakehouseId,
    "TableMaintenance",
    Object.keys(executionData).length > 0 ? executionData : undefined
  );

  return [
    "## Table Maintenance Job Started",
    "",
    `- **Job ID**: ${result.id ?? "N/A"}`,
    `- **Status**: ${result.status ?? "Accepted"}`,
    args.tableName ? `- **Table**: ${args.tableName}` : "- **Scope**: All tables",
    args.optimizeSettings?.vOrder !== false ? "- **V-Order**: Enabled" : "",
    args.optimizeSettings?.zOrderColumns?.length
      ? `- **Z-Order Columns**: ${args.optimizeSettings.zOrderColumns.join(", ")}`
      : "",
    args.vacuumSettings
      ? `- **Vacuum Retention**: ${args.vacuumSettings.retentionPeriod ?? "7 days"}`
      : "",
    "",
    "Use `lakehouse_get_job_status` to check progress.",
  ].filter(Boolean).join("\n");
}

// ──────────────────────────────────────────────
// Tool: lakehouse_get_job_status
// ──────────────────────────────────────────────

export async function lakehouseGetJobStatus(args: {
  workspaceId: string;
  lakehouseId: string;
  jobInstanceId: string;
}): Promise<string> {
  const job = await getLakehouseJobStatus(
    args.workspaceId,
    args.lakehouseId,
    args.jobInstanceId
  );

  const lines = [
    `## Job Status`,
    "",
    `- **Job ID**: ${job.id}`,
    `- **Type**: ${job.jobType}`,
    `- **Status**: ${job.status}`,
    job.startTimeUtc ? `- **Started**: ${job.startTimeUtc}` : null,
    job.endTimeUtc ? `- **Completed**: ${job.endTimeUtc}` : null,
  ];

  if (job.failureReason) {
    lines.push(
      "",
      "### Failure Details",
      `- **Error**: ${job.failureReason.message}`,
      `- **Code**: ${job.failureReason.errorCode}`
    );
  }

  return lines.filter(Boolean).join("\n");
}

// ──────────────────────────────────────────────
// SQL Diagnostics for Lakehouse SQL Endpoint
// ──────────────────────────────────────────────

const LAKEHOUSE_SQL_DIAGNOSTICS = {
  tableInfo: `
    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES
    ORDER BY TABLE_SCHEMA, TABLE_NAME`,

  columnInfo: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
           CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`,

  tableRowCounts: `
    SELECT s.name AS schema_name, t.name AS table_name,
           SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1)
    GROUP BY s.name, t.name
    ORDER BY row_count DESC`,

  // Nullable columns ratio per table
  nullableColumnsRatio: `
    SELECT TABLE_SCHEMA, TABLE_NAME,
           COUNT(*) AS total_columns,
           SUM(CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END) AS nullable_count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
    GROUP BY TABLE_SCHEMA, TABLE_NAME
    ORDER BY total_columns DESC`,

  // Data type distribution across all tables
  dataTypeDistribution: `
    SELECT DATA_TYPE, COUNT(*) AS column_count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
    GROUP BY DATA_TYPE
    ORDER BY column_count DESC`,

  // Wide varchar columns (potential optimization targets)
  wideStringColumns: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
           CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar')
      AND CHARACTER_MAXIMUM_LENGTH > 500
    ORDER BY CHARACTER_MAXIMUM_LENGTH DESC`,

  // Key columns (ID/Key) that are nullable (data integrity risk)
  nullableKeyColumns: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND IS_NULLABLE = 'YES'
      AND (COLUMN_NAME LIKE '%Id' OR COLUMN_NAME LIKE '%_id'
           OR COLUMN_NAME LIKE '%Key' OR COLUMN_NAME LIKE '%_key'
           OR COLUMN_NAME = 'id' OR COLUMN_NAME = 'pk')
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  // Float/real columns (precision risk)
  floatingPointColumns: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND DATA_TYPE IN ('float', 'real')
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  // Column naming issues (spaces/special chars)
  columnNamingIssues: `
    SELECT t.name AS table_name, c.name AS column_name
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE c.name COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9_]%'
      AND t.is_ms_shipped = 0
    ORDER BY t.name, c.name`,

  // Audit columns check
  missingAuditColumns: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           SUM(CASE WHEN c.name IN ('created_at','created_date','CreatedAt','CreatedDate','__created_at') THEN 1 ELSE 0 END) AS has_created,
           SUM(CASE WHEN c.name IN ('updated_at','updated_date','modified_at','ModifiedAt','__updated_at') THEN 1 ELSE 0 END) AS has_updated
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING SUM(CASE WHEN c.name IN ('created_at','created_date','CreatedAt','CreatedDate','updated_at','updated_date','modified_at','ModifiedAt','__created_at','__updated_at') THEN 1 ELSE 0 END) = 0`,

  // Mixed date types per table
  mixedDateTypes: `
    SELECT table_name, date_type_count, date_types_used FROM (
      SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
             COUNT(DISTINCT ty.name) AS date_type_count,
             STRING_AGG(ty.name, ', ') AS date_types_used
      FROM sys.tables t
      JOIN sys.columns c ON t.object_id = c.object_id
      JOIN sys.types ty ON c.user_type_id = ty.user_type_id
      WHERE t.is_ms_shipped = 0
        AND ty.name IN ('date','datetime','datetime2','smalldatetime','datetimeoffset')
      GROUP BY t.schema_id, t.name
    ) sub WHERE date_type_count > 1`,

  // Empty tables
  emptyTables: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1) AND t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING SUM(p.rows) = 0`,

  // Date-like columns stored as text
  textDateColumns: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND DATA_TYPE IN ('varchar', 'nvarchar')
      AND (COLUMN_NAME LIKE '%date%' OR COLUMN_NAME LIKE '%time%'
           OR COLUMN_NAME LIKE '%created%' OR COLUMN_NAME LIKE '%modified%'
           OR COLUMN_NAME LIKE '%updated%' OR COLUMN_NAME LIKE '%_dt' OR COLUMN_NAME LIKE '%_ts')
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  // Numeric-like columns stored as text
  textNumericColumns: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND DATA_TYPE IN ('varchar', 'nvarchar', 'char')
      AND CHARACTER_MAXIMUM_LENGTH <= 20
      AND (COLUMN_NAME LIKE '%id' OR COLUMN_NAME LIKE '%num%' OR COLUMN_NAME LIKE '%code%'
           OR COLUMN_NAME LIKE '%amount%' OR COLUMN_NAME LIKE '%price%' OR COLUMN_NAME LIKE '%qty%')
      AND COLUMN_NAME NOT LIKE '%guid%' AND COLUMN_NAME NOT LIKE '%uuid%'
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  // Sensitive/PII columns
  sensitiveColumns: `
    SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS table_name, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND (COLUMN_NAME LIKE '%credit%' OR COLUMN_NAME LIKE '%ssn%' OR COLUMN_NAME LIKE '%password%'
           OR COLUMN_NAME LIKE '%secret%' OR COLUMN_NAME LIKE '%phone%' OR COLUMN_NAME LIKE '%email%'
           OR COLUMN_NAME LIKE '%IBAN%' OR COLUMN_NAME LIKE '%SWIFT%' OR COLUMN_NAME LIKE '%BIC%'
           OR COLUMN_NAME LIKE '%license%' OR COLUMN_NAME LIKE '%tax%id%')
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  // Large tables (>1M rows)
  largeTables: `
    SELECT s.name AS schema_name, t.name AS table_name,
           SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1) AND t.is_ms_shipped = 0
    GROUP BY s.name, t.name
    HAVING SUM(p.rows) > 1000000
    ORDER BY SUM(p.rows) DESC`,

  // Deprecated types (TEXT/NTEXT/IMAGE)
  deprecatedTypes: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           c.name AS column_name, TYPE_NAME(c.user_type_id) AS data_type
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
      AND TYPE_NAME(c.user_type_id) IN ('text', 'ntext', 'image')
    ORDER BY t.name, c.name`,

  // Tables without any key column
  tablesWithoutKeys: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name
    FROM sys.tables t
    WHERE t.is_ms_shipped = 0
      AND NOT EXISTS (
        SELECT 1 FROM sys.columns c
        WHERE c.object_id = t.object_id
          AND (c.name LIKE '%Id' OR c.name LIKE '%_id' OR c.name LIKE '%Key'
               OR c.name LIKE '%_key' OR c.name = 'id' OR c.name = 'pk')
      )
    ORDER BY t.name`,
};

// ──────────────────────────────────────────────
// Tool: lakehouse_optimization_recommendations
// ──────────────────────────────────────────────

export async function lakehouseOptimizationRecommendations(args: {
  workspaceId: string;
  lakehouseId: string;
}): Promise<string> {
  const [lakehouse, tables] = await Promise.all([
    getLakehouse(args.workspaceId, args.lakehouseId),
    listLakehouseTables(args.workspaceId, args.lakehouseId),
  ]);

  const rules: RuleResult[] = [];
  const header: string[] = [];

  const sqlStatus = lakehouse.properties?.sqlEndpointProperties?.provisioningStatus;
  const sqlConnection = lakehouse.properties?.sqlEndpointProperties?.connectionString;

  // ── Header: Endpoints ──
  header.push(
    "## 🔌 Connection Info",
    "",
    `- **SQL Endpoint**: ${sqlStatus === "Success" ? `✅ Active (\`${sqlConnection}\`)` : `⚠️ ${sqlStatus ?? "unknown"}`}`,
    `- **Tables**: ${tables.length} (${tables.filter(t => t.format?.toLowerCase() === "delta").length} Delta)`,
    ""
  );

  // ══════════════════════════════════════════════
  // RULE LH-001: SQL Endpoint Status
  // ══════════════════════════════════════════════
  rules.push({
    id: "LH-001",
    rule: "SQL Endpoint Active",
    category: "Availability",
    severity: "HIGH",
    status: sqlStatus === "Success" ? "PASS" : "FAIL",
    details: sqlStatus === "Success"
      ? "SQL Analytics Endpoint is provisioned and active."
      : `SQL Endpoint status: ${sqlStatus ?? "unknown"}. Deep analysis not possible.`,
    recommendation: "Provision the SQL Analytics Endpoint to enable cross-query analytics and deep analysis.",
  });

  // ══════════════════════════════════════════════
  // RULE LH-002: Medallion Architecture Naming
  // ══════════════════════════════════════════════
  const lhName = lakehouse.displayName.toLowerCase();
  const bronzeKw = ["bronze", "raw", "landing", "ingest", "stage"];
  const silverKw = ["silver", "refined", "intermediate", "cleansed", "enriched"];
  const goldKw = ["gold", "curated", "consumption", "serving", "analytics"];
  const hasLayer = bronzeKw.some(k => lhName.includes(k)) || silverKw.some(k => lhName.includes(k)) || goldKw.some(k => lhName.includes(k));
  const detectedLayer = bronzeKw.some(k => lhName.includes(k)) ? "Bronze" : silverKw.some(k => lhName.includes(k)) ? "Silver" : goldKw.some(k => lhName.includes(k)) ? "Gold" : null;

  rules.push({
    id: "LH-002",
    rule: "Medallion Architecture Naming",
    category: "Maintainability",
    severity: "LOW",
    status: hasLayer ? "PASS" : "WARN",
    details: hasLayer
      ? `Lakehouse follows ${detectedLayer} layer naming convention.`
      : `Name "${lakehouse.displayName}" doesn't follow bronze/silver/gold pattern.`,
    recommendation: "Prefix lakehouse names with bronze_/silver_/gold_ for clear data architecture.",
  });

  // ══════════════════════════════════════════════
  // RULE LH-003: Non-Delta Tables
  // ══════════════════════════════════════════════
  const nonDelta = tables.filter(t => t.format?.toLowerCase() !== "delta");
  rules.push({
    id: "LH-003",
    rule: "All Tables Use Delta Format",
    category: "Performance",
    severity: "HIGH",
    status: tables.length === 0 ? "N/A" : nonDelta.length === 0 ? "PASS" : "FAIL",
    details: tables.length === 0
      ? "No tables in lakehouse."
      : nonDelta.length === 0
        ? `All ${tables.length} tables use Delta format.`
        : `${nonDelta.length} table(s) not using Delta: ${nonDelta.map(t => t.name).join(", ")}`,
    recommendation: "Convert non-Delta tables to Delta format for OPTIMIZE/VACUUM/V-Order support.",
  });

  // ══════════════════════════════════════════════
  // RULE LH-004: Table Maintenance (Delta tables exist)
  // ══════════════════════════════════════════════
  const deltaTables = tables.filter(t => t.format?.toLowerCase() === "delta");
  rules.push({
    id: "LH-004",
    rule: "Table Maintenance Recommended",
    category: "Performance",
    severity: "MEDIUM",
    status: deltaTables.length === 0 ? "N/A" : "WARN",
    details: deltaTables.length === 0
      ? "No Delta tables to maintain."
      : `${deltaTables.length} Delta table(s) should have regular OPTIMIZE + VACUUM: ${deltaTables.map(t => t.name).join(", ")}`,
    recommendation: "Run lakehouse_run_table_maintenance regularly (OPTIMIZE with V-Order + VACUUM).",
  });

  // ── SQL Endpoint Analysis ──
  if (sqlConnection && sqlStatus === "Success") {
    const sql = await runDiagnosticQueries(sqlConnection, lakehouse.displayName, LAKEHOUSE_SQL_DIAGNOSTICS);

    // ══════════════════════════════════════════════
    // RULE LH-005: Empty Tables
    // ══════════════════════════════════════════════
    const emptyCount = sql.emptyTables?.rows?.length ?? 0;
    rules.push({
      id: "LH-005",
      rule: "No Empty Tables",
      category: "Data Quality",
      severity: "MEDIUM",
      status: sql.emptyTables?.error ? "ERROR" : emptyCount === 0 ? "PASS" : "WARN",
      details: sql.emptyTables?.error
        ? `Could not check: ${sql.emptyTables.error}`
        : emptyCount === 0
          ? "All tables contain data."
          : `${emptyCount} empty table(s): ${sql.emptyTables!.rows!.slice(0, 5).map(r => r.table_name).join(", ")}`,
      recommendation: "Remove unused tables or verify data pipelines are running.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-006: Wide String Columns (>500)
    // ══════════════════════════════════════════════
    const wideCount = sql.wideStringColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-006",
      rule: "No Over-Provisioned String Columns",
      category: "Performance",
      severity: "MEDIUM",
      status: sql.wideStringColumns?.error ? "ERROR" : wideCount === 0 ? "PASS" : "WARN",
      details: sql.wideStringColumns?.error
        ? `Could not check: ${sql.wideStringColumns.error}`
        : wideCount === 0
          ? "All string columns have reasonable lengths."
          : `${wideCount} column(s) with length >500: ${sql.wideStringColumns!.rows!.slice(0, 3).map(r => `${r.TABLE_NAME}.${r.COLUMN_NAME}(${r.CHARACTER_MAXIMUM_LENGTH})`).join(", ")}`,
      recommendation: "Reduce column lengths in source pipeline for better Delta/V-Order compression.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-007: Nullable Key Columns
    // ══════════════════════════════════════════════
    const nullKeyCount = sql.nullableKeyColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-007",
      rule: "Key Columns Are NOT NULL",
      category: "Data Quality",
      severity: "HIGH",
      status: sql.nullableKeyColumns?.error ? "ERROR" : nullKeyCount === 0 ? "PASS" : "FAIL",
      details: sql.nullableKeyColumns?.error
        ? `Could not check: ${sql.nullableKeyColumns.error}`
        : nullKeyCount === 0
          ? "All key/ID columns are NOT NULL."
          : `${nullKeyCount} key column(s) allow NULL: ${sql.nullableKeyColumns!.rows!.slice(0, 5).map(r => `${r.TABLE_NAME}.${r.COLUMN_NAME}`).join(", ")}`,
      recommendation: "Add NOT NULL constraints to ID/key columns in the source pipeline.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-008: Floating Point Columns
    // ══════════════════════════════════════════════
    const floatCount = sql.floatingPointColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-008",
      rule: "No Float/Real Precision Issues",
      category: "Data Quality",
      severity: "MEDIUM",
      status: sql.floatingPointColumns?.error ? "ERROR" : floatCount === 0 ? "PASS" : "WARN",
      details: sql.floatingPointColumns?.error
        ? `Could not check: ${sql.floatingPointColumns.error}`
        : floatCount === 0
          ? "No float/real columns found. All numeric types use fixed precision."
          : `${floatCount} float/real column(s): ${sql.floatingPointColumns!.rows!.slice(0, 5).map(r => `${r.TABLE_NAME}.${r.COLUMN_NAME}`).join(", ")}`,
      recommendation: "Use DECIMAL/NUMERIC for exact values (monetary, percentages).",
    });

    // ══════════════════════════════════════════════
    // RULE LH-009: Column Naming Issues
    // ══════════════════════════════════════════════
    const namingCount = sql.columnNamingIssues?.rows?.length ?? 0;
    rules.push({
      id: "LH-009",
      rule: "Column Naming Convention",
      category: "Maintainability",
      severity: "LOW",
      status: sql.columnNamingIssues?.error ? "ERROR" : namingCount === 0 ? "PASS" : "WARN",
      details: sql.columnNamingIssues?.error
        ? `Could not check: ${sql.columnNamingIssues.error}`
        : namingCount === 0
          ? "All columns follow alphanumeric + underscore naming."
          : `${namingCount} column(s) with spaces or special characters: ${sql.columnNamingIssues!.rows!.slice(0, 5).map(r => `${r.table_name}.${r.column_name}`).join(", ")}`,
      recommendation: "Use only letters, digits, and underscores (snake_case preferred).",
    });

    // ══════════════════════════════════════════════
    // RULE LH-010: Date Columns Stored as Text
    // ══════════════════════════════════════════════
    const textDateCount = sql.textDateColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-010",
      rule: "Date Columns Use Proper Types",
      category: "Data Quality",
      severity: "MEDIUM",
      status: sql.textDateColumns?.error ? "ERROR" : textDateCount === 0 ? "PASS" : "FAIL",
      details: sql.textDateColumns?.error
        ? `Could not check: ${sql.textDateColumns.error}`
        : textDateCount === 0
          ? "All date-like columns use proper DATE/DATETIME2 types."
          : `${textDateCount} date column(s) stored as text: ${sql.textDateColumns!.rows!.slice(0, 3).map(r => `${r.TABLE_NAME}.${r.COLUMN_NAME}`).join(", ")}`,
      recommendation: "Convert to DATE/DATETIME2 for time intelligence, sorting, filtering.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-011: Numeric Columns Stored as Text
    // ══════════════════════════════════════════════
    const textNumCount = sql.textNumericColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-011",
      rule: "Numeric Columns Use Proper Types",
      category: "Data Quality",
      severity: "MEDIUM",
      status: sql.textNumericColumns?.error ? "ERROR" : textNumCount === 0 ? "PASS" : "FAIL",
      details: sql.textNumericColumns?.error
        ? `Could not check: ${sql.textNumericColumns.error}`
        : textNumCount === 0
          ? "All numeric-like columns use proper numeric types."
          : `${textNumCount} numeric column(s) stored as text: ${sql.textNumericColumns!.rows!.slice(0, 3).map(r => `${r.TABLE_NAME}.${r.COLUMN_NAME}`).join(", ")}`,
      recommendation: "Convert to INT/BIGINT/DECIMAL in source pipeline for proper aggregation.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-012: Wide Tables (>30 columns)
    // ══════════════════════════════════════════════
    const wideTables = (sql.nullableColumnsRatio?.rows ?? []).filter(r => (r.total_columns as number) > 30);
    rules.push({
      id: "LH-012",
      rule: "No Excessively Wide Tables",
      category: "Maintainability",
      severity: "LOW",
      status: sql.nullableColumnsRatio?.error ? "ERROR" : wideTables.length === 0 ? "PASS" : "WARN",
      details: sql.nullableColumnsRatio?.error
        ? `Could not check: ${sql.nullableColumnsRatio.error}`
        : wideTables.length === 0
          ? "All tables have ≤30 columns."
          : `${wideTables.length} table(s) with >30 columns: ${wideTables.slice(0, 3).map(r => `${r.TABLE_NAME}(${r.total_columns})`).join(", ")}`,
      recommendation: "Consider normalizing into fact + dimension tables.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-013: Highly Nullable Tables
    // ══════════════════════════════════════════════
    const highNullable = (sql.nullableColumnsRatio?.rows ?? []).filter(r => {
      const total = r.total_columns as number;
      const nullable = r.nullable_count as number;
      return total > 5 && nullable / total > 0.9;
    });
    rules.push({
      id: "LH-013",
      rule: "Schema Has NOT NULL Constraints",
      category: "Data Quality",
      severity: "MEDIUM",
      status: sql.nullableColumnsRatio?.error ? "ERROR" : highNullable.length === 0 ? "PASS" : "WARN",
      details: sql.nullableColumnsRatio?.error
        ? `Could not check: ${sql.nullableColumnsRatio.error}`
        : highNullable.length === 0
          ? "No tables with >90% nullable columns."
          : `${highNullable.length} table(s) are >90% nullable: ${highNullable.slice(0, 3).map(r => `${r.TABLE_NAME}(${r.nullable_count}/${r.total_columns})`).join(", ")}`,
      recommendation: "Add NOT NULL constraints where data should always be present.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-014: Missing Audit Columns
    // ══════════════════════════════════════════════
    const noAuditCount = sql.missingAuditColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-014",
      rule: "Tables Have Audit Columns",
      category: "Maintainability",
      severity: "LOW",
      status: sql.missingAuditColumns?.error ? "ERROR" : noAuditCount === 0 ? "PASS" : "WARN",
      details: sql.missingAuditColumns?.error
        ? `Could not check: ${sql.missingAuditColumns.error}`
        : noAuditCount === 0
          ? "All tables have created_at/updated_at audit columns."
          : `${noAuditCount} table(s) lack audit columns: ${sql.missingAuditColumns!.rows!.slice(0, 5).map(r => r.table_name).join(", ")}`,
      recommendation: "Add created_at/updated_at columns for data lineage tracking.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-015: Mixed Date Types
    // ══════════════════════════════════════════════
    const mixedDateCount = sql.mixedDateTypes?.rows?.length ?? 0;
    rules.push({
      id: "LH-015",
      rule: "Consistent Date Types Per Table",
      category: "Data Quality",
      severity: "LOW",
      status: sql.mixedDateTypes?.error ? "ERROR" : mixedDateCount === 0 ? "PASS" : "WARN",
      details: sql.mixedDateTypes?.error
        ? `Could not check: ${sql.mixedDateTypes.error}`
        : mixedDateCount === 0
          ? "Each table uses a single consistent date/time type."
          : `${mixedDateCount} table(s) mix date types: ${sql.mixedDateTypes!.rows!.slice(0, 3).map(r => `${r.table_name}(${r.date_types_used})`).join(", ")}`,
      recommendation: "Standardize on datetime2 across all tables.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-S01: Sensitive/PII Columns
    // ══════════════════════════════════════════════
    const sensitiveCount = sql.sensitiveColumns?.rows?.length ?? 0;
    rules.push({
      id: "LH-S01", rule: "No Unprotected Sensitive Data", category: "Security", severity: "HIGH",
      status: sql.sensitiveColumns?.error ? "ERROR" : sensitiveCount === 0 ? "PASS" : "WARN",
      details: sql.sensitiveColumns?.error
        ? `Could not check: ${sql.sensitiveColumns.error}`
        : sensitiveCount === 0
          ? "No sensitive column patterns (PII) detected."
          : `${sensitiveCount} sensitive column(s) found: ${sql.sensitiveColumns!.rows!.slice(0, 5).map(r => `${r.table_name}.${r.COLUMN_NAME}`).join(", ")}`,
      recommendation: "Review PII columns and apply data masking or move to a secure layer.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-S02: Large Tables (>1M rows)
    // ══════════════════════════════════════════════
    const largeCount = sql.largeTables?.rows?.length ?? 0;
    rules.push({
      id: "LH-S02", rule: "Large Tables Identified", category: "Performance", severity: "INFO",
      status: sql.largeTables?.error ? "ERROR" : largeCount === 0 ? "PASS" : "PASS",
      details: sql.largeTables?.error
        ? `Could not check: ${sql.largeTables.error}`
        : largeCount === 0
          ? "No tables exceed 1M rows."
          : `${largeCount} table(s) >1M rows: ${sql.largeTables!.rows!.slice(0, 5).map(r => `${r.table_name}(${((r.row_count as number) ?? 0).toLocaleString()} rows)`).join(", ")}`,
    });

    // ══════════════════════════════════════════════
    // RULE LH-S03: Deprecated Data Types
    // ══════════════════════════════════════════════
    const deprecatedCount = sql.deprecatedTypes?.rows?.length ?? 0;
    rules.push({
      id: "LH-S03", rule: "No Deprecated Data Types", category: "Maintainability", severity: "HIGH",
      status: sql.deprecatedTypes?.error ? "ERROR" : deprecatedCount === 0 ? "PASS" : "FAIL",
      details: sql.deprecatedTypes?.error
        ? `Could not check: ${sql.deprecatedTypes.error}`
        : deprecatedCount === 0
          ? "No TEXT/NTEXT/IMAGE columns found."
          : `${deprecatedCount} column(s) with deprecated types: ${sql.deprecatedTypes!.rows!.slice(0, 5).map(r => `${r.table_name}.${r.column_name}(${r.data_type})`).join(", ")}`,
      recommendation: "Migrate TEXT/NTEXT/IMAGE to VARCHAR(MAX)/NVARCHAR(MAX)/VARBINARY(MAX).",
    });

    // ══════════════════════════════════════════════
    // RULE LH-S04: Tables Without Any Key Column
    // ══════════════════════════════════════════════
    const noKeyCount = sql.tablesWithoutKeys?.rows?.length ?? 0;
    rules.push({
      id: "LH-S04", rule: "All Tables Have Key Columns", category: "Data Quality", severity: "MEDIUM",
      status: sql.tablesWithoutKeys?.error ? "ERROR" : noKeyCount === 0 ? "PASS" : "WARN",
      details: sql.tablesWithoutKeys?.error
        ? `Could not check: ${sql.tablesWithoutKeys.error}`
        : noKeyCount === 0
          ? "All tables have at least one ID/Key column."
          : `${noKeyCount} table(s) without any key column: ${sql.tablesWithoutKeys!.rows!.slice(0, 5).map(r => r.table_name).join(", ")}`,
      recommendation: "Add a unique identifier column (ID/Key) for row identification and joins.",
    });

    // ══════════════════════════════════════════════
    // RULE LH-031: Tables with Nested/Complex Types
    // ══════════════════════════════════════════════
    const nestedCols = (sql.columnInfo?.rows ?? []).filter((c: SqlRow) => {
      const dt = ((c.DATA_TYPE as string) ?? "").toLowerCase();
      return dt === "array" || dt === "struct" || dt === "map" || dt.includes("row(");
    });
    rules.push({
      id: "LH-031", rule: "No Deeply Nested Types", category: "Performance", severity: "LOW",
      status: sql.columnInfo?.error ? "ERROR" : nestedCols.length === 0 ? "PASS" : "WARN",
      details: sql.columnInfo?.error
        ? `Could not check: ${sql.columnInfo.error}`
        : nestedCols.length === 0
          ? "No nested complex type columns found."
          : `${nestedCols.length} column(s) with nested types (STRUCT/ARRAY/MAP).`,
      recommendation: "Consider flattening nested types for better query performance and Direct Lake compatibility.",
    });

  } else {
    // SQL endpoint not available — mark all SQL rules as N/A
    const sqlRuleIds = ["LH-005","LH-006","LH-007","LH-008","LH-009","LH-010","LH-011","LH-012","LH-013","LH-014","LH-015"];
    const sqlRuleNames = ["No Empty Tables","No Over-Provisioned String Columns","Key Columns Are NOT NULL","No Float/Real Precision Issues","Column Naming Convention","Date Columns Use Proper Types","Numeric Columns Use Proper Types","No Excessively Wide Tables","Schema Has NOT NULL Constraints","Tables Have Audit Columns","Consistent Date Types Per Table"];
    for (let i = 0; i < sqlRuleIds.length; i++) {
      rules.push({
        id: sqlRuleIds[i],
        rule: sqlRuleNames[i],
        category: "Data Quality",
        severity: "MEDIUM",
        status: "N/A",
        details: "SQL Endpoint not available — cannot perform deep analysis.",
      });
    }
  }

  // ══════════════════════════════════════════════
  // DELTA LOG ANALYSIS (via OneLake ADLS Gen2)
  // ══════════════════════════════════════════════
  if (deltaTables.length > 0) {
    let workspace: { displayName: string } | null = null;
    try {
      workspace = await getWorkspace(args.workspaceId);
    } catch {
      // Can't get workspace name → skip delta log analysis
    }

    if (workspace) {
      const deltaLogResults: Array<{ table: string; log: DeltaLogAnalysis }> = [];
      const deltaTableLimit = 20;
      const skippedDeltaTables = Math.max(0, deltaTables.length - deltaTableLimit);

      for (const t of deltaTables.slice(0, deltaTableLimit)) {
        try {
          const log = await readDeltaLog(workspace.displayName, lakehouse.displayName, t.name);
          deltaLogResults.push({ table: t.name, log });
        } catch {
          // Skip tables where delta log can't be read
        }
      }

      if (skippedDeltaTables > 0) {
        header.push(`> ⚠️ Delta Log analysis limited to ${deltaTableLimit} tables. ${skippedDeltaTables} table(s) skipped.`, "");
      }

      if (deltaLogResults.length > 0) {
        // ══════════════════════════════════════════════
        // RULE LH-016: Partitioning Check
        // ══════════════════════════════════════════════
        const unpartitioned: string[] = [];
        const partitioned: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const parts = getPartitionColumns(log);
          if (parts.length > 0) {
            partitioned.push(`${table}(${parts.join(",")})`);
          } else {
            const stats = getFileSizeStats(log);
            if (stats.totalSizeBytes > 10 * 1024 * 1024 * 1024) { // >10GB
              unpartitioned.push(table);
            }
          }
        }
        rules.push({
          id: "LH-016", rule: "Large Tables Are Partitioned", category: "Performance", severity: "MEDIUM",
          status: unpartitioned.length === 0 ? "PASS" : "WARN",
          details: unpartitioned.length === 0
            ? `Partitioned tables: ${partitioned.length > 0 ? partitioned.join(", ") : "No tables >10GB need partitioning."}`
            : `${unpartitioned.length} large table(s) >10GB without partitioning: ${unpartitioned.join(", ")}`,
          recommendation: "Partition large tables by frequently filtered columns (date, region).",
        });

        // ══════════════════════════════════════════════
        // RULE LH-017: VACUUM History
        // ══════════════════════════════════════════════
        const noVacuum: string[] = [];
        const staleVacuum: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const lastVac = getLastOperation(log, "VACUUM");
          if (!lastVac) {
            noVacuum.push(table);
          } else if (lastVac.timestamp && daysSinceTimestamp(lastVac.timestamp) > 7) {
            staleVacuum.push(`${table}(${daysSinceTimestamp(lastVac.timestamp)}d ago)`);
          }
        }
        const vacuumIssues = [...noVacuum, ...staleVacuum];
        rules.push({
          id: "LH-017", rule: "Regular VACUUM Executed", category: "Maintenance", severity: "MEDIUM",
          status: vacuumIssues.length === 0 ? "PASS" : "WARN",
          details: vacuumIssues.length === 0
            ? "All tables have recent VACUUM operations."
            : `${vacuumIssues.length} table(s) need VACUUM: ${vacuumIssues.slice(0, 5).join(", ")}`,
          recommendation: "Run VACUUM weekly to remove stale files and reduce storage costs.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-018: OPTIMIZE History
        // ══════════════════════════════════════════════
        const noOptimize: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const lastOpt = getLastOperation(log, "OPTIMIZE");
          if (!lastOpt) noOptimize.push(table);
        }
        rules.push({
          id: "LH-018", rule: "Regular OPTIMIZE Executed", category: "Performance", severity: "MEDIUM",
          status: noOptimize.length === 0 ? "PASS" : "WARN",
          details: noOptimize.length === 0
            ? "All tables have OPTIMIZE operations in history."
            : `${noOptimize.length} table(s) never optimized: ${noOptimize.slice(0, 5).join(", ")}`,
          recommendation: "Run OPTIMIZE regularly for file compaction and V-Order.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-019: Small File Problem
        // ══════════════════════════════════════════════
        const smallFileIssues: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const stats = getFileSizeStats(log);
          if (stats.totalFiles > 10 && stats.smallFileCount > stats.totalFiles * 0.5) {
            smallFileIssues.push(`${table}(${stats.smallFileCount}/${stats.totalFiles} files <25MB, avg ${stats.avgFileSizeMB.toFixed(1)}MB)`);
          }
        }
        rules.push({
          id: "LH-019", rule: "No Small File Problem", category: "Performance", severity: "HIGH",
          status: smallFileIssues.length === 0 ? "PASS" : "FAIL",
          details: smallFileIssues.length === 0
            ? "File sizes are in optimal range."
            : `${smallFileIssues.length} table(s) with small file problem: ${smallFileIssues.slice(0, 3).join(", ")}`,
          recommendation: "Run OPTIMIZE to compact small files. Enable autoOptimize for future writes.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-020: Auto-Optimize Enabled
        // ══════════════════════════════════════════════
        const noAutoOpt: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          const autoOpt = config["delta.autoOptimize.optimizeWrite"] ?? "false";
          if (autoOpt !== "true") noAutoOpt.push(table);
        }
        rules.push({
          id: "LH-020", rule: "Auto-Optimize Enabled", category: "Performance", severity: "MEDIUM",
          status: noAutoOpt.length === 0 ? "PASS" : "WARN",
          details: noAutoOpt.length === 0
            ? "All tables have autoOptimize.optimizeWrite enabled."
            : `${noAutoOpt.length} table(s) without auto-optimize: ${noAutoOpt.slice(0, 5).join(", ")}`,
          recommendation: "ALTER TABLE SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true').",
        });

        // ══════════════════════════════════════════════
        // RULE LH-021: Retention Policy Configured
        // ══════════════════════════════════════════════
        const noRetention: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          if (!config["delta.logRetentionDuration"] && !config["delta.deletedFileRetentionDuration"]) {
            noRetention.push(table);
          }
        }
        rules.push({
          id: "LH-021", rule: "Retention Policy Configured", category: "Maintenance", severity: "LOW",
          status: noRetention.length === 0 ? "PASS" : "WARN",
          details: noRetention.length === 0
            ? "All tables have retention policies configured."
            : `${noRetention.length} table(s) without retention policy: ${noRetention.slice(0, 5).join(", ")}`,
          recommendation: "Set logRetentionDuration and deletedFileRetentionDuration to control storage costs.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-022: Excessive Delta Log Versions
        // ══════════════════════════════════════════════
        const tooManyVersions: string[] = [];
        for (const { table, log } of deltaLogResults) {
          if (log.totalVersions > 100) {
            tooManyVersions.push(`${table}(${log.totalVersions} versions)`);
          }
        }
        rules.push({
          id: "LH-022", rule: "Delta Log Version Count Reasonable", category: "Performance", severity: "LOW",
          status: tooManyVersions.length === 0 ? "PASS" : "WARN",
          details: tooManyVersions.length === 0
            ? "All tables have reasonable version counts."
            : `${tooManyVersions.length} table(s) with many versions: ${tooManyVersions.slice(0, 5).join(", ")}`,
          recommendation: "Run VACUUM to trigger checkpoint creation and reduce log replay time.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-023: Write Amplification Check
        // ══════════════════════════════════════════════
        const highWriteAmp: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const ops = countOperations(log);
          const totalOps = Object.values(ops).reduce((s, v) => s + v, 0);
          const mergeDeletes = (ops["MERGE"] ?? 0) + (ops["DELETE"] ?? 0) + (ops["UPDATE"] ?? 0);
          if (totalOps > 10 && mergeDeletes / totalOps > 0.5) {
            highWriteAmp.push(`${table}(${mergeDeletes}/${totalOps} ops are MERGE/UPDATE/DELETE)`);
          }
        }
        rules.push({
          id: "LH-023", rule: "Low Write Amplification", category: "Performance", severity: "MEDIUM",
          status: highWriteAmp.length === 0 ? "PASS" : "WARN",
          details: highWriteAmp.length === 0
            ? "Write operations are mostly appends — low write amplification."
            : `${highWriteAmp.length} table(s) with high MERGE/UPDATE/DELETE ratio: ${highWriteAmp.slice(0, 3).join(", ")}`,
          recommendation: "Consider append-only patterns or Liquid Clustering to reduce write amplification.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-024: Data Skipping Configured
        // ══════════════════════════════════════════════
        const noDataSkipping: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          const skipCols = parseInt(config["delta.dataSkippingNumIndexedCols"] ?? "0", 10);
          if (skipCols === 0) noDataSkipping.push(table);
        }
        rules.push({
          id: "LH-024", rule: "Data Skipping Configured", category: "Performance", severity: "LOW",
          status: noDataSkipping.length === 0 ? "PASS" : "WARN",
          details: noDataSkipping.length === 0
            ? "All tables have data skipping configured."
            : `${noDataSkipping.length} table(s) without explicit data skipping: ${noDataSkipping.slice(0, 5).join(", ")}`,
          recommendation: "SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32') for faster queries.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-025: Z-Order Applied to Large Tables
        // ══════════════════════════════════════════════
        const needsZOrder: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const stats = getFileSizeStats(log);
          if (stats.totalSizeBytes > 10 * 1024 * 1024 * 1024) { // >10GB
            const hasZOrder = log.commits.some(c =>
              c.operation === "OPTIMIZE" && JSON.stringify(c.operationParameters ?? {}).includes("zOrderBy")
            );
            if (!hasZOrder) needsZOrder.push(table);
          }
        }
        rules.push({
          id: "LH-025", rule: "Z-Order on Large Tables", category: "Performance", severity: "MEDIUM",
          status: needsZOrder.length === 0 ? "PASS" : "WARN",
          details: needsZOrder.length === 0
            ? "All large tables have Z-Order applied or are <10GB."
            : `${needsZOrder.length} large table(s) >10GB without Z-Order: ${needsZOrder.join(", ")}`,
          recommendation: "OPTIMIZE table ZORDER BY (frequently filtered columns) for faster queries.",
        });

        // ══════════════════════════════════════════════
        // RULE LH-026: V-Order Enabled
        // ══════════════════════════════════════════════
        const noVOrder: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          if (!config["delta.parquet.vorder.enabled"] || config["delta.parquet.vorder.enabled"] !== "true") {
            noVOrder.push(table);
          }
        }
        rules.push({
          id: "LH-026", rule: "V-Order Enabled", category: "Performance", severity: "MEDIUM",
          status: noVOrder.length === 0 ? "PASS" : "WARN",
          details: noVOrder.length === 0 ? "All tables have V-Order enabled." : `${noVOrder.length} table(s) without V-Order: ${noVOrder.slice(0, 3).join(", ")}`,
          recommendation: "Enable V-Order for 30-50% better compression and faster reads. Fix: v-order",
        });

        // ══════════════════════════════════════════════
        // RULE LH-027: Change Data Feed on Large Tables
        // ══════════════════════════════════════════════
        const largeDeltaWithoutCDF: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          const stats = getFileSizeStats(log);
          if (stats.totalFiles > 0 && config["delta.enableChangeDataFeed"] !== "true") {
            // Check if table is "large" via file count as proxy (>100 files)
            if (stats.totalFiles > 100) {
              largeDeltaWithoutCDF.push(table);
            }
          }
        }
        rules.push({
          id: "LH-027", rule: "Change Data Feed on Large Tables", category: "Data Management", severity: "LOW",
          status: largeDeltaWithoutCDF.length === 0 ? "PASS" : "WARN",
          details: largeDeltaWithoutCDF.length === 0 ? "All large tables have CDF enabled." : `${largeDeltaWithoutCDF.length} large table(s) without Change Data Feed.`,
          recommendation: "Enable CDF for incremental ETL. Fix: change-data-feed",
        });

        // ══════════════════════════════════════════════
        // RULE LH-028: Column Mapping Enabled
        // ══════════════════════════════════════════════
        const withoutColMapping: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          if (!config["delta.columnMapping.mode"] || config["delta.columnMapping.mode"] === "none") {
            withoutColMapping.push(table);
          }
        }
        rules.push({
          id: "LH-028", rule: "Column Mapping Enabled", category: "Maintainability", severity: "LOW",
          status: withoutColMapping.length === 0 ? "PASS" : "WARN",
          details: withoutColMapping.length === 0 ? "All tables have column mapping." : `${withoutColMapping.length} table(s) without column mapping mode=name.`,
          recommendation: "Enable column mapping for schema evolution support. Fix: column-mapping",
        });

        // ══════════════════════════════════════════════
        // RULE LH-029: Deletion Vectors Enabled
        // ══════════════════════════════════════════════
        const withoutDV: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          if (config["delta.enableDeletionVectors"] !== "true") {
            withoutDV.push(table);
          }
        }
        rules.push({
          id: "LH-029", rule: "Deletion Vectors Enabled", category: "Performance", severity: "LOW",
          status: withoutDV.length === 0 ? "PASS" : "WARN",
          details: withoutDV.length === 0 ? "All tables have deletion vectors." : `${withoutDV.length} table(s) without deletion vectors.`,
          recommendation: "Enable deletion vectors for faster UPDATE/DELETE/MERGE. Fix: deletion-vectors",
        });

        // ══════════════════════════════════════════════
        // RULE LH-030: Checkpoint Interval Check
        // ══════════════════════════════════════════════
        const badCheckpoint: string[] = [];
        for (const { table, log } of deltaLogResults) {
          const config = getTableConfig(log);
          const interval = parseInt(config["delta.checkpointInterval"] ?? "10", 10);
          if (interval > 50) badCheckpoint.push(table);
        }
        rules.push({
          id: "LH-030", rule: "Checkpoint Interval Appropriate", category: "Performance", severity: "LOW",
          status: badCheckpoint.length === 0 ? "PASS" : "WARN",
          details: badCheckpoint.length === 0 ? "All tables have reasonable checkpoint intervals." : `${badCheckpoint.length} table(s) with high checkpoint interval (>50).`,
          recommendation: "Set checkpoint interval to 10 for faster query startup. Fix: checkpoint-interval",
        });
      }
    }
  }

  return renderRuleReport(
    `Lakehouse Analysis: ${lakehouse.displayName}`,
    new Date().toISOString(),
    header,
    rules
  );
}

// ──────────────────────────────────────────────
// Tool: lakehouse_fix — Spark SQL fixes via temp Notebook
// ──────────────────────────────────────────────

const LAKEHOUSE_FIX_COMMANDS: Record<string, {
  description: string;
  getCode: (lakehouseName: string, tableName: string) => string;
}> = {
  "auto-optimize": {
    description: "Enable auto-optimize (optimizeWrite + autoCompact)",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')")\nprint("✅ Auto-optimize enabled for ${t}")`,
  },
  "retention": {
    description: "Set log retention (30 days) and deleted file retention (7 days)",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days', 'delta.deletedFileRetentionDuration' = 'interval 7 days')")\nprint("✅ Retention policy set for ${t}")`,
  },
  "data-skipping": {
    description: "Enable data skipping with 32 indexed columns",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32')")\nprint("✅ Data skipping enabled for ${t}")`,
  },
  "audit-columns": {
    description: "Add created_at and updated_at audit columns (idempotent)",
    getCode: (lh, t) =>
      `existing = [c.name.lower() for c in spark.table("\`${lh}\`.\`${t}\`").schema]\nadded = []\nif "created_at" not in existing:\n    spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` ADD COLUMNS (created_at TIMESTAMP)")\n    added.append("created_at")\nif "updated_at" not in existing:\n    spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` ADD COLUMNS (updated_at TIMESTAMP)")\n    added.append("updated_at")\nif added:\n    print(f"✅ Added {', '.join(added)} to ${t}")\nelse:\n    print("✅ Audit columns already exist on ${t} - skipped")`,
  },
  "v-order": {
    description: "Enable V-Order compression for better read performance",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.parquet.vorder.enabled' = 'true')")\nprint("✅ V-Order enabled for ${t}")`,
  },
  "change-data-feed": {
    description: "Enable Change Data Feed for incremental processing",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")\nprint("✅ Change Data Feed enabled for ${t}")`,
  },
  "column-mapping": {
    description: "Enable column mapping mode=name for schema evolution",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')")\nprint("✅ Column mapping enabled for ${t}")`,
  },
  "checkpoint-interval": {
    description: "Set optimal checkpoint interval (10 commits)",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.checkpointInterval' = '10')")\nprint("✅ Checkpoint interval set for ${t}")`,
  },
  "deletion-vectors": {
    description: "Enable deletion vectors for faster deletes/updates",
    getCode: (lh, t) =>
      `spark.sql("ALTER TABLE \`${lh}\`.\`${t}\` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")\nprint("✅ Deletion vectors enabled for ${t}")`,
  },
  "compute-stats": {
    description: "Compute table statistics for query optimization",
    getCode: (lh, t) =>
      `spark.sql("ANALYZE TABLE \`${lh}\`.\`${t}\` COMPUTE STATISTICS")\nprint("✅ Statistics computed for ${t}")`,
  },
};

// ──────────────────────────────────────────────
// Tool: lakehouse_auto_optimize — Fix ALL tables via single Livy session
// ──────────────────────────────────────────────

export async function lakehouseAutoOptimize(args: {
  workspaceId: string;
  lakehouseId: string;
  fixIds?: string[];
  dryRun?: boolean;
}): Promise<string> {
  const lakehouse = await getLakehouse(args.workspaceId, args.lakehouseId);
  const lhName = lakehouse.displayName;
  validateSparkName(lhName, "lakehouse name");
  const isDryRun = args.dryRun ?? false;
  const fixIds = args.fixIds ?? ["auto-optimize", "retention", "data-skipping"];

  // Discover all Delta tables
  const allTables = await listLakehouseTables(args.workspaceId, args.lakehouseId);
  const deltaTables = allTables.filter(
    (t: LakehouseTable) => (t.format ?? "").toLowerCase() === "delta"
  );

  if (deltaTables.length === 0) {
    return `# 🔧 Lakehouse Auto-Optimize: ${lhName}\n\nNo Delta tables found. Nothing to optimize.`;
  }

  // Build commands for every table × every fix
  const commands: Array<{ table: string; fixId: string; description: string; code: string }> = [];
  for (const t of deltaTables) {
    validateSparkName(t.name, "table name");
    for (const fixId of fixIds) {
      const fix = LAKEHOUSE_FIX_COMMANDS[fixId];
      if (!fix) continue;
      commands.push({
        table: t.name,
        fixId,
        description: fix.description,
        code: fix.getCode(lhName, t.name),
      });
    }
  }

  // Dry-run: preview only
  if (isDryRun) {
    const lines = [
      `# 🔧 Lakehouse Auto-Optimize: ${lhName}`,
      "",
      `_DRY RUN at ${new Date().toISOString()}_`,
      "",
      `**${deltaTables.length} tables × ${fixIds.length} fixes = ${commands.length} commands previewed**`,
      "",
      "| Table | Fix | Description |",
      "|-------|-----|-------------|",
    ];
    for (const cmd of commands) {
      lines.push(`| ${cmd.table} | ${cmd.fixId} | ${cmd.description} |`);
    }
    lines.push("", "> 💡 Set `dryRun: false` to execute via Livy API.");
    return lines.join("\n");
  }

  // Execute via single Livy session
  const lines = [
    `# 🔧 Lakehouse Auto-Optimize: ${lhName}`,
    "",
    `_Executed at ${new Date().toISOString()} via Livy API_`,
    "",
  ];

  try {
    const { results } = await runSparkFixesViaLivy(
      args.workspaceId,
      args.lakehouseId,
      commands
    );

    const passed = results.filter(r => r.status === "ok").length;
    const failed = results.length - passed;

    lines.push(
      `**${passed} succeeded, ${failed} failed** across ${deltaTables.length} tables`,
      "",
      "| Table | Fix | Status | Detail |",
      "|-------|-----|--------|--------|",
    );

    for (const r of results) {
      const icon = r.status === "ok" ? "✅" : "❌";
      const detail = r.status === "ok" ? (r.output ?? "OK") : (r.error ?? "Failed");
      lines.push(`| ${r.table} | ${r.fixId} | ${icon} | ${detail} |`);
    }
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    lines.push(`**❌ Livy session failed**: ${msg}`);
  }

  return lines.join("\n");
}

export async function lakehouseFix(args: {
  workspaceId: string;
  lakehouseId: string;
  tableName: string;
  fixIds?: string[];
  dryRun?: boolean;
}): Promise<string> {
  // Input validation
  validateSparkName(args.tableName, "tableName");

  const lakehouse = await getLakehouse(args.workspaceId, args.lakehouseId);
  const fixIds = args.fixIds ?? Object.keys(LAKEHOUSE_FIX_COMMANDS);
  const lhName = lakehouse.displayName;
  const isDryRun = args.dryRun ?? false;

  validateSparkName(lhName, "lakehouse name");

  const fixDescriptions: { id: string; description: string; code: string }[] = [];

  for (const fixId of fixIds) {
    const fix = LAKEHOUSE_FIX_COMMANDS[fixId];
    if (fix) {
      const code = fix.getCode(lhName, args.tableName);
      fixDescriptions.push({ id: fixId, description: fix.description, code });
    }
  }

  if (fixDescriptions.length === 0) {
    return "❌ No valid fix IDs provided. Available: auto-optimize, retention, data-skipping, audit-columns";
  }

  // Dry-run: return preview without executing
  if (isDryRun) {
    const lines = [
      `# 🔧 Lakehouse Fix: ${lhName}.${args.tableName}`,
      "",
      `_DRY RUN (preview only) at ${new Date().toISOString()}_`,
      "",
      `**${fixDescriptions.length} command(s) previewed** — re-run without dryRun to apply.`,
      "",
      "| Fix | Description | Spark SQL |",
      "|-----|-------------|-----------|",
    ];
    for (const f of fixDescriptions) {
      lines.push(`| ${f.id} | 🔍 ${f.description} | \`${f.code.substring(0, 80)}...\` |`);
    }
    lines.push("", "> 💡 Set `dryRun: false` to execute these commands via Livy API.");
    return lines.join("\n");
  }

  // Build commands for Livy
  const commands = fixDescriptions.map((f) => ({
    table: args.tableName,
    fixId: f.id,
    description: f.description,
    code: f.code,
  }));

  // Try Livy API first (no notebook needed)
  let usedMethod = "Livy API";
  let fixResults: Array<{ id: string; description: string; status: string; detail: string }> = [];

  try {
    const { results } = await runSparkFixesViaLivy(
      args.workspaceId,
      args.lakehouseId,
      commands
    );

    for (const r of results) {
      fixResults.push({
        id: r.fixId,
        description: r.description,
        status: r.status === "ok" ? "✅" : "❌",
        detail: r.status === "ok" ? (r.output ?? "OK") : (r.error ?? "Failed"),
      });
    }
  } catch (livyError) {
    // Livy failed — fall back to notebook approach
    usedMethod = "Notebook (Livy fallback)";

    const code = fixDescriptions.map((f) => f.code).join("\n\n");
    const result = await runTemporaryNotebook(args.workspaceId, code);

    for (const f of fixDescriptions) {
      fixResults.push({
        id: f.id,
        description: f.description,
        status: result.status === "Completed" ? "✅" : "❌",
        detail: result.status === "Completed" ? "OK" : (result.error ?? "Failed"),
      });
    }
  }

  const allOk = fixResults.every((r) => r.status === "✅");

  const lines = [
    `# 🔧 Lakehouse Fix: ${lhName}.${args.tableName}`,
    "",
    `_Executed at ${new Date().toISOString()} via ${usedMethod}_`,
    "",
    `**Status**: ${allOk ? "✅ Success" : "⚠️ Partial/Failed"}`,
    "",
    "| Fix | Status | Detail |",
    "|-----|--------|--------|",
  ];

  for (const f of fixResults) {
    lines.push(`| ${f.id} ${f.description} | ${f.status} | ${f.detail} |`);
  }

  return lines.join("\n");
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const lakehouseTools = [
  {
    name: "lakehouse_list",
    description:
      "List all lakehouses in a Fabric workspace with their metadata, SQL endpoint status, and OneLake paths.",
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
    handler: lakehouseList,
  },
  {
    name: "lakehouse_list_tables",
    description:
      "List all tables in a Fabric Lakehouse with their type, format (Delta/Parquet), and location.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        lakehouseId: {
          type: "string",
          description: "The ID of the lakehouse",
        },
      },
      required: ["workspaceId", "lakehouseId"],
    },
    handler: lakehouseListTables,
  },
  {
    name: "lakehouse_run_table_maintenance",
    description:
      "Run table maintenance (OPTIMIZE with V-Order, Z-ORDER, VACUUM) on a Fabric Lakehouse. " +
      "Can target a specific table or all tables. Compacts small files, applies V-Order compression, " +
      "and removes unreferenced old files.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        lakehouseId: {
          type: "string",
          description: "The ID of the lakehouse",
        },
        tableName: {
          type: "string",
          description: "Optional: name of a specific table to optimize. If omitted, all tables are processed.",
        },
        optimizeSettings: {
          type: "object",
          description: "OPTIMIZE settings",
          properties: {
            vOrder: {
              type: "boolean",
              description: "Enable V-Order optimization (default: true)",
            },
            zOrderColumns: {
              type: "array",
              items: { type: "string" },
              description: "Columns to Z-ORDER by for faster filtered reads",
            },
          },
        },
        vacuumSettings: {
          type: "object",
          description: "VACUUM settings",
          properties: {
            retentionPeriod: {
              type: "string",
              description:
                "Retention period in format 'D.HH:MM:SS' (default: '7.00:00:00' = 7 days)",
            },
          },
        },
      },
      required: ["workspaceId", "lakehouseId"],
    },
    handler: lakehouseRunTableMaintenance,
  },
  {
    name: "lakehouse_get_job_status",
    description:
      "Check the status of a table maintenance job on a Fabric Lakehouse.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        lakehouseId: {
          type: "string",
          description: "The ID of the lakehouse",
        },
        jobInstanceId: {
          type: "string",
          description: "The ID of the job instance to check",
        },
      },
      required: ["workspaceId", "lakehouseId", "jobInstanceId"],
    },
    handler: lakehouseGetJobStatus,
  },
  {
    name: "lakehouse_optimization_recommendations",
    description:
      "LIVE SCAN: Analyzes a Fabric Lakehouse by checking table formats (Delta vs non-Delta), " +
      "connecting to the SQL Analytics Endpoint to inspect row counts, column data types, " +
      "empty tables, and large tables. Returns findings with prioritized action items.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        lakehouseId: {
          type: "string",
          description: "The ID of the lakehouse to analyze",
        },
      },
      required: ["workspaceId", "lakehouseId"],
    },
    handler: lakehouseOptimizationRecommendations,
  },
  {
    name: "lakehouse_fix",
    description:
      "AUTO-FIX: Applies Spark SQL fixes to a Lakehouse via Livy API (no notebooks needed). " +
      "Falls back to temporary Notebook if Livy is unavailable. " +
      "Can fix: auto-optimize, retention policy, data skipping, audit columns, " +
      "v-order, change-data-feed, column-mapping, checkpoint-interval, deletion-vectors, compute-stats. " +
      "Use dryRun=true to preview commands without executing them. " +
      "Available fixIds: auto-optimize, retention, data-skipping, audit-columns, v-order, change-data-feed, column-mapping, checkpoint-interval, deletion-vectors, compute-stats.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: { type: "string", description: "The ID of the Fabric workspace" },
        lakehouseId: { type: "string", description: "The ID of the lakehouse" },
        tableName: { type: "string", description: "The table to fix" },
        fixIds: {
          type: "array", items: { type: "string" },
          description: "Fix IDs to apply: auto-optimize, retention, data-skipping, audit-columns, v-order, change-data-feed, column-mapping, checkpoint-interval, deletion-vectors, compute-stats. If omitted, all are applied.",
        },
        dryRun: {
          type: "boolean",
          description: "If true, preview commands without executing them (default: false)",
        },
      },
      required: ["workspaceId", "lakehouseId", "tableName"],
    },
    handler: lakehouseFix,
  },
  {
    name: "lakehouse_auto_optimize",
    description:
      "AUTO-OPTIMIZE: Discovers ALL Delta tables in a Lakehouse and applies fixes to every table " +
      "in a single Livy Spark session (no notebooks needed). " +
      "Default fixes: auto-optimize, retention, data-skipping. " +
      "Use dryRun=true to preview. Use fixIds to select specific fixes. " +
      "Additional fixes: v-order, change-data-feed, column-mapping, checkpoint-interval, deletion-vectors, compute-stats.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: { type: "string", description: "The ID of the Fabric workspace" },
        lakehouseId: { type: "string", description: "The ID of the lakehouse" },
        fixIds: {
          type: "array", items: { type: "string" },
          description: "Fix IDs: auto-optimize, retention, data-skipping, audit-columns, v-order, change-data-feed, column-mapping, checkpoint-interval, deletion-vectors, compute-stats. Default: first three.",
        },
        dryRun: {
          type: "boolean",
          description: "If true, preview commands without executing (default: false)",
        },
      },
      required: ["workspaceId", "lakehouseId"],
    },
    handler: lakehouseAutoOptimize,
  },
];
