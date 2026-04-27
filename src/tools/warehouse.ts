import {
  listWarehouses,
  getWarehouse,
} from "../clients/fabricClient.js";
import { runDiagnosticQueries, executeSqlQuery } from "../clients/sqlClient.js";
import type { FabricWarehouse } from "../clients/fabricClient.js";
import type { SqlRow } from "../clients/sqlClient.js";
import { renderRuleReport } from "./ruleEngine.js";
import type { RuleResult } from "./ruleEngine.js";

// ──────────────────────────────────────────────
// Input validation — prevent SQL injection in auto-fix
// ──────────────────────────────────────────────

const SAFE_SQL_NAME = /^[a-zA-Z0-9_\[\].\- ]+$/;

function validateSqlName(value: string, label: string): void {
  if (!SAFE_SQL_NAME.test(value)) {
    throw new Error(`Invalid ${label}: must be alphanumeric/underscore/bracket/dot only.`);
  }
}

/** Bracket-quote a SQL identifier (schema.table → [schema].[table]) */
function quoteSqlId(name: string): string {
  // If already bracketed, return as-is
  if (name.startsWith("[") && name.endsWith("]")) return name;
  // Handle schema.table format
  return name.split(".").map(part => {
    const trimmed = part.replace(/^\[|\]$/g, "");
    return `[${trimmed}]`;
  }).join(".");
}

// ──────────────────────────────────────────────
// Tool: warehouse_list
// ──────────────────────────────────────────────

export async function warehouseList(args: { workspaceId: string }): Promise<string> {
  const warehouses = await listWarehouses(args.workspaceId);

  if (warehouses.length === 0) {
    return "No warehouses found in this workspace.";
  }

  const lines = warehouses.map((wh: FabricWarehouse) =>
    [
      `- **${wh.displayName}** (ID: ${wh.id})`,
      wh.properties?.connectionString
        ? `  Connection: ${wh.properties.connectionString}`
        : null,
      wh.properties?.createdDate
        ? `  Created: ${wh.properties.createdDate}`
        : null,
    ].filter(Boolean).join("\n")
  );

  return `## Warehouses in workspace ${args.workspaceId}\n\n${lines.join("\n\n")}`;
}

// ──────────────────────────────────────────────
// SQL Diagnostic Queries
// ──────────────────────────────────────────────

const WAREHOUSE_DIAGNOSTICS = {
  tables: `
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY s.name, t.name`,

  columns: `
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
           CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
           IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`,

  stats: `
    SELECT s.name AS schema_name, t.name AS table_name,
           st.name AS stat_name, st.auto_created
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN sys.stats st ON t.object_id = st.object_id
    ORDER BY s.name, t.name`,

  slowQueries: `
    SELECT TOP 15
        LEFT(command, 300) AS query_text,
        start_time, end_time,
        total_elapsed_time_ms,
        row_count, status
    FROM queryinsights.exec_requests_history
    WHERE status = 'Succeeded'
    ORDER BY total_elapsed_time_ms DESC`,

  frequentQueries: `
    SELECT TOP 15
        LEFT(command, 300) AS query_text,
        COUNT(*) AS execution_count,
        AVG(total_elapsed_time_ms) AS avg_duration_ms,
        MAX(total_elapsed_time_ms) AS max_duration_ms
    FROM queryinsights.exec_requests_history
    WHERE status = 'Succeeded'
    GROUP BY LEFT(command, 300)
    ORDER BY execution_count DESC`,

  failedQueries: `
    SELECT TOP 10
        LEFT(command, 300) AS query_text,
        start_time, status
    FROM queryinsights.exec_requests_history
    WHERE status = 'Failed'
    ORDER BY start_time DESC`,

  queryVolume: `
    SELECT
        CAST(start_time AS DATE) AS query_date,
        COUNT(*) AS query_count,
        AVG(total_elapsed_time_ms) AS avg_duration_ms
    FROM queryinsights.exec_requests_history
    GROUP BY CAST(start_time AS DATE)
    ORDER BY query_date DESC`,

  // ── Structural Analysis Queries (BPA-style) ──

  missingPrimaryKeys: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name
    FROM sys.tables t
    LEFT JOIN sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
    WHERE i.object_id IS NULL AND t.is_ms_shipped = 0
    ORDER BY t.name`,

  deprecatedTypes: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           c.name AS column_name, typ.name AS data_type
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    JOIN sys.types typ ON c.user_type_id = typ.user_type_id
    WHERE typ.name IN ('text', 'ntext', 'image')
    ORDER BY t.name, c.name`,

  floatingPointColumns: `
    SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS table_name,
           COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND DATA_TYPE IN ('float', 'real')
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  oversizedColumns: `
    SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS table_name,
           COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH AS max_length
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar')
      AND CHARACTER_MAXIMUM_LENGTH > 500
    ORDER BY CHARACTER_MAXIMUM_LENGTH DESC`,

  namingIssues: `
    SELECT t.name AS table_name, c.name AS column_name
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE c.name COLLATE Latin1_General_BIN LIKE '%[^a-zA-Z0-9_]%'
      AND t.is_ms_shipped = 0
    ORDER BY t.name, c.name`,

  viewsWithSelectStar: `
    SELECT SCHEMA_NAME(v.schema_id) + '.' + v.name AS view_name
    FROM sys.views v
    JOIN sys.sql_modules m ON v.object_id = m.object_id
    WHERE m.definition LIKE '%SELECT *%'
      AND SCHEMA_NAME(v.schema_id) NOT IN ('sys', 'queryinsights')
      AND v.name NOT IN ('exec_requests_history', 'long_running_queries',
                         'frequently_run_queries', 'exec_sessions_history')
    ORDER BY v.name`,

  staleStatistics: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           s.name AS stat_name,
           STATS_DATE(s.object_id, s.stats_id) AS last_updated,
           DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) AS days_old
    FROM sys.stats s
    JOIN sys.tables t ON s.object_id = t.object_id
    WHERE s.auto_created = 1
      AND DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30
    ORDER BY days_old DESC`,

  constraintCheck: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           f.name AS constraint_name,
           f.is_disabled, f.is_not_trusted
    FROM sys.foreign_keys f
    JOIN sys.tables t ON f.parent_object_id = t.object_id
    WHERE f.is_disabled = 1 OR f.is_not_trusted = 1`,

  // ── Data Quality Queries (from Force BPA) ──

  nullableKeyColumns: `
    SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS table_name,
           COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
      AND IS_NULLABLE = 'YES'
      AND (COLUMN_NAME LIKE '%Id' OR COLUMN_NAME LIKE '%_id'
           OR COLUMN_NAME LIKE '%Key' OR COLUMN_NAME LIKE '%_key'
           OR COLUMN_NAME = 'id')
    ORDER BY TABLE_NAME, COLUMN_NAME`,

  emptyTables: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1) AND t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING SUM(p.rows) = 0
    ORDER BY t.name`,

  wideTables: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           COUNT(c.column_id) AS column_count
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING COUNT(c.column_id) > 50
    ORDER BY COUNT(c.column_id) DESC`,

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

  missingForeignKeys: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name
    FROM sys.tables t
    LEFT JOIN sys.foreign_keys fk ON t.object_id = fk.parent_object_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING COUNT(fk.object_id) = 0
    ORDER BY t.name`,

  blobColumns: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           c.name AS column_name,
           TYPE_NAME(c.user_type_id) AS data_type
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
      AND TYPE_NAME(c.user_type_id) IN ('varbinary','varchar','nvarchar')
      AND (c.max_length = -1 OR c.max_length > 8000)
    ORDER BY t.name, c.name`,

  missingAuditColumns: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           SUM(CASE WHEN c.name IN ('created_at','created_date','CreatedAt','CreatedDate') THEN 1 ELSE 0 END) AS has_created,
           SUM(CASE WHEN c.name IN ('updated_at','updated_date','modified_at','modified_date','UpdatedAt','ModifiedAt','UpdatedDate','ModifiedDate') THEN 1 ELSE 0 END) AS has_updated,
           SUM(CASE WHEN c.name IN ('created_by','CreatedBy') THEN 1 ELSE 0 END) AS has_created_by,
           SUM(CASE WHEN c.name IN ('updated_by','modified_by','UpdatedBy','ModifiedBy') THEN 1 ELSE 0 END) AS has_updated_by
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING SUM(CASE WHEN c.name IN ('created_at','created_date','CreatedAt','CreatedDate','updated_at','updated_date','modified_at','modified_date','UpdatedAt','ModifiedAt','UpdatedDate','ModifiedDate') THEN 1 ELSE 0 END) = 0`,

  sensitiveColumns: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           c.name AS column_name
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
      AND (c.name LIKE '%credit%' OR c.name LIKE '%ssn%' OR c.name LIKE '%password%'
           OR c.name LIKE '%secret%' OR c.name LIKE '%phone%' OR c.name LIKE '%email%'
           OR c.name LIKE '%IBAN%' OR c.name LIKE '%SWIFT%' OR c.name LIKE '%BIC%'
           OR c.name LIKE '%license%' OR c.name LIKE '%tax%id%')
    ORDER BY t.name, c.name`,

  dataMaskingCheck: `
    SELECT OBJECT_SCHEMA_NAME(c.object_id) + '.' + OBJECT_NAME(c.object_id) AS table_name,
           c.name AS column_name,
           m.masking_function
    FROM sys.masked_columns m
    JOIN sys.columns c ON m.object_id = c.object_id AND m.column_id = c.column_id`,

  rlsCheck: `
    SELECT name AS policy_name, is_enabled
    FROM sys.security_policies`,

  dbOwnerMembers: `
    SELECT p.name AS member_name, r.name AS role_name
    FROM sys.database_role_members rm
    JOIN sys.database_principals p ON rm.member_principal_id = p.principal_id
    JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
    WHERE r.name = 'db_owner'`,

  viewDependencies: `
    SELECT SCHEMA_NAME(v.schema_id) + '.' + v.name AS view_name,
           COUNT(d.referenced_id) AS dependency_count
    FROM sys.views v
    LEFT JOIN sys.sql_expression_dependencies d ON v.object_id = d.referencing_id
    WHERE SCHEMA_NAME(v.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
    GROUP BY v.schema_id, v.name
    HAVING COUNT(d.referenced_id) > 10
    ORDER BY COUNT(d.referenced_id) DESC`,

  crossSchemaDeps: `
    SELECT SCHEMA_NAME(o.schema_id) + '.' + o.name AS referencing_object,
           SCHEMA_NAME(ref.schema_id) + '.' + ref.name AS referenced_object
    FROM sys.sql_expression_dependencies d
    JOIN sys.objects o ON d.referencing_id = o.object_id
    JOIN sys.objects ref ON d.referenced_id = ref.object_id
    WHERE o.schema_id <> ref.schema_id
      AND o.type IN ('V','P','FN','IF','TF')
      AND SCHEMA_NAME(o.schema_id) NOT IN ('sys','INFORMATION_SCHEMA','queryinsights')
      AND SCHEMA_NAME(ref.schema_id) NOT IN ('sys','INFORMATION_SCHEMA','queryinsights')`,

  circularForeignKeys: `
    SELECT OBJECT_SCHEMA_NAME(fk1.parent_object_id) + '.' + OBJECT_NAME(fk1.parent_object_id) AS table1,
           OBJECT_SCHEMA_NAME(fk1.referenced_object_id) + '.' + OBJECT_NAME(fk1.referenced_object_id) AS table2
    FROM sys.foreign_keys fk1
    JOIN sys.foreign_keys fk2 ON fk1.referenced_object_id = fk2.parent_object_id
    WHERE fk1.parent_object_id = fk2.referenced_object_id`,

  tableNamingIssues: `
    SELECT name AS table_name
    FROM sys.tables
    WHERE is_ms_shipped = 0
      AND (name LIKE '% %' OR name LIKE '%[^0-9A-Za-z_]%')`,

  // ── Database-level settings ──

  dbSettings: `
    SELECT
      is_auto_update_stats_on,
      is_auto_update_stats_async_on,
      is_result_set_caching_on,
      compatibility_level,
      is_ansi_nulls_on,
      is_ansi_padding_on,
      is_ansi_warnings_on,
      is_arithabort_on,
      is_quoted_identifier_on,
      snapshot_isolation_state,
      is_read_committed_snapshot_on,
      page_verify_option_desc,
      state_desc,
      user_access_desc,
      containment_desc,
      is_fulltext_enabled,
      is_data_retention_enabled
    FROM sys.databases
    WHERE name = DB_NAME()`,

  rowCounts: `
    SELECT s.name AS schema_name, t.name AS table_name,
           SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1) AND t.is_ms_shipped = 0
    GROUP BY s.name, t.name
    ORDER BY row_count DESC`,

  // ── New queries ──

  lowRowCountTables: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1) AND t.is_ms_shipped = 0
    GROUP BY t.schema_id, t.name
    HAVING SUM(p.rows) BETWEEN 1 AND 10
    ORDER BY SUM(p.rows)`,

  storedProcedures: `
    SELECT o.name AS proc_name,
           CASE WHEN ep.value IS NULL THEN 0 ELSE 1 END AS has_description
    FROM sys.procedures o
    LEFT JOIN sys.extended_properties ep
      ON o.object_id = ep.major_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
    WHERE o.is_ms_shipped = 0
    ORDER BY o.name`,

  missingDefaults: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           c.name AS column_name
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    LEFT JOIN sys.default_constraints dc ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
    WHERE t.is_ms_shipped = 0
      AND dc.object_id IS NULL
      AND c.is_nullable = 0
      AND c.is_identity = 0
    ORDER BY t.name, c.name`,

  unicodeMix: `
    SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS table_name,
           SUM(CASE WHEN TYPE_NAME(c.user_type_id) IN ('nvarchar','nchar') THEN 1 ELSE 0 END) AS unicode_count,
           SUM(CASE WHEN TYPE_NAME(c.user_type_id) IN ('varchar','char') THEN 1 ELSE 0 END) AS non_unicode_count
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.is_ms_shipped = 0
      AND TYPE_NAME(c.user_type_id) IN ('nvarchar','nchar','varchar','char')
    GROUP BY t.schema_id, t.name
    HAVING SUM(CASE WHEN TYPE_NAME(c.user_type_id) IN ('nvarchar','nchar') THEN 1 ELSE 0 END) > 0
      AND SUM(CASE WHEN TYPE_NAME(c.user_type_id) IN ('varchar','char') THEN 1 ELSE 0 END) > 0`,

  schemaDocumentation: `
    SELECT s.name AS schema_name,
           CASE WHEN ep.value IS NULL THEN 0 ELSE 1 END AS has_description
    FROM sys.schemas s
    LEFT JOIN sys.extended_properties ep
      ON ep.class = 3 AND ep.major_id = s.schema_id AND ep.name = 'MS_Description'
    WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA','guest','db_owner','db_accessadmin',
      'db_securityadmin','db_ddladmin','db_backupoperator','db_datareader',
      'db_datawriter','db_denydatareader','db_denydatawriter','queryinsights')
    ORDER BY s.name`,

  queryVolumeAvg: `
    SELECT
        AVG(total_elapsed_time_ms) AS avg_duration_ms,
        COUNT(*) AS total_queries
    FROM queryinsights.exec_requests_history
    WHERE start_time > DATEADD(day, -7, GETDATE())
      AND status = 'Succeeded'`,

  computedColumns: `SELECT c.name AS column_name, SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
    FROM sys.computed_columns c
    JOIN sys.tables t ON c.object_id = t.object_id`,
  allColumns: `SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, COUNT(*) AS col_count
    FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id
    GROUP BY SCHEMA_NAME(t.schema_id), t.name`,
  queryHints: `SELECT DISTINCT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name, o.type_desc
    FROM sys.sql_modules m JOIN sys.objects o ON m.object_id = o.object_id
    WHERE m.definition LIKE '%NOLOCK%' OR m.definition LIKE '%FORCESEEK%' OR m.definition LIKE '%FORCESCAN%'`,
  dbSettingsExtended: `SELECT 
    is_auto_create_stats_on, is_query_store_on
    FROM sys.databases WHERE name = DB_NAME()`,
  fkWithoutIndex: `SELECT fk.name AS fk_name, SCHEMA_NAME(fk.schema_id) AS schema_name,
    OBJECT_NAME(fk.parent_object_id) AS table_name,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    WHERE NOT EXISTS (
      SELECT 1 FROM sys.index_columns ic
      WHERE ic.object_id = fkc.parent_object_id AND ic.column_id = fkc.parent_column_id
    )`,
};

// ──────────────────────────────────────────────
// Analysis helpers
// ──────────────────────────────────────────────

function analyzeDataTypes(columns: SqlRow[]): string[] {
  const issues: string[] = [];
  const wideVarchars: string[] = [];
  const textDates: string[] = [];
  const textNumbers: string[] = [];

  for (const col of columns) {
    const table = `${col.TABLE_SCHEMA}.${col.TABLE_NAME}`;
    const colName = col.COLUMN_NAME as string;
    const dtype = (col.DATA_TYPE as string ?? "").toLowerCase();
    const maxLen = col.CHARACTER_MAXIMUM_LENGTH as number | null;

    // VARCHAR columns that could be narrower types
    if ((dtype === "varchar" || dtype === "nvarchar") && maxLen && maxLen > 500) {
      wideVarchars.push(`${table}.${colName} (${dtype}(${maxLen}))`);
    }

    // Potential date columns stored as text
    if ((dtype === "varchar" || dtype === "nvarchar") && colName.toLowerCase().match(/date|time|created|modified|updated/)) {
      textDates.push(`${table}.${colName}`);
    }

    // Potential numeric columns stored as text
    if ((dtype === "varchar" || dtype === "nvarchar") && colName.toLowerCase().match(/^(id|count|amount|price|qty|quantity|total|num|number)/)) {
      textNumbers.push(`${table}.${colName}`);
    }
  }

  if (wideVarchars.length > 0) {
    issues.push(
      `**⚠️ Wide VARCHAR columns (${wideVarchars.length})**: May hurt columnstore compression.\n` +
      wideVarchars.slice(0, 5).map(c => `  - ${c}`).join("\n") +
      (wideVarchars.length > 5 ? `\n  - ...and ${wideVarchars.length - 5} more` : "")
    );
  }

  if (textDates.length > 0) {
    issues.push(
      `**⚠️ Date-like columns stored as text (${textDates.length})**: Use DATE/DATETIME2 for better compression and filtering.\n` +
      textDates.slice(0, 5).map(c => `  - ${c}`).join("\n")
    );
  }

  if (textNumbers.length > 0) {
    issues.push(
      `**⚠️ Numeric-like columns stored as text (${textNumbers.length})**: Use INT/BIGINT/DECIMAL for better performance.\n` +
      textNumbers.slice(0, 5).map(c => `  - ${c}`).join("\n")
    );
  }

  return issues;
}

function analyzeStats(tables: SqlRow[], stats: SqlRow[]): string[] {
  const issues: string[] = [];

  // Count stats per table
  const statsPerTable = new Map<string, { total: number; autoCreated: number }>();
  for (const s of stats) {
    const key = `${s.schema_name}.${s.table_name}`;
    const current = statsPerTable.get(key) ?? { total: 0, autoCreated: 0 };
    if (s.stat_name) {
      current.total++;
      if (s.auto_created) current.autoCreated++;
    }
    statsPerTable.set(key, current);
  }

  const noStats = tables.filter(t => {
    const key = `${t.schema_name}.${t.table_name}`;
    const st = statsPerTable.get(key);
    return !st || st.total === 0;
  });

  if (noStats.length > 0) {
    issues.push(
      `**⚠️ Tables without statistics (${noStats.length})**: May cause suboptimal query plans.\n` +
      noStats.slice(0, 10).map(t => `  - ${t.schema_name}.${t.table_name}`).join("\n") +
      "\n  → Consider running queries on these tables to trigger auto-stats, or create manual statistics."
    );
  }

  return issues;
}

function analyzeSlowQueries(rows: SqlRow[]): string[] {
  if (rows.length === 0) return ["**✅ No slow query data found** — Warehouse may be newly created or lightly used."];

  const lines: string[] = [`**Top ${rows.length} slowest queries:**\n`];
  lines.push("| Duration (s) | Rows | Query |");
  lines.push("|-------------|------|-------|");

  for (const r of rows.slice(0, 10)) {
    const durationSec = ((r.total_elapsed_time_ms as number) / 1000).toFixed(1);
    const rowCount = r.row_count ?? "?";
    const query = (r.query_text as string ?? "").substring(0, 80).replace(/\|/g, "\\|").replace(/\n/g, " ");
    lines.push(`| ${durationSec}s | ${rowCount} | ${query}... |`);
  }

  // Flag extremely slow queries
  const verySlowCount = rows.filter(r => (r.total_elapsed_time_ms as number) > 60000).length;
  if (verySlowCount > 0) {
    lines.push(`\n**🔴 ${verySlowCount} queries took >60 seconds** — These need optimization.`);
  }

  return lines;
}

function analyzeFrequentQueries(rows: SqlRow[]): string[] {
  if (rows.length === 0) return [];

  const lines: string[] = [`**Most frequently executed queries:**\n`];
  lines.push("| Executions | Avg (s) | Max (s) | Query |");
  lines.push("|-----------|---------|---------|-------|");

  for (const r of rows.slice(0, 10)) {
    const count = r.execution_count;
    const avg = ((r.avg_duration_ms as number) / 1000).toFixed(1);
    const max = ((r.max_duration_ms as number) / 1000).toFixed(1);
    const query = (r.query_text as string ?? "").substring(0, 60).replace(/\|/g, "\\|").replace(/\n/g, " ");
    lines.push(`| ${count} | ${avg}s | ${max}s | ${query}... |`);
  }

  // Flag expensive repetitive queries
  const expensiveRepeat = rows.filter(r =>
    (r.execution_count as number) > 10 && (r.avg_duration_ms as number) > 10000
  );
  if (expensiveRepeat.length > 0) {
    lines.push(
      `\n**🔴 ${expensiveRepeat.length} queries are both frequent AND slow (>10s avg)** — ` +
      "Prime candidates for optimization or caching."
    );
  }

  return lines;
}

function analyzeFailedQueries(rows: SqlRow[]): string[] {
  if (rows.length === 0) return ["**✅ No failed queries found.**"];

  const lines: string[] = [`**⚠️ ${rows.length} recent query failures:**\n`];
  for (const r of rows.slice(0, 5)) {
    const query = (r.query_text as string ?? "").substring(0, 100).replace(/\n/g, " ");
    lines.push(`- \`${r.start_time}\`: ${query}...`);
  }

  return lines;
}

// ──────────────────────────────────────────────
// Tool: warehouse_optimization_recommendations
// ──────────────────────────────────────────────

export async function warehouseOptimizationRecommendations(args: {
  workspaceId: string;
  warehouseId: string;
}): Promise<string> {
  const warehouse = await getWarehouse(args.workspaceId, args.warehouseId);
  const rules: RuleResult[] = [];
  const header: string[] = [];

  const connectionString = warehouse.properties?.connectionString;
  if (!connectionString) {
    return renderRuleReport(
      `Warehouse Analysis: ${warehouse.displayName}`,
      new Date().toISOString(),
      ["## ❌ No SQL connection string available."],
      [{ id: "WH-001", rule: "SQL Connection", category: "Availability", severity: "HIGH", status: "ERROR", details: "No connection string available." }]
    );
  }

  header.push("## 🔌 Connection Info", "", `- **Connection**: \`${connectionString}\``, "");

  const r = await runDiagnosticQueries(connectionString, warehouse.displayName, WAREHOUSE_DIAGNOSTICS);

  // Helper: count rows or 0
  const cnt = (key: string) => ((r as Record<string, {rows?: SqlRow[]}>)[key]?.rows?.length ?? 0);
  const err = (key: string) => ((r as Record<string, {error?: string}>)[key]?.error);
  const rows = (key: string) => ((r as Record<string, {rows?: SqlRow[]}>)[key]?.rows ?? []);

  // ── Table inventory header ──
  if (r.tables?.rows) {
    const bySchema = new Map<string, string[]>();
    for (const t of r.tables.rows) {
      const list = bySchema.get(t.schema_name as string) ?? [];
      list.push(t.table_name as string);
      bySchema.set(t.schema_name as string, list);
    }
    header.push("## 📋 Tables", "");
    for (const [schema, tbls] of bySchema) {
      header.push(`**${schema}** (${tbls.length}): ${tbls.join(", ")}`, "");
    }
  }

  // ── Query Performance header ──
  if (r.slowQueries?.rows && r.slowQueries.rows.length > 0) {
    header.push("## 🐢 Top Slow Queries", "");
    header.push("| Duration (s) | Rows | Query |", "|-------------|------|-------|");
    for (const q of r.slowQueries.rows.slice(0, 10)) {
      const dur = ((q.total_elapsed_time_ms as number) / 1000).toFixed(1);
      const query = (q.query_text as string ?? "").substring(0, 80).replace(/\|/g, "\\|").replace(/\n/g, " ");
      header.push(`| ${dur}s | ${q.row_count ?? "?"} | ${query}... |`);
    }
    header.push("");
  }

  // ════════════════════════════════════════════════════
  // RULES
  // ════════════════════════════════════════════════════

  // WH-001: Missing Primary Keys
  rules.push({ id: "WH-001", rule: "Primary Keys Defined", category: "Data Quality", severity: "HIGH",
    status: err("missingPrimaryKeys") ? "ERROR" : cnt("missingPrimaryKeys") === 0 ? "PASS" : "FAIL",
    details: err("missingPrimaryKeys") ?? (cnt("missingPrimaryKeys") === 0 ? "All tables have primary keys." : `${cnt("missingPrimaryKeys")} table(s) missing PKs: ${rows("missingPrimaryKeys").slice(0,5).map(x=>x.table_name).join(", ")}`),
    recommendation: "Add PRIMARY KEY NOT ENFORCED constraints for data integrity and query optimization.",
  });

  // WH-002: Deprecated Types
  rules.push({ id: "WH-002", rule: "No Deprecated Data Types", category: "Maintainability", severity: "HIGH",
    status: err("deprecatedTypes") ? "ERROR" : cnt("deprecatedTypes") === 0 ? "PASS" : "FAIL",
    details: err("deprecatedTypes") ?? (cnt("deprecatedTypes") === 0 ? "No TEXT/NTEXT/IMAGE columns." : `${cnt("deprecatedTypes")} column(s) use deprecated types: ${rows("deprecatedTypes").slice(0,5).map(x=>`${x.table_name}.${x.column_name}(${x.data_type})`).join(", ")}`),
    recommendation: "Migrate TEXT/NTEXT/IMAGE to VARCHAR(MAX)/NVARCHAR(MAX)/VARBINARY(MAX).",
  });

  // WH-003: Floating Point
  rules.push({ id: "WH-003", rule: "No Float/Real Precision Issues", category: "Data Quality", severity: "MEDIUM",
    status: err("floatingPointColumns") ? "ERROR" : cnt("floatingPointColumns") === 0 ? "PASS" : "WARN",
    details: err("floatingPointColumns") ?? (cnt("floatingPointColumns") === 0 ? "All numeric columns use fixed precision." : `${cnt("floatingPointColumns")} float/real column(s): ${rows("floatingPointColumns").slice(0,5).map(x=>`${x.table_name}.${x.COLUMN_NAME}`).join(", ")}`),
    recommendation: "Use DECIMAL/NUMERIC for exact values (monetary, percentages).",
  });

  // WH-004: Oversized Columns
  rules.push({ id: "WH-004", rule: "No Over-Provisioned Columns", category: "Performance", severity: "MEDIUM",
    status: err("oversizedColumns") ? "ERROR" : cnt("oversizedColumns") === 0 ? "PASS" : "WARN",
    details: err("oversizedColumns") ?? (cnt("oversizedColumns") === 0 ? "All string columns have reasonable lengths." : `${cnt("oversizedColumns")} column(s) >500 chars: ${rows("oversizedColumns").slice(0,5).map(x=>`${x.table_name}.${x.COLUMN_NAME}(${x.max_length})`).join(", ")}`),
    recommendation: "Reduce column lengths for better columnstore compression.",
  });

  // WH-005: Column Naming
  rules.push({ id: "WH-005", rule: "Column Naming Convention", category: "Maintainability", severity: "LOW",
    status: err("namingIssues") ? "ERROR" : cnt("namingIssues") === 0 ? "PASS" : "WARN",
    details: err("namingIssues") ?? (cnt("namingIssues") === 0 ? "All columns follow alphanumeric naming." : `${cnt("namingIssues")} column(s) with spaces/special chars: ${rows("namingIssues").slice(0,5).map(x=>`${x.table_name}.${x.column_name}`).join(", ")}`),
    recommendation: "Use only letters, digits, and underscores.",
  });

  // WH-006: Table Naming
  rules.push({ id: "WH-006", rule: "Table Naming Convention", category: "Maintainability", severity: "LOW",
    status: err("tableNamingIssues") ? "ERROR" : cnt("tableNamingIssues") === 0 ? "PASS" : "WARN",
    details: err("tableNamingIssues") ?? (cnt("tableNamingIssues") === 0 ? "All table names follow conventions." : `${cnt("tableNamingIssues")} table(s) with invalid names: ${rows("tableNamingIssues").slice(0,5).map(x=>x.table_name).join(", ")}`),
    recommendation: "Use only letters, numbers, and underscores in table names.",
  });

  // WH-007: Views with SELECT *
  rules.push({ id: "WH-007", rule: "No SELECT * in Views", category: "Maintainability", severity: "LOW",
    status: err("viewsWithSelectStar") ? "ERROR" : cnt("viewsWithSelectStar") === 0 ? "PASS" : "WARN",
    details: err("viewsWithSelectStar") ?? (cnt("viewsWithSelectStar") === 0 ? "No views use SELECT *." : `${cnt("viewsWithSelectStar")} view(s) use SELECT *: ${rows("viewsWithSelectStar").slice(0,5).map(x=>x.view_name).join(", ")}`),
    recommendation: "Explicitly list columns in views to prevent breakage.",
  });

  // WH-008: Stale Statistics
  rules.push({ id: "WH-008", rule: "Statistics Are Fresh", category: "Performance", severity: "MEDIUM",
    status: err("staleStatistics") ? "ERROR" : cnt("staleStatistics") === 0 ? "PASS" : "FAIL",
    details: err("staleStatistics") ?? (cnt("staleStatistics") === 0 ? "All statistics updated within 30 days." : `${cnt("staleStatistics")} stale statistic(s) >30 days old: ${rows("staleStatistics").slice(0,5).map(x=>`${x.table_name}.${x.stat_name}(${x.days_old}d)`).join(", ")}`),
    recommendation: "Run UPDATE STATISTICS to refresh stale stats.",
  });

  // WH-009: Disabled Constraints
  rules.push({ id: "WH-009", rule: "No Disabled Constraints", category: "Data Quality", severity: "MEDIUM",
    status: err("constraintCheck") ? "ERROR" : cnt("constraintCheck") === 0 ? "PASS" : "WARN",
    details: err("constraintCheck") ?? (cnt("constraintCheck") === 0 ? "All foreign keys enabled and trusted." : `${cnt("constraintCheck")} disabled/untrusted constraint(s): ${rows("constraintCheck").slice(0,5).map(x=>`${x.table_name}.${x.constraint_name}`).join(", ")}`),
    recommendation: "Re-enable constraints: ALTER TABLE [t] WITH CHECK CHECK CONSTRAINT ALL.",
  });

  // WH-010: Nullable Key Columns
  rules.push({ id: "WH-010", rule: "Key Columns Are NOT NULL", category: "Data Quality", severity: "HIGH",
    status: err("nullableKeyColumns") ? "ERROR" : cnt("nullableKeyColumns") === 0 ? "PASS" : "FAIL",
    details: err("nullableKeyColumns") ?? (cnt("nullableKeyColumns") === 0 ? "All key/ID columns are NOT NULL." : `${cnt("nullableKeyColumns")} nullable key column(s): ${rows("nullableKeyColumns").slice(0,5).map(x=>`${x.table_name}.${x.COLUMN_NAME}`).join(", ")}`),
    recommendation: "Add NOT NULL constraints to ID/key columns.",
  });

  // WH-011: Empty Tables
  rules.push({ id: "WH-011", rule: "No Empty Tables", category: "Maintainability", severity: "MEDIUM",
    status: err("emptyTables") ? "ERROR" : cnt("emptyTables") === 0 ? "PASS" : "WARN",
    details: err("emptyTables") ?? (cnt("emptyTables") === 0 ? "All tables contain data." : `${cnt("emptyTables")} empty table(s): ${rows("emptyTables").slice(0,5).map(x=>x.table_name).join(", ")}`),
    recommendation: "Remove unused tables or fix data pipelines.",
  });

  // WH-012: Wide Tables
  rules.push({ id: "WH-012", rule: "No Excessively Wide Tables", category: "Maintainability", severity: "MEDIUM",
    status: err("wideTables") ? "ERROR" : cnt("wideTables") === 0 ? "PASS" : "WARN",
    details: err("wideTables") ?? (cnt("wideTables") === 0 ? "All tables have ≤50 columns." : `${cnt("wideTables")} table(s) with >50 columns: ${rows("wideTables").slice(0,5).map(x=>`${x.table_name}(${x.column_count})`).join(", ")}`),
    recommendation: "Split wide tables into related fact/dimension tables.",
  });

  // WH-013: Mixed Date Types
  rules.push({ id: "WH-013", rule: "Consistent Date Types", category: "Data Quality", severity: "LOW",
    status: err("mixedDateTypes") ? "ERROR" : cnt("mixedDateTypes") === 0 ? "PASS" : "WARN",
    details: err("mixedDateTypes") ?? (cnt("mixedDateTypes") === 0 ? "Each table uses consistent date types." : `${cnt("mixedDateTypes")} table(s) mix date types: ${rows("mixedDateTypes").slice(0,5).map(x=>`${x.table_name}(${x.date_types_used})`).join(", ")}`),
    recommendation: "Standardize on datetime2 across all tables.",
  });

  // WH-014: Missing Foreign Keys
  const totalTableCount = r.tables?.rows?.length ?? 0;
  const fkMissing = cnt("missingForeignKeys");
  rules.push({ id: "WH-014", rule: "Foreign Keys Defined", category: "Maintainability", severity: "MEDIUM",
    status: err("missingForeignKeys") ? "ERROR" : totalTableCount <= 3 ? "N/A" : fkMissing === 0 ? "PASS" : "WARN",
    details: err("missingForeignKeys") ?? (totalTableCount <= 3 ? "Too few tables to evaluate." : fkMissing === 0 ? "All tables have foreign key relationships." : `${fkMissing} of ${totalTableCount} table(s) have no FKs: ${rows("missingForeignKeys").slice(0,5).map(x=>x.table_name).join(", ")}`),
    recommendation: "Add FK constraints (NOT ENFORCED) to document relationships.",
  });

  // WH-015: BLOB Columns
  rules.push({ id: "WH-015", rule: "No Large BLOB Columns", category: "Performance", severity: "MEDIUM",
    status: err("blobColumns") ? "ERROR" : cnt("blobColumns") === 0 ? "PASS" : "WARN",
    details: err("blobColumns") ?? (cnt("blobColumns") === 0 ? "No MAX-length columns." : `${cnt("blobColumns")} MAX-length column(s): ${rows("blobColumns").slice(0,5).map(x=>`${x.table_name}.${x.column_name}(${x.data_type})`).join(", ")}`),
    recommendation: "Use OneLake Files for large unstructured data instead of warehouse columns.",
  });

  // WH-016: Missing Audit Columns
  rules.push({ id: "WH-016", rule: "Tables Have Audit Columns", category: "Maintainability", severity: "LOW",
    status: err("missingAuditColumns") ? "ERROR" : cnt("missingAuditColumns") === 0 ? "PASS" : "WARN",
    details: err("missingAuditColumns") ?? (cnt("missingAuditColumns") === 0 ? "All tables have created_at/updated_at." : `${cnt("missingAuditColumns")} table(s) lack audit columns: ${rows("missingAuditColumns").slice(0,5).map(x=>x.table_name).join(", ")}`),
    recommendation: "Add created_at, updated_at, created_by columns for tracking.",
  });

  // WH-017: Circular Foreign Keys
  rules.push({ id: "WH-017", rule: "No Circular Foreign Keys", category: "Data Quality", severity: "HIGH",
    status: err("circularForeignKeys") ? "ERROR" : cnt("circularForeignKeys") === 0 ? "PASS" : "FAIL",
    details: err("circularForeignKeys") ?? (cnt("circularForeignKeys") === 0 ? "No circular FK relationships." : `${cnt("circularForeignKeys")} circular FK(s): ${rows("circularForeignKeys").slice(0,5).map(x=>`${x.table1} ↔ ${x.table2}`).join(", ")}`),
    recommendation: "Refactor to eliminate circular references.",
  });

  // WH-018: Sensitive Columns without Masking
  const maskedSet = new Set(rows("dataMaskingCheck").map(x => `${x.table_name}.${x.column_name}`));
  const unmaskedSensitive = rows("sensitiveColumns").filter(x => !maskedSet.has(`${x.table_name}.${x.column_name}`));
  rules.push({ id: "WH-018", rule: "Sensitive Data Protected", category: "Security", severity: "HIGH",
    status: err("sensitiveColumns") ? "ERROR" : unmaskedSensitive.length === 0 ? "PASS" : "FAIL",
    details: err("sensitiveColumns") ?? (unmaskedSensitive.length === 0 ? "All sensitive columns are masked or none detected." : `${unmaskedSensitive.length} sensitive column(s) without data masking.`),
    recommendation: "Apply dynamic data masking to PII columns.",
  });

  // WH-019: RLS
  const rlsPolicies = rows("rlsCheck");
  rules.push({ id: "WH-019", rule: "Row-Level Security", category: "Security", severity: "MEDIUM",
    status: err("rlsCheck") ? "ERROR" : rlsPolicies.length > 0 ? "PASS" : "WARN",
    details: err("rlsCheck") ?? (rlsPolicies.length > 0 ? `${rlsPolicies.length} RLS policies defined.` : "No RLS policies — consider adding if data requires row-level isolation."),
    recommendation: "Add RLS security policies for multi-tenant or sensitive data scenarios.",
  });

  // WH-020: db_owner Members
  const ownerCount = cnt("dbOwnerMembers");
  rules.push({ id: "WH-020", rule: "Minimal db_owner Privileges", category: "Security", severity: "MEDIUM",
    status: err("dbOwnerMembers") ? "ERROR" : ownerCount <= 3 ? "PASS" : "WARN",
    details: err("dbOwnerMembers") ?? (ownerCount <= 3 ? `${ownerCount} db_owner member(s) — acceptable.` : `${ownerCount} db_owner members: ${rows("dbOwnerMembers").map(x=>x.member_name).join(", ")}`),
    recommendation: "Reduce db_owner membership to minimize security risk.",
  });

  // WH-021: View Dependencies
  rules.push({ id: "WH-021", rule: "No Over-Complex Views", category: "Maintainability", severity: "LOW",
    status: err("viewDependencies") ? "ERROR" : cnt("viewDependencies") === 0 ? "PASS" : "WARN",
    details: err("viewDependencies") ?? (cnt("viewDependencies") === 0 ? "No views with >10 dependencies." : `${cnt("viewDependencies")} over-complex view(s): ${rows("viewDependencies").slice(0,5).map(x=>`${x.view_name}(${x.dependency_count} deps)`).join(", ")}`),
    recommendation: "Simplify view chains to at most 3 levels of nesting.",
  });

  // WH-022: Cross-Schema Dependencies
  rules.push({ id: "WH-022", rule: "Minimal Cross-Schema Dependencies", category: "Maintainability", severity: "LOW",
    status: err("crossSchemaDeps") ? "ERROR" : cnt("crossSchemaDeps") === 0 ? "PASS" : "WARN",
    details: err("crossSchemaDeps") ?? (cnt("crossSchemaDeps") === 0 ? "No cross-schema references." : `${cnt("crossSchemaDeps")} cross-schema reference(s): ${rows("crossSchemaDeps").slice(0,5).map(x=>`${x.referencing_object} → ${x.referenced_object}`).join(", ")}`),
    recommendation: "Minimize cross-schema dependencies for cleaner architecture.",
  });

  // WH-023: Slow Queries (>60s)
  const verySlowCount = (r.slowQueries?.rows ?? []).filter(q => (q.total_elapsed_time_ms as number) > 60000).length;
  rules.push({ id: "WH-023", rule: "No Very Slow Queries (>60s)", category: "Performance", severity: "HIGH",
    status: err("slowQueries") ? "ERROR" : verySlowCount === 0 ? "PASS" : "FAIL",
    details: err("slowQueries") ?? (verySlowCount === 0 ? "No queries exceeding 60 seconds." : `${verySlowCount} query/queries took >60 seconds.`),
    recommendation: "Review and optimize slow queries — see Slow Queries table above.",
  });

  // WH-024: Frequent Slow Queries
  const expensiveRepeat = (r.frequentQueries?.rows ?? []).filter(q => (q.execution_count as number) > 10 && (q.avg_duration_ms as number) > 10000);
  rules.push({ id: "WH-024", rule: "No Frequently Slow Queries", category: "Performance", severity: "HIGH",
    status: err("frequentQueries") ? "ERROR" : expensiveRepeat.length === 0 ? "PASS" : "FAIL",
    details: err("frequentQueries") ?? (expensiveRepeat.length === 0 ? "No recurring slow queries." : `${expensiveRepeat.length} queries are both frequent (>10x) AND slow (>10s avg).`),
    recommendation: "Cache results or optimize these high-impact queries.",
  });

  // WH-025: Failed Queries with error categorization
  const failedRows = rows("failedQueries");
  let failedDetails = "";
  if (err("failedQueries")) {
    failedDetails = err("failedQueries")!;
  } else if (failedRows.length === 0) {
    failedDetails = "No recent query failures.";
  } else {
    // Categorize failures by status/error pattern
    const categories = new Map<string, number>();
    for (const r of failedRows) {
      const text = ((r.query_text as string) ?? "").substring(0, 50);
      const cat = text.match(/timeout/i) ? "Timeout" :
        text.match(/permission|denied|unauthorized/i) ? "Permission" :
        text.match(/syntax|parse/i) ? "Syntax" :
        text.match(/deadlock/i) ? "Deadlock" : "Other";
      categories.set(cat, (categories.get(cat) ?? 0) + 1);
    }
    const breakdown = [...categories.entries()].map(([k, v]) => `${k}: ${v}`).join(", ");
    failedDetails = `${failedRows.length} failure(s) — ${breakdown}`;
  }
  rules.push({ id: "WH-025", rule: "No Recent Query Failures", category: "Reliability", severity: "MEDIUM",
    status: err("failedQueries") ? "ERROR" : failedRows.length === 0 ? "PASS" : "WARN",
    details: failedDetails,
    recommendation: "Investigate failed queries grouped by error type.",
  });

  // WH-026: Database Settings
  if (r.dbSettings?.rows?.[0]) {
    const db = r.dbSettings.rows[0];

    rules.push({ id: "WH-026", rule: "AUTO_UPDATE_STATISTICS Enabled", category: "Performance", severity: "HIGH",
      status: db.is_auto_update_stats_on ? "PASS" : "FAIL",
      details: db.is_auto_update_stats_on ? "Auto-update statistics is enabled." : "AUTO_UPDATE_STATISTICS is OFF — stale stats cause bad query plans.",
      recommendation: "ALTER DATABASE SET AUTO_UPDATE_STATISTICS ON.",
    });

    rules.push({ id: "WH-027", rule: "Result Set Caching Enabled", category: "Performance", severity: "MEDIUM",
      status: db.is_result_set_caching_on ? "PASS" : "WARN",
      details: db.is_result_set_caching_on ? "Result set caching is enabled." : "Result set caching is OFF.",
      recommendation: "ALTER DATABASE SET RESULT_SET_CACHING ON.",
    });

    rules.push({ id: "WH-028", rule: "Snapshot Isolation Enabled", category: "Concurrency", severity: "MEDIUM",
      status: db.snapshot_isolation_state ? "PASS" : "WARN",
      details: db.snapshot_isolation_state ? "Snapshot isolation enabled — readers don't block writers." : "Snapshot isolation OFF — may cause blocking.",
      recommendation: "ALTER DATABASE SET ALLOW_SNAPSHOT_ISOLATION ON.",
    });

    rules.push({ id: "WH-029", rule: "Page Verify CHECKSUM", category: "Reliability", severity: "MEDIUM",
      status: db.page_verify_option_desc === "CHECKSUM" ? "PASS" : "WARN",
      details: `PAGE_VERIFY is ${db.page_verify_option_desc}.`,
      recommendation: "ALTER DATABASE SET PAGE_VERIFY CHECKSUM for I/O corruption detection.",
    });

    const ansiFlags = [db.is_ansi_nulls_on, db.is_ansi_padding_on, db.is_ansi_warnings_on, db.is_arithabort_on, db.is_quoted_identifier_on];
    const ansiOff = ansiFlags.filter(f => !f).length;
    rules.push({ id: "WH-030", rule: "ANSI Settings Correct", category: "Standards", severity: "LOW",
      status: ansiOff === 0 ? "PASS" : "WARN",
      details: ansiOff === 0 ? "All ANSI settings are ON." : `${ansiOff} ANSI setting(s) are OFF.`,
      recommendation: "Enable all ANSI settings for predictable behavior.",
    });

    rules.push({ id: "WH-031", rule: "Database ONLINE", category: "Availability", severity: "HIGH",
      status: db.state_desc === "ONLINE" ? "PASS" : "FAIL",
      details: `Database state: ${db.state_desc}.`,
      recommendation: "Ensure database is ONLINE.",
    });
  } else {
    rules.push({ id: "WH-026", rule: "Database Settings Check", category: "Performance", severity: "MEDIUM",
      status: "ERROR", details: `Could not read database settings: ${err("dbSettings") ?? "unknown"}.`,
    });
  }

  // WH-032: Statistics Coverage
  if (r.stats?.rows && r.tables?.rows) {
    const noStatsTables = r.tables.rows.filter(t => {
      const key = `${t.schema_name}.${t.table_name}`;
      return !r.stats!.rows!.some(s => `${s.schema_name}.${s.table_name}` === key && s.stat_name);
    });
    rules.push({ id: "WH-032", rule: "All Tables Have Statistics", category: "Performance", severity: "MEDIUM",
      status: noStatsTables.length === 0 ? "PASS" : "WARN",
      details: noStatsTables.length === 0 ? "All tables have statistics." : `${noStatsTables.length} table(s) without statistics.`,
      recommendation: "Query these tables to trigger auto-stats creation.",
    });
  }

  // WH-033: Data Type Issues (wide varchar, text dates/numbers)
  if (r.columns?.rows) {
    const dtIssues = analyzeDataTypes(r.columns.rows);
    rules.push({ id: "WH-033", rule: "Optimal Data Types", category: "Performance", severity: "MEDIUM",
      status: dtIssues.length === 0 ? "PASS" : "WARN",
      details: dtIssues.length === 0 ? "No data type issues detected." : `${dtIssues.length} data type issue(s) found.`,
      recommendation: "Fix wide varchar, text dates, and text numeric columns.",
    });
  }

  // WH-034: Low Row Count Tables
  const lowRowTables = rows("lowRowCountTables");
  rules.push({ id: "WH-034", rule: "No Near-Empty Tables", category: "Maintainability", severity: "LOW",
    status: err("lowRowCountTables") ? "ERROR" : lowRowTables.length === 0 ? "PASS" : "WARN",
    details: err("lowRowCountTables") ?? (lowRowTables.length === 0
      ? "No tables with <10 rows."
      : `${lowRowTables.length} table(s) with <10 rows: ${lowRowTables.slice(0, 5).map(x => `${x.table_name}(${x.row_count})`).join(", ")}`),
    recommendation: "Tables with very few rows may be test/staging tables. Remove if unused.",
  });

  // WH-035: Stored Procedures Documentation
  const procs = rows("storedProcedures");
  const undocProcs = procs.filter(p => !p.has_description);
  rules.push({ id: "WH-035", rule: "Stored Procedures Documented", category: "Maintainability", severity: "LOW",
    status: err("storedProcedures") ? "ERROR" : procs.length === 0 ? "N/A" : undocProcs.length === 0 ? "PASS" : "WARN",
    details: err("storedProcedures") ?? (procs.length === 0
      ? "No stored procedures."
      : undocProcs.length === 0
        ? `All ${procs.length} procedure(s) documented.`
        : `${undocProcs.length} procedure(s) undocumented: ${undocProcs.slice(0, 5).map(x => x.proc_name).join(", ")}`),
    recommendation: "Add MS_Description extended properties to stored procedures.",
  });

  // WH-036: NOT NULL columns without defaults
  const noDefaults = rows("missingDefaults");
  rules.push({ id: "WH-036", rule: "NOT NULL Columns Have Defaults", category: "Data Quality", severity: "MEDIUM",
    status: err("missingDefaults") ? "ERROR" : noDefaults.length === 0 ? "PASS" : "WARN",
    details: err("missingDefaults") ?? (noDefaults.length === 0
      ? "All NOT NULL columns have DEFAULT constraints."
      : `${noDefaults.length} NOT NULL column(s) without defaults: ${noDefaults.slice(0, 5).map(x => `${x.table_name}.${x.column_name}`).join(", ")}`),
    recommendation: "Add DEFAULT constraints to NOT NULL columns to prevent insert failures.",
  });

  // WH-037: Unicode/Non-Unicode Mix
  const unicodeMixed = rows("unicodeMix");
  rules.push({ id: "WH-037", rule: "Consistent String Types", category: "Maintainability", severity: "LOW",
    status: err("unicodeMix") ? "ERROR" : unicodeMixed.length === 0 ? "PASS" : "WARN",
    details: err("unicodeMix") ?? (unicodeMixed.length === 0
      ? "All tables use consistent string types."
      : `${unicodeMixed.length} table(s) mix varchar/nvarchar: ${unicodeMixed.slice(0, 5).map(x => `${x.table_name}(${x.unicode_count}n + ${x.non_unicode_count}v)`).join(", ")}`),
    recommendation: "Standardize on nvarchar (Unicode) or varchar (non-Unicode) within each table.",
  });

  // WH-038: Schema Documentation
  const schemas = rows("schemaDocumentation");
  const undocSchemas = schemas.filter(s => !s.has_description);
  rules.push({ id: "WH-038", rule: "Schemas Are Documented", category: "Maintainability", severity: "LOW",
    status: err("schemaDocumentation") ? "ERROR" : schemas.length === 0 ? "N/A" : undocSchemas.length === 0 ? "PASS" : "WARN",
    details: err("schemaDocumentation") ?? (schemas.length === 0
      ? "No user schemas."
      : undocSchemas.length === 0
        ? `All ${schemas.length} schema(s) documented.`
        : `${undocSchemas.length} schema(s) undocumented: ${undocSchemas.slice(0, 5).map(x => x.schema_name).join(", ")}`),
    recommendation: "Add MS_Description extended properties to schemas for documentation.",
  });

  // WH-039: Query Performance Average
  const qvAvg = rows("queryVolumeAvg");
  if (qvAvg.length > 0 && qvAvg[0].avg_duration_ms != null) {
    const avgMs = qvAvg[0].avg_duration_ms as number;
    const totalQueries = qvAvg[0].total_queries as number;
    rules.push({ id: "WH-039", rule: "Query Performance Healthy", category: "Performance", severity: "MEDIUM",
      status: avgMs < 5000 ? "PASS" : avgMs < 30000 ? "WARN" : "FAIL",
      details: `Average query duration: ${(avgMs / 1000).toFixed(1)}s over ${totalQueries} queries (last 7 days).`,
      recommendation: avgMs >= 5000 ? "Investigate slow query patterns — average exceeds 5s." : undefined,
    });
  }

    // WH-040: AUTO_CREATE_STATISTICS enabled
    const autoCreateStats = rows("dbSettingsExtended");
    const autoCreateEnabled = autoCreateStats.length > 0 && autoCreateStats[0].is_auto_create_stats_on === true;
    rules.push({
      id: "WH-040", rule: "AUTO_CREATE_STATISTICS Enabled", category: "Performance", severity: "HIGH",
      status: err("dbSettingsExtended") ? "ERROR" : autoCreateEnabled ? "PASS" : "FAIL",
      details: err("dbSettingsExtended") ?? (autoCreateEnabled ? "Auto-create statistics is enabled." : "Auto-create statistics is disabled."),
      recommendation: "Enable with: ALTER DATABASE SET AUTO_CREATE_STATISTICS ON",
    });

    // WH-041: QUERY_STORE enabled
    const queryStoreOn = autoCreateStats.length > 0 && autoCreateStats[0].is_query_store_on === true;
    rules.push({
      id: "WH-041", rule: "Query Store Enabled", category: "Performance", severity: "MEDIUM",
      status: err("dbSettingsExtended") ? "ERROR" : queryStoreOn ? "PASS" : "WARN",
      details: err("dbSettingsExtended") ?? (queryStoreOn ? "Query Store is enabled for performance monitoring." : "Query Store is not enabled."),
      recommendation: "Enable with: ALTER DATABASE SET QUERY_STORE = ON",
    });

    // WH-042: Excessive computed columns
    const computedCols = rows("computedColumns");
    const allColCounts = rows("allColumns");
    const tablesWithExcessiveComputed: string[] = [];
    for (const tc of allColCounts) {
      const tbl = `${tc.schema_name}.${tc.table_name}`;
      const totalCols = tc.col_count as number;
      const compCount = computedCols.filter(c => `${c.schema_name}.${c.table_name}` === tbl).length;
      if (totalCols > 0 && compCount / totalCols > 0.3) {
        tablesWithExcessiveComputed.push(`${tbl} (${compCount}/${totalCols})`);
      }
    }
    rules.push({
      id: "WH-042", rule: "No Excessive Computed Columns", category: "Maintainability", severity: "LOW",
      status: err("computedColumns") ? "ERROR" : tablesWithExcessiveComputed.length === 0 ? "PASS" : "WARN",
      details: err("computedColumns") ?? (tablesWithExcessiveComputed.length === 0 ? "No tables with >30% computed columns." : `${tablesWithExcessiveComputed.length} table(s): ${tablesWithExcessiveComputed.slice(0, 3).join(", ")}`),
      recommendation: "Review computed columns — consider materializing in source or using views.",
    });

    // WH-043: Query hints audit
    const hintObjects = rows("queryHints");
    rules.push({
      id: "WH-043", rule: "No Forced Query Hints", category: "Performance", severity: "LOW",
      status: err("queryHints") ? "ERROR" : hintObjects.length === 0 ? "PASS" : "WARN",
      details: err("queryHints") ?? (hintObjects.length === 0 ? "No objects using query hints." : `${hintObjects.length} object(s) using hints: ${hintObjects.slice(0, 3).map(h => h.object_name).join(", ")}`),
      recommendation: "Review NOLOCK/FORCESEEK hints — they may mask optimizer issues.",
    });

    // WH-044: Missing indexes on FK columns
    const missingFkIdx = rows("fkWithoutIndex");
    rules.push({
      id: "WH-044", rule: "FK Columns Have Indexes", category: "Performance", severity: "MEDIUM",
      status: err("fkWithoutIndex") ? "ERROR" : missingFkIdx.length === 0 ? "PASS" : "WARN",
      details: err("fkWithoutIndex") ?? (missingFkIdx.length === 0 ? "All FK columns have supporting indexes." : `${missingFkIdx.length} FK column(s) missing indexes: ${missingFkIdx.slice(0, 3).map(f => `${f.table_name}.${f.column_name}`).join(", ")}`),
      recommendation: "Create indexes on FK columns for better join performance.",
    });

  return renderRuleReport(
    `Warehouse Analysis: ${warehouse.displayName}`,
    new Date().toISOString(),
    header,
    rules
  );
}

// ──────────────────────────────────────────────
// Tool: warehouse_analyze_query_patterns
// ──────────────────────────────────────────────

export async function warehouseAnalyzeQueryPatterns(args: {
  workspaceId: string;
  warehouseId: string;
}): Promise<string> {
  const warehouse = await getWarehouse(args.workspaceId, args.warehouseId);

  const connectionString = warehouse.properties?.connectionString;
  if (!connectionString) {
    return `Warehouse "${warehouse.displayName}" has no SQL connection string available. Cannot analyze queries.`;
  }

  const results = await runDiagnosticQueries(connectionString, warehouse.displayName, {
    slowQueries: WAREHOUSE_DIAGNOSTICS.slowQueries,
    frequentQueries: WAREHOUSE_DIAGNOSTICS.frequentQueries,
    failedQueries: WAREHOUSE_DIAGNOSTICS.failedQueries,
    queryVolume: WAREHOUSE_DIAGNOSTICS.queryVolume,
  });

  const report: string[] = [
    `# 📊 Query Pattern Analysis: ${warehouse.displayName}`,
    "",
    `_Live analysis at ${new Date().toISOString()}_`,
    "",
  ];

  if (results.slowQueries?.rows) {
    report.push("## 🐢 Slowest Queries", "");
    report.push(...analyzeSlowQueries(results.slowQueries.rows), "");
  }

  if (results.frequentQueries?.rows) {
    report.push("## 🔄 Most Frequent Queries", "");
    report.push(...analyzeFrequentQueries(results.frequentQueries.rows), "");
  }

  if (results.failedQueries?.rows) {
    report.push("## ❌ Recent Failures", "");
    report.push(...analyzeFailedQueries(results.failedQueries.rows), "");
  }

  if (results.queryVolume?.rows && results.queryVolume.rows.length > 0) {
    report.push("## 📈 Daily Volume", "");
    report.push("| Date | Queries | Avg Duration |");
    report.push("|------|---------|-------------|");
    for (const r of results.queryVolume.rows.slice(0, 14)) {
      const avg = ((r.avg_duration_ms as number) / 1000).toFixed(1);
      report.push(`| ${r.query_date} | ${r.query_count} | ${avg}s |`);
    }
    report.push("");
  }

  return report.join("\n");
}

// ──────────────────────────────────────────────
// Tool: warehouse_fix — Auto-fix detected issues
// ──────────────────────────────────────────────

const WAREHOUSE_FIXES: Record<string, {
  description: string;
  getSql: (args: Record<string, unknown>, diagnostics: Record<string, { rows?: SqlRow[] }>) => string[];
}> = {
  "WH-001": {
    description: "Add PRIMARY KEY NOT ENFORCED constraints to tables missing PKs",
    getSql: (_args, diag) => {
      const tables = diag.missingPrimaryKeys?.rows ?? [];
      return tables.map(t => {
        const tbl = quoteSqlId(t.table_name as string);
        const tblSafe = (t.table_name as string).replace(/[^a-zA-Z0-9_]/g, "_");
        // Find first *Id or *_id column as PK candidate
        const cols = diag.nullableKeyColumns?.rows?.filter(c =>
          (c.table_name as string) === (t.table_name as string)
        ) ?? [];
        const pkCol = cols.length > 0 ? `[${cols[0].COLUMN_NAME as string}]` : "[id]";
        return `ALTER TABLE ${tbl} ADD CONSTRAINT [PK_${tblSafe}] PRIMARY KEY NONCLUSTERED (${pkCol}) NOT ENFORCED`;
      });
    },
  },
  "WH-008": {
    description: "Refresh stale statistics (>30 days old)",
    getSql: (_args, diag) => {
      const stale = diag.staleStatistics?.rows ?? [];
      const tables = [...new Set(stale.map(s => s.table_name as string))];
      return tables.map(t => `UPDATE STATISTICS ${quoteSqlId(t)}`);
    },
  },
  "WH-009": {
    description: "Re-enable disabled/untrusted constraints",
    getSql: (_args, diag) => {
      const constraints = diag.constraintCheck?.rows ?? [];
      return constraints.map(c => `ALTER TABLE ${quoteSqlId(c.table_name as string)} WITH CHECK CHECK CONSTRAINT [${c.constraint_name}]`);
    },
  },
  "WH-016": {
    description: "Add audit columns (created_at, updated_at) to tables",
    getSql: (_args, diag) => {
      const tables = diag.missingAuditColumns?.rows ?? [];
      return tables.map(t =>
        `ALTER TABLE ${quoteSqlId(t.table_name as string)} ADD [created_at] DATETIME2 NULL DEFAULT GETDATE(), [updated_at] DATETIME2 NULL DEFAULT GETDATE()`
      );
    },
  },
  "WH-018": {
    description: "Apply dynamic data masking to sensitive/PII columns",
    getSql: (_args, diag) => {
      const sensitive = diag.sensitiveColumns?.rows ?? [];
      const masked = new Set((diag.dataMaskingCheck?.rows ?? []).map(r => `${r.table_name}.${r.column_name}`));
      return sensitive
        .filter(c => !masked.has(`${c.table_name}.${c.column_name}`))
        .map(c => {
          const col = (c.column_name ?? c.COLUMN_NAME) as string;
          const colLower = col.toLowerCase();
          const fn = colLower.includes("email") ? "email()" :
            colLower.includes("phone") || colLower.includes("mobile") ? "partial(0,\"XXX-XXX-\",4)" :
              "default()";
          return `ALTER TABLE ${quoteSqlId(c.table_name as string)} ALTER COLUMN [${col}] ADD MASKED WITH (FUNCTION = '${fn}')`;
        });
    },
  },
  "WH-026": {
    description: "Enable AUTO_UPDATE_STATISTICS",
    getSql: (args) => [`ALTER DATABASE [${args.warehouseName ?? "current"}] SET AUTO_UPDATE_STATISTICS ON`],
  },
  "WH-027": {
    description: "Enable result set caching",
    getSql: (args) => [`ALTER DATABASE [${args.warehouseName ?? "current"}] SET RESULT_SET_CACHING ON`],
  },
  "WH-028": {
    description: "Enable snapshot isolation",
    getSql: (args) => [`ALTER DATABASE [${args.warehouseName ?? "current"}] SET ALLOW_SNAPSHOT_ISOLATION ON`],
  },
  "WH-029": {
    description: "Set PAGE_VERIFY to CHECKSUM",
    getSql: (args) => [`ALTER DATABASE [${args.warehouseName ?? "current"}] SET PAGE_VERIFY CHECKSUM`],
  },
  "WH-030": {
    description: "Enable all ANSI settings",
    getSql: (args) => {
      const db = args.warehouseName ?? "current";
      return [
        `ALTER DATABASE [${db}] SET ANSI_NULLS ON`,
        `ALTER DATABASE [${db}] SET ANSI_PADDING ON`,
        `ALTER DATABASE [${db}] SET ANSI_WARNINGS ON`,
        `ALTER DATABASE [${db}] SET ARITHABORT ON`,
        `ALTER DATABASE [${db}] SET QUOTED_IDENTIFIER ON`,
      ];
    },
  },
  "WH-032": {
    description: "Create statistics on tables without any",
    getSql: (_args, diag) => {
      const tables = diag.tables?.rows ?? [];
      const stats = diag.stats?.rows ?? [];
      const noStats = tables.filter(t => {
        const key = `${t.schema_name}.${t.table_name}`;
        return !stats.some(s => `${s.schema_name}.${s.table_name}` === key && s.stat_name);
      });
      return noStats.map(t => `UPDATE STATISTICS [${t.schema_name}].[${t.table_name}]`);
    },
  },
  "WH-036": {
    description: "Add DEFAULT constraints to NOT NULL columns without them",
    getSql: (_args, diag) => {
      const missing = diag.missingDefaults?.rows ?? [];
      return missing.slice(0, 20).map(r =>
        `ALTER TABLE ${quoteSqlId(r.table_name as string)} ADD DEFAULT '' FOR [${r.column_name}]`
      );
    },
  },
  "WH-040": {
    description: "Enable AUTO_CREATE_STATISTICS",
    getSql: (args) => [`ALTER DATABASE [${args.warehouseName ?? "current"}] SET AUTO_CREATE_STATISTICS ON`],
  },
  "WH-041": {
    description: "Enable Query Store",
    getSql: (args) => [`ALTER DATABASE [${args.warehouseName ?? "current"}] SET QUERY_STORE = ON`],
  },
  "WH-044": {
    description: "Create indexes on FK columns missing them",
    getSql: (_args, diag) => {
      const missing = diag.fkWithoutIndex?.rows ?? [];
      return missing.slice(0, 10).map(r => {
        const schema = r.schema_name as string;
        const table = r.table_name as string;
        const col = r.column_name as string;
        const safeName = `${schema}_${table}_${col}`.replace(/[^a-zA-Z0-9_]/g, "_");
        return `CREATE NONCLUSTERED INDEX [IX_FK_${safeName}] ON [${schema}].[${table}] ([${col}])`;
      });
    },
  },
};

export async function warehouseFix(args: {
  workspaceId: string;
  warehouseId: string;
  ruleIds?: string[];
  dryRun?: boolean;
}): Promise<string> {
  const warehouse = await getWarehouse(args.workspaceId, args.warehouseId);
  const connectionString = warehouse.properties?.connectionString;

  if (!connectionString) {
    return "❌ No SQL connection string available. Cannot apply fixes.";
  }

  // Input validation
  validateSqlName(warehouse.displayName, "warehouse name");

  const isDryRun = args.dryRun ?? false;

  // Run diagnostics to get current state
  const DIAG_QUERIES: Record<string, string> = {};
  // Only include queries needed by the fixes
  const neededQueries = ["missingPrimaryKeys", "nullableKeyColumns", "staleStatistics", "constraintCheck",
    "missingAuditColumns", "sensitiveColumns", "dataMaskingCheck", "tables", "stats", "missingDefaults", "dbSettings",
    "fkWithoutIndex", "dbSettingsExtended"];
  for (const key of neededQueries) {
    const allDiag = WAREHOUSE_DIAGNOSTICS as Record<string, string>;
    if (allDiag[key]) DIAG_QUERIES[key] = allDiag[key];
  }

  const diagnostics = await runDiagnosticQueries(connectionString, warehouse.displayName, DIAG_QUERIES);

  // Determine which rules to fix
  const fixableRuleIds = Object.keys(WAREHOUSE_FIXES);
  const ruleIds = args.ruleIds && args.ruleIds.length > 0
    ? args.ruleIds.filter(id => fixableRuleIds.includes(id))
    : fixableRuleIds;

  const results: string[] = [];
  let totalFixed = 0;
  let totalFailed = 0;
  let totalSkipped = 0;

  for (const ruleId of ruleIds) {
    const fix = WAREHOUSE_FIXES[ruleId];
    if (!fix) continue;

    const sqls = fix.getSql(
      { warehouseName: warehouse.displayName },
      diagnostics as Record<string, { rows?: SqlRow[] }>
    );

    if (sqls.length === 0) {
      results.push(`| ${ruleId} | ⚪ | No action needed | — |`);
      totalSkipped++;
      continue;
    }

    for (const sql of sqls) {
      if (isDryRun) {
        results.push(`| ${ruleId} | 🔍 | ${fix.description} | \`${sql.substring(0, 80)}...\` |`);
        totalSkipped++;
      } else {
        try {
          await executeSqlQuery(connectionString, warehouse.displayName, sql);
          results.push(`| ${ruleId} | ✅ | ${fix.description} | \`${sql.substring(0, 80)}...\` |`);
          totalFixed++;
        } catch (error) {
          const msg = error instanceof Error ? error.message : String(error);
          results.push(`| ${ruleId} | ❌ | Failed: ${msg.substring(0, 80)} | \`${sql.substring(0, 60)}...\` |`);
          totalFailed++;
        }
      }
    }
  }

  const mode = isDryRun ? "DRY RUN (preview only)" : "Applying fixes";
  return [
    `# 🔧 Warehouse Fix: ${warehouse.displayName}`,
    "",
    `_${mode} at ${new Date().toISOString()}_`,
    "",
    isDryRun
      ? `**${totalSkipped} command(s) previewed** — re-run without dryRun to apply.`
      : `**${totalFixed} fixed, ${totalFailed} failed${totalSkipped > 0 ? `, ${totalSkipped} skipped` : ""}**`,
    "",
    "| Rule | Status | Action | SQL |",
    "|------|--------|--------|-----|",
    ...results,
    "",
    isDryRun ? "> 💡 Set `dryRun: false` to execute these commands." : "",
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool: warehouse_auto_optimize — Scan + fix all issues
// ──────────────────────────────────────────────

export async function warehouseAutoOptimize(args: {
  workspaceId: string;
  warehouseId: string;
  dryRun?: boolean;
}): Promise<string> {
  // Run the existing fix handler with all rules — it already runs diagnostics internally
  return warehouseFix({
    workspaceId: args.workspaceId,
    warehouseId: args.warehouseId,
    ruleIds: undefined, // all rules
    dryRun: args.dryRun,
  });
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const warehouseTools = [
  {
    name: "warehouse_list",
    description:
      "List all warehouses in a Fabric workspace with their metadata and connection details.",
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
    handler: warehouseList,
  },
  {
    name: "warehouse_optimization_recommendations",
    description:
      "LIVE SCAN: Connects to a Fabric Warehouse SQL endpoint and runs real diagnostic queries. " +
      "Analyzes table schemas, data types, statistics coverage, slow queries, frequent queries, " +
      "failed queries, and query volume trends. Returns findings with prioritized action items.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        warehouseId: {
          type: "string",
          description: "The ID of the warehouse to analyze",
        },
      },
      required: ["workspaceId", "warehouseId"],
    },
    handler: warehouseOptimizationRecommendations,
  },
  {
    name: "warehouse_analyze_query_patterns",
    description:
      "LIVE SCAN: Connects to a Fabric Warehouse SQL endpoint and analyzes real query execution " +
      "history. Returns top slow queries, most frequent queries, recent failures, and daily " +
      "query volume trends with actual data.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        warehouseId: {
          type: "string",
          description: "The ID of the warehouse",
        },
      },
      required: ["workspaceId", "warehouseId"],
    },
    handler: warehouseAnalyzeQueryPatterns,
  },
  {
    name: "warehouse_fix",
    description:
      "AUTO-FIX: Connects to a Fabric Warehouse and applies fixes for detected issues. " +
      "Can fix: stale statistics, missing PKs, disabled constraints, missing audit columns, " +
      "sensitive data masking, database settings (AUTO_UPDATE_STATISTICS, AUTO_CREATE_STATISTICS, result set caching, " +
      "snapshot isolation, ANSI settings), Query Store, and missing FK indexes. " +
      "Fixable rule IDs: WH-001, WH-008, WH-026, WH-027, WH-028, WH-029, WH-030, WH-032, WH-036, WH-040, WH-041, WH-044. " +
      "Specify ruleIds to fix specific issues, or omit to fix all auto-fixable issues. " +
      "Use dryRun=true to preview SQL commands without executing them.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        warehouseId: {
          type: "string",
          description: "The ID of the warehouse to fix",
        },
        ruleIds: {
          type: "array",
          items: { type: "string" },
          description: "Optional: specific rule IDs to fix (e.g. ['WH-008', 'WH-026']). If omitted, all auto-fixable rules are applied.",
        },
        dryRun: {
          type: "boolean",
          description: "If true, preview SQL commands without executing them (default: false)",
        },
      },
      required: ["workspaceId", "warehouseId"],
    },
    handler: warehouseFix,
  },
  {
    name: "warehouse_auto_optimize",
    description:
      "AUTO-OPTIMIZE: Scans a Fabric Warehouse for all fixable issues and applies all safe fixes automatically. " +
      "Runs diagnostics first, then applies: stale statistics refresh, PK constraints, ANSI settings, " +
      "result set caching, snapshot isolation, AUTO_CREATE_STATISTICS, Query Store, FK indexes, and more. " +
      "Fixable rule IDs: WH-001, WH-008, WH-026, WH-027, WH-028, WH-029, WH-030, WH-032, WH-036, WH-040, WH-041, WH-044. " +
      "Use dryRun=true to preview.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: { type: "string", description: "The ID of the Fabric workspace" },
        warehouseId: { type: "string", description: "The ID of the warehouse to optimize" },
        dryRun: {
          type: "boolean",
          description: "If true, preview SQL commands without executing (default: false)",
        },
      },
      required: ["workspaceId", "warehouseId"],
    },
    handler: warehouseAutoOptimize,
  },
];
