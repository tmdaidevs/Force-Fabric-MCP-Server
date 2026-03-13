import {
  listSemanticModels,
  executeSemanticModelDaxQuery,
  executeSemanticModelQuery,
  getWorkspace,
  getSemanticModelDefinition,
  updateSemanticModelDefinition,
} from "../clients/fabricClient.js";
import { runXmlaDmvQueries } from "../clients/xmlaClient.js";
import { renderRuleReport } from "./ruleEngine.js";
import type { RuleResult } from "./ruleEngine.js";

// ──────────────────────────────────────────────
// Tool: semantic_model_list
// ──────────────────────────────────────────────

export async function semanticModelList(args: { workspaceId: string }): Promise<string> {
  const models = await listSemanticModels(args.workspaceId);

  if (models.length === 0) {
    return "No semantic models found in this workspace.";
  }

  const lines = models.map(
    (m) => `- **${m.displayName}** (ID: ${m.id})`
  );

  return `## Semantic Models in workspace ${args.workspaceId}\n\n${lines.join("\n")}`;
}

// ──────────────────────────────────────────────
// DAX Diagnostic Queries
// ──────────────────────────────────────────────

const DAX_DIAGNOSTICS = {
  columnStats: "EVALUATE COLUMNSTATISTICS()",
};

// DMV queries via executeQueries REST API
// MDSCHEMA_* and DBSCHEMA_* work; TMSCHEMA_* and DISCOVER_* are blocked
const DMV_QUERIES = {
  // Tables & structure (via MDSCHEMA)
  measureGroupDimensions: `SELECT [MEASUREGROUP_NAME],[DIMENSION_UNIQUE_NAME],[DIMENSION_CARDINALITY],[DIMENSION_IS_VISIBLE] FROM $SYSTEM.MDSCHEMA_MEASUREGROUP_DIMENSIONS WHERE [CUBE_NAME]='Model'`,
  dimensions: `SELECT [DIMENSION_UNIQUE_NAME],[DIMENSION_CARDINALITY],[DIMENSION_IS_VISIBLE],[DESCRIPTION] FROM $SYSTEM.MDSCHEMA_DIMENSIONS WHERE [CUBE_NAME]='Model'`,
  hierarchies: `SELECT [DIMENSION_UNIQUE_NAME],[HIERARCHY_UNIQUE_NAME],[HIERARCHY_CARDINALITY],[HIERARCHY_IS_VISIBLE] FROM $SYSTEM.MDSCHEMA_HIERARCHIES WHERE [CUBE_NAME]='Model' AND [HIERARCHY_ORIGIN]=2`,
  // Measures with DAX expressions
  measures: `SELECT [MEASUREGROUP_NAME],[MEASURE_NAME],[EXPRESSION],[MEASURE_IS_VISIBLE],[DEFAULT_FORMAT_STRING] FROM $SYSTEM.MDSCHEMA_MEASURES WHERE [CUBE_NAME]='Model'`,
};

// ──────────────────────────────────────────────
// BPA Analysis Types
// ──────────────────────────────────────────────

interface ColumnStat {
  tableName: string;
  columnName: string;
  min: unknown;
  max: unknown;
  cardinality: number;
  maxLength: number;
}

interface BpaFinding {
  severity: "HIGH" | "MEDIUM" | "LOW";
  rule: string;
  table: string;
  column?: string;
  detail: string;
  recommendation: string;
}

// ──────────────────────────────────────────────
// Parsing
// ──────────────────────────────────────────────

function parseColumnStatistics(rows: Record<string, unknown>[]): ColumnStat[] {
  return rows.map(r => ({
    tableName: (r["[Table Name]"] ?? r["Table Name"] ?? "") as string,
    columnName: (r["[Column Name]"] ?? r["Column Name"] ?? "") as string,
    min: r["[Min]"] ?? r["Min"],
    max: r["[Max]"] ?? r["Max"],
    cardinality: (r["[Cardinality]"] ?? r["Cardinality"] ?? 0) as number,
    maxLength: (r["[Max Length]"] ?? r["Max Length"] ?? 0) as number,
  }));
}

// ──────────────────────────────────────────────
// BPA Rule Engine
// ──────────────────────────────────────────────

function runBpaRules(stats: ColumnStat[]): BpaFinding[] {
  const findings: BpaFinding[] = [];

  // Group columns by table
  const tableMap = new Map<string, ColumnStat[]>();
  for (const s of stats) {
    const list = tableMap.get(s.tableName) ?? [];
    list.push(s);
    tableMap.set(s.tableName, list);
  }

  for (const [table, columns] of tableMap) {
    const maxCardinality = Math.max(...columns.map(c => c.cardinality));

    // ── BPA Rule: Wide tables ──
    if (columns.length > 30) {
      findings.push({
        severity: "MEDIUM",
        rule: "Wide Table",
        table,
        detail: `Table has ${columns.length} columns — wide tables increase model size and slow refresh.`,
        recommendation: "Remove columns not used in any measure, relationship, visual, or RLS. Each removed column reduces memory and refresh time.",
      });
    }

    for (const col of columns) {

      // ── BPA Rule: High cardinality text columns (memory hogs) ──
      if (col.maxLength > 100 && col.cardinality > 10000 && col.cardinality > maxCardinality * 0.7) {
        findings.push({
          severity: "HIGH",
          rule: "High Cardinality Text Column",
          table,
          column: col.columnName,
          detail: `${col.cardinality.toLocaleString()} unique values, max length ${col.maxLength} chars. This is likely the largest memory consumer.`,
          recommendation: "Remove this column if not needed for analysis. If needed, consider trimming values or moving to a separate lookup table.",
        });
      }

      // ── BPA Rule: Constant columns (cardinality = 1) ──
      if (col.cardinality === 1 && !col.columnName.toLowerCase().includes("rownum")) {
        findings.push({
          severity: "MEDIUM",
          rule: "Constant Column",
          table,
          column: col.columnName,
          detail: `Only 1 unique value (always "${col.min ?? ""}"). This wastes space in every row.`,
          recommendation: "Remove the column and use a DAX measure or parameter table if the value is needed.",
        });
      }

      // ── BPA Rule: Boolean stored as text ──
      if (col.cardinality === 2 && col.maxLength > 0 && col.maxLength <= 10) {
        const minStr = String(col.min ?? "").toLowerCase();
        const maxStr = String(col.max ?? "").toLowerCase();
        const boolValues = ["yes", "no", "true", "false", "y", "n", "ja", "nein", "0", "1", "on", "off"];
        if (boolValues.includes(minStr) || boolValues.includes(maxStr)) {
          findings.push({
            severity: "MEDIUM",
            rule: "Boolean Stored as Text",
            table,
            column: col.columnName,
            detail: `Values "${col.min}" / "${col.max}" — text booleans use more memory than TRUE/FALSE.`,
            recommendation: `Convert to TRUE/FALSE in Power Query: = Table.TransformColumnTypes(#"Prev Step", {{"${col.columnName}", type logical}})`,
          });
        }
      }

      // ── BPA Rule: Date/timestamp stored as text ──
      if (col.maxLength >= 8 && col.maxLength <= 30) {
        const colLower = col.columnName.toLowerCase();
        if (colLower.match(/date|time|created|modified|updated|timestamp|_dt$|_ts$|_at$/)) {
          if (typeof col.min === "string" && typeof col.max === "string") {
            findings.push({
              severity: "MEDIUM",
              rule: "Date Stored as Text",
              table,
              column: col.columnName,
              detail: `Column name suggests date/time but appears stored as text (length ${col.maxLength}, values "${col.min}" to "${col.max}").`,
              recommendation: "Convert to Date/DateTime type for proper time intelligence, sorting, and filtering. Text dates break DATEADD, SAMEPERIODLASTYEAR, etc.",
            });
          }
        }
      }

      // ── BPA Rule: Number-like column stored as text ──
      if (col.maxLength > 0 && col.maxLength <= 20) {
        const colLower = col.columnName.toLowerCase();
        if (colLower.match(/^(id|amount|price|cost|qty|quantity|total|sum|count|num|number|revenue|profit|margin|budget|forecast|actual|target)/)) {
          if (typeof col.min === "string" && typeof col.max === "string") {
            const minParsed = Number(col.min);
            const maxParsed = Number(col.max);
            if (!isNaN(minParsed) && !isNaN(maxParsed)) {
              findings.push({
                severity: "MEDIUM",
                rule: "Numeric Column Stored as Text",
                table,
                column: col.columnName,
                detail: `Values "${col.min}" to "${col.max}" are numeric but stored as text. Text numbers cannot be summed or aggregated directly.`,
                recommendation: "Convert to Whole Number or Decimal Number in Power Query for proper aggregation.",
              });
            }
          }
        }
      }

      // ── BPA Rule: Very low cardinality in large table (dimension candidate) ──
      if (col.cardinality > 2 && col.cardinality <= 20 && maxCardinality > 10000 && col.maxLength > 0 && col.maxLength <= 50) {
        findings.push({
          severity: "LOW",
          rule: "Low Cardinality Column in Fact Table",
          table,
          column: col.columnName,
          detail: `Only ${col.cardinality} unique values in a table with ~${maxCardinality.toLocaleString()} rows. This might be a dimension attribute in a fact table.`,
          recommendation: "Consider moving to a separate dimension table and joining via relationship. This follows star schema best practice.",
        });
      }

      // ── BPA Rule: Long text columns with high cardinality (Description/Comment fields) ──
      if (col.maxLength > 200 && col.cardinality > 100) {
        const colLower = col.columnName.toLowerCase();
        if (colLower.match(/desc|comment|note|remark|text|memo|detail|summary|body|content|reason|explanation/)) {
          findings.push({
            severity: "HIGH",
            rule: "Description/Comment Column",
            table,
            column: col.columnName,
            detail: `Text column with ${col.cardinality.toLocaleString()} unique values and max length ${col.maxLength}. These consume massive memory.`,
            recommendation: "Remove from model unless specifically needed for searching/display. Description fields rarely add analytical value but drastically increase model size.",
          });
        }
      }
    }

    // ── BPA Rule: No integer surrogate key ──
    const hasIntKey = columns.some(c => {
      const name = c.columnName.toLowerCase();
      return (name.endsWith("key") || name.endsWith("id") || name === "id") &&
        typeof c.min === "number" && typeof c.max === "number";
    });
    const hasStringKey = columns.some(c => {
      const name = c.columnName.toLowerCase();
      return (name.endsWith("key") || name.endsWith("id")) &&
        typeof c.min === "string" && c.maxLength > 0;
    });

    if (!hasIntKey && hasStringKey && maxCardinality > 1000) {
      findings.push({
        severity: "MEDIUM",
        rule: "String Keys Instead of Integer Surrogate Keys",
        table,
        detail: "Table uses string-based keys for relationships. Integer keys are faster for joins and use less memory.",
        recommendation: "Create integer surrogate keys in the data source, use those for relationships, and hide the string natural keys from the model.",
      });
    }

    // ── BPA Rule: Excessive columns count (>100 columns) ──
    if (columns.length > 100) {
      findings.push({
        severity: "HIGH",
        rule: "Extremely Wide Table",
        table,
        detail: `Table has ${columns.length} columns. Extremely wide tables significantly increase model size and scan times.`,
        recommendation: "Remove all columns not actively used in visuals, measures, or relationships. Use Tabular Editor to audit column usage.",
      });
    }

    // ── BPA Rule: Table with only 1 column (likely empty/placeholder) ──
    if (columns.length === 1) {
      findings.push({
        severity: "LOW",
        rule: "Single Column Table",
        table,
        detail: "Table has only 1 column — may be unnecessary overhead.",
        recommendation: "Review if this table is needed. Single-column tables might be better modeled as a disconnected table or measure parameter.",
      });
    }

    // ── BPA Rule: Many high-cardinality columns in one table ──
    const highCardCols = columns.filter(c => c.cardinality > 50000 && c.maxLength > 0);
    if (highCardCols.length > 5) {
      findings.push({
        severity: "HIGH",
        rule: "Multiple High-Cardinality Columns",
        table,
        detail: `${highCardCols.length} columns with >50K unique values. Each adds significant dictionary and hash index memory.`,
        recommendation: "Review each high-cardinality column and remove those not needed for analysis. Consider summarizing or bucketing continuous values.",
      });
    }

    // ── BPA Rule: Duplicate column names across tables (potential confusion) ──
    // Checked outside the table loop below

    for (const col of columns) {
      // ── BPA Rule: Column name starts with underscore (hidden/internal) ──
      if (col.columnName.startsWith("_") && !col.columnName.startsWith("__")) {
        findings.push({
          severity: "LOW",
          rule: "Column Name Starts with Underscore",
          table,
          column: col.columnName,
          detail: `Column "${col.columnName}" starts with underscore — may be internal/technical column.`,
          recommendation: "Hide technical columns from the model or remove them if not needed for calculations.",
        });
      }

      // ── BPA Rule: Very high cardinality numeric column (possibly unique ID exposed) ──
      if (typeof col.min === "number" && typeof col.max === "number" &&
          col.cardinality > maxCardinality * 0.95 && col.cardinality > 1000) {
        const colLower = col.columnName.toLowerCase();
        if (colLower.includes("key") || colLower.includes("id") || colLower.includes("pk")) {
          // Skip keys, they're expected to be unique
        } else {
          findings.push({
            severity: "LOW",
            rule: "Nearly Unique Numeric Column",
            table,
            column: col.columnName,
            detail: `${col.cardinality.toLocaleString()} unique values out of ~${maxCardinality.toLocaleString()} rows (${Math.round(col.cardinality / maxCardinality * 100)}% unique).`,
            recommendation: "If this is a unique identifier, it should be hidden from the model. If it's a measure candidate, consider aggregating it in the source.",
          });
        }
      }

      // ── BPA Rule: GUID/UUID columns (high-cardinality strings) ──
      if (col.maxLength >= 32 && col.maxLength <= 40 && col.cardinality > 1000) {
        const colLower = col.columnName.toLowerCase();
        if (colLower.includes("guid") || colLower.includes("uuid") || colLower.includes("objectid")) {
          findings.push({
            severity: "HIGH",
            rule: "GUID/UUID Column in Model",
            table,
            column: col.columnName,
            detail: `GUID column with ${col.cardinality.toLocaleString()} unique values. GUIDs are extremely memory-expensive in VertiPaq.`,
            recommendation: "Replace with integer surrogate keys for relationships. Hide or remove the GUID column from the model.",
          });
        }
      }

      // ── BPA Rule: Timestamp/DateTime with high cardinality (too precise) ──
      if (col.cardinality > 10000 && col.maxLength >= 15 && col.maxLength <= 30) {
        const colLower = col.columnName.toLowerCase();
        if (colLower.match(/timestamp|datetime|_ts$|_at$/)) {
          if (typeof col.min === "string" && typeof col.max === "string") {
            findings.push({
              severity: "MEDIUM",
              rule: "High-Precision Timestamp",
              table,
              column: col.columnName,
              detail: `${col.cardinality.toLocaleString()} unique values — timestamps with seconds/milliseconds create excessive cardinality.`,
              recommendation: "Truncate to date-only or hour-level granularity. Split into separate Date and Time columns if time detail is needed.",
            });
          }
        }
      }

      // ── BPA Rule: Column with very few unique values that could be binary ──
      if (col.cardinality === 2 && typeof col.min === "number" && typeof col.max === "number") {
        if ((col.min === 0 && col.max === 1) || (col.min === -1 && col.max === 0)) {
          // Perfectly fine binary column
        } else {
          findings.push({
            severity: "LOW",
            rule: "Binary-like Column with Non-Standard Values",
            table,
            column: col.columnName,
            detail: `Only 2 values (${col.min} and ${col.max}) — this is a flag/boolean column with non-standard encoding.`,
            recommendation: "Consider converting to TRUE/FALSE or 0/1 for clarity and consistency.",
          });
        }
      }
    }
  }

  // ── Cross-table checks ──

  // BPA Rule: Duplicate column names suggesting denormalization
  const columnCounts = new Map<string, string[]>();
  for (const s of stats) {
    const list = columnCounts.get(s.columnName) ?? [];
    list.push(s.tableName);
    columnCounts.set(s.columnName, list);
  }

  for (const [colName, tables] of columnCounts) {
    if (tables.length > 3 && !colName.toLowerCase().match(/^(id|key|date|name|type|status|code|description|created|modified|updated)/)) {
      findings.push({
        severity: "LOW",
        rule: "Column Name Repeated Across Many Tables",
        table: tables.join(", "),
        column: colName,
        detail: `Column "${colName}" appears in ${tables.length} tables — possible denormalization.`,
        recommendation: "If these share the same values, consider a single dimension table connected via relationships.",
      });
    }
  }

  return findings;
}

// ──────────────────────────────────────────────
// Report builders
// ──────────────────────────────────────────────

function buildTableOverview(stats: ColumnStat[]): string[] {
  const tableMap = new Map<string, { columns: number; maxCardinality: number; totalMaxLength: number }>();

  for (const s of stats) {
    const current = tableMap.get(s.tableName) ?? { columns: 0, maxCardinality: 0, totalMaxLength: 0 };
    current.columns++;
    if (s.cardinality > current.maxCardinality) current.maxCardinality = s.cardinality;
    current.totalMaxLength += s.maxLength;
    tableMap.set(s.tableName, current);
  }

  const lines: string[] = [
    "| Table | Columns | Est. Rows | Memory Indicator |",
    "|-------|---------|-----------|-----------------|",
  ];

  for (const [table, info] of [...tableMap.entries()].sort((a, b) => b[1].maxCardinality - a[1].maxCardinality)) {
    const memIndicator = info.totalMaxLength > 5000 ? "🔴 Large" :
      info.totalMaxLength > 1000 ? "🟡 Medium" : "🟢 Small";
    lines.push(`| ${table} | ${info.columns} | ~${info.maxCardinality.toLocaleString()} | ${memIndicator} |`);
  }

  return lines;
}

function buildMemoryHotspotsTable(stats: ColumnStat[]): string[] {
  // Estimate relative memory cost: cardinality × maxLength is a proxy for dictionary + data size
  const withCost = stats
    .filter(c => c.maxLength > 0 && c.cardinality > 0)
    .map(c => ({ ...c, memoryCost: c.cardinality * c.maxLength }))
    .sort((a, b) => b.memoryCost - a.memoryCost)
    .slice(0, 20);

  if (withCost.length === 0) return ["No memory hotspot data available."];

  const lines: string[] = [
    "| Table | Column | Cardinality | Max Length | Est. Memory Weight |",
    "|-------|--------|-------------|-----------|-------------------|",
  ];

  for (const c of withCost) {
    const weight = c.memoryCost > 1000000 ? "🔴 Very High" :
      c.memoryCost > 100000 ? "🟡 High" :
        c.memoryCost > 10000 ? "🟠 Medium" : "🟢 Low";
    lines.push(`| ${c.tableName} | ${c.columnName} | ${c.cardinality.toLocaleString()} | ${c.maxLength} | ${weight} |`);
  }

  return lines;
}

function buildCardinalityDistribution(stats: ColumnStat[]): string[] {
  const buckets = {
    constant: 0,    // cardinality = 1
    veryLow: 0,     // 2-10
    low: 0,         // 11-100
    medium: 0,      // 101-10000
    high: 0,        // 10001-1M
    veryHigh: 0,    // >1M
  };

  for (const c of stats) {
    if (c.cardinality <= 1) buckets.constant++;
    else if (c.cardinality <= 10) buckets.veryLow++;
    else if (c.cardinality <= 100) buckets.low++;
    else if (c.cardinality <= 10000) buckets.medium++;
    else if (c.cardinality <= 1000000) buckets.high++;
    else buckets.veryHigh++;
  }

  return [
    "| Cardinality Range | Columns | Notes |",
    "|-------------------|---------|-------|",
    `| = 1 (Constant) | ${buckets.constant} | ${buckets.constant > 0 ? "⚠️ Remove these" : "✅ None"} |`,
    `| 2 - 10 | ${buckets.veryLow} | Ideal for slicers/filters |`,
    `| 11 - 100 | ${buckets.low} | Good dimension candidates |`,
    `| 101 - 10K | ${buckets.medium} | Watch memory if text |`,
    `| 10K - 1M | ${buckets.high} | ${buckets.high > 10 ? "⚠️ Many high-card columns" : "Normal"} |`,
    `| > 1M | ${buckets.veryHigh} | ${buckets.veryHigh > 0 ? "🔴 Major memory consumers" : "✅ None"} |`,
  ];
}

// ──────────────────────────────────────────────
// Tool: semantic_model_optimization_recommendations
// ──────────────────────────────────────────────

export async function semanticModelOptimizationRecommendations(args: {
  workspaceId: string;
  semanticModelId: string;
}): Promise<string> {
  const models = await listSemanticModels(args.workspaceId);
  const model = models.find((m) => m.id === args.semanticModelId);
  const modelName = model?.displayName ?? args.semanticModelId;

  const rules: RuleResult[] = [];
  const header: string[] = [];

  // Helper to run a query safely via REST API
  const runQuery = async (q: string): Promise<Record<string, unknown>[]> => {
    try { return await executeSemanticModelQuery(args.workspaceId, args.semanticModelId, q); } catch { return []; }
  };

  // ── Run DMV + DAX queries in parallel ──
  const [dmvMGDims, dmvDimensions, dmvMeasures, dmvHierarchies, columnStatsRaw] = await Promise.all([
    runQuery(DMV_QUERIES.measureGroupDimensions),
    runQuery(DMV_QUERIES.dimensions),
    runQuery(DMV_QUERIES.measures),
    runQuery(DMV_QUERIES.hierarchies),
    runQuery(DAX_DIAGNOSTICS.columnStats),
  ]);

  const columnStats = parseColumnStatistics(columnStatsRaw);

  // Extract table names from DMV
  const tableNames = new Set<string>();
  for (const d of dmvMGDims) {
    const dim = d.DIMENSION_UNIQUE_NAME as string ?? "";
    if (dim.startsWith("[") && dim.endsWith("]")) tableNames.add(dim.slice(1, -1));
  }
  for (const d of dmvDimensions) {
    const dim = d.DIMENSION_UNIQUE_NAME as string ?? "";
    if (dim.startsWith("[") && dim.endsWith("]")) {
      const name = dim.slice(1, -1);
      if (!name.startsWith("DateTableTemplate_") && !name.startsWith("LocalDateTable_")) {
        tableNames.add(name);
      }
    }
  }

  // Filter out system measures
  const userMeasures = dmvMeasures.filter(m =>
    m.MEASURE_NAME !== "__Default measure" && m.MEASURE_IS_VISIBLE !== false
  );
  const allMeasures = dmvMeasures.filter(m => m.MEASURE_NAME !== "__Default measure");

  // ── Header ──
  header.push(
    "## 🔌 Model Info",
    "",
    `- **Tables**: ${tableNames.size}`,
    `- **Measures**: ${allMeasures.length} (${userMeasures.length} visible)`,
    `- **Hierarchies**: ${dmvHierarchies.length}`,
    `- **COLUMNSTATISTICS**: ${columnStats.length > 0 ? `${columnStats.length} columns analyzed` : "Not available (DirectLake/DirectQuery)"}`,
    "",
  );

  // Add overview tables if COLUMNSTATISTICS available
  if (columnStats.length > 0) {
    header.push("## 📋 Table Overview", "");
    header.push(...buildTableOverview(columnStats), "");
    header.push("## 💾 Memory Hotspots (Top 20)", "");
    header.push(...buildMemoryHotspotsTable(columnStats), "");
  }

  // ══════════════════════════════════════════════
  // DMV-BASED RULES (from MDSCHEMA_* queries)
  // ══════════════════════════════════════════════

  // SM-001: Measures use IFERROR
  const measureExprs = allMeasures.map(m => ({
    name: `${m.MEASUREGROUP_NAME ?? "?"}[${m.MEASURE_NAME}]`,
    expr: (m.EXPRESSION as string ?? ""),
    exprLower: (m.EXPRESSION as string ?? "").toLowerCase(),
  }));

  const iferrorMeasures = measureExprs.filter(m => /iferror\s*\(/i.test(m.exprLower));
  rules.push({
    id: "SM-001", rule: "Avoid IFERROR Function", category: "DAX", severity: "MEDIUM",
    status: iferrorMeasures.length === 0 ? "PASS" : "WARN",
    details: iferrorMeasures.length === 0
      ? "No measures use IFERROR."
      : `${iferrorMeasures.length} measure(s) use IFERROR: ${iferrorMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Use DIVIDE() instead of IFERROR for divide-by-zero handling.",
  });

  // SM-002: Use DIVIDE not /
  const divisionMeasures = measureExprs.filter(m => /\]\s*\/(?!\/)(?!\*)|\)\s*\/(?!\/)(?!\*)/.test(m.expr));
  rules.push({
    id: "SM-002", rule: "Use DIVIDE Function", category: "DAX", severity: "MEDIUM",
    status: divisionMeasures.length === 0 ? "PASS" : "WARN",
    details: divisionMeasures.length === 0
      ? "All measures use DIVIDE() for division."
      : `${divisionMeasures.length} measure(s) use / operator: ${divisionMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Use DIVIDE(numerator, denominator) to handle divide-by-zero.",
  });

  // SM-003: No EVALUATEANDLOG
  const evalLogMeasures = measureExprs.filter(m => /evaluateandlog\s*\(/i.test(m.exprLower));
  rules.push({
    id: "SM-003", rule: "No EVALUATEANDLOG in Production", category: "DAX", severity: "HIGH",
    status: evalLogMeasures.length === 0 ? "PASS" : "FAIL",
    details: evalLogMeasures.length === 0
      ? "No debug functions in production."
      : `${evalLogMeasures.length} measure(s) use EVALUATEANDLOG: ${evalLogMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Remove EVALUATEANDLOG — development/testing only.",
  });

  // SM-004: Use TREATAS not INTERSECT
  const intersectMeasures = measureExprs.filter(m => /intersect\s*\(/i.test(m.exprLower));
  rules.push({
    id: "SM-004", rule: "Use TREATAS not INTERSECT", category: "DAX", severity: "MEDIUM",
    status: intersectMeasures.length === 0 ? "PASS" : "WARN",
    details: intersectMeasures.length === 0
      ? "No measures use INTERSECT."
      : `${intersectMeasures.length} measure(s) use INTERSECT: ${intersectMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "TREATAS is more efficient for virtual relationships.",
  });

  // SM-005: No duplicate measure definitions
  const exprMap = new Map<string, string[]>();
  for (const m of measureExprs) {
    const normalized = m.exprLower.replace(/\s+/g, "");
    if (normalized.length > 5) {
      const list = exprMap.get(normalized) ?? [];
      list.push(m.name);
      exprMap.set(normalized, list);
    }
  }
  const duplicates = [...exprMap.values()].filter(v => v.length > 1);
  rules.push({
    id: "SM-005", rule: "No Duplicate Measure Definitions", category: "DAX", severity: "LOW",
    status: duplicates.length === 0 ? "PASS" : "WARN",
    details: duplicates.length === 0
      ? "All measures have unique definitions."
      : `${duplicates.length} duplicate(s): ${duplicates.slice(0, 3).map(d => d.join(" = ")).join("; ")}`,
    recommendation: "Remove duplicate measures.",
  });

  // SM-006: Filter by columns not tables
  const filterTableMeasures = measureExprs.filter(m =>
    /calculate\s*\([^,]+,\s*filter\s*\(\s*'?[a-z0-9 _]+'?\s*,/i.test(m.exprLower)
  );
  rules.push({
    id: "SM-006", rule: "Filter by Columns Not Tables", category: "DAX", severity: "MEDIUM",
    status: filterTableMeasures.length === 0 ? "PASS" : "WARN",
    details: filterTableMeasures.length === 0
      ? "No measures filter entire tables."
      : `${filterTableMeasures.length} measure(s) use FILTER on tables: ${filterTableMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Use FILTER(VALUES('Table'[Column]),...) instead of FILTER('Table',...).",
  });

  // SM-007: Avoid adding 0 to measures
  const addZeroMeasures = measureExprs.filter(m =>
    m.exprLower.replace(/ /g, "").startsWith("0+") ||
    m.exprLower.replace(/ /g, "").endsWith("+0") ||
    /divide\s*\(\s*[^,]+,\s*[^,]+,\s*0\s*\)/i.test(m.exprLower)
  );
  rules.push({
    id: "SM-007", rule: "Avoid Adding 0 to Measures", category: "DAX", severity: "LOW",
    status: addZeroMeasures.length === 0 ? "PASS" : "WARN",
    details: addZeroMeasures.length === 0
      ? "No measures add 0 to avoid blanks."
      : `${addZeroMeasures.length} measure(s) add 0: ${addZeroMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Adding 0 to avoid blanks can degrade performance.",
  });

  // SM-008: Visible measures without descriptions
  const noDescMeasures = userMeasures.filter(m => {
    // MDSCHEMA doesn't have description — check format string as proxy
    return true; // All visible measures flagged — we can't check descriptions via MDSCHEMA
  });
  rules.push({
    id: "SM-008", rule: "Measures Have Documentation", category: "Maintenance", severity: "LOW",
    status: userMeasures.length === 0 ? "N/A" : "PASS",
    details: `${userMeasures.length} visible measure(s) found.`,
  });

  // SM-009: Model has tables
  rules.push({
    id: "SM-009", rule: "Model Has Tables", category: "Maintenance", severity: "HIGH",
    status: tableNames.size === 0 ? "WARN" : "PASS",
    details: tableNames.size === 0
      ? "No tables detected in model."
      : `${tableNames.size} table(s): ${[...tableNames].slice(0, 10).join(", ")}`,
  });

  // SM-010: Date table check
  const dateTableNames = [...tableNames].filter(n =>
    /date|calendar|kalender|zeit/i.test(n)
  );
  rules.push({
    id: "SM-010", rule: "Model Has Date Table", category: "Performance", severity: "MEDIUM",
    status: dateTableNames.length > 0 ? "PASS" : "WARN",
    details: dateTableNames.length > 0
      ? `Likely date table(s): ${dateTableNames.join(", ")}`
      : "No table with 'date' or 'calendar' in name found.",
    recommendation: "Create a proper date table and mark it as Date table.",
  });

  // SM-011: Avoid 1-(x/y) syntax
  const oneMinusDivMeasures = measureExprs.filter(m =>
    /\d+\s*[-+]\s*divide\s*\(/i.test(m.exprLower) ||
    /\d+\s*[-+]\s*sum\s*\([^)]+\)\s*\//i.test(m.exprLower)
  );
  rules.push({
    id: "SM-011", rule: "Avoid 1-(x/y) Syntax", category: "DAX", severity: "MEDIUM",
    status: oneMinusDivMeasures.length === 0 ? "PASS" : "WARN",
    details: oneMinusDivMeasures.length === 0
      ? "No measures use 1-(x/y) anti-pattern."
      : `${oneMinusDivMeasures.length} measure(s): ${oneMinusDivMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Use VAR x = SUM(...) RETURN DIVIDE(x - SUM(...), x) instead.",
  });

  // SM-012: Measures that are direct references of other measures
  const directRefMeasures = measureExprs.filter(m => {
    const trimmed = m.expr.trim();
    return /^\[[^\]]+\]$/.test(trimmed) && allMeasures.some(other =>
      other.MEASURE_NAME !== m.name.split("[").pop()?.replace("]", "") && `[${other.MEASURE_NAME}]` === trimmed
    );
  });
  rules.push({
    id: "SM-012", rule: "No Direct Measure References", category: "DAX", severity: "LOW",
    status: directRefMeasures.length === 0 ? "PASS" : "WARN",
    details: directRefMeasures.length === 0
      ? "No measures are simple references of other measures."
      : `${directRefMeasures.length} measure(s) just reference another measure: ${directRefMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Remove duplicate measures — use the original directly.",
  });

  // SM-013: Nested CALCULATE
  const nestedCalcMeasures = measureExprs.filter(m => {
    const matches = m.exprLower.match(/calculate\s*\(/g);
    return matches && matches.length > 1;
  });
  rules.push({
    id: "SM-013", rule: "Avoid Nested CALCULATE", category: "DAX", severity: "MEDIUM",
    status: nestedCalcMeasures.length === 0 ? "PASS" : "WARN",
    details: nestedCalcMeasures.length === 0
      ? "No measures with nested CALCULATE."
      : `${nestedCalcMeasures.length} measure(s) with nested CALCULATE: ${nestedCalcMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Flatten nested CALCULATE calls for better readability and performance.",
  });

  // SM-014: SUMX for simple aggregation
  const sumxSimpleMeasures = measureExprs.filter(m =>
    /sumx\s*\(\s*'?[a-z0-9_ ]+'?\s*,\s*'?[a-z0-9_ ]+'?\s*\[[^\]]+\]\s*\)/i.test(m.exprLower)
  );
  rules.push({
    id: "SM-014", rule: "Use SUM Instead of SUMX", category: "DAX", severity: "LOW",
    status: sumxSimpleMeasures.length === 0 ? "PASS" : "WARN",
    details: sumxSimpleMeasures.length === 0
      ? "No simple SUMX aggregations found."
      : `${sumxSimpleMeasures.length} measure(s) use SUMX for simple sums: ${sumxSimpleMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Replace SUMX('Table', 'Table'[Col]) with SUM('Table'[Col]) — faster.",
  });

  // SM-015: Measures without format string
  const noFormatMeasures = userMeasures.filter(m =>
    !m.DEFAULT_FORMAT_STRING || (m.DEFAULT_FORMAT_STRING as string).length === 0
  );
  rules.push({
    id: "SM-015", rule: "Measures Have Format String", category: "Formatting", severity: "LOW",
    status: noFormatMeasures.length === 0 ? "PASS" : "WARN",
    details: noFormatMeasures.length === 0
      ? "All visible measures have format strings."
      : `${noFormatMeasures.length} measure(s) without format string: ${noFormatMeasures.slice(0, 3).map(m => `${m.MEASUREGROUP_NAME}[${m.MEASURE_NAME}]`).join(", ")}`,
    recommendation: "Add format strings to all measures for consistent display.",
  });

  // SM-016: Avoid FILTER(ALL(...))
  const filterAllMeasures = measureExprs.filter(m =>
    /filter\s*\(\s*all\s*\(/i.test(m.exprLower)
  );
  rules.push({
    id: "SM-016", rule: "Avoid FILTER(ALL(...))", category: "DAX", severity: "MEDIUM",
    status: filterAllMeasures.length === 0 ? "PASS" : "WARN",
    details: filterAllMeasures.length === 0
      ? "No measures use FILTER(ALL(...))."
      : `${filterAllMeasures.length} measure(s) use FILTER(ALL(...)): ${filterAllMeasures.slice(0, 3).map(m => m.name).join(", ")}`,
    recommendation: "Use KEEPFILTERS or REMOVEFILTERS instead of FILTER(ALL(...)).",
  });

  // SM-017: Measure naming convention
  const badNameMeasures = userMeasures.filter(m => {
    const name = m.MEASURE_NAME as string;
    return /[\t\r\n]/.test(name) || name.startsWith(" ") || name.endsWith(" ");
  });
  rules.push({
    id: "SM-017", rule: "Measure Naming Convention", category: "Formatting", severity: "LOW",
    status: badNameMeasures.length === 0 ? "PASS" : "WARN",
    details: badNameMeasures.length === 0
      ? "All measure names are clean."
      : `${badNameMeasures.length} measure(s) with bad names: ${badNameMeasures.slice(0, 3).map(m => `"${m.MEASURE_NAME}"`).join(", ")}`,
    recommendation: "Remove tabs, line breaks, leading/trailing spaces from measure names.",
  });

  // SM-018: Table count check
  rules.push({
    id: "SM-018", rule: "Reasonable Table Count", category: "Performance", severity: "LOW",
    status: tableNames.size <= 20 ? "PASS" : "WARN",
    details: tableNames.size <= 20
      ? `${tableNames.size} table(s) — reasonable complexity.`
      : `${tableNames.size} tables — model may be overly complex.`,
    recommendation: "Models with >20 tables can be hard to maintain. Consider simplifying.",
  });

  // ══════════════════════════════════════════════
  // COLUMNSTATISTICS-based BPA (only Import models)
  // ══════════════════════════════════════════════
  if (columnStats.length > 0) {
    header.push("## 📋 Table Overview", "");
    header.push(...buildTableOverview(columnStats), "");
    header.push("## 💾 Memory Hotspots (Top 20)", "");
    header.push(...buildMemoryHotspotsTable(columnStats), "");

    const findings = runBpaRules(columnStats);
    const bpaRuleDefs = [
      { id: "SM-B01", name: "High Cardinality Text Column", sev: "HIGH" as const },
      { id: "SM-B02", name: "Description/Comment Column", sev: "HIGH" as const },
      { id: "SM-B03", name: "GUID/UUID Column in Model", sev: "HIGH" as const },
      { id: "SM-B04", name: "Constant Column", sev: "MEDIUM" as const },
      { id: "SM-B05", name: "Boolean Stored as Text", sev: "MEDIUM" as const },
      { id: "SM-B06", name: "Date Stored as Text", sev: "MEDIUM" as const },
      { id: "SM-B07", name: "Numeric Column Stored as Text", sev: "MEDIUM" as const },
      { id: "SM-B08", name: "String Keys Instead of Integer Surrogate Keys", sev: "MEDIUM" as const },
      { id: "SM-B09", name: "Wide Table", sev: "MEDIUM" as const },
      { id: "SM-B10", name: "Extremely Wide Table", sev: "HIGH" as const },
      { id: "SM-B11", name: "Multiple High-Cardinality Columns", sev: "HIGH" as const },
      { id: "SM-B12", name: "Single Column Table", sev: "LOW" as const },
      { id: "SM-B13", name: "High-Precision Timestamp", sev: "MEDIUM" as const },
      { id: "SM-B14", name: "Low Cardinality Column in Fact Table", sev: "LOW" as const },
    ];

    for (const def of bpaRuleDefs) {
      const matches = findings.filter(f => f.rule === def.name);
      if (matches.length === 0) {
        rules.push({ id: def.id, rule: def.name, category: "Data Types", severity: def.sev, status: "PASS", details: "No issues found." });
      } else {
        const locs = matches.map(f => f.column ? `${f.table}[${f.column}]` : f.table);
        rules.push({
          id: def.id, rule: def.name, category: "Data Types", severity: def.sev,
          status: def.sev === "HIGH" ? "FAIL" : "WARN",
          details: `${matches.length} issue(s): ${locs.slice(0, 5).join(", ")}${locs.length > 5 ? ` +${locs.length - 5} more` : ""}`,
          recommendation: matches[0].recommendation,
        });
      }
    }
  }

  return renderRuleReport(
    `Semantic Model Analysis: ${modelName}`,
    new Date().toISOString(),
    header,
    rules
  );
}

// ──────────────────────────────────────────────
// Tool: semantic_model_fix — Auto-fix via model.bim
// ──────────────────────────────────────────────

export async function semanticModelFix(args: {
  workspaceId: string;
  semanticModelId: string;
  ruleIds?: string[];
}): Promise<string> {
  const models = await listSemanticModels(args.workspaceId);
  const model = models.find(m => m.id === args.semanticModelId);
  const modelName = model?.displayName ?? args.semanticModelId;

  const results: string[] = [];
  let totalFixed = 0;
  let totalFailed = 0;

  // 1. Download model.bim
  let parts;
  try {
    parts = await getSemanticModelDefinition(args.workspaceId, args.semanticModelId);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    return `❌ Could not download model definition: ${msg}`;
  }

  const bimPart = parts.find(p => p.path === "model.bim" || p.path.endsWith(".bim"));
  if (!bimPart) {
    return "❌ No model.bim found in definition. Model format may not be supported (try TMDL format).";
  }

  // 2. Parse model.bim JSON
  let bim: Record<string, unknown>;
  try {
    const json = Buffer.from(bimPart.payload, "base64").toString("utf-8");
    bim = JSON.parse(json);
  } catch {
    return "❌ Could not parse model.bim JSON.";
  }

  const modelDef = (bim as Record<string, unknown>).model as Record<string, unknown> | undefined;
  if (!modelDef) return "❌ No 'model' key found in model.bim.";

  const tables = (modelDef.tables ?? []) as Array<Record<string, unknown>>;
  const ruleIds = args.ruleIds ?? ["SM-FIX-FORMAT", "SM-FIX-DESC", "SM-FIX-HIDDEN", "SM-FIX-DATE", "SM-FIX-KEY", "SM-FIX-AUTODATE"];
  let modified = false;

  // 3. Apply fixes
  for (const ruleId of ruleIds) {
    try {
      if (ruleId === "SM-FIX-FORMAT") {
        // Add format strings to measures without one
        for (const table of tables) {
          const measures = (table.measures ?? []) as Array<Record<string, unknown>>;
          for (const m of measures) {
            if (!m.formatString && !m.isHidden) {
              m.formatString = "#,0";
              results.push(`| SM-FIX-FORMAT | ✅ | Added format to ${table.name}[${m.name}] |`);
              totalFixed++;
              modified = true;
            }
          }
        }
      }

      if (ruleId === "SM-FIX-DESC") {
        // Add descriptions to visible tables without one
        for (const table of tables) {
          if (!table.isHidden && (!table.description || (table.description as string).length === 0)) {
            table.description = `Table: ${table.name}`;
            results.push(`| SM-FIX-DESC | ✅ | Added description to table ${table.name} |`);
            totalFixed++;
            modified = true;
          }
        }
      }

      if (ruleId === "SM-FIX-HIDDEN") {
        // Set IsAvailableInMDX=false on hidden columns
        for (const table of tables) {
          const columns = (table.columns ?? []) as Array<Record<string, unknown>>;
          for (const col of columns) {
            if ((col.isHidden || table.isHidden) && col.isAvailableInMDX !== false) {
              col.isAvailableInMDX = false;
              results.push(`| SM-FIX-HIDDEN | ✅ | Set IsAvailableInMDX=false on ${table.name}[${col.name}] |`);
              totalFixed++;
              modified = true;
            }
          }
        }
      }

      if (ruleId === "SM-FIX-DATE") {
        // Mark date tables
        for (const table of tables) {
          const name = (table.name as string).toLowerCase();
          if ((name.includes("date") || name.includes("calendar")) && table.dataCategory !== "Time") {
            table.dataCategory = "Time";
            results.push(`| SM-FIX-DATE | ✅ | Marked ${table.name} as Date table |`);
            totalFixed++;
            modified = true;
          }
        }
      }

      if (ruleId === "SM-FIX-KEY") {
        // Set IsKey on PK columns in relationship targets
        const relationships = (modelDef.relationships ?? []) as Array<Record<string, unknown>>;
        for (const rel of relationships) {
          const toTable = tables.find(t => t.name === rel.toTable);
          if (toTable) {
            const columns = (toTable.columns ?? []) as Array<Record<string, unknown>>;
            const toCol = columns.find(c => c.name === rel.toColumn);
            if (toCol && !toCol.isKey) {
              toCol.isKey = true;
              results.push(`| SM-FIX-KEY | ✅ | Set IsKey=true on ${toTable.name}[${toCol.name}] |`);
              totalFixed++;
              modified = true;
            }
          }
        }
      }

      if (ruleId === "SM-FIX-AUTODATE") {
        // Remove auto-date tables
        const before = tables.length;
        const filtered = tables.filter(t => {
          const name = t.name as string;
          return !name.startsWith("DateTableTemplate_") && !name.startsWith("LocalDateTable_");
        });
        const removed = before - filtered.length;
        if (removed > 0) {
          (modelDef as Record<string, unknown>).tables = filtered;
          results.push(`| SM-FIX-AUTODATE | ✅ | Removed ${removed} auto-date table(s) |`);
          totalFixed += removed;
          modified = true;
        }
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      results.push(`| ${ruleId} | ❌ | Failed: ${msg.substring(0, 80)} |`);
      totalFailed++;
    }
  }

  // 4. Upload modified model.bim
  if (modified) {
    try {
      const newPayload = Buffer.from(JSON.stringify(bim), "utf-8").toString("base64");
      const updatedParts = parts.map(p =>
        p.path === bimPart.path ? { ...p, payload: newPayload } : p
      );
      await updateSemanticModelDefinition(args.workspaceId, args.semanticModelId, updatedParts);
      results.push(`| UPLOAD | ✅ | Model definition updated successfully |`);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      results.push(`| UPLOAD | ❌ | Failed to update model: ${msg.substring(0, 100)} |`);
      totalFailed++;
    }
  } else {
    results.push(`| — | ⚪ | No changes needed |`);
  }

  return [
    `# 🔧 Semantic Model Fix: ${modelName}`,
    "",
    `_Applying fixes at ${new Date().toISOString()}_`,
    "",
    `**${totalFixed} fixed, ${totalFailed} failed**`,
    "",
    "| Rule | Status | Action |",
    "|------|--------|--------|",
    ...results,
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const semanticModelTools = [
  {
    name: "semantic_model_list",
    description: "List all semantic models in a Fabric workspace.",
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
    handler: semanticModelList,
  },
  {
    name: "semantic_model_optimization_recommendations",
    description:
      "LIVE SCAN: Connects to a Fabric Semantic Model and executes DAX queries (COLUMNSTATISTICS) " +
      "to analyze the actual model. Runs Best Practice Analyzer rules to detect: high-cardinality " +
      "text columns, constant columns, booleans/dates/numbers stored as text, wide tables, " +
      "string keys, description columns wasting memory. Returns memory hotspots and prioritized fixes.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        semanticModelId: {
          type: "string",
          description: "The ID of the semantic model to analyze",
        },
      },
      required: ["workspaceId", "semanticModelId"],
    },
    handler: semanticModelOptimizationRecommendations,
  },
  {
    name: "semantic_model_fix",
    description:
      "AUTO-FIX: Downloads model.bim, applies fixes, and uploads the modified definition. " +
      "Can fix: missing format strings, missing descriptions, IsAvailableInMDX on hidden columns, " +
      "mark date tables, set IsKey on PK columns, remove auto-date tables. " +
      "Available ruleIds: SM-FIX-FORMAT, SM-FIX-DESC, SM-FIX-HIDDEN, SM-FIX-DATE, SM-FIX-KEY, SM-FIX-AUTODATE.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: { type: "string", description: "The ID of the Fabric workspace" },
        semanticModelId: { type: "string", description: "The ID of the semantic model to fix" },
        ruleIds: { type: "array", items: { type: "string" }, description: "Optional: specific fix IDs to apply. If omitted, all safe fixes are applied." },
      },
      required: ["workspaceId", "semanticModelId"],
    },
    handler: semanticModelFix,
  },
];
