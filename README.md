<h1 align="center">Force Fabric MCP Server</h1>

<p align="center">
  <strong>Detect issues. Auto-fix problems. Optimize your Fabric tenant.</strong><br>
  An MCP server that scans Lakehouses, Warehouses, Eventhouses, Semantic Models, and Gateways with 158+ diagnostic rules — and can auto-fix 67 of them.
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#detect--scan">Detect</a> •
  <a href="#auto-fix">Auto-Fix</a> •
  <a href="#rule-reference">Rules</a> •
  <a href="#architecture">Architecture</a>
</p>

---

## Key Features

### Detect — 158+ Rules Across 5 Fabric Item Types

| Item | Rules | What's Scanned |
|------|-------|----------------|
| **Lakehouse** | 35 | SQL Endpoint + OneLake Delta Log (VACUUM history, file sizes, partitioning, retention, V-Order, CDF, deletion vectors) |
| **Warehouse** | 44 | Schema, query performance, security (PII, RLS), database config, FK indexes, computed columns |
| **Eventhouse** | 27/db | Extent fragmentation, caching/retention/merge/encoding/partitioning/sharding/autocompaction policies, ingestion, streaming, materialized views |
| **Semantic Model** | 41 | DAX anti-patterns, model structure, COLUMNSTATISTICS BPA, relationships, disconnected tables, implicit measures |
| **Gateway** | 12 | Gateway status, version, unused datasources, connectivity health, excess admins, orphaned connections, duplicates |
| | **158+ total** | |

### Fix — 67 Auto-Fixable Issues

| Item | Auto-Fixes | Method |
|------|-----------|--------|
| **Warehouse** | 16 fixes | SQL DDL executed directly |
| **Lakehouse** | 20 fixes | Livy Spark SQL + REST API |
| **Semantic Model** | 19 fixes | XMLA/TMSL atomic commands + BIM/TMDL fallback |
| **Eventhouse** | 11 fixes | KQL management commands (with dry-run preview) |
| **Gateway** | 4 fixes | Fabric + Power BI REST API |
| | **67 total** | |

### Unified Output

Every scan returns a clean results table — only issues shown, passed rules counted in summary:

```
29 rules — 18 passed | 1 failed | 10 warning

| Rule | Status | Finding | Recommendation |
|------|--------|---------|----------------|
| LH-007 Key Columns Are NOT NULL | FAIL | 16 key column(s) allow NULL | Add NOT NULL constraints |
| LH-017 Regular VACUUM Executed  | WARN | 4 table(s) need VACUUM     | Run VACUUM weekly        |
```

---

## Quick Start

### Prerequisites

- **Node.js** 18+
- **Azure CLI** with `az login` completed
- **Fabric capacity** with items to scan

### Install

```bash
git clone https://github.com/tmdaidevs/Force-Fabric-MCP-Server.git
cd Force-Fabric-MCP-Server
npm install
npm run build
```

### Configure VS Code

Add to `.vscode/mcp.json` in your project:

```json
{
  "servers": {
    "fabric-optimization": {
      "type": "stdio",
      "command": "node",
      "args": ["dist/index.js"],
      "cwd": "/path/to/Force-Fabric-MCP-Server"
    }
  }
}
```

### Use

```
1. "Login to Fabric with azure_cli"
2. "List all lakehouses in workspace <id>"
3. "Scan lakehouse <id> in workspace <id>"
4. "Fix warehouse <id> in workspace <id>"
```

---

## Detect & Scan

### Available Scan Tools

| Tool | What It Does |
|------|-------------|
| `lakehouse_optimization_recommendations` | Scans SQL Endpoint + reads Delta Log files from OneLake |
| `warehouse_optimization_recommendations` | Connects via SQL and runs 44 diagnostic queries |
| `warehouse_analyze_query_patterns` | Focused analysis of slow/frequent/failed queries |
| `eventhouse_optimization_recommendations` | Runs KQL diagnostics on each KQL database |
| `semantic_model_optimization_recommendations` | Executes DAX + MDSCHEMA DMVs for BPA analysis |
| `gateway_optimization_recommendations` | Scans all gateways and connections for health, usage, and security |

### Data Sources

```
                          ┌─────────────────────────────────────┐
                          │         Fabric REST API             │
                          │  Workspaces, Items, Gateways,       │
                          │  Connections, Metadata              │
                          └──────────────┬──────────────────────┘
                                         │
          ┌──────────────┬───────────────┼───────────────┬──────────────┬──────────────┐
          ▼              ▼               ▼               ▼              ▼              ▼
   ┌─────────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐
   │  SQL Client │ │ KQL REST │ │ OneLake ADLS │ │ DAX API  │ │ MDSCHEMA DMV │ │ Power BI API │
   │  (tedious)  │ │   API    │ │  Gen2 API    │ │executeQry│ │  via REST    │ │  Gateways    │
   └──────┬──────┘ └────┬─────┘ └──────┬───────┘ └────┬─────┘ └──────┬───────┘ └──────┬───────┘
          │              │              │              │              │              │
    Lakehouse SQL   Eventhouse    Delta Log JSON   Semantic     Semantic        Gateway
    Warehouse SQL   KQL DBs       File Metadata    Model DAX    Model Meta     Datasources
```

---

## Auto-Fix

All fix tools support **dry-run mode** (`dryRun: true`) to preview commands before execution.

### Warehouse Fixes — `warehouse_fix`

| Rule ID | What It Fixes | SQL Command |
|---------|--------------|-------------|
| WH-001 | Missing primary keys | `ALTER TABLE ADD CONSTRAINT PK NOT ENFORCED` |
| WH-008 | Stale statistics (>30 days) | `UPDATE STATISTICS [table]` |
| WH-009 | Disabled constraints | `ALTER TABLE WITH CHECK CHECK CONSTRAINT ALL` |
| WH-016 | Missing audit columns | `ALTER TABLE ADD created_at DATETIME2 DEFAULT GETDATE()` |
| WH-018 | Unmasked sensitive data | `ALTER COLUMN ADD MASKED WITH (FUNCTION='...')` |
| WH-026 | Auto-update statistics off | `ALTER DATABASE SET AUTO_UPDATE_STATISTICS ON` |
| WH-027 | Result set caching off | `ALTER DATABASE SET RESULT_SET_CACHING ON` |
| WH-028 | Snapshot isolation off | `ALTER DATABASE SET ALLOW_SNAPSHOT_ISOLATION ON` |
| WH-029 | Page verify not CHECKSUM | `ALTER DATABASE SET PAGE_VERIFY CHECKSUM` |
| WH-030 | ANSI settings off | `ALTER DATABASE SET ANSI_NULLS ON; ...` |
| WH-032 | Missing statistics | `UPDATE STATISTICS [table]` |
| WH-036 | NOT NULL without defaults | `ALTER TABLE ADD DEFAULT ... FOR column` |
| WH-040 | Auto-create statistics off | `ALTER DATABASE SET AUTO_CREATE_STATISTICS ON` |
| WH-041 | Query Store off | `ALTER DATABASE SET QUERY_STORE = ON` |
| WH-044 | FK columns missing indexes | `CREATE NONCLUSTERED INDEX ON [table]([col])` |

### Eventhouse Fixes — `eventhouse_fix`

| Rule ID | What It Fixes | KQL Command |
|---------|--------------|-------------|
| EH-002 | Fragmented extents | `.merge table ['name']` |
| EH-004 | Missing caching policy | `.alter database policy caching hot = 30d` |
| EH-005 | Missing retention policy | `.alter database policy retention softdelete = 365d` |
| EH-006 | Unhealthy materialized views | `.enable materialized-view ['name']` |
| EH-014 | Missing ingestion batching | `.alter database policy ingestionbatching ...` |
| EH-016 | Large tables without partitioning | `.alter table policy partitioning ...` |
| EH-017 | Suboptimal merge policy | `.alter table policy merge ...` |
| EH-021 | Autocompaction disabled | `.alter database policy autocompaction ...` |
| EH-022 | Extent tags retention missing | `.alter database policy extent_tags_retention ...` |
| EH-024 | High-volume tables without streaming | `.alter table policy streamingingestion enable` |
| EH-025 | Stale materialized views | `.refresh materialized-view ['name']` |

### Lakehouse Fixes — `lakehouse_fix` / `lakehouse_auto_optimize`

Executes Spark SQL via **Livy API** (no notebook needed). Falls back to temporary notebook if Livy is unavailable.

| Fix ID | What It Fixes | Spark SQL |
|--------|--------------|-----------|
| auto-optimize | Auto-optimize disabled | `SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')` |
| retention | No log retention policy | `SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days')` |
| data-skipping | Data skipping not configured | `SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols'='32')` |
| audit-columns | Missing created_at/updated_at | `ADD COLUMNS (created_at TIMESTAMP, updated_at TIMESTAMP)` |
| v-order | V-Order compression disabled | `SET TBLPROPERTIES ('delta.parquet.vorder.enabled'='true')` |
| change-data-feed | Change Data Feed not enabled | `SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')` |
| column-mapping | Column mapping disabled | `SET TBLPROPERTIES ('delta.columnMapping.mode'='name')` |
| checkpoint-interval | Checkpoint interval too high | `SET TBLPROPERTIES ('delta.checkpointInterval'='10')` |
| deletion-vectors | Deletion vectors not enabled | `SET TBLPROPERTIES ('delta.enableDeletionVectors'='true')` |
| compute-stats | Statistics missing or stale | `ANALYZE TABLE ... COMPUTE STATISTICS` |

### Semantic Model Fixes — `semantic_model_fix`

Uses XMLA/TMSL for atomic per-object changes. Falls back to BIM/TMDL download-modify-upload if XMLA is unavailable.

| Fix ID | What It Fixes |
|--------|--------------|
| SM-FIX-FORMAT | Add format strings to unformatted measures |
| SM-FIX-DESC | Add descriptions to visible tables |
| SM-FIX-HIDDEN | Set IsAvailableInMDX=false on hidden columns |
| SM-FIX-DATE | Mark date/calendar tables as Date table |
| SM-FIX-KEY | Set IsKey=true on PK columns in relationships |
| SM-FIX-AUTODATE | Remove auto-date tables |
| SM-FIX-IFERROR | Replace IFERROR with IF(ISERROR()) |
| SM-FIX-EVALLOG | Strip EVALUATEANDLOG debug wrappers |
| SM-FIX-ADDZERO | Remove +0 anti-pattern from measures |
| SM-FIX-DIRECTREF | Remove duplicate direct-reference measures |
| SM-FIX-SUMX | Replace SUMX(T, T[Col]) with SUM(T[Col]) |
| SM-FIX-MEASUREDESC | Auto-generate measure descriptions |
| SM-FIX-MEASURENAME | Clean whitespace/tabs from measure names |
| SM-FIX-HIDEDESC | Hide description/comment columns |
| SM-FIX-HIDEGUID | Hide GUID/UUID columns |
| SM-FIX-CONSTCOL | Remove constant columns (1 distinct value) |
| SM-FIX-REMOVEFILTERS | Replace ALL() with REMOVEFILTERS() |
| SM-FIX-BIDI | Switch bidirectional cross-filters to single direction |
| SM-FIX-SUMMARIZE | Set SummarizeBy=None on implicit measure columns |

### Gateway Fixes — `gateway_fix`

| Rule ID | What It Fixes | API Action |
|---------|--------------|------------|
| GW-004 | Unused datasources (no bindings) | `DELETE /gateways/{id}/datasources/{id}` |
| GW-006 | Excessive admins (>5 per datasource) | `DELETE /gateways/{id}/datasources/{id}/users/{email}` |
| GW-008 | Orphaned cloud connections | `DELETE /v1/connections/{id}` |
| GW-010 | Duplicate datasources on same gateway | `DELETE` duplicate datasource |

---

## Rule Reference

### Summary

| Category | Rules | Auto-Fixable | Scan Method |
|----------|-------|-------------|-------------|
| Lakehouse | 35 | 20 | SQL + Delta Log + Livy |
| Warehouse | 44 | 16 | SQL DDL |
| Eventhouse | 27/db | 11 | KQL management commands |
| Semantic Model | 41 | 19 | DAX + DMV + XMLA/TMSL |
| Gateway | 12 | 4 | Fabric + Power BI REST |
| **Total** | **158+** | **67** | |

<details>
<summary><strong>Lakehouse — 35 Rules</strong> (click to expand)</summary>

| # | Rule | Category | Severity | Fixable |
|---|------|----------|----------|---------|
| LH-001 | SQL Endpoint Active | Availability | HIGH | — |
| LH-002 | Medallion Architecture Naming | Maintainability | LOW | — |
| LH-003 | All Tables Use Delta Format | Performance | HIGH | Spark |
| LH-004 | Table Maintenance Recommended | Performance | MEDIUM | REST |
| LH-005 | No Empty Tables | Data Quality | MEDIUM | Spark |
| LH-006 | No Over-Provisioned String Columns | Performance | MEDIUM | — |
| LH-007 | Key Columns Are NOT NULL | Data Quality | HIGH | — |
| LH-008 | No Float/Real Precision Issues | Data Quality | MEDIUM | — |
| LH-009 | Column Naming Convention | Maintainability | LOW | Spark |
| LH-010 | Date Columns Use Proper Types | Data Quality | MEDIUM | — |
| LH-011 | Numeric Columns Use Proper Types | Data Quality | MEDIUM | — |
| LH-012 | No Excessively Wide Tables | Maintainability | LOW | — |
| LH-013 | Schema Has NOT NULL Constraints | Data Quality | MEDIUM | — |
| LH-014 | Tables Have Audit Columns | Maintainability | LOW | Livy |
| LH-015 | Consistent Date Types Per Table | Data Quality | LOW | — |
| LH-016 | Large Tables Are Partitioned | Performance | MEDIUM | — |
| LH-017 | Regular VACUUM Executed | Maintenance | MEDIUM | REST |
| LH-018 | Regular OPTIMIZE Executed | Performance | MEDIUM | REST |
| LH-019 | No Small File Problem | Performance | HIGH | REST |
| LH-020 | Auto-Optimize Enabled | Performance | MEDIUM | Livy |
| LH-021 | Retention Policy Configured | Maintenance | LOW | Livy |
| LH-022 | Delta Log Version Count Reasonable | Performance | LOW | REST |
| LH-023 | Low Write Amplification | Performance | MEDIUM | — |
| LH-024 | Data Skipping Configured | Performance | LOW | Livy |
| LH-025 | Z-Order on Large Tables | Performance | MEDIUM | REST |
| LH-026 | V-Order Enabled | Performance | MEDIUM | Livy |
| LH-027 | Change Data Feed on Large Tables | Data Management | LOW | Livy |
| LH-028 | Column Mapping Enabled | Maintainability | LOW | Livy |
| LH-029 | Deletion Vectors Enabled | Performance | LOW | Livy |
| LH-030 | Checkpoint Interval Appropriate | Performance | LOW | Livy |
| LH-031 | No Deeply Nested Types | Performance | LOW | — |
| LH-S01 | No Unprotected Sensitive Data | Security | HIGH | — |
| LH-S02 | Large Tables Identified | Performance | INFO | — |
| LH-S03 | No Deprecated Data Types | Maintainability | HIGH | — |
| LH-S04 | All Tables Have Key Columns | Data Quality | MEDIUM | Spark |

</details>

<details>
<summary><strong>Warehouse — 44 Rules</strong> (click to expand)</summary>

| # | Rule | Category | Severity | Fixable |
|---|------|----------|----------|---------|
| WH-001 | Primary Keys Defined | Data Quality | HIGH | SQL |
| WH-002 | No Deprecated Data Types | Maintainability | HIGH | — |
| WH-003 | No Float/Real Precision Issues | Data Quality | MEDIUM | — |
| WH-004 | No Over-Provisioned Columns | Performance | MEDIUM | — |
| WH-005 | Column Naming Convention | Maintainability | LOW | — |
| WH-006 | Table Naming Convention | Maintainability | LOW | — |
| WH-007 | No SELECT * in Views | Maintainability | LOW | — |
| WH-008 | Statistics Are Fresh | Performance | MEDIUM | SQL |
| WH-009 | No Disabled Constraints | Data Quality | MEDIUM | SQL |
| WH-010 | Key Columns Are NOT NULL | Data Quality | HIGH | — |
| WH-011 | No Empty Tables | Maintainability | MEDIUM | — |
| WH-012 | No Excessively Wide Tables | Maintainability | MEDIUM | — |
| WH-013 | Consistent Date Types | Data Quality | LOW | — |
| WH-014 | Foreign Keys Defined | Maintainability | MEDIUM | — |
| WH-015 | No Large BLOB Columns | Performance | MEDIUM | — |
| WH-016 | Tables Have Audit Columns | Maintainability | LOW | SQL |
| WH-017 | No Circular Foreign Keys | Data Quality | HIGH | — |
| WH-018 | Sensitive Data Protected | Security | HIGH | SQL |
| WH-019 | Row-Level Security | Security | MEDIUM | — |
| WH-020 | Minimal db_owner Privileges | Security | MEDIUM | — |
| WH-021 | No Over-Complex Views | Maintainability | LOW | — |
| WH-022 | Minimal Cross-Schema Dependencies | Maintainability | LOW | — |
| WH-023 | No Very Slow Queries | Performance | HIGH | — |
| WH-024 | No Frequently Slow Queries | Performance | HIGH | — |
| WH-025 | No Recent Query Failures | Reliability | MEDIUM | — |
| WH-026 | AUTO_UPDATE_STATISTICS Enabled | Performance | HIGH | SQL |
| WH-027 | Result Set Caching Enabled | Performance | MEDIUM | SQL |
| WH-028 | Snapshot Isolation Enabled | Concurrency | MEDIUM | SQL |
| WH-029 | Page Verify CHECKSUM | Reliability | MEDIUM | SQL |
| WH-030 | ANSI Settings Correct | Standards | LOW | SQL |
| WH-031 | Database ONLINE | Availability | HIGH | — |
| WH-032 | All Tables Have Statistics | Performance | MEDIUM | SQL |
| WH-033 | Optimal Data Types | Performance | MEDIUM | — |
| WH-034 | No Near-Empty Tables | Maintainability | LOW | — |
| WH-035 | Stored Procedures Documented | Maintainability | LOW | — |
| WH-036 | NOT NULL Columns Have Defaults | Data Quality | MEDIUM | SQL |
| WH-037 | Consistent String Types | Maintainability | LOW | — |
| WH-038 | Schemas Are Documented | Maintainability | LOW | — |
| WH-039 | Query Performance Healthy | Performance | MEDIUM | — |
| WH-040 | AUTO_CREATE_STATISTICS Enabled | Performance | HIGH | SQL |
| WH-041 | Query Store Enabled | Performance | MEDIUM | SQL |
| WH-042 | No Excessive Computed Columns | Maintainability | LOW | — |
| WH-043 | No Forced Query Hints | Performance | LOW | — |
| WH-044 | FK Columns Have Indexes | Performance | MEDIUM | SQL |

</details>

<details>
<summary><strong>Eventhouse — 27 Rules per KQL Database</strong> (click to expand)</summary>

| # | Rule | Category | Severity | Fixable |
|---|------|----------|----------|---------|
| EH-001 | Query Endpoint Available | Availability | HIGH | — |
| EH-002 | No Extent Fragmentation | Performance | HIGH | KQL |
| EH-003 | Good Compression Ratio | Performance | MEDIUM | — |
| EH-004 | Caching Policy Configured | Performance | MEDIUM | KQL |
| EH-005 | Retention Policy Configured | Data Management | MEDIUM | KQL |
| EH-006 | Materialized Views Healthy | Reliability | HIGH | KQL |
| EH-007 | Data Is Fresh | Data Quality | MEDIUM | — |
| EH-008 | No Slow Query Patterns | Performance | HIGH | — |
| EH-009 | No Recent Failed Commands | Reliability | MEDIUM | — |
| EH-010 | No Ingestion Failures | Reliability | HIGH | — |
| EH-011 | Streaming Ingestion Config | Performance | INFO | — |
| EH-012 | Continuous Exports Healthy | Reliability | MEDIUM | — |
| EH-013 | Hot Cache Coverage | Performance | MEDIUM | — |
| EH-014 | Ingestion Batching Configured | Performance | LOW | KQL |
| EH-015 | Update Policies Configured | Data Management | INFO | — |
| EH-016 | Partitioning on Large Tables | Performance | MEDIUM | KQL |
| EH-017 | Merge Policy Configured | Performance | LOW | KQL |
| EH-018 | Encoding Policy for Poorly Compressed | Performance | MEDIUM | — |
| EH-019 | Row Order Policy | Performance | LOW | — |
| EH-020 | Stored Functions Inventory | Data Management | INFO | — |
| EH-021 | Autocompaction Policy Enabled | Performance | MEDIUM | KQL |
| EH-022 | Extent Tags Retention Configured | Data Management | LOW | KQL |
| EH-023 | Sharding Policy Configured | Performance | INFO | — |
| EH-024 | Streaming on High-Volume Tables | Performance | LOW | KQL |
| EH-025 | Materialized Views Fresh | Data Quality | MEDIUM | KQL |
| EH-026 | Query Volume Health | Performance | INFO | — |
| EH-027 | Ingestion Latency Within SLA | Data Quality | MEDIUM | — |

</details>

<details>
<summary><strong>Semantic Model — 41 Rules</strong> (click to expand)</summary>

| # | Rule | Category | Severity | Fixable |
|---|------|----------|----------|---------|
| SM-001 | Avoid IFERROR Function | DAX | MEDIUM | XMLA |
| SM-002 | Use DIVIDE Function | DAX | MEDIUM | — |
| SM-003 | No EVALUATEANDLOG in Production | DAX | HIGH | XMLA |
| SM-004 | Use TREATAS not INTERSECT | DAX | MEDIUM | — |
| SM-005 | No Duplicate Measure Definitions | DAX | LOW | — |
| SM-006 | Filter by Columns Not Tables | DAX | MEDIUM | — |
| SM-007 | Avoid Adding 0 to Measures | DAX | LOW | XMLA |
| SM-008 | Measures Have Documentation | Maintenance | LOW | XMLA |
| SM-009 | Model Has Tables | Maintenance | HIGH | — |
| SM-010 | Model Has Date Table | Performance | MEDIUM | XMLA |
| SM-011 | Avoid 1-(x/y) Syntax | DAX | MEDIUM | — |
| SM-012 | No Direct Measure References | DAX | LOW | XMLA |
| SM-013 | Avoid Nested CALCULATE | DAX | MEDIUM | — |
| SM-014 | Use SUM Instead of SUMX | DAX | LOW | XMLA |
| SM-015 | Measures Have Format String | Formatting | LOW | XMLA |
| SM-016 | Avoid FILTER(ALL(...)) | DAX | MEDIUM | — |
| SM-017 | Measure Naming Convention | Formatting | LOW | XMLA |
| SM-018 | Reasonable Table Count | Performance | LOW | — |
| SM-021 | Bidirectional Cross-Filter Overuse | Performance | MEDIUM | XMLA |
| SM-022 | No Implicit Measures | DAX | MEDIUM | XMLA |
| SM-023 | No Disconnected Tables | Data Modeling | MEDIUM | — |
| SM-024 | Use REMOVEFILTERS() not ALL() | DAX | LOW | XMLA |
| SM-025 | No Excessive USERELATIONSHIP | DAX | LOW | — |
| SM-026 | No Complex Relationship Webs | Data Modeling | LOW | — |
| SM-027 | Inactive Relationships Documented | Data Modeling | LOW | — |
| SM-028 | All Measures Have Format Strings | Usability | LOW | XMLA |
| SM-029 | No Pseudo-Hierarchies | Usability | INFO | — |
| SM-B01 | No High Cardinality Text Columns | BPA | HIGH | — |
| SM-B02 | No Description/Comment Columns | BPA | HIGH | XMLA |
| SM-B03 | No GUID/UUID Columns | BPA | HIGH | XMLA |
| SM-B04 | No Constant Columns | BPA | MEDIUM | XMLA |
| SM-B05 | No Booleans Stored as Text | BPA | MEDIUM | — |
| SM-B06 | No Dates Stored as Text | BPA | MEDIUM | — |
| SM-B07 | No Numbers Stored as Text | BPA | MEDIUM | — |
| SM-B08 | Integer Keys Not String Keys | BPA | MEDIUM | — |
| SM-B09 | No Excessively Wide Tables | BPA | MEDIUM | — |
| SM-B10 | No Extremely Wide Tables | BPA | HIGH | — |
| SM-B11 | No Multiple High-Cardinality Columns | BPA | HIGH | — |
| SM-B12 | No Single Column Tables | BPA | LOW | — |
| SM-B13 | No High-Precision Timestamps | BPA | MEDIUM | — |
| SM-B14 | No Low Cardinality in Fact Tables | BPA | LOW | — |

</details>

<details>
<summary><strong>Gateway — 12 Rules</strong> (click to expand)</summary>

| # | Rule | Category | Severity | Fixable |
|---|------|----------|----------|---------|
| GW-001 | Gateway Online | Availability | HIGH | — |
| GW-002 | Gateway Version Current | Maintenance | MEDIUM | — |
| GW-003 | No Unused Gateways | Hygiene | MEDIUM | — |
| GW-004 | No Unused Datasources | Hygiene | MEDIUM | REST |
| GW-005 | Datasource Connectivity Healthy | Availability | HIGH | — |
| GW-006 | No Excessive Admins | Security | MEDIUM | REST |
| GW-007 | Credentials Not Expired | Security | HIGH | — |
| GW-008 | No Orphaned Cloud Connections | Hygiene | MEDIUM | REST |
| GW-009 | VNet Gateway Configured | Configuration | MEDIUM | — |
| GW-010 | No Duplicate Datasources | Hygiene | LOW | REST |
| GW-011 | Privacy Level Configured | Security | LOW | — |
| GW-012 | Connections Have Display Names | Maintainability | LOW | — |

</details>

---

## Architecture

```
src/
├── index.ts                    MCP server entry point (stdio transport)
├── auth/
│   └── fabricAuth.ts           Azure AD auth (6 methods, token caching)
├── clients/
│   ├── fabricClient.ts         Fabric REST API + Power BI API + DAX + model CRUD
│   ├── sqlClient.ts            SQL via tedious (Lakehouse + Warehouse)
│   ├── kqlClient.ts            KQL/Kusto REST API (Eventhouse)
│   ├── livyClient.ts           Livy Spark API (Lakehouse fixes)
│   ├── onelakeClient.ts        OneLake ADLS Gen2 + Delta Log parser
│   └── xmlaClient.ts           XMLA/TMSL SOAP client (Semantic Model fixes)
└── tools/
    ├── ruleEngine.ts           Shared RuleResult type + unified report renderer
    ├── auth.ts                 auth_login, auth_status, auth_logout
    ├── workspace.ts            workspace_list, capacity_info, optimization_report
    ├── lakehouse.ts            35 rules + 10 Livy fixes + table maintenance
    ├── warehouse.ts            44 rules + 16 SQL fixes
    ├── eventhouse.ts           27 rules + 11 KQL fixes + materialized view repair
    ├── semanticModel.ts        41 rules + 19 XMLA/TMSL fixes
    └── gateway.ts              12 rules + 4 REST API fixes
```

## Authentication

| Method | Use Case |
|--------|----------|
| `azure_cli` | **Recommended** — uses your existing `az login` session |
| `interactive_browser` | Opens browser for interactive login |
| `device_code` | Headless or remote environments |
| `vscode` | Uses VS Code Azure account |
| `service_principal` | CI/CD pipelines (requires tenantId, clientId, clientSecret) |
| `default` | Auto-detect best available method |

## License

MIT
