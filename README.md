<p align="center">
  <img src="banner.png" alt="Force Fabric MCP Server — Detect & Optimize" width="100%">
</p>

# Force Fabric MCP Server

A **Model Context Protocol (MCP) server** that provides live optimization analysis for Microsoft Fabric items. It connects to your Fabric tenant via Azure authentication and runs **100+ rules** across Lakehouses, Warehouses, Eventhouses, and Semantic Models — detecting real issues with specific table and column names.

## Features

### 🏠 Lakehouse Analysis (29 rules)
- **REST API**: SQL Endpoint status, Delta format check, medallion architecture naming
- **SQL Endpoint**: Data type analysis, nullable keys, empty tables, wide columns, naming conventions, audit columns, sensitive data
- **OneLake Delta Log**: VACUUM/OPTIMIZE history, auto-optimize settings, retention policies, file size analysis, write amplification, Z-Order, data skipping, partitioning

### 🏗️ Warehouse Analysis (39 rules)
- **Schema**: Primary keys, deprecated types, float precision, column/table naming, wide tables, foreign keys, circular FKs
- **Data Quality**: Nullable keys, empty tables, mixed date types, missing defaults, sensitive/PII columns
- **Query Performance**: Top slow queries, frequent queries, failed queries, volume trends, average duration
- **Security**: Data masking, RLS, privilege audit
- **Database Config**: AUTO_UPDATE_STATISTICS, result set caching, ANSI settings, snapshot isolation, page verify

### 📊 Eventhouse Analysis (13 rules per KQL database)
- **Storage**: Extent fragmentation, compression ratios, storage by table
- **Policies**: Caching, retention, merge, encoding, row order, partitioning, ingestion batching, streaming
- **Health**: Materialized views, data freshness, continuous exports, ingestion failures
- **Queries**: Performance summary (P95/avg/max), slow queries, failed commands

### 📐 Semantic Model Analysis (32 rules)
- **DAX Expression Checks** (via MDSCHEMA_MEASURES DMV): IFERROR, DIVIDE vs /, EVALUATEANDLOG, INTERSECT, duplicates, FILTER patterns, nested CALCULATE, SUMX, 1-(x/y), format strings
- **Model Structure** (via MDSCHEMA DMVs): Table count, date table, measure documentation, naming
- **COLUMNSTATISTICS BPA** (Import models): High-cardinality text, GUIDs, constants, boolean/date/number as text, string keys, wide tables, timestamps

## Setup

### Prerequisites

- **Node.js** 18+
- **Azure CLI** (`az login`) or another Azure authentication method
- **Fabric capacity** with items (Lakehouse, Warehouse, Eventhouse, or Semantic Model)

### Install

```bash
git clone https://github.com/tmdaidevs/Force-Fabric-MCP-Server.git
cd Force-Fabric-MCP-Server
npm install
npm run build
```

### Configure in VS Code

Add to your `.vscode/mcp.json`:

```json
{
  "servers": {
    "fabric-optimization": {
      "type": "stdio",
      "command": "node",
      "args": ["dist/index.js"],
      "cwd": "${workspaceFolder}"
    }
  }
}
```

Or add to your global VS Code settings (`settings.json`):

```json
{
  "mcp": {
    "servers": {
      "fabric-optimization": {
        "type": "stdio",
        "command": "node",
        "args": ["/absolute/path/to/Force-Fabric-MCP-Server/dist/index.js"]
      }
    }
  }
}
```

## Usage

### 1. Authenticate

```
Use auth_login with method "azure_cli"
```

Available methods: `azure_cli`, `interactive_browser`, `device_code`, `vscode`, `default`, `service_principal`

### 2. List items

```
List all lakehouses in workspace <workspace-id>
List all warehouses in workspace <workspace-id>
List all eventhouses in workspace <workspace-id>
List all semantic models in workspace <workspace-id>
```

### 3. Run optimization scan

```
Run lakehouse optimization recommendations for <lakehouse-id> in workspace <workspace-id>
Run warehouse optimization recommendations for <warehouse-id> in workspace <workspace-id>
Run eventhouse optimization recommendations for <eventhouse-id> in workspace <workspace-id>
Run semantic model optimization recommendations for <model-id> in workspace <workspace-id>
```

### Output Format

Every scan returns a unified rule results table:

```
15 rules — ✅ 9 passed | 🔴 1 failed | 🟡 5 warning

| Rule | Status | Finding | Recommendation |
|------|--------|---------|----------------|
| LH-007 Key Columns Are NOT NULL | 🔴 | 16 key column(s) allow NULL: ... | Add NOT NULL constraints... |
| LH-004 Table Maintenance | 🟡 | 4 Delta tables need OPTIMIZE... | Run lakehouse_run_table_maintenance... |
```

Only issues (FAIL/WARN) are shown in the table. Passed rules are counted in the summary.

## Available Tools

| Tool | Description |
|------|-------------|
| `auth_login` | Authenticate to Fabric |
| `auth_status` | Check authentication status |
| `auth_logout` | Disconnect |
| `workspace_list` | List all workspaces |
| `lakehouse_list` | List lakehouses in a workspace |
| `lakehouse_list_tables` | List tables in a lakehouse |
| `lakehouse_run_table_maintenance` | Run OPTIMIZE/VACUUM on tables |
| `lakehouse_get_job_status` | Check maintenance job status |
| `lakehouse_optimization_recommendations` | Full scan with 29 rules |
| `warehouse_list` | List warehouses in a workspace |
| `warehouse_optimization_recommendations` | Full scan with 39 rules |
| `warehouse_analyze_query_patterns` | Focused query performance analysis |
| `eventhouse_list` | List eventhouses in a workspace |
| `eventhouse_list_kql_databases` | List KQL databases |
| `eventhouse_optimization_recommendations` | Full scan with 13+ rules per KQL DB |
| `semantic_model_list` | List semantic models |
| `semantic_model_optimization_recommendations` | Full scan with 32 rules |

## Complete Rule Reference

### 🏠 Lakehouse Rules (29)

| Rule | Category | Severity | Description |
|------|----------|----------|-------------|
| LH-001 | Availability | HIGH | SQL Endpoint is active and provisioned |
| LH-002 | Maintainability | LOW | Lakehouse follows medallion naming (bronze/silver/gold) |
| LH-003 | Performance | HIGH | All tables use Delta format |
| LH-004 | Performance | MEDIUM | Delta tables have regular OPTIMIZE + VACUUM |
| LH-005 | Data Quality | MEDIUM | No empty tables |
| LH-006 | Performance | MEDIUM | No over-provisioned string columns (>500 chars) |
| LH-007 | Data Quality | HIGH | Key/ID columns are NOT NULL |
| LH-008 | Data Quality | MEDIUM | No float/real precision issues |
| LH-009 | Maintainability | LOW | Column naming convention (no spaces/special chars) |
| LH-010 | Data Quality | MEDIUM | Date columns use proper DATE/DATETIME2 types |
| LH-011 | Data Quality | MEDIUM | Numeric columns use proper numeric types |
| LH-012 | Maintainability | LOW | No excessively wide tables (>30 columns) |
| LH-013 | Data Quality | MEDIUM | Schema has NOT NULL constraints (not >90% nullable) |
| LH-014 | Maintainability | LOW | Tables have audit columns (created_at/updated_at) |
| LH-015 | Data Quality | LOW | Consistent date types per table |
| LH-S01 | Security | HIGH | No unprotected sensitive/PII columns |
| LH-S02 | Performance | INFO | Large tables (>1M rows) identified |
| LH-S03 | Maintainability | HIGH | No deprecated data types (TEXT/NTEXT/IMAGE) |
| LH-S04 | Data Quality | MEDIUM | All tables have key columns |
| LH-016 | Performance | MEDIUM | Large tables (>10GB) are partitioned |
| LH-017 | Maintenance | MEDIUM | Regular VACUUM executed (within 7 days) |
| LH-018 | Performance | MEDIUM | Regular OPTIMIZE executed |
| LH-019 | Performance | HIGH | No small file problem (>50% files <25MB) |
| LH-020 | Performance | MEDIUM | Auto-optimize enabled |
| LH-021 | Maintenance | LOW | Retention policy configured |
| LH-022 | Performance | LOW | Delta log version count reasonable (<100) |
| LH-023 | Performance | MEDIUM | Low write amplification (MERGE/UPDATE/DELETE ratio) |
| LH-024 | Performance | LOW | Data skipping configured |
| LH-025 | Performance | MEDIUM | Z-Order applied on large tables (>10GB) |

### 🏗️ Warehouse Rules (39)

| Rule | Category | Severity | Description |
|------|----------|----------|-------------|
| WH-001 | Data Quality | HIGH | Primary keys defined (NOT ENFORCED) |
| WH-002 | Maintainability | HIGH | No deprecated data types (TEXT/NTEXT/IMAGE) |
| WH-003 | Data Quality | MEDIUM | No float/real precision issues |
| WH-004 | Performance | MEDIUM | No over-provisioned columns (>500 chars) |
| WH-005 | Maintainability | LOW | Column naming convention |
| WH-006 | Maintainability | LOW | Table naming convention |
| WH-007 | Maintainability | LOW | No SELECT * in views |
| WH-008 | Performance | MEDIUM | Statistics are fresh (<30 days) |
| WH-009 | Data Quality | MEDIUM | No disabled/untrusted constraints |
| WH-010 | Data Quality | HIGH | Key columns are NOT NULL |
| WH-011 | Maintainability | MEDIUM | No empty tables |
| WH-012 | Maintainability | MEDIUM | No excessively wide tables (>50 columns) |
| WH-013 | Data Quality | LOW | Consistent date types per table |
| WH-014 | Maintainability | MEDIUM | Foreign keys defined |
| WH-015 | Performance | MEDIUM | No large BLOB/MAX columns |
| WH-016 | Maintainability | LOW | Tables have audit columns |
| WH-017 | Data Quality | HIGH | No circular foreign keys |
| WH-018 | Security | HIGH | Sensitive data protected (PII masking) |
| WH-019 | Security | MEDIUM | Row-Level Security defined |
| WH-020 | Security | MEDIUM | Minimal db_owner privileges |
| WH-021 | Maintainability | LOW | No over-complex views (>10 dependencies) |
| WH-022 | Maintainability | LOW | Minimal cross-schema dependencies |
| WH-023 | Performance | HIGH | No very slow queries (>60s) |
| WH-024 | Performance | HIGH | No frequently slow queries (>10x and >10s avg) |
| WH-025 | Reliability | MEDIUM | No recent query failures |
| WH-026 | Performance | HIGH | AUTO_UPDATE_STATISTICS enabled |
| WH-027 | Performance | MEDIUM | Result set caching enabled |
| WH-028 | Concurrency | MEDIUM | Snapshot isolation enabled |
| WH-029 | Reliability | MEDIUM | Page verify CHECKSUM |
| WH-030 | Standards | LOW | ANSI settings correct |
| WH-031 | Availability | HIGH | Database ONLINE |
| WH-032 | Performance | MEDIUM | All tables have statistics |
| WH-033 | Performance | MEDIUM | Optimal data types |
| WH-034 | Maintainability | LOW | No near-empty tables (<10 rows) |
| WH-035 | Maintainability | LOW | Stored procedures documented |
| WH-036 | Data Quality | MEDIUM | NOT NULL columns have defaults |
| WH-037 | Maintainability | LOW | Consistent string types (varchar/nvarchar) |
| WH-038 | Maintainability | LOW | Schemas are documented |
| WH-039 | Performance | MEDIUM | Query performance healthy (avg <5s) |

### 📊 Eventhouse Rules (13 per KQL Database)

| Rule | Category | Severity | Description |
|------|----------|----------|-------------|
| EH-001 | Availability | HIGH | Query endpoint available |
| EH-002 | Performance | HIGH | No extent fragmentation |
| EH-003 | Performance | MEDIUM | Good compression ratio (>40%) |
| EH-004 | Performance | MEDIUM | Caching policy configured |
| EH-005 | Data Management | MEDIUM | Retention policy configured |
| EH-006 | Reliability | HIGH | Materialized views healthy |
| EH-007 | Data Quality | MEDIUM | Data is fresh (<7 days) |
| EH-008 | Performance | HIGH | No slow query patterns (>30s avg) |
| EH-009 | Reliability | MEDIUM | No recent failed commands |
| EH-010 | Reliability | HIGH | No ingestion failures |
| EH-011 | Performance | INFO | Streaming ingestion config |
| EH-012 | Reliability | MEDIUM | Continuous exports healthy |
| EH-013 | Performance | MEDIUM | Hot cache coverage (>50% hot) |

### 📐 Semantic Model Rules (32)

| Rule | Category | Severity | Description |
|------|----------|----------|-------------|
| SM-001 | DAX | MEDIUM | Avoid IFERROR function |
| SM-002 | DAX | MEDIUM | Use DIVIDE function instead of / |
| SM-003 | DAX | HIGH | No EVALUATEANDLOG in production |
| SM-004 | DAX | MEDIUM | Use TREATAS not INTERSECT |
| SM-005 | DAX | LOW | No duplicate measure definitions |
| SM-006 | DAX | MEDIUM | Filter by columns not tables |
| SM-007 | DAX | LOW | Avoid adding 0 to measures |
| SM-008 | Maintenance | LOW | Measures have documentation |
| SM-009 | Maintenance | HIGH | Model has tables |
| SM-010 | Performance | MEDIUM | Model has date table |
| SM-011 | DAX | MEDIUM | Avoid 1-(x/y) syntax |
| SM-012 | DAX | LOW | No direct measure references |
| SM-013 | DAX | MEDIUM | Avoid nested CALCULATE |
| SM-014 | DAX | LOW | Use SUM instead of SUMX for simple aggregation |
| SM-015 | Formatting | LOW | Measures have format string |
| SM-016 | DAX | MEDIUM | Avoid FILTER(ALL(...)) |
| SM-017 | Formatting | LOW | Measure naming convention |
| SM-018 | Performance | LOW | Reasonable table count (<20) |
| SM-B01 | Data Types | HIGH | No high cardinality text columns |
| SM-B02 | Data Types | HIGH | No description/comment columns |
| SM-B03 | Data Types | HIGH | No GUID/UUID columns in model |
| SM-B04 | Data Types | MEDIUM | No constant columns (cardinality=1) |
| SM-B05 | Data Types | MEDIUM | No booleans stored as text |
| SM-B06 | Data Types | MEDIUM | No dates stored as text |
| SM-B07 | Data Types | MEDIUM | No numbers stored as text |
| SM-B08 | Data Types | MEDIUM | Integer keys instead of string keys |
| SM-B09 | Data Types | MEDIUM | No excessively wide tables |
| SM-B10 | Data Types | HIGH | No extremely wide tables (>100 cols) |
| SM-B11 | Data Types | HIGH | No multiple high-cardinality columns |
| SM-B12 | Data Types | LOW | No single column tables |
| SM-B13 | Data Types | MEDIUM | No high-precision timestamps |
| SM-B14 | Data Types | LOW | No low cardinality columns in fact tables |

## Architecture

```
src/
├── index.ts                 # MCP server entry point
├── auth/
│   └── fabricAuth.ts        # Azure authentication (CLI, browser, device code, SP)
├── clients/
│   ├── fabricClient.ts      # Fabric REST API + DAX executeQueries
│   ├── sqlClient.ts         # SQL endpoint via tedious (Lakehouse + Warehouse)
│   ├── kqlClient.ts         # KQL/Kusto REST API (Eventhouse)
│   ├── onelakeClient.ts     # OneLake ADLS Gen2 + Delta Log parser
│   └── xmlaClient.ts        # XMLA SOAP client (experimental)
└── tools/
    ├── ruleEngine.ts        # Shared RuleResult type + unified renderer
    ├── auth.ts              # Auth tools
    ├── workspace.ts         # Workspace tools
    ├── lakehouse.ts         # 29 rules (REST + SQL + Delta Log)
    ├── warehouse.ts         # 39 rules (SQL)
    ├── eventhouse.ts        # 13 rules per KQL DB (KQL)
    └── semanticModel.ts     # 32 rules (DAX + DMV + COLUMNSTATISTICS)
```

## Authentication Methods

| Method | Use Case |
|--------|----------|
| `azure_cli` | Development - uses your az login session |
| `interactive_browser` | Opens browser for interactive login |
| `device_code` | Headless/remote environments |
| `vscode` | Uses VS Code Azure account |
| `service_principal` | CI/CD and automation (requires tenantId, clientId, clientSecret) |
| `default` | Auto-detect (tries CLI, managed identity, env vars, VS Code) |

## License

MIT
