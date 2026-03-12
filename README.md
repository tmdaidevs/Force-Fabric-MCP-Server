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
| `eventhouse_optimization_recommendations` | Full scan with 13+ rules |
| `semantic_model_list` | List semantic models |
| `semantic_model_optimization_recommendations` | Full scan with 32 rules |

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
