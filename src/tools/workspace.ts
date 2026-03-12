import {
  listWorkspaces,
  getWorkspace,
  listWorkspaceItems,
  listCapacities,
  listLakehouses,
  listWarehouses,
  listEventhouses,
  listSemanticModels,
} from "../clients/fabricClient.js";
import type { FabricWorkspace, FabricItem } from "../clients/fabricClient.js";

// ──────────────────────────────────────────────
// Tool: workspace_list
// ──────────────────────────────────────────────

export async function workspaceList(): Promise<string> {
  const workspaces = await listWorkspaces();

  if (workspaces.length === 0) {
    return "No workspaces found. Ensure you have access to at least one Fabric workspace.";
  }

  const lines = workspaces.map((ws: FabricWorkspace) =>
    [
      `- **${ws.displayName}** (ID: ${ws.id})`,
      `  Type: ${ws.type}`,
      ws.capacityId ? `  Capacity: ${ws.capacityId}` : null,
      ws.description ? `  Description: ${ws.description}` : null,
    ].filter(Boolean).join("\n")
  );

  return `## Your Fabric Workspaces\n\nTotal: ${workspaces.length}\n\n${lines.join("\n\n")}`;
}

// ──────────────────────────────────────────────
// Tool: workspace_list_items
// ──────────────────────────────────────────────

export async function workspaceListItems(args: {
  workspaceId: string;
  itemType?: string;
}): Promise<string> {
  const [workspace, items] = await Promise.all([
    getWorkspace(args.workspaceId),
    listWorkspaceItems(args.workspaceId, args.itemType),
  ]);

  if (items.length === 0) {
    const filter = args.itemType ? ` of type "${args.itemType}"` : "";
    return `No items${filter} found in workspace "${workspace.displayName}".`;
  }

  // Group items by type
  const grouped = new Map<string, FabricItem[]>();
  for (const item of items) {
    const group = grouped.get(item.type) ?? [];
    group.push(item);
    grouped.set(item.type, group);
  }

  const sections: string[] = [];
  for (const [type, typeItems] of grouped) {
    const lines = typeItems.map((item) => `  - ${item.displayName} (ID: ${item.id})`);
    sections.push(`### ${type} (${typeItems.length})\n${lines.join("\n")}`);
  }

  return [
    `## Items in workspace "${workspace.displayName}"`,
    "",
    `Total: ${items.length} item(s)`,
    "",
    ...sections,
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool: workspace_capacity_info
// ──────────────────────────────────────────────

export async function workspaceCapacityInfo(): Promise<string> {
  const capacities = await listCapacities();

  if (capacities.length === 0) {
    return "No capacities found or you don't have access to view capacity information.";
  }

  const lines = capacities.map((cap) =>
    [
      `- **${cap.displayName}** (ID: ${cap.id})`,
      `  SKU: ${cap.sku}`,
      `  State: ${cap.state}`,
      `  Region: ${cap.region}`,
    ].join("\n")
  );

  return [
    "## Fabric Capacities",
    "",
    `Total: ${capacities.length}`,
    "",
    ...lines,
    "",
    "### Capacity Optimization Tips",
    "",
    "- **Right-size your capacity**: Monitor CU utilization in the Capacity Metrics app",
    "- **Use autoscale**: Enable capacity autoscale for bursty workloads",
    "- **Pause unused capacities**: Save costs by pausing dev/test capacities when not in use",
    "- **Smoothing**: Fabric smooths CU consumption over 24h windows — short spikes are acceptable",
    "- **Throttling**: If >100% utilization is sustained, background jobs are throttled, then interactive queries",
  ].join("\n");
}

// ──────────────────────────────────────────────
// Tool: fabric_optimization_report
// ──────────────────────────────────────────────

export async function fabricOptimizationReport(args: {
  workspaceId: string;
}): Promise<string> {
  const workspace = await getWorkspace(args.workspaceId);

  // Fetch all item types in parallel
  const [lakehouses, warehouses, eventhouses, semanticModels, allItems] = await Promise.all([
    listLakehouses(args.workspaceId).catch(() => []),
    listWarehouses(args.workspaceId).catch(() => []),
    listEventhouses(args.workspaceId).catch(() => []),
    listSemanticModels(args.workspaceId).catch(() => []),
    listWorkspaceItems(args.workspaceId).catch(() => []),
  ]);

  const report: string[] = [
    `# Fabric Optimization Report`,
    `## Workspace: ${workspace.displayName}`,
    "",
    `Generated: ${new Date().toISOString()}`,
    "",

    "---",
    "",
    "## 📋 Inventory Summary",
    "",
    `| Item Type | Count |`,
    `|-----------|-------|`,
    `| Lakehouses | ${lakehouses.length} |`,
    `| Warehouses | ${warehouses.length} |`,
    `| Eventhouses | ${eventhouses.length} |`,
    `| Semantic Models | ${semanticModels.length} |`,
    `| Total Items | ${allItems.length} |`,
    "",
  ];

  // Lakehouse section
  if (lakehouses.length > 0) {
    report.push(
      "---",
      "",
      "## 🏠 Lakehouse Optimization",
      "",
    );

    for (const lh of lakehouses) {
      report.push(
        `### ${lh.displayName}`,
        "",
        "**Action Items:**",
        "- [ ] Run OPTIMIZE with V-Order on all tables (`lakehouse_run_table_maintenance`)",
        "- [ ] Run VACUUM to clean up old files",
        "- [ ] Review partition strategy for tables > 1 GB",
        "- [ ] Check for small files problem (many files < 128 MB)",
        "- [ ] Verify Z-ORDER on frequently filtered columns",
        `- [ ] Use \`lakehouse_optimization_recommendations\` for detailed analysis`,
        "",
      );
    }
  }

  // Warehouse section
  if (warehouses.length > 0) {
    report.push(
      "---",
      "",
      "## 🏭 Warehouse Optimization",
      "",
    );

    for (const wh of warehouses) {
      report.push(
        `### ${wh.displayName}`,
        "",
        "**Action Items:**",
        "- [ ] Review Query Insights for slow/frequent queries",
        "- [ ] Verify statistics are up to date on key columns",
        "- [ ] Check for proper data types (narrow types preferred)",
        "- [ ] Review batch loading patterns (avoid small inserts)",
        "- [ ] Rebuild columnstore indexes after bulk modifications",
        `- [ ] Use \`warehouse_optimization_recommendations\` for detailed analysis`,
        "",
      );
    }
  }

  // Eventhouse section
  if (eventhouses.length > 0) {
    report.push(
      "---",
      "",
      "## ⚡ Eventhouse Optimization",
      "",
    );

    for (const eh of eventhouses) {
      report.push(
        `### ${eh.displayName}`,
        "",
        "**Action Items:**",
        "- [ ] Review caching policies — hot cache should cover common query ranges",
        "- [ ] Verify retention policies match data lifecycle requirements",
        "- [ ] Check ingestion batching configuration",
        "- [ ] Evaluate materialized views for common aggregation patterns",
        "- [ ] Review partitioning policy for large tables",
        "- [ ] Merge small extents if present",
        `- [ ] Use \`eventhouse_optimization_recommendations\` for detailed analysis`,
        "",
      );
    }
  }

  // Semantic Model section
  if (semanticModels.length > 0) {
    report.push(
      "---",
      "",
      "## 📊 Semantic Model Optimization",
      "",
    );

    for (const sm of semanticModels) {
      report.push(
        `### ${sm.displayName}`,
        "",
        "**Action Items:**",
        "- [ ] Remove unused columns to reduce model size",
        "- [ ] Verify star schema design (fact + dimension tables)",
        "- [ ] Consider DirectLake mode for Fabric-native access",
        "- [ ] Set up incremental refresh for large tables",
        "- [ ] Review DAX measures for optimization opportunities",
        "- [ ] Ensure integer surrogate keys for relationships",
        `- [ ] Use \`semantic_model_optimization_recommendations\` for detailed analysis`,
        "",
      );
    }
  }

  // General recommendations
  report.push(
    "---",
    "",
    "## 🎯 Cross-Cutting Recommendations",
    "",
    "### Capacity Management",
    "- Monitor CU consumption via the Capacity Metrics app",
    "- Schedule heavy jobs (refreshes, maintenance) during off-peak hours",
    "- Consider separate capacities for dev/test vs production",
    "",
    "### Data Architecture",
    "- Use **medallion architecture** (Bronze → Silver → Gold) in Lakehouses",
    "- Leverage **shortcuts** to avoid data duplication across workspaces",
    "- Use the **Warehouse** for complex SQL analytics, **Lakehouse** for data engineering",
    "",
    "### Security & Governance",
    "- Implement workspace-level access control",
    "- Use service principals for automated workloads",
    "- Enable data lineage tracking through Microsoft Purview",
    "",
    "### Cost Optimization",
    "- Right-size capacity SKU based on actual usage patterns",
    "- Pause development capacities after hours",
    "- Clean up unused items and old data to reduce storage costs",
    "- Use OneLake data compaction (V-Order) to reduce storage volume",
  );

  return report.join("\n");
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const workspaceTools = [
  {
    name: "workspace_list",
    description: "List all Fabric workspaces you have access to with their IDs, types, and capacity assignments.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
    handler: workspaceList,
  },
  {
    name: "workspace_list_items",
    description:
      "List all items in a Fabric workspace, optionally filtered by type (Lakehouse, Warehouse, " +
      "Notebook, Pipeline, SemanticModel, Report, etc.). Items are grouped by type.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace",
        },
        itemType: {
          type: "string",
          description:
            "Optional: filter by item type (e.g., Lakehouse, Warehouse, Notebook, SemanticModel, " +
            "Pipeline, Report, Eventhouse, KQLDatabase, Dashboard, Dataflow)",
        },
      },
      required: ["workspaceId"],
    },
    handler: workspaceListItems,
  },
  {
    name: "workspace_capacity_info",
    description:
      "List Fabric capacities with their SKU, state, and region. Includes capacity optimization tips.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
    handler: workspaceCapacityInfo,
  },
  {
    name: "fabric_optimization_report",
    description:
      "Generate a comprehensive optimization report for an entire Fabric workspace. " +
      "Scans all Lakehouses, Warehouses, Eventhouses, and Semantic Models and provides " +
      "a checklist of optimization action items for each item, plus cross-cutting recommendations " +
      "for capacity management, data architecture, security, and cost optimization.",
    inputSchema: {
      type: "object" as const,
      properties: {
        workspaceId: {
          type: "string",
          description: "The ID of the Fabric workspace to analyze",
        },
      },
      required: ["workspaceId"],
    },
    handler: fabricOptimizationReport,
  },
];
