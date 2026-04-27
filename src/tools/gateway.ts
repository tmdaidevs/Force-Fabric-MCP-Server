import {
  listGateways,
  listConnections,
  listGatewayDatasources,
  getGatewayDatasourceStatus,
  listGatewayDatasourceUsers,
  deleteGatewayDatasource,
  deleteGatewayDatasourceUser,
  deleteConnection,
} from "../clients/fabricClient.js";
import { renderRuleReport } from "./ruleEngine.js";
import type { RuleResult } from "./ruleEngine.js";
import type {
  FabricGateway,
  FabricConnection,
  GatewayDatasource,
  GatewayDatasourceUser,
} from "../clients/fabricClient.js";

// ──────────────────────────────────────────────
// Tool: gateway_list
// ──────────────────────────────────────────────

export async function gatewayList(): Promise<string> {
  const gateways = await listGateways();

  if (gateways.length === 0) {
    return "No gateways found.";
  }

  const lines = gateways.map((gw: FabricGateway) =>
    [
      `- **${gw.displayName}** (ID: ${gw.id})`,
      `  Type: ${gw.type}`,
      gw.gatewayStatus ? `  Status: ${gw.gatewayStatus}` : null,
      gw.version ? `  Version: ${gw.version}` : null,
      gw.virtualNetworkAzureResource
        ? `  VNet: ${gw.virtualNetworkAzureResource.virtualNetworkName}/${gw.virtualNetworkAzureResource.subnetName}`
        : null,
    ]
      .filter(Boolean)
      .join("\n")
  );

  return `## Gateways\n\n${lines.join("\n\n")}`;
}

// ──────────────────────────────────────────────
// Tool: gateway_list_connections
// ──────────────────────────────────────────────

export async function gatewayListConnections(): Promise<string> {
  const connections = await listConnections();

  if (connections.length === 0) {
    return "No connections found.";
  }

  const lines = connections.map((conn: FabricConnection) =>
    [
      `- **${conn.displayName ?? "(unnamed)"}** (ID: ${conn.id})`,
      `  Connectivity Type: ${conn.connectivityType}`,
      conn.gatewayId ? `  Gateway ID: ${conn.gatewayId}` : null,
      conn.privacyLevel ? `  Privacy Level: ${conn.privacyLevel}` : null,
    ]
      .filter(Boolean)
      .join("\n")
  );

  return `## Connections\n\n${lines.join("\n\n")}`;
}

// ──────────────────────────────────────────────
// Tool: gateway_optimization_recommendations
// ──────────────────────────────────────────────

interface GatewayDiagnostics {
  gateways: FabricGateway[];
  connections: FabricConnection[];
  datasourcesByGateway: Map<string, GatewayDatasource[]>;
  usersByDatasource: Map<string, GatewayDatasourceUser[]>;
  statusByDatasource: Map<string, string>;
}

async function collectDiagnostics(): Promise<GatewayDiagnostics> {
  const [gateways, connections] = await Promise.all([
    listGateways(),
    listConnections(),
  ]);

  const datasourcesByGateway = new Map<string, GatewayDatasource[]>();
  const usersByDatasource = new Map<string, GatewayDatasourceUser[]>();
  const statusByDatasource = new Map<string, string>();

  // Only fetch datasources for non-Personal gateways
  const eligibleGateways = gateways.filter((gw) => gw.type !== "Personal");

  for (const gw of eligibleGateways) {
    try {
      const datasources = await listGatewayDatasources(gw.id);
      datasourcesByGateway.set(gw.id, datasources);

      for (const ds of datasources) {
        const dsKey = `${gw.id}|${ds.id}`;
        try {
          const status = await getGatewayDatasourceStatus(gw.id, ds.id);
          statusByDatasource.set(dsKey, status);
        } catch {
          statusByDatasource.set(dsKey, "ERROR");
        }
        try {
          const users = await listGatewayDatasourceUsers(gw.id, ds.id);
          usersByDatasource.set(dsKey, users);
        } catch {
          usersByDatasource.set(dsKey, []);
        }
      }
    } catch {
      datasourcesByGateway.set(gw.id, []);
    }
  }

  return { gateways, connections, datasourcesByGateway, usersByDatasource, statusByDatasource };
}

function runGatewayRules(diag: GatewayDiagnostics): RuleResult[] {
  const rules: RuleResult[] = [];

  // GW-001: Gateway online
  for (const gw of diag.gateways) {
    if (gw.type === "Personal") continue;
    const status = (gw.gatewayStatus ?? "").toLowerCase();
    const isOnline = status === "live" || status === "online" || status === "";
    rules.push({
      id: "GW-001",
      rule: "Gateway online",
      category: "Availability",
      severity: "HIGH",
      status: isOnline ? "PASS" : "FAIL",
      details: isOnline
        ? `Gateway "${gw.displayName}" is online.`
        : `Gateway "${gw.displayName}" status: ${gw.gatewayStatus ?? "unknown"}.`,
      recommendation: isOnline ? undefined : "Check gateway service and network connectivity.",
    });
  }

  // GW-002: Gateway version current
  for (const gw of diag.gateways) {
    if (gw.type === "Personal" || !gw.version) continue;
    const parts = gw.version.split(".").map(Number);
    const major = parts[0] ?? 0;
    const isOld = major > 0 && major < 3000;
    // Heuristic: warn if version looks very old (month-based versions like 3000.xxx.xxx)
    const versionNum = parseInt(gw.version.replace(/\./g, ""), 10);
    const warnThreshold = isOld || versionNum < 300000;
    rules.push({
      id: "GW-002",
      rule: "Gateway version current",
      category: "Maintenance",
      severity: "MEDIUM",
      status: warnThreshold ? "WARN" : "PASS",
      details: `Gateway "${gw.displayName}" version: ${gw.version}.`,
      recommendation: warnThreshold ? "Update gateway to latest version from https://aka.ms/gateway." : undefined,
    });
  }

  // GW-003: No unused gateways (0 datasources)
  for (const gw of diag.gateways) {
    if (gw.type === "Personal") continue;
    const datasources = diag.datasourcesByGateway.get(gw.id) ?? [];
    rules.push({
      id: "GW-003",
      rule: "No unused gateways",
      category: "Governance",
      severity: "MEDIUM",
      status: datasources.length > 0 ? "PASS" : "WARN",
      details: datasources.length > 0
        ? `Gateway "${gw.displayName}" has ${datasources.length} datasource(s).`
        : `Gateway "${gw.displayName}" has no datasources.`,
      recommendation: datasources.length > 0 ? undefined : "Consider removing unused gateways to reduce management overhead.",
    });
  }

  // GW-004: No unused datasources — cross-ref with connections
  const connectedGatewayIds = new Set(diag.connections.filter((c) => c.gatewayId).map((c) => c.gatewayId));
  for (const gw of diag.gateways) {
    if (gw.type === "Personal") continue;
    const datasources = diag.datasourcesByGateway.get(gw.id) ?? [];
    for (const ds of datasources) {
      const hasConnection = connectedGatewayIds.has(gw.id);
      rules.push({
        id: "GW-004",
        rule: "No unused datasources",
        category: "Governance",
        severity: "LOW",
        status: hasConnection ? "PASS" : "WARN",
        details: hasConnection
          ? `Datasource "${ds.datasourceName ?? ds.id}" on "${gw.displayName}" is referenced.`
          : `Datasource "${ds.datasourceName ?? ds.id}" on "${gw.displayName}" has no matching connections.`,
        recommendation: hasConnection ? undefined : "Delete unused datasources with gateway_fix rule GW-004.",
      });
    }
  }

  // GW-005: Datasource connectivity healthy
  for (const [dsKey, status] of diag.statusByDatasource) {
    const [gatewayId, datasourceId] = dsKey.split("|");
    const gw = diag.gateways.find((g) => g.id === gatewayId);
    const ds = (diag.datasourcesByGateway.get(gatewayId!) ?? []).find((d) => d.id === datasourceId);
    const isOk = status === "OK";
    rules.push({
      id: "GW-005",
      rule: "Datasource connectivity healthy",
      category: "Availability",
      severity: "HIGH",
      status: isOk ? "PASS" : "FAIL",
      details: isOk
        ? `Datasource "${ds?.datasourceName ?? datasourceId}" on "${gw?.displayName ?? gatewayId}" is reachable.`
        : `Datasource "${ds?.datasourceName ?? datasourceId}" on "${gw?.displayName ?? gatewayId}": ${status}.`,
      recommendation: isOk ? undefined : "Check datasource credentials and network connectivity.",
    });
  }

  // GW-006: No excessive admins (>5 per datasource)
  for (const [dsKey, users] of diag.usersByDatasource) {
    const [gatewayId, datasourceId] = dsKey.split("|");
    const gw = diag.gateways.find((g) => g.id === gatewayId);
    const ds = (diag.datasourcesByGateway.get(gatewayId!) ?? []).find((d) => d.id === datasourceId);
    const adminCount = users.filter((u) => u.datasourceAccessRight === "Admin" || u.datasourceAccessRight === "ReadOverrideEffectiveIdentity").length;
    rules.push({
      id: "GW-006",
      rule: "No excessive admins",
      category: "Security",
      severity: "MEDIUM",
      status: adminCount <= 5 ? "PASS" : "WARN",
      details: adminCount <= 5
        ? `Datasource "${ds?.datasourceName ?? datasourceId}" on "${gw?.displayName ?? gatewayId}" has ${adminCount} admin(s).`
        : `Datasource "${ds?.datasourceName ?? datasourceId}" on "${gw?.displayName ?? gatewayId}" has ${adminCount} admins (>5).`,
      recommendation: adminCount <= 5 ? undefined : "Reduce admin users via gateway_fix rule GW-006.",
    });
  }

  // GW-007: Connection credentials check
  for (const conn of diag.connections) {
    const hasCreds = conn.credentialDetails && Object.keys(conn.credentialDetails).length > 0;
    rules.push({
      id: "GW-007",
      rule: "Connection credentials configured",
      category: "Security",
      severity: "MEDIUM",
      status: hasCreds ? "PASS" : "WARN",
      details: hasCreds
        ? `Connection "${conn.displayName ?? conn.id}" has credentials configured.`
        : `Connection "${conn.displayName ?? conn.id}" has no credential details.`,
      recommendation: hasCreds ? undefined : "Update connection to configure valid credentials.",
    });
  }

  // GW-008: No orphaned cloud connections
  const gatewayIds = new Set(diag.gateways.map((g) => g.id));
  for (const conn of diag.connections) {
    if (conn.connectivityType !== "ShareableCloud") continue;
    const isOrphaned = conn.gatewayId && !gatewayIds.has(conn.gatewayId);
    rules.push({
      id: "GW-008",
      rule: "No orphaned cloud connections",
      category: "Governance",
      severity: "LOW",
      status: isOrphaned ? "WARN" : "PASS",
      details: isOrphaned
        ? `Connection "${conn.displayName ?? conn.id}" references missing gateway ${conn.gatewayId}.`
        : `Connection "${conn.displayName ?? conn.id}" is properly bound.`,
      recommendation: isOrphaned ? "Delete orphaned connection with gateway_fix rule GW-008." : undefined,
    });
  }

  // GW-009: VNet gateway properly configured
  for (const gw of diag.gateways) {
    if (gw.type !== "VirtualNetwork") continue;
    const vnet = gw.virtualNetworkAzureResource;
    const fullyConfigured = vnet && vnet.subscriptionId && vnet.resourceGroupName && vnet.virtualNetworkName && vnet.subnetName;
    rules.push({
      id: "GW-009",
      rule: "VNet gateway configured",
      category: "Configuration",
      severity: "HIGH",
      status: fullyConfigured ? "PASS" : "FAIL",
      details: fullyConfigured
        ? `VNet gateway "${gw.displayName}" is fully configured (${vnet.virtualNetworkName}/${vnet.subnetName}).`
        : `VNet gateway "${gw.displayName}" is missing Azure resource configuration fields.`,
      recommendation: fullyConfigured ? undefined : "Complete VNet gateway configuration with subscription, resource group, VNet, and subnet.",
    });
  }

  // GW-010: No duplicate datasources
  for (const gw of diag.gateways) {
    if (gw.type === "Personal") continue;
    const datasources = diag.datasourcesByGateway.get(gw.id) ?? [];
    const seen = new Map<string, GatewayDatasource[]>();
    for (const ds of datasources) {
      const key = `${ds.datasourceType}|${ds.connectionDetails}`;
      const group = seen.get(key) ?? [];
      group.push(ds);
      seen.set(key, group);
    }
    for (const [, group] of seen) {
      if (group.length > 1) {
        rules.push({
          id: "GW-010",
          rule: "No duplicate datasources",
          category: "Governance",
          severity: "LOW",
          status: "WARN",
          details: `Gateway "${gw.displayName}" has ${group.length} duplicate ${group[0].datasourceType} datasources.`,
          recommendation: "Remove duplicates with gateway_fix rule GW-010.",
        });
      }
    }
  }

  // GW-011: Privacy level configured
  for (const conn of diag.connections) {
    const hasPrivacy = conn.privacyLevel && conn.privacyLevel !== "" && conn.privacyLevel.toLowerCase() !== "none";
    rules.push({
      id: "GW-011",
      rule: "Privacy level configured",
      category: "Security",
      severity: "LOW",
      status: hasPrivacy ? "PASS" : "WARN",
      details: hasPrivacy
        ? `Connection "${conn.displayName ?? conn.id}" privacy level: ${conn.privacyLevel}.`
        : `Connection "${conn.displayName ?? conn.id}" has no privacy level set.`,
      recommendation: hasPrivacy ? undefined : "Set an appropriate privacy level (Organizational, Private, or Public) on the connection.",
    });
  }

  // GW-012: All connections have display names
  for (const conn of diag.connections) {
    const hasName = conn.displayName && conn.displayName.trim() !== "";
    rules.push({
      id: "GW-012",
      rule: "Connection has display name",
      category: "Governance",
      severity: "LOW",
      status: hasName ? "PASS" : "WARN",
      details: hasName
        ? `Connection "${conn.displayName}" (${conn.id}) is named.`
        : `Connection ${conn.id} has no display name.`,
      recommendation: hasName ? undefined : "Add a descriptive display name to the connection for easier management.",
    });
  }

  return rules;
}

export async function gatewayOptimizationRecommendations(): Promise<string> {
  const diag = await collectDiagnostics();

  const headerSections: string[] = [
    `**Gateways scanned:** ${diag.gateways.length}`,
    `**Connections scanned:** ${diag.connections.length}`,
  ];

  const rules = runGatewayRules(diag);

  return renderRuleReport(
    "Gateway & Connection Optimization Report",
    new Date().toISOString(),
    headerSections,
    rules,
  );
}

// ──────────────────────────────────────────────
// Structured Fix Definitions
// ──────────────────────────────────────────────

interface GatewayFixDef {
  description: string;
  apply: (diag: GatewayDiagnostics, dryRun: boolean) => Promise<string[]>;
}

const GATEWAY_FIXES: Record<string, GatewayFixDef> = {
  "GW-004": {
    description: "Delete unused datasources (no matching connections)",
    apply: async (diag, dryRun) => {
      const results: string[] = [];
      const connectedGatewayIds = new Set(diag.connections.filter((c) => c.gatewayId).map((c) => c.gatewayId));

      for (const gw of diag.gateways) {
        if (gw.type === "Personal") continue;
        const datasources = diag.datasourcesByGateway.get(gw.id) ?? [];
        for (const ds of datasources) {
          if (!connectedGatewayIds.has(gw.id)) {
            if (dryRun) {
              results.push(`🔍 Would delete datasource "${ds.datasourceName ?? ds.id}" from gateway "${gw.displayName}"`);
            } else {
              try {
                await deleteGatewayDatasource(gw.id, ds.id);
                results.push(`✅ Deleted datasource "${ds.datasourceName ?? ds.id}" from gateway "${gw.displayName}"`);
              } catch (e) {
                results.push(`❌ Failed to delete datasource "${ds.datasourceName ?? ds.id}": ${e instanceof Error ? e.message : String(e)}`);
              }
            }
          }
        }
      }
      if (results.length === 0) results.push("No unused datasources found.");
      return results;
    },
  },

  "GW-006": {
    description: "Remove excess admin users (keep first 5)",
    apply: async (diag, dryRun) => {
      const results: string[] = [];

      for (const [dsKey, users] of diag.usersByDatasource) {
        const [gatewayId, datasourceId] = dsKey.split("|");
        const admins = users.filter((u) => u.datasourceAccessRight === "Admin" || u.datasourceAccessRight === "ReadOverrideEffectiveIdentity");
        if (admins.length <= 5) continue;

        const toRemove = admins.slice(5);
        for (const user of toRemove) {
          if (dryRun) {
            results.push(`🔍 Would remove admin "${user.emailAddress}" from datasource ${datasourceId}`);
          } else {
            try {
              await deleteGatewayDatasourceUser(gatewayId!, datasourceId!, user.emailAddress);
              results.push(`✅ Removed admin "${user.emailAddress}" from datasource ${datasourceId}`);
            } catch (e) {
              results.push(`❌ Failed to remove "${user.emailAddress}": ${e instanceof Error ? e.message : String(e)}`);
            }
          }
        }
      }
      if (results.length === 0) results.push("No excessive admins found.");
      return results;
    },
  },

  "GW-008": {
    description: "Delete orphaned cloud connections (referencing missing gateways)",
    apply: async (diag, dryRun) => {
      const results: string[] = [];
      const gatewayIds = new Set(diag.gateways.map((g) => g.id));

      for (const conn of diag.connections) {
        if (conn.connectivityType !== "ShareableCloud") continue;
        if (conn.gatewayId && !gatewayIds.has(conn.gatewayId)) {
          if (dryRun) {
            results.push(`🔍 Would delete orphaned connection "${conn.displayName ?? conn.id}" (references missing gateway ${conn.gatewayId})`);
          } else {
            try {
              await deleteConnection(conn.id);
              results.push(`✅ Deleted orphaned connection "${conn.displayName ?? conn.id}"`);
            } catch (e) {
              results.push(`❌ Failed to delete connection "${conn.displayName ?? conn.id}": ${e instanceof Error ? e.message : String(e)}`);
            }
          }
        }
      }
      if (results.length === 0) results.push("No orphaned connections found.");
      return results;
    },
  },

  "GW-010": {
    description: "Delete duplicate datasources (keep first, remove rest)",
    apply: async (diag, dryRun) => {
      const results: string[] = [];

      for (const gw of diag.gateways) {
        if (gw.type === "Personal") continue;
        const datasources = diag.datasourcesByGateway.get(gw.id) ?? [];
        const seen = new Map<string, GatewayDatasource[]>();
        for (const ds of datasources) {
          const key = `${ds.datasourceType}|${ds.connectionDetails}`;
          const group = seen.get(key) ?? [];
          group.push(ds);
          seen.set(key, group);
        }

        for (const [, group] of seen) {
          if (group.length <= 1) continue;
          const duplicates = group.slice(1);
          for (const dup of duplicates) {
            if (dryRun) {
              results.push(`🔍 Would delete duplicate datasource "${dup.datasourceName ?? dup.id}" from gateway "${gw.displayName}"`);
            } else {
              try {
                await deleteGatewayDatasource(gw.id, dup.id);
                results.push(`✅ Deleted duplicate datasource "${dup.datasourceName ?? dup.id}" from gateway "${gw.displayName}"`);
              } catch (e) {
                results.push(`❌ Failed to delete duplicate: ${e instanceof Error ? e.message : String(e)}`);
              }
            }
          }
        }
      }
      if (results.length === 0) results.push("No duplicate datasources found.");
      return results;
    },
  },
};

const FIXABLE_RULE_IDS = Object.keys(GATEWAY_FIXES);

// ──────────────────────────────────────────────
// Tool: gateway_fix — Auto-fix detected issues
// ──────────────────────────────────────────────

export async function gatewayFix(args: {
  ruleIds?: string[];
  dryRun?: boolean;
}): Promise<string> {
  const isDryRun = args.dryRun ?? false;
  const requestedRules = args.ruleIds ?? FIXABLE_RULE_IDS;

  // Validate rule IDs
  const invalidRules = requestedRules.filter((r) => !GATEWAY_FIXES[r]);
  if (invalidRules.length > 0) {
    return `❌ Unknown rule IDs: ${invalidRules.join(", ")}. Fixable rules: ${FIXABLE_RULE_IDS.join(", ")}`;
  }

  const diag = await collectDiagnostics();

  const lines: string[] = [
    `# 🔧 Gateway Fix: ${isDryRun ? "DRY RUN" : "Executing"}`,
    "",
    `_${new Date().toISOString()}_`,
    "",
    `**Rules to fix:** ${requestedRules.join(", ")}`,
    "",
  ];

  for (const ruleId of requestedRules) {
    const fix = GATEWAY_FIXES[ruleId];
    lines.push(`## ${ruleId}: ${fix.description}`, "");
    const results = await fix.apply(diag, isDryRun);
    lines.push(...results.map((r) => `- ${r}`), "");
  }

  return lines.join("\n");
}

// ──────────────────────────────────────────────
// Tool definitions for MCP registration
// ──────────────────────────────────────────────

export const gatewayTools = [
  {
    name: "gateway_list",
    description:
      "List all gateways with their status, version, type, and VNet configuration.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [] as string[],
    },
    handler: gatewayList,
  },
  {
    name: "gateway_list_connections",
    description:
      "List all connections with their connectivity type, gateway binding, and privacy level.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [] as string[],
    },
    handler: gatewayListConnections,
  },
  {
    name: "gateway_optimization_recommendations",
    description:
      "LIVE SCAN: Scans all gateways and connections with 12 rules covering availability (online status, connectivity), " +
      "security (credentials, excessive admins, privacy levels), governance (unused gateways/datasources, orphaned connections, " +
      "duplicates, display names), and configuration (VNet setup, version currency). Returns findings with prioritized action items.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [] as string[],
    },
    handler: gatewayOptimizationRecommendations,
  },
  {
    name: "gateway_fix",
    description:
      "AUTO-FIX: Applies fixes to gateway and connection issues. " +
      "Fixable rules: GW-004 (delete unused datasources), GW-006 (remove excess admins), " +
      "GW-008 (delete orphaned connections), GW-010 (delete duplicate datasources). " +
      "Use dryRun=true to preview changes without executing them.",
    inputSchema: {
      type: "object" as const,
      properties: {
        ruleIds: {
          type: "array",
          items: { type: "string" },
          description: "Rule IDs to fix: GW-004, GW-006, GW-008, GW-010",
        },
        dryRun: {
          type: "boolean",
          description: "If true, preview changes without executing them (default: false)",
        },
      },
      required: [] as string[],
    },
    handler: gatewayFix,
  },
];
