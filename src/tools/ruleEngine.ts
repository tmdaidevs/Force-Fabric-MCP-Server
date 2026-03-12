// ──────────────────────────────────────────────
// Shared Rule Engine — Unified rule evaluation & rendering
// ──────────────────────────────────────────────

export type RuleSeverity = "HIGH" | "MEDIUM" | "LOW" | "INFO";
export type RuleStatus = "PASS" | "FAIL" | "WARN" | "N/A" | "ERROR";

export interface RuleResult {
  id: string;
  rule: string;
  category: string;
  severity: RuleSeverity;
  status: RuleStatus;
  details: string;
  recommendation?: string;
}

const STATUS_ICON: Record<RuleStatus, string> = {
  PASS: "✅",
  FAIL: "🔴",
  WARN: "🟡",
  "N/A": "⚪",
  ERROR: "⚠️",
};

const SEVERITY_ICON: Record<RuleSeverity, string> = {
  HIGH: "🔴",
  MEDIUM: "🟡",
  LOW: "🔵",
  INFO: "ℹ️",
};

/**
 * Render a complete rule results report — all output as tables.
 * 1. Summary counts
 * 2. Full rule results table (all rules)
 * 3. Issues table (FAIL/WARN only, with details + recommendation)
 */
export function renderRuleReport(
  title: string,
  scanTime: string,
  headerSections: string[],
  rules: RuleResult[]
): string {
  const lines: string[] = [
    `# 🔍 ${title}`,
    "",
    `_Live scan at ${scanTime}_`,
    "",
  ];

  // ── Header sections (endpoints, metadata, etc.) ──
  if (headerSections.length > 0) {
    lines.push(...headerSections, "");
  }

  // ── Summary ──
  const passCount = rules.filter(r => r.status === "PASS").length;
  const failCount = rules.filter(r => r.status === "FAIL").length;
  const warnCount = rules.filter(r => r.status === "WARN").length;
  const naCount = rules.filter(r => r.status === "N/A").length;
  const errCount = rules.filter(r => r.status === "ERROR").length;

  lines.push(
    `**${rules.length} rules** — ✅ ${passCount} passed | 🔴 ${failCount} failed | 🟡 ${warnCount} warning | ⚪ ${naCount} n/a${errCount > 0 ? ` | ⚠️ ${errCount} error` : ""}`,
    "",
  );

  // ── Results Table — only issues (FAIL/WARN/ERROR) ──
  const sevOrder: Record<RuleSeverity, number> = { HIGH: 0, MEDIUM: 1, LOW: 2, INFO: 3 };
  const statusOrder: Record<RuleStatus, number> = { FAIL: 0, ERROR: 1, WARN: 2, PASS: 3, "N/A": 4 };
  const issues = rules.filter(r => r.status === "FAIL" || r.status === "WARN" || r.status === "ERROR");

  if (issues.length === 0) {
    lines.push("✅ **All rules passed — no issues found!**");
    return lines.join("\n");
  }

  const sorted = [...issues].sort((a, b) => {
    const sd = statusOrder[a.status] - statusOrder[b.status];
    if (sd !== 0) return sd;
    return sevOrder[a.severity] - sevOrder[b.severity];
  });

  lines.push(
    "| Rule | Status | Finding | Recommendation |",
    "|------|--------|---------|----------------|",
  );

  for (const r of sorted) {
    const finding = r.details.replace(/\|/g, "\\|").replace(/\n/g, " ");
    const rec = (r.recommendation ?? "—").replace(/\|/g, "\\|").replace(/\n/g, " ");
    lines.push(
      `| ${r.id} ${r.rule} | ${STATUS_ICON[r.status]} ${SEVERITY_ICON[r.severity]} | ${finding} | ${rec} |`
    );
  }

  return lines.join("\n");
}
