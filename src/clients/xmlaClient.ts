import { getAccessToken } from "../auth/fabricAuth.js";
import { getTokenForScope } from "../auth/fabricAuth.js";
import { XMLParser } from "fast-xml-parser";

// ──────────────────────────────────────────────
// XMLA Client — connects to Fabric/Power BI
// Analysis Services XMLA endpoint for DMV queries
// ──────────────────────────────────────────────

const ANALYSIS_SCOPE = "https://analysis.windows.net/powerbi/api/.default";

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  textNodeName: "#text",
  isArray: (tagName) => tagName === "row" || tagName === "xsd:element",
  removeNSPrefix: true,
});

/**
 * Get the correct XMLA endpoint URL for a workspace.
 * Fabric uses: https://{region}.pbidedicated.windows.net/xmla?vs={workspace}
 * We discover this from the connection string.
 */
function getXmlaUrl(workspaceName: string): string {
  // The standard Fabric XMLA endpoint
  return `https://analysis.windows.net/powerbi/api/v1.0/myorg/${encodeURIComponent(workspaceName)}`;
}

/**
 * Execute a DMV query via XMLA SOAP over HTTP.
 */
export async function executeXmlaQuery(
  workspaceName: string,
  datasetName: string,
  query: string
): Promise<Record<string, unknown>[]> {
  const token = await getTokenForScope(ANALYSIS_SCOPE);
  const xmlaUrl = getXmlaUrl(workspaceName);

  const soapEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
  <Header>
    <BeginSession xmlns="urn:schemas-microsoft-com:xml-analysis" mustUnderstand="1"/>
  </Header>
  <Body>
    <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
      <Command>
        <Statement>${escapeXml(query)}</Statement>
      </Command>
      <Properties>
        <PropertyList>
          <Catalog>${escapeXml(datasetName)}</Catalog>
          <Format>Tabular</Format>
          <Content>Data</Content>
        </PropertyList>
      </Properties>
    </Execute>
  </Body>
</Envelope>`;

  const response = await fetch(xmlaUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/xml",
    },
    body: soapEnvelope,
  });

  if (!response.ok) {
    const errorText = (await response.text()).substring(0, 500);
    throw new Error(`XMLA query failed (${response.status}): ${errorText}`);
  }

  const xml = await response.text();
  return parseXmlaResponse(xml);
}

/**
 * Run multiple DMV queries and return named results.
 */
export async function runXmlaDmvQueries(
  workspaceName: string,
  datasetName: string,
  queries: Record<string, string>
): Promise<Record<string, { rows?: Record<string, unknown>[]; error?: string }>> {
  const results: Record<string, { rows?: Record<string, unknown>[]; error?: string }> = {};

  for (const [name, query] of Object.entries(queries)) {
    try {
      const rows = await executeXmlaQuery(workspaceName, datasetName, query);
      results[name] = { rows };
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      results[name] = { error: msg };
    }
  }

  return results;
}

// ──────────────────────────────────────────────
// XML Helpers
// ──────────────────────────────────────────────

function escapeXml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

/**
 * Parse XMLA SOAP response using fast-xml-parser.
 */
function parseXmlaResponse(xml: string): Record<string, unknown>[] {
  const parsed = xmlParser.parse(xml);

  // Navigate SOAP envelope
  const envelope = parsed?.Envelope ?? parsed?.["soap:Envelope"] ?? parsed;
  const body = envelope?.Body ?? envelope?.["soap:Body"] ?? envelope;

  // Check for SOAP Fault
  const fault = body?.Fault ?? body?.["soap:Fault"];
  if (fault) {
    const faultString = fault?.faultstring ?? fault?.detail?.Error?.["@_Description"] ?? "Unknown SOAP fault";
    throw new Error(`XMLA SOAP Fault: ${faultString}`);
  }

  // Navigate to the execute response
  const execResponse = body?.ExecuteResponse ?? body;
  const returnVal = execResponse?.return ?? execResponse;
  const root = returnVal?.root ?? returnVal;

  // Check for Analysis Services error
  const error = root?.Exception ?? root?.Messages?.Error;
  if (error) {
    const desc = Array.isArray(error) ? error[0]?.["@_Description"] : error?.["@_Description"];
    throw new Error(`XMLA Error: ${desc ?? "Unknown error"}`);
  }

  // Extract rows
  const rowData = root?.row;
  if (!rowData) return [];

  const rowArray = Array.isArray(rowData) ? rowData : [rowData];
  return rowArray.map((row: Record<string, unknown>) => {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (key.startsWith("@_") || key === "#text") continue;
      result[key] = typeof value === "object" && value !== null && "#text" in (value as Record<string, unknown>)
        ? (value as Record<string, unknown>)["#text"]
        : value;
    }
    return result;
  });
}
