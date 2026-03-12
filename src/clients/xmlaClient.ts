import { getAccessToken } from "../auth/fabricAuth.js";
import { getTokenForScope } from "../auth/fabricAuth.js";

// ──────────────────────────────────────────────
// XMLA Client — connects to Fabric/Power BI
// Analysis Services XMLA endpoint for DMV queries
// ──────────────────────────────────────────────

const ANALYSIS_SCOPE = "https://analysis.windows.net/powerbi/api/.default";

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

  const dataSource = `powerbi://api.powerbi.com/v1.0/myorg/${workspaceName}`;

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
    const errorText = await response.text();
    throw new Error(`XMLA query failed (${response.status}): ${errorText.substring(0, 500)}`);
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
 * Parse XMLA SOAP response into rows.
 * Handles the standard Analysis Services tabular response format.
 */
function parseXmlaResponse(xml: string): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];

  // Check for SOAP Fault
  const faultMatch = xml.match(/<faultstring[^>]*>([\s\S]*?)<\/faultstring>/);
  if (faultMatch) {
    throw new Error(`XMLA SOAP Fault: ${faultMatch[1]}`);
  }

  // Check for Analysis Services error
  const errorMatch = xml.match(/<Error[^>]*Description="([^"]*)"[^>]*>/);
  if (errorMatch) {
    throw new Error(`XMLA Error: ${errorMatch[1]}`);
  }

  // Extract column names from schema
  const columnNames: string[] = [];
  const schemaRegex = /<xsd:element name="([^"]+)"/g;
  let schemaMatch;
  // Skip the first "row" element definition
  const schemaSection = xml.match(/<xsd:complexType[^>]*>[\s\S]*?<\/xsd:complexType>/);
  if (schemaSection) {
    while ((schemaMatch = schemaRegex.exec(schemaSection[0])) !== null) {
      if (schemaMatch[1] !== "row") {
        columnNames.push(schemaMatch[1]);
      }
    }
  }

  if (columnNames.length === 0) {
    // Try alternative schema format
    const altSchemaRegex = /<xsd:element[^>]+name="([^"]+)"[^>]*\/>/g;
    const fullSchema = xml.match(/<xsd:schema[\s\S]*?<\/xsd:schema>/);
    if (fullSchema) {
      while ((schemaMatch = altSchemaRegex.exec(fullSchema[0])) !== null) {
        if (schemaMatch[1] !== "root" && schemaMatch[1] !== "row") {
          columnNames.push(schemaMatch[1]);
        }
      }
    }
  }

  // Extract row data
  const rowRegex = /<row>([\s\S]*?)<\/row>/g;
  let rowMatch;
  while ((rowMatch = rowRegex.exec(xml)) !== null) {
    const row: Record<string, unknown> = {};
    const rowContent = rowMatch[1];

    for (const colName of columnNames) {
      const valRegex = new RegExp(`<${escapeRegex(colName)}[^>]*>([\s\S]*?)<\/${escapeRegex(colName)}>`, "i");
      const valMatch = rowContent.match(valRegex);
      if (valMatch) {
        row[colName] = parseValue(valMatch[1]);
      } else {
        row[colName] = null;
      }
    }

    // If no schema columns found, extract all elements from row
    if (columnNames.length === 0) {
      const elemRegex = /<([A-Za-z_][A-Za-z0-9_.]*)(?:\s[^>]*)?>([^<]*)<\/\1>/g;
      let elemMatch;
      while ((elemMatch = elemRegex.exec(rowContent)) !== null) {
        row[elemMatch[1]] = parseValue(elemMatch[2]);
      }
    }

    if (Object.keys(row).length > 0) {
      rows.push(row);
    }
  }

  return rows;
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseValue(val: string): unknown {
  const trimmed = val.trim();
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  if (trimmed === "") return null;
  // Try number
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed.length > 0 && trimmed.length < 20) return num;
  return trimmed;
}
