import { Connection, Request } from "tedious";
import { getTokenForScope, SQL_SCOPE } from "../auth/fabricAuth.js";

export type SqlRow = Record<string, unknown>;

/**
 * Execute a single SQL query. Creates a new connection each time.
 */
export async function executeSqlQuery(
  server: string,
  database: string,
  sql: string
): Promise<SqlRow[]> {
  const token = await getTokenForScope(SQL_SCOPE);

  return new Promise((resolve, reject) => {
    const rows: SqlRow[] = [];

    const connection = new Connection({
      server,
      authentication: {
        type: "azure-active-directory-access-token",
        options: { token },
      },
      options: {
        encrypt: true,
        database,
        port: 1433,
        connectTimeout: 30000,
        requestTimeout: 60000,
        trustServerCertificate: false,
      },
    });

    connection.on("connect", (err) => {
      if (err) {
        connection.close();
        reject(new Error(`SQL connection failed: ${err.message}`));
        return;
      }

      const request = new Request(sql, (err) => {
        connection.close();
        if (err) reject(new Error(`SQL query failed: ${err.message}`));
        else resolve(rows);
      });

      request.on("row", (columns: Array<{ metadata: { colName: string }; value: unknown }>) => {
        const row: SqlRow = {};
        for (const col of columns) {
          row[col.metadata.colName] = col.value;
        }
        rows.push(row);
      });

      connection.execSql(request);
    });

    connection.on("error", (err) => {
      reject(new Error(`SQL connection error: ${err.message}`));
    });

    connection.connect();
  });
}

/**
 * Execute a SQL query on an existing open connection.
 */
function executeOnConnection(
  connection: Connection,
  sql: string
): Promise<SqlRow[]> {
  return new Promise((resolve, reject) => {
    const rows: SqlRow[] = [];

    const request = new Request(sql, (err) => {
      if (err) reject(new Error(`SQL query failed: ${err.message}`));
      else resolve(rows);
    });

    request.on("row", (columns: Array<{ metadata: { colName: string }; value: unknown }>) => {
      const row: SqlRow = {};
      for (const col of columns) {
        row[col.metadata.colName] = col.value;
      }
      rows.push(row);
    });

    connection.execSql(request);
  });
}

/**
 * Create a reusable SQL connection.
 */
function createConnection(server: string, database: string, token: string): Promise<Connection> {
  return new Promise((resolve, reject) => {
    const connection = new Connection({
      server,
      authentication: {
        type: "azure-active-directory-access-token",
        options: { token },
      },
      options: {
        encrypt: true,
        database,
        port: 1433,
        connectTimeout: 30000,
        requestTimeout: 60000,
        trustServerCertificate: false,
      },
    });

    connection.on("connect", (err) => {
      if (err) {
        connection.close();
        reject(new Error(`SQL connection failed: ${err.message}`));
        return;
      }
      resolve(connection);
    });

    connection.on("error", (err) => {
      reject(new Error(`SQL connection error: ${err.message}`));
    });

    connection.connect();
  });
}

/**
 * Run multiple diagnostic queries on a single reusable connection.
 * Opens one connection, runs all queries sequentially, then closes.
 * Much faster than opening a new connection per query.
 */
export async function runDiagnosticQueries(
  server: string,
  database: string,
  queries: Record<string, string>
): Promise<Record<string, { rows?: SqlRow[]; error?: string }>> {
  const results: Record<string, { rows?: SqlRow[]; error?: string }> = {};
  const token = await getTokenForScope(SQL_SCOPE);

  let connection: Connection;
  try {
    connection = await createConnection(server, database, token);
  } catch (error) {
    // If connection fails, mark all queries as error
    const msg = error instanceof Error ? error.message : String(error);
    for (const name of Object.keys(queries)) {
      results[name] = { error: msg };
    }
    return results;
  }

  try {
    for (const [name, sql] of Object.entries(queries)) {
      try {
        const rows = await executeOnConnection(connection, sql);
        results[name] = { rows };
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        results[name] = { error: msg };
      }
    }
  } finally {
    connection.close();
  }

  return results;
}
