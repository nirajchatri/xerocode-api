import mysql from 'mysql2/promise';
import sql from 'mssql';
import pg from 'pg';
import { normalizeHost, parseSqlServerServerInput, buildSqlServerTlsOptions } from './hostUtils.js';
import { parseSafePostgresTableRef } from './postgresSchema.js';
import { parseSafeSqlServerTableRef } from './sqlServerSchema.js';
import { closeControlSqlServer, connectToControlSqlServer } from '../controlDb/sqlserver.js';

const qMy = (name) => `\`${String(name).replace(/`/g, '``')}\``;
const qPg = (name) => `"${String(name).replace(/"/g, '""')}"`;
const qMs = (name) => `[${String(name).replace(/]/g, '')}]`;

const isRecord = (v) => v && typeof v === 'object' && !Array.isArray(v);

const loadProfile = async (pool, id) => {
  const result = await pool.request().input('id', sql.Int, Number(id)).query(`
    SELECT TOP 1 id, connector_type, host, port, database_name, username, password_value
    FROM dbo.connection_profiles
    WHERE id = @id
  `);
  return Array.isArray(result.recordset) && result.recordset.length > 0 ? result.recordset[0] : null;
};

const parseSafeMysqlTableRef = (table, defaultSchema) => {
  const t = String(table || '').trim();
  if (!t || !/^([a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+$/.test(t)) return null;
  if (t.includes('.')) {
    const [schema, name] = t.split('.');
    return { schema, table: name };
  }
  return { schema: defaultSchema || null, table: t };
};

const qualifiedTableName = (ref, dialect) => {
  if (dialect === 'mysql') {
    return ref.schema ? `${qMy(ref.schema)}.${qMy(ref.table)}` : qMy(ref.table);
  }
  if (dialect === 'postgresql') {
    return `${qPg(ref.schema)}.${qPg(ref.table)}`;
  }
  return `${qMs(ref.schema)}.${qMs(ref.table)}`;
};

async function dropTableOnConnection(ref, dialect, profile) {
  const tableSql = qualifiedTableName(ref, dialect);
  if (dialect === 'mysql') {
    const host = normalizeHost(profile.host);
    const portNum = Number(profile.port);
    const db = await mysql.createConnection({
      host,
      port: Number.isFinite(portNum) && portNum > 0 ? portNum : 3306,
      database: String(profile.database_name),
      user: String(profile.username),
      password: String(profile.password_value),
    });
    try {
      await db.query(`DROP TABLE IF EXISTS ${tableSql}`);
    } finally {
      await db.end();
    }
    return;
  }

  if (dialect === 'postgresql') {
    const host = normalizeHost(profile.host);
    const portNum = Number(profile.port);
    const client = new pg.Client({
      host,
      port: Number.isFinite(portNum) && portNum > 0 ? portNum : 5432,
      database: String(profile.database_name),
      user: String(profile.username),
      password: String(profile.password_value),
      ssl: process.env.POSTGRES_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
    });
    try {
      await client.connect();
      await client.query(`DROP TABLE IF EXISTS ${tableSql}`);
    } finally {
      await client.end();
    }
    return;
  }

  const { server, instanceName, port } = parseSqlServerServerInput(profile.host, profile.port);
  const config = {
    user: String(profile.username),
    password: String(profile.password_value),
    server,
    database: String(profile.database_name),
    options: buildSqlServerTlsOptions(server, instanceName),
    connectionTimeout: 15_000,
    requestTimeout: 30_000,
  };
  if (instanceName) {
    if (port && Number.isFinite(port) && port > 0) config.port = port;
  } else {
    config.port = port && Number.isFinite(port) && port > 0 ? port : 1433;
  }
  const pool = new sql.ConnectionPool(config);
  try {
    await pool.connect();
    await pool.request().query(`DROP TABLE IF EXISTS ${tableSql}`);
  } finally {
    await pool.close();
  }
}

export async function deleteTableOnProfile(profile, tableName) {
  const tableRaw = String(tableName || '').trim();
  if (!tableRaw) throw new Error('Missing table name.');

  const connector = profile.connector_type;
  if (!['mysql', 'sqlserver', 'postgresql'].includes(connector)) {
    throw new Error('Drop table is only supported for MySQL, SQL Server, and PostgreSQL.');
  }

  const dialect =
    connector === 'postgresql' ? 'postgresql' : connector === 'mysql' ? 'mysql' : 'sqlserver';

  let ref;
  if (dialect === 'mysql') {
    ref = parseSafeMysqlTableRef(tableRaw, String(profile.database_name || ''));
    if (!ref) throw new Error('Invalid table name.');
  } else if (dialect === 'postgresql') {
    if (tableRaw.includes('.')) {
      ref = parseSafePostgresTableRef(tableRaw);
    } else {
      ref = parseSafePostgresTableRef(`public.${tableRaw}`);
    }
    if (!ref) throw new Error('Invalid table name.');
  } else {
    if (tableRaw.includes('.')) {
      ref = parseSafeSqlServerTableRef(tableRaw);
    } else {
      ref = parseSafeSqlServerTableRef(`dbo.${tableRaw}`);
    }
    if (!ref) throw new Error('Invalid table name.');
  }

  await dropTableOnConnection(ref, dialect, profile);

  const qualified =
    dialect === 'mysql'
      ? ref.schema
        ? `${ref.schema}.${ref.table}`
        : ref.table
      : `${ref.schema}.${ref.table}`;

  return { table: qualified, dropped: true };
}

export const deleteConnectionTable = async (req, res) => {
  const { id } = req.params;
  if (!id) {
    return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  }
  if (!isRecord(req.body)) {
    return res.status(400).json({ ok: false, message: 'Invalid request body.' });
  }
  const table = String(req.body.table || '').trim();
  if (!table) {
    return res.status(400).json({ ok: false, message: 'Missing table name.' });
  }

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);
    if (!profile) {
      return res.status(404).json({ ok: false, message: 'Connection not found.' });
    }
    const out = await deleteTableOnProfile(profile, table);
    return res.json({ ok: true, ...out });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete table.';
    const status = /invalid table/i.test(message) ? 400 : 500;
    return res.status(status).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};
