import mysql from 'mysql2/promise';
import sql from 'mssql';
import { buildSqlServerTlsOptions, normalizeHost } from './hostUtils.js';
import { getPostgresTableDataForProfile, listPostgresTablesForProfile } from './postgresSchema.js';
import { getSqlServerTableDataForProfile, listSqlServerTablesForProfile } from './sqlServerSchema.js';
import { parseSafePostgresTableRef } from './postgresSchema.js';
import { parseSafeSqlServerTableRef } from './sqlServerSchema.js';
import pg from 'pg';
import { closeControlSqlServer, connectToControlSqlServer } from '../controlDb/sqlserver.js';
import { getMySqlTableData, listMySqlConnectionTables } from '../controlDb/sqlserverConnections.js';

const loadProfile = async (pool, id) => {
  const result = await pool.request().input('id', sql.Int, Number(id)).query(`
    SELECT TOP 1 id, connector_type, host, port, database_name, username, password_value
    FROM dbo.connection_profiles
    WHERE id = @id
  `);
  return Array.isArray(result.recordset) && result.recordset.length > 0 ? result.recordset[0] : null;
};

const parseSafeMysqlTableRef = (table) => {
  const t = String(table || '').trim();
  if (!t || !/^([a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+$/.test(t)) return null;
  if (t.includes('.')) {
    const [schema, name] = t.split('.');
    return { schema, table: name };
  }
  return { schema: null, table: t };
};

const qMy = (name) => `\`${String(name).replace(/`/g, '``')}\``;
const qPg = (name) => `"${String(name).replace(/"/g, '""')}"`;
const qMs = (name) => `[${String(name).replace(/]/g, '')}]`;

const isRecord = (v) => v && typeof v === 'object' && !Array.isArray(v);

export const listConnectionTables = async (req, res) => {
  const { id } = req.params;
  if (!id) {
    return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  }

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);

    if (!profile) {
      return res.status(404).json({ ok: false, message: 'Connection not found.' });
    }

    if (profile.connector_type === 'mysql') {
      await closeControlSqlServer(controlConnection);
      controlConnection = null;
      return listMySqlConnectionTables(req, res);
    }

    if (profile.connector_type === 'sqlserver') {
      const tables = await listSqlServerTablesForProfile(profile);
      return res.json({ ok: true, tables });
    }

    if (profile.connector_type === 'postgresql') {
      const tables = await listPostgresTablesForProfile(profile);
      return res.json({ ok: true, tables });
    }

    return res.status(400).json({
      ok: false,
      message: 'Schema browser is only available for MySQL, SQL Server, and PostgreSQL connections.',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load tables.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};

export const getConnectionTableData = async (req, res) => {
  const { id } = req.params;
  const table = req.query.table;
  const limitRaw = Number(req.query.limit);
  const offsetRaw = Number(req.query.offset);
  const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 50, 1), 5000);
  const offset = Math.max(Number.isFinite(offsetRaw) ? offsetRaw : 0, 0);
  const search = String(req.query.q || '').trim();
  const filters =
    req.query.filters && typeof req.query.filters === 'string'
      ? (() => {
          try {
            const parsed = JSON.parse(req.query.filters);
            return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
          } catch {
            return {};
          }
        })()
      : {};

  if (!id) {
    return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  }
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

    if (profile.connector_type === 'mysql') {
      await closeControlSqlServer(controlConnection);
      controlConnection = null;
      return getMySqlTableData(req, res);
    }

    if (profile.connector_type === 'sqlserver') {
      const { columns, rows, total } = await getSqlServerTableDataForProfile(profile, table, limit, offset, {
        q: search,
        filters,
      });
      return res.json({
        ok: true,
        columns,
        rows,
        total,
        limit,
        offset,
      });
    }

    if (profile.connector_type === 'postgresql') {
      try {
        const { columns, rows, total } = await getPostgresTableDataForProfile(profile, table, limit, offset, {
          q: search,
          filters,
        });
        return res.json({
          ok: true,
          columns,
          rows,
          total,
          limit,
          offset,
        });
      } catch (error) {
        if (error && typeof error === 'object' && error.code === 'TABLE_NOT_FOUND') {
          return res.status(404).json({
            ok: false,
            message: error instanceof Error ? error.message : 'Table not found in this database.',
          });
        }
        throw error;
      }
    }

    return res.status(400).json({
      ok: false,
      message: 'Schema browser is only available for MySQL, SQL Server, and PostgreSQL connections.',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load table data.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};

export const mutateConnectionTableData = async (req, res) => {
  const { id } = req.params;
  const tableParam = String(req.body?.table || '');
  const action = String(req.body?.action || '').toLowerCase();
  const values = isRecord(req.body?.values) ? req.body.values : {};
  const rowMatch = isRecord(req.body?.rowMatch) ? req.body.rowMatch : {};

  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  if (!tableParam) return res.status(400).json({ ok: false, message: 'Missing table name.' });
  if (!['create', 'update', 'delete'].includes(action)) {
    return res.status(400).json({ ok: false, message: 'Invalid action.' });
  }

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);
    if (!profile) return res.status(404).json({ ok: false, message: 'Connection not found.' });

    if (profile.connector_type === 'mysql') {
      const ref = parseSafeMysqlTableRef(tableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid table name.' });
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
        const tableSql = ref.schema ? `${qMy(ref.schema)}.${qMy(ref.table)}` : qMy(ref.table);
        if (action === 'create') {
          const cols = Object.keys(values);
          if (cols.length === 0) throw new Error('No values provided.');
          const sqlText = `INSERT INTO ${tableSql} (${cols.map(qMy).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`;
          await db.query(sqlText, cols.map((c) => values[c]));
        } else if (action === 'update') {
          const setCols = Object.keys(values);
          const whereCols = Object.keys(rowMatch);
          if (setCols.length === 0 || whereCols.length === 0) throw new Error('Missing update payload.');
          const sqlText = `UPDATE ${tableSql} SET ${setCols.map((c) => `${qMy(c)} = ?`).join(', ')} WHERE ${whereCols
            .map((c) => `${qMy(c)} <=> ?`)
            .join(' AND ')} LIMIT 1`;
          await db.query(sqlText, [...setCols.map((c) => values[c]), ...whereCols.map((c) => rowMatch[c])]);
        } else {
          const whereCols = Object.keys(rowMatch);
          if (whereCols.length === 0) throw new Error('Missing delete match payload.');
          const sqlText = `DELETE FROM ${tableSql} WHERE ${whereCols.map((c) => `${qMy(c)} <=> ?`).join(' AND ')} LIMIT 1`;
          await db.query(sqlText, whereCols.map((c) => rowMatch[c]));
        }
      } finally {
        await db.end();
      }
      return res.json({ ok: true });
    }

    if (profile.connector_type === 'postgresql') {
      const ref = parseSafePostgresTableRef(tableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid table name.' });
      const client = new pg.Client({
        host: normalizeHost(profile.host),
        port: Number(profile.port) || 5432,
        database: String(profile.database_name),
        user: String(profile.username),
        password: String(profile.password_value),
        connectionTimeoutMillis: 15_000,
      });
      await client.connect();
      try {
        const tableSql = `${qPg(ref.schema)}.${qPg(ref.table)}`;
        if (action === 'create') {
          const cols = Object.keys(values);
          if (cols.length === 0) throw new Error('No values provided.');
          const sqlText = `INSERT INTO ${tableSql} (${cols.map(qPg).join(', ')}) VALUES (${cols
            .map((_, i) => `$${i + 1}`)
            .join(', ')})`;
          await client.query(sqlText, cols.map((c) => values[c]));
        } else if (action === 'update') {
          const setCols = Object.keys(values);
          const whereCols = Object.keys(rowMatch);
          if (setCols.length === 0 || whereCols.length === 0) throw new Error('Missing update payload.');
          const setStart = 1;
          const whereStart = setCols.length + 1;
          const setSql = setCols.map((c, i) => `${qPg(c)} = $${setStart + i}`).join(', ');
          const whereSql = whereCols.map((c, i) => `${qPg(c)} IS NOT DISTINCT FROM $${whereStart + i}`).join(' AND ');
          await client.query(`UPDATE ${tableSql} SET ${setSql} WHERE ${whereSql}`, [
            ...setCols.map((c) => values[c]),
            ...whereCols.map((c) => rowMatch[c]),
          ]);
        } else {
          const whereCols = Object.keys(rowMatch);
          if (whereCols.length === 0) throw new Error('Missing delete match payload.');
          const whereSql = whereCols.map((c, i) => `${qPg(c)} IS NOT DISTINCT FROM $${i + 1}`).join(' AND ');
          await client.query(`DELETE FROM ${tableSql} WHERE ${whereSql}`, whereCols.map((c) => rowMatch[c]));
        }
      } finally {
        await client.end();
      }
      return res.json({ ok: true });
    }

    if (profile.connector_type === 'sqlserver') {
      const ref = parseSafeSqlServerTableRef(tableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid table name.' });
      const serverInput = String(profile.host || '');
      const slash = serverInput.indexOf('\\');
      const comma = serverInput.indexOf(',');
      const instanceName = slash >= 0 ? serverInput.slice(slash + 1).trim() : undefined;
      const server = (slash >= 0 ? serverInput.slice(0, slash) : comma >= 0 ? serverInput.slice(0, comma) : serverInput).trim();
      const port = comma >= 0 ? Number(serverInput.slice(comma + 1)) : Number(profile.port);
      const pool = new sql.ConnectionPool({
        user: String(profile.username),
        password: String(profile.password_value),
        server,
        database: String(profile.database_name),
        options: buildSqlServerTlsOptions(server, instanceName),
        ...(Number.isFinite(port) && port > 0 ? { port } : {}),
      });
      await pool.connect();
      try {
        const tableSql = `${qMs(ref.schema)}.${qMs(ref.table)}`;
        if (action === 'create') {
          const cols = Object.keys(values);
          if (cols.length === 0) throw new Error('No values provided.');
          const req2 = pool.request();
          const valsSql = cols.map((c, i) => {
            req2.input(`v${i}`, values[c]);
            return `@v${i}`;
          });
          await req2.query(`INSERT INTO ${tableSql} (${cols.map(qMs).join(', ')}) VALUES (${valsSql.join(', ')})`);
        } else if (action === 'update') {
          const setCols = Object.keys(values);
          const whereCols = Object.keys(rowMatch);
          if (setCols.length === 0 || whereCols.length === 0) throw new Error('Missing update payload.');
          const req2 = pool.request();
          const setSql = setCols.map((c, i) => {
            req2.input(`s${i}`, values[c]);
            return `${qMs(c)} = @s${i}`;
          });
          const whereSql = whereCols.map((c, i) => {
            req2.input(`w${i}`, rowMatch[c]);
            return `(${qMs(c)} = @w${i} OR (${qMs(c)} IS NULL AND @w${i} IS NULL))`;
          });
          await req2.query(`UPDATE TOP (1) ${tableSql} SET ${setSql.join(', ')} WHERE ${whereSql.join(' AND ')}`);
        } else {
          const whereCols = Object.keys(rowMatch);
          if (whereCols.length === 0) throw new Error('Missing delete match payload.');
          const req2 = pool.request();
          const whereSql = whereCols.map((c, i) => {
            req2.input(`w${i}`, rowMatch[c]);
            return `(${qMs(c)} = @w${i} OR (${qMs(c)} IS NULL AND @w${i} IS NULL))`;
          });
          await req2.query(`DELETE TOP (1) FROM ${tableSql} WHERE ${whereSql.join(' AND ')}`);
        }
      } finally {
        await pool.close();
      }
      return res.json({ ok: true });
    }

    return res.status(400).json({ ok: false, message: 'Connector not supported for CRUD.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to mutate table data.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};

const openMysqlClientForProfile = async (profile) => {
  const host = normalizeHost(profile.host);
  const portNum = Number(profile.port);
  return mysql.createConnection({
    host,
    port: Number.isFinite(portNum) && portNum > 0 ? portNum : 3306,
    database: String(profile.database_name),
    user: String(profile.username),
    password: String(profile.password_value),
  });
};

const pickLabelColumnName = (columnRows, pkColumn) => {
  const pkLower = String(pkColumn || '').toLowerCase();
  const cols = columnRows
    .map((r) => ({
      name: String(r.COLUMN_NAME || r.column_name || ''),
      dataType: String(r.DATA_TYPE || r.data_type || ''),
    }))
    .filter((c) => c.name);
  const preferred = ['name', 'title', 'label', 'description', 'display_name', 'full_name', 'email', 'code'];
  for (const p of preferred) {
    const hit = cols.find((c) => c.name.toLowerCase() === p && c.name.toLowerCase() !== pkLower);
    if (hit) return hit.name;
  }
  const textRe = /(char|text|varchar|nchar|nvarchar|string|citext)/i;
  const textCol = cols.find((c) => c.name.toLowerCase() !== pkLower && textRe.test(c.dataType));
  if (textCol) return textCol.name;
  return null;
};

const groupSingleColumnForeignKeys = (rows, getConstraint, pick) => {
  const byC = new Map();
  for (const r of rows) {
    const cname = getConstraint(r);
    if (!byC.has(cname)) byC.set(cname, []);
    byC.get(cname).push(r);
  }
  const foreignKeys = [];
  for (const [, arr] of byC) {
    if (arr.length !== 1) continue;
    foreignKeys.push(pick(arr[0]));
  }
  return foreignKeys;
};

export const getConnectionTableForeignKeys = async (req, res) => {
  const { id } = req.params;
  const tableParam = String(req.query.table || '');
  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  if (!tableParam) return res.status(400).json({ ok: false, message: 'Missing table name.' });

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);
    if (!profile) return res.status(404).json({ ok: false, message: 'Connection not found.' });

    if (profile.connector_type === 'mysql') {
      const ref = parseSafeMysqlTableRef(tableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid table name.' });
      const tableSchema = ref.schema || String(profile.database_name);
      const tableName = ref.table;
      const db = await openMysqlClientForProfile(profile);
      try {
        const [rows] = await db.query(
          `SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
           FROM information_schema.KEY_COLUMN_USAGE
           WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
             AND REFERENCED_TABLE_NAME IS NOT NULL`,
          [tableSchema, tableName]
        );
        const foreignKeys = groupSingleColumnForeignKeys(
          rows,
          (r) => String(r.CONSTRAINT_NAME),
          (r) => ({
            column: String(r.COLUMN_NAME),
            referencedSchema: r.REFERENCED_TABLE_SCHEMA != null ? String(r.REFERENCED_TABLE_SCHEMA) : null,
            referencedTable: String(r.REFERENCED_TABLE_NAME),
            referencedColumn: String(r.REFERENCED_COLUMN_NAME),
          })
        );
        return res.json({ ok: true, foreignKeys });
      } finally {
        await db.end();
      }
    }

    if (profile.connector_type === 'postgresql') {
      const ref = parseSafePostgresTableRef(tableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid table name.' });
      const client = new pg.Client({
        host: normalizeHost(profile.host),
        port: Number(profile.port) || 5432,
        database: String(profile.database_name),
        user: String(profile.username),
        password: String(profile.password_value),
        connectionTimeoutMillis: 15_000,
      });
      await client.connect();
      try {
        const result = await client.query(
          `SELECT tc.constraint_name,
                  kcu.column_name,
                  ccu.table_schema AS referenced_schema,
                  ccu.table_name AS referenced_table,
                  ccu.column_name AS referenced_column
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON tc.constraint_schema = kcu.constraint_schema
            AND tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
           JOIN information_schema.constraint_column_usage ccu
             ON ccu.constraint_schema = tc.constraint_schema
             AND ccu.constraint_name = tc.constraint_name
           WHERE tc.constraint_type = 'FOREIGN KEY'
             AND tc.table_schema = $1
             AND tc.table_name = $2`,
          [ref.schema, ref.table]
        );
        const foreignKeys = groupSingleColumnForeignKeys(
          result.rows || [],
          (r) => String(r.constraint_name),
          (r) => ({
            column: String(r.column_name),
            referencedSchema: String(r.referenced_schema),
            referencedTable: String(r.referenced_table),
            referencedColumn: String(r.referenced_column),
          })
        );
        return res.json({ ok: true, foreignKeys });
      } finally {
        await client.end();
      }
    }

    if (profile.connector_type === 'sqlserver') {
      const ref = parseSafeSqlServerTableRef(tableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid table name.' });
      const serverInput = String(profile.host || '');
      const slash = serverInput.indexOf('\\');
      const comma = serverInput.indexOf(',');
      const instanceName = slash >= 0 ? serverInput.slice(slash + 1).trim() : undefined;
      const server = (slash >= 0 ? serverInput.slice(0, slash) : comma >= 0 ? serverInput.slice(0, comma) : serverInput).trim();
      const port = comma >= 0 ? Number(serverInput.slice(comma + 1)) : Number(profile.port);
      const pool = new sql.ConnectionPool({
        user: String(profile.username),
        password: String(profile.password_value),
        server,
        database: String(profile.database_name),
        options: buildSqlServerTlsOptions(server, instanceName),
        ...(Number.isFinite(port) && port > 0 ? { port } : {}),
      });
      await pool.connect();
      try {
        const result = await pool
          .request()
          .input('schema', sql.NVarChar, ref.schema)
          .input('table', sql.NVarChar, ref.table)
          .query(`
            SELECT fk.name AS constraint_name,
                   COL_NAME(fc.parent_object_id, fc.parent_column_id) AS column_name,
                   OBJECT_SCHEMA_NAME(fc.referenced_object_id) AS referenced_schema,
                   OBJECT_NAME(fc.referenced_object_id) AS referenced_table,
                   COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column
            FROM sys.foreign_key_columns fc
            INNER JOIN sys.foreign_keys fk ON fk.object_id = fc.constraint_object_id
            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = @schema
              AND OBJECT_NAME(fk.parent_object_id) = @table
          `);
        const rows = result.recordset || [];
        const foreignKeys = groupSingleColumnForeignKeys(
          rows,
          (r) => String(r.constraint_name),
          (r) => ({
            column: String(r.column_name),
            referencedSchema: r.referenced_schema != null ? String(r.referenced_schema) : null,
            referencedTable: String(r.referenced_table),
            referencedColumn: String(r.referenced_column),
          })
        );
        return res.json({ ok: true, foreignKeys });
      } finally {
        await pool.close();
      }
    }

    return res.status(400).json({
      ok: false,
      message: 'Foreign keys are only available for MySQL, PostgreSQL, and SQL Server.',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load foreign keys.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};

export const getConnectionFkLookup = async (req, res) => {
  const { id } = req.params;
  const refTableParam = String(req.query.refTable || '');
  const pkColumn = String(req.query.pkColumn || '');
  const limitRaw = Number(req.query.limit);
  const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 500, 1), 2000);

  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  if (!refTableParam) return res.status(400).json({ ok: false, message: 'Missing refTable.' });
  if (!pkColumn) return res.status(400).json({ ok: false, message: 'Missing pkColumn.' });

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);
    if (!profile) return res.status(404).json({ ok: false, message: 'Connection not found.' });

    if (profile.connector_type === 'mysql') {
      const ref = parseSafeMysqlTableRef(refTableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid refTable.' });
      const schema = ref.schema || String(profile.database_name);
      const tname = ref.table;
      const db = await openMysqlClientForProfile(profile);
      try {
        const [colRows] = await db.query(
          `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
           ORDER BY ORDINAL_POSITION`,
          [schema, tname]
        );
        const labelCol = pickLabelColumnName(colRows, pkColumn);
        const pkQ = qMy(pkColumn);
        const labelQ = qMy(labelCol || pkColumn);
        const tableSql = `${qMy(schema)}.${qMy(tname)}`;
        const [dataRows] = await db.query(`SELECT ${pkQ} AS __v, ${labelQ} AS __l FROM ${tableSql} ORDER BY ${pkQ} ASC LIMIT ?`, [
          limit,
        ]);
        const options = (dataRows || []).map((r) => {
          const value = r.__v == null ? '' : String(r.__v);
          const labelRaw = r.__l == null ? value : String(r.__l);
          return { value, label: labelRaw || value };
        });
        return res.json({ ok: true, options, labelColumn: labelCol || pkColumn });
      } finally {
        await db.end();
      }
    }

    if (profile.connector_type === 'postgresql') {
      const ref = parseSafePostgresTableRef(refTableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid refTable.' });
      const client = new pg.Client({
        host: normalizeHost(profile.host),
        port: Number(profile.port) || 5432,
        database: String(profile.database_name),
        user: String(profile.username),
        password: String(profile.password_value),
        connectionTimeoutMillis: 15_000,
      });
      await client.connect();
      try {
        const colResult = await client.query(
          `SELECT column_name, data_type FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = $2
           ORDER BY ordinal_position`,
          [ref.schema, ref.table]
        );
        const labelCol = pickLabelColumnName(colResult.rows || [], pkColumn);
        const labelName = labelCol || pkColumn;
        const tableSql = `${qPg(ref.schema)}.${qPg(ref.table)}`;
        const dataResult = await client.query(
          `SELECT ${qPg(pkColumn)}::text AS v, ${qPg(labelName)}::text AS l
           FROM ${tableSql}
           ORDER BY 1
           LIMIT $1`,
          [limit]
        );
        const options = (dataResult.rows || []).map((r) => {
          const value = r.v == null ? '' : String(r.v);
          const labelRaw = r.l == null ? value : String(r.l);
          return { value, label: labelRaw || value };
        });
        return res.json({ ok: true, options, labelColumn: labelName });
      } finally {
        await client.end();
      }
    }

    if (profile.connector_type === 'sqlserver') {
      const ref = parseSafeSqlServerTableRef(refTableParam);
      if (!ref) return res.status(400).json({ ok: false, message: 'Invalid refTable.' });
      const serverInput = String(profile.host || '');
      const slash = serverInput.indexOf('\\');
      const comma = serverInput.indexOf(',');
      const instanceName = slash >= 0 ? serverInput.slice(slash + 1).trim() : undefined;
      const server = (slash >= 0 ? serverInput.slice(0, slash) : comma >= 0 ? serverInput.slice(0, comma) : serverInput).trim();
      const port = comma >= 0 ? Number(serverInput.slice(comma + 1)) : Number(profile.port);
      const pool = new sql.ConnectionPool({
        user: String(profile.username),
        password: String(profile.password_value),
        server,
        database: String(profile.database_name),
        options: buildSqlServerTlsOptions(server, instanceName),
        ...(Number.isFinite(port) && port > 0 ? { port } : {}),
      });
      await pool.connect();
      try {
        const colReq = pool.request();
        colReq.input('sch', sql.NVarChar, ref.schema);
        colReq.input('tbl', sql.NVarChar, ref.table);
        const colResult = await colReq.query(`
          SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = @sch AND TABLE_NAME = @tbl
          ORDER BY ORDINAL_POSITION
        `);
        const labelCol = pickLabelColumnName(colResult.recordset || [], pkColumn);
        const labelName = labelCol || pkColumn;
        const tableSql = `${qMs(ref.schema)}.${qMs(ref.table)}`;
        const dataReq = pool.request();
        dataReq.input('lim', sql.Int, limit);
        const dataResult = await dataReq.query(
          `SELECT TOP (@lim) ${qMs(pkColumn)} AS v, ${qMs(labelName)} AS l FROM ${tableSql} ORDER BY ${qMs(pkColumn)}`
        );
        const options = (dataResult.recordset || []).map((r) => {
          const value = r.v == null ? '' : String(r.v);
          const labelRaw = r.l == null ? value : String(r.l);
          return { value, label: labelRaw || value };
        });
        return res.json({ ok: true, options, labelColumn: labelName });
      } finally {
        await pool.close();
      }
    }

    return res.status(400).json({
      ok: false,
      message: 'FK lookup is only available for MySQL, PostgreSQL, and SQL Server.',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load FK options.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};
