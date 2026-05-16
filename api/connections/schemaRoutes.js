import mysql from 'mysql2/promise';
import sql from 'mssql';
import { buildSqlServerTlsOptions, normalizeHost, parseSqlServerServerInput } from './hostUtils.js';
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

/** Case-insensitive field read (MySQL column names vs information_schema / FK metadata). */
const pickRowFieldCi = (row, name) => {
  if (!row || !name) return undefined;
  if (Object.prototype.hasOwnProperty.call(row, name)) return row[name];
  const lower = String(name).toLowerCase();
  const hit = Object.keys(row).find((k) => k.toLowerCase() === lower);
  return hit !== undefined ? row[hit] : undefined;
};

const resolveMysqlColumnNameCi = async (db, tableSchema, tableName, columnName) => {
  const [rows] = await db.query(
    `SELECT COLUMN_NAME FROM information_schema.COLUMNS
     WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)
       AND LOWER(COLUMN_NAME) = LOWER(?)
     LIMIT 1`,
    [tableSchema, tableName, columnName]
  );
  const hit = Array.isArray(rows) && rows[0] ? rows[0] : null;
  return hit ? String(hit.COLUMN_NAME || columnName) : String(columnName || '');
};

/** Turn MySQL/Postgres/SQL Server FK and duplicate-key errors into actionable messages. */
const formatDbConstraintError = (error) => {
  const message = error instanceof Error ? error.message : String(error);
  const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : '';
  const sqlMessage =
    error && typeof error === 'object' && 'sqlMessage' in error ? String(error.sqlMessage || '') : message;
  const combined = `${message} ${sqlMessage}`;

  const fkMatch = combined.match(
    /FOREIGN KEY\s*\(?[`"[]?([^`"\])]+)[`"\]]?\)?\s*REFERENCES\s*[`"[]?([^`"\].]+)[`"\]]?\s*\(?[`"[]?([^`"\])]+)[`"\]]?\)?/i
  );
  if (
    code === 'ER_NO_REFERENCED_ROW_2' ||
    /foreign key constraint fails/i.test(combined) ||
    /violates foreign key constraint/i.test(combined)
  ) {
    if (fkMatch) {
      const [, childCol, refTable, refCol] = fkMatch;
      return (
        `${childCol} must match an existing row in ${refTable}.${refCol}. ` +
        `Choose a value from the dropdown (if shown), or create that ${refTable} record in the database first.`
      );
    }
    return (
      'A foreign key value does not exist in the related table. ' +
      'Use a valid lookup value or create the parent row before saving this line.'
    );
  }

  if (code === 'ER_ROW_IS_REFERENCED_2' || /is still referenced from table/i.test(combined)) {
    if (fkMatch) {
      const [, childCol, refTable] = fkMatch;
      return `Cannot delete or change this row because other rows still reference it via ${childCol} → ${refTable}. Remove or update those rows first.`;
    }
    return 'Cannot delete this row because other tables still reference it. Remove dependent rows first.';
  }

  if (code === 'ER_DUP_ENTRY' || /duplicate key|unique constraint|Unique constraint/i.test(combined)) {
    const dupKey = sqlMessage || message;
    const custHint = /CUST_ID/i.test(dupKey)
      ? ' If the duplicate key is CUST_ID, the screen may be linking lines on the wrong column—use ORDER_ID to the order header PK, not customer or order-number fields.'
      : '';
    return (
      `Duplicate key: ${dupKey.trim()}.${custHint} ` +
      'For order + line items, the detail link must be ORDER_ID (FK to the master primary key), not Order_No/ORDER_NUMBER. ' +
      'Many lines can share the same ORDER_ID; only the detail line primary key (e.g. ORDER_ITEM_ID) must be unique.'
    );
  }

  return message;
};

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
  const { parseTableDataQueryOptions } = await import('./tableDataQuery.js');
  const parsedQuery = parseTableDataQueryOptions(req.query);
  const queryOpts = {
    q: parsedQuery.search,
    filters: parsedQuery.filters,
    conditions: parsedQuery.conditions,
  };

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
      const { columns, rows, total } = await getSqlServerTableDataForProfile(profile, table, limit, offset, queryOpts);
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
        const { columns, rows, total } = await getPostgresTableDataForProfile(profile, table, limit, offset, queryOpts);
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
  if (!['create', 'update', 'delete', 'delete_many'].includes(action)) {
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
        } else if (action === 'delete_many') {
          const whereCols = Object.keys(rowMatch);
          if (whereCols.length === 0) throw new Error('Missing delete_many match payload.');
          const sqlText = `DELETE FROM ${tableSql} WHERE ${whereCols
            .map((c) => `${qMy(c)} <=> ?`)
            .join(' AND ')}`;
          await db.query(sqlText, whereCols.map((c) => rowMatch[c]));
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
        } else if (action === 'delete_many') {
          const whereCols = Object.keys(rowMatch);
          if (whereCols.length === 0) throw new Error('Missing delete_many match payload.');
          const whereSql = whereCols.map((c, i) => `${qPg(c)} IS NOT DISTINCT FROM $${i + 1}`).join(' AND ');
          await client.query(`DELETE FROM ${tableSql} WHERE ${whereSql}`, whereCols.map((c) => rowMatch[c]));
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
        } else if (action === 'delete_many') {
          const whereCols = Object.keys(rowMatch);
          if (whereCols.length === 0) throw new Error('Missing delete_many match payload.');
          const req2 = pool.request();
          const whereSql = whereCols.map((c, i) => {
            req2.input(`w${i}`, rowMatch[c]);
            return `(${qMs(c)} = @w${i} OR (${qMs(c)} IS NULL AND @w${i} IS NULL))`;
          });
          await req2.query(`DELETE FROM ${tableSql} WHERE ${whereSql.join(' AND ')}`);
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
    const message = formatDbConstraintError(error);
    const status = /foreign key|duplicate key|referenced/i.test(message) ? 400 : 500;
    return res.status(status).json({ ok: false, message });
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

const safeSqlIdent = (name) => {
  const s = String(name || '').trim();
  if (!/^[a-zA-Z0-9_]+$/.test(s)) return null;
  return s;
};

/** All single-column FKs on detail that reference the master table. */
const listMysqlDetailFkColumnsToMaster = async (
  db,
  detailSchema,
  detailTable,
  masterSchema,
  masterTable
) => {
  const [rows] = await db.query(
    `SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION,
            kcu.REFERENCED_COLUMN_NAME
     FROM information_schema.KEY_COLUMN_USAGE kcu
     INNER JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
       ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
       AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
     WHERE LOWER(kcu.TABLE_SCHEMA) = LOWER(?)
       AND LOWER(kcu.TABLE_NAME) = LOWER(?)
       AND LOWER(kcu.REFERENCED_TABLE_SCHEMA) = LOWER(?)
       AND LOWER(kcu.REFERENCED_TABLE_NAME) = LOWER(?)
       AND kcu.REFERENCED_COLUMN_NAME IS NOT NULL
     ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`,
    [detailSchema, detailTable, masterSchema, masterTable]
  );
  const list = Array.isArray(rows) ? rows : [];
  const byConstraint = new Map();
  for (const r of list) {
    const cn = String(r.CONSTRAINT_NAME || '');
    if (!byConstraint.has(cn)) byConstraint.set(cn, []);
    byConstraint.get(cn).push(r);
  }
  const out = [];
  for (const [, cols] of byConstraint) {
    if (cols.length !== 1) continue;
    const column = String(cols[0].COLUMN_NAME || '').trim();
    const referencedColumn = String(cols[0].REFERENCED_COLUMN_NAME || '').trim();
    if (column) out.push({ column, referencedColumn });
  }
  return out;
};

const getMysqlColumnForeignKeyTarget = async (db, detailSchema, detailTable, columnName) => {
  const col = String(columnName || '').trim();
  if (!col) return null;
  const [rows] = await db.query(
    `SELECT kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
     FROM information_schema.KEY_COLUMN_USAGE kcu
     INNER JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
       ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
       AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
     WHERE LOWER(kcu.TABLE_SCHEMA) = LOWER(?)
       AND LOWER(kcu.TABLE_NAME) = LOWER(?)
       AND kcu.COLUMN_NAME = ?
       AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
     LIMIT 1`,
    [detailSchema, detailTable, col]
  );
  const r = Array.isArray(rows) && rows[0] ? rows[0] : null;
  if (!r) return null;
  return {
    referencedSchema: r.REFERENCED_TABLE_SCHEMA != null ? String(r.REFERENCED_TABLE_SCHEMA) : null,
    referencedTable: String(r.REFERENCED_TABLE_NAME || ''),
    referencedColumn: String(r.REFERENCED_COLUMN_NAME || ''),
  };
};

const loadMysqlTableColumnSet = async (db, tableSchema, tableName) => {
  const [rows] = await db.query(
    `SELECT COLUMN_NAME FROM information_schema.COLUMNS
     WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)`,
    [tableSchema, tableName]
  );
  return new Set(
    (Array.isArray(rows) ? rows : [])
      .map((r) => String(r.COLUMN_NAME || '').trim())
      .filter((n) => safeSqlIdent(n))
  );
};

const loadMysqlNonInsertableColumns = async (db, tableSchema, tableName) => {
  const skip = new Set();
  const [rows] = await db.query(
    `SELECT COLUMN_NAME, EXTRA, COLUMN_KEY
     FROM information_schema.COLUMNS
     WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)`,
    [tableSchema, tableName]
  );
  for (const r of Array.isArray(rows) ? rows : []) {
    const name = String(r.COLUMN_NAME || '').trim();
    if (!name) continue;
    const extra = String(r.EXTRA || '').toLowerCase();
    const colKey = String(r.COLUMN_KEY || '').toUpperCase();
    if (extra.includes('auto_increment')) skip.add(name);
    if (/\b(stored|virtual)\s+generated\b/.test(extra)) skip.add(name);
    // Detail line PK (e.g. ORDER_ITEM_ID) must be generated by MySQL, not copied per grid row.
    if (colKey === 'PRI') skip.add(name);
  }
  return skip;
};

/** Primary key column(s) on a MySQL table (information_schema). */
const loadMysqlPrimaryKeyColumns = async (db, tableSchema, tableName) => {
  const [rows] = await db.query(
    `SELECT COLUMN_NAME, EXTRA, ORDINAL_POSITION
     FROM information_schema.COLUMNS
     WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)
       AND COLUMN_KEY = 'PRI'
     ORDER BY ORDINAL_POSITION`,
    [tableSchema, tableName]
  );
  return (Array.isArray(rows) ? rows : []).map((r) => ({
    name: String(r.COLUMN_NAME || '').trim(),
    extra: String(r.EXTRA || '').toLowerCase(),
  }));
};

/** Single AUTO_INCREMENT column used as surrogate PK (for insertId / master link). */
const loadMysqlAutoIncrementPrimaryKey = async (db, tableSchema, tableName) => {
  const pks = await loadMysqlPrimaryKeyColumns(db, tableSchema, tableName);
  const auto = pks.filter((c) => c.name && c.extra.includes('auto_increment'));
  if (auto.length === 1) return auto[0].name;
  if (pks.length === 1 && pks[0].name) return pks[0].name;
  return null;
};

/**
 * Detail column linking lines to the master row. Only FK → master PK (e.g. ORDER_ID → orders.ORDER_ID).
 * Rejects FKs to other tables (PROD_ID → products) and non-PK master columns (ORDER_NUMBER, CUST_ID on orders).
 */
const resolveMysqlDetailFkColumnToMaster = async (
  db,
  detailSchema,
  detailTable,
  masterSchema,
  masterTable,
  clientFkColumn,
  masterPkColumn
) => {
  const client = String(clientFkColumn || '').trim();
  const actualMasterPk =
    (await loadMysqlAutoIncrementPrimaryKey(db, masterSchema, masterTable)) ||
    String(masterPkColumn || 'id').trim();

  const masterLinks = await listMysqlDetailFkColumnsToMaster(
    db,
    detailSchema,
    detailTable,
    masterSchema,
    masterTable
  );

  const pkLinks = masterLinks.filter(
    (l) => l.referencedColumn.toLowerCase() === actualMasterPk.toLowerCase()
  );

  const rejectNonPkMasterLink = (columnName) => {
    const hit = masterLinks.find((l) => l.column.toLowerCase() === columnName.toLowerCase());
    if (!hit) return null;
    throw new Error(
      `Column "${hit.column}" on "${detailTable}" references ${masterTable}.${hit.referencedColumn}, not the master primary key ${actualMasterPk}. ` +
        `Use the foreign key to ${masterTable}.${actualMasterPk} (e.g. ORDER_ID). Many detail lines may share the same ${actualMasterPk}; only the detail table primary key (e.g. ORDER_ITEM_ID) must be unique.`
    );
  };

  if (pkLinks.length === 1) {
    return pkLinks[0].column;
  }

  if (pkLinks.length > 1) {
    if (client) {
      const hit = pkLinks.find((l) => l.column.toLowerCase() === client.toLowerCase());
      if (hit) return hit.column;
    }
    throw new Error(
      `Table "${detailTable}" has multiple foreign keys to ${masterTable}.${actualMasterPk}: ${pkLinks.map((l) => l.column).join(', ')}. ` +
        `Pick the detail → master link in the wizard (usually ORDER_ID).`
    );
  }

  if (client) {
    rejectNonPkMasterLink(client);
    const target = await getMysqlColumnForeignKeyTarget(db, detailSchema, detailTable, client);
    if (target && target.referencedTable.toLowerCase() !== masterTable.toLowerCase()) {
      throw new Error(
        `Column "${client}" on "${detailTable}" references "${target.referencedTable}", not master "${masterTable}". ` +
          `Use the column that foreign-keys to ${masterTable}.${actualMasterPk} (e.g. ORDER_ID), not product/customer FKs.`
      );
    }
    if (
      target &&
      target.referencedTable.toLowerCase() === masterTable.toLowerCase() &&
      target.referencedColumn.toLowerCase() === actualMasterPk.toLowerCase()
    ) {
      return client;
    }
    if (target && target.referencedTable.toLowerCase() === masterTable.toLowerCase()) {
      throw new Error(
        `Column "${client}" on "${detailTable}" references ${masterTable}.${target.referencedColumn}, not ${actualMasterPk}. ` +
          `Use ORDER_ID (or the column that FKs to the master primary key), not Order_No/ORDER_NUMBER.`
      );
    }
  }

  if (masterLinks.length > 0) {
    throw new Error(
      `Table "${detailTable}" links to "${masterTable}" via ${masterLinks.map((l) => `${l.column}→${l.referencedColumn}`).join(', ')} but not via primary key ${actualMasterPk}. ` +
        `Add or pick a foreign key to ${masterTable}.${actualMasterPk} (e.g. ORDER_ID), then recreate the master + detail screen.`
    );
  }

  return client || null;
};

const openSqlServerPoolForProfile = async (profile) => {
  const { server, instanceName, port } = parseSqlServerServerInput(profile.host, profile.port);
  const pool = new sql.ConnectionPool({
    user: String(profile.username),
    password: String(profile.password_value),
    server,
    database: String(profile.database_name),
    options: buildSqlServerTlsOptions(server, instanceName),
    ...(Number.isFinite(port) && port > 0 ? { port } : {}),
  });
  await pool.connect();
  return pool;
};

const listSqlServerDetailFkColumnsToMaster = async (
  pool,
  detailSchema,
  detailTable,
  masterSchema,
  masterTable
) => {
  const result = await pool
    .request()
    .input('detailSchema', sql.NVarChar, detailSchema)
    .input('detailTable', sql.NVarChar, detailTable)
    .input('masterSchema', sql.NVarChar, masterSchema)
    .input('masterTable', sql.NVarChar, masterTable)
    .query(`
      SELECT fk.name AS constraint_name,
             COL_NAME(fc.parent_object_id, fc.parent_column_id) AS column_name,
             COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column
      FROM sys.foreign_key_columns fc
      INNER JOIN sys.foreign_keys fk ON fk.object_id = fc.constraint_object_id
      WHERE LOWER(OBJECT_SCHEMA_NAME(fk.parent_object_id)) = LOWER(@detailSchema)
        AND LOWER(OBJECT_NAME(fk.parent_object_id)) = LOWER(@detailTable)
        AND LOWER(OBJECT_SCHEMA_NAME(fc.referenced_object_id)) = LOWER(@masterSchema)
        AND LOWER(OBJECT_NAME(fc.referenced_object_id)) = LOWER(@masterTable)
    `);
  const list = result.recordset || [];
  const byConstraint = new Map();
  for (const r of list) {
    const cn = String(r.constraint_name || '');
    if (!byConstraint.has(cn)) byConstraint.set(cn, []);
    byConstraint.get(cn).push(r);
  }
  const out = [];
  for (const [, cols] of byConstraint) {
    if (cols.length !== 1) continue;
    const column = String(cols[0].column_name || '').trim();
    const referencedColumn = String(cols[0].referenced_column || '').trim();
    if (column) out.push({ column, referencedColumn });
  }
  return out;
};

const getSqlServerColumnForeignKeyTarget = async (pool, detailSchema, detailTable, columnName) => {
  const col = String(columnName || '').trim();
  if (!col) return null;
  const result = await pool
    .request()
    .input('detailSchema', sql.NVarChar, detailSchema)
    .input('detailTable', sql.NVarChar, detailTable)
    .input('column', sql.NVarChar, col)
    .query(`
      SELECT TOP 1
             OBJECT_SCHEMA_NAME(fc.referenced_object_id) AS referenced_schema,
             OBJECT_NAME(fc.referenced_object_id) AS referenced_table,
             COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column
      FROM sys.foreign_key_columns fc
      INNER JOIN sys.foreign_keys fk ON fk.object_id = fc.constraint_object_id
      WHERE LOWER(OBJECT_SCHEMA_NAME(fk.parent_object_id)) = LOWER(@detailSchema)
        AND LOWER(OBJECT_NAME(fk.parent_object_id)) = LOWER(@detailTable)
        AND LOWER(COL_NAME(fc.parent_object_id, fc.parent_column_id)) = LOWER(@column)
    `);
  const r = result.recordset?.[0];
  if (!r) return null;
  return {
    referencedSchema: r.referenced_schema != null ? String(r.referenced_schema) : null,
    referencedTable: String(r.referenced_table || ''),
    referencedColumn: String(r.referenced_column || ''),
  };
};

const loadSqlServerTableColumnSet = async (pool, tableSchema, tableName) => {
  const result = await pool
    .request()
    .input('schema', sql.NVarChar, tableSchema)
    .input('table', sql.NVarChar, tableName)
    .query(`
      SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
      WHERE LOWER(TABLE_SCHEMA) = LOWER(@schema) AND LOWER(TABLE_NAME) = LOWER(@table)
    `);
  return new Set(
    (result.recordset || [])
      .map((r) => String(r.COLUMN_NAME || '').trim())
      .filter((n) => safeSqlIdent(n))
  );
};

const loadSqlServerNonInsertableColumns = async (pool, tableSchema, tableName) => {
  const skip = new Set();
  const result = await pool
    .request()
    .input('schema', sql.NVarChar, tableSchema)
    .input('table', sql.NVarChar, tableName)
    .query(`
      SELECT c.name AS column_name, c.is_identity
      FROM sys.columns c
      INNER JOIN sys.tables t ON c.object_id = t.object_id
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      INNER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
      INNER JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND i.is_primary_key = 1
      WHERE LOWER(s.name) = LOWER(@schema) AND LOWER(t.name) = LOWER(@table)
    `);
  for (const r of result.recordset || []) {
    const name = String(r.column_name || '').trim();
    if (!name) continue;
    if (r.is_identity) skip.add(name);
    skip.add(name);
  }
  return skip;
};

const loadSqlServerPrimaryKeyColumns = async (pool, tableSchema, tableName) => {
  const result = await pool
    .request()
    .input('schema', sql.NVarChar, tableSchema)
    .input('table', sql.NVarChar, tableName)
    .query(`
      SELECT c.name AS column_name, c.is_identity, ic.key_ordinal
      FROM sys.columns c
      INNER JOIN sys.tables t ON c.object_id = t.object_id
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      INNER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
      INNER JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND i.is_primary_key = 1
      WHERE LOWER(s.name) = LOWER(@schema) AND LOWER(t.name) = LOWER(@table)
      ORDER BY ic.key_ordinal
    `);
  return (result.recordset || []).map((r) => ({
    name: String(r.column_name || '').trim(),
    isIdentity: Boolean(r.is_identity),
  }));
};

const loadSqlServerIdentityPrimaryKey = async (pool, tableSchema, tableName) => {
  const pks = await loadSqlServerPrimaryKeyColumns(pool, tableSchema, tableName);
  const identity = pks.filter((c) => c.name && c.isIdentity);
  if (identity.length === 1) return identity[0].name;
  if (pks.length === 1 && pks[0].name) return pks[0].name;
  return null;
};

const resolveSqlServerColumnNameCi = async (pool, tableSchema, tableName, columnName) => {
  const result = await pool
    .request()
    .input('schema', sql.NVarChar, tableSchema)
    .input('table', sql.NVarChar, tableName)
    .input('column', sql.NVarChar, columnName)
    .query(`
      SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
      WHERE LOWER(TABLE_SCHEMA) = LOWER(@schema) AND LOWER(TABLE_NAME) = LOWER(@table)
        AND LOWER(COLUMN_NAME) = LOWER(@column)
    `);
  const hit = result.recordset?.[0];
  return hit ? String(hit.COLUMN_NAME || columnName) : String(columnName || '');
};

const resolveSqlServerDetailFkColumnToMaster = async (
  pool,
  detailSchema,
  detailTable,
  masterSchema,
  masterTable,
  clientFkColumn,
  masterPkColumn
) => {
  const client = String(clientFkColumn || '').trim();
  const actualMasterPk =
    (await loadSqlServerIdentityPrimaryKey(pool, masterSchema, masterTable)) ||
    String(masterPkColumn || 'id').trim();

  const masterLinks = await listSqlServerDetailFkColumnsToMaster(
    pool,
    detailSchema,
    detailTable,
    masterSchema,
    masterTable
  );

  const pkLinks = masterLinks.filter(
    (l) => l.referencedColumn.toLowerCase() === actualMasterPk.toLowerCase()
  );

  const rejectNonPkMasterLink = (columnName) => {
    const hit = masterLinks.find((l) => l.column.toLowerCase() === columnName.toLowerCase());
    if (!hit) return null;
    throw new Error(
      `Column "${hit.column}" on "${detailTable}" references ${masterTable}.${hit.referencedColumn}, not the master primary key ${actualMasterPk}. ` +
        `Use the foreign key to ${masterTable}.${actualMasterPk} (e.g. ORDER_ID). Many detail lines may share the same ${actualMasterPk}; only the detail table primary key (e.g. ORDER_ITEM_ID) must be unique.`
    );
  };

  if (pkLinks.length === 1) return pkLinks[0].column;

  if (pkLinks.length > 1) {
    if (client) {
      const hit = pkLinks.find((l) => l.column.toLowerCase() === client.toLowerCase());
      if (hit) return hit.column;
    }
    throw new Error(
      `Table "${detailTable}" has multiple foreign keys to ${masterTable}.${actualMasterPk}: ${pkLinks.map((l) => l.column).join(', ')}. ` +
        `Pick the detail → master link in the wizard (usually ORDER_ID).`
    );
  }

  if (client) {
    rejectNonPkMasterLink(client);
    const target = await getSqlServerColumnForeignKeyTarget(pool, detailSchema, detailTable, client);
    if (target && target.referencedTable.toLowerCase() !== masterTable.toLowerCase()) {
      throw new Error(
        `Column "${client}" on "${detailTable}" references "${target.referencedTable}", not master "${masterTable}". ` +
          `Use the column that foreign-keys to ${masterTable}.${actualMasterPk} (e.g. ORDER_ID), not product/customer FKs.`
      );
    }
    if (
      target &&
      target.referencedTable.toLowerCase() === masterTable.toLowerCase() &&
      target.referencedColumn.toLowerCase() === actualMasterPk.toLowerCase()
    ) {
      return client;
    }
    if (target && target.referencedTable.toLowerCase() === masterTable.toLowerCase()) {
      throw new Error(
        `Column "${client}" on "${detailTable}" references ${masterTable}.${target.referencedColumn}, not ${actualMasterPk}. ` +
          `Use ORDER_ID (or the column that FKs to the master primary key), not Order_No/ORDER_NUMBER.`
      );
    }
  }

  if (masterLinks.length > 0) {
    throw new Error(
      `Table "${detailTable}" links to "${masterTable}" via ${masterLinks.map((l) => `${l.column}→${l.referencedColumn}`).join(', ')} but not via primary key ${actualMasterPk}. ` +
        `Add or pick a foreign key to ${masterTable}.${actualMasterPk} (e.g. ORDER_ID), then recreate the master + detail screen.`
    );
  }

  return client || null;
};

const sqlServerInsertRow = async (transaction, tableSql, vals) => {
  const cols = Object.keys(vals).filter((c) => safeSqlIdent(c));
  if (cols.length === 0) return;
  const req = new sql.Request(transaction);
  cols.forEach((c, i) => {
    req.input(`p${i}`, vals[c]);
  });
  await req.query(
    `INSERT INTO ${tableSql} (${cols.map(qMs).join(', ')}) VALUES (${cols.map((_, i) => `@p${i}`).join(', ')})`
  );
};

/** Value for the detail → master link column (always the new master PK id, e.g. ORDER_ID). */
const resolveDetailMasterLinkValue = (detailFkColumn, masterLinks, actualMasterPk, masterId) => {
  const link = masterLinks.find((l) => l.column.toLowerCase() === detailFkColumn.toLowerCase());
  const refCol = link?.referencedColumn || actualMasterPk;
  if (refCol.toLowerCase() !== actualMasterPk.toLowerCase()) {
    throw new Error(
      `Detail link "${detailFkColumn}" must reference ${actualMasterPk}, not ${refCol}. Recreate the master + detail screen and choose ORDER_ID.`
    );
  }
  return masterId;
};

/**
 * MySQL / SQL Server: insert one master row then N detail rows in a single transaction.
 * Sets each detail master-link column to the new master row primary key (e.g. ORDER_ID).
 */
export const saveMasterDetailBundle = async (req, res) => {
  const { id } = req.params;
  const masterTable = String(req.body?.masterTable || '').trim();
  const masterValues = isRecord(req.body?.masterValues) ? req.body.masterValues : {};
  const detailBundlesRaw = req.body?.detailBundles;
  const legacyDetailTable = String(req.body?.detailTable || '').trim();
  const legacyDetailFk = String(req.body?.detailFkColumn || '').trim();
  const legacyRows = Array.isArray(req.body?.detailRows) ? req.body.detailRows : [];

  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  if (!masterTable) return res.status(400).json({ ok: false, message: 'Missing master table.' });

  const masterFormFieldsRaw = req.body?.masterFormFields;
  const masterFormFields =
    Array.isArray(masterFormFieldsRaw) && masterFormFieldsRaw.length > 0
      ? masterFormFieldsRaw.map((c) => String(c || '').trim()).filter(Boolean)
      : null;

  let detailBundles = [];
  if (Array.isArray(detailBundlesRaw) && detailBundlesRaw.length > 0) {
    detailBundles = detailBundlesRaw
      .map((b) => ({
        detailTable: String(b?.detailTable || '').trim(),
        detailFkColumn: String(b?.detailFkColumn || '').trim(),
        detailGridFields: Array.isArray(b?.detailGridFields) ? b.detailGridFields : null,
        detailRows: Array.isArray(b?.detailRows) ? b.detailRows : [],
      }))
      .filter((b) => b.detailTable && b.detailFkColumn);
  } else if (legacyDetailTable && legacyDetailFk) {
    detailBundles = [{ detailTable: legacyDetailTable, detailFkColumn: legacyDetailFk, detailRows: legacyRows }];
  }

  if (detailBundles.length === 0) {
    return res.status(400).json({ ok: false, message: 'No detail tables / rows to save.' });
  }

  for (const b of detailBundles) {
    if (!safeSqlIdent(b.detailFkColumn)) {
      return res.status(400).json({ ok: false, message: 'Invalid detail FK column.' });
    }
  }

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);
    if (!profile) return res.status(404).json({ ok: false, message: 'Connection not found.' });
    const connector = String(profile.connector_type || '').toLowerCase();
    if (connector !== 'mysql' && connector !== 'sqlserver') {
      return res.status(400).json({
        ok: false,
        message:
          'Master-detail bundle save is only supported for MySQL and SQL Server connections in this release.',
      });
    }

    const parseTableRef =
      connector === 'sqlserver' ? parseSafeSqlServerTableRef : parseSafeMysqlTableRef;
    const refM = parseTableRef(masterTable);
    if (!refM) {
      return res.status(400).json({ ok: false, message: 'Invalid master table name.' });
    }
    for (const b of detailBundles) {
      if (!parseTableRef(b.detailTable)) {
        return res.status(400).json({ ok: false, message: 'Invalid detail table name.' });
      }
    }

    if (connector === 'sqlserver') {
      const tableSqlM = `${qMs(refM.schema)}.${qMs(refM.table)}`;
      const masterSchemaForFk = refM.schema;
      const masterTableNameForFk = refM.table;
      const pool = await openSqlServerPoolForProfile(profile);
      const transaction = new sql.Transaction(pool);
      try {
        const actualMasterPk =
          (await loadSqlServerIdentityPrimaryKey(pool, masterSchemaForFk, masterTableNameForFk)) ||
          String(req.body?.masterPkColumn || 'id').trim();
        if (!actualMasterPk) {
          throw new Error(
            `Could not detect a single primary key on master table "${masterTableNameForFk}". ` +
              `Use one IDENTITY primary key column (e.g. ORDER_ID).`
          );
        }

        const masterSkipInsert = await loadSqlServerNonInsertableColumns(
          pool,
          masterSchemaForFk,
          masterTableNameForFk
        );
        let mCols = Object.keys(masterValues).filter((c) => safeSqlIdent(c));
        if (masterFormFields && masterFormFields.length > 0) {
          const allow = new Set(masterFormFields.filter((c) => safeSqlIdent(c)));
          mCols = mCols.filter((c) => allow.has(c));
        }
        mCols = mCols.filter((c) => !masterSkipInsert.has(c) && c !== actualMasterPk);
        if (mCols.length === 0) {
          return res.status(400).json({ ok: false, message: 'No master field values provided.' });
        }

        await transaction.begin();
        const mReq = new sql.Request(transaction);
        mCols.forEach((c, i) => {
          mReq.input(`m${i}`, masterValues[c]);
        });
        const masterOutAlias = 'bundle_master_id';
        const mResult = await mReq.query(`
          INSERT INTO ${tableSqlM} (${mCols.map(qMs).join(', ')})
          OUTPUT INSERTED.${qMs(actualMasterPk)} AS ${masterOutAlias}
          VALUES (${mCols.map((_, i) => `@m${i}`).join(', ')})
        `);
        const masterIdRaw = mResult.recordset?.[0]?.[masterOutAlias];
        const masterId = Number(masterIdRaw);
        if (!Number.isFinite(masterId) || masterId <= 0) {
          throw new Error(
            `Could not read new master row id from ${actualMasterPk}. ` +
              `The master table needs an IDENTITY primary key on ${actualMasterPk}.`
          );
        }

        for (const bundle of detailBundles) {
          const refD = parseSafeSqlServerTableRef(bundle.detailTable);
          const tableSqlD = `${qMs(refD.schema)}.${qMs(refD.table)}`;
          const detailSchemaForFk = refD.schema;
          const detailTableNameForFk = refD.table;

          const detailFkColumn = await resolveSqlServerDetailFkColumnToMaster(
            pool,
            detailSchemaForFk,
            detailTableNameForFk,
            masterSchemaForFk,
            masterTableNameForFk,
            bundle.detailFkColumn,
            actualMasterPk
          );
          if (!detailFkColumn || !safeSqlIdent(detailFkColumn)) {
            throw new Error(
              `Could not determine which column on "${detailTableNameForFk}" links to master "${masterTableNameForFk}". ` +
                `Add a foreign key in SQL Server or pick the correct column in the master + detail wizard.`
            );
          }

          const masterLinks = await listSqlServerDetailFkColumnsToMaster(
            pool,
            detailSchemaForFk,
            detailTableNameForFk,
            masterSchemaForFk,
            masterTableNameForFk
          );
          const allMasterLinkCols = new Set(masterLinks.map((l) => l.column));
          const detailLinkValue = resolveDetailMasterLinkValue(
            detailFkColumn,
            masterLinks,
            actualMasterPk,
            masterId
          );
          const skipInsertCols = await loadSqlServerNonInsertableColumns(
            pool,
            detailSchemaForFk,
            detailTableNameForFk
          );
          const detailTableCols = await loadSqlServerTableColumnSet(
            pool,
            detailSchemaForFk,
            detailTableNameForFk
          );

          const gridFields =
            Array.isArray(bundle.detailGridFields) && bundle.detailGridFields.length > 0
              ? bundle.detailGridFields
                  .map((c) => String(c || '').trim())
                  .filter((c) => safeSqlIdent(c) && detailTableCols.has(c))
              : null;

          for (const row of bundle.detailRows) {
            if (!isRecord(row)) continue;
            const vals = {};
            const assignGridCol = (c, v) => {
              if (!safeSqlIdent(c)) return;
              if (!detailTableCols.has(c)) return;
              if (c === detailFkColumn) return;
              if (allMasterLinkCols.has(c)) return;
              if (skipInsertCols.has(c)) return;
              if (v == null || String(v).trim() === '') return;
              const s = String(v).trim();
              if (s === String(masterId) && c.toLowerCase() !== detailFkColumn.toLowerCase()) {
                return;
              }
              vals[c] = v;
            };
            if (gridFields && gridFields.length > 0) {
              for (const c of gridFields) {
                assignGridCol(c, row[c]);
              }
            } else {
              for (const k of Object.keys(row)) {
                assignGridCol(k, row[k]);
              }
            }
            vals[detailFkColumn] = detailLinkValue;

            const cols = Object.keys(vals).filter((c) => safeSqlIdent(c));
            if (cols.length === 0) continue;
            const allEmpty = cols.every((c) => {
              if (c === detailFkColumn) return false;
              const v = vals[c];
              if (v == null) return true;
              return String(v).trim() === '';
            });
            if (allEmpty) continue;

            for (const c of cols) {
              if (c === detailFkColumn || allMasterLinkCols.has(c)) continue;
              const fkTarget = await getSqlServerColumnForeignKeyTarget(
                pool,
                detailSchemaForFk,
                detailTableNameForFk,
                c
              );
              if (!fkTarget?.referencedTable) continue;
              const raw = vals[c];
              if (raw == null || String(raw).trim() === '') continue;
              const refSchema = String(fkTarget.referencedSchema || detailSchemaForFk).trim() || detailSchemaForFk;
              const refTable = String(fkTarget.referencedTable || '').trim();
              if (!refTable) continue;
              const actualRefCol = await resolveSqlServerColumnNameCi(
                pool,
                refSchema,
                refTable,
                fkTarget.referencedColumn
              );
              const refSql = `${qMs(refSchema)}.${qMs(refTable)}`;
              const existsReq = new sql.Request(transaction);
              existsReq.input('fkVal', raw);
              const existsResult = await existsReq.query(
                `SELECT TOP 1 1 AS ok FROM ${refSql} WHERE ${qMs(actualRefCol)} = @fkVal`
              );
              if (!existsResult.recordset?.length) {
                throw new Error(
                  `${c} value "${String(raw).trim()}" was not found in ${refTable}.${actualRefCol}. ` +
                    `Use the product dropdown so ${c} stores the ${refTable} primary key (e.g. ${actualRefCol}), not the display name.`
                );
              }
            }

            await sqlServerInsertRow(transaction, tableSqlD, vals);
          }
        }

        await transaction.commit();
        return res.json({
          ok: true,
          masterId,
          masterPkColumn: actualMasterPk,
          message: `Saved master (${actualMasterPk}=${masterId}) and detail rows.`,
        });
      } catch (err) {
        await transaction.rollback().catch(() => {});
        throw err;
      } finally {
        await pool.close();
      }
    }

    const tableSqlM = refM.schema ? `${qMy(refM.schema)}.${qMy(refM.table)}` : qMy(refM.table);
    const dbName = String(profile.database_name || '').trim();
    const masterSchemaForFk = refM.schema || dbName;
    const masterTableNameForFk = refM.table;

    const db = await openMysqlClientForProfile(profile);
    try {
      const actualMasterPk =
        (await loadMysqlAutoIncrementPrimaryKey(db, masterSchemaForFk, masterTableNameForFk)) ||
        String(req.body?.masterPkColumn || 'id').trim();
      if (!actualMasterPk) {
        throw new Error(
          `Could not detect a single primary key on master table "${masterTableNameForFk}". ` +
            `Use one AUTO_INCREMENT primary key column (e.g. ORDER_ID).`
        );
      }

      const masterSkipInsert = await loadMysqlNonInsertableColumns(db, masterSchemaForFk, masterTableNameForFk);
      let mCols = Object.keys(masterValues).filter((c) => safeSqlIdent(c));
      if (masterFormFields && masterFormFields.length > 0) {
        const allow = new Set(masterFormFields.filter((c) => safeSqlIdent(c)));
        mCols = mCols.filter((c) => allow.has(c));
      }
      mCols = mCols.filter((c) => !masterSkipInsert.has(c) && c !== actualMasterPk);
      if (mCols.length === 0) {
        return res.status(400).json({ ok: false, message: 'No master field values provided.' });
      }

      await db.beginTransaction();
      const [mResult] = await db.query(
        `INSERT INTO ${tableSqlM} (${mCols.map((c) => qMy(c)).join(', ')}) VALUES (${mCols.map(() => '?').join(', ')})`,
        mCols.map((c) => masterValues[c])
      );
      const insertIdRaw =
        mResult && typeof mResult === 'object' && 'insertId' in mResult ? Number(mResult.insertId) : NaN;
      const masterId = insertIdRaw;
      if (!Number.isFinite(masterId) || masterId <= 0) {
        throw new Error(
          `Could not read new master row id from ${actualMasterPk}. ` +
            `The master table needs an AUTO_INCREMENT primary key on ${actualMasterPk}.`
        );
      }

      for (const bundle of detailBundles) {
        const refD = parseSafeMysqlTableRef(bundle.detailTable);
        const tableSqlD = refD.schema ? `${qMy(refD.schema)}.${qMy(refD.table)}` : qMy(refD.table);
        const detailSchemaForFk = refD.schema || dbName;
        const detailTableNameForFk = refD.table;

        const detailFkColumn = await resolveMysqlDetailFkColumnToMaster(
          db,
          detailSchemaForFk,
          detailTableNameForFk,
          masterSchemaForFk,
          masterTableNameForFk,
          bundle.detailFkColumn,
          actualMasterPk
        );
        if (!detailFkColumn || !safeSqlIdent(detailFkColumn)) {
          throw new Error(
            `Could not determine which column on "${detailTableNameForFk}" links to master "${masterTableNameForFk}". ` +
              `Add a foreign key in MySQL or pick the correct column in the master + detail wizard.`
          );
        }

        const masterLinks = await listMysqlDetailFkColumnsToMaster(
          db,
          detailSchemaForFk,
          detailTableNameForFk,
          masterSchemaForFk,
          masterTableNameForFk
        );
        const allMasterLinkCols = new Set(masterLinks.map((l) => l.column));
        const detailLinkValue = resolveDetailMasterLinkValue(
          detailFkColumn,
          masterLinks,
          actualMasterPk,
          masterId
        );
        const skipInsertCols = await loadMysqlNonInsertableColumns(db, detailSchemaForFk, detailTableNameForFk);
        const detailTableCols = await loadMysqlTableColumnSet(db, detailSchemaForFk, detailTableNameForFk);

        const gridFields =
          Array.isArray(bundle.detailGridFields) && bundle.detailGridFields.length > 0
            ? bundle.detailGridFields
                .map((c) => String(c || '').trim())
                .filter((c) => safeSqlIdent(c) && detailTableCols.has(c))
            : null;

        for (const row of bundle.detailRows) {
          if (!isRecord(row)) continue;
          const vals = {};
          const assignGridCol = (c, v) => {
            if (!safeSqlIdent(c)) return;
            if (!detailTableCols.has(c)) return;
            if (c === detailFkColumn) return;
            if (allMasterLinkCols.has(c)) return;
            if (skipInsertCols.has(c)) return;
            if (v == null || String(v).trim() === '') return;
            const s = String(v).trim();
            if (s === String(masterId) && c.toLowerCase() !== detailFkColumn.toLowerCase()) {
              return;
            }
            vals[c] = v;
          };
          if (gridFields && gridFields.length > 0) {
            for (const c of gridFields) {
              assignGridCol(c, row[c]);
            }
          } else {
            for (const k of Object.keys(row)) {
              assignGridCol(k, row[k]);
            }
          }
          vals[detailFkColumn] = detailLinkValue;

          const cols = Object.keys(vals).filter((c) => safeSqlIdent(c));
          if (cols.length === 0) continue;
          const allEmpty = cols.every((c) => {
            if (c === detailFkColumn) return false;
            const v = vals[c];
            if (v == null) return true;
            return String(v).trim() === '';
          });
          if (allEmpty) continue;

          for (const c of cols) {
            if (c === detailFkColumn || allMasterLinkCols.has(c)) continue;
            const fkTarget = await getMysqlColumnForeignKeyTarget(
              db,
              detailSchemaForFk,
              detailTableNameForFk,
              c
            );
            if (!fkTarget?.referencedTable) continue;
            const raw = vals[c];
            if (raw == null || String(raw).trim() === '') continue;
            const refSchema = String(fkTarget.referencedSchema || detailSchemaForFk || dbName).trim() || dbName;
            const refTable = String(fkTarget.referencedTable || '').trim();
            if (!refTable) continue;
            const actualRefCol = await resolveMysqlColumnNameCi(
              db,
              refSchema,
              refTable,
              fkTarget.referencedColumn
            );
            const refSql = `${qMy(refSchema)}.${qMy(refTable)}`;
            const [existsRows] = await db.query(
              `SELECT 1 AS ok FROM ${refSql} WHERE ${qMy(actualRefCol)} = ? LIMIT 1`,
              [raw]
            );
            if (!Array.isArray(existsRows) || existsRows.length === 0) {
              throw new Error(
                `${c} value "${String(raw).trim()}" was not found in ${refTable}.${actualRefCol}. ` +
                  `Use the product dropdown so ${c} stores the ${refTable} primary key (e.g. ${actualRefCol}), not the display name.`
              );
            }
          }

          await db.query(
            `INSERT INTO ${tableSqlD} (${cols.map((c) => qMy(c)).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`,
            cols.map((c) => vals[c])
          );
        }
      }

      await db.commit();
      return res.json({
        ok: true,
        masterId,
        masterPkColumn: actualMasterPk,
        message: `Saved master (${actualMasterPk}=${masterId}) and detail rows.`,
      });
    } catch (err) {
      await db.rollback().catch(() => {});
      throw err;
    } finally {
      await db.end();
    }
  } catch (error) {
    const message = formatDbConstraintError(error);
    const status = /foreign key|duplicate key|referenced/i.test(message) ? 400 : 500;
    return res.status(status).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};

const pickLabelColumnName = (columnRows, pkColumn) => {
  const pkLower = String(pkColumn || '').toLowerCase();
  const cols = columnRows
    .map((r) => ({
      name: String(r.COLUMN_NAME || r.column_name || ''),
      dataType: String(r.DATA_TYPE || r.data_type || ''),
    }))
    .filter((c) => c.name);
  const preferred = [
    'name',
    'customer_name',
    'product_name',
    'company_name',
    'title',
    'label',
    'description',
    'display_name',
    'full_name',
    'email',
    'code',
  ];
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
  const includeFields =
    String(req.query.includeFields || '').trim() === '1' ||
    String(req.query.includeFields || '').toLowerCase() === 'true';

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
        const labelColPick = pickLabelColumnName(colRows, pkColumn);
        const labelColName = labelColPick || pkColumn;
        const pkQ = qMy(pkColumn);
        const labelQ = qMy(labelColName);
        const tableSql = `${qMy(schema)}.${qMy(tname)}`;
        const colNames = (colRows || [])
          .map((r) => String(r.COLUMN_NAME || r.column_name || ''))
          .filter((n) => safeSqlIdent(n));
        let dataRows;
        if (includeFields && colNames.length > 0) {
          const selectList = colNames.map((c) => `${qMy(c)}`).join(', ');
          ;[dataRows] = await db.query(`SELECT ${selectList} FROM ${tableSql} ORDER BY ${pkQ} ASC LIMIT ?`, [limit]);
        } else {
          ;[dataRows] = await db.query(
            `SELECT ${pkQ} AS __v, ${labelQ} AS __l FROM ${tableSql} ORDER BY ${pkQ} ASC LIMIT ?`,
            [limit]
          );
        }
        const options = (dataRows || []).map((r) => {
          if (
            includeFields &&
            colNames.length > 0 &&
            r &&
            typeof r === 'object' &&
            !Object.prototype.hasOwnProperty.call(r, '__v')
          ) {
            const valueRaw = pickRowFieldCi(r, pkColumn);
            const value = valueRaw == null ? '' : String(valueRaw);
            const labelRaw = pickRowFieldCi(r, labelColName);
            const label = labelRaw == null ? value : String(labelRaw);
            const fields = Object.fromEntries(
              colNames.map((c) => {
                const cell = pickRowFieldCi(r, c);
                return [c, cell == null ? '' : String(cell)];
              })
            );
            return { value, label: label || value, fields };
          }
          const value = r.__v == null ? '' : String(r.__v);
          const labelRaw = r.__l == null ? value : String(r.__l);
          return { value, label: labelRaw || value };
        });
        return res.json({ ok: true, options, labelColumn: labelColName });
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
        const colNames = (colResult.rows || [])
          .map((r) => String(r.column_name || ''))
          .filter((n) => safeSqlIdent(n));
        let dataResult;
        if (includeFields && colNames.length > 0) {
          const selectList = colNames.map((c) => `${qPg(c)}::text`).join(', ');
          dataResult = await client.query(
            `SELECT ${selectList} FROM ${tableSql} ORDER BY ${qPg(pkColumn)} ASC NULLS LAST LIMIT $1`,
            [limit]
          );
        } else {
          dataResult = await client.query(
            `SELECT ${qPg(pkColumn)}::text AS v, ${qPg(labelName)}::text AS l
             FROM ${tableSql}
             ORDER BY 1
             LIMIT $1`,
            [limit]
          );
        }
        const options = (dataResult.rows || []).map((r) => {
          if (includeFields && colNames.length > 0 && r && typeof r === 'object') {
            const keys = Object.keys(r);
            const isCompactVl = keys.length === 2 && keys.includes('v') && keys.includes('l');
            if (!isCompactVl) {
            const pick = (name) => {
              if (name in r) return r[name];
              const lower = name.toLowerCase();
              const hit = Object.keys(r).find((k) => k.toLowerCase() === lower);
              return hit ? r[hit] : undefined;
            };
            const value = pick(pkColumn) == null ? '' : String(pick(pkColumn));
            const labelRaw = pick(labelName) == null ? value : String(pick(labelName));
            const fields = Object.fromEntries(
              colNames.map((c) => [c, pick(c) == null ? '' : String(pick(c))])
            );
            return { value, label: labelRaw || value, fields };
            }
          }
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
        const colNames = (colResult.recordset || [])
          .map((r) => String(r.COLUMN_NAME || r.column_name || ''))
          .filter((n) => safeSqlIdent(n));
        const dataReq = pool.request();
        dataReq.input('lim', sql.Int, limit);
        let dataResult;
        if (includeFields && colNames.length > 0) {
          const selectList = colNames.map((c) => `${qMs(c)}`).join(', ');
          dataResult = await dataReq.query(
            `SELECT TOP (@lim) ${selectList} FROM ${tableSql} ORDER BY ${qMs(pkColumn)}`
          );
        } else {
          dataResult = await dataReq.query(
            `SELECT TOP (@lim) ${qMs(pkColumn)} AS v, ${qMs(labelName)} AS l FROM ${tableSql} ORDER BY ${qMs(pkColumn)}`
          );
        }
        const options = (dataResult.recordset || []).map((r) => {
          const row = r || {};
          if (includeFields && colNames.length > 0) {
            const keys = Object.keys(row);
            const isCompactVl = keys.length === 2 && keys.includes('v') && keys.includes('l');
            if (!isCompactVl) {
            const pick = (name) => {
              if (Object.prototype.hasOwnProperty.call(row, name)) return row[name];
              const lower = name.toLowerCase();
              const hit = Object.keys(row).find((k) => k.toLowerCase() === lower);
              return hit ? row[hit] : undefined;
            };
            const value = pick(pkColumn) == null ? '' : String(pick(pkColumn));
            const labelRaw = pick(labelName) == null ? value : String(pick(labelName));
            const fields = Object.fromEntries(
              colNames.map((c) => [c, pick(c) == null ? '' : String(pick(c))])
            );
            return { value, label: labelRaw || value, fields };
            }
          }
          const value = row.v == null ? '' : String(row.v);
          const labelRaw = row.l == null ? value : String(row.l);
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
