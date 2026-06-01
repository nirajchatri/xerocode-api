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

const BATCH_SIZE = 150;
const MAX_ROWS = 50_000;

const isRecord = (v) => v && typeof v === 'object' && !Array.isArray(v);

const loadProfile = async (pool, id) => {
  const result = await pool.request().input('id', sql.Int, Number(id)).query(`
    SELECT TOP 1 id, connector_type, host, port, database_name, username, password_value
    FROM dbo.connection_profiles
    WHERE id = @id
  `);
  return Array.isArray(result.recordset) && result.recordset.length > 0 ? result.recordset[0] : null;
};

/** Safe SQL identifier: letters, numbers, underscore; leading digit prefixed. */
export const sanitizeSqlIdentifier = (raw, fallback = 'column') => {
  let s = String(raw ?? '')
    .trim()
    .replace(/[^\w]+/g, '_')
    .replace(/^_+|_+$/g, '');
  if (!s) s = fallback;
  if (/^\d/.test(s)) s = `_${s}`;
  return s.slice(0, 120);
};

export const normalizeColumnTypeForDialect = (columnType, type, dialect) => {
  const raw = String(columnType || type || 'text').trim();
  const u = raw.toUpperCase();

  if (dialect === 'mysql') {
    if (u.includes('BIGINT')) return 'BIGINT';
    if (u.includes('INT')) return 'INT';
    if (u.includes('DECIMAL') || u.includes('NUMERIC') || u.includes('FLOAT') || u.includes('DOUBLE')) {
      const m = u.match(/DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/i);
      if (m) return `DECIMAL(${m[1]},${m[2]})`;
      return 'DECIMAL(18,4)';
    }
    if (u.includes('BIT') || u.includes('BOOL')) return 'TINYINT(1)';
    if (u.includes('DATETIME')) return 'DATETIME';
    if (u === 'DATE' || u.includes('DATE')) return 'DATE';
    if (u.includes('MAX')) return 'LONGTEXT';
    const vm = u.match(/NVARCHAR\s*\(\s*(\d+)\s*\)/i) || u.match(/VARCHAR\s*\(\s*(\d+)\s*\)/i);
    if (vm) return `VARCHAR(${Math.min(Number(vm[1]) || 255, 16383)})`;
    return 'VARCHAR(255)';
  }

  if (dialect === 'postgresql') {
    if (u.includes('BIGINT')) return 'BIGINT';
    if (u.includes('INT')) return 'INTEGER';
    if (u.includes('DECIMAL') || u.includes('NUMERIC')) {
      const m = u.match(/DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/i);
      if (m) return `NUMERIC(${m[1]},${m[2]})`;
      return 'NUMERIC(18,4)';
    }
    if (u.includes('BIT') || u.includes('BOOL')) return 'BOOLEAN';
    if (u.includes('DATETIME') || u.includes('TIMESTAMP')) return 'TIMESTAMP';
    if (u === 'DATE' || u.includes('DATE')) return 'DATE';
    if (u.includes('MAX') || u.includes('TEXT')) return 'TEXT';
    const vm = u.match(/NVARCHAR\s*\(\s*(\d+)\s*\)/i) || u.match(/VARCHAR\s*\(\s*(\d+)\s*\)/i);
    if (vm) return `VARCHAR(${Math.min(Number(vm[1]) || 255, 10485760)})`;
    return 'VARCHAR(255)';
  }

  if (u.includes('BIGINT')) return 'BIGINT';
  if (u.includes('INT')) return 'INT';
  if (u.includes('DECIMAL') || u.includes('NUMERIC')) {
    const m = u.match(/DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/i);
    if (m) return `DECIMAL(${m[1]},${m[2]})`;
    return 'DECIMAL(18,4)';
  }
  if (u.includes('BIT') || u.includes('BOOL')) return 'BIT';
  if (u.includes('DATETIME')) return 'DATETIME2';
  if (u === 'DATE' || u.includes('DATE')) return 'DATE';
  if (u.includes('MAX')) return 'NVARCHAR(MAX)';
  const vm = u.match(/NVARCHAR\s*\(\s*(\d+)\s*\)/i) || u.match(/VARCHAR\s*\(\s*(\d+)\s*\)/i);
  if (vm) return `NVARCHAR(${Math.min(Number(vm[1]) || 255, 4000)})`;
  return 'NVARCHAR(255)';
};

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}/;
const US_DATE_RE = /^\d{1,2}\/\d{1,2}\/\d{2,4}/;
const PLAIN_NUM_RE = /^-?\d+(\.\d+)?$/;

const parseToDate = (raw) => {
  const s = raw == null ? '' : String(raw).trim();
  if (!s) return null;
  const compact = s.replace(/,/g, '');
  if (/^\d{4,5}(\.\d+)?$/.test(compact)) {
    const serial = Number(compact);
    if (serial > 20_000 && serial < 80_000) {
      const excelEpoch = Date.UTC(1899, 11, 30);
      const d = new Date(excelEpoch + serial * 86_400_000);
      return Number.isFinite(d.getTime()) ? d : null;
    }
  }
  const us = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s*(?:AM|PM))?)?/i);
  if (us) {
    let y = parseInt(us[3], 10);
    if (y < 100) y += y < 50 ? 2000 : 1900;
    let h = us[4] ? parseInt(us[4], 10) : 0;
    const mi = us[5] ? parseInt(us[5], 10) : 0;
    const sec = us[6] ? parseInt(us[6], 10) : 0;
    if (/pm/i.test(s) && h < 12) h += 12;
    if (/am/i.test(s) && h === 12) h = 0;
    const d = new Date(y, parseInt(us[1], 10) - 1, parseInt(us[2], 10), h, mi, sec);
    return Number.isFinite(d.getTime()) ? d : null;
  }
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?/);
  if (iso) {
    const d = new Date(parseInt(iso[1], 10), parseInt(iso[2], 10) - 1, parseInt(iso[3], 10), iso[4] ? parseInt(iso[4], 10) : 0, iso[5] ? parseInt(iso[5], 10) : 0, iso[6] ? parseInt(iso[6], 10) : 0);
    return Number.isFinite(d.getTime()) ? d : null;
  }
  if (ISO_DATE_RE.test(s) || US_DATE_RE.test(s)) {
    const parsed = Date.parse(s);
    if (Number.isFinite(parsed)) return new Date(parsed);
  }
  if (PLAIN_NUM_RE.test(compact)) return null;
  const parsed = Date.parse(s);
  if (!Number.isFinite(parsed)) return null;
  return new Date(parsed);
};

const isDateSqlType = (sqlType) => {
  const u = String(sqlType || '').toUpperCase();
  return u === 'DATE' || u.includes('DATETIME') || u.includes('TIMESTAMP') || u === 'SMALLDATETIME';
};

const coerceCellValue = (raw, sqlType) => {
  const s = raw == null ? '' : String(raw).trim();
  if (s === '') return null;
  const u = sqlType.toUpperCase();
  if (isDateSqlType(sqlType)) return parseToDate(s);
  if (u.includes('INT') || u.includes('BIGINT')) {
    const n = Number(s.replace(/,/g, ''));
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }
  if (u.includes('DECIMAL') || u.includes('NUMERIC') || u.includes('FLOAT') || u.includes('DOUBLE')) {
    const n = Number(s.replace(/,/g, ''));
    return Number.isFinite(n) ? n : null;
  }
  if (u.includes('BIT') || u === 'BOOLEAN' || u.includes('TINYINT(1)')) {
    if (/^(true|yes|y|1)$/i.test(s)) return 1;
    if (/^(false|no|n|0)$/i.test(s)) return 0;
    return null;
  }
  return s;
};

const formatDateForMysql = (d) => {
  if (!(d instanceof Date) || !Number.isFinite(d.getTime())) return null;
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
};

const bindMssqlParam = (req, paramName, sqlType, value) => {
  const u = String(sqlType || '').toUpperCase();
  if (value === null || value === undefined) {
    if (u.includes('BIGINT')) return req.input(paramName, sql.BigInt, null);
    if (u.includes('INT')) return req.input(paramName, sql.Int, null);
    if (u.includes('DECIMAL') || u.includes('NUMERIC')) {
      const m = u.match(/DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/i);
      return req.input(paramName, sql.Decimal(m ? parseInt(m[1], 10) : 18, m ? parseInt(m[2], 10) : 4), null);
    }
    if (u.includes('BIT')) return req.input(paramName, sql.Bit, null);
    if (u.includes('DATETIME')) return req.input(paramName, sql.DateTime2, null);
    if (u === 'DATE') return req.input(paramName, sql.Date, null);
    return req.input(paramName, sql.NVarChar, null);
  }
  if (value instanceof Date) {
    if (u.includes('DATETIME')) return req.input(paramName, sql.DateTime2, value);
    if (u === 'DATE' || (u.includes('DATE') && !u.includes('TIME'))) return req.input(paramName, sql.Date, value);
  }
  if (u.includes('BIGINT')) return req.input(paramName, sql.BigInt, value);
  if (u.includes('INT')) return req.input(paramName, sql.Int, value);
  if (u.includes('DECIMAL') || u.includes('NUMERIC')) {
    const m = u.match(/DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/i);
    return req.input(paramName, sql.Decimal(m ? parseInt(m[1], 10) : 18, m ? parseInt(m[2], 10) : 4), value);
  }
  if (u.includes('BIT')) return req.input(paramName, sql.Bit, value);
  if (u.includes('DATETIME')) {
    const d = value instanceof Date ? value : parseToDate(value);
    return req.input(paramName, sql.DateTime2, d);
  }
  if (u === 'DATE' || (u.includes('DATE') && !u.includes('TIME'))) {
    const d = value instanceof Date ? value : parseToDate(value);
    return req.input(paramName, sql.Date, d);
  }
  return req.input(paramName, sql.NVarChar, String(value));
};

const buildColumnDefs = (columns, dialect) => {
  const seen = new Set();
  const out = [];
  for (let i = 0; i < columns.length; i += 1) {
    const col = columns[i];
    const name = sanitizeSqlIdentifier(col?.name, `column_${i + 1}`);
    let unique = name;
    let n = 2;
    while (seen.has(unique.toLowerCase())) {
      unique = `${name}_${n}`;
      n += 1;
    }
    seen.add(unique.toLowerCase());
    const sqlType = normalizeColumnTypeForDialect(col?.columnType, col?.type, dialect);
    out.push({ name: unique, sqlType });
  }
  return out;
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

const parseSafeMysqlTableRef = (table, defaultSchema) => {
  const t = String(table || '').trim();
  if (!t || !/^([a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+$/.test(t)) return null;
  if (t.includes('.')) {
    const [schema, name] = t.split('.');
    return { schema, table: name };
  }
  return { schema: defaultSchema || null, table: t };
};

const buildCreateTableSql = (ref, colDefs, dialect) => {
  const colsSql = colDefs.map((c) => {
    if (dialect === 'mysql') return `${qMy(c.name)} ${c.sqlType} NULL`;
    if (dialect === 'postgresql') return `${qPg(c.name)} ${c.sqlType} NULL`;
    return `${qMs(c.name)} ${c.sqlType} NULL`;
  });
  return `CREATE TABLE ${qualifiedTableName(ref, dialect)} (${colsSql.join(', ')})`;
};

async function tableExistsMysql(db, ref) {
  const schema =
    ref.schema ||
    String((await db.query('SELECT DATABASE() AS db'))[0]?.[0]?.db || '');
  const [rows] = await db.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE' LIMIT 1`,
    [schema, ref.table]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function tableExistsPostgres(client, ref) {
  const r = await client.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE' LIMIT 1`,
    [ref.schema, ref.table]
  );
  return (r.rows || []).length > 0;
}

async function tableExistsSqlServer(pool, ref) {
  const r = await pool
    .request()
    .input('schema', sql.NVarChar, ref.schema)
    .input('table', sql.NVarChar, ref.table)
    .query(
      `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND TABLE_TYPE = 'BASE TABLE'`
    );
  return (r.recordset || []).length > 0;
}

async function loadSqlServerTableColumns(pool, ref) {
  const r = await pool
    .request()
    .input('schema', sql.NVarChar, ref.schema)
    .input('table', sql.NVarChar, ref.table)
    .query(`
      SELECT
        c.COLUMN_NAME AS name,
        c.DATA_TYPE AS data_type,
        CASE
          WHEN COLUMNPROPERTY(
            OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table)),
            c.COLUMN_NAME,
            'IsIdentity'
          ) = 1 THEN 1
          ELSE 0
        END AS is_identity
      FROM INFORMATION_SCHEMA.COLUMNS c
      WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
      ORDER BY c.ORDINAL_POSITION
    `);
  return (r.recordset || []).map((row) => ({
    name: String(row.name),
    data_type: String(row.data_type || 'nvarchar'),
    is_identity: row.is_identity === 1,
  }));
}

async function loadMysqlTableColumns(db, ref) {
  const schema =
    ref.schema ||
    String((await db.query('SELECT DATABASE() AS db'))[0]?.[0]?.db || '');
  const [rows] = await db.query(
    `SELECT COLUMN_NAME AS name, DATA_TYPE AS data_type,
      CASE WHEN EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END AS is_identity
     FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
     ORDER BY ORDINAL_POSITION`,
    [schema, ref.table]
  );
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    name: String(row.name),
    data_type: String(row.data_type || 'varchar'),
    is_identity: row.is_identity === 1,
  }));
}

async function loadPostgresTableColumns(client, ref) {
  const r = await client.query(
    `SELECT column_name AS name, data_type,
      CASE WHEN is_identity = 'YES' THEN 1 ELSE 0 END AS is_identity
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position`,
    [ref.schema, ref.table]
  );
  return (r.rows || []).map((row) => ({
    name: String(row.name),
    data_type: String(row.data_type || 'character varying'),
    is_identity: row.is_identity === 1,
  }));
}

/** Map uploaded sheet columns to existing DB table columns (by name, case-insensitive). */
function buildInsertMapping(uploadColumns, uploadRows, dbColumns, dialect) {
  const colDefs = [];
  const uploadIndices = [];
  for (const dbCol of dbColumns) {
    if (dbCol.is_identity) continue;
    const dbName = dbCol.name;
    let excelIdx = -1;
    for (let i = 0; i < uploadColumns.length; i += 1) {
      const uploadName = String(uploadColumns[i]?.name ?? '').trim();
      if (!uploadName) continue;
      if (
        uploadName.toLowerCase() === dbName.toLowerCase() ||
        sanitizeSqlIdentifier(uploadName).toLowerCase() === dbName.toLowerCase()
      ) {
        excelIdx = i;
        break;
      }
    }
    if (excelIdx < 0) continue;
    colDefs.push({
      name: dbName,
      sqlType: normalizeColumnTypeForDialect(dbCol.data_type, dbCol.data_type, dialect),
    });
    uploadIndices.push(excelIdx);
  }
  if (!colDefs.length) {
    throw new Error(
      'No Excel columns match the target table. Ensure row 1 headers match database column names.'
    );
  }
  const mappedRows = uploadRows.map((row) => uploadIndices.map((idx) => row[idx] ?? ''));
  return { colDefs, rows: mappedRows };
}

async function dropTable(ref, dialect, db, client, pool) {
  const tableSql = qualifiedTableName(ref, dialect);
  if (dialect === 'mysql') {
    await db.query(`DROP TABLE IF EXISTS ${tableSql}`);
    return;
  }
  if (dialect === 'postgresql') {
    await client.query(`DROP TABLE IF EXISTS ${tableSql}`);
    return;
  }
  await pool.request().query(`DROP TABLE IF EXISTS ${tableSql}`);
}

async function insertRowsMysql(db, ref, colDefs, rows) {
  const tableSql = qualifiedTableName(ref, 'mysql');
  const limited = rows.slice(0, MAX_ROWS);
  const colNames = colDefs.map((c) => qMy(c.name)).join(', ');
  for (let i = 0; i < limited.length; i += BATCH_SIZE) {
    const batch = limited.slice(i, i + BATCH_SIZE);
    const placeholders = batch.map(() => `(${colDefs.map(() => '?').join(', ')})`).join(', ');
    const flat = [];
    for (const row of batch) {
      for (let j = 0; j < colDefs.length; j += 1) {
        const v = coerceCellValue(row[j], colDefs[j].sqlType);
        flat.push(v instanceof Date ? formatDateForMysql(v) : v);
      }
    }
    await db.query(`INSERT INTO ${tableSql} (${colNames}) VALUES ${placeholders}`, flat);
  }
}

async function insertRowsPostgres(client, ref, colDefs, rows) {
  const tableSql = qualifiedTableName(ref, 'postgresql');
  const limited = rows.slice(0, MAX_ROWS);
  const colNames = colDefs.map((c) => qPg(c.name)).join(', ');
  for (const row of limited) {
    const vals = colDefs.map((c, j) => {
      const v = coerceCellValue(row[j], c.sqlType);
      return v instanceof Date ? v.toISOString() : v;
    });
    const params = vals.map((_, idx) => `$${idx + 1}`).join(', ');
    await client.query(`INSERT INTO ${tableSql} (${colNames}) VALUES (${params})`, vals);
  }
}

async function insertRowsSqlServer(pool, ref, colDefs, rows) {
  const tableSql = qualifiedTableName(ref, 'sqlserver');
  const limited = rows.slice(0, MAX_ROWS);
  const colNames = colDefs.map((c) => qMs(c.name)).join(', ');
  const paramList = colDefs.map((_, j) => `@p${j}`).join(', ');
  for (const row of limited) {
    const req = pool.request();
    colDefs.forEach((c, j) => {
      const value = coerceCellValue(row[j], c.sqlType);
      bindMssqlParam(req, `p${j}`, c.sqlType, value);
    });
    await req.query(`INSERT INTO ${tableSql} (${colNames}) VALUES (${paramList})`);
  }
}

const resolveSpreadsheetMode = (payload) => {
  const rawMode = String(payload?.mode || '').toLowerCase();
  if (rawMode === 'insert' || rawMode === 'create' || rawMode === 'auto') return rawMode;
  if (String(payload?.ifExists || '').toLowerCase() === 'insert') return 'insert';
  return 'auto';
};

export async function createTableFromSpreadsheetOnProfile(profile, payload) {
  const tableNameRaw = String(payload?.tableName || '').trim();
  const schemaNameRaw = String(payload?.schemaName || '').trim();
  const ifExists = String(payload?.ifExists || 'fail').toLowerCase();
  const mode = resolveSpreadsheetMode(payload);
  const columns = Array.isArray(payload?.columns) ? payload.columns : [];
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];

  if (!tableNameRaw) throw new Error('Missing table name.');
  if (!columns.length) throw new Error('Missing column definitions.');

  const connector = profile.connector_type;
  if (!['mysql', 'sqlserver', 'postgresql'].includes(connector)) {
    throw new Error('Create table is only supported for MySQL, SQL Server, and PostgreSQL.');
  }

  const dialect =
    connector === 'postgresql' ? 'postgresql' : connector === 'mysql' ? 'mysql' : 'sqlserver';
  const colDefs = buildColumnDefs(columns, dialect);
  const tableSanitized = sanitizeSqlIdentifier(tableNameRaw, 'uploaded_table');

  let ref;
  if (dialect === 'mysql') {
    const schema = schemaNameRaw
      ? sanitizeSqlIdentifier(schemaNameRaw, 'schema')
      : String(profile.database_name || '');
    ref = parseSafeMysqlTableRef(
      schema ? `${schema}.${tableSanitized}` : tableSanitized,
      String(profile.database_name || '')
    );
    if (!ref) throw new Error('Invalid table name.');
  } else if (dialect === 'postgresql') {
    const schema = sanitizeSqlIdentifier(schemaNameRaw || 'public', 'public');
    ref = parseSafePostgresTableRef(`${schema}.${tableSanitized}`);
    if (!ref) throw new Error('Invalid table name.');
  } else {
    const schema = sanitizeSqlIdentifier(schemaNameRaw || 'dbo', 'dbo');
    ref = parseSafeSqlServerTableRef(`${schema}.${tableSanitized}`);
    if (!ref) throw new Error('Invalid table name.');
  }

  const qualified =
    dialect === 'mysql'
      ? ref.schema
        ? `${ref.schema}.${ref.table}`
        : ref.table
      : `${ref.schema}.${ref.table}`;

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
      const exists = await tableExistsMysql(db, ref);
      if (exists && (mode === 'insert' || mode === 'auto')) {
        const dbCols = await loadMysqlTableColumns(db, ref);
        const mapped = buildInsertMapping(columns, rows, dbCols, dialect);
        await insertRowsMysql(db, ref, mapped.colDefs, mapped.rows);
        return {
          qualifiedName: qualified,
          created: false,
          rowsInserted: Math.min(mapped.rows.length, MAX_ROWS),
          insertedIntoExisting: true,
        };
      }
      if (exists) {
        if (ifExists === 'skip') {
          return { qualifiedName: qualified, created: false, rowsInserted: 0, skipped: true };
        }
        if (ifExists === 'fail') throw new Error(`Table ${qualified} already exists.`);
        await dropTable(ref, dialect, db, null, null);
      }
      if (mode === 'insert') {
        throw new Error(`Table ${qualified} does not exist. Open it in the explorer or use create mode.`);
      }
      await db.query(buildCreateTableSql(ref, colDefs, dialect));
      await insertRowsMysql(db, ref, colDefs, rows);
      return { qualifiedName: qualified, created: true, rowsInserted: Math.min(rows.length, MAX_ROWS) };
    } finally {
      await db.end();
    }
  }

  if (dialect === 'postgresql') {
    const client = new pg.Client({
      host: normalizeHost(profile.host),
      port: Number(profile.port) > 0 ? Number(profile.port) : 5432,
      database: String(profile.database_name),
      user: String(profile.username),
      password: String(profile.password_value),
      ssl: false,
    });
    await client.connect();
    try {
      const exists = await tableExistsPostgres(client, ref);
      if (exists && (mode === 'insert' || mode === 'auto')) {
        const dbCols = await loadPostgresTableColumns(client, ref);
        const mapped = buildInsertMapping(columns, rows, dbCols, dialect);
        await insertRowsPostgres(client, ref, mapped.colDefs, mapped.rows);
        return {
          qualifiedName: qualified,
          created: false,
          rowsInserted: Math.min(mapped.rows.length, MAX_ROWS),
          insertedIntoExisting: true,
        };
      }
      if (exists) {
        if (ifExists === 'skip') {
          return { qualifiedName: qualified, created: false, rowsInserted: 0, skipped: true };
        }
        if (ifExists === 'fail') throw new Error(`Table ${qualified} already exists.`);
        await dropTable(ref, dialect, null, client, null);
      }
      if (mode === 'insert') {
        throw new Error(`Table ${qualified} does not exist. Open it in the explorer or use create mode.`);
      }
      await client.query(buildCreateTableSql(ref, colDefs, dialect));
      await insertRowsPostgres(client, ref, colDefs, rows);
      return { qualifiedName: qualified, created: true, rowsInserted: Math.min(rows.length, MAX_ROWS) };
    } finally {
      await client.end();
    }
  }

  const { server, instanceName, port } = parseSqlServerServerInput(profile.host, profile.port);
  const config = {
    user: String(profile.username),
    password: String(profile.password_value),
    server,
    database: String(profile.database_name),
    options: buildSqlServerTlsOptions(server, instanceName),
    connectionTimeout: 15_000,
    requestTimeout: 120_000,
  };
  if (instanceName) {
    if (port && Number.isFinite(port) && port > 0) config.port = port;
  } else {
    config.port = port && Number.isFinite(port) && port > 0 ? port : 1433;
  }

  const pool = await sql.connect(config);
  try {
    const exists = await tableExistsSqlServer(pool, ref);
    if (exists && (mode === 'insert' || mode === 'auto')) {
      const dbCols = await loadSqlServerTableColumns(pool, ref);
      const mapped = buildInsertMapping(columns, rows, dbCols, dialect);
      await insertRowsSqlServer(pool, ref, mapped.colDefs, mapped.rows);
      return {
        qualifiedName: qualified,
        created: false,
        rowsInserted: Math.min(mapped.rows.length, MAX_ROWS),
        insertedIntoExisting: true,
      };
    }
    if (exists) {
      if (ifExists === 'skip') {
        return { qualifiedName: qualified, created: false, rowsInserted: 0, skipped: true };
      }
      if (ifExists === 'fail') throw new Error(`Table ${qualified} already exists.`);
      await dropTable(ref, dialect, null, null, pool);
    }
    if (mode === 'insert') {
      throw new Error(`Table ${qualified} does not exist. Open it in the explorer or use create mode.`);
    }
    await pool.request().query(buildCreateTableSql(ref, colDefs, dialect));
    await insertRowsSqlServer(pool, ref, colDefs, rows);
    return { qualifiedName: qualified, created: true, rowsInserted: Math.min(rows.length, MAX_ROWS) };
  } finally {
    await pool.close();
  }
}

export const createConnectionTableFromSpreadsheet = async (req, res) => {
  const { id } = req.params;
  if (!id) {
    return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  }
  if (!isRecord(req.body)) {
    return res.status(400).json({ ok: false, message: 'Invalid request body.' });
  }

  let controlConnection;
  try {
    controlConnection = await connectToControlSqlServer();
    const profile = await loadProfile(controlConnection, id);
    if (!profile) {
      return res.status(404).json({ ok: false, message: 'Connection not found.' });
    }
    const out = await createTableFromSpreadsheetOnProfile(profile, req.body);
    return res.json({ ok: true, ...out });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to create table.';
    const status = /already exists/i.test(message) ? 409 : 500;
    return res.status(status).json({ ok: false, message });
  } finally {
    if (controlConnection) {
      await closeControlSqlServer(controlConnection);
    }
  }
};
