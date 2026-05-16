import pg from 'pg';
import { normalizeHost } from './hostUtils.js';
import { buildPostgresWhere } from './tableDataQuery.js';

const buildPostgresClient = (row) => {
  const host = normalizeHost(row.host);
  const portNum = Number(row.port);
  return new pg.Client({
    host,
    port: Number.isFinite(portNum) && portNum > 0 ? portNum : 5432,
    database: String(row.database_name),
    user: String(row.username),
    password: String(row.password_value),
    connectionTimeoutMillis: 15_000,
  });
};

/** schema.table or table (defaults to public). */
export const parseSafePostgresTableRef = (table) => {
  const t = String(table || '').trim();
  if (!t) {
    return null;
  }
  if (!/^([a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+$/.test(t)) {
    return null;
  }
  if (t.includes('.')) {
    const dot = t.indexOf('.');
    return { schema: t.slice(0, dot), table: t.slice(dot + 1) };
  }
  return { schema: 'public', table: t };
};

const qIdent = (name) => `"${String(name).replace(/"/g, '""')}"`;

const formatCellForPreview = (value) => {
  if (value === null || value === undefined) {
    return '';
  }
  if (value instanceof Date) {
    return value.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '');
  }
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
    return value.toString('hex');
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

export const listPostgresTablesForProfile = async (row) => {
  const client = buildPostgresClient(row);
  try {
    await client.connect();
    const result = await client.query(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY table_schema, table_name
    `);
    const tables = (result.rows || []).map((r) =>
      r.table_schema === 'public' ? r.table_name : `${r.table_schema}.${r.table_name}`
    );
    await client.end();
    return tables;
  } catch (e) {
    try {
      await client.end();
    } catch {
      /* ignore */
    }
    throw e;
  }
};

const PG_STRING_TYPES = new Set([
  'character',
  'character varying',
  'varchar',
  'text',
  'citext',
  'name',
  'uuid',
]);

export const getPostgresTableDataForProfile = async (row, tableParam, limit, offset, options = {}) => {
  const ref = parseSafePostgresTableRef(tableParam);
  if (!ref) {
    throw new Error('Invalid table name.');
  }

  const { schema, table } = ref;
  const client = buildPostgresClient(row);
  const search = String(options?.q || '').trim();
  const filters =
    options?.filters && typeof options.filters === 'object' && !Array.isArray(options.filters)
      ? options.filters
      : {};
  const conditions = Array.isArray(options?.conditions) ? options.conditions : [];

  try {
    await client.connect();

    const exists = await client.query(
      `
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
        LIMIT 1
      `,
      [schema, table]
    );
    if (!exists.rows?.length) {
      await client.end();
      const err = new Error('Table not found in this database.');
      err.code = 'TABLE_NOT_FOUND';
      throw err;
    }

    const colResult = await client.query(
      `
        SELECT
          column_name,
          data_type,
          data_type AS column_type,
          is_nullable,
          COALESCE(column_default, '') AS column_default,
          COALESCE(is_identity, 'NO') AS is_identity,
          COALESCE(col_description(format('%I.%I', table_schema, table_name)::regclass, ordinal_position::int), '') AS column_comment
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `,
      [schema, table]
    );

    const pkResult = await client.query(
      `
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        INNER JOIN information_schema.key_column_usage kcu
          ON tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
          AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = $1
          AND tc.table_name = $2
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position NULLS LAST
      `,
      [schema, table]
    );
    const pkNames = new Set((pkResult.rows || []).map((r) => String(r.column_name || '')));

    const columns = (colResult.rows || []).map((c) => ({
      name: c.column_name,
      type: c.data_type,
      columnType: c.column_type,
      key: pkNames.has(String(c.column_name || '')) ? 'PRI' : '',
      extra: '',
      columnDefault: c.column_default == null ? '' : String(c.column_default),
      isIdentity: String(c.is_identity ?? 'NO').trim().toUpperCase() === 'YES',
      comment: c.column_comment == null ? '' : String(c.column_comment).trim(),
      nullable: String(c.is_nullable ?? 'YES').trim().toUpperCase() === 'YES',
    }));

    const { whereSql, whereParams } = buildPostgresWhere(columns, { search, filters, conditions });

    const countResult = await client.query(
      `SELECT COUNT(*)::bigint AS total FROM ${qIdent(schema)}.${qIdent(table)}${whereSql}`,
      whereParams
    );
    const total = Number(countResult.rows?.[0]?.total ?? 0);

    const orderExpr = columns.length > 0 ? qIdent(columns[0].name) : '1';

    const lim = Number(limit);
    const off = Number(offset);
    const limIdx = whereParams.length + 1;
    const offIdx = whereParams.length + 2;
    const dataResult = await client.query(
      `SELECT * FROM ${qIdent(schema)}.${qIdent(table)}${whereSql} ORDER BY ${orderExpr} LIMIT $${limIdx} OFFSET $${offIdx}`,
      [...whereParams, lim, off]
    );

    const packets = dataResult.rows || [];
    const rows = packets.map((packet) =>
      columns.map((c) => {
        const raw =
          packet[c.name] ??
          packet[c.name?.toLowerCase?.()] ??
          packet[c.name?.toUpperCase?.()];
        return formatCellForPreview(raw);
      })
    );

    await client.end();

    return { columns, rows, total };
  } catch (e) {
    try {
      await client.end();
    } catch {
      /* ignore */
    }
    throw e;
  }
};
