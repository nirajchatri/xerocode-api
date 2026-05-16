import sql from 'mssql';
import { buildSqlServerTlsOptions, parseSqlServerServerInput } from './hostUtils.js';
import { buildSqlServerWhere } from './tableDataQuery.js';

const buildSqlServerConfig = (row) => {
  const { server, instanceName, port } = parseSqlServerServerInput(row.host, row.port);
  const config = {
    user: String(row.username),
    password: String(row.password_value),
    server,
    database: String(row.database_name),
    options: buildSqlServerTlsOptions(server, instanceName),
    connectionTimeout: 15_000,
    requestTimeout: 15_000,
  };

  if (instanceName) {
    if (port && Number.isFinite(port) && port > 0) {
      config.port = port;
    }
  } else {
    config.port = port && Number.isFinite(port) && port > 0 ? port : 1433;
  }

  return config;
};

/** Allow dbo.Table or Table (defaults to dbo). */
export const parseSafeSqlServerTableRef = (table) => {
  const t = String(table || '').trim();
  if (!t) {
    return null;
  }
  if (!/^([a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+$/.test(t)) {
    return null;
  }
  if (t.includes('.')) {
    const [s, n] = t.split('.');
    return { schema: s, table: n };
  }
  return { schema: 'dbo', table: t };
};

export const listSqlServerTablesForProfile = async (row) => {
  const config = buildSqlServerConfig(row);
  const pool = new sql.ConnectionPool(config);
  try {
    await pool.connect();
    const result = await pool.request().query(`
      SELECT TABLE_SCHEMA, TABLE_NAME
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_TYPE = 'BASE TABLE'
      ORDER BY TABLE_SCHEMA, TABLE_NAME
    `);
    const tables = (result.recordset || []).map((r) =>
      r.TABLE_SCHEMA === 'dbo' ? r.TABLE_NAME : `${r.TABLE_SCHEMA}.${r.TABLE_NAME}`
    );
    await pool.close();
    return tables;
  } catch (e) {
    try {
      await pool.close();
    } catch {
      /* ignore */
    }
    throw e;
  }
};

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

const STRING_DATA_TYPES = new Set([
  'char',
  'varchar',
  'nchar',
  'nvarchar',
  'text',
  'ntext',
  'sysname',
  'xml',
]);

export const getSqlServerTableDataForProfile = async (row, tableParam, limit, offset, options = {}) => {
  const ref = parseSafeSqlServerTableRef(tableParam);
  if (!ref) {
    throw new Error('Invalid table name.');
  }

  const { schema, table } = ref;
  const config = buildSqlServerConfig(row);
  const pool = new sql.ConnectionPool(config);

  const search = String(options?.q || '').trim();
  const filters =
    options?.filters && typeof options.filters === 'object' && !Array.isArray(options.filters)
      ? options.filters
      : {};
  const conditions = Array.isArray(options?.conditions) ? options.conditions : [];

  try {
    await pool.connect();

    const colResult = await pool.request().query(`
      SELECT
        c.COLUMN_NAME AS column_name,
        c.DATA_TYPE AS data_type,
        c.DATA_TYPE AS column_type,
        c.IS_NULLABLE AS is_nullable,
        ISNULL(c.COLUMN_DEFAULT, '') AS column_default,
        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS column_key,
        ISNULL(CAST(sep.value AS NVARCHAR(MAX)), '') AS column_comment
      FROM INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN (
        SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
          ON tc.CONSTRAINT_SCHEMA = ku.CONSTRAINT_SCHEMA
          AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
          AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
          AND tc.TABLE_NAME = ku.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      ) pk
        ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA
        AND pk.TABLE_NAME = c.TABLE_NAME
        AND pk.COLUMN_NAME = c.COLUMN_NAME
      LEFT JOIN sys.columns sc
        ON sc.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + N'.' + QUOTENAME(c.TABLE_NAME))
        AND sc.name = c.COLUMN_NAME
      LEFT JOIN sys.extended_properties sep
        ON sep.major_id = sc.object_id
        AND sep.minor_id = sc.column_id
        AND sep.class = 1
        AND sep.name = N'MS_Description'
      WHERE c.TABLE_SCHEMA = N'${schema.replace(/'/g, "''")}' AND c.TABLE_NAME = N'${table.replace(/'/g, "''")}'
      ORDER BY c.ORDINAL_POSITION
    `);

    const columns = (colResult.recordset || []).map((c) => ({
      name: c.column_name,
      type: c.data_type,
      columnType: c.column_type,
      key: c.column_key || '',
      extra: '',
      columnDefault: c.column_default == null ? '' : String(c.column_default),
      comment: c.column_comment == null ? '' : String(c.column_comment).trim(),
      nullable: String(c.is_nullable ?? c.IS_NULLABLE ?? 'YES').trim().toUpperCase() === 'YES',
    }));

    const { whereClause, requestParams } = buildSqlServerWhere(columns, { search, filters, conditions });

    const countReq = pool.request();
    requestParams.forEach((p) => countReq.input(p.name, sql.NVarChar, p.value));
    const countResult = await countReq.query(`
      SELECT COUNT(*) AS total FROM [${schema}].[${table}] ${whereClause}
    `);
    const total = Number(countResult.recordset?.[0]?.total ?? 0);

    const orderCol =
      columns.length > 0 ? `[${columns[0].name.replace(/]/g, '')}]` : '(SELECT 1)';

    const dataReq = pool.request();
    requestParams.forEach((p) => dataReq.input(p.name, sql.NVarChar, p.value));
    const dataResult = await dataReq.query(`
      SELECT * FROM [${schema}].[${table}]
      ${whereClause}
      ORDER BY ${orderCol}
      OFFSET ${Number(offset)} ROWS FETCH NEXT ${Number(limit)} ROWS ONLY
    `);

    const packets = dataResult.recordset || [];
    const rows = packets.map((packet) =>
      columns.map((c) => {
        const raw =
          packet[c.name] ??
          packet[c.name?.toLowerCase?.()] ??
          packet[c.name?.toUpperCase?.()];
        return formatCellForPreview(raw);
      })
    );

    await pool.close();

    return { columns, rows, total };
  } catch (e) {
    try {
      await pool.close();
    } catch {
      /* ignore */
    }
    throw e;
  }
};
