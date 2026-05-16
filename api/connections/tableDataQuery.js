/** @typedef {{ name: string; type?: string }} ColumnMeta */

/** @typedef {{ column: string; op: string; value?: string }} TableDataCondition */

const ALLOWED_OPS = new Set([
  'eq',
  'neq',
  'contains',
  'not_contains',
  'gt',
  'gte',
  'lt',
  'lte',
  'starts_with',
  'ends_with',
  'is_empty',
  'is_not_empty',
]);

const NUMERIC_TYPES = new Set([
  'tinyint',
  'smallint',
  'mediumint',
  'int',
  'integer',
  'bigint',
  'decimal',
  'numeric',
  'float',
  'double',
  'real',
  'money',
  'smallmoney',
]);

const STRING_TYPES = new Set([
  'char',
  'varchar',
  'nvarchar',
  'nchar',
  'tinytext',
  'text',
  'mediumtext',
  'longtext',
  'ntext',
  'enum',
  'set',
  'json',
  'jsonb',
  'uuid',
]);

export function parseConditionsParam(raw) {
  if (!raw || typeof raw !== 'string') return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((c) => ({
        column: String(c?.column || '').trim(),
        op: String(c?.op || '').trim().toLowerCase(),
        value: c?.value == null ? '' : String(c.value),
      }))
      .filter((c) => c.column && ALLOWED_OPS.has(c.op));
  } catch {
    return [];
  }
}

export function parseTableDataQueryOptions(query = {}) {
  const search = String(query.q || '').trim();
  let filters = {};
  if (typeof query.filters === 'string') {
    try {
      const parsed = JSON.parse(query.filters);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) filters = parsed;
    } catch {
      filters = {};
    }
  }
  const conditions = parseConditionsParam(query.conditions);
  return { search, filters, conditions };
}

function colByName(columns, name) {
  return columns.find((c) => c.name === name);
}

/** Match filters / conditions keys case-insensitively against catalog column names. */
function resolveColumnName(columns, requested) {
  const r = String(requested || '').trim();
  if (!r) return null;
  const direct = columns.find((c) => c.name === r);
  if (direct) return direct.name;
  const rl = r.toLowerCase();
  const hit = columns.find((c) => String(c.name || '').toLowerCase() === rl);
  return hit?.name ?? null;
}

function isNumericType(type) {
  return NUMERIC_TYPES.has(String(type || '').toLowerCase());
}

function isStringType(type) {
  const t = String(type || '').toLowerCase();
  return STRING_TYPES.has(t) || t.includes('char') || t.includes('text');
}

function mysqlIdent(name) {
  return `\`${String(name).replace(/`/g, '``')}\``;
}

/**
 * @param {ColumnMeta[]} columns
 * @param {{ search?: string; filters?: Record<string, string>; conditions?: TableDataCondition[] }} options
 */
export function buildMysqlWhere(columns, options = {}) {
  const stringCols = columns.filter((c) => isStringType(c.type));
  const whereParts = [];
  const whereParams = [];

  const search = String(options.search || '').trim();
  if (search && stringCols.length > 0) {
    const orParts = stringCols.map((c) => {
      whereParams.push(`%${search}%`);
      return `${mysqlIdent(c.name)} LIKE ?`;
    });
    whereParts.push(`(${orParts.join(' OR ')})`);
  }

  for (const [col, val] of Object.entries(options.filters || {})) {
    if (val == null || String(val).trim() === '') continue;
    const resolved = resolveColumnName(columns, col);
    if (!resolved) continue;
    whereParts.push(`${mysqlIdent(resolved)} = ?`);
    whereParams.push(String(val));
  }

  for (const cond of options.conditions || []) {
    const resolved = resolveColumnName(columns, cond.column);
    if (!resolved) continue;
    const def = colByName(columns, resolved);
    if (!def) continue;
    appendMysqlCondition({ ...cond, column: resolved }, def, whereParts, whereParams);
  }

  const whereSql = whereParts.length > 0 ? ` WHERE ${whereParts.join(' AND ')}` : '';
  return { whereSql, whereParams };
}

function appendMysqlCondition(cond, colDef, whereParts, whereParams) {
  const name = mysqlIdent(cond.column);
  const t = colDef?.type;
  const numeric = isNumericType(t);
  const asText = (expr) => (numeric ? `CAST(${expr} AS CHAR)` : expr);
  const colExpr = asText(name);
  const val = String(cond.value ?? '');

  switch (cond.op) {
    case 'is_empty':
      whereParts.push(`(${name} IS NULL OR ${colExpr} = '')`);
      return;
    case 'is_not_empty':
      whereParts.push(`(${name} IS NOT NULL AND ${colExpr} <> '')`);
      return;
    case 'contains':
      whereParams.push(`%${val}%`);
      whereParts.push(`${colExpr} LIKE ?`);
      return;
    case 'not_contains':
      whereParams.push(`%${val}%`);
      whereParts.push(`(${name} IS NULL OR ${colExpr} NOT LIKE ?)`);
      return;
    case 'starts_with':
      whereParams.push(`${val}%`);
      whereParts.push(`${colExpr} LIKE ?`);
      return;
    case 'ends_with':
      whereParams.push(`%${val}`);
      whereParts.push(`${colExpr} LIKE ?`);
      return;
    case 'eq':
      whereParams.push(val);
      whereParts.push(`${name} = ?`);
      return;
    case 'neq':
      whereParams.push(val);
      whereParts.push(`(${name} IS NULL OR ${name} <> ?)`);
      return;
    case 'gt':
      whereParams.push(val);
      whereParts.push(`${name} > ?`);
      return;
    case 'gte':
      whereParams.push(val);
      whereParts.push(`${name} >= ?`);
      return;
    case 'lt':
      whereParams.push(val);
      whereParts.push(`${name} < ?`);
      return;
    case 'lte':
      whereParams.push(val);
      whereParts.push(`${name} <= ?`);
      return;
    default:
      return;
  }
}

function pgIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

/**
 * @param {ColumnMeta[]} columns
 * @param {{ search?: string; filters?: Record<string, string>; conditions?: TableDataCondition[] }} options
 */
export function buildPostgresWhere(columns, options = {}) {
  const stringCols = columns.filter((c) => isStringType(c.type));
  const whereParts = [];
  const whereParams = [];
  let p = 0;

  const search = String(options.search || '').trim();
  if (search && stringCols.length > 0) {
    const orParts = stringCols.map((c) => {
      whereParams.push(`%${search}%`);
      p += 1;
      return `${pgIdent(c.name)}::text ILIKE $${p}`;
    });
    whereParts.push(`(${orParts.join(' OR ')})`);
  }

  for (const [col, val] of Object.entries(options.filters || {})) {
    if (val == null || String(val).trim() === '') continue;
    const resolved = resolveColumnName(columns, col);
    if (!resolved) continue;
    whereParams.push(String(val));
    p += 1;
    whereParts.push(`${pgIdent(resolved)}::text = $${p}`);
  }

  for (const cond of options.conditions || []) {
    const resolved = resolveColumnName(columns, cond.column);
    if (!resolved) continue;
    const def = colByName(columns, resolved);
    if (!def) continue;
    p = appendPostgresCondition({ ...cond, column: resolved }, def, whereParts, whereParams, p);
  }

  const whereSql = whereParts.length > 0 ? ` WHERE ${whereParts.join(' AND ')}` : '';
  return { whereSql, whereParams };
}

function appendPostgresCondition(cond, colDef, whereParts, whereParams, p) {
  const name = pgIdent(cond.column);
  const val = String(cond.value ?? '');
  const textCol = `${name}::text`;

  switch (cond.op) {
    case 'is_empty':
      whereParts.push(`(${name} IS NULL OR ${textCol} = '')`);
      return p;
    case 'is_not_empty':
      whereParts.push(`(${name} IS NOT NULL AND ${textCol} <> '')`);
      return p;
    case 'contains':
      whereParams.push(`%${val}%`);
      p += 1;
      whereParts.push(`${textCol} ILIKE $${p}`);
      return p;
    case 'not_contains':
      whereParams.push(`%${val}%`);
      p += 1;
      whereParts.push(`(${name} IS NULL OR ${textCol} NOT ILIKE $${p})`);
      return p;
    case 'starts_with':
      whereParams.push(`${val}%`);
      p += 1;
      whereParts.push(`${textCol} ILIKE $${p}`);
      return p;
    case 'ends_with':
      whereParams.push(`%${val}`);
      p += 1;
      whereParts.push(`${textCol} ILIKE $${p}`);
      return p;
    case 'eq':
      whereParams.push(val);
      p += 1;
      whereParts.push(`${name}::text = $${p}`);
      return p;
    case 'neq':
      whereParams.push(val);
      p += 1;
      whereParts.push(`(${name} IS NULL OR ${name}::text <> $${p})`);
      return p;
    case 'gt':
      whereParams.push(val);
      p += 1;
      whereParts.push(`${name} > $${p}`);
      return p;
    case 'gte':
      whereParams.push(val);
      p += 1;
      whereParts.push(`${name} >= $${p}`);
      return p;
    case 'lt':
      whereParams.push(val);
      p += 1;
      whereParts.push(`${name} < $${p}`);
      return p;
    case 'lte':
      whereParams.push(val);
      p += 1;
      whereParts.push(`${name} <= $${p}`);
      return p;
    default:
      return p;
  }
}

function sqlServerIdent(name) {
  return `[${String(name).replace(/]/g, '')}]`;
}

/**
 * @returns {{ whereClause: string; requestParams: Array<{ name: string; value: string }> }}
 */
export function buildSqlServerWhere(columns, options = {}) {
  const stringCols = columns.filter((c) => isStringType(c.type));
  const whereParts = [];
  const requestParams = [];
  let paramIdx = 0;

  const nextParam = (value) => {
    const name = `p${paramIdx++}`;
    requestParams.push({ name, value: String(value) });
    return `@${name}`;
  };

  const search = String(options.search || '').trim();
  if (search && stringCols.length > 0) {
    const likeExprs = stringCols.map((c) => `${sqlServerIdent(c.name)} LIKE ${nextParam(`%${search}%`)}`);
    whereParts.push(`(${likeExprs.join(' OR ')})`);
  }

  for (const [col, val] of Object.entries(options.filters || {})) {
    if (val == null || String(val).trim() === '') continue;
    const resolved = resolveColumnName(columns, col);
    if (!resolved) continue;
    whereParts.push(`${sqlServerIdent(resolved)} = ${nextParam(String(val))}`);
  }

  for (const cond of options.conditions || []) {
    const resolved = resolveColumnName(columns, cond.column);
    if (!resolved) continue;
    const def = colByName(columns, resolved);
    if (!def) continue;
    appendSqlServerCondition({ ...cond, column: resolved }, def, whereParts, nextParam);
  }

  const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';
  return { whereClause, requestParams };
}

function appendSqlServerCondition(cond, colDef, whereParts, nextParam) {
  const name = sqlServerIdent(cond.column);
  const val = String(cond.value ?? '');
  const textCast = `CAST(${name} AS NVARCHAR(MAX))`;

  switch (cond.op) {
    case 'is_empty':
      whereParts.push(`(${name} IS NULL OR ${textCast} = N'')`);
      return;
    case 'is_not_empty':
      whereParts.push(`(${name} IS NOT NULL AND ${textCast} <> N'')`);
      return;
    case 'contains':
      whereParts.push(`${textCast} LIKE ${nextParam(`%${val}%`)}`);
      return;
    case 'not_contains':
      whereParts.push(`(${name} IS NULL OR ${textCast} NOT LIKE ${nextParam(`%${val}%`)})`);
      return;
    case 'starts_with':
      whereParts.push(`${textCast} LIKE ${nextParam(`${val}%`)}`);
      return;
    case 'ends_with':
      whereParts.push(`${textCast} LIKE ${nextParam(`%${val}`)}`);
      return;
    case 'eq':
      whereParts.push(`${name} = ${nextParam(val)}`);
      return;
    case 'neq':
      whereParts.push(`(${name} IS NULL OR ${name} <> ${nextParam(val)})`);
      return;
    case 'gt':
      whereParts.push(`${name} > ${nextParam(val)}`);
      return;
    case 'gte':
      whereParts.push(`${name} >= ${nextParam(val)}`);
      return;
    case 'lt':
      whereParts.push(`${name} < ${nextParam(val)}`);
      return;
    case 'lte':
      whereParts.push(`${name} <= ${nextParam(val)}`);
      return;
    default:
      return;
  }
}
