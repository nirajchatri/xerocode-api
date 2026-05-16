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

function pickFieldCi(row, key) {
  if (key in row) return row[key];
  const lower = key.toLowerCase();
  const hit = Object.keys(row).find((k) => k.toLowerCase() === lower);
  return hit ? row[hit] : undefined;
}

function parseJsonReply(raw) {
  const cleaned = String(raw || '')
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();
  return JSON.parse(cleaned);
}

function normalizeConditions(raw, allowedColumns) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue;
    const column = String(pickFieldCi(item, 'column') ?? '').trim();
    const op = String(pickFieldCi(item, 'op') ?? '')
      .trim()
      .toLowerCase();
    if (!column || !allowedColumns.has(column) || !ALLOWED_OPS.has(op)) continue;
    const valueRaw = pickFieldCi(item, 'value');
    const value = valueRaw == null ? undefined : String(valueRaw);
    if (op !== 'is_empty' && op !== 'is_not_empty' && (!value || !value.trim())) continue;
    out.push({ column, op, value: value?.trim() });
  }
  return out.slice(0, 12);
}

function normalizeFilters(raw, allowedColumns) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out = {};
  for (const [k, v] of Object.entries(raw)) {
    if (!allowedColumns.has(k)) continue;
    const s = String(v ?? '').trim();
    if (s) out[k] = s;
  }
  return out;
}

export function buildNlGridSearchPrompt(table, columns, userQuery) {
  const colLines = columns
    .map((c) => {
      const label = c.label?.trim();
      const type = c.type?.trim() || 'unknown';
      return label && label.toLowerCase() !== c.name.toLowerCase()
        ? `- ${c.name} (${type}) — ${label}`
        : `- ${c.name} (${type})`;
    })
    .join('\n');

  return [
    'You translate natural-language questions about tabular data into structured search instructions.',
    'Think like Google: infer intent, pick the right columns, combine filters, and use broad keywords when the question is vague.',
    '',
    `Table: ${table}`,
    'Columns:',
    colLines || '- (no columns listed)',
    '',
    `User question: ${userQuery}`,
    '',
    'Return strict JSON only:',
    '{',
    '  "summary": "short phrase describing what you searched for",',
    '  "keywords": "optional broad text for fuzzy match across text columns; omit if not needed",',
    '  "conditions": [',
    '    { "column": "exact_column_name", "op": "contains|eq|neq|gt|gte|lt|lte|starts_with|ends_with|is_empty|is_not_empty", "value": "..." }',
    '  ]',
    '}',
    '',
    'Rules:',
    '- Use only column names from the list above.',
    '- Prefer specific conditions (contains, eq, comparisons) over keywords when you know the column.',
    '- For names, titles, descriptions, statuses: use "contains" unless exact match is obvious.',
    '- For numbers/dates: use gt/gte/lt/lte/eq with sensible values.',
    '- Use "keywords" for exploratory questions ("anything about john", "acme") when no single column is clear.',
    '- You may use both keywords and conditions together.',
    '- If the question is empty or nonsense, return { "summary": "...", "keywords": "<original query>" }.',
    '- Do not invent columns or values not implied by the question.',
  ].join('\n');
}

export function parseNlGridSearchReply(raw, allowedColumns, fallbackQuery) {
  const allowed = new Set(allowedColumns);
  try {
    const parsed = parseJsonReply(raw);
    const summary = String(parsed?.summary ?? '').trim() || fallbackQuery;
    const keywords = String(parsed?.keywords ?? '').trim();
    const conditions = normalizeConditions(parsed?.conditions, allowed);
    const filters = normalizeFilters(parsed?.filters, allowed);

    if (conditions.length === 0 && !keywords && Object.keys(filters).length === 0) {
      return { summary, keywords: fallbackQuery };
    }

    return {
      summary,
      ...(keywords ? { keywords } : {}),
      ...(Object.keys(filters).length > 0 ? { filters } : {}),
      ...(conditions.length > 0 ? { conditions } : {}),
    };
  } catch {
    return { summary: fallbackQuery, keywords: fallbackQuery };
  }
}
