import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';
import { invokeLlmChat } from './llm/chat.js';

const slugRegex = /^dsp-[a-f0-9]{24}$/i;
const PUBLISH_LIVE_SNAPSHOT_KEY = '__publish_live_snapshot';
const MAX_CONTEXT_ROWS = 80;
const MAX_HISTORY = 10;
const MAX_MESSAGE_LEN = 4000;
const MAX_HISTORY_TEXT = 2000;

const parsePayload = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

const clip = (s, max) => {
  const t = String(s ?? '')
    .trim()
    .replace(/\s+/g, ' ');
  if (t.length <= max) return t;
  return `${t.slice(0, max)}…`;
};

const loadPreferredLlmConfig = async (pool) => {
  const result = await pool.request().query(`
    SELECT TOP 1 provider, model_name
    FROM dbo.LLM_Config
    WHERE api_key IS NOT NULL AND LTRIM(RTRIM(api_key)) <> ''
    ORDER BY CASE provider WHEN 'google' THEN 0 WHEN 'openai' THEN 1 WHEN 'anthropic' THEN 2 ELSE 3 END, provider ASC
  `);
  return result.recordset?.[0] || null;
};

function columnNamesFromRows(rows) {
  const names = new Set();
  for (const r of rows.slice(0, MAX_CONTEXT_ROWS)) {
    if (r && typeof r === 'object' && !Array.isArray(r)) {
      Object.keys(r).forEach((k) => names.add(k));
    }
  }
  return [...names];
}

function summarizeColumn(rows, col) {
  const vals = rows
    .map((r) => (r && typeof r === 'object' ? r[col] : undefined))
    .filter((v) => v != null && v !== '');
  if (!vals.length) return '(no values in snapshot)';
  const nums = vals.map((v) => Number(v)).filter((n) => Number.isFinite(n));
  if (nums.length >= Math.max(3, Math.floor(vals.length * 0.5))) {
    const sum = nums.reduce((a, b) => a + b, 0);
    const min = Math.min(...nums);
    const max = Math.max(...nums);
    return `numeric — n=${nums.length}, min=${min}, max=${max}, sum=${sum.toFixed(2)}, avg=${(sum / nums.length).toFixed(2)}`;
  }
  const uniq = [...new Set(vals.map((v) => clip(String(v), 80)))];
  const samples = uniq.slice(0, 6).join(' | ');
  return `text/categorical — n=${vals.length}, distinct≈${uniq.length}, samples: ${samples || '(empty)'}`;
}

function describePanel(panel) {
  if (!panel || typeof panel !== 'object' || Array.isArray(panel)) return null;
  const kind = String(panel.kind || '').trim();
  const title = clip(panel.title || panel.id || 'Panel', 120);
  if (kind === 'kpi') {
    return `• KPI "${title}" — metric column: ${clip(panel.column, 80)}`;
  }
  if (kind === 'trend') {
    const yCols = Array.isArray(panel.yColumns)
      ? panel.yColumns.map((c) => clip(String(c), 60)).join(', ')
      : '';
    return `• Chart "${title}" (${panel.chartType || 'line'}) — x: ${clip(panel.xColumn, 60)}, y: ${clip(panel.yColumn, 60)}${yCols ? `, series: ${yCols}` : ''}${panel.seriesColumn ? `, group: ${clip(panel.seriesColumn, 60)}` : ''}`;
  }
  if (kind === 'donut') {
    return `• ${panel.chartType === 'pie' ? 'Pie' : 'Donut'} "${title}" — category: ${clip(panel.categoryColumn, 60)}, value: ${clip(panel.valueColumn, 60)}`;
  }
  if (kind === 'table') {
    const cols = Array.isArray(panel.columns) ? panel.columns.map((c) => clip(String(c), 40)).join(', ') : '(all columns)';
    return `• Table "${title}" — columns: ${cols}`;
  }
  return `• Panel "${title}" (${kind || 'unknown'})`;
}

function buildDashboardFacts(proposal, title) {
  const snap = proposal[PUBLISH_LIVE_SNAPSHOT_KEY];
  const rowsRaw = snap && typeof snap === 'object' && !Array.isArray(snap) ? snap.rows : [];
  const rows = Array.isArray(rowsRaw)
    ? rowsRaw
        .filter((r) => r && typeof r === 'object' && !Array.isArray(r))
        .slice(0, MAX_CONTEXT_ROWS)
    : [];
  const sourceLabel =
    snap && typeof snap === 'object' && typeof snap.source_label === 'string'
      ? String(snap.source_label).trim()
      : '';

  const panelsRaw = proposal.editable_panels ?? proposal.editablePanels;
  const panelLines = Array.isArray(panelsRaw)
    ? panelsRaw.map(describePanel).filter(Boolean).slice(0, 48)
    : [];

  const dt =
    typeof proposal.dashboard_title === 'string'
      ? proposal.dashboard_title
      : typeof proposal.dashboardTitle === 'string'
        ? proposal.dashboardTitle
        : '';
  const dashboardTitle = clip(title || dt, 200) || 'Dashboard';

  const blocks = [
    `Dashboard title: ${dashboardTitle}`,
    panelLines.length ? `Layout (${panelLines.length} panels):\n${panelLines.join('\n')}` : 'Layout: (no editable panel metadata in publish JSON)',
  ];

  if (rows.length) {
    const cols = columnNamesFromRows(rows);
    blocks.push(
      `Data snapshot: ${rows.length} row(s)${sourceLabel ? ` from "${clip(sourceLabel, 120)}"` : ''}, columns (${cols.length}): ${cols.slice(0, 40).join(', ')}${cols.length > 40 ? '…' : ''}`
    );
    const colSummaries = cols.slice(0, 24).map((c) => `  - ${c}: ${summarizeColumn(rows, c)}`);
    if (colSummaries.length) blocks.push(`Column summaries (from snapshot):\n${colSummaries.join('\n')}`);
  } else {
    blocks.push(
      'Data snapshot: none embedded in this publish link — answer from layout/metadata only; do not invent numeric metrics.'
    );
  }

  const joined = blocks.join('\n\n');
  return joined.length <= 12000 ? joined : `${joined.slice(0, 12000)}\n…(facts truncated)`;
}

function transcript(history) {
  if (!Array.isArray(history) || !history.length) return '(no prior messages)';
  return history
    .filter((m) => m && (m.role === 'user' || m.role === 'assistant'))
    .map((m) => `${m.role === 'user' ? 'User' : 'Assistant'}: ${clip(m.text, MAX_HISTORY_TEXT)}`)
    .join('\n\n');
}

function buildUserBundle({ title, facts, history, message }) {
  return [
    '--- Published dashboard ---',
    clip(title, 200) || 'Dashboard',
    '',
    '--- Dashboard facts (ground answers here only) ---',
    facts.trim() || '(no facts)',
    '',
    '--- Conversation so far ---',
    transcript(history),
    '',
    '--- Latest user question ---',
    clip(message, MAX_MESSAGE_LEN),
    '',
    'Answer in clear, professional language. Cite numbers from the snapshot when relevant. If the question cannot be answered from facts above, say what is missing. Do not invent metrics or row values.',
  ].join('\n');
}

export const postPublicDesignStudioPreviewChat = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegex.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid preview slug.' });
    }

    const message = String(req.body?.message ?? req.body?.userMessage ?? '').trim();
    if (!message) {
      return res.status(400).json({ ok: false, message: 'message is required.' });
    }
    if (message.length > MAX_MESSAGE_LEN) {
      return res.status(400).json({ ok: false, message: 'message is too long.' });
    }

    const historyRaw = Array.isArray(req.body?.history) ? req.body.history : [];
    const history = historyRaw.slice(-MAX_HISTORY).map((m) => ({
      role: m?.role === 'assistant' ? 'assistant' : 'user',
      text: clip(String(m?.text ?? ''), MAX_HISTORY_TEXT),
    }));

    pool = await connectToControlSqlServer();
    const result = await pool.request().input('slug', sql.NVarChar, slug).query(`
      SELECT TOP 1 slug, title, proposal_json
      FROM dbo.design_studio_public_previews
      WHERE slug = @slug
    `);
    const row = result.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Preview not found.' });
    }

    const proposal = parsePayload(row.proposal_json);
    const title = String(row.title || '').trim();
    const facts = buildDashboardFacts(proposal, title);
    const bundledUser = buildUserBundle({ title, facts, history, message });

    const llmRow = await loadPreferredLlmConfig(pool);
    if (!llmRow) {
      return res.status(503).json({
        ok: false,
        message: 'AI insights are not configured on this server. Add an LLM API key in workspace settings.',
      });
    }

    const provider = String(llmRow.provider || 'google').trim().toLowerCase();
    const model = String(llmRow.model_name || '').trim();
    if (!model) {
      return res.status(503).json({ ok: false, message: 'AI model is not configured.' });
    }

    const payload = await invokeLlmChat({
      provider,
      model,
      userMessage: bundledUser,
      expectJson: false,
      maxTokens: 4096,
      systemPrompt:
        'You are a data analyst helping viewers understand a published executive dashboard. Use only the dashboard facts in the user message. Be concise, accurate, and actionable. Never fabricate numbers.',
      dataSourceName: title || 'Published dashboard',
    });

    const reply = String(payload?.reply || '').trim() || 'No reply text returned.';
    return res.json({ ok: true, reply });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to answer.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
