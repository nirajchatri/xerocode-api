/**
 * Deterministic DAG traversal / topo order for Agent Studio workflows.
 * Shared by the Express stub endpoint and the browser (fallback when API is offline).
 */

/** @param {Array<{ id: string, type?: string }>} nodes */
export function findStartNodeId(nodes) {
  const s = nodes.find((n) => n.type === 'start');
  return s ? s.id : null;
}

/**
 * @param {string} startId
 * @param {Array<{ id: string }>} allNodes
 * @param {Array<{ source: string, target: string }>} edges
 */
export function computeWorkflowRunOrder(startId, allNodes, edges) {
  const idSet = new Set(allNodes.map((n) => n.id));
  if (!idSet.has(startId)) {
    return { order: [], error: 'Start node not found on canvas.' };
  }

  const adj = new Map();
  for (const n of allNodes) adj.set(n.id, []);
  for (const e of edges) {
    if (!idSet.has(e.source) || !idSet.has(e.target)) continue;
    adj.get(e.source).push(e.target);
  }

  const reachable = new Set();
  const stack = [startId];
  reachable.add(startId);
  while (stack.length) {
    const u = stack.pop();
    for (const v of adj.get(u) || []) {
      if (!reachable.has(v)) {
        reachable.add(v);
        stack.push(v);
      }
    }
  }

  const subInd = new Map();
  for (const id of reachable) subInd.set(id, 0);
  const subAdj = new Map();
  for (const id of reachable) subAdj.set(id, []);

  for (const e of edges) {
    if (!reachable.has(e.source) || !reachable.has(e.target)) continue;
    subAdj.get(e.source).push(e.target);
    subInd.set(e.target, (subInd.get(e.target) || 0) + 1);
  }

  const q = [];
  for (const id of reachable) {
    if ((subInd.get(id) || 0) === 0) q.push(id);
  }
  q.sort();

  const order = [];
  let processed = 0;
  while (q.length) {
    const u = q.shift();
    processed += 1;
    if (u !== startId) order.push(u);
    for (const v of subAdj.get(u) || []) {
      subInd.set(v, (subInd.get(v) || 1) - 1);
      if ((subInd.get(v) || 0) === 0) {
        q.push(v);
        q.sort();
      }
    }
  }

  const nonStartReachable = [...reachable].filter((id) => id !== startId);
  if (processed !== reachable.size || order.length !== nonStartReachable.length) {
    return {
      order: [],
      error: 'Could not order this graph (cycle) or counts mismatch. Fix edges so the run path is a DAG from Start.',
    };
  }

  return { order, error: null };
}

/**
 * Topological order for nodes that lie on at least one path from `startId` to `targetId` (inclusive of target, exclusive numbering of start).
 * Used to run workflows toward an End node (chat / output).
 *
 * @param {string} startId
 * @param {string} targetId
 * @param {Array<{ id: string }>} allNodes
 * @param {Array<{ source: string, target: string }>} edges
 */
export function computeOrderFromStartToTarget(startId, targetId, allNodes, edges) {
  const idSet = new Set(allNodes.map((n) => n.id));
  if (!idSet.has(startId)) {
    return { order: [], error: 'Start node not found on canvas.' };
  }
  if (!idSet.has(targetId)) {
    return { order: [], error: 'End node not found.' };
  }

  const forward = new Map();
  const reverse = new Map();
  for (const n of allNodes) {
    forward.set(n.id, []);
    reverse.set(n.id, []);
  }
  for (const e of edges) {
    if (!idSet.has(e.source) || !idSet.has(e.target)) continue;
    forward.get(e.source).push(e.target);
    reverse.get(e.target).push(e.source);
  }

  const fromStart = new Set();
  const stack = [startId];
  fromStart.add(startId);
  while (stack.length) {
    const u = stack.pop();
    for (const v of forward.get(u) || []) {
      if (!fromStart.has(v)) {
        fromStart.add(v);
        stack.push(v);
      }
    }
  }
  if (!fromStart.has(targetId)) {
    return {
      order: [],
      error: 'This End node is not reachable from Start. Connect a path from Start.',
    };
  }

  const toTarget = new Set();
  const stack2 = [targetId];
  toTarget.add(targetId);
  while (stack2.length) {
    const u = stack2.pop();
    for (const v of reverse.get(u) || []) {
      if (!toTarget.has(v)) {
        toTarget.add(v);
        stack2.push(v);
      }
    }
  }

  const relevant = [...fromStart].filter((id) => toTarget.has(id));
  const relSet = new Set(relevant);

  const subInd = new Map();
  for (const id of relevant) subInd.set(id, 0);
  const subAdj = new Map();
  for (const id of relevant) subAdj.set(id, []);

  for (const e of edges) {
    if (!relSet.has(e.source) || !relSet.has(e.target)) continue;
    subAdj.get(e.source).push(e.target);
    subInd.set(e.target, (subInd.get(e.target) || 0) + 1);
  }

  const q = [];
  for (const id of relevant) {
    if ((subInd.get(id) || 0) === 0) q.push(id);
  }
  q.sort();

  const order = [];
  let processed = 0;
  while (q.length) {
    const u = q.shift();
    processed += 1;
    if (u !== startId) order.push(u);
    for (const v of subAdj.get(u) || []) {
      subInd.set(v, (subInd.get(v) || 1) - 1);
      if ((subInd.get(v) || 0) === 0) {
        q.push(v);
        q.sort();
      }
    }
  }

  if (processed !== relevant.length) {
    return {
      order: [],
      error: 'Path from Start to this End is not a valid DAG (cycle). Fix edges and try again.',
    };
  }

  return { order, error: null };
}

/** @type {Record<string, string>} Preset MCP catalog titles — keep in sync with `components/agentStudio/mcpCatalog.ts`. */
const MCP_CATALOG_TITLES = {
  gmail: 'Gmail',
  google_calendar: 'Google Calendar',
  google_drive: 'Google Drive',
  outlook_mail: 'Outlook Email',
  outlook_calendar: 'Outlook Calendar',
  sharepoint: 'Sharepoint',
  microsoft_teams: 'Microsoft Teams',
  dropbox: 'Dropbox',
  box: 'Box',
  zapier: 'Zapier',
  shopify: 'Shopify',
  intercom: 'Intercom',
  stripe: 'Stripe',
  plaid: 'Plaid',
  square: 'Square',
  cloudflare_browser: 'Cloudflare Browser',
  hubspot: 'HubSpot',
  pipedream: 'Pipedream',
  paypal: 'PayPal',
  deepwiki_devin: 'DeepWiki (Devin)',
};

/** @param {{ type?: string, data?: Record<string, unknown> }} node */
export function summarizeNode(node) {
  const t = String(node?.type || '');
  const d = /** @type {Record<string, unknown>} */ (node?.data || {});
  const id = String(node?.id || '');
  switch (t) {
    case 'api': {
      const method = String(d.method || 'GET').toUpperCase();
      const url = String(d.url || '').trim() || '(no URL)';
      const name = String(d.apiName || '').trim() || 'API';
      return {
        id,
        kind: 'api',
        title: name,
        detail: `${method} ${url}`.slice(0, 200),
      };
    }
    case 'llm': {
      const model = String(d.model || '').trim() || '(no model)';
      const name = String(d.agentDisplayName || '').trim();
      return {
        id,
        kind: 'llm',
        title: name || 'Agent Name',
        detail: `${model}`.slice(0, 200),
      };
    }
    case 'mcp': {
      const transport = String(d.transport || 'http');
      const serverUrl = String(d.serverUrl || '').trim() || '(no endpoint)';
      const cid = String(d.mcpCatalogId ?? '').trim();
      const humanLabel = String(d.mcpServerLabel ?? '').trim();
      /** @type {string} */
      let title = 'MCP Server';
      if (cid && cid !== 'custom' && MCP_CATALOG_TITLES[cid]) {
        title = MCP_CATALOG_TITLES[cid];
      } else if (humanLabel) {
        title = humanLabel.replace(/_/g, ' ');
      }
      return {
        id,
        kind: 'mcp',
        title,
        detail: `${transport} · ${serverUrl}`.slice(0, 200),
      };
    }
    case 'if_else': {
      const branches = Array.isArray(d.ifElseBranches) ? d.ifElseBranches : [];
      const n = branches.length > 0 ? branches.length : 3;
      const filled = branches.filter((b) => String(b?.expression ?? '').trim()).length;
      return {
        id,
        kind: 'if_else',
        title: 'If / else',
        detail: `${n} ${n === 1 ? 'branch' : 'branches'} · ${filled} with expressions`.slice(0, 200),
      };
    }
    case 'guardrails': {
      const ge = d.guardrailsState?.checkEnabled;
      const LABELS = {
        pii: 'PII',
        moderation: 'Moderation',
        jailbreak: 'Jailbreak',
        hallucination: 'Hallucination',
        nsfw: 'NSFW',
        urlFilter: 'URL filter',
        promptInjection: 'Prompt injection',
        customPrompt: 'Custom prompt',
      };
      const on = ge
        ? Object.keys(LABELS).filter((k) => ge[k])
        : [];
      const titled = String(d.guardrailsState?.displayName || '').trim();
      const rules = String(d.rulesText || '').trim();
      let detail;
      if (on.length) {
        detail = on.map((k) => LABELS[k] || k).join(' · ');
      } else if (rules) {
        detail = rules.split(/\r?\n/).find((l) => l.trim()) || rules;
      } else {
        detail = 'No checks enabled';
      }
      return {
        id,
        kind: 'guardrails',
        title: titled || 'Guardrails',
        detail: String(detail).slice(0, 200),
      };
    }
    case 'data': {
      const dk = String(d.dataFieldKind || 'string').trim() || 'string';
      const label = String(d.dataNodeLabel || '').trim();
      const raw = String(d.dataRawValue || '').trim().slice(0, 140);
      return {
        id,
        kind: 'data',
        title: label || `Data · ${dk}`,
        detail: raw || `(empty · ${dk})`,
      };
    }
    case 'end': {
      const label = String(d.endNodeLabel || '').trim();
      return {
        id,
        kind: 'end',
        title: label || 'End',
        detail: 'Workflow output · chat preview',
      };
    }
    default:
      return {
        id,
        kind: t || 'unknown',
        title: t || 'Node',
        detail: String(d.subtitle || ''),
      };
  }
}

/**
 * @param {string | null | undefined} workflowName
 * @param {Array<import('@xyflow/react').Node>} nodes
 * @param {Array<import('@xyflow/react').Edge>} edges
 */
export function runWorkflowLocally(workflowName, nodes, edges) {
  const startId = findStartNodeId(nodes);
  if (!startId) {
    return { ok: false, workflowName: workflowName || '', steps: [], error: 'Canvas needs a Start node.' };
  }

  const { order, error } = computeWorkflowRunOrder(startId, nodes, edges);
  if (error) {
    return { ok: false, workflowName: workflowName || '', steps: [], error };
  }

  const steps = order.map((nodeId) => {
    const node = nodes.find((n) => n.id === nodeId);
    return node ? summarizeNode(node) : { id: nodeId, kind: 'missing', title: '?', detail: 'Missing node' };
  });

  return { ok: true, workflowName: workflowName || '', steps, error: null };
}
