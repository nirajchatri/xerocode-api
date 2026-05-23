/**
 * POST /api/agent-studio/mcp/test
 * Server-side MCP HTTP reachability + lightweight JSON-RPC initialize check.
 */

const INIT_RPC = Object.freeze({
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'xerocode-agent-studio', version: '0.0.0' },
  },
});

const FETCH_TIMEOUT_MS = 18_000;

/** @param {string} urlString @param {string} body */
function mcpUnexpectedResponseError(status, body, urlString) {
  const trimmed = String(body ?? '').trim();
  const lower = trimmed.toLowerCase();
  let host = '';
  try {
    host = new URL(urlString).hostname.toLowerCase();
  } catch {
    host = '';
  }

  const isHtml = lower.startsWith('<!doctype') || lower.startsWith('<html');

  if (isHtml) {
    if (host.includes('accounts.google.com') || (host.includes('google.com') && lower.includes('signin'))) {
      return (
        'That URL is a Google sign-in page, not an MCP server. Gmail does not expose accounts.google.com as a Streamable HTTP MCP endpoint. ' +
        'Use a hosted MCP bridge (e.g. Zapier MCP, Pipedream) or your own HTTPS MCP server URL, and put your OAuth access token in Credential.'
      );
    }
    if (host.includes('login.microsoftonline.com') || host.includes('login.live.com')) {
      return (
        'That URL is a Microsoft login page, not an MCP server. Use a remote Streamable HTTP MCP bridge URL plus a Graph OAuth token in Credential.'
      );
    }
    if (host.includes('console.cloud.google.com')) {
      return (
        'That URL is the Google Cloud Console, not an MCP server. Create OAuth credentials there, then point URL at your MCP bridge HTTPS endpoint.'
      );
    }
    return (
      'The server returned an HTML web page instead of MCP JSON-RPC. Paste your Streamable HTTP MCP endpoint (HTTPS), not a sign-in or marketing page.'
    );
  }

  const preview = trimmed.slice(0, 120);
  return `Unexpected response (${status}). Check that this is your MCP Streamable HTTP endpoint.${preview ? ` Preview: ${preview}${trimmed.length > 120 ? '…' : ''}` : ''}`;
}

function bearerHeader(raw) {
  const t = String(raw ?? '').trim();
  if (!t) return null;
  if (/^Bearer\s+/i.test(t)) return t;
  return `Bearer ${t}`;
}

/** Trailing slash on path often yields 404 on Google MCP (e.g. /mcp/v1/). */
function normalizeMcpUrl(urlString) {
  const raw = String(urlString ?? '').trim();
  if (!raw) return raw;
  try {
    const u = new URL(raw);
    if (u.pathname.length > 1 && u.pathname.endsWith('/')) {
      u.pathname = u.pathname.replace(/\/+$/, '');
    }
    return u.toString();
  } catch {
    return raw.replace(/\/+$/, '');
  }
}

/** @param {number} status @param {string} body @param {string} urlString */
function mcpHandshakeError(status, body, urlString) {
  const trimmed = String(body ?? '').trim();
  let host = '';
  try {
    host = new URL(urlString).hostname.toLowerCase();
  } catch {
    host = '';
  }
  if (status === 404) {
    if (host === 'gmailmcp.googleapis.com') {
      const hadSlash = /\/mcp\/v1\/$/i.test(urlString) || /\/$/.test(urlString.replace(/\?.*$/, ''));
      return hadSlash
        ? 'HTTP 404 — remove the trailing slash. Use exactly https://gmailmcp.googleapis.com/mcp/v1 (no slash at the end).'
        : 'HTTP 404 — Gmail MCP path not found. Use https://gmailmcp.googleapis.com/mcp/v1 and enable Gmail MCP API in your Google Cloud project.';
    }
    return `Could not complete handshake (HTTP 404). Check the MCP path — many servers use /mcp or /mcp/v1 with no trailing slash.${trimmed && !trimmed.startsWith('<') ? ` ${trimmed.slice(0, 120)}` : ''}`;
  }
  const hint = trimmed && !trimmed.startsWith('<') ? ` ${trimmed.slice(0, 160)}${trimmed.length > 160 ? '…' : ''}` : '';
  return `Could not complete handshake (HTTP ${status}).${hint}`;
}

export async function postAgentStudioMcpTest(req, res) {
  const { url: urlRaw, transport, accessToken } = req.body || {};
  const transportId = String(transport || 'http').toLowerCase();
  const urlString = String(urlRaw ?? '').trim();

  try {
    if (transportId === 'stdio') {
      return res.status(422).json({
        ok: false,
        error:
          'stdio transports cannot be tested from here — start your MCP locally and use Streamable HTTP (HTTP/SSE URL) when you want a connection check.',
      });
    }
    if (!urlString) {
      return res.status(422).json({ ok: false, error: 'Enter a server URL.' });
    }
    const fetchUrl = normalizeMcpUrl(urlString);
    /** @type {URL} */
    let urlObj;
    try {
      urlObj = new URL(fetchUrl);
    } catch {
      return res.status(422).json({ ok: false, error: 'URL must be valid (e.g. https://host/path).' });
    }
    if (urlObj.protocol !== 'http:' && urlObj.protocol !== 'https:') {
      return res.status(422).json({ ok: false, error: 'Only HTTP and HTTPS URLs can be tested.' });
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
    };
    const auth = bearerHeader(accessToken);
    if (auth) headers.Authorization = auth;

    /** @type {Response} */
    let response;
    try {
      response = await fetch(fetchUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(INIT_RPC),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    const ct = response.headers.get('content-type') || '';

    if (ct.includes('text/event-stream')) {
      await response.body?.cancel().catch(() => {});
      return res.json({
        ok: true,
        detail: `Connection OK — MCP stream responded (HTTP ${response.status}).`,
        status: response.status,
      });
    }

    const text = await response.text().catch(() => '');
    const trimmed = text.trim();

    /** @type {unknown} */
    let json;
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        json = JSON.parse(trimmed);
      } catch {
        json = null;
      }
    }

    if (json && typeof json === 'object' && json.error != null) {
      const em =
        typeof json.error === 'object' && json.error && json.error.message != null
          ? String(json.error.message)
          : typeof json.error === 'string'
            ? json.error
            : 'MCP reported an error.';
      return res.status(422).json({ ok: false, error: `${em} (HTTP ${response.status}).` });
    }

    const rpcOk = Boolean(json && typeof json === 'object' && 'result' in json && json.result != null);

    if (response.status === 401 || response.status === 403) {
      return res.status(422).json({
        ok: false,
        error: response.status === 401 ? 'Unauthorized — credential missing or rejected.' : 'Forbidden — check scopes or credential.',
      });
    }

    if (!response.ok && !rpcOk) {
      return res.status(422).json({
        ok: false,
        error: mcpHandshakeError(response.status, trimmed, fetchUrl),
      });
    }

    if (rpcOk && json.result && typeof json.result === 'object' && json.result.protocolVersion != null) {
      return res.json({
        ok: true,
        detail: `Connected — MCP handshake OK (protocol ${String(json.result.protocolVersion)}).`,
        status: response.status,
      });
    }

    if (rpcOk) {
      return res.json({
        ok: true,
        detail: 'Connected — MCP server accepted initialize.',
        status: response.status,
      });
    }

    if (response.ok && !trimmed) {
      return res.json({
        ok: true,
        detail: `Connected — MCP endpoint responded with HTTP ${response.status} (no JSON body returned).`,
        status: response.status,
      });
    }

    if (response.ok && trimmed.startsWith('{')) {
      return res.json({
        ok: true,
        detail: `Connection OK — server responded (HTTP ${response.status}); response was not standard JSON-RPC result.`,
        status: response.status,
      });
    }

    return res.status(422).json({
      ok: false,
      error: mcpUnexpectedResponseError(response.status, trimmed, urlString),
    });
  } catch (err) {
    if (String(err?.name || '') === 'AbortError') {
      return res.status(422).json({ ok: false, error: 'Request timed out — host did not reply in time.' });
    }
    const msg =
      typeof err?.cause?.code === 'string' && err.cause.code === 'ENOTFOUND'
        ? 'DNS lookup failed — check the hostname.'
        : err instanceof Error
          ? err.message
          : 'Connection test failed.';
    return res.status(422).json({ ok: false, error: msg });
  }
}
