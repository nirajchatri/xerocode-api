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

function bearerHeader(raw) {
  const t = String(raw ?? '').trim();
  if (!t) return null;
  if (/^Bearer\s+/i.test(t)) return t;
  return `Bearer ${t}`;
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
    /** @type {URL} */
    let urlObj;
    try {
      urlObj = new URL(urlString);
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
      response = await fetch(urlString, {
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
      const hint = trimmed && !trimmed.startsWith('<') ? ` ${trimmed.slice(0, 160)}${trimmed.length > 160 ? '…' : ''}` : '';
      return res.status(422).json({
        ok: false,
        error: `Could not complete handshake (HTTP ${response.status}).${hint}`,
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

    const preview = trimmed.slice(0, 120);
    return res.status(422).json({
      ok: false,
      error: `Unexpected response (${response.status}). Check that this is your MCP Streamable HTTP endpoint.${preview ? ` Preview: ${preview}${trimmed.length > 120 ? '…' : ''}` : ''}`,
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
