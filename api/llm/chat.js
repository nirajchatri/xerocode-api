import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from '../controlDb/sqlserver.js';

function normalizeImageAttachments(raw) {
  if (!Array.isArray(raw)) return [];
  const allowedMime = new Set(['image/png', 'image/jpeg', 'image/webp', 'image/gif']);
  const out = [];
  for (const x of raw.slice(0, 8)) {
    let mimeType = String(x?.mimeType ?? '')
      .trim()
      .toLowerCase()
      .replace(/^image\/jpg$/, 'image/jpeg');
    const dataBase64 = String(x?.dataBase64 ?? '')
      .trim()
      .replace(/\s/g, '');
    if (!mimeType || !allowedMime.has(mimeType) || !dataBase64) continue;
    if (dataBase64.length > 14_000_000) continue;
    out.push({ mimeType, dataBase64 });
    if (out.length >= 6) break;
  }
  return out;
}

export const chatWithLlm = async (req, res) => {
  const body = req.body ?? {};
  const provider = String(body.provider ?? '').trim().toLowerCase();
  const model = String(body.model ?? '').trim();
  const incomingUserMessage = String(body.userMessage ?? '').trim();
  const dataSourceName = String(body.dataSourceName ?? '').trim();
  const basePrompt = String(body.basePrompt ?? '').trim();
  const expectJson = Boolean(body.expectJson);
  const systemPromptOverride = String(body.systemPrompt ?? '').trim();
  const maxTokens = Number.isFinite(Number(body.maxTokens)) && Number(body.maxTokens) > 0
    ? Math.min(Math.floor(Number(body.maxTokens)), 16384)
    : 4096;

  const attachments = normalizeImageAttachments(body.attachments);

  if (!provider || !model || (!incomingUserMessage && attachments.length === 0)) {
    return res.status(400).json({
      ok: false,
      message: 'provider, model, and either userMessage or image attachments are required.',
    });
  }

  const userMessage =
    incomingUserMessage ||
    (attachments.length > 0
      ? 'The user attached dashboard reference image(s). Analyze the mockup(s): layout, chart types, KPIs, filters, and hierarchy. Tie recommendations to any datasource and constraints described above.'
      : '');

  const defaultSystemPrompt = expectJson
    ? 'You are a senior web application generator. Always return strict, valid JSON only when a JSON schema is requested. Never include markdown code fences, prose, or explanations alongside JSON output. Follow every instruction in the user message exactly.'
    : 'You are an AI assistant helping build a data-driven web application. Follow user instructions precisely. When the user asks for JSON, return only JSON. Otherwise be concise and practical.';
  const systemPrompt = systemPromptOverride || defaultSystemPrompt;

  // Pass userMessage through verbatim. Frontend is responsible for embedding
  // datasource/base-prompt context inside userMessage; we only forward extras
  // when the caller did NOT include them, to avoid double-injection.
  const userContent = userMessage;
  const contextNotes = [];
  if (dataSourceName && !userMessage.toLowerCase().includes('datasource')) {
    contextNotes.push(`Datasource: ${dataSourceName}`);
  }
  if (basePrompt && basePrompt !== userMessage && !userMessage.includes(basePrompt)) {
    contextNotes.push(`Base prompt: ${basePrompt}`);
  }
  const finalUserContent = contextNotes.length > 0
    ? `${contextNotes.join('\n')}\n\n${userContent}`
    : userContent;

  try {
    let configuredModel = model;
    let configuredApiKey = '';
    let configuredBaseUrl = '';
    let connection;
    try {
      connection = await connectToControlSqlServer();
      const result = await connection
        .request()
        .input('provider', sql.NVarChar, provider)
        .query(
          `SELECT TOP 1 model_name, api_key, base_url FROM LLM_Config WHERE provider = @provider`
        );
      const row = Array.isArray(result.recordset) && result.recordset.length > 0 ? result.recordset[0] : null;
      if (row) {
        configuredModel = String(row.model_name || configuredModel || '').trim() || configuredModel;
        configuredApiKey = String(row.api_key || '').trim();
        configuredBaseUrl = String(row.base_url || '').trim();
      }
    } catch {
      /* config table optional fallback */
    } finally {
      await closeControlSqlServer(connection);
    }

    if (provider === 'google') {
      const key = configuredApiKey || process.env.GEMINI_API_KEY || process.env.API_KEY;
      if (!key) {
        throw new Error('GEMINI_API_KEY is missing.');
      }
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(configuredModel)}:generateContent?key=${encodeURIComponent(key)}`;
      const generationConfig = {
        maxOutputTokens: maxTokens,
        temperature: expectJson ? 0.2 : 0.4,
      };
      if (expectJson) {
        generationConfig.responseMimeType = 'application/json';
      }
      const userParts = [];
      for (const a of attachments) {
        userParts.push({ inlineData: { mimeType: a.mimeType, data: a.dataBase64 } });
      }
      userParts.push({ text: finalUserContent });

      const r = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          systemInstruction: { parts: [{ text: systemPrompt }] },
          contents: [{ role: 'user', parts: userParts }],
          generationConfig,
        }),
      });
      const j = await r.json().catch(() => ({}));
      const text =
        j?.candidates?.[0]?.content?.parts?.map((p) => p?.text || '').join('').trim() ||
        j?.candidates?.[0]?.content?.parts?.[0]?.text ||
        '';
      if (!r.ok || !text) {
        throw new Error(j?.error?.message || 'Google model failed to generate response.');
      }
      return res.json({ ok: true, reply: text });
    }

    if (provider === 'openai') {
      const key = configuredApiKey || process.env.OPENAI_API_KEY;
      if (!key) {
        throw new Error('OPENAI_API_KEY is missing.');
      }
      const base = configuredBaseUrl || 'https://api.openai.com';
      let userContentOpenAi = /** @type {string | object[]} */ (finalUserContent);
      if (attachments.length > 0) {
        userContentOpenAi = [{ type: 'text', text: finalUserContent }];
        for (const a of attachments) {
          userContentOpenAi.push({
            type: 'image_url',
            image_url: { url: `data:${a.mimeType};base64,${a.dataBase64}` },
          });
        }
      }
      const payload = {
        model: configuredModel,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userContentOpenAi },
        ],
        temperature: expectJson ? 0.2 : 0.4,
        max_tokens: maxTokens,
      };
      if (expectJson) {
        payload.response_format = { type: 'json_object' };
      }
      const r = await fetch(`${base}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${key}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => ({}));
      const text = String(j?.choices?.[0]?.message?.content || '').trim();
      if (!r.ok || !text) {
        throw new Error(j?.error?.message || 'OpenAI model failed to generate response.');
      }
      return res.json({ ok: true, reply: text });
    }

    if (provider === 'anthropic') {
      const key = configuredApiKey || process.env.ANTHROPIC_API_KEY;
      if (!key) {
        throw new Error('ANTHROPIC_API_KEY is missing.');
      }
      const base = configuredBaseUrl || 'https://api.anthropic.com';
      const r = await fetch(`${base}/v1/messages`, {
        method: 'POST',
        headers: {
          'x-api-key': key,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          model: configuredModel,
          max_tokens: maxTokens,
          temperature: expectJson ? 0.2 : 0.4,
          system: systemPrompt,
          messages: [
            {
              role: 'user',
              content: (() => {
                const blocks = [];
                for (const a of attachments) {
                  blocks.push({
                    type: 'image',
                    source: { type: 'base64', media_type: a.mimeType, data: a.dataBase64 },
                  });
                }
                blocks.push({ type: 'text', text: finalUserContent });
                return blocks.length === 1 && blocks[0].type === 'text'
                  ? finalUserContent
                  : blocks;
              })(),
            },
          ],
        }),
      });
      const j = await r.json().catch(() => ({}));
      const text = String(j?.content?.[0]?.text || '').trim();
      if (!r.ok || !text) {
        throw new Error(j?.error?.message || 'Anthropic model failed to generate response.');
      }
      return res.json({ ok: true, reply: text });
    }

    if (provider === 'deepseek') {
      const key = configuredApiKey || process.env.DEEPSEEK_API_KEY;
      if (!key) {
        throw new Error('DEEPSEEK_API_KEY is missing.');
      }
      const base = configuredBaseUrl || 'https://api.deepseek.com';
      /** Same multimodal shape as OpenAI chat completions; requires a vision-capable DeepSeek model in LLM config. */
      let deepSeekUserContent = /** @type {string | object[]} */ (finalUserContent);
      if (attachments.length > 0) {
        deepSeekUserContent = [{ type: 'text', text: finalUserContent }];
        for (const a of attachments) {
          deepSeekUserContent.push({
            type: 'image_url',
            image_url: { url: `data:${a.mimeType};base64,${a.dataBase64}` },
          });
        }
      }
      const payload = {
        model: configuredModel,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: deepSeekUserContent },
        ],
        temperature: expectJson ? 0.2 : 0.4,
        max_tokens: maxTokens,
      };
      if (expectJson) {
        payload.response_format = { type: 'json_object' };
      }
      const r = await fetch(`${base}/chat/completions`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${key}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => ({}));
      const text = String(j?.choices?.[0]?.message?.content || '').trim();
      if (!r.ok || !text) {
        throw new Error(j?.error?.message || 'DeepSeek model failed to generate response.');
      }
      return res.json({ ok: true, reply: text });
    }

    return res.status(400).json({ ok: false, message: `Unsupported provider: ${provider}` });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to generate LLM response.';
    return res.status(500).json({ ok: false, message });
  }
};

export const testLlmConfig = async (req, res) => {
  const provider = String(req.body?.provider ?? '').trim().toLowerCase();
  const model = String(req.body?.model ?? '').trim();
  const apiKey = String(req.body?.apiKey ?? '').trim();
  const baseUrl = String(req.body?.baseUrl ?? '').trim();

  if (!provider || !model || !apiKey) {
    return res.status(400).json({ ok: false, message: 'provider, model, and apiKey are required.' });
  }

  try {
    if (provider === 'google') {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: 'Respond with: OK' }] }],
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        throw new Error(j?.error?.message || 'Google test failed.');
      }
      return res.json({ ok: true, message: 'Google connection successful.' });
    }

    if (provider === 'openai') {
      const base = baseUrl || 'https://api.openai.com';
      const r = await fetch(`${base}/v1/chat/completions`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          messages: [{ role: 'user', content: 'Respond with: OK' }],
          max_tokens: 8,
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        throw new Error(j?.error?.message || 'OpenAI test failed.');
      }
      return res.json({ ok: true, message: 'OpenAI connection successful.' });
    }

    if (provider === 'anthropic') {
      const base = baseUrl || 'https://api.anthropic.com';
      const r = await fetch(`${base}/v1/messages`, {
        method: 'POST',
        headers: {
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          model,
          max_tokens: 8,
          messages: [{ role: 'user', content: 'Respond with: OK' }],
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        throw new Error(j?.error?.message || 'Anthropic test failed.');
      }
      return res.json({ ok: true, message: 'Anthropic connection successful.' });
    }

    if (provider === 'deepseek') {
      const base = baseUrl || 'https://api.deepseek.com';
      const r = await fetch(`${base}/chat/completions`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          messages: [{ role: 'user', content: 'Respond with: OK' }],
          max_tokens: 8,
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) {
        throw new Error(j?.error?.message || 'DeepSeek test failed.');
      }
      return res.json({ ok: true, message: 'DeepSeek connection successful.' });
    }

    return res.status(400).json({ ok: false, message: `Unsupported provider: ${provider}` });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Connection test failed.';
    return res.status(500).json({ ok: false, message });
  }
};

