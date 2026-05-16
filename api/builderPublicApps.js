import crypto from 'node:crypto';
import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';
import {
  getConnectionFkLookup,
  getConnectionTableData,
  getConnectionTableForeignKeys,
  mutateConnectionTableData,
  saveMasterDetailBundle,
} from './connections/schemaRoutes.js';
import { buildNlGridSearchPrompt, parseNlGridSearchReply } from './nlGridSearch.js';
import { invokeLlmChat } from './llm/chat.js';

const slugRegexBuilderApp = /^bapp-[a-f0-9]{24}$/i;

const parsePayload = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

const ensureBuilderPublicAppsTable = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.builder_public_apps', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.builder_public_apps (
        slug NVARCHAR(80) NOT NULL PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        title NVARCHAR(512) NOT NULL,
        schema_connection_id INT NOT NULL,
        connector_type NVARCHAR(32) NOT NULL,
        payload_json NVARCHAR(MAX) NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_builder_public_apps_owner ON dbo.builder_public_apps (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
};

const loadAppRowBySlug = async (pool, slug, { includeOwner = false } = {}) => {
  const cols = includeOwner
    ? 'slug, title, schema_connection_id, connector_type, payload_json, updated_at, tenant_id, owner_user_id'
    : 'slug, title, schema_connection_id, connector_type, payload_json, updated_at';
  const result = await pool.request().input('slug', sql.NVarChar, slug).query(`
    SELECT TOP 1 ${cols}
    FROM dbo.builder_public_apps
    WHERE slug = @slug
  `);
  return result.recordset?.[0] || null;
};

const publicConfigFromStored = (payload) => {
  const p = parsePayload(payload);
  const { schemaConnectionId: _cid, ...rest } = p;
  return rest;
};

const findPublishedScreen = (config, screenId) => {
  const screens = Array.isArray(config?.appScreens) ? config.appScreens : [];
  return screens.find((s) => String(s?.id || '') === String(screenId || '')) || null;
};

export const publishBuilderPublicApp = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to publish an application.' });
    }

    const titleRaw = req.body?.title;
    const title =
      typeof titleRaw === 'string' && titleRaw.trim() ? String(titleRaw).trim().slice(0, 500) : 'Application';

    const schemaConnectionId = Number(req.body?.schemaConnectionId);
    if (!Number.isFinite(schemaConnectionId) || schemaConnectionId <= 0) {
      return res.status(400).json({ ok: false, message: 'schemaConnectionId is required.' });
    }

    const connectorType = String(req.body?.connectorType || 'mysql').trim().slice(0, 32) || 'mysql';
    const appConfig = req.body?.app;
    if (!appConfig || typeof appConfig !== 'object' || Array.isArray(appConfig)) {
      return res.status(400).json({ ok: false, message: 'Request body must include an app object.' });
    }

    const screens = Array.isArray(appConfig.appScreens) ? appConfig.appScreens : [];
    if (screens.length === 0) {
      return res.status(400).json({ ok: false, message: 'At least one screen with a form is required.' });
    }

    const slug = `bapp-${crypto.randomBytes(12).toString('hex')}`;
    const now = Date.now();
    const payloadToStore = { ...appConfig, schemaConnectionId };

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicAppsTable(pool);
    await pool
      .request()
      .input('slug', sql.NVarChar, slug)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('title', sql.NVarChar, title)
      .input('schemaConnectionId', sql.Int, schemaConnectionId)
      .input('connectorType', sql.NVarChar, connectorType)
      .input('payloadJson', sql.NVarChar(sql.MAX), JSON.stringify(payloadToStore))
      .input('updatedAt', sql.BigInt, now)
      .query(`
        INSERT INTO dbo.builder_public_apps (slug, tenant_id, owner_user_id, title, schema_connection_id, connector_type, payload_json, updated_at)
        VALUES (@slug, @tenantId, @ownerUserId, @title, @schemaConnectionId, @connectorType, @payloadJson, @updatedAt)
      `);

    return res.json({ ok: true, slug, title, updatedAt: now });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to publish application.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const updateBuilderPublicApp = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to update an application.' });
    }

    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderApp.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid application slug.' });
    }

    const titleRaw = req.body?.title;
    const title =
      typeof titleRaw === 'string' && titleRaw.trim() ? String(titleRaw).trim().slice(0, 500) : 'Application';

    const schemaConnectionId = Number(req.body?.schemaConnectionId);
    if (!Number.isFinite(schemaConnectionId) || schemaConnectionId <= 0) {
      return res.status(400).json({ ok: false, message: 'schemaConnectionId is required.' });
    }

    const connectorType = String(req.body?.connectorType || 'mysql').trim().slice(0, 32) || 'mysql';
    const appConfig = req.body?.app;
    if (!appConfig || typeof appConfig !== 'object' || Array.isArray(appConfig)) {
      return res.status(400).json({ ok: false, message: 'Request body must include an app object.' });
    }

    const screens = Array.isArray(appConfig.appScreens) ? appConfig.appScreens : [];
    if (screens.length === 0) {
      return res.status(400).json({ ok: false, message: 'At least one screen with a form is required.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicAppsTable(pool);
    const existing = await loadAppRowBySlug(pool, slug, { includeOwner: true });
    if (!existing) {
      return res.status(404).json({ ok: false, message: 'Application not found.' });
    }
    if (
      Number(existing.tenant_id) !== Number(tenantId) ||
      Number(existing.owner_user_id) !== Number(userId)
    ) {
      return res.status(403).json({ ok: false, message: 'You do not have permission to update this application.' });
    }

    const now = Date.now();
    const payloadToStore = { ...appConfig, schemaConnectionId };

    await pool
      .request()
      .input('slug', sql.NVarChar, slug)
      .input('title', sql.NVarChar, title)
      .input('schemaConnectionId', sql.Int, schemaConnectionId)
      .input('connectorType', sql.NVarChar, connectorType)
      .input('payloadJson', sql.NVarChar(sql.MAX), JSON.stringify(payloadToStore))
      .input('updatedAt', sql.BigInt, now)
      .query(`
        UPDATE dbo.builder_public_apps
        SET title = @title,
            schema_connection_id = @schemaConnectionId,
            connector_type = @connectorType,
            payload_json = @payloadJson,
            updated_at = @updatedAt
        WHERE slug = @slug
      `);

    return res.json({ ok: true, slug, title, updatedAt: now });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to update application.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicBuilderApp = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderApp.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid application slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicAppsTable(pool);
    const row = await loadAppRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Application not found.' });
    }

    return res.json({
      ok: true,
      app: {
        slug: String(row.slug),
        title: String(row.title || ''),
        connectorType: String(row.connector_type || 'mysql'),
        updatedAt: Number(row.updated_at || Date.now()),
        config: publicConfigFromStored(row.payload_json),
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load application.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

const proxyConnectionRoute = async (req, res, slug, handler) => {
  let pool;
  try {
    if (!slug || !slugRegexBuilderApp.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid application slug.' });
    }
    pool = await connectToControlSqlServer();
    await ensureBuilderPublicAppsTable(pool);
    const row = await loadAppRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Application not found.' });
    }
    const connectionId = String(row.schema_connection_id);
    const proxyReq = { ...req, params: { ...req.params, id: connectionId }, query: req.query };
    return handler(proxyReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Request failed.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicBuilderAppTableForeignKeys = async (req, res) => {
  const slug = String(req.params?.slug || '').trim();
  return proxyConnectionRoute(req, res, slug, getConnectionTableForeignKeys);
};

export const getPublicBuilderAppFkLookup = async (req, res) => {
  const slug = String(req.params?.slug || '').trim();
  return proxyConnectionRoute(req, res, slug, getConnectionFkLookup);
};

export const getPublicBuilderAppTableData = async (req, res) => {
  const slug = String(req.params?.slug || '').trim();
  return proxyConnectionRoute(req, res, slug, getConnectionTableData);
};

export const postPublicBuilderAppMutate = async (req, res) => {
  const slug = String(req.params?.slug || '').trim();
  return proxyConnectionRoute(req, res, slug, mutateConnectionTableData);
};

const loadPreferredLlmConfig = async (pool) => {
  const result = await pool.request().query(`
    SELECT TOP 1 provider, model_name, api_key, base_url
    FROM dbo.LLM_Config
    WHERE api_key IS NOT NULL AND LTRIM(RTRIM(api_key)) <> ''
    ORDER BY CASE provider WHEN 'google' THEN 0 WHEN 'openai' THEN 1 WHEN 'anthropic' THEN 2 ELSE 3 END, provider ASC
  `);
  return result.recordset?.[0] || null;
};

export const postPublicBuilderAppGridSearch = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderApp.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid application slug.' });
    }

    const query = String(req.body?.query ?? '').trim();
    if (!query) {
      return res.json({ ok: true, spec: { summary: '' } });
    }

    const screenId = String(req.body?.screenId || '').trim();
    if (!screenId) {
      return res.status(400).json({ ok: false, message: 'screenId is required.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicAppsTable(pool);
    const row = await loadAppRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Application not found.' });
    }

    const stored = parsePayload(row.payload_json);
    const screen = findPublishedScreen(stored, screenId);
    if (!screen) {
      return res.status(400).json({ ok: false, message: 'Screen not found.' });
    }

    const table = String(screen.table || screen.form?.masterTable || '').trim();
    const listColumns = Array.isArray(screen.listColumns) ? screen.listColumns.filter(Boolean) : [];
    if (!table || listColumns.length === 0) {
      return res.json({ ok: true, spec: { summary: query, keywords: query } });
    }

    const fieldLabels = stored.fieldLabels && typeof stored.fieldLabels === 'object' ? stored.fieldLabels : {};
    const tableLabels = fieldLabels[table] && typeof fieldLabels[table] === 'object' ? fieldLabels[table] : {};
    const columnHints = listColumns.map((name) => {
      const label = tableLabels[name]?.trim();
      return { name, type: 'text', ...(label ? { label } : {}) };
    });

    const llmRow = await loadPreferredLlmConfig(pool);
    if (!llmRow) {
      return res.json({ ok: true, spec: { summary: query, keywords: query }, fallback: true });
    }

    const provider = String(llmRow.provider || 'google').trim().toLowerCase();
    const model = String(llmRow.model_name || '').trim();
    if (!model) {
      return res.json({ ok: true, spec: { summary: query, keywords: query }, fallback: true });
    }

    const prompt = buildNlGridSearchPrompt(table, columnHints, query);
    try {
      const payload = await invokeLlmChat({
        provider,
        model,
        userMessage: prompt,
        expectJson: true,
        maxTokens: 1024,
        systemPrompt:
          'You translate natural-language data questions into JSON search specs. Return strict valid JSON only, no markdown.',
      });
      const reply = String(payload?.reply || '').trim();
      const spec = parseNlGridSearchReply(
        reply,
        listColumns,
        query
      );
      return res.json({ ok: true, spec });
    } catch (llmErr) {
      const message = llmErr instanceof Error ? llmErr.message : 'LLM unavailable';
      return res.json({
        ok: true,
        spec: { summary: query, keywords: query },
        fallback: true,
        message,
      });
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to interpret search.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const submitPublicBuilderAppScreen = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderApp.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid application slug.' });
    }

    const screenId = String(req.body?.screenId || '').trim();
    if (!screenId) {
      return res.status(400).json({ ok: false, message: 'screenId is required.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicAppsTable(pool);
    const row = await loadAppRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Application not found.' });
    }

    const stored = parsePayload(row.payload_json);
    const screen = findPublishedScreen(stored, screenId);
    if (!screen?.form) {
      return res.status(400).json({ ok: false, message: 'Screen not found or has no form.' });
    }

    const connectionId = String(row.schema_connection_id);
    const form = screen.form;
    const masterTable = String(form.masterTable || '').trim();
    const masterValues =
      req.body?.masterValues && typeof req.body.masterValues === 'object' ? req.body.masterValues : {};
    const detailBundlesRaw = Array.isArray(req.body?.detailBundles) ? req.body.detailBundles : [];
    const hasDetailRows = detailBundlesRaw.some(
      (b) => Array.isArray(b?.detailRows) && b.detailRows.some((r) => r && typeof r === 'object')
    );

    if (!hasDetailRows) {
      const proxyReq = {
        ...req,
        params: { id: connectionId },
        body: { table: masterTable, action: 'create', values: masterValues },
      };
      return mutateConnectionTableData(proxyReq, res);
    }

    const proxyReq = {
      ...req,
      params: { id: connectionId },
      body: {
        masterTable,
        masterPkColumn: String(form.masterPkColumn || 'id').trim() || 'id',
        masterFormFields: Array.isArray(form.masterFormFields) ? form.masterFormFields : null,
        masterValues,
        detailBundles: detailBundlesRaw,
      },
    };
    return saveMasterDetailBundle(proxyReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to submit.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
