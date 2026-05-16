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

const slugRegexBuilderForm = /^bfm-[a-f0-9]{24}$/i;

const parsePayload = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

const ensureBuilderPublicFormsTable = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.builder_public_forms', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.builder_public_forms (
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
      CREATE INDEX idx_builder_public_forms_owner ON dbo.builder_public_forms (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
};

const loadFormRowBySlug = async (pool, slug) => {
  const result = await pool.request().input('slug', sql.NVarChar, slug).query(`
    SELECT TOP 1 slug, title, schema_connection_id, connector_type, payload_json, updated_at
    FROM dbo.builder_public_forms
    WHERE slug = @slug
  `);
  return result.recordset?.[0] || null;
};

const publicPayloadFromStored = (payload) => {
  const p = parsePayload(payload);
  const { schemaConnectionId: _cid, ...rest } = p;
  return rest;
};

export const publishBuilderPublicForm = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to publish a form.' });
    }

    const titleRaw = req.body?.title;
    const title =
      typeof titleRaw === 'string' && titleRaw.trim() ? String(titleRaw).trim().slice(0, 500) : 'New record form';

    const schemaConnectionId = Number(req.body?.schemaConnectionId);
    if (!Number.isFinite(schemaConnectionId) || schemaConnectionId <= 0) {
      return res.status(400).json({ ok: false, message: 'schemaConnectionId is required.' });
    }

    const connectorType = String(req.body?.connectorType || 'mysql').trim().slice(0, 32) || 'mysql';
    const formPayload = req.body?.form;
    if (!formPayload || typeof formPayload !== 'object' || Array.isArray(formPayload)) {
      return res.status(400).json({ ok: false, message: 'Request body must include a form object.' });
    }

    const kind = String(formPayload.kind || '').trim();
    if (!['simple', 'master-detail'].includes(kind)) {
      return res.status(400).json({ ok: false, message: 'form.kind must be simple or master-detail.' });
    }

    const slug = `bfm-${crypto.randomBytes(12).toString('hex')}`;
    const now = Date.now();
    const payloadToStore = {
      ...formPayload,
      kind,
      schemaConnectionId,
    };

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicFormsTable(pool);
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
        INSERT INTO dbo.builder_public_forms (slug, tenant_id, owner_user_id, title, schema_connection_id, connector_type, payload_json, updated_at)
        VALUES (@slug, @tenantId, @ownerUserId, @title, @schemaConnectionId, @connectorType, @payloadJson, @updatedAt)
      `);

    return res.json({ ok: true, slug, title, updatedAt: now });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to publish form.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicBuilderForm = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderForm.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid form slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicFormsTable(pool);
    const row = await loadFormRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Form not found.' });
    }

    return res.json({
      ok: true,
      form: {
        slug: String(row.slug),
        title: String(row.title || ''),
        connectorType: String(row.connector_type || 'mysql'),
        updatedAt: Number(row.updated_at || Date.now()),
        config: publicPayloadFromStored(row.payload_json),
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load form.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicBuilderFormTableData = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderForm.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid form slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicFormsTable(pool);
    const row = await loadFormRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Form not found.' });
    }

    const connectionId = String(row.schema_connection_id);
    const proxyReq = {
      ...req,
      params: { ...req.params, id: connectionId },
      query: req.query,
    };
    return getConnectionTableData(proxyReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load table data.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicBuilderFormTableForeignKeys = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderForm.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid form slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicFormsTable(pool);
    const row = await loadFormRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Form not found.' });
    }

    const connectionId = String(row.schema_connection_id);
    const proxyReq = {
      ...req,
      params: { ...req.params, id: connectionId },
      query: req.query,
    };
    return getConnectionTableForeignKeys(proxyReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load foreign keys.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicBuilderFormFkLookup = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderForm.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid form slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicFormsTable(pool);
    const row = await loadFormRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Form not found.' });
    }

    const connectionId = String(row.schema_connection_id);
    const proxyReq = {
      ...req,
      params: { ...req.params, id: connectionId },
      query: req.query,
    };
    return getConnectionFkLookup(proxyReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load lookup options.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const submitPublicBuilderForm = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexBuilderForm.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid form slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureBuilderPublicFormsTable(pool);
    const row = await loadFormRowBySlug(pool, slug);
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Form not found.' });
    }

    const stored = parsePayload(row.payload_json);
    const connectionId = String(row.schema_connection_id);
    const kind = String(stored.kind || 'simple');

    if (kind === 'simple') {
      const table = String(stored.simple?.table || '').trim();
      const values = req.body?.values && typeof req.body.values === 'object' ? req.body.values : {};
      if (!table) {
        return res.status(400).json({ ok: false, message: 'Form configuration is invalid.' });
      }
      const proxyReq = {
        ...req,
        params: { id: connectionId },
        body: { table, action: 'create', values },
      };
      return mutateConnectionTableData(proxyReq, res);
    }

    if (kind === 'master-detail') {
      const md = stored.masterDetail;
      if (!md || typeof md !== 'object') {
        return res.status(400).json({ ok: false, message: 'Form configuration is invalid.' });
      }
      const masterTable = String(md.masterTable || '').trim();
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
          masterPkColumn: String(md.masterPkColumn || 'id').trim() || 'id',
          masterFormFields: Array.isArray(md.masterFormFields) ? md.masterFormFields : null,
          masterValues,
          detailBundles: detailBundlesRaw,
        },
      };
      return saveMasterDetailBundle(proxyReq, res);
    }

    return res.status(400).json({ ok: false, message: 'Unsupported form kind.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to submit form.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
