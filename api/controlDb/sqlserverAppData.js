import crypto from 'node:crypto';
import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './sqlserver.js';
import { getSqlServerTableDataForProfile } from '../connections/sqlServerSchema.js';
import { getPostgresTableDataForProfile } from '../connections/postgresSchema.js';
import { getMySqlTableData } from './sqlserverConnections.js';

const parsePayload = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

const ensureSavedWorkspaceTables = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.saved_apps', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.saved_apps (
        id NVARCHAR(255) PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        visibility NVARCHAR(20) NOT NULL DEFAULT 'private',
        name NVARCHAR(255) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_saved_apps_tenant_owner ON dbo.saved_apps (tenant_id, owner_user_id, updated_at);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.saved_dashboards', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.saved_dashboards (
        id NVARCHAR(255) PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        visibility NVARCHAR(20) NOT NULL DEFAULT 'private',
        name NVARCHAR(255) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_saved_dashboards_tenant_owner ON dbo.saved_dashboards (tenant_id, owner_user_id, updated_at);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.saved_blueprint_apis', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.saved_blueprint_apis (
        id NVARCHAR(255) PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        visibility NVARCHAR(20) NOT NULL DEFAULT 'private',
        name NVARCHAR(512) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        saved_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME())
      );
      CREATE INDEX idx_saved_blueprint_apis_tenant_owner ON dbo.saved_blueprint_apis (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.saved_external_apis', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.saved_external_apis (
        id NVARCHAR(255) PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        visibility NVARCHAR(20) NOT NULL DEFAULT 'private',
        name NVARCHAR(512) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        saved_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME())
      );
      CREATE INDEX idx_saved_external_apis_tenant_owner ON dbo.saved_external_apis (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.design_studio_public_previews', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.design_studio_public_previews (
        slug NVARCHAR(80) NOT NULL PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        title NVARCHAR(512) NOT NULL,
        proposal_json NVARCHAR(MAX) NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_design_studio_public_previews_owner ON dbo.design_studio_public_previews (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
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
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.saved_studio_agents', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.saved_studio_agents (
        id NVARCHAR(255) PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        visibility NVARCHAR(20) NOT NULL DEFAULT 'private',
        name NVARCHAR(255) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_saved_studio_agents_tenant_owner ON dbo.saved_studio_agents (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.workspace_guardrails_catalog', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.workspace_guardrails_catalog (
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        updated_at BIGINT NOT NULL,
        CONSTRAINT PK_workspace_guardrails_catalog PRIMARY KEY (tenant_id, owner_user_id)
      );
    END
  `);
};

const getColumnDataType = async (pool, tableName, columnName) => {
  const rs = await pool
    .request()
    .input('tableName', sql.NVarChar, tableName)
    .input('columnName', sql.NVarChar, columnName)
    .query(`
      SELECT DATA_TYPE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @tableName AND COLUMN_NAME = @columnName
    `);
  return String(rs.recordset?.[0]?.DATA_TYPE || '').toLowerCase();
};

export const listSavedApps = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, userId != null ? Number(userId) : -1).query(`
        SELECT id, tenant_id, owner_user_id, visibility, name, payload, updated_at
        FROM dbo.saved_apps
        WHERE tenant_id = @tenantId
          AND (owner_user_id = @userId OR visibility = 'tenant_shared')
        ORDER BY updated_at DESC
      `);
    const apps = (result.recordset || []).map((row) => ({
      id: String(row.id),
      name: String(row.name || ''),
      ownerUserId: row.owner_user_id != null ? Number(row.owner_user_id) : null,
      visibility: String(row.visibility || 'private'),
      updatedAt: Number(row.updated_at || Date.now()),
      payload: parsePayload(row.payload),
    }));
    return res.json({ ok: true, apps });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list saved apps.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveAppRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const body = req.body || {};
    const id = String(body.id || `app-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
    const name = String(body.name || '').trim();
    const visibility =
      body.visibility === 'public'
        ? 'public'
        : body.visibility === 'tenant_shared'
          ? 'tenant_shared'
          : 'private';
    const payload = body.payload ?? {};
    const updatedAt = Number(body.updatedAt) || Date.now();
    if (!name) return res.status(400).json({ ok: false, message: 'App name is required.' });

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const appCreatedType = await getColumnDataType(pool, 'saved_apps', 'created_at');
    const appUpdatedType = await getColumnDataType(pool, 'saved_apps', 'updated_at');
    const appCreatedBigInt = appCreatedType === 'bigint';
    const appUpdatedBigInt = appUpdatedType === 'bigint';
    const appReq = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('visibility', sql.NVarChar, visibility)
      .input('name', sql.NVarChar, name)
      .input('payload', sql.NVarChar(sql.MAX), JSON.stringify(payload))
      .input('updatedAt', sql.BigInt, updatedAt);
    const appCreatedExpr = appCreatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const appUpdatedExpr = appUpdatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const appExists = await pool.request().input('id', sql.NVarChar, id).query(`SELECT TOP 1 id FROM dbo.saved_apps WHERE id = @id`);
    if (appExists.recordset?.length) {
      await appReq.query(`
        UPDATE dbo.saved_apps
        SET name = @name, visibility = @visibility, payload = @payload, updated_at = ${appUpdatedExpr}
        WHERE id = @id
      `);
    } else {
      await appReq.query(`
        INSERT INTO dbo.saved_apps (id, tenant_id, owner_user_id, visibility, name, payload, created_at, updated_at)
        VALUES (@id, @tenantId, @ownerUserId, @visibility, @name, @payload, ${appCreatedExpr}, ${appUpdatedExpr})
      `);
    }
    return res.json({ ok: true, id });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save app.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicAppTableData = async (req, res) => {
  let pool;
  try {
    const id = String(req.params?.id || '').trim();
    const table = String(req.query?.table || '').trim();
    if (!id || !table) {
      return res.status(400).json({ ok: false, message: 'Missing app id or table.' });
    }
    const limit = Math.min(
      Math.max(Number.isFinite(Number(req.query.limit)) ? Number(req.query.limit) : 50, 1),
      5000
    );
    const offset = Math.max(
      Number.isFinite(Number(req.query.offset)) ? Number(req.query.offset) : 0,
      0
    );
    const search = String(req.query.q || '').trim();
    let filters = {};
    if (typeof req.query.filters === 'string') {
      try {
        const parsed = JSON.parse(req.query.filters);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) filters = parsed;
      } catch {
        filters = {};
      }
    }

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const appRs = await pool.request().input('id', sql.NVarChar, id).query(`
      SELECT TOP 1 id, payload
      FROM dbo.saved_apps
      WHERE id = @id AND visibility = 'public'
    `);
    const appRow = appRs.recordset?.[0];
    if (!appRow) {
      return res.status(404).json({ ok: false, message: 'Public webpage not found.' });
    }
    const payload = parsePayload(appRow.payload);
    const allowedTables = Array.isArray(payload?.selectedTables) ? payload.selectedTables : [];
    if (!allowedTables.includes(table)) {
      return res.status(403).json({ ok: false, message: 'Table is not exposed by this webpage.' });
    }
    const connectionId = payload?.schemaConnectionId;
    if (!connectionId) {
      return res.status(400).json({ ok: false, message: 'This webpage has no underlying connection.' });
    }

    const profileRs = await pool.request().input('cid', sql.Int, Number(connectionId)).query(`
      SELECT TOP 1 id, connector_type, host, port, database_name, username, password_value
      FROM dbo.connection_profiles
      WHERE id = @cid
    `);
    const profile = profileRs.recordset?.[0];
    if (!profile) {
      return res.status(404).json({ ok: false, message: 'Underlying connection not found.' });
    }

    if (profile.connector_type === 'sqlserver') {
      const data = await getSqlServerTableDataForProfile(profile, table, limit, offset, {
        q: search,
        filters,
      });
      return res.json({ ok: true, ...data, limit, offset });
    }

    if (profile.connector_type === 'postgresql') {
      const data = await getPostgresTableDataForProfile(profile, table, limit, offset, {
        q: search,
        filters,
      });
      return res.json({ ok: true, ...data, limit, offset });
    }

    if (profile.connector_type === 'mysql') {
      req.params.id = String(connectionId);
      req.query = { ...req.query, table };
      return getMySqlTableData(req, res);
    }

    return res.status(400).json({ ok: false, message: 'Unsupported connector type.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load public webpage data.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

const slugRegexDesignStudioPreview = /^dsp-[a-f0-9]{24}$/i;

const PUBLISH_LIVE_SNAPSHOT_KEY = '__publish_live_snapshot';
const PUBLISH_LIVE_SNAPSHOT_MAX_ROWS = 500;

const sanitizePublishLiveSnapshotRows = (rows) => {
  if (!Array.isArray(rows)) return [];
  const out = [];
  for (let i = 0; i < rows.length && out.length < PUBLISH_LIVE_SNAPSHOT_MAX_ROWS; i++) {
    const r = rows[i];
    if (r && typeof r === 'object' && !Array.isArray(r)) {
      out.push({ ...r });
    }
  }
  return out;
};

export const publishDesignStudioPreview = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to publish a preview.' });
    }
    const proposal = req.body?.proposal;
    if (!proposal || typeof proposal !== 'object' || Array.isArray(proposal)) {
      return res.status(400).json({ ok: false, message: 'Request body must include a proposal object.' });
    }
    const dt = proposal.dashboard_title ?? proposal.dashboardTitle;
    if (typeof dt !== 'string' || !dt.trim()) {
      return res.status(400).json({ ok: false, message: 'proposal.dashboard_title is required.' });
    }
    const titleRaw = req.body?.title ?? dt;
    const title =
      typeof titleRaw === 'string' && titleRaw.trim() ? String(titleRaw).trim().slice(0, 500) : String(dt).trim().slice(0, 500);
    const slug = `dsp-${crypto.randomBytes(12).toString('hex')}`;
    const liveSnap = req.body?.liveSnapshot;
    let proposalToStore = proposal;
    if (liveSnap && typeof liveSnap === 'object' && !Array.isArray(liveSnap)) {
      const snapRows = sanitizePublishLiveSnapshotRows(liveSnap.rows);
      if (snapRows.length > 0) {
        const sourceLabel =
          typeof liveSnap.sourceLabel === 'string' ? String(liveSnap.sourceLabel).trim().slice(0, 500) : '';
        proposalToStore = {
          ...proposal,
          [PUBLISH_LIVE_SNAPSHOT_KEY]: {
            source_label: sourceLabel || 'Linked API',
            captured_at: Date.now(),
            rows: snapRows,
          },
        };
      }
    }
    const proposalStr = JSON.stringify(proposalToStore);
    const now = Date.now();

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('slug', sql.NVarChar, slug)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('title', sql.NVarChar, title)
      .input('proposalJson', sql.NVarChar(sql.MAX), proposalStr)
      .input('updatedAt', sql.BigInt, now)
      .query(`
        INSERT INTO dbo.design_studio_public_previews (slug, tenant_id, owner_user_id, title, proposal_json, updated_at)
        VALUES (@slug, @tenantId, @ownerUserId, @title, @proposalJson, @updatedAt)
      `);

    return res.json({
      ok: true,
      slug,
      title,
      updatedAt: now,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to publish preview.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicDesignStudioPreview = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegexDesignStudioPreview.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid preview slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool.request().input('slug', sql.NVarChar, slug).query(`
      SELECT TOP 1 slug, title, proposal_json, updated_at
      FROM dbo.design_studio_public_previews
      WHERE slug = @slug
    `);
    const row = result.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Preview not found.' });
    }
    return res.json({
      ok: true,
      preview: {
        slug: String(row.slug),
        title: String(row.title || ''),
        updatedAt: Number(row.updated_at || Date.now()),
        proposal: parsePayload(row.proposal_json),
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load preview.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicAppRecord = async (req, res) => {
  let pool;
  try {
    const id = String(req.params?.id || '').trim();
    if (!id) {
      return res.status(400).json({ ok: false, message: 'Missing app id.' });
    }

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool.request().input('id', sql.NVarChar, id).query(`
      SELECT TOP 1 id, name, payload, updated_at
      FROM dbo.saved_apps
      WHERE id = @id
        AND visibility = 'public'
    `);
    const row = result.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Public webpage not found.' });
    }
    return res.json({
      ok: true,
      app: {
        id: String(row.id),
        name: String(row.name || ''),
        updatedAt: Number(row.updated_at || Date.now()),
        payload: parsePayload(row.payload),
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load public webpage.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const deleteAppRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing app id.' });
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`DELETE FROM dbo.saved_apps WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @userId`);
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete app.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const listSavedDashboards = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, userId != null ? Number(userId) : -1).query(`
        SELECT id, tenant_id, owner_user_id, visibility, name, payload, updated_at
        FROM dbo.saved_dashboards
        WHERE tenant_id = @tenantId
          AND (owner_user_id = @userId OR visibility = 'tenant_shared')
        ORDER BY updated_at DESC
      `);
    const dashboards = (result.recordset || []).map((row) => ({
      id: String(row.id),
      name: String(row.name || ''),
      ownerUserId: row.owner_user_id != null ? Number(row.owner_user_id) : null,
      visibility: String(row.visibility || 'private'),
      updatedAt: Number(row.updated_at || Date.now()),
      payload: parsePayload(row.payload),
    }));
    return res.json({ ok: true, dashboards });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list dashboards.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveDashboardRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const body = req.body || {};
    const id = String(body.id || `dash-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
    const name = String(body.name || '').trim();
    const visibility = body.visibility === 'tenant_shared' ? 'tenant_shared' : 'private';
    const payload = body.payload ?? {};
    const updatedAt = Number(body.updatedAt) || Date.now();
    if (!name) return res.status(400).json({ ok: false, message: 'Dashboard name is required.' });

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const dashCreatedType = await getColumnDataType(pool, 'saved_dashboards', 'created_at');
    const dashUpdatedType = await getColumnDataType(pool, 'saved_dashboards', 'updated_at');
    const dashCreatedBigInt = dashCreatedType === 'bigint';
    const dashUpdatedBigInt = dashUpdatedType === 'bigint';
    const dashReq = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('visibility', sql.NVarChar, visibility)
      .input('name', sql.NVarChar, name)
      .input('payload', sql.NVarChar(sql.MAX), JSON.stringify(payload))
      .input('updatedAt', sql.BigInt, updatedAt);
    const dashCreatedExpr = dashCreatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const dashUpdatedExpr = dashUpdatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const dashExists = await pool.request().input('id', sql.NVarChar, id).query(`SELECT TOP 1 id FROM dbo.saved_dashboards WHERE id = @id`);
    if (dashExists.recordset?.length) {
      await dashReq.query(`
        UPDATE dbo.saved_dashboards
        SET name = @name, visibility = @visibility, payload = @payload, updated_at = ${dashUpdatedExpr}
        WHERE id = @id
      `);
    } else {
      await dashReq.query(`
        INSERT INTO dbo.saved_dashboards (id, tenant_id, owner_user_id, visibility, name, payload, created_at, updated_at)
        VALUES (@id, @tenantId, @ownerUserId, @visibility, @name, @payload, ${dashCreatedExpr}, ${dashUpdatedExpr})
      `);
    }
    return res.json({ ok: true, id });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save dashboard.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const deleteDashboardRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing dashboard id.' });
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`DELETE FROM dbo.saved_dashboards WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @userId`);
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete dashboard.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** Agent Studio workflows — same row shape as dashboards; payload holds nodes, edges, agentKind. */
export const listSavedAgents = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, userId != null ? Number(userId) : -1).query(`
        SELECT id, tenant_id, owner_user_id, visibility, name, payload, updated_at
        FROM dbo.saved_studio_agents
        WHERE tenant_id = @tenantId
          AND (owner_user_id = @userId OR visibility = 'tenant_shared')
        ORDER BY updated_at DESC
      `);
    const agents = (result.recordset || []).map((row) => ({
      id: String(row.id),
      name: String(row.name || ''),
      ownerUserId: row.owner_user_id != null ? Number(row.owner_user_id) : null,
      visibility: String(row.visibility || 'private'),
      updatedAt: Number(row.updated_at || Date.now()),
      payload: parsePayload(row.payload),
    }));
    return res.json({ ok: true, agents });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list saved agents.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveAgentStudioRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const body = req.body || {};
    const id = String(body.id || `agent-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
    const name = String(body.name || '').trim();
    const visibility = body.visibility === 'tenant_shared' ? 'tenant_shared' : 'private';
    const payload = body.payload ?? {};
    const updatedAt = Number(body.updatedAt) || Date.now();
    if (!name) return res.status(400).json({ ok: false, message: 'Agent name is required.' });

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const aCreatedType = await getColumnDataType(pool, 'saved_studio_agents', 'created_at');
    const aUpdatedType = await getColumnDataType(pool, 'saved_studio_agents', 'updated_at');
    const aCreatedBigInt = aCreatedType === 'bigint';
    const aUpdatedBigInt = aUpdatedType === 'bigint';
    const reqAgent = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('visibility', sql.NVarChar, visibility)
      .input('name', sql.NVarChar, name)
      .input('payload', sql.NVarChar(sql.MAX), JSON.stringify(payload))
      .input('updatedAt', sql.BigInt, updatedAt);
    const aCreatedExpr = aCreatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const aUpdatedExpr = aUpdatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const aExists = await pool.request().input('id', sql.NVarChar, id).query(`SELECT TOP 1 id FROM dbo.saved_studio_agents WHERE id = @id`);
    if (aExists.recordset?.length) {
      await reqAgent.query(`
        UPDATE dbo.saved_studio_agents
        SET name = @name, visibility = @visibility, payload = @payload, updated_at = ${aUpdatedExpr}
        WHERE id = @id
      `);
    } else {
      await reqAgent.query(`
        INSERT INTO dbo.saved_studio_agents (id, tenant_id, owner_user_id, visibility, name, payload, created_at, updated_at)
        VALUES (@id, @tenantId, @ownerUserId, @visibility, @name, @payload, ${aCreatedExpr}, ${aUpdatedExpr})
      `);
    }
    return res.json({ ok: true, id });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save agent.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const deleteAgentStudioRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing agent id.' });
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(
        `DELETE FROM dbo.saved_studio_agents WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @userId`
      );
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete agent.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** Saved blueprint API configs (API Builder) — stored in control DB `xerocode`, keyed by tenant + user. */
export const listSavedBlueprintApis = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to load saved blueprint APIs.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`
        SELECT payload
        FROM dbo.saved_blueprint_apis
        WHERE tenant_id = @tenantId AND owner_user_id = @userId
        ORDER BY updated_at DESC
      `);
    const apis = (result.recordset || []).map((row) => parsePayload(row.payload));
    return res.json({ ok: true, apis });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list saved blueprint APIs.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveBlueprintApiRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to save blueprint APIs.' });
    }
    const body = req.body || {};
    const api = body.api ?? body;
    const id = String(api?.id || '').trim();
    const name = String(api?.name || '').trim();
    if (!id || !name) {
      return res.status(400).json({ ok: false, message: 'Blueprint API id and name are required.' });
    }
    const savedAt = Number(api.savedAt) || Date.now();
    const updatedAt = savedAt;
    const visibility =
      api.visibility === 'tenant_shared'
        ? 'tenant_shared'
        : api.visibility === 'public'
          ? 'public'
          : 'private';
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const payloadStr = JSON.stringify(api);
    const bpCreatedType = await getColumnDataType(pool, 'saved_blueprint_apis', 'created_at');
    const bpUpdatedType = await getColumnDataType(pool, 'saved_blueprint_apis', 'updated_at');
    const bpCreatedBigInt = bpCreatedType === 'bigint';
    const bpUpdatedBigInt = bpUpdatedType === 'bigint';
    const bpReq = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('visibility', sql.NVarChar, visibility)
      .input('name', sql.NVarChar, name)
      .input('payload', sql.NVarChar(sql.MAX), payloadStr)
      .input('savedAt', sql.BigInt, savedAt)
      .input('updatedAt', sql.BigInt, updatedAt);
    const bpCreatedExpr = bpCreatedBigInt ? '@savedAt' : 'SYSDATETIME()';
    const bpUpdatedExpr = bpUpdatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const bpExists = await pool.request().input('id', sql.NVarChar, id).query(`SELECT TOP 1 id FROM dbo.saved_blueprint_apis WHERE id = @id`);
    if (bpExists.recordset?.length) {
      await bpReq.query(`
        UPDATE dbo.saved_blueprint_apis
        SET name = @name, visibility = @visibility, payload = @payload, saved_at = @savedAt, updated_at = ${bpUpdatedExpr}
        WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @ownerUserId
      `);
    } else {
      await bpReq.query(`
        INSERT INTO dbo.saved_blueprint_apis (id, tenant_id, owner_user_id, visibility, name, payload, saved_at, updated_at, created_at)
        VALUES (@id, @tenantId, @ownerUserId, @visibility, @name, @payload, @savedAt, ${bpUpdatedExpr}, ${bpCreatedExpr})
      `);
    }
    return res.json({ ok: true, id });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save blueprint API.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const deleteBlueprintApiRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing blueprint API id.' });
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`DELETE FROM dbo.saved_blueprint_apis WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @userId`);
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete blueprint API.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const listSavedExternalApis = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to load external APIs.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`
        SELECT payload
        FROM dbo.saved_external_apis
        WHERE tenant_id = @tenantId AND owner_user_id = @userId
        ORDER BY updated_at DESC
      `);
    const externalApis = (result.recordset || []).map((row) => parsePayload(row.payload));
    return res.json({ ok: true, externalApis });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list external APIs.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveExternalApiRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to save external APIs.' });
    }
    const body = req.body || {};
    const ext = body.externalApi ?? body.api ?? body;
    const id = String(ext?.id || '').trim();
    const name = String(ext?.name || '').trim();
    if (!id || !name) {
      return res.status(400).json({ ok: false, message: 'External API id and name are required.' });
    }
    const savedAt = Number(ext.savedAt) || Date.now();
    const updatedAt = savedAt;
    const visibility = ext.visibility === 'tenant_shared' ? 'tenant_shared' : 'private';
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const payloadStr = JSON.stringify(ext);
    const exCreatedType = await getColumnDataType(pool, 'saved_external_apis', 'created_at');
    const exUpdatedType = await getColumnDataType(pool, 'saved_external_apis', 'updated_at');
    const exCreatedBigInt = exCreatedType === 'bigint';
    const exUpdatedBigInt = exUpdatedType === 'bigint';
    const exReq = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('visibility', sql.NVarChar, visibility)
      .input('name', sql.NVarChar, name)
      .input('payload', sql.NVarChar(sql.MAX), payloadStr)
      .input('savedAt', sql.BigInt, savedAt)
      .input('updatedAt', sql.BigInt, updatedAt);
    const exCreatedExpr = exCreatedBigInt ? '@savedAt' : 'SYSDATETIME()';
    const exUpdatedExpr = exUpdatedBigInt ? '@updatedAt' : 'SYSDATETIME()';
    const exExists = await pool.request().input('id', sql.NVarChar, id).query(`SELECT TOP 1 id FROM dbo.saved_external_apis WHERE id = @id`);
    if (exExists.recordset?.length) {
      await exReq.query(`
        UPDATE dbo.saved_external_apis
        SET name = @name, visibility = @visibility, payload = @payload, saved_at = @savedAt, updated_at = ${exUpdatedExpr}
        WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @ownerUserId
      `);
    } else {
      await exReq.query(`
        INSERT INTO dbo.saved_external_apis (id, tenant_id, owner_user_id, visibility, name, payload, saved_at, updated_at, created_at)
        VALUES (@id, @tenantId, @ownerUserId, @visibility, @name, @payload, @savedAt, ${exUpdatedExpr}, ${exCreatedExpr})
      `);
    }
    return res.json({ ok: true, id });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save external API.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const deleteExternalApiRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing external API id.' });
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`DELETE FROM dbo.saved_external_apis WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @userId`);
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete external API.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getWorkspaceGuardrailsCatalog = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const rs = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`
        SELECT TOP 1 payload, updated_at
        FROM dbo.workspace_guardrails_catalog
        WHERE tenant_id = @tenantId AND owner_user_id = @userId
      `);
    const row = rs.recordset?.[0];
    if (!row) return res.json({ ok: true, catalog: null, updatedAt: null });
    return res.json({
      ok: true,
      catalog: parsePayload(row.payload),
      updatedAt: row.updated_at != null ? Number(row.updated_at) : null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load Guardrails catalog.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveWorkspaceGuardrailsCatalog = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const catalog = req.body?.catalog;
    if (catalog == null || typeof catalog !== 'object' || Array.isArray(catalog)) {
      return res.status(400).json({ ok: false, message: 'A JSON catalog object is required.' });
    }
    const payload = JSON.stringify(catalog);
    if (payload.length > 950_000) {
      return res.status(400).json({ ok: false, message: 'Catalog payload too large.' });
    }
    const updatedAt = Date.now();
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('payload', sql.NVarChar(sql.MAX), payload)
      .input('updatedAt', sql.BigInt, updatedAt)
      .query(`
        MERGE dbo.workspace_guardrails_catalog AS t
        USING (
          SELECT @tenantId AS tenant_id, @ownerUserId AS owner_user_id,
                 @payload AS payload, @updatedAt AS updated_at
        ) AS s
          ON (t.tenant_id = s.tenant_id AND t.owner_user_id = s.owner_user_id)
        WHEN MATCHED THEN UPDATE SET payload = s.payload, updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (tenant_id, owner_user_id, payload, updated_at)
          VALUES (s.tenant_id, s.owner_user_id, s.payload, s.updated_at);
      `);
    return res.json({ ok: true, updatedAt });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save Guardrails catalog.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
