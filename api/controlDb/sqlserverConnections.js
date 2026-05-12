import mysql from 'mysql2/promise';
import sql from 'mssql';
import crypto from 'crypto';
import { CONNECTOR_DEFAULT_PORTS, normalizeConnectorType } from '../connections/connectorTypes.js';
import { hostFallbacks, normalizeHost, sqlServerHostForStorage } from '../connections/hostUtils.js';
import { logActivity } from '../lib/activityLog.js';
import { closeControlSqlServer, connectToControlSqlServer } from './sqlserver.js';
import { getOrCreateUserAndTenantByEmail } from './sqlserverAuth.js';
import { getPublicApiJwtSecret, signPublicApiJwt, verifyPublicApiJwt } from '../lib/publicApiJwt.js';

const makeProfileKey = (tenantId, connector, host, port, database, username) =>
  crypto
    .createHash('sha256')
    .update(
      [String(tenantId ?? ''), String(connector), String(host), String(Number(port)), String(database), String(username)].join('\u001e'),
      'utf8'
    )
    .digest('hex');

const getDefaultControlConfig = () => {
  const env = process.env || {};
  const host = normalizeHost(env.SQLSERVER_CONTROL_HOST || '');
  const port = Number(env.SQLSERVER_CONTROL_PORT || 1433) || 1433;
  const database = String(env.SQLSERVER_CONTROL_DATABASE || 'xerocode').trim().toLowerCase();
  const username = String(env.SQLSERVER_CONTROL_USER || '').trim().toLowerCase();
  return { host, port, database, username };
};

const isDefaultControlProfileRow = (row) => {
  if (!row) return false;
  const defaults = getDefaultControlConfig();
  const rowDb = String(row.database_name || '').trim().toLowerCase();
  const connector = String(row.connector_type || '').trim().toLowerCase();
  // Treat control SQL Server database as protected default even if host formatting differs.
  return connector === 'sqlserver' && rowDb === defaults.database;
};

const buildMysqlConnectionUri = (fields, { includePassword = false } = {}) => {
  const host = String(fields.host ?? '').trim();
  const port = Number(fields.port);
  const database = String(fields.database ?? '').trim();
  const username = String(fields.username ?? '').trim();
  const password = fields.password != null ? String(fields.password) : '';
  if (!host || !Number.isFinite(port) || port <= 0 || !database || !username) return '';
  const userEnc = encodeURIComponent(username);
  const dbEnc = encodeURIComponent(database);
  if (!includePassword || password === '') return `mysql://${userEnc}:***@${host}:${port}/${dbEnc}`;
  return `mysql://${userEnc}:${encodeURIComponent(password)}@${host}:${port}/${dbEnc}`;
};

const buildPreviewConnectionUri = (row) => {
  const connector = String(row.connector_type || 'mysql');
  const host = String(row.host ?? '').trim();
  const port = Number(row.port);
  const database = String(row.database_name ?? '').trim();
  const username = String(row.username ?? '').trim();
  if (connector === 'mysql') {
    return buildMysqlConnectionUri({ host, port, database, username }, { includePassword: false });
  }
  if (connector === 'postgresql' && host && Number.isFinite(port) && database && username) {
    return `postgresql://${encodeURIComponent(username)}:***@${host}:${port}/${encodeURIComponent(database)}`;
  }
  if (connector === 'sqlserver' && host && Number.isFinite(port) && database && username) {
    return `sqlserver://${encodeURIComponent(username)}:***@${host}:${port};database=${encodeURIComponent(database)}`;
  }
  if (connector === 'mongodb') {
    if (/^mongodb(\+srv)?:\/\//i.test(host)) return 'mongodb://*** (connection URI)';
    if (host && Number.isFinite(port) && database && username) {
      return `mongodb://${encodeURIComponent(username)}:***@${host}:${port}/${encodeURIComponent(database)}`;
    }
  }
  return '';
};

export const ensureCoreTables = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.tenants', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.tenants (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
      );
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.connection_profiles', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.connection_profiles (
        id INT IDENTITY(1,1) PRIMARY KEY,
        tenant_id INT NULL,
        owner_user_id INT NULL,
        visibility NVARCHAR(20) NOT NULL DEFAULT 'tenant_shared',
        connector_type NVARCHAR(40) NOT NULL,
        friendly_name NVARCHAR(255) NOT NULL,
        host NVARCHAR(2000) NOT NULL,
        port INT NOT NULL,
        database_name NVARCHAR(255) NOT NULL,
        username NVARCHAR(255) NOT NULL,
        password_value NVARCHAR(MAX) NOT NULL,
        profile_key NVARCHAR(64) NOT NULL UNIQUE,
        created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        updated_at DATETIME2 NULL
      );
      CREATE INDEX idx_connection_tenant_owner ON dbo.connection_profiles (tenant_id, owner_user_id, connector_type);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.connection_profiles', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.connection_profiles', 'visibility') IS NULL
    BEGIN
      ALTER TABLE dbo.connection_profiles ADD visibility NVARCHAR(20) NOT NULL CONSTRAINT DF_connection_profiles_visibility DEFAULT 'tenant_shared';
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.connection_profiles', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.connection_profiles', 'visibility') IS NOT NULL
    BEGIN
      UPDATE dbo.connection_profiles SET visibility = 'tenant_shared' WHERE visibility IS NULL;
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.connection_profiles', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.connection_profiles', 'tenant_id') IS NULL
    BEGIN
      ALTER TABLE dbo.connection_profiles ADD tenant_id INT NULL;
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.connection_profiles', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.connection_profiles', 'owner_user_id') IS NULL
    BEGIN
      ALTER TABLE dbo.connection_profiles ADD owner_user_id INT NULL;
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.LLM_Config', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.LLM_Config (
        provider NVARCHAR(40) PRIMARY KEY,
        model_name NVARCHAR(255) NOT NULL DEFAULT '',
        api_key NVARCHAR(MAX) NULL,
        base_url NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NULL
      );
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.api_builder_slug_routes', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.api_builder_slug_routes (
        id INT IDENTITY(1,1) PRIMARY KEY,
        tenant_id INT NOT NULL,
        slug NVARCHAR(200) NOT NULL,
        connection_id INT NOT NULL,
        table_name NVARCHAR(512) NOT NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT UQ_api_builder_slug UNIQUE (tenant_id, slug)
      );
      CREATE INDEX idx_api_builder_slug_lookup ON dbo.api_builder_slug_routes (tenant_id, slug);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.api_builder_slug_routes', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.api_builder_slug_routes', 'id_column') IS NULL
    BEGIN
      ALTER TABLE dbo.api_builder_slug_routes ADD id_column NVARCHAR(512) NULL;
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_public_api_tokens', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.user_public_api_tokens (
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        connection_id INT NOT NULL,
        jwt_token NVARCHAR(MAX) NOT NULL,
        expires_at BIGINT NOT NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_user_public_api_tokens PRIMARY KEY (tenant_id, owner_user_id, connection_id)
      );
      CREATE INDEX idx_user_public_api_tokens_lookup ON dbo.user_public_api_tokens (tenant_id, owner_user_id, connection_id);
    END
  `);
};

const getOrCreateDefaultTenantId = async () => {
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const hit = await pool.request().input('name', sql.NVarChar, 'default_tenant').query(`
      SELECT TOP 1 id FROM dbo.tenants WHERE name = @name
    `);
    if (hit.recordset?.length) return Number(hit.recordset[0].id) || null;
    const nextTenantIdRs = await pool.request().query(`SELECT ISNULL(MAX(id), 0) + 1 AS next_id FROM dbo.tenants`);
    const nextTenantId = Number(nextTenantIdRs.recordset?.[0]?.next_id || 1);
    const inserted = await pool
      .request()
      .input('id', sql.Int, nextTenantId)
      .input('name', sql.NVarChar, 'default_tenant')
      .query(`
        INSERT INTO dbo.tenants (id, name, created_at) VALUES (@id, @name, SYSDATETIME());
        SELECT @id AS id;
      `);
    return Number(inserted.recordset?.[0]?.id || 0) || null;
  } finally {
    await closeControlSqlServer(pool);
  }
};

const nextConnectionProfileId = async (pool) => {
  const rs = await pool.request().query(`SELECT ISNULL(MAX(id), 0) + 1 AS next_id FROM dbo.connection_profiles`);
  return Number(rs.recordset?.[0]?.next_id || 1);
};

/** Values that may appear in dbo.connection_profiles.connector_type for the canonical connector. */
const connectorTypeVariantsForFilter = (canonical) => {
  switch (canonical) {
    case 'postgresql':
      return ['postgresql', 'postgres', 'pgsql'];
    case 'mongodb':
      return ['mongodb', 'mongo'];
    case 'sqlserver':
      return ['sqlserver', 'mssql'];
    case 'mysql':
      return ['mysql', 'mariadb'];
    default:
      return [canonical];
  }
};

export const bootstrapControlDbTables = async () => {
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveConnectionProfile = async (req, res) => {
  const { host, port: dbPort, database, username, password, friendlyName = 'MySQL Connection', connectorType: rawConnector } = req.body ?? {};
  const ctx = req.context || {};
  let tenantId = ctx.tenantId ?? null;
  let ownerUserId = ctx.userId ?? null;
  const effectiveConnector = normalizeConnectorType(rawConnector) || 'mysql';
  let sanitizedHost;
  if (effectiveConnector === 'mongodb' && /^\s*mongodb(\+srv)?:\/\//i.test(String(host))) sanitizedHost = String(host).trim();
  else if (effectiveConnector === 'sqlserver') sanitizedHost = sqlServerHostForStorage(host);
  else sanitizedHost = normalizeHost(host);
  const defaultPort = CONNECTOR_DEFAULT_PORTS[effectiveConnector] ?? 3306;
  const portNum = Number(dbPort && String(dbPort).trim().length > 0 ? dbPort : defaultPort);
  if (!sanitizedHost || !Number.isFinite(portNum) || portNum <= 0 || !database || !username || !password) {
    return res.status(400).json({ ok: false, message: 'Missing required fields: host, port, database, username, password.' });
  }
  if (tenantId == null) {
    if (ctx.email || req.body?.email) {
      const resolved = await getOrCreateUserAndTenantByEmail(ctx.email || req.body?.email, ctx.fullName || req.body?.fullName || '');
      tenantId = resolved.tenantId;
      ownerUserId = resolved.userId;
    } else {
      tenantId = await getOrCreateDefaultTenantId();
      ownerUserId = null;
    }
  }

  const connectionString =
    effectiveConnector === 'mysql'
      ? buildMysqlConnectionUri({ host: sanitizedHost, port: portNum, database, username, password }, { includePassword: true })
      : buildPreviewConnectionUri({ connector_type: effectiveConnector, host: sanitizedHost, port: portNum, database_name: database, username });
  const profileKey = makeProfileKey(tenantId, effectiveConnector, sanitizedHost, portNum, database, username);

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const profileId = await nextConnectionProfileId(pool);
    await pool
      .request()
      .input('profileId', sql.Int, profileId)
      .input('profileKey', sql.NVarChar, profileKey)
      .input('tenantId', sql.Int, tenantId)
      .input('ownerUserId', sql.Int, ownerUserId)
      .input('visibility', sql.NVarChar, 'tenant_shared')
      .input('connectorType', sql.NVarChar, effectiveConnector)
      .input('friendlyName', sql.NVarChar, String(friendlyName))
      .input('host', sql.NVarChar(sql.MAX), sanitizedHost)
      .input('port', sql.Int, portNum)
      .input('databaseName', sql.NVarChar, String(database))
      .input('username', sql.NVarChar, String(username))
      .input('passwordValue', sql.NVarChar(sql.MAX), String(password)).query(`
        MERGE dbo.connection_profiles AS target
        USING (SELECT @profileKey AS profile_key) AS source
        ON target.profile_key = source.profile_key
        WHEN MATCHED THEN
          UPDATE SET
            tenant_id = ISNULL(target.tenant_id, @tenantId),
            owner_user_id = ISNULL(target.owner_user_id, @ownerUserId),
            visibility = ISNULL(target.visibility, 'tenant_shared'),
            friendly_name = @friendlyName,
            password_value = @passwordValue,
            updated_at = SYSDATETIME()
        WHEN NOT MATCHED THEN
          INSERT (id, tenant_id, owner_user_id, visibility, connector_type, friendly_name, host, port, database_name, username, password_value, profile_key, created_at, updated_at)
          VALUES (@profileId, @tenantId, @ownerUserId, @visibility, @connectorType, @friendlyName, @host, @port, @databaseName, @username, @passwordValue, @profileKey, SYSDATETIME(), SYSDATETIME());
      `);

    await logActivity({
      tenantId,
      userId: ownerUserId,
      entityType: 'connection',
      entityId: profileKey,
      action: 'connection.create',
      payload: { connectorType: effectiveConnector, friendlyName: String(friendlyName) },
      userAgent: req.headers['user-agent'],
      ipAddress: req.ip,
    });
    return res.json({ ok: true, message: 'Connection details saved successfully.', connectionString });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unexpected server error';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/**
 * Connection lists need both tenantId and ownerUserId. Middleware sometimes omits userId after errors;
 * profiles may also carry a mismatched tenant_id but correct owner_user_id.
 */
const resolveTenantAndOwnerForConnectionList = async (req) => {
  const ctx = req.context || {};
  let tenantId = ctx.tenantId != null ? Number(ctx.tenantId) : null;
  let ownerUserId = ctx.userId != null ? Number(ctx.userId) : null;
  const email = ctx.email ? String(ctx.email).trim().toLowerCase() : '';

  if (
    email &&
    (tenantId == null ||
      ownerUserId == null ||
      !Number.isFinite(tenantId) ||
      !Number.isFinite(ownerUserId))
  ) {
    try {
      const resolved = await getOrCreateUserAndTenantByEmail(email, ctx.fullName || '');
      if (tenantId == null || !Number.isFinite(tenantId)) {
        tenantId = resolved.tenantId != null ? Number(resolved.tenantId) : null;
      }
      if (ownerUserId == null || !Number.isFinite(ownerUserId)) {
        ownerUserId = resolved.userId != null ? Number(resolved.userId) : null;
      }
    } catch {
      /* keep partial ctx */
    }
  }

  if (tenantId == null || !Number.isFinite(tenantId)) {
    tenantId = await getOrCreateDefaultTenantId();
  }

  return {
    tenantId: tenantId != null && Number.isFinite(tenantId) ? tenantId : null,
    ownerUserId: ownerUserId != null && Number.isFinite(ownerUserId) ? ownerUserId : null,
  };
};

/** Workspace visibility: tenant bucket OR rows owned by this user (fixes tenant_id drift / NULL tenant). */
const sqlWorkspaceConnectionScopeWhere = (rq, tenantId, ownerUserId) => {
  rq.input('tenantId', sql.Int, tenantId);
  let fragment = `(tenant_id = @tenantId`;
  const oid = ownerUserId != null ? Number(ownerUserId) : NaN;
  if (Number.isFinite(oid)) {
    rq.input('ownerUserId', sql.Int, oid);
    fragment += ` OR owner_user_id = @ownerUserId`;
  }
  fragment += `)`;
  return fragment;
};

export const listConnectionProfiles = async (req, res) => {
  let pool;
  try {
    const { tenantId, ownerUserId } = await resolveTenantAndOwnerForConnectionList(req);
    if (tenantId == null) {
      return res.status(500).json({ ok: false, message: 'Unable to resolve workspace tenant.' });
    }
    const connectorFilter = normalizeConnectorType(req.query?.connector) || 'mysql';
    const typeVariants = connectorTypeVariantsForFilter(connectorFilter);
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);

    let sqlText = `
        SELECT id, friendly_name, host, port, database_name, username, connector_type, created_at, updated_at
        FROM dbo.connection_profiles
        WHERE connector_type IN (`;
    const rq = pool.request();
    sqlText += typeVariants.map((_, i) => `@ct${i}`).join(', ');
    typeVariants.forEach((v, i) => {
      rq.input(`ct${i}`, sql.NVarChar, v);
    });
    sqlText += `) AND ${sqlWorkspaceConnectionScopeWhere(rq, tenantId, ownerUserId)} ORDER BY id DESC`;

    const rs = await rq.query(sqlText);
    const connections = (rs.recordset || []).map((row) => {
      const canonical = normalizeConnectorType(row.connector_type) || connectorFilter;
      const rowForPreview = { ...row, connector_type: canonical };
      return {
        ...row,
        connector_type: canonical,
        is_default: canonical === 'sqlserver' && isDefaultControlProfileRow(rowForPreview),
        connection_string: buildPreviewConnectionUri(rowForPreview),
      };
    });
    return res.json({ ok: true, connections });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to fetch connections.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/**
 * All saved connection profiles for the workspace (every connector) in one round-trip.
 * Rows match workspace tenant OR owning user (handles legacy NULL tenant_id / tenant drift).
 */
export const listAllWorkspaceConnectionProfiles = async (req, res) => {
  let pool;
  try {
    const { tenantId, ownerUserId } = await resolveTenantAndOwnerForConnectionList(req);
    if (tenantId == null) {
      return res.status(500).json({ ok: false, message: 'Unable to resolve workspace tenant.' });
    }

    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);

    const rq = pool.request();
    const scopeWhere = sqlWorkspaceConnectionScopeWhere(rq, tenantId, ownerUserId);
    const sqlText = `
      SELECT id, friendly_name, host, port, database_name, username, connector_type, created_at, updated_at
      FROM dbo.connection_profiles
      WHERE ${scopeWhere}
      ORDER BY connector_type ASC, id DESC`;

    const rs = await rq.query(sqlText);
    const connections = (rs.recordset || []).map((row) => {
      const canonical = normalizeConnectorType(row.connector_type) || normalizeConnectorType(String(row.connector_type || '').replace(/-/g, ''));
      const connectorOut = canonical || String(row.connector_type || 'mysql').toLowerCase().trim() || 'mysql';
      const rowForPreview = { ...row, connector_type: connectorOut };
      return {
        id: row.id,
        friendly_name: row.friendly_name,
        host: row.host,
        port: row.port,
        database_name: row.database_name,
        username: row.username,
        connector_type: connectorOut,
        created_at: row.created_at,
        updated_at: row.updated_at,
        is_default: connectorOut === 'sqlserver' && isDefaultControlProfileRow(rowForPreview),
        connection_string: buildPreviewConnectionUri(rowForPreview),
      };
    });

    return res.json({ ok: true, connections });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to fetch connections.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const updateConnectionProfile = async (req, res) => {
  const { id } = req.params;
  const { friendlyName, host, port, database, username, password } = req.body ?? {};
  if (!id || !friendlyName || !host || !port || !database || !username || !password) {
    return res.status(400).json({ ok: false, message: 'Missing required fields for update.' });
  }
  let pool;
  try {
    const ctx = req.context || {};
    let tenantId = ctx.tenantId ?? null;
    if (tenantId == null) {
      if (ctx.email || req.body?.email) {
        const resolved = await getOrCreateUserAndTenantByEmail(ctx.email || req.body?.email, ctx.fullName || req.body?.fullName || '');
        tenantId = resolved.tenantId;
      } else {
        tenantId = await getOrCreateDefaultTenantId();
      }
    }
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const existing = await pool.request().input('id', sql.Int, Number(id)).query(`
      SELECT TOP 1 connector_type, tenant_id FROM dbo.connection_profiles WHERE id = @id
    `);
    if (!existing.recordset?.length) return res.status(404).json({ ok: false, message: 'Connection not found.' });
    const existingTenantId = existing.recordset[0].tenant_id;
    if (existingTenantId == null || Number(existingTenantId) !== Number(tenantId)) {
      return res.status(403).json({ ok: false, message: 'Connection does not belong to this tenant.' });
    }
    const effectiveConnector = String(existing.recordset[0].connector_type || 'mysql');
    let sanitizedHost;
    if (effectiveConnector === 'mongodb' && /^\s*mongodb(\+srv)?:\/\//i.test(String(host))) sanitizedHost = String(host).trim();
    else if (effectiveConnector === 'sqlserver') sanitizedHost = sqlServerHostForStorage(host);
    else sanitizedHost = normalizeHost(host);
    const defaultPort = CONNECTOR_DEFAULT_PORTS[effectiveConnector] ?? 3306;
    const portNum = Number(port && String(port).trim().length > 0 ? port : defaultPort);
    if (!Number.isFinite(portNum) || portNum <= 0) return res.status(400).json({ ok: false, message: 'Invalid port.' });
    const profileKey = makeProfileKey(tenantId, effectiveConnector, sanitizedHost, portNum, database, username);
    await pool
      .request()
      .input('id', sql.Int, Number(id))
      .input('friendlyName', sql.NVarChar, String(friendlyName))
      .input('host', sql.NVarChar(sql.MAX), sanitizedHost)
      .input('port', sql.Int, portNum)
      .input('databaseName', sql.NVarChar, String(database))
      .input('username', sql.NVarChar, String(username))
      .input('passwordValue', sql.NVarChar(sql.MAX), String(password))
      .input('profileKey', sql.NVarChar, profileKey).query(`
        UPDATE dbo.connection_profiles
        SET friendly_name = @friendlyName, host = @host, port = @port, database_name = @databaseName, username = @username,
            password_value = @passwordValue, profile_key = @profileKey, updated_at = SYSDATETIME()
        WHERE id = @id
      `);
    const connectionString =
      effectiveConnector === 'mysql'
        ? buildMysqlConnectionUri({ host: sanitizedHost, port: portNum, database, username, password }, { includePassword: true })
        : buildPreviewConnectionUri({ connector_type: effectiveConnector, host: sanitizedHost, port: portNum, database_name: database, username });
    await logActivity({
      tenantId,
      userId: ctx.userId ?? null,
      entityType: 'connection',
      entityId: String(id),
      action: 'connection.update',
      payload: { friendlyName: String(friendlyName), connectorType: effectiveConnector },
      userAgent: req.headers['user-agent'],
      ipAddress: req.ip,
    });
    return res.json({ ok: true, message: 'Connection updated successfully.', connectionString });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to update connection.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

const isSafeMysqlTableName = (name) => typeof name === 'string' && /^[a-zA-Z0-9_]+$/.test(name);

const formatCellForPreview = (value) => {
  if (value === null || value === undefined) return '';
  if (value instanceof Date) return value.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '');
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) return value.toString('hex');
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

const openMysqlTargetConnection = async (row) => {
  const sanitizedHost = normalizeHost(row.host);
  const directHost = hostFallbacks[sanitizedHost] || sanitizedHost;
  const portNum = Number(row.port) || 3306;
  return mysql.createConnection({
    host: directHost,
    port: portNum,
    database: String(row.database_name),
    user: String(row.username),
    password: String(row.password_value),
  });
};

export const getMySqlTableData = async (req, res) => {
  const { id } = req.params;
  const table = req.query.table;
  const limit = Math.min(Math.max(Number.isFinite(Number(req.query.limit)) ? Number(req.query.limit) : 50, 1), 5000);
  const offset = Math.max(Number.isFinite(Number(req.query.offset)) ? Number(req.query.offset) : 0, 0);
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
  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  if (!isSafeMysqlTableName(table)) return res.status(400).json({ ok: false, message: 'Invalid or missing table name.' });

  let pool;
  let targetConnection;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const profileRs = await pool.request().input('id', sql.Int, Number(id)).query(`
      SELECT TOP 1 host, port, database_name, username, password_value
      FROM dbo.connection_profiles
      WHERE id = @id AND connector_type = 'mysql'
    `);
    const profile = profileRs.recordset?.[0];
    if (!profile) return res.status(404).json({ ok: false, message: 'Connection not found.' });
    targetConnection = await openMysqlTargetConnection(profile);

    const [existsRows] = await targetConnection.query(
      `SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ? AND table_type = 'BASE TABLE' LIMIT 1`,
      [table]
    );
    if (!Array.isArray(existsRows) || existsRows.length === 0) {
      return res.status(404).json({ ok: false, message: 'Table not found in this database.' });
    }
    const [columnRows] = await targetConnection.query(
      `SELECT column_name, data_type, column_type, column_key, extra, column_default, column_comment
       FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`,
      [table]
    );
    const columns = (columnRows || []).map((c) => ({
      name: c.column_name,
      type: c.data_type,
      columnType: c.column_type,
      key: c.column_key || '',
      extra: c.extra || '',
      columnDefault: c.column_default == null ? '' : String(c.column_default),
      comment: c.column_comment == null ? '' : String(c.column_comment).trim(),
    }));
    const colNameSet = new Set(columns.map((c) => c.name));
    const STRING_TYPES = new Set([
      'char',
      'varchar',
      'tinytext',
      'text',
      'mediumtext',
      'longtext',
      'enum',
      'set',
      'json',
    ]);
    const stringCols = columns.filter((c) => STRING_TYPES.has(String(c.type || '').toLowerCase()));
    const whereParts = [];
    const whereParams = [];
    if (search && stringCols.length > 0) {
      const orParts = stringCols.map((c) => {
        whereParams.push(`%${search}%`);
        return `\`${c.name.replace(/`/g, '``')}\` LIKE ?`;
      });
      whereParts.push(`(${orParts.join(' OR ')})`);
    }
    for (const [col, val] of Object.entries(filters)) {
      if (val == null || String(val).trim() === '') continue;
      if (!colNameSet.has(col)) continue;
      whereParts.push(`\`${col.replace(/`/g, '``')}\` = ?`);
      whereParams.push(String(val));
    }
    const whereSql = whereParts.length > 0 ? ` WHERE ${whereParts.join(' AND ')}` : '';
    const [countRows] = await targetConnection.query(
      `SELECT COUNT(*) AS total FROM \`${table.replace(/`/g, '``')}\`${whereSql}`,
      whereParams
    );
    const total = Number(countRows?.[0]?.total ?? 0);
    const [dataRows] = await targetConnection.query(
      `SELECT * FROM \`${table.replace(/`/g, '``')}\`${whereSql} LIMIT ? OFFSET ?`,
      [...whereParams, limit, offset]
    );
    const rows = (dataRows || []).map((packet) =>
      columns.map((c) => {
        const raw = packet[c.name] ?? packet[c.name?.toLowerCase?.()] ?? packet[c.name?.toUpperCase?.()];
        return formatCellForPreview(raw);
      })
    );
    return res.json({ ok: true, columns, rows, total, limit, offset });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load table data.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (targetConnection) await targetConnection.end();
    await closeControlSqlServer(pool);
  }
};

export const listMySqlConnectionTables = async (req, res) => {
  const { id } = req.params;
  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  let pool;
  let targetConnection;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool.request().input('id', sql.Int, Number(id)).query(`
      SELECT TOP 1 host, port, database_name, username, password_value
      FROM dbo.connection_profiles
      WHERE id = @id AND connector_type = 'mysql'
    `);
    const row = rs.recordset?.[0];
    if (!row) return res.status(404).json({ ok: false, message: 'Connection not found.' });
    targetConnection = await openMysqlTargetConnection(row);
    const [tableRows] = await targetConnection.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' ORDER BY table_name ASC`
    );
    const tables = (tableRows || []).map((item) => item.table_name).filter(Boolean);
    return res.json({ ok: true, tables });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load tables.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (targetConnection) await targetConnection.end();
    await closeControlSqlServer(pool);
  }
};

export const deleteConnectionProfile = async (req, res) => {
  const { id } = req.params;
  if (!id) return res.status(400).json({ ok: false, message: 'Missing connection id.' });
  let pool;
  try {
    const ctx = req.context || {};
    let tenantId = ctx.tenantId ?? null;
    if (tenantId == null) {
      if (ctx.email || req.query?.email) {
        const resolved = await getOrCreateUserAndTenantByEmail(ctx.email || req.query?.email, ctx.fullName || '');
        tenantId = resolved.tenantId;
      } else {
        tenantId = await getOrCreateDefaultTenantId();
      }
    }
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool.request().input('id', sql.Int, Number(id)).query(`
      SELECT TOP 1 host, port, database_name, username, connector_type, tenant_id
      FROM dbo.connection_profiles
      WHERE id = @id
    `);
    const row = rs.recordset?.[0];
    if (!row) return res.status(404).json({ ok: false, message: 'Connection not found.' });
    if (row.tenant_id == null || Number(row.tenant_id) !== Number(tenantId)) {
      return res.status(403).json({ ok: false, message: 'Connection does not belong to this tenant.' });
    }
    if (row.connector_type === 'sqlserver' && isDefaultControlProfileRow(row)) {
      return res.status(400).json({ ok: false, message: 'The default application database cannot be deleted.' });
    }
    await pool.request().input('id', sql.Int, Number(id)).query(`DELETE FROM dbo.connection_profiles WHERE id = @id`);
    await logActivity({
      tenantId,
      userId: ctx.userId ?? null,
      entityType: 'connection',
      entityId: String(id),
      action: 'connection.delete',
      payload: { connectorType: row.connector_type, host: row.host },
      userAgent: req.headers['user-agent'],
      ipAddress: req.ip,
    });
    return res.json({ ok: true, message: 'Connection deleted successfully.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete connection.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getLlmConfigs = async (_req, res) => {
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool.request().query(`
      SELECT provider, model_name, api_key, base_url, updated_at
      FROM dbo.LLM_Config
      ORDER BY provider ASC
    `);
    return res.json({ ok: true, configs: rs.recordset || [] });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load LLM configurations.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveLlmConfig = async (req, res) => {
  const provider = String(req.body?.provider ?? '').trim().toLowerCase();
  const modelName = String(req.body?.modelName ?? '').trim();
  const apiKey = String(req.body?.apiKey ?? '').trim();
  const baseUrl = String(req.body?.baseUrl ?? '').trim();
  if (!provider) return res.status(400).json({ ok: false, message: 'provider is required.' });
  if (!modelName) return res.status(400).json({ ok: false, message: 'modelName is required.' });
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    await pool
      .request()
      .input('provider', sql.NVarChar, provider)
      .input('modelName', sql.NVarChar, modelName)
      .input('apiKey', sql.NVarChar(sql.MAX), apiKey || null)
      .input('baseUrl', sql.NVarChar(sql.MAX), baseUrl || null).query(`
        MERGE dbo.LLM_Config AS target
        USING (SELECT @provider AS provider) AS source
        ON target.provider = source.provider
        WHEN MATCHED THEN
          UPDATE SET model_name = @modelName, api_key = @apiKey, base_url = @baseUrl, updated_at = SYSDATETIME()
        WHEN NOT MATCHED THEN
          INSERT (provider, model_name, api_key, base_url, updated_at)
          VALUES (@provider, @modelName, @apiKey, @baseUrl, SYSDATETIME());
      `);
    return res.json({ ok: true, message: 'LLM configuration saved.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save LLM configuration.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/**
 * Latest issued datasource Bearer JWT for this signed-in user + connection (from control DB).
 */
export const getStoredPublicApiBearerToken = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email || ctx.tenantId == null || ctx.userId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in required.' });
  }
  const connectionId = Number(req.query?.connectionId);
  if (!Number.isFinite(connectionId) || connectionId <= 0) {
    return res.status(400).json({ ok: false, message: 'connectionId query parameter is required.' });
  }

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rsConn = await pool.request().input('id', sql.Int, connectionId).query(`
      SELECT TOP 1 id, tenant_id FROM dbo.connection_profiles WHERE id = @id
    `);
    const connRow = rsConn.recordset?.[0];
    if (!connRow) {
      return res.status(404).json({ ok: false, message: 'Connection id does not exist.' });
    }
    if (Number(connRow.tenant_id) !== Number(ctx.tenantId)) {
      return res.status(403).json({ ok: false, message: 'This connection belongs to another workspace.' });
    }

    const rsTok = await pool
      .request()
      .input('tenantId', sql.Int, Number(ctx.tenantId))
      .input('ownerUserId', sql.Int, Number(ctx.userId))
      .input('connectionId', sql.Int, connectionId)
      .query(`
        SELECT jwt_token, expires_at
        FROM dbo.user_public_api_tokens
        WHERE tenant_id = @tenantId AND owner_user_id = @ownerUserId AND connection_id = @connectionId
      `);
    const row = rsTok.recordset?.[0];
    if (!row) {
      return res.json({ ok: true, token: null, expiresAt: null, connectionId });
    }

    const jwtToken = String(row.jwt_token || '');
    const verified = verifyPublicApiJwt(jwtToken);
    const nowSec = Math.floor(Date.now() / 1000);
    const dbExp = Number(row.expires_at);

    if (!verified.ok || (Number.isFinite(dbExp) && dbExp < nowSec)) {
      return res.json({ ok: true, token: null, expiresAt: null, connectionId });
    }

    return res.json({
      ok: true,
      token: jwtToken,
      expiresAt: new Date(dbExp * 1000).toISOString(),
      connectionId,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load stored token.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/**
 * Issue a Bearer JWT scoped to the tenant + saved connection profile.
 * The token binds to `profile_key` (credential fingerprint in DB), not the raw password.
 */
export const issuePublicApiBearerToken = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email || ctx.tenantId == null || ctx.userId == null) {
    return res.status(401).json({
      ok: false,
      message: 'Sign in required. The API server needs x-user-email (active profile) to issue a datasource token.',
    });
  }
  const connectionId = Number(req.body?.connectionId);
  if (!Number.isFinite(connectionId) || connectionId <= 0) {
    return res.status(400).json({ ok: false, message: 'connectionId is required.' });
  }
  let expiresInHours = Number(req.body?.expiresInHours);
  if (!Number.isFinite(expiresInHours) || expiresInHours <= 0) expiresInHours = 24;
  expiresInHours = Math.min(168, Math.max(1, Math.floor(expiresInHours)));

  const secret = getPublicApiJwtSecret();

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool
      .request()
      .input('id', sql.Int, connectionId).query(`
        SELECT TOP 1 id, tenant_id, profile_key, database_name, friendly_name, connector_type, username, host, port
        FROM dbo.connection_profiles
        WHERE id = @id
      `);
    const row = rs.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Connection id does not exist.' });
    }
    if (row.tenant_id == null) {
      return res.status(403).json({
        ok: false,
        message:
          'This connection has no workspace tenant assigned. Re-save it under Data Sources, then try again.',
      });
    }
    if (Number(row.tenant_id) !== Number(ctx.tenantId)) {
      return res.status(403).json({
        ok: false,
        message:
          'This connection belongs to another workspace account. Click Refresh in API Builder (after signing in) so the connection list matches your profile, then try again.',
      });
    }
    const profileKey = String(row.profile_key || '');
    const fp = crypto.createHmac('sha256', secret).update(`pk:${profileKey}`).digest('hex').slice(0, 24);
    const nowSec = Math.floor(Date.now() / 1000);
    const exp = nowSec + expiresInHours * 3600;
    const jti = crypto.randomBytes(16).toString('hex');
    const payload = {
      sub: ctx.email,
      uid: ctx.userId,
      tid: ctx.tenantId,
      cid: connectionId,
      fp,
      db: String(row.database_name || ''),
      cn: String(row.connector_type || ''),
      fn: String(row.friendly_name || ''),
      iat: nowSec,
      exp,
      jti,
    };
    const token = signPublicApiJwt(payload, secret);

    await pool
      .request()
      .input('tenantId', sql.Int, Number(ctx.tenantId))
      .input('ownerUserId', sql.Int, Number(ctx.userId))
      .input('connectionId', sql.Int, connectionId)
      .input('jwtToken', sql.NVarChar(sql.MAX), token)
      .input('expiresAt', sql.BigInt, exp).query(`
        MERGE dbo.user_public_api_tokens AS target
        USING (
          SELECT @tenantId AS tenant_id, @ownerUserId AS owner_user_id, @connectionId AS connection_id
        ) AS src
        ON target.tenant_id = src.tenant_id
          AND target.owner_user_id = src.owner_user_id
          AND target.connection_id = src.connection_id
        WHEN MATCHED THEN
          UPDATE SET jwt_token = @jwtToken, expires_at = @expiresAt, updated_at = SYSDATETIME()
        WHEN NOT MATCHED THEN
          INSERT (tenant_id, owner_user_id, connection_id, jwt_token, expires_at, updated_at)
          VALUES (@tenantId, @ownerUserId, @connectionId, @jwtToken, @expiresAt, SYSDATETIME());
      `);

    return res.json({
      ok: true,
      token,
      expiresAt: new Date(exp * 1000).toISOString(),
      expiresInHours,
      connectionId,
      databaseName: payload.db,
      connectorType: payload.cn,
      friendlyName: payload.fn,
      note:
        'JWT is signed by the server and scoped to this datasource profile. It does not contain your database password. Set PUBLIC_API_JWT_SECRET in production.',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to issue token.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

