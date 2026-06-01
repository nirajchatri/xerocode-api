import crypto from 'crypto';
import sql from 'mssql';
import {
  buildModulesAccessFromContext,
  resolveProjectGrantContext,
  serializeProjectGrants,
} from './lib/projectGrantAccess.js';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';
import { ensureSqlServerAuthTables } from './controlDb/sqlserverAuth.js';
import { ensureSavedWorkspaceTables } from './controlDb/sqlserverAppData.js';
import { ensureCoreTables } from './controlDb/sqlserverConnections.js';
import { getSmtpConfig, sendWelcomeUserEmail } from './lib/smtpMail.js';
import {
  isPlatformSuperAdminSession,
} from './lib/platformSuperAdmin.js';

const ALL_MODULE_TYPES = ['application', 'agent', 'dashboard', 'automation', 'datasource', 'api'];

const PROJECT_TYPES = new Set(['application', 'agent', 'dashboard', 'automation', 'datasource', 'api']);
const MODULE_ACCESS_LEVELS = new Set(['view', 'edit']);
const PROJECT_ACCESS_LEVELS = new Set(['view', 'edit', 'none']);
export const MODULE_GRANT_PROJECT_ID = '*';

const hashPassword = (password) => {
  const salt = crypto.randomBytes(16).toString('hex');
  const digest = crypto.createHash('sha256').update(`${salt}:${String(password)}`, 'utf8').digest('hex');
  return `${salt}:${digest}`;
};

export const ensureTenantUserManagementTables = async (pool) => {
  await ensureSqlServerAuthTables(pool);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL AND COL_LENGTH('dbo.user_profile', 'is_tenant_admin') IS NULL
      ALTER TABLE dbo.user_profile ADD is_tenant_admin BIT NOT NULL DEFAULT 0;
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_project_grants', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.user_project_grants (
        id NVARCHAR(64) PRIMARY KEY,
        tenant_id INT NOT NULL,
        user_id INT NOT NULL,
        project_type NVARCHAR(32) NOT NULL,
        project_id NVARCHAR(255) NOT NULL,
        access_level NVARCHAR(16) NOT NULL,
        granted_by INT NULL,
        updated_at BIGINT NOT NULL,
        CONSTRAINT UQ_user_project_grants UNIQUE (tenant_id, user_id, project_type, project_id)
      );
      CREATE INDEX idx_user_project_grants_user ON dbo.user_project_grants (tenant_id, user_id);
    END
  `);
};

const nextUserId = async (pool) => {
  const result = await pool.request().query(`SELECT ISNULL(MAX(id), 0) + 1 AS next_id FROM dbo.user_profile`);
  return Number(result.recordset?.[0]?.next_id || 1);
};

async function ensureTenantHasAdmin(pool, tenantId, userId) {
  if (!tenantId || !userId) return;
  const rs = await pool
    .request()
    .input('tid', sql.Int, Number(tenantId))
    .query(`
      SELECT COUNT(*) AS c FROM dbo.user_profile
      WHERE tenant_id = @tid AND is_tenant_admin = 1
    `);
  if (Number(rs.recordset?.[0]?.c || 0) > 0) return;
  await pool
    .request()
    .input('uid', sql.Int, Number(userId))
    .query(`UPDATE dbo.user_profile SET is_tenant_admin = 1 WHERE id = @uid`);
}

async function isTenantAdmin(pool, tenantId, userId) {
  const rs = await pool
    .request()
    .input('tid', sql.Int, Number(tenantId))
    .input('uid', sql.Int, Number(userId))
    .query(`
      SELECT TOP 1 is_tenant_admin FROM dbo.user_profile
      WHERE id = @uid AND tenant_id = @tid
    `);
  return Boolean(rs.recordset?.[0]?.is_tenant_admin);
}

async function requireTenantAdminContext(req, res) {
  const ctx = req.context || {};
  if (!ctx.email || ctx.tenantId == null || ctx.userId == null) {
    res.status(401).json({ ok: false, message: 'Sign in required for User Management.' });
    return null;
  }
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureTenantUserManagementTables(pool);
    await ensureTenantHasAdmin(pool, ctx.tenantId, ctx.userId);
    const admin = await isTenantAdmin(pool, ctx.tenantId, ctx.userId);
    if (!admin) {
      res.status(403).json({ ok: false, message: 'Only tenant admins can manage users and project access.' });
      return null;
    }
    const isPlatformSuperAdmin = isPlatformSuperAdminSession(ctx.email, admin);
    return { ctx: { ...ctx, isPlatformSuperAdmin }, pool };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to verify admin access.';
    res.status(500).json({ ok: false, message });
    if (pool) await closeControlSqlServer(pool);
    return null;
  }
}

/** Resolve target user's tenant for admin APIs (cross-tenant when platform super-admin). */
async function resolveAdminTargetMember(pool, ctx, targetUserId) {
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) return null;
  if (ctx.isPlatformSuperAdmin) {
    const rs = await pool.request().input('uid', sql.Int, targetUserId).query(`
      SELECT TOP 1 id, tenant_id FROM dbo.user_profile WHERE id = @uid
    `);
    const row = rs.recordset?.[0];
    if (!row) return null;
    return { tenantId: Number(row.tenant_id), userId: targetUserId };
  }
  const rs = await pool
    .request()
    .input('tid', sql.Int, Number(ctx.tenantId))
    .input('uid', sql.Int, targetUserId)
    .query(`SELECT TOP 1 id, tenant_id FROM dbo.user_profile WHERE tenant_id = @tid AND id = @uid`);
  const row = rs.recordset?.[0];
  if (!row) return null;
  return { tenantId: Number(ctx.tenantId), userId: targetUserId };
}

function mapUserRow(row) {
  return {
    id: Number(row.id),
    fullName: String(row.full_name || ''),
    email: String(row.email || ''),
    phone: String(row.phone || ''),
    isTenantAdmin: Boolean(row.is_tenant_admin),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    tenantId: row.tenant_id != null ? Number(row.tenant_id) : undefined,
    tenantName: row.tenant_name != null ? String(row.tenant_name) : undefined,
  };
}

/** GET /api/tenant/users */
export const listTenantUsers = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  try {
    const result = ctx.isPlatformSuperAdmin
      ? await pool.request().query(`
          SELECT u.id, u.full_name, u.email, u.phone, u.is_tenant_admin, u.created_at, u.updated_at,
                 u.tenant_id, t.name AS tenant_name
          FROM dbo.user_profile u
          LEFT JOIN dbo.tenants t ON t.id = u.tenant_id
          ORDER BY t.name ASC, u.is_tenant_admin DESC, u.full_name ASC
        `)
      : await pool
          .request()
          .input('tid', sql.Int, Number(ctx.tenantId))
          .query(`
            SELECT id, full_name, email, phone, is_tenant_admin, created_at, updated_at, tenant_id
            FROM dbo.user_profile
            WHERE tenant_id = @tid
            ORDER BY is_tenant_admin DESC, full_name ASC
          `);
    const users = (result.recordset || []).map((row) => mapUserRow(row));
    return res.json({
      ok: true,
      users,
      scopeAllTenants: Boolean(ctx.isPlatformSuperAdmin),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list tenant users.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** POST /api/tenant/users — create user in admin's tenant */
export const createTenantUser = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  const fullName = String(req.body?.fullName ?? req.body?.username ?? '').trim();
  const email = String(req.body?.email ?? '').trim().toLowerCase();
  const phone = String(req.body?.phone ?? req.body?.mobile ?? '').trim();
  const password = String(req.body?.password ?? '');
  const roleRaw = String(req.body?.role ?? '').trim().toLowerCase();
  const isTenantAdminFlag =
    roleRaw === 'admin' || req.body?.isTenantAdmin === true || req.body?.isTenantAdmin === 1 ? 1 : 0;
  if (!fullName) return res.status(400).json({ ok: false, message: 'Full name is required.' });
  if (!email) return res.status(400).json({ ok: false, message: 'Email is required.' });
  if (password.length < 6) {
    return res.status(400).json({ ok: false, message: 'Password must be at least 6 characters.' });
  }
  try {
    const existing = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id, tenant_id FROM dbo.user_profile WHERE email = @email
    `);
    const hit = existing.recordset?.[0];
    if (hit) {
      if (Number(hit.tenant_id) === Number(ctx.tenantId)) {
        return res.status(409).json({ ok: false, message: 'User already exists in this workspace.' });
      }
      return res.status(409).json({ ok: false, message: 'Email is already registered in another workspace.' });
    }
    const id = await nextUserId(pool);
    const passwordHash = hashPassword(password);
    await pool
      .request()
      .input('id', sql.Int, id)
      .input('tid', sql.Int, Number(ctx.tenantId))
      .input('fullName', sql.NVarChar, fullName)
      .input('email', sql.NVarChar, email)
      .input('phone', sql.NVarChar, phone || null)
      .input('passwordHash', sql.NVarChar, passwordHash)
      .input('isAdmin', sql.Bit, isTenantAdminFlag)
      .query(`
        INSERT INTO dbo.user_profile (id, tenant_id, full_name, email, phone, password_hash, is_tenant_admin, created_at, updated_at)
        VALUES (@id, @tid, @fullName, @email, @phone, @passwordHash, @isAdmin, SYSDATETIME(), SYSDATETIME())
      `);

    let welcomeEmailSent = false;
    let welcomeEmailError = null;
    const smtp = getSmtpConfig();
    if (smtp.isConfigured) {
      try {
        await sendWelcomeUserEmail({
          toEmail: email,
          fullName,
          email,
          password,
          invitedByName: ctx.fullName || ctx.email,
          smtp,
        });
        welcomeEmailSent = true;
      } catch (mailErr) {
        welcomeEmailError = mailErr instanceof Error ? mailErr.message : 'Unable to send welcome email.';
        console.error('Welcome email failed for new tenant user:', welcomeEmailError);
      }
    } else {
      welcomeEmailError = 'SMTP is not configured (set SMTP_PASS for Gmail). Welcome email was skipped.';
    }

    return res.json({
      ok: true,
      user: { id, fullName, email, phone, isTenantAdmin: Boolean(isTenantAdminFlag) },
      welcomeEmailSent,
      ...(welcomeEmailError ? { welcomeEmailError } : {}),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to create user.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

async function loadUserGrantAccessByType(pool, tenantId, userId) {
  const rs = await pool
    .request()
    .input('tid', sql.Int, Number(tenantId))
    .input('uid', sql.Int, Number(userId))
    .query(`
      SELECT project_type, project_id, access_level
      FROM dbo.user_project_grants
      WHERE tenant_id = @tid AND user_id = @uid
    `);
  const byType = {};
  for (const row of rs.recordset || []) {
    const projectType = String(row.project_type || '').trim().toLowerCase();
    const accessLevel = String(row.access_level || '').trim().toLowerCase();
    if (!PROJECT_TYPES.has(projectType)) continue;
    if (!byType[projectType]) byType[projectType] = { module: false, any: false };
    if (String(row.project_id) === MODULE_GRANT_PROJECT_ID) {
      byType[projectType].module = true;
    } else if (accessLevel === 'view' || accessLevel === 'edit') {
      byType[projectType].any = true;
    }
    // access_level 'none' is an explicit project deny — does not enable module access
  }
  return byType;
}

function buildModulesAccessMap(isAdmin, grantsByType) {
  const modules = {};
  for (const projectType of ALL_MODULE_TYPES) {
    modules[projectType] = isAdmin || Boolean(grantsByType[projectType]?.module || grantsByType[projectType]?.any);
  }
  return modules;
}

/** GET /api/tenant/projects — all projects in tenant for assignment */
export const listTenantProjects = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  try {
    await ensureSavedWorkspaceTables(pool);
    await ensureCoreTables(pool);
    let tid = Number(ctx.tenantId);
    if (ctx.isPlatformSuperAdmin) {
      const qtid = Number(req.query?.tenantId);
      if (Number.isFinite(qtid) && qtid > 0) tid = qtid;
    }
    const [apps, agents, dashboards, automations, datasources, blueprintApis, externalApis] = await Promise.all([
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, name, owner_user_id, updated_at FROM dbo.saved_apps WHERE tenant_id = @tid ORDER BY name
      `),
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, name, owner_user_id, updated_at FROM dbo.saved_studio_agents WHERE tenant_id = @tid ORDER BY name
      `),
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, name, owner_user_id, updated_at FROM dbo.saved_dashboards WHERE tenant_id = @tid ORDER BY name
      `),
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, name, owner_user_id, updated_at FROM dbo.saved_automation_projects WHERE tenant_id = @tid ORDER BY name
      `),
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, friendly_name AS name, owner_user_id, connector_type,
          COALESCE(DATEDIFF_BIG(ms, '1970-01-01', updated_at), 0) AS updated_at
        FROM dbo.connection_profiles
        WHERE tenant_id = @tid
        ORDER BY friendly_name
      `),
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, name, owner_user_id, updated_at FROM dbo.saved_blueprint_apis WHERE tenant_id = @tid ORDER BY name
      `),
      pool.request().input('tid', sql.Int, tid).query(`
        SELECT id, name, owner_user_id, updated_at FROM dbo.saved_external_apis WHERE tenant_id = @tid ORDER BY name
      `),
    ]);
    const mapRow = (row, projectType, idPrefix = '') => ({
      id: `${idPrefix}${String(row.id)}`,
      name: String(row.name || row.friendly_name || 'Untitled'),
      projectType,
      ownerUserId: row.owner_user_id != null ? Number(row.owner_user_id) : null,
      updatedAt: Number(row.updated_at) || 0,
      subtitle: row.connector_type ? String(row.connector_type) : undefined,
    });
    const apis = [
      ...(blueprintApis.recordset || []).map((r) => mapRow(r, 'api', 'bp:')),
      ...(externalApis.recordset || []).map((r) => mapRow(r, 'api', 'ext:')),
    ].sort((a, b) => a.name.localeCompare(b.name));
    return res.json({
      ok: true,
      projects: {
        applications: (apps.recordset || []).map((r) => mapRow(r, 'application')),
        agents: (agents.recordset || []).map((r) => mapRow(r, 'agent')),
        dashboards: (dashboards.recordset || []).map((r) => mapRow(r, 'dashboard')),
        automations: (automations.recordset || []).map((r) => mapRow(r, 'automation')),
        datasources: (datasources.recordset || []).map((r) => mapRow(r, 'datasource')),
        apis,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list tenant projects.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** GET /api/tenant/users/:userId — full user_profile row for workspace member */
export const getTenantUser = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  const targetUserId = Number(req.params?.userId);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
    await closeControlSqlServer(pool);
    return res.status(400).json({ ok: false, message: 'Invalid user id.' });
  }
  try {
    const memberScope = await resolveAdminTargetMember(pool, ctx, targetUserId);
    if (!memberScope) {
      return res.status(404).json({ ok: false, message: 'User not found.' });
    }
    const result = ctx.isPlatformSuperAdmin
      ? await pool.request().input('uid', sql.Int, targetUserId).query(`
          SELECT u.id, u.full_name, u.email, u.phone, u.company, u.role_title, u.bio, u.avatar_url,
                 u.is_tenant_admin, u.created_at, u.updated_at, u.tenant_id, t.name AS tenant_name
          FROM dbo.user_profile u
          LEFT JOIN dbo.tenants t ON t.id = u.tenant_id
          WHERE u.id = @uid
        `)
      : await pool
          .request()
          .input('tid', sql.Int, memberScope.tenantId)
          .input('uid', sql.Int, targetUserId)
          .query(`
            SELECT id, full_name, email, phone, company, role_title, bio, avatar_url,
                   is_tenant_admin, created_at, updated_at, tenant_id
            FROM dbo.user_profile
            WHERE tenant_id = @tid AND id = @uid
          `);
    const row = result.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'User not found in this workspace.' });
    }
    return res.json({
      ok: true,
      user: {
        id: Number(row.id),
        fullName: String(row.full_name || ''),
        email: String(row.email || ''),
        phone: String(row.phone || ''),
        company: String(row.company || ''),
        roleTitle: String(row.role_title || ''),
        bio: String(row.bio || ''),
        avatarUrl: String(row.avatar_url || ''),
        isTenantAdmin: Boolean(row.is_tenant_admin),
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        tenantId: row.tenant_id != null ? Number(row.tenant_id) : memberScope.tenantId,
        tenantName: row.tenant_name != null ? String(row.tenant_name) : undefined,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load user profile.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** PUT /api/tenant/users/:userId — update workspace member profile */
export const updateTenantUser = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  const targetUserId = Number(req.params?.userId);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
    await closeControlSqlServer(pool);
    return res.status(400).json({ ok: false, message: 'Invalid user id.' });
  }
  const fullName = String(req.body?.fullName ?? req.body?.username ?? '').trim();
  const email = String(req.body?.email ?? '').trim().toLowerCase();
  const phone = String(req.body?.phone ?? req.body?.mobile ?? '').trim();
  const password = String(req.body?.password ?? '');
  const roleRaw = String(req.body?.role ?? '').trim().toLowerCase();
  const roleProvided = req.body?.role != null || req.body?.isTenantAdmin != null;
  if (!fullName) return res.status(400).json({ ok: false, message: 'Full name is required.' });
  if (!email) return res.status(400).json({ ok: false, message: 'Email is required.' });
  if (password && password.length < 6) {
    return res.status(400).json({ ok: false, message: 'Password must be at least 6 characters.' });
  }
  try {
    const memberScope = await resolveAdminTargetMember(pool, ctx, targetUserId);
    if (!memberScope) {
      return res.status(404).json({ ok: false, message: 'User not found.' });
    }
    const targetTid = memberScope.tenantId;
    const member = await pool
      .request()
      .input('tid', sql.Int, targetTid)
      .input('uid', sql.Int, targetUserId)
      .query(`
        SELECT TOP 1 id, email, is_tenant_admin FROM dbo.user_profile
        WHERE tenant_id = @tid AND id = @uid
      `);
    const row = member.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'User not found in this workspace.' });
    }
    if (email !== String(row.email || '').toLowerCase()) {
      const existing = await pool.request().input('email', sql.NVarChar, email).query(`
        SELECT TOP 1 id, tenant_id FROM dbo.user_profile WHERE email = @email
      `);
      const hit = existing.recordset?.[0];
      if (hit && Number(hit.id) !== targetUserId) {
        return res.status(409).json({ ok: false, message: 'Email is already registered.' });
      }
    }
    let isTenantAdminFlag = Boolean(row.is_tenant_admin);
    if (roleProvided) {
      isTenantAdminFlag =
        roleRaw === 'admin' || req.body?.isTenantAdmin === true || req.body?.isTenantAdmin === 1;
    }
    if (Boolean(row.is_tenant_admin) && !isTenantAdminFlag) {
      const adminCountRs = await pool.request().input('tid', sql.Int, targetTid).query(`
        SELECT COUNT(*) AS c FROM dbo.user_profile
        WHERE tenant_id = @tid AND is_tenant_admin = 1
      `);
      if (Number(adminCountRs.recordset?.[0]?.c || 0) <= 1) {
        return res.status(400).json({ ok: false, message: 'Cannot demote the last workspace admin.' });
      }
    }
    const reqUpdate = pool
      .request()
      .input('uid', sql.Int, targetUserId)
      .input('tid', sql.Int, targetTid)
      .input('fullName', sql.NVarChar, fullName)
      .input('email', sql.NVarChar, email)
      .input('phone', sql.NVarChar, phone || null)
      .input('isAdmin', sql.Bit, isTenantAdminFlag ? 1 : 0);
    if (password) {
      const passwordHash = hashPassword(password);
      await reqUpdate
        .input('passwordHash', sql.NVarChar, passwordHash)
        .query(`
          UPDATE dbo.user_profile
          SET full_name = @fullName, email = @email, phone = @phone, password_hash = @passwordHash,
              is_tenant_admin = @isAdmin, updated_at = SYSDATETIME()
          WHERE id = @uid AND tenant_id = @tid
        `);
    } else {
      await reqUpdate.query(`
        UPDATE dbo.user_profile
        SET full_name = @fullName, email = @email, phone = @phone, is_tenant_admin = @isAdmin, updated_at = SYSDATETIME()
        WHERE id = @uid AND tenant_id = @tid
      `);
    }
    return res.json({
      ok: true,
      user: {
        id: targetUserId,
        fullName,
        email,
        phone,
        isTenantAdmin: isTenantAdminFlag,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to update user.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** DELETE /api/tenant/users/:userId — remove user from workspace */
export const deleteTenantUser = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  const targetUserId = Number(req.params?.userId);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
    await closeControlSqlServer(pool);
    return res.status(400).json({ ok: false, message: 'Invalid user id.' });
  }
  if (Number(ctx.userId) === targetUserId) {
    await closeControlSqlServer(pool);
    return res.status(400).json({ ok: false, message: 'You cannot delete your own account.' });
  }
  try {
    const memberScope = await resolveAdminTargetMember(pool, ctx, targetUserId);
    if (!memberScope) {
      return res.status(404).json({ ok: false, message: 'User not found.' });
    }
    const targetTid = memberScope.tenantId;
    const member = await pool
      .request()
      .input('tid', sql.Int, targetTid)
      .input('uid', sql.Int, targetUserId)
      .query(`
        SELECT TOP 1 id, is_tenant_admin FROM dbo.user_profile
        WHERE tenant_id = @tid AND id = @uid
      `);
    const row = member.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'User not found in this workspace.' });
    }
    if (Boolean(row.is_tenant_admin)) {
      return res.status(400).json({ ok: false, message: 'Tenant admin accounts cannot be deleted.' });
    }
    await pool
      .request()
      .input('tid', sql.Int, targetTid)
      .input('uid', sql.Int, targetUserId)
      .query(`DELETE FROM dbo.user_project_grants WHERE tenant_id = @tid AND user_id = @uid`);
    await pool
      .request()
      .input('tid', sql.Int, targetTid)
      .input('uid', sql.Int, targetUserId)
      .query(`DELETE FROM dbo.user_profile WHERE tenant_id = @tid AND id = @uid`);
    return res.json({ ok: true, deleted: targetUserId });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete user.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** GET /api/tenant/users/:userId/grants */
export const getTenantUserGrants = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  const targetUserId = Number(req.params?.userId);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
    await closeControlSqlServer(pool);
    return res.status(400).json({ ok: false, message: 'Invalid user id.' });
  }
  try {
    const memberScope = await resolveAdminTargetMember(pool, ctx, targetUserId);
    if (!memberScope) {
      return res.status(404).json({ ok: false, message: 'User not found.' });
    }
    const targetTid = memberScope.tenantId;
    const rs = await pool
      .request()
      .input('tid', sql.Int, targetTid)
      .input('uid', sql.Int, targetUserId)
      .query(`
        SELECT project_type, project_id, access_level, updated_at
        FROM dbo.user_project_grants
        WHERE tenant_id = @tid AND user_id = @uid
      `);
    const grants = (rs.recordset || []).map((row) => ({
      projectType: String(row.project_type),
      projectId: String(row.project_id),
      accessLevel: String(row.access_level),
      isModuleGrant: String(row.project_id) === MODULE_GRANT_PROJECT_ID,
      updatedAt: Number(row.updated_at) || 0,
    }));
    const moduleGrants = grants.filter((g) => g.isModuleGrant);
    const projectGrants = grants.filter((g) => !g.isModuleGrant);
    return res.json({ ok: true, userId: targetUserId, grants, moduleGrants, projectGrants });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load project grants.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** PUT /api/tenant/users/:userId/grants — body: { grants: [{ projectType, projectId, accessLevel }] } */
export const saveTenantUserGrants = async (req, res) => {
  const gate = await requireTenantAdminContext(req, res);
  if (!gate) return;
  const { ctx, pool } = gate;
  const targetUserId = Number(req.params?.userId);
  if (!Number.isFinite(targetUserId) || targetUserId <= 0) {
    await closeControlSqlServer(pool);
    return res.status(400).json({ ok: false, message: 'Invalid user id.' });
  }
  const rawGrants = Array.isArray(req.body?.grants) ? req.body.grants : [];
  const rawModuleGrants = Array.isArray(req.body?.moduleGrants) ? req.body.moduleGrants : [];
  const normalized = [];
  for (const g of rawGrants) {
    const projectType = String(g?.projectType || g?.project_type || '').trim().toLowerCase();
    const projectId = String(g?.projectId || g?.project_id || '').trim();
    const accessLevel = String(g?.accessLevel || g?.access_level || '').trim().toLowerCase();
    if (!PROJECT_TYPES.has(projectType) || !projectId || projectId === MODULE_GRANT_PROJECT_ID) continue;
    if (!PROJECT_ACCESS_LEVELS.has(accessLevel)) continue;
    normalized.push({ projectType, projectId, accessLevel });
  }
  for (const g of rawModuleGrants) {
    const projectType = String(g?.projectType || g?.project_type || '').trim().toLowerCase();
    const accessLevel = String(g?.accessLevel || g?.access_level || '').trim().toLowerCase();
    const enabled = g?.enabled !== false && g?.enabled !== 0;
    if (!PROJECT_TYPES.has(projectType) || !MODULE_ACCESS_LEVELS.has(accessLevel) || !enabled) continue;
    normalized.push({ projectType, projectId: MODULE_GRANT_PROJECT_ID, accessLevel });
  }
  try {
    const memberScope = await resolveAdminTargetMember(pool, ctx, targetUserId);
    if (!memberScope) {
      return res.status(404).json({ ok: false, message: 'User not found.' });
    }
    const targetTid = memberScope.tenantId;
    await pool
      .request()
      .input('tid', sql.Int, targetTid)
      .input('uid', sql.Int, targetUserId)
      .query(`DELETE FROM dbo.user_project_grants WHERE tenant_id = @tid AND user_id = @uid`);

    const now = Date.now();
    for (const g of normalized) {
      const grantId = crypto
        .createHash('sha256')
        .update(`${targetUserId}:${g.projectType}:${g.projectId}`)
        .digest('hex')
        .slice(0, 64);
      await pool
        .request()
        .input('id', sql.NVarChar, grantId)
        .input('tid', sql.Int, targetTid)
        .input('uid', sql.Int, targetUserId)
        .input('ptype', sql.NVarChar, g.projectType)
        .input('pid', sql.NVarChar, g.projectId)
        .input('level', sql.NVarChar, g.accessLevel)
        .input('by', sql.Int, Number(ctx.userId))
        .input('updated', sql.BigInt, now)
        .query(`
          INSERT INTO dbo.user_project_grants (id, tenant_id, user_id, project_type, project_id, access_level, granted_by, updated_at)
          VALUES (@id, @tid, @uid, @ptype, @pid, @level, @by, @updated)
        `);
    }
    return res.json({ ok: true, saved: normalized.length });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save project grants.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** GET /api/tenant/my-access — modules the signed-in user may view on welcome dashboard */
export const getTenantMyAccess = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email || ctx.tenantId == null || ctx.userId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in required.' });
  }
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureTenantUserManagementTables(pool);
    await ensureTenantHasAdmin(pool, ctx.tenantId, ctx.userId);
    const admin = await isTenantAdmin(pool, ctx.tenantId, ctx.userId);
    if (admin) {
      const modules = buildModulesAccessMap(true, {});
      return res.json({
        ok: true,
        userId: ctx.userId,
        tenantId: ctx.tenantId,
        isTenantAdmin: true,
        modules,
        projectGrants: {},
      });
    }
    const grantCtx = await resolveProjectGrantContext(pool, ctx.tenantId, ctx.userId);
    const modules = buildModulesAccessFromContext(grantCtx);
    const projectGrants = serializeProjectGrants(grantCtx);
    return res.json({
      ok: true,
      userId: ctx.userId,
      tenantId: ctx.tenantId,
      isTenantAdmin: false,
      modules,
      projectGrants,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load module access.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** GET /api/tenant/me — current user admin flag (for showing User Management) */
export const getTenantMe = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email || ctx.tenantId == null || ctx.userId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in required.' });
  }
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureTenantUserManagementTables(pool);
    await ensureTenantHasAdmin(pool, ctx.tenantId, ctx.userId);
    const admin = await isTenantAdmin(pool, ctx.tenantId, ctx.userId);
    const isPlatformSuperAdmin = isPlatformSuperAdminSession(ctx.email, admin);
    return res.json({
      ok: true,
      userId: ctx.userId,
      tenantId: ctx.tenantId,
      isTenantAdmin: admin,
      isPlatformSuperAdmin,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load tenant profile.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
