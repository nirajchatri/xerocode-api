import sql from 'mssql';
import { ensureTenantUserManagementTables } from '../tenantUserManagement.js';

export const MODULE_GRANT_PROJECT_ID = '*';

export const ALL_PROJECT_TYPES = ['application', 'agent', 'dashboard', 'automation', 'datasource', 'api'];

export async function resolveProjectGrantContext(pool, tenantId, userId) {
  await ensureTenantUserManagementTables(pool);
  const tid = Number(tenantId);
  const uid = Number(userId);

  const adminRs = await pool
    .request()
    .input('tid', sql.Int, tid)
    .input('uid', sql.Int, uid)
    .query(`
      SELECT TOP 1 is_tenant_admin
      FROM dbo.user_profile
      WHERE id = @uid AND tenant_id = @tid
    `);
  const isAdmin = Boolean(adminRs.recordset?.[0]?.is_tenant_admin);

  if (isAdmin) {
    return { isAdmin: true, moduleTypes: new Set(ALL_PROJECT_TYPES), projectsByType: new Map() };
  }

  const rs = await pool
    .request()
    .input('tid', sql.Int, tid)
    .input('uid', sql.Int, uid)
    .query(`
      SELECT project_type, project_id, access_level
      FROM dbo.user_project_grants
      WHERE tenant_id = @tid AND user_id = @uid
    `);

  const moduleTypes = new Set();
  const projectsByType = new Map();

  for (const row of rs.recordset || []) {
    const projectType = String(row.project_type || '').trim().toLowerCase();
    const projectId = String(row.project_id || '').trim();
    const accessLevel = String(row.access_level || '').trim().toLowerCase();
    if (!ALL_PROJECT_TYPES.includes(projectType)) continue;
    if (accessLevel !== 'view' && accessLevel !== 'edit') continue;
    if (projectId === MODULE_GRANT_PROJECT_ID) {
      moduleTypes.add(projectType);
    } else if (projectId) {
      if (!projectsByType.has(projectType)) projectsByType.set(projectType, new Set());
      projectsByType.get(projectType).add(projectId);
    }
  }

  return { isAdmin: false, moduleTypes, projectsByType };
}

export function hasFullModuleAccess(grantCtx, projectType) {
  return grantCtx.isAdmin || grantCtx.moduleTypes.has(projectType);
}

export function serializeProjectGrants(grantCtx) {
  const out = {};
  for (const type of ALL_PROJECT_TYPES) {
    out[type] = [...(grantCtx.projectsByType.get(type) || [])];
  }
  return out;
}

export function buildModulesAccessFromContext(grantCtx) {
  const modules = {};
  for (const type of ALL_PROJECT_TYPES) {
    modules[type] =
      hasFullModuleAccess(grantCtx, type) || Boolean(grantCtx.projectsByType.get(type)?.size);
  }
  return modules;
}

/** Map stored grant ids to ids used in workspace tables (handles bp:/ext: prefixes). */
export function tableIdsFromGrantIds(grantIds, kind = 'plain') {
  const out = new Set();
  for (const gid of grantIds) {
    const id = String(gid || '').trim();
    if (!id) continue;
    if (kind === 'blueprint') {
      if (id.startsWith('bp:')) out.add(id.slice(3));
      else if (!id.startsWith('ext:')) out.add(id);
    } else if (kind === 'external') {
      if (id.startsWith('ext:')) out.add(id.slice(4));
      else if (!id.startsWith('bp:')) out.add(id);
    } else {
      out.add(id);
    }
  }
  return [...out];
}

export function appendGrantIdInClause(rq, ids, paramPrefix = 'grantId') {
  const clean = [...new Set(ids.map((x) => String(x || '').trim()).filter(Boolean))];
  if (clean.length === 0) return null;
  const parts = [];
  clean.forEach((id, i) => {
    const key = `${paramPrefix}${i}`;
    rq.input(key, sql.NVarChar, id);
    parts.push(`@${key}`);
  });
  return parts.join(', ');
}

/**
 * Scope for saved workspace rows.
 * Only tenant admins see all rows; everyone else sees explicitly granted project ids only.
 * Module grants control welcome-screen module visibility, not full project lists.
 */
export function buildVisibilityResourceScope(grantCtx, projectType, rq, opts = {}) {
  const { tenantParam = 'tenantId', idColumn = 'id', idKind = 'plain' } = opts;

  if (grantCtx.isAdmin) {
    return `tenant_id = @${tenantParam}`;
  }

  const grantSet = grantCtx.projectsByType.get(projectType);
  const grantIds = grantSet ? tableIdsFromGrantIds([...grantSet], idKind) : [];
  const inClause = appendGrantIdInClause(rq, grantIds, `g_${projectType}_`);

  if (!inClause) {
    return '1 = 0';
  }

  return `tenant_id = @${tenantParam} AND ${idColumn} IN (${inClause})`;
}

/** Scope for connection_profiles (no visibility column). */
export function buildConnectionResourceScope(grantCtx, rq, tenantId) {
  rq.input('tenantId', sql.Int, Number(tenantId));

  if (grantCtx.isAdmin) {
    return 'tenant_id = @tenantId';
  }

  const grantSet = grantCtx.projectsByType.get('datasource');
  const grantIds = grantSet ? tableIdsFromGrantIds([...grantSet], 'plain') : [];
  const inClause = appendGrantIdInClause(rq, grantIds, 'g_ds_');
  if (!inClause) {
    return '1 = 0';
  }

  return `tenant_id = @tenantId AND id IN (${inClause})`;
}
