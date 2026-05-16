import { verifyPublicApiJwt } from '../lib/publicApiJwt.js';
import {
  RESERVED_BLUEPRINT_SLUGS,
  isBlueprintRecordKeyShape,
  isBlueprintSlugShape,
} from '../apiBuilderSlugRoutes.js';

/** Normalize path inside `/api` router (handles accidental `/api/api/...` or trailing slashes). */
const normalizedExpressRouterPath = (reqPath) => {
  let path = String(reqPath || '').split('?')[0];
  while (path.startsWith('/api/')) path = path.slice(4);
  if (!path.startsWith('/')) path = `/${path}`;
  path = path.replace(/\/+$/, '') || '/';
  return path;
};

/**
 * If `Authorization: Bearer <jwt>` verifies, attach workspace context from claims
 * (when session headers are absent) or attach `publicApiJwt` when session matches tenant.
 */
export const applyPublicApiJwtBearer = (req, _res, next) => {
  const auth = String(req.headers.authorization || '').trim();
  const m = /^Bearer\s+(\S+)/i.exec(auth);
  if (!m) return next();

  const raw = m[1];
  if (raw.split('.').length !== 3) return next();

  const v = verifyPublicApiJwt(raw);
  if (!v.ok) return next();

  const p = v.payload;
  const ctx = req.context || {};
  const hasEmailSession = Boolean(ctx.email && ctx.tenantId != null);

  if (hasEmailSession) {
    if (Number(ctx.tenantId) !== Number(p.tid)) {
      return next();
    }
    req.context = {
      ...ctx,
      publicApiJwt: p,
      jwtConnectionId: Number(p.cid),
      authViaPublicApiJwt: false,
    };
    return next();
  }

  req.context = {
    userId: p.uid ?? null,
    tenantId: p.tid ?? null,
    email: p.sub ?? null,
    fullName: p.sub ?? null,
    authViaPublicApiJwt: true,
    publicApiJwt: p,
    jwtConnectionId: Number(p.cid),
  };
  return next();
};

/** JWT-only clients may only hit explicit datasource/read routes (plus token refresh). */
export const restrictPublicApiJwtRoutes = (req, res, next) => {
  if (!req.context?.authViaPublicApiJwt) return next();

  const path = normalizedExpressRouterPath(req.path || '');
  const method = String(req.method || 'GET').toUpperCase();

  if (method === 'OPTIONS') return next();

  if (path === '/health') return next();
  if (path.startsWith('/public/')) return next();

  if (method === 'POST' && path === '/public-api-token/issue') return next();

  if (method === 'GET' && path === '/api-builder/published-routes') return next();

  if (
    method === 'GET' &&
    (path === '/connections/list' || path === '/connections/list-all')
  ) {
    return next();
  }

  if (
    method === 'GET' &&
    /^\/connections\/(\d+)\/(tables|table-data|table-foreign-keys|fk-lookup)$/.test(path)
  ) {
    return next();
  }
  if (method === 'POST' && /^\/connections\/(\d+)\/table-data\/mutate$/.test(path)) {
    return next();
  }
  if (method === 'POST' && /^\/connections\/(\d+)\/table-data\/master-detail-save$/.test(path)) {
    return next();
  }
  if (method === 'GET' && /^\/connections\/mysql\/(\d+)\/(tables|table-data)$/.test(path)) {
    return next();
  }

  const pathSegments = path.split('/').filter(Boolean);
  if (
    method === 'GET' &&
    pathSegments.length === 1 &&
    isBlueprintSlugShape(pathSegments[0]) &&
    !RESERVED_BLUEPRINT_SLUGS.has(pathSegments[0].toLowerCase())
  ) {
    return next();
  }
  if (
    method === 'GET' &&
    pathSegments.length === 2 &&
    isBlueprintSlugShape(pathSegments[0]) &&
    !RESERVED_BLUEPRINT_SLUGS.has(pathSegments[0].toLowerCase()) &&
    isBlueprintRecordKeyShape(pathSegments[1])
  ) {
    return next();
  }

  if (
    method === 'POST' &&
    pathSegments.length === 1 &&
    isBlueprintSlugShape(pathSegments[0]) &&
    !RESERVED_BLUEPRINT_SLUGS.has(pathSegments[0].toLowerCase())
  ) {
    return next();
  }

  return res.status(403).json({
    ok: false,
      message:
        'JWT-only mode allows GET …/api/connections/list, GET …/api/connections/list-all, GET …/api/connections/:id/tables, table-data, table-foreign-keys, fk-lookup, table-data/mutate, GET …/api/<published-blueprint-slug> or …/api/<published-blueprint-slug>/<id>, POST …/api/<published-blueprint-slug>, or POST …/api/public-api-token/issue.',
  });
};

/** When authenticated via JWT alone, :id must equal the token connection scope. */
export const enforcePublicApiJwtConnectionScope = (req, res, next) => {
  if (!req.context?.authViaPublicApiJwt) return next();
  const rid = Number(req.params.id);
  const cid = Number(req.context.jwtConnectionId);
  if (!Number.isFinite(rid) || !Number.isFinite(cid) || rid !== cid) {
    return res.status(403).json({
      ok: false,
      message: `Bearer token is scoped to connection id ${cid}. Requested id ${req.params.id}.`,
    });
  }
  return next();
};
