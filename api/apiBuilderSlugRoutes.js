import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';
import { ensureCoreTables } from './controlDb/sqlserverConnections.js';
import { getConnectionTableData, mutateConnectionTableData } from './connections/schemaRoutes.js';

/** Path segments reserved so GET /:slug does not shadow core APIs */
export const RESERVED_BLUEPRINT_SLUGS = new Set([
  'health',
  'profile',
  'apps',
  'dashboards',
  'connections',
  'public',
  'auth',
  'llm-config',
  'public-api-token',
  'api-builder',
  'signup',
  'login',
  'google',
  'github',
  'mysql',
  'save',
  'test',
  'list',
  'forgot-password',
]);

export const isBlueprintSlugShape = (s) => /^[a-z0-9][a-z0-9-]*$/i.test(String(s || '').trim());

/** Single path segment after /api/<slug>/ — primary-key value (UUID, int as string, etc.). */
export const isBlueprintRecordKeyShape = (s) => {
  const t = String(s ?? '').trim();
  return t.length > 0 && t.length <= 512 && !/[/#?\\]/.test(t);
};

/** Safe SQL identifier for column names supplied by API Builder sync (path param name). */
export const isSafeSqlIdentifier = (s) => /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(String(s || '').trim());

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

/**
 * Body from API Builder Run / clients: either explicit mutate shape or a flat field map (create).
 */
export const buildBlueprintMutateBodyFromPost = (table, rawBody) => {
  if (!isPlainObject(rawBody)) return null;
  const action = String(rawBody.action || '').toLowerCase();
  const explicit =
    ['create', 'update', 'delete'].includes(action) && ('values' in rawBody || 'rowMatch' in rawBody);
  if (explicit) {
    return {
      table,
      action,
      values: isPlainObject(rawBody.values) ? rawBody.values : {},
      rowMatch: isPlainObject(rawBody.rowMatch) ? rawBody.rowMatch : {},
    };
  }
  const reserved = new Set(['action', 'values', 'rowMatch', 'table']);
  const values = {};
  for (const [k, v] of Object.entries(rawBody)) {
    if (!reserved.has(k)) values[k] = v;
  }
  return { table, action: 'create', values, rowMatch: {} };
};

/**
 * Publish slug→table mappings for GET /api/<slug> (and POST when mutation is enabled in the blueprint).
 * Body: { connectionId: number, routes: Array<{ slug: string, table: string, idColumn?: string }> }
 */
export const syncApiBuilderSlugRoutes = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email || ctx.tenantId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in required to publish blueprint routes.' });
  }
  const connectionId = Number(req.body?.connectionId);
  const routes = Array.isArray(req.body?.routes) ? req.body.routes : [];
  if (!Number.isFinite(connectionId) || connectionId <= 0) {
    return res.status(400).json({ ok: false, message: 'connectionId is required.' });
  }

  const normalized = [];
  const seen = new Set();
  for (const r of routes) {
    const raw = String(r?.slug || '').trim().replace(/^\/+|\/+$/g, '');
    const slugPart = raw.replace(/^api\/+/i, '').split('/')[0]?.trim().toLowerCase() || '';
    const table = String(r?.table || '').trim();
    const idColRaw = String(r?.idColumn ?? r?.id_column ?? '').trim();
    const idColumn = isSafeSqlIdentifier(idColRaw) ? idColRaw : null;
    if (!slugPart || !table || RESERVED_BLUEPRINT_SLUGS.has(slugPart)) continue;
    if (!isBlueprintSlugShape(slugPart)) continue;
    if (seen.has(slugPart)) continue;
    seen.add(slugPart);
    normalized.push({ slug: slugPart, table, idColumn });
  }

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const verify = await pool.request().input('id', sql.Int, connectionId).query(`
      SELECT TOP 1 id, tenant_id FROM dbo.connection_profiles WHERE id = @id
    `);
    const connRow = verify.recordset?.[0];
    if (!connRow) {
      return res.status(403).json({ ok: false, message: 'Connection not found for this workspace.' });
    }
    if (
      connRow.tenant_id != null &&
      Number(connRow.tenant_id) !== Number(ctx.tenantId)
    ) {
      return res.status(403).json({ ok: false, message: 'Connection not found for this workspace.' });
    }

    // Empty payload: do not wipe existing routes (GET disabled everywhere still avoids accidental unpublish).
    if (normalized.length === 0) {
      return res.json({
        ok: true,
        synced: 0,
        slugs: [],
        message:
          'No blueprint routes to sync (enable GET and use Base path like /api/your-slug). Existing published routes were left unchanged.',
      });
    }

    for (const row of normalized) {
      await pool
        .request()
        .input('tid', sql.Int, ctx.tenantId)
        .input('slug', sql.NVarChar, row.slug)
        .input('cid', sql.Int, connectionId)
        .input('tn', sql.NVarChar(sql.MAX), row.table)
        .input('idc', sql.NVarChar(512), row.idColumn == null ? null : row.idColumn).query(`
          MERGE dbo.api_builder_slug_routes AS tgt
          USING (
            SELECT @tid AS tenant_id, @slug AS slug, @cid AS connection_id, @tn AS table_name, @idc AS id_column
          ) AS src
          ON tgt.tenant_id = src.tenant_id AND tgt.slug = src.slug
          WHEN MATCHED THEN UPDATE SET
            connection_id = src.connection_id,
            table_name = src.table_name,
            id_column = src.id_column,
            updated_at = SYSDATETIME()
          WHEN NOT MATCHED THEN INSERT (tenant_id, slug, connection_id, table_name, id_column, updated_at)
            VALUES (src.tenant_id, src.slug, src.connection_id, src.table_name, src.id_column, SYSDATETIME());
        `);
    }

    const delReq = pool
      .request()
      .input('tid', sql.Int, ctx.tenantId)
      .input('cid', sql.Int, connectionId);
    normalized.forEach((row, i) => delReq.input(`s${i}`, sql.NVarChar, row.slug));
    const inList = normalized.map((_, i) => `@s${i}`).join(', ');
    await delReq.query(`
      DELETE FROM dbo.api_builder_slug_routes
      WHERE tenant_id = @tid AND connection_id = @cid AND slug NOT IN (${inList})
    `);

    return res.json({ ok: true, synced: normalized.length, slugs: normalized.map((x) => x.slug) });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to sync blueprint routes.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/**
 * GET /api/api-builder/published-routes — list slug routes for this workspace (debug / verify Publish).
 * Query: optional connectionId (ignored for JWT-only clients; scope is always the token connection).
 */
export const listPublishedBlueprintRoutes = async (req, res) => {
  const ctx = req.context || {};
  if (ctx.tenantId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in or use a Bearer token for this workspace.' });
  }

  let connectionId = null;
  if (ctx.authViaPublicApiJwt) {
    const cid = Number(ctx.jwtConnectionId);
    if (!Number.isFinite(cid) || cid <= 0) {
      return res.status(403).json({ ok: false, message: 'Bearer token is missing connection scope.' });
    }
    connectionId = cid;
  } else {
    if (!ctx.email) {
      return res.status(401).json({ ok: false, message: 'Sign in required to list published routes.' });
    }
    const q = Number(req.query?.connectionId);
    if (Number.isFinite(q) && q > 0) connectionId = q;
  }

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rq = pool.request().input('tid', sql.Int, ctx.tenantId);
    let qtext = `
      SELECT slug, connection_id, table_name, id_column, updated_at
      FROM dbo.api_builder_slug_routes
      WHERE tenant_id = @tid
    `;
    if (connectionId != null) {
      rq.input('cid', sql.Int, connectionId);
      qtext += ` AND connection_id = @cid`;
    }
    qtext += ` ORDER BY slug`;
    const rs = await rq.query(qtext);
    return res.json({
      ok: true,
      tenantId: ctx.tenantId,
      connectionId: connectionId ?? null,
      routes: (rs.recordset || []).map((row) => ({
        slug: String(row.slug || ''),
        connectionId: Number(row.connection_id),
        table: String(row.table_name || ''),
        idColumn: row.id_column != null && String(row.id_column).trim() ? String(row.id_column).trim() : null,
        updatedAt: row.updated_at,
      })),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list blueprint routes.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/**
 * GET /api/:slug/:recordKey — single row (Path params first field = PK column, stored as id_column on publish).
 */
export const getBlueprintSlugSingleRecord = async (req, res, next) => {
  const slug = String(req.params.slug || '').trim().toLowerCase();
  const recordKey = String(req.params.recordKey ?? '').trim();
  if (!slug || !isBlueprintSlugShape(slug) || RESERVED_BLUEPRINT_SLUGS.has(slug)) {
    return next();
  }
  if (!isBlueprintRecordKeyShape(recordKey)) {
    return res.status(400).json({ ok: false, message: 'Invalid resource id in URL path.' });
  }

  const ctx = req.context || {};
  if (ctx.tenantId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in or use a Bearer token for this workspace.' });
  }

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool
      .request()
      .input('tid', sql.Int, ctx.tenantId)
      .input('slug', sql.NVarChar, slug).query(`
        SELECT TOP 1 connection_id, table_name, id_column
        FROM dbo.api_builder_slug_routes
        WHERE tenant_id = @tid AND slug = @slug
      `);
    const row = rs.recordset?.[0];
    await closeControlSqlServer(pool);
    pool = null;

    if (!row) {
      return res.status(404).json({
        ok: false,
        message: `Route GET /api/${slug}/${recordKey} is not published for your workspace. Publish routes from API Builder or use GET /api/connections/<connectionId>/table-data?table=<name>.`,
      });
    }

    const idColumn = String(row.id_column || '').trim();
    if (!idColumn || !isSafeSqlIdentifier(idColumn)) {
      return res.status(400).json({
        ok: false,
        message: `GET /api/${slug}/:id requires a primary-key Path param. In API Builder open the table, set Path params (first = key column), enable GET, and Publish routes.`,
      });
    }

    const cid = Number(row.connection_id);
    const table = String(row.table_name || '').trim();
    if (ctx.authViaPublicApiJwt && Number(ctx.jwtConnectionId) !== cid) {
      return res.status(403).json({ ok: false, message: 'Bearer token is not scoped to this blueprint route.' });
    }

    let filtersObj = {};
    const filtersRaw = req.query.filters;
    if (typeof filtersRaw === 'string' && filtersRaw.trim()) {
      try {
        const p = JSON.parse(filtersRaw);
        if (p && typeof p === 'object' && !Array.isArray(p)) filtersObj = { ...p };
      } catch {
        /* ignore malformed filters */
      }
    }
    filtersObj[idColumn] = recordKey;

    const fakeReq = {
      params: { id: String(cid) },
      query: {
        ...req.query,
        table,
        filters: JSON.stringify(filtersObj),
        limit: '1',
        offset: '0',
      },
      headers: req.headers,
      context: req.context,
    };
    return await getConnectionTableData(fakeReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Blueprint route failed.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (pool) await closeControlSqlServer(pool);
  }
};

/**
 * POST /api/:slug → proxies to connection table-data/mutate using the published table for this slug.
 * Accepts either `{ action, values?, rowMatch? }` (same as /table-data/mutate) or a flat JSON object (insert/create).
 */
export const postBlueprintSlugMutate = async (req, res, next) => {
  const slug = String(req.params.slug || '').trim().toLowerCase();
  if (!slug || !isBlueprintSlugShape(slug) || RESERVED_BLUEPRINT_SLUGS.has(slug)) {
    return next();
  }

  const ctx = req.context || {};
  if (ctx.tenantId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in or use a Bearer token for this workspace.' });
  }

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool
      .request()
      .input('tid', sql.Int, ctx.tenantId)
      .input('slug', sql.NVarChar, slug).query(`
        SELECT TOP 1 connection_id, table_name
        FROM dbo.api_builder_slug_routes
        WHERE tenant_id = @tid AND slug = @slug
      `);
    const row = rs.recordset?.[0];
    await closeControlSqlServer(pool);
    pool = null;

    if (!row) {
      return res.status(404).json({
        ok: false,
        message: `Route POST /api/${slug} is not published for your workspace. Publish slug routes from API Builder (enable GET for this base path), then POST again — or call POST /api/connections/<connectionId>/table-data/mutate with body { table, action, values, rowMatch }.`,
      });
    }

    const cid = Number(row.connection_id);
    const table = String(row.table_name || '').trim();
    if (ctx.authViaPublicApiJwt && Number(ctx.jwtConnectionId) !== cid) {
      return res.status(403).json({ ok: false, message: 'Bearer token is not scoped to this blueprint route.' });
    }

    const mutateBody = buildBlueprintMutateBodyFromPost(table, req.body);
    if (!mutateBody) {
      return res.status(400).json({ ok: false, message: 'JSON body must be an object.' });
    }

    const fakeReq = {
      params: { id: String(cid) },
      body: mutateBody,
      headers: req.headers,
      context: req.context,
    };
    return await mutateConnectionTableData(fakeReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Blueprint POST failed.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (pool) await closeControlSqlServer(pool);
  }
};

/**
 * GET /api/:slug → proxies to connection table-data when slug was published via sync.
 */
export const getBlueprintSlugTableData = async (req, res, next) => {
  const slug = String(req.params.slug || '').trim().toLowerCase();
  if (!slug || !isBlueprintSlugShape(slug) || RESERVED_BLUEPRINT_SLUGS.has(slug)) {
    return next();
  }

  const ctx = req.context || {};
  if (ctx.tenantId == null) {
    return res.status(401).json({ ok: false, message: 'Sign in or use a Bearer token for this workspace.' });
  }

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureCoreTables(pool);
    const rs = await pool
      .request()
      .input('tid', sql.Int, ctx.tenantId)
      .input('slug', sql.NVarChar, slug).query(`
        SELECT TOP 1 connection_id, table_name
        FROM dbo.api_builder_slug_routes
        WHERE tenant_id = @tid AND slug = @slug
      `);
    const row = rs.recordset?.[0];
    await closeControlSqlServer(pool);
    pool = null;

    if (!row) {
      return res.status(404).json({
        ok: false,
        message: `Route GET /api/${slug} is not published for your workspace (no row for your tenant). Publish from API Builder or GET /api/api-builder/published-routes?connectionId=<id> to verify. Enable GET and Base path /api/${slug}, then Publish routes.`,
      });
    }

    const cid = Number(row.connection_id);
    const table = String(row.table_name || '').trim();
    if (ctx.authViaPublicApiJwt && Number(ctx.jwtConnectionId) !== cid) {
      return res.status(403).json({ ok: false, message: 'Bearer token is not scoped to this blueprint route.' });
    }

    const fakeReq = {
      params: { id: String(cid) },
      query: { ...req.query, table },
      headers: req.headers,
      context: req.context,
    };
    return await getConnectionTableData(fakeReq, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Blueprint route failed.';
    return res.status(500).json({ ok: false, message });
  } finally {
    if (pool) await closeControlSqlServer(pool);
  }
};
