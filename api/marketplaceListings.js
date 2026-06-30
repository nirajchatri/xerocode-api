import crypto from 'node:crypto';
import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';

const VALID_RESOURCE_TYPES = new Set([
  'application',
  'agent',
  'dashboard',
  'api',
  'automation',
  'mcp_server',
  'integration_tool',
  'webhook',
]);

const VALID_CATEGORIES = new Set([
  'applications',
  'agents',
  'apis',
  'mcp-servers',
  'integration-tools',
  'webhooks',
  'dashboard-templates',
]);

const RESOURCE_TO_CATEGORY = {
  application: 'applications',
  agent: 'agents',
  dashboard: 'dashboard-templates',
  api: 'apis',
  automation: 'integration-tools',
  mcp_server: 'mcp-servers',
  integration_tool: 'integration-tools',
  webhook: 'webhooks',
};

const VALID_PRICING_TYPES = new Set(['free', 'paid']);
const MAX_APPLICATION_IMAGE_CHARS = 3_500_000;

export const ensureMarketplaceTables = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.marketplace_public_listings', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.marketplace_public_listings (
        id NVARCHAR(64) NOT NULL PRIMARY KEY,
        resource_type NVARCHAR(32) NOT NULL,
        resource_id NVARCHAR(255) NOT NULL,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        title NVARCHAR(512) NOT NULL,
        description NVARCHAR(MAX) NULL,
        category NVARCHAR(64) NOT NULL,
        is_public BIT NOT NULL DEFAULT 1,
        pricing_type NVARCHAR(20) NOT NULL DEFAULT 'free',
        price_amount DECIMAL(18,2) NULL,
        currency NVARCHAR(3) NULL DEFAULT 'USD',
        listing_payload NVARCHAR(MAX) NULL,
        application_image NVARCHAR(MAX) NULL,
        published_at BIGINT NULL,
        updated_at BIGINT NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        CONSTRAINT UQ_marketplace_resource UNIQUE (resource_type, resource_id)
      );
      CREATE INDEX idx_marketplace_public_browse
        ON dbo.marketplace_public_listings (is_public, category, updated_at DESC);
    END
  `);
  await pool.request().query(`
    IF COL_LENGTH('dbo.marketplace_public_listings', 'application_image') IS NULL
    BEGIN
      ALTER TABLE dbo.marketplace_public_listings ADD application_image NVARCHAR(MAX) NULL;
    END
  `);
};

const LISTING_SELECT_COLUMNS = `
  id, resource_type, resource_id, tenant_id, owner_user_id, title, description,
  category, is_public, pricing_type, price_amount, currency, listing_payload,
  application_image, published_at, updated_at, created_at
`;

const extractApplicationImage = (body, listingPayload) => {
  const fromBody = typeof body?.applicationImage === 'string' ? body.applicationImage.trim() : '';
  const fromPayload =
    listingPayload && typeof listingPayload === 'object' && typeof listingPayload.imageUrl === 'string'
      ? listingPayload.imageUrl.trim()
      : '';
  const image = fromBody || fromPayload || null;
  if (image && image.length > MAX_APPLICATION_IMAGE_CHARS) {
    throw new Error('Application image is too large.');
  }
  return image;
};

const mergeListingPayloadWithImage = (listingPayload, applicationImage) => {
  const base =
    listingPayload && typeof listingPayload === 'object' && !Array.isArray(listingPayload)
      ? { ...listingPayload }
      : {};
  if (applicationImage) {
    base.imageUrl = applicationImage;
  } else if ('imageUrl' in base) {
    delete base.imageUrl;
  }
  return Object.keys(base).length > 0 ? base : null;
};

const newListingId = () => `mkt-${crypto.randomBytes(12).toString('hex')}`;

const rowToListing = (row) => {
  const applicationImage = row.application_image != null ? String(row.application_image) : null;
  let listingPayload = row.listing_payload ? JSON.parse(String(row.listing_payload)) : null;
  listingPayload = mergeListingPayloadWithImage(listingPayload, applicationImage);
  return {
    id: String(row.id),
    resourceType: String(row.resource_type),
    resourceId: String(row.resource_id),
    tenantId: Number(row.tenant_id),
    ownerUserId: Number(row.owner_user_id),
    title: String(row.title || ''),
    description: row.description != null ? String(row.description) : '',
    category: String(row.category || ''),
    isPublic: Boolean(row.is_public),
    pricingType: String(row.pricing_type || 'free'),
    priceAmount: row.price_amount != null ? Number(row.price_amount) : null,
    currency: row.currency != null ? String(row.currency) : 'USD',
    listingPayload,
    applicationImage,
    publishedAt: row.published_at != null ? Number(row.published_at) : null,
    updatedAt: Number(row.updated_at || Date.now()),
    createdAt: Number(row.created_at || Date.now()),
  };
};

const publicListingShape = (listing) => ({
  id: listing.id,
  resourceType: listing.resourceType,
  resourceId: listing.resourceId,
  title: listing.title,
  description: listing.description,
  category: listing.category,
  pricingType: listing.pricingType,
  priceAmount: listing.priceAmount,
  currency: listing.currency,
  listingPayload: listing.listingPayload,
  applicationImage: listing.applicationImage ?? null,
  publishedAt: listing.publishedAt,
  updatedAt: listing.updatedAt,
});

const parseJson = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

const uniqueStrings = (items) => [...new Set(items.map((s) => String(s || '').trim()).filter(Boolean))];

const inferLlmProvider = (model) => {
  const m = String(model || '').toLowerCase();
  if (m.includes('gpt') || m.includes('o1') || m.includes('o3')) return 'openai';
  if (m.includes('claude')) return 'anthropic';
  if (m.includes('gemini')) return 'google';
  if (m.includes('llama') || m.includes('meta')) return 'meta';
  if (m.includes('mistral')) return 'mistral';
  if (m.includes('deepseek')) return 'deepseek';
  return 'other';
};

const sanitizeWorkflowNodes = (nodes) =>
  (Array.isArray(nodes) ? nodes : []).map((n) => ({
    id: String(n?.id || ''),
    type: String(n?.type || 'label'),
    label: String(n?.data?.label || n?.data?.agentDisplayName || n?.data?.apiName || n?.type || 'Node'),
    x: Number(n?.position?.x) || 0,
    y: Number(n?.position?.y) || 0,
  }));

const sanitizeWorkflowEdges = (edges) =>
  (Array.isArray(edges) ? edges : []).map((e) => ({
    source: String(e?.source || ''),
    target: String(e?.target || ''),
  }));

const extractWorkflowMeta = (payload) => {
  const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
  const llmModels = uniqueStrings(
    nodes.filter((n) => n?.type === 'llm').map((n) => n?.data?.model || n?.data?.agentDisplayName)
  );
  const apis = uniqueStrings(nodes.filter((n) => n?.type === 'api').map((n) => n?.data?.apiName || n?.data?.label));
  const mcps = uniqueStrings(
    nodes.filter((n) => n?.type === 'mcp').map((n) => n?.data?.mcpServerLabel || n?.data?.label)
  );
  const llmProviders = uniqueStrings(llmModels.map(inferLlmProvider));
  return {
    llmProviders,
    llmModels,
    datasourceSchemas: [],
    datasourceLabel: null,
    connectorType: null,
    apis: [...apis, ...mcps],
    snapshot: {
      kind: 'workflow',
      workflowName: String(payload?.workflowName || payload?.name || ''),
      nodes: sanitizeWorkflowNodes(nodes),
      edges: sanitizeWorkflowEdges(payload?.edges),
    },
  };
};

const extractApplicationMeta = (payload) => {
  const tables = uniqueStrings(payload?.selectedTables || payload?.lastState?.selectedTables || []);
  return {
    llmProviders: uniqueStrings([payload?.llmProvider]),
    llmModels: uniqueStrings([payload?.llmModel]),
    datasourceSchemas: tables,
    datasourceLabel: payload?.dataSourceName ? String(payload.dataSourceName) : null,
    connectorType: payload?.connectorType ? String(payload.connectorType) : null,
    apis: uniqueStrings(payload?.suggestedApiRef ? [payload.suggestedApiRef] : []),
    snapshot: {
      kind: 'application',
      applicationName: String(payload?.name || payload?.lastState?.applicationName || ''),
      selectedTables: tables,
      dataSourceName: payload?.dataSourceName ? String(payload.dataSourceName) : '',
    },
  };
};

const extractDashboardMeta = (payload) => {
  let chartCount = 0;
  try {
    const proposal = typeof payload?.proposalJson === 'string' ? JSON.parse(payload.proposalJson) : payload?.proposalJson;
    const widgets = proposal?.widgets || proposal?.charts || proposal?.panels;
    if (Array.isArray(widgets)) chartCount = widgets.length;
  } catch {
    /* ignore */
  }
  return {
    llmProviders: uniqueStrings([payload?.llmProvider]),
    llmModels: uniqueStrings([payload?.llmModel]),
    datasourceSchemas: uniqueStrings(
      payload?.workspaceDataSourceLabel ? [payload.workspaceDataSourceLabel] : []
    ),
    datasourceLabel: payload?.workspaceDataSourceLabel ? String(payload.workspaceDataSourceLabel) : null,
    connectorType: null,
    apis: uniqueStrings(payload?.selectedApiKey ? [payload.selectedApiKey] : []),
    snapshot: {
      kind: 'dashboard',
      chartCount,
      objective: String(payload?.workspacePrompt || payload?.projectDescription || ''),
    },
  };
};

const extractApiMeta = (payload) => {
  const tables = Array.isArray(payload?.tables) ? payload.tables : [];
  const endpoints = tables
    .filter((t) => t?.api?.enabled !== false)
    .map((t) => ({
      table: String(t?.table || ''),
      basePath: String(t?.api?.basePath || `/${t?.table || ''}`),
      methods: Object.entries(t?.api?.methods || {})
        .filter(([, on]) => Boolean(on))
        .map(([m]) => m.toUpperCase()),
    }));
  return {
    llmProviders: [],
    llmModels: [],
    datasourceSchemas: uniqueStrings([
      payload?.connection?.database_name,
      payload?.connection?.friendly_name,
      ...tables.map((t) => t?.table),
    ]),
    datasourceLabel: payload?.connection?.friendly_name ? String(payload.connection.friendly_name) : null,
    connectorType: payload?.connection?.connector_type ? String(payload.connection.connector_type) : null,
    apis: uniqueStrings([payload?.name, ...endpoints.map((e) => e.basePath)]),
    snapshot: {
      kind: 'api',
      apiName: String(payload?.name || ''),
      apiMode: String(payload?.apiMode || 'separate'),
      endpoints,
    },
  };
};

async function loadOwnerProfile(pool, userId) {
  const rs = await pool
    .request()
    .input('userId', sql.Int, Number(userId))
    .query(`SELECT TOP 1 full_name, email, avatar_url FROM dbo.user_profile WHERE id = @userId`);
  const row = rs.recordset?.[0];
  const name = String(row?.full_name || '').trim();
  return {
    authorName: name || 'XeroCode Creator',
    authorEmail: row?.email ? String(row.email) : null,
    authorAvatarUrl: row?.avatar_url ? String(row.avatar_url) : null,
  };
}

async function resolveListingDetail(pool, listing) {
  const author = await loadOwnerProfile(pool, listing.ownerUserId);
  const base = {
    authorName: author.authorName,
    authorEmail: author.authorEmail,
    authorAvatarUrl: author.authorAvatarUrl,
    llmProviders: [],
    llmModels: [],
    datasourceSchemas: [],
    datasourceLabel: null,
    connectorType: null,
    apis: [],
    categories: [listing.category],
    snapshot: listing.listingPayload?.snapshot || { kind: 'generic' },
  };

  const { resourceType, resourceId, tenantId } = listing;
  try {
    if (resourceType === 'application') {
      const rs = await pool
        .request()
        .input('id', sql.NVarChar, resourceId)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`SELECT TOP 1 payload FROM dbo.saved_apps WHERE id = @id AND tenant_id = @tenantId`);
      const payload = parseJson(rs.recordset?.[0]?.payload);
      return { ...base, ...extractApplicationMeta(payload) };
    }
    if (resourceType === 'agent') {
      const rs = await pool
        .request()
        .input('id', sql.NVarChar, resourceId)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`SELECT TOP 1 payload FROM dbo.saved_studio_agents WHERE id = @id AND tenant_id = @tenantId`);
      const payload = parseJson(rs.recordset?.[0]?.payload);
      return { ...base, ...extractWorkflowMeta(payload) };
    }
    if (resourceType === 'automation') {
      const rs = await pool
        .request()
        .input('id', sql.NVarChar, resourceId)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`SELECT TOP 1 payload FROM dbo.saved_automation_projects WHERE id = @id AND tenant_id = @tenantId`);
      const row = rs.recordset?.[0];
      const payload = parseJson(row?.payload);
      const inner =
        payload?.payload && Array.isArray(payload.payload?.nodes)
          ? payload.payload
          : Array.isArray(payload?.nodes)
            ? payload
            : {};
      return { ...base, ...extractWorkflowMeta(inner) };
    }
    if (resourceType === 'dashboard') {
      const rs = await pool
        .request()
        .input('id', sql.NVarChar, resourceId)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`SELECT TOP 1 payload FROM dbo.saved_dashboards WHERE id = @id AND tenant_id = @tenantId`);
      const payload = parseJson(rs.recordset?.[0]?.payload);
      return { ...base, ...extractDashboardMeta(payload) };
    }
    if (resourceType === 'api') {
      const rs = await pool
        .request()
        .input('id', sql.NVarChar, resourceId)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`SELECT TOP 1 payload FROM dbo.saved_blueprint_apis WHERE id = @id AND tenant_id = @tenantId`);
      const payload = parseJson(rs.recordset?.[0]?.payload);
      return { ...base, ...extractApiMeta(payload) };
    }
  } catch (err) {
    console.error('resolveListingDetail:', err);
  }

  if (listing.listingPayload && typeof listing.listingPayload === 'object') {
    const lp = listing.listingPayload;
    return {
      ...base,
      llmProviders: uniqueStrings(lp.llmProviders || base.llmProviders),
      llmModels: uniqueStrings(lp.llmModels || base.llmModels),
      datasourceSchemas: uniqueStrings(lp.datasourceSchemas || base.datasourceSchemas),
      datasourceLabel: lp.datasourceLabel || base.datasourceLabel,
      connectorType: lp.connectorType || base.connectorType,
      apis: uniqueStrings(lp.apis || base.apis),
      snapshot: lp.snapshot || base.snapshot,
    };
  }
  if (listing.applicationImage) {
    return {
      ...base,
      snapshot: {
        ...(base.snapshot && typeof base.snapshot === 'object' ? base.snapshot : { kind: 'generic' }),
        imageUrl: listing.applicationImage,
      },
    };
  }
  return base;
}

/** GET /api/marketplace/listings — public browse (optional ?category=) */
export const listMarketplaceListings = async (req, res) => {
  let pool;
  try {
    const category = String(req.query?.category || '').trim();
    pool = await connectToControlSqlServer();
    await ensureMarketplaceTables(pool);
    const rq = pool.request();
    let where = 'is_public = 1';
    if (category && VALID_CATEGORIES.has(category)) {
      rq.input('category', sql.NVarChar, category);
      where += ' AND category = @category';
    }
    const result = await rq.query(`
      SELECT ${LISTING_SELECT_COLUMNS}
      FROM dbo.marketplace_public_listings
      WHERE ${where}
      ORDER BY updated_at DESC
    `);
    const listings = (result.recordset || []).map((row) => publicListingShape(rowToListing(row)));
    return res.json({ ok: true, listings, count: listings.length });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list marketplace listings.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** GET /api/marketplace/listings/:id — public detail */
export const getMarketplaceListing = async (req, res) => {
  let pool;
  try {
    const id = String(req.params?.id || '').trim();
    if (!id) return res.status(400).json({ ok: false, message: 'Listing id is required.' });
    pool = await connectToControlSqlServer();
    await ensureMarketplaceTables(pool);
    const result = await pool
      .request()
      .input('id', sql.NVarChar, id)
      .query(`
        SELECT ${LISTING_SELECT_COLUMNS}
        FROM dbo.marketplace_public_listings
        WHERE id = @id AND is_public = 1
      `);
    const row = result.recordset?.[0];
    if (!row) return res.status(404).json({ ok: false, message: 'Listing not found.' });
    const listing = rowToListing(row);
    const detail = await resolveListingDetail(pool, listing);
    return res.json({ ok: true, listing: publicListingShape(listing), detail });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load marketplace listing.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** GET /api/marketplace/listings/by-resource/:resourceType/:resourceId — owner lookup */
export const getMarketplaceListingByResource = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const resourceType = String(req.params?.resourceType || '').trim();
    const resourceId = String(req.params?.resourceId || '').trim();
    if (!VALID_RESOURCE_TYPES.has(resourceType) || !resourceId) {
      return res.status(400).json({ ok: false, message: 'Invalid resource type or id.' });
    }
    pool = await connectToControlSqlServer();
    await ensureMarketplaceTables(pool);
    const result = await pool
      .request()
      .input('resourceType', sql.NVarChar, resourceType)
      .input('resourceId', sql.NVarChar, resourceId)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`
        SELECT ${LISTING_SELECT_COLUMNS}
        FROM dbo.marketplace_public_listings
        WHERE resource_type = @resourceType AND resource_id = @resourceId
          AND tenant_id = @tenantId AND owner_user_id = @userId
      `);
    const row = result.recordset?.[0];
    if (!row) return res.json({ ok: true, listing: null });
    return res.json({ ok: true, listing: rowToListing(row) });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load marketplace listing.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** POST /api/marketplace/listings — publish or update listing */
export const saveMarketplaceListing = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const body = req.body || {};
    const resourceType = String(body.resourceType || '').trim();
    const resourceId = String(body.resourceId || '').trim();
    const title = String(body.title || '').trim();
    const description = String(body.description || '').trim();
    const isPublic = body.isPublic !== false;
    const pricingType = VALID_PRICING_TYPES.has(body.pricingType) ? body.pricingType : 'free';
    const priceAmount =
      pricingType === 'paid' && body.priceAmount != null && !Number.isNaN(Number(body.priceAmount))
        ? Number(body.priceAmount)
        : null;
    const currency = String(body.currency || 'USD').trim().slice(0, 3).toUpperCase() || 'USD';
    const category =
      body.category && VALID_CATEGORIES.has(body.category)
        ? body.category
        : RESOURCE_TO_CATEGORY[resourceType] || 'applications';
    const listingPayloadRaw = body.listingPayload ?? null;
    const applicationImage = extractApplicationImage(body, listingPayloadRaw);
    const listingPayload = mergeListingPayloadWithImage(listingPayloadRaw, applicationImage);

    if (!VALID_RESOURCE_TYPES.has(resourceType)) {
      return res.status(400).json({ ok: false, message: 'Invalid resource type.' });
    }
    if (!resourceId) return res.status(400).json({ ok: false, message: 'Resource id is required.' });
    if (!title) return res.status(400).json({ ok: false, message: 'Title is required.' });
    if (pricingType === 'paid' && (priceAmount == null || priceAmount < 0)) {
      return res.status(400).json({ ok: false, message: 'Paid listings require a valid price.' });
    }

    const now = Date.now();
    pool = await connectToControlSqlServer();
    await ensureMarketplaceTables(pool);

    const existing = await pool
      .request()
      .input('resourceType', sql.NVarChar, resourceType)
      .input('resourceId', sql.NVarChar, resourceId)
      .query(`
        SELECT TOP 1 id, tenant_id, owner_user_id
        FROM dbo.marketplace_public_listings
        WHERE resource_type = @resourceType AND resource_id = @resourceId
      `);
    const existingRow = existing.recordset?.[0];

    if (!isPublic) {
      if (existingRow) {
        await pool
          .request()
          .input('id', sql.NVarChar, String(existingRow.id))
          .query(`DELETE FROM dbo.marketplace_public_listings WHERE id = @id`);
      }
      return res.json({ ok: true, id: null, isPublic: false });
    }

    const id = existingRow ? String(existingRow.id) : newListingId();
    const publishedAt = existingRow ? undefined : now;

    const rq = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('resourceType', sql.NVarChar, resourceType)
      .input('resourceId', sql.NVarChar, resourceId)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('title', sql.NVarChar, title)
      .input('description', sql.NVarChar(sql.MAX), description || null)
      .input('category', sql.NVarChar, category)
      .input('isPublic', sql.Bit, 1)
      .input('pricingType', sql.NVarChar, pricingType)
      .input('priceAmount', sql.Decimal(18, 2), priceAmount)
      .input('currency', sql.NVarChar, currency)
      .input('listingPayload', sql.NVarChar(sql.MAX), listingPayload ? JSON.stringify(listingPayload) : null)
      .input('applicationImage', sql.NVarChar(sql.MAX), applicationImage)
      .input('updatedAt', sql.BigInt, now);

    if (existingRow) {
      if (Number(existingRow.tenant_id) !== Number(tenantId) || Number(existingRow.owner_user_id) !== Number(userId)) {
        return res.status(403).json({ ok: false, message: 'Not allowed to update this listing.' });
      }
      await rq.query(`
        UPDATE dbo.marketplace_public_listings
        SET title = @title, description = @description, category = @category,
            is_public = @isPublic, pricing_type = @pricingType, price_amount = @priceAmount,
            currency = @currency, listing_payload = @listingPayload,
            application_image = @applicationImage, updated_at = @updatedAt
        WHERE id = @id
      `);
    } else {
      await rq
        .input('publishedAt', sql.BigInt, publishedAt)
        .input('createdAt', sql.BigInt, now)
        .query(`
          INSERT INTO dbo.marketplace_public_listings (
            id, resource_type, resource_id, tenant_id, owner_user_id, title, description,
            category, is_public, pricing_type, price_amount, currency, listing_payload,
            application_image, published_at, updated_at, created_at
          ) VALUES (
            @id, @resourceType, @resourceId, @tenantId, @ownerUserId, @title, @description,
            @category, @isPublic, @pricingType, @priceAmount, @currency, @listingPayload,
            @applicationImage, @publishedAt, @updatedAt, @createdAt
          )
        `);
    }

    const storedListing = {
      id,
      resourceType,
      resourceId,
      tenantId: Number(tenantId),
      ownerUserId: Number(userId),
      title,
      description,
      category,
      isPublic: true,
      pricingType,
      priceAmount,
      currency,
      listingPayload: listingPayload || null,
      applicationImage,
      publishedAt: publishedAt ?? now,
      updatedAt: now,
      createdAt: now,
    };
    try {
      const detail = await resolveListingDetail(pool, storedListing);
      const enrichedPayload = mergeListingPayloadWithImage(
        {
          ...(listingPayload && typeof listingPayload === 'object' ? listingPayload : {}),
          snapshot: detail.snapshot,
          llmProviders: detail.llmProviders,
          llmModels: detail.llmModels,
          datasourceSchemas: detail.datasourceSchemas,
          datasourceLabel: detail.datasourceLabel,
          connectorType: detail.connectorType,
          apis: detail.apis,
        },
        applicationImage
      );
      await pool
        .request()
        .input('id', sql.NVarChar, id)
        .input('payload', sql.NVarChar(sql.MAX), enrichedPayload ? JSON.stringify(enrichedPayload) : null)
        .query(`UPDATE dbo.marketplace_public_listings SET listing_payload = @payload WHERE id = @id`);
    } catch (enrichErr) {
      console.error('marketplace listing_payload enrich:', enrichErr);
    }

    return res.json({ ok: true, id, isPublic: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save marketplace listing.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** DELETE /api/marketplace/listings/:id — unpublish */
export const deleteMarketplaceListing = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(400).json({ ok: false, message: 'Missing tenant/user context.' });
    }
    const id = String(req.params?.id || '').trim();
    if (!id) return res.status(400).json({ ok: false, message: 'Listing id is required.' });
    pool = await connectToControlSqlServer();
    await ensureMarketplaceTables(pool);
    const result = await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, Number(userId))
      .query(`
        DELETE FROM dbo.marketplace_public_listings
        WHERE id = @id AND tenant_id = @tenantId AND owner_user_id = @userId
      `);
    if (!result.rowsAffected?.[0]) {
      return res.status(404).json({ ok: false, message: 'Listing not found.' });
    }
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete marketplace listing.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
