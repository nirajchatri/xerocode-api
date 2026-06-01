import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';

const parseJson = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

export const ensureWebsiteCmsTables = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.website_pages', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.website_pages (
        id NVARCHAR(64) PRIMARY KEY,
        slug NVARCHAR(120) NOT NULL UNIQUE,
        title NVARCHAR(512) NOT NULL,
        meta_description NVARCHAR(1024) NULL,
        is_published BIT NOT NULL DEFAULT 1,
        sort_order INT NOT NULL DEFAULT 0,
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_website_pages_published ON dbo.website_pages (is_published, sort_order);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.website_sections', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.website_sections (
        id NVARCHAR(64) PRIMARY KEY,
        page_slug NVARCHAR(120) NOT NULL,
        section_key NVARCHAR(120) NOT NULL,
        section_type NVARCHAR(64) NOT NULL,
        title NVARCHAR(512) NULL,
        subtitle NVARCHAR(1024) NULL,
        body_json NVARCHAR(MAX) NOT NULL,
        sort_order INT NOT NULL DEFAULT 0,
        is_visible BIT NOT NULL DEFAULT 1,
        updated_at BIGINT NOT NULL,
        CONSTRAINT UQ_website_sections_page_key UNIQUE (page_slug, section_key)
      );
      CREATE INDEX idx_website_sections_page ON dbo.website_sections (page_slug, sort_order);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.website_settings', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.website_settings (
        setting_key NVARCHAR(120) PRIMARY KEY,
        value_json NVARCHAR(MAX) NOT NULL,
        updated_at BIGINT NOT NULL
      );
    END
  `);
};

const DEFAULT_SETTINGS = {
  brandName: 'XeroCode.ai',
  tagline: 'AI automation for technical teams',
  navLinks: [
    { label: 'Product', href: '#product' },
    { label: 'Automation', href: '#automation' },
    { label: 'APIs', href: '#apis' },
    { label: 'Agents', href: '#agents' },
    { label: 'Pricing', href: '#pricing' },
  ],
  footerLinks: [
    { label: 'Documentation', href: '#' },
    { label: 'Privacy', href: '#' },
    { label: 'Contact', href: 'mailto:hello@xerocode.ai' },
  ],
  ctaPrimary: { label: 'Start building', href: 'http://localhost:3000' },
  ctaSecondary: { label: 'Book a demo', href: '#contact' },
};

const DEFAULT_HOME_SECTIONS = [
  {
    id: 'hero',
    page_slug: 'home',
    section_key: 'hero',
    section_type: 'hero',
    title: 'The AI platform to automate, integrate, and ship faster',
    subtitle:
      'Design agentic workflows, publish blueprint APIs, and build live dashboards — with guardrails, MCP servers, and SQL-backed workspace data.',
    body_json: {
      badge: 'Workflow automation · APIs · Agents',
      primaryCta: { label: 'Open Studio', href: 'http://localhost:3000' },
      secondaryCta: { label: 'Watch overview', href: '#product' },
      highlights: ['Visual + code', 'MS SQL workspace', 'Enterprise-ready'],
    },
    sort_order: 10,
  },
  {
    id: 'stats',
    page_slug: 'home',
    section_key: 'stats',
    section_type: 'stats',
    title: 'Built for builders',
    subtitle: null,
    body_json: {
      items: [
        { value: '10x', label: 'Faster integrations' },
        { value: 'API', label: 'Blueprint publishing' },
        { value: 'MCP', label: 'Server connectors' },
        { value: '24/7', label: 'Automation runs' },
      ],
    },
    sort_order: 20,
  },
  {
    id: 'features',
    page_slug: 'home',
    section_key: 'features',
    section_type: 'features',
    title: 'Code when you need it. UI when you don\'t.',
    subtitle: 'Everything your team needs to go from idea to production — in one workspace.',
    body_json: {
      items: [
        {
          icon: 'workflow',
          title: 'Automation Studio',
          description: 'Orchestrate webhooks, APIs, LLMs, MCP tools, and guardrails with a live workflow canvas.',
        },
        {
          icon: 'api',
          title: 'API Builder',
          description: 'Generate REST blueprints from your database, publish slugs, and test with Bearer JWT.',
        },
        {
          icon: 'agent',
          title: 'Agent Builder',
          description: 'Standalone and managerial agents with chat, tools, and publishable public links.',
        },
        {
          icon: 'dashboard',
          title: 'Design Studio',
          description: 'AI-assisted dashboards bound to live datasources with filters and insight chat.',
        },
        {
          icon: 'mcp',
          title: 'MCP Servers',
          description: 'Connect Gmail, Slack, GitHub, and custom MCP endpoints with one config per brand.',
        },
        {
          icon: 'shield',
          title: 'Guardrails & governance',
          description: 'Tenant-scoped data, JWT-scoped APIs, and human-in-the-loop controls.',
        },
      ],
    },
    sort_order: 30,
  },
  {
    id: 'product',
    page_slug: 'home',
    section_key: 'product',
    section_type: 'split',
    title: 'Move fast. Break nothing.',
    subtitle: 'Short feedback loops keep your team in flow — replay steps, mock data, and ship with confidence.',
    body_json: {
      bullets: [
        'Re-run single workflow steps, not entire pipelines',
        'Publish blueprint GET routes to /api/your-slug',
        'Persist automations and MCP configs in SQL Server',
      ],
      imageAlt: 'Workflow canvas preview',
    },
    sort_order: 40,
  },
  {
    id: 'testimonials',
    page_slug: 'home',
    section_key: 'testimonials',
    section_type: 'testimonials',
    title: 'Teams ship AI workflows without the glue code',
    subtitle: null,
    body_json: {
      items: [
        {
          quote:
            'XeroCode replaced three internal tools — we design APIs, automate ops, and embed agents in one place.',
          author: 'Platform lead',
          company: 'Series B SaaS',
        },
        {
          quote: 'Blueprint APIs and JWT scopes let us expose customer data safely without rewriting backends.',
          author: 'Staff engineer',
          company: 'Fintech',
        },
      ],
    },
    sort_order: 50,
  },
  {
    id: 'pricing',
    page_slug: 'home',
    section_key: 'pricing',
    section_type: 'pricing',
    title: 'Start free. Scale with your team.',
    subtitle: 'Self-host the API or use our cloud — same studio experience.',
    body_json: {
      plans: [
        {
          name: 'Builder',
          price: 'Free',
          description: 'Local studio + SQL workspace',
          features: ['Automation Studio', 'API Builder', 'MCP profiles'],
        },
        {
          name: 'Team',
          price: 'Custom',
          description: 'Shared tenant + published agents',
          features: ['Everything in Builder', 'SSO-ready API', 'Priority support'],
          highlighted: true,
        },
        {
          name: 'Enterprise',
          price: 'Talk to us',
          description: 'On-prem and advanced governance',
          features: ['Dedicated control DB', 'Audit logs', 'Custom SLAs'],
        },
      ],
    },
    sort_order: 60,
  },
  {
    id: 'cta',
    page_slug: 'home',
    section_key: 'cta',
    section_type: 'cta',
    title: 'Ready to automate your stack?',
    subtitle: 'Open the studio, connect a datasource, and publish your first workflow in minutes.',
    body_json: {
      primaryCta: { label: 'Launch XeroCode Studio', href: 'http://localhost:3000' },
      secondaryCta: { label: 'Contact sales', href: 'mailto:hello@xerocode.ai' },
    },
    sort_order: 70,
  },
];

async function seedWebsiteCmsIfEmpty(pool) {
  const pageCount = await pool.request().query(`SELECT COUNT(*) AS c FROM dbo.website_pages`);
  if (Number(pageCount.recordset?.[0]?.c) > 0) return;

  const now = Date.now();
  await pool
    .request()
    .input('id', sql.NVarChar, 'page-home')
    .input('slug', sql.NVarChar, 'home')
    .input('title', sql.NVarChar, 'XeroCode.ai — AI automation platform')
    .input('meta', sql.NVarChar, 'Automate workflows, build APIs, and deploy AI agents.')
    .input('updated', sql.BigInt, now)
    .query(`
      INSERT INTO dbo.website_pages (id, slug, title, meta_description, is_published, sort_order, updated_at)
      VALUES (@id, @slug, @title, @meta, 1, 0, @updated)
    `);

  for (const s of DEFAULT_HOME_SECTIONS) {
    await pool
      .request()
      .input('id', sql.NVarChar, s.id)
      .input('page', sql.NVarChar, s.page_slug)
      .input('key', sql.NVarChar, s.section_key)
      .input('type', sql.NVarChar, s.section_type)
      .input('title', sql.NVarChar, s.title)
      .input('subtitle', sql.NVarChar, s.subtitle)
      .input('body', sql.NVarChar(sql.MAX), JSON.stringify(s.body_json))
      .input('sort', sql.Int, s.sort_order)
      .input('updated', sql.BigInt, now)
      .query(`
        INSERT INTO dbo.website_sections (id, page_slug, section_key, section_type, title, subtitle, body_json, sort_order, is_visible, updated_at)
        VALUES (@id, @page, @key, @type, @title, @subtitle, @body, @sort, 1, @updated)
      `);
  }

  await pool
    .request()
    .input('key', sql.NVarChar, 'site')
    .input('body', sql.NVarChar(sql.MAX), JSON.stringify(DEFAULT_SETTINGS))
    .input('updated', sql.BigInt, now)
    .query(`
      INSERT INTO dbo.website_settings (setting_key, value_json, updated_at)
      VALUES (@key, @body, @updated)
    `);
}

/** Public: full page payload for marketing site */
export const getPublicWebsitePage = async (req, res) => {
  const slug = String(req.query?.slug || req.params?.slug || 'home').trim().toLowerCase() || 'home';
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureWebsiteCmsTables(pool);
    await seedWebsiteCmsIfEmpty(pool);

    const pageRs = await pool
      .request()
      .input('slug', sql.NVarChar, slug)
      .query(`
        SELECT TOP 1 id, slug, title, meta_description, updated_at
        FROM dbo.website_pages
        WHERE slug = @slug AND is_published = 1
      `);
    const page = pageRs.recordset?.[0];
    if (!page) {
      return res.status(404).json({ ok: false, message: `Page "${slug}" not found.` });
    }

    const sectionsRs = await pool.request().input('slug', sql.NVarChar, slug).query(`
      SELECT id, section_key, section_type, title, subtitle, body_json, sort_order
      FROM dbo.website_sections
      WHERE page_slug = @slug AND is_visible = 1
      ORDER BY sort_order ASC
    `);

    const settingsRs = await pool.request().query(`SELECT setting_key, value_json FROM dbo.website_settings`);
    const settings = {};
    for (const row of settingsRs.recordset || []) {
      settings[String(row.setting_key)] = parseJson(row.value_json);
    }

    const sections = (sectionsRs.recordset || []).map((row) => ({
      id: String(row.id),
      key: String(row.section_key),
      type: String(row.section_type),
      title: row.title != null ? String(row.title) : '',
      subtitle: row.subtitle != null ? String(row.subtitle) : '',
      body: parseJson(row.body_json),
      sortOrder: Number(row.sort_order) || 0,
    }));

    return res.json({
      ok: true,
      page: {
        slug: String(page.slug),
        title: String(page.title),
        metaDescription: page.meta_description != null ? String(page.meta_description) : '',
        updatedAt: Number(page.updated_at) || Date.now(),
      },
      settings: settings.site || DEFAULT_SETTINGS,
      sections,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load website content.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** Admin: list pages + sections (requires signed-in studio user) */
export const listWebsiteCmsAdmin = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email) {
    return res.status(401).json({ ok: false, message: 'Sign in required for CMS admin.' });
  }
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureWebsiteCmsTables(pool);
    await seedWebsiteCmsIfEmpty(pool);

    const pagesRs = await pool.request().query(`
      SELECT id, slug, title, meta_description, is_published, sort_order, updated_at
      FROM dbo.website_pages ORDER BY sort_order ASC
    `);
    const sectionsRs = await pool.request().query(`
      SELECT id, page_slug, section_key, section_type, title, subtitle, body_json, sort_order, is_visible, updated_at
      FROM dbo.website_sections ORDER BY page_slug, sort_order ASC
    `);
    const settingsRs = await pool.request().query(`SELECT setting_key, value_json, updated_at FROM dbo.website_settings`);

    return res.json({
      ok: true,
      pages: (pagesRs.recordset || []).map((p) => ({
        id: String(p.id),
        slug: String(p.slug),
        title: String(p.title),
        metaDescription: p.meta_description != null ? String(p.meta_description) : '',
        isPublished: Boolean(p.is_published),
        sortOrder: Number(p.sort_order) || 0,
        updatedAt: Number(p.updated_at) || 0,
      })),
      sections: (sectionsRs.recordset || []).map((s) => ({
        id: String(s.id),
        pageSlug: String(s.page_slug),
        sectionKey: String(s.section_key),
        sectionType: String(s.section_type),
        title: s.title != null ? String(s.title) : '',
        subtitle: s.subtitle != null ? String(s.subtitle) : '',
        body: parseJson(s.body_json),
        sortOrder: Number(s.sort_order) || 0,
        isVisible: Boolean(s.is_visible),
        updatedAt: Number(s.updated_at) || 0,
      })),
      settings: (settingsRs.recordset || []).map((r) => ({
        key: String(r.setting_key),
        value: parseJson(r.value_json),
        updatedAt: Number(r.updated_at) || 0,
      })),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list CMS data.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** Admin: upsert section */
export const saveWebsiteSection = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email) {
    return res.status(401).json({ ok: false, message: 'Sign in required for CMS admin.' });
  }
  const section = req.body?.section ?? req.body;
  const id = String(section?.id || '').trim() || `sec-${Date.now()}`;
  const pageSlug = String(section?.pageSlug || section?.page_slug || 'home').trim();
  const sectionKey = String(section?.sectionKey || section?.section_key || '').trim();
  if (!sectionKey) {
    return res.status(400).json({ ok: false, message: 'sectionKey is required.' });
  }
  const updatedAt = Date.now();
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureWebsiteCmsTables(pool);
    const bodyJson = JSON.stringify(section?.body ?? section?.body_json ?? {});
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('page', sql.NVarChar, pageSlug)
      .input('key', sql.NVarChar, sectionKey)
      .input('type', sql.NVarChar, String(section?.sectionType || section?.section_type || 'custom'))
      .input('title', sql.NVarChar, String(section?.title || ''))
      .input('subtitle', sql.NVarChar, section?.subtitle != null ? String(section.subtitle) : null)
      .input('body', sql.NVarChar(sql.MAX), bodyJson)
      .input('sort', sql.Int, Number(section?.sortOrder ?? section?.sort_order) || 0)
      .input('visible', sql.Bit, section?.isVisible === false ? 0 : 1)
      .input('updated', sql.BigInt, updatedAt)
      .query(`
        MERGE dbo.website_sections AS tgt
        USING (SELECT @id AS id, @page AS page_slug, @key AS section_key) AS src
        ON tgt.page_slug = src.page_slug AND tgt.section_key = src.section_key
        WHEN MATCHED THEN UPDATE SET
          section_type = @type, title = @title, subtitle = @subtitle, body_json = @body,
          sort_order = @sort, is_visible = @visible, updated_at = @updated
        WHEN NOT MATCHED THEN INSERT (id, page_slug, section_key, section_type, title, subtitle, body_json, sort_order, is_visible, updated_at)
          VALUES (@id, @page, @key, @type, @title, @subtitle, @body, @sort, @visible, @updated);
      `);
    return res.json({ ok: true, id });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save section.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

/** Admin: update site settings JSON */
export const saveWebsiteSettings = async (req, res) => {
  const ctx = req.context || {};
  if (!ctx.email) {
    return res.status(401).json({ ok: false, message: 'Sign in required for CMS admin.' });
  }
  const value = req.body?.value ?? req.body?.settings ?? req.body;
  const updatedAt = Date.now();
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureWebsiteCmsTables(pool);
    await pool
      .request()
      .input('key', sql.NVarChar, 'site')
      .input('body', sql.NVarChar(sql.MAX), JSON.stringify(value))
      .input('updated', sql.BigInt, updatedAt)
      .query(`
        MERGE dbo.website_settings AS tgt
        USING (SELECT @key AS setting_key) AS src ON tgt.setting_key = src.setting_key
        WHEN MATCHED THEN UPDATE SET value_json = @body, updated_at = @updated
        WHEN NOT MATCHED THEN INSERT (setting_key, value_json, updated_at) VALUES (@key, @body, @updated);
      `);
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save settings.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
