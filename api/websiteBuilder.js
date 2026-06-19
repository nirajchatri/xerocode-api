import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';
import { ensureSavedWorkspaceTables } from './controlDb/sqlserverAppData.js';

const appRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
export const WEBSITES_FILES_ROOT = path.join(appRoot, 'generated-websites');

const parseJson = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

export function slugifyWebsiteFolder(name) {
  const base =
    String(name || 'website')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 64) || 'website';
  return base;
}

function defaultScaffoldFiles(siteName) {
  const title = String(siteName || 'Website').trim() || 'Website';
  return [
    {
      path: 'index.html',
      content: `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${title}</title>
    <link rel="stylesheet" href="css/styles.css" />
  </head>
  <body>
    <main class="placeholder">
      <h1>${title}</h1>
      <p>Your website will be generated here by the Website Builder agent.</p>
    </main>
  </body>
</html>
`,
    },
    {
      path: 'css/styles.css',
      content: `* { box-sizing: border-box; }
body { margin: 0; font-family: system-ui, -apple-system, sans-serif; }
.placeholder { padding: 3rem 1.5rem; max-width: 960px; margin: 0 auto; }
`,
    },
  ];
}

async function writeDesignAssetFiles(root, designImages) {
  const written = [];
  for (const img of Array.isArray(designImages) ? designImages : []) {
    const rel = String(img?.assetPath || img?.path || '').replace(/^\/+/, '');
    const dataBase64 = String(img?.dataBase64 || img?.content || '');
    if (!rel || !dataBase64 || rel.includes('..')) continue;
    const full = path.join(root, rel);
    await fs.mkdir(path.dirname(full), { recursive: true });
    await fs.writeFile(full, Buffer.from(dataBase64, 'base64'));
    written.push(rel);
  }
  return written;
}

export async function writeWebsiteProjectFiles(folderName, files) {
  const safeFolder = String(folderName || '').replace(/[^a-zA-Z0-9._-]/g, '-');
  if (!safeFolder) throw new Error('Invalid website folder name.');
  const root = path.join(WEBSITES_FILES_ROOT, safeFolder);
  await fs.mkdir(root, { recursive: true });
  const written = [];
  for (const file of Array.isArray(files) ? files : []) {
    const rel = String(file?.path || '').replace(/^\/+/, '');
    if (!rel || rel.includes('..')) continue;
    const full = path.join(root, rel);
    await fs.mkdir(path.dirname(full), { recursive: true });
    if (file?.encoding === 'base64') {
      await fs.writeFile(full, Buffer.from(String(file.content ?? ''), 'base64'));
    } else {
      await fs.writeFile(full, String(file.content ?? ''), 'utf8');
    }
    written.push(rel);
  }
  return { folder: safeFolder, files: written };
}

export async function writeWebsiteReactFiles(folderName, files) {
  return writeWebsiteProjectFiles(folderName, files);
}

async function scaffoldWebsiteFolder(folderName, siteName) {
  const root = path.join(WEBSITES_FILES_ROOT, folderName);
  await fs.mkdir(path.join(root, 'assets'), { recursive: true });
  return writeWebsiteProjectFiles(folderName, defaultScaffoldFiles(siteName));
}

async function resolveUniqueFolderName(pool, tenantId, siteName) {
  const base = slugifyWebsiteFolder(siteName);
  let candidate = base;
  let suffix = 2;
  while (true) {
    const result = await pool
      .request()
      .input('folderName', sql.NVarChar, candidate)
      .input('tenantId', sql.Int, Number(tenantId))
      .query(`
        SELECT TOP 1 id FROM dbo.saved_website_projects
        WHERE tenant_id = @tenantId AND folder_name = @folderName
      `);
    if (!result.recordset?.length) return candidate;
    candidate = `${base}-${suffix}`;
    suffix += 1;
  }
}

function rowToWebsite(row) {
  const payload = parseJson(row.payload);
  return {
    id: String(row.id),
    name: String(row.name || ''),
    description: String(row.description || ''),
    llmProvider: String(row.llm_provider || 'google'),
    llmModel: String(row.llm_model || ''),
    folderName: String(row.folder_name || ''),
    selectedTemplateId: row.selected_template_id ? String(row.selected_template_id) : null,
    pages: Array.isArray(payload.pages) ? payload.pages : [],
    activePageId: String(payload.activePageId || ''),
    agentMessages: Array.isArray(payload.agentMessages) ? payload.agentMessages : [],
    templateConfirmed: Boolean(payload.templateConfirmed),
    referenceWebsites: Array.isArray(payload.referenceWebsites) ? payload.referenceWebsites.map(String) : [],
    referenceWebsiteSnapshots: Array.isArray(payload.referenceWebsiteSnapshots)
      ? payload.referenceWebsiteSnapshots
      : [],
    designImages: Array.isArray(payload.designImages) ? payload.designImages : [],
    pageMode: payload.pageMode === 'multi_page' ? 'multi_page' : 'one_page',
    initialBuildDone: Boolean(payload.initialBuildDone),
    mockupLayoutSpec: payload.mockupLayoutSpec ?? undefined,
    reactFiles: Array.isArray(payload.reactFiles) ? payload.reactFiles : [],
    updatedAt: Number(row.updated_at || Date.now()),
    createdAt: Number(row.created_at || row.updated_at || Date.now()),
  };
}

export const listWebsiteProjects = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to load websites.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const result = await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .query(`
        SELECT id, name, description, llm_provider, llm_model, folder_name, selected_template_id, payload, updated_at, created_at
        FROM dbo.saved_website_projects
        WHERE tenant_id = @tenantId
        ORDER BY updated_at DESC
      `);
    const websites = (result.recordset || []).map(rowToWebsite);
    return res.json({ ok: true, websites });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to list websites.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveWebsiteProjectRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to save websites.' });
    }
    const body = req.body || {};
    const id = String(body.id || `web-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
    const name = String(body.name || '').trim();
    if (!name) return res.status(400).json({ ok: false, message: 'Website name is required.' });
    const description = String(body.description || '').trim();
    const llmProvider = String(body.llmProvider || 'google');
    const llmModel = String(body.llmModel || '');
    const selectedTemplateId = body.selectedTemplateId ? String(body.selectedTemplateId) : null;
    const updatedAt = Number(body.updatedAt) || Date.now();
    const payload = {
      pages: Array.isArray(body.pages) ? body.pages : [],
      activePageId: String(body.activePageId || ''),
      agentMessages: Array.isArray(body.agentMessages) ? body.agentMessages : [],
      templateConfirmed: Boolean(body.templateConfirmed),
      referenceWebsites: Array.isArray(body.referenceWebsites) ? body.referenceWebsites.map(String) : [],
      referenceWebsiteSnapshots: Array.isArray(body.referenceWebsiteSnapshots)
        ? body.referenceWebsiteSnapshots
        : [],
      designImages: Array.isArray(body.designImages) ? body.designImages : [],
      pageMode: body.pageMode === 'multi_page' ? 'multi_page' : 'one_page',
      initialBuildDone: Boolean(body.initialBuildDone),
      mockupLayoutSpec: body.mockupLayoutSpec ?? undefined,
      reactFiles: Array.isArray(body.reactFiles) ? body.reactFiles : [],
    };

    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const exists = await pool.request().input('id', sql.NVarChar, id).query(`
      SELECT TOP 1 folder_name FROM dbo.saved_website_projects WHERE id = @id
    `);
    let folderName = exists.recordset?.[0]?.folder_name
      ? String(exists.recordset[0].folder_name)
      : await resolveUniqueFolderName(pool, tenantId, name);

    if (!exists.recordset?.length) {
      await scaffoldWebsiteFolder(folderName, name);
    }

    const projectRoot = path.join(WEBSITES_FILES_ROOT, folderName);
    if (payload.designImages?.length) {
      await fs.mkdir(path.join(projectRoot, 'assets'), { recursive: true });
      await writeDesignAssetFiles(projectRoot, payload.designImages);
    }

    const reqDb = pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('name', sql.NVarChar, name)
      .input('description', sql.NVarChar(sql.MAX), description)
      .input('llmProvider', sql.NVarChar, llmProvider)
      .input('llmModel', sql.NVarChar, llmModel)
      .input('folderName', sql.NVarChar, folderName)
      .input('selectedTemplateId', sql.NVarChar, selectedTemplateId)
      .input('payload', sql.NVarChar(sql.MAX), JSON.stringify(payload))
      .input('updatedAt', sql.BigInt, updatedAt);

    if (exists.recordset?.length) {
      await reqDb.query(`
        UPDATE dbo.saved_website_projects
        SET name = @name, description = @description, llm_provider = @llmProvider, llm_model = @llmModel,
            selected_template_id = @selectedTemplateId, payload = @payload, updated_at = @updatedAt
        WHERE id = @id AND tenant_id = @tenantId
      `);
    } else {
      await reqDb.query(`
        INSERT INTO dbo.saved_website_projects
          (id, tenant_id, owner_user_id, name, description, llm_provider, llm_model, folder_name, selected_template_id, payload, updated_at, created_at)
        VALUES
          (@id, @tenantId, @ownerUserId, @name, @description, @llmProvider, @llmModel, @folderName, @selectedTemplateId, @payload, @updatedAt, @updatedAt)
      `);
    }

    return res.json({ ok: true, id, folderName, projectPath: projectRoot });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save website.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveWebsiteReactFilesRecord = async (req, res) => {
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    if (tenantId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required.' });
    }
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing website id.' });

    let pool;
    try {
      pool = await connectToControlSqlServer();
      await ensureSavedWorkspaceTables(pool);
      const row = await pool
        .request()
        .input('id', sql.NVarChar, id)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`
          SELECT folder_name FROM dbo.saved_website_projects
          WHERE id = @id AND tenant_id = @tenantId
        `);
      const folderName = row.recordset?.[0]?.folder_name;
      if (!folderName) {
        return res.status(404).json({ ok: false, message: 'Website not found.' });
      }
      const files = Array.isArray(req.body?.files) ? req.body.files : [];
      const result = await writeWebsiteProjectFiles(String(folderName), files);
      return res.json({ ok: true, ...result });
    } finally {
      await closeControlSqlServer(pool);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save React files.';
    return res.status(500).json({ ok: false, message });
  }
};

export const saveWebsiteHtmlProjectRecord = async (req, res) => {
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    if (tenantId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required.' });
    }
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing website id.' });

    let pool;
    try {
      pool = await connectToControlSqlServer();
      await ensureSavedWorkspaceTables(pool);
      const row = await pool
        .request()
        .input('id', sql.NVarChar, id)
        .input('tenantId', sql.Int, Number(tenantId))
        .query(`
          SELECT folder_name FROM dbo.saved_website_projects
          WHERE id = @id AND tenant_id = @tenantId
        `);
      const folderName = row.recordset?.[0]?.folder_name;
      if (!folderName) {
        return res.status(404).json({ ok: false, message: 'Website not found.' });
      }
      const files = Array.isArray(req.body?.files) ? req.body.files : [];
      const designImages = Array.isArray(req.body?.designImages) ? req.body.designImages : [];
      const projectRoot = path.join(WEBSITES_FILES_ROOT, String(folderName));
      await fs.mkdir(path.join(projectRoot, 'assets'), { recursive: true });
      if (designImages.length) {
        await writeDesignAssetFiles(projectRoot, designImages);
      }
      const result = await writeWebsiteProjectFiles(String(folderName), files);
      return res.json({
        ok: true,
        ...result,
        projectPath: projectRoot,
      });
    } finally {
      await closeControlSqlServer(pool);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save HTML project.';
    return res.status(500).json({ ok: false, message });
  }
};

export const deleteWebsiteProjectRecord = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const id = String(req.params?.id || '');
    if (!id) return res.status(400).json({ ok: false, message: 'Missing website id.' });
    if (tenantId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required.' });
    }
    pool = await connectToControlSqlServer();
    await ensureSavedWorkspaceTables(pool);
    const row = await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .query(`SELECT folder_name FROM dbo.saved_website_projects WHERE id = @id AND tenant_id = @tenantId`);
    const folderName = row.recordset?.[0]?.folder_name;
    await pool
      .request()
      .input('id', sql.NVarChar, id)
      .input('tenantId', sql.Int, Number(tenantId))
      .query(`DELETE FROM dbo.saved_website_projects WHERE id = @id AND tenant_id = @tenantId`);
    if (folderName) {
      const dir = path.join(WEBSITES_FILES_ROOT, String(folderName));
      await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
    }
    return res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to delete website.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

function stripHtmlToText(html) {
  return String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractHeadings(html) {
  const headings = [];
  const re = /<h[1-3][^>]*>([\s\S]*?)<\/h[1-3]>/gi;
  let m;
  while ((m = re.exec(html)) && headings.length < 12) {
    const text = stripHtmlToText(m[1]);
    if (text) headings.push(text.slice(0, 120));
  }
  return headings;
}

async function fetchReferenceSnapshot(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12000);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'XeroCode-WebsiteBuilder/1.0',
        Accept: 'text/html,application/xhtml+xml',
      },
      redirect: 'follow',
    });
    if (!res.ok) {
      return { url, title: '', description: '', headings: [], textSnippet: `Fetch failed: HTTP ${res.status}` };
    }
    const html = (await res.text()).slice(0, 200_000);
    const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
    const descMatch = html.match(
      /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
    );
    const title = stripHtmlToText(titleMatch?.[1] || '');
    const description = String(descMatch?.[1] || '').trim();
    const headings = extractHeadings(html);
    const textSnippet = stripHtmlToText(html).slice(0, 1500);
    return { url, title, description, headings, textSnippet };
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Fetch failed';
    return { url, title: '', description: '', headings: [], textSnippet: msg };
  } finally {
    clearTimeout(timer);
  }
}

export const resolveWebsiteReferences = async (req, res) => {
  try {
    const urls = Array.isArray(req.body?.urls) ? req.body.urls.map(String) : [];
    const clean = urls
      .map((u) => u.trim())
      .filter((u) => /^https?:\/\//i.test(u))
      .slice(0, 8);
    const snapshots = [];
    for (const url of clean) {
      snapshots.push(await fetchReferenceSnapshot(url));
    }
    return res.json({ ok: true, snapshots });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to resolve reference websites.';
    return res.status(500).json({ ok: false, message });
  }
};
