import crypto from 'node:crypto';
import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './controlDb/sqlserver.js';

const slugRegex = /^agt-[a-f0-9]{24}$/i;

const parsePayload = (raw) => {
  if (raw == null) return {};
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
};

const ensureAgentPublicWorkflowsTable = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.agent_public_workflows', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.agent_public_workflows (
        slug NVARCHAR(80) NOT NULL PRIMARY KEY,
        tenant_id INT NOT NULL,
        owner_user_id INT NOT NULL,
        title NVARCHAR(512) NOT NULL,
        workflow_json NVARCHAR(MAX) NOT NULL,
        created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(MILLISECOND, '1970-01-01', SYSUTCDATETIME()),
        updated_at BIGINT NOT NULL
      );
      CREATE INDEX idx_agent_public_workflows_owner ON dbo.agent_public_workflows (tenant_id, owner_user_id, updated_at DESC);
    END
  `);
};

function resolveDefaultEndNodeId(nodes) {
  if (!Array.isArray(nodes)) return null;
  const ends = nodes.filter((n) => n && typeof n === 'object' && String(n.type || '') === 'end');
  if (!ends.length) return null;
  return String(ends[0].id || '').trim() || null;
}

export const publishAgentWorkflow = async (req, res) => {
  let pool;
  try {
    const ctx = req.context || {};
    const tenantId = ctx.tenantId ?? null;
    const userId = ctx.userId ?? null;
    if (tenantId == null || userId == null) {
      return res.status(401).json({ ok: false, message: 'Sign in required to publish an agent.' });
    }

    const workflow = req.body?.workflow;
    if (!workflow || typeof workflow !== 'object' || Array.isArray(workflow)) {
      return res.status(400).json({ ok: false, message: 'Request body must include a workflow object.' });
    }

    const nodes = workflow.nodes;
    const edges = workflow.edges;
    if (!Array.isArray(nodes) || !Array.isArray(edges)) {
      return res.status(400).json({ ok: false, message: 'workflow.nodes and workflow.edges are required.' });
    }

    const defaultEndNodeId = resolveDefaultEndNodeId(nodes);
    if (!defaultEndNodeId) {
      return res.status(400).json({ ok: false, message: 'Add at least one End node before publishing.' });
    }

    const titleRaw = req.body?.title ?? workflow.workflowName;
    const title =
      typeof titleRaw === 'string' && titleRaw.trim() ? String(titleRaw).trim().slice(0, 500) : 'Agent workflow';

    const requestedSlug =
      typeof req.body?.slug === 'string' && slugRegex.test(req.body.slug.trim()) ? req.body.slug.trim() : '';

    const workflowToStore = {
      workflowName: String(workflow.workflowName || title).trim().slice(0, 500) || title,
      agentKind: workflow.agentKind === 'managerial' ? 'managerial' : 'standalone',
      description:
        typeof workflow.description === 'string' ? String(workflow.description).trim().slice(0, 4000) : undefined,
      nodes,
      edges,
      defaultEndNodeId:
        typeof workflow.defaultEndNodeId === 'string' && workflow.defaultEndNodeId.trim()
          ? workflow.defaultEndNodeId.trim()
          : defaultEndNodeId,
    };

    const workflowStr = JSON.stringify(workflowToStore);
    const now = Date.now();

    pool = await connectToControlSqlServer();
    await ensureAgentPublicWorkflowsTable(pool);

    if (requestedSlug) {
      const owned = await pool
        .request()
        .input('slug', sql.NVarChar, requestedSlug)
        .input('tenantId', sql.Int, Number(tenantId))
        .input('ownerUserId', sql.Int, Number(userId))
        .query(`
          SELECT TOP 1 slug FROM dbo.agent_public_workflows
          WHERE slug = @slug AND tenant_id = @tenantId AND owner_user_id = @ownerUserId
        `);
      if (owned.recordset?.length) {
        await pool
          .request()
          .input('slug', sql.NVarChar, requestedSlug)
          .input('tenantId', sql.Int, Number(tenantId))
          .input('ownerUserId', sql.Int, Number(userId))
          .input('title', sql.NVarChar, title)
          .input('workflowJson', sql.NVarChar(sql.MAX), workflowStr)
          .input('updatedAt', sql.BigInt, now)
          .query(`
            UPDATE dbo.agent_public_workflows
            SET title = @title, workflow_json = @workflowJson, updated_at = @updatedAt
            WHERE slug = @slug AND tenant_id = @tenantId AND owner_user_id = @ownerUserId
          `);
        return res.json({ ok: true, slug: requestedSlug, title, updatedAt: now });
      }
    }

    const slug = requestedSlug || `agt-${crypto.randomBytes(12).toString('hex')}`;
    await pool
      .request()
      .input('slug', sql.NVarChar, slug)
      .input('tenantId', sql.Int, Number(tenantId))
      .input('ownerUserId', sql.Int, Number(userId))
      .input('title', sql.NVarChar, title)
      .input('workflowJson', sql.NVarChar(sql.MAX), workflowStr)
      .input('updatedAt', sql.BigInt, now)
      .query(`
        INSERT INTO dbo.agent_public_workflows (slug, tenant_id, owner_user_id, title, workflow_json, updated_at)
        VALUES (@slug, @tenantId, @ownerUserId, @title, @workflowJson, @updatedAt)
      `);

    return res.json({ ok: true, slug, title, updatedAt: now });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to publish agent.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getPublicAgentWorkflow = async (req, res) => {
  let pool;
  try {
    const slug = String(req.params?.slug || '').trim();
    if (!slug || !slugRegex.test(slug)) {
      return res.status(400).json({ ok: false, message: 'Invalid agent slug.' });
    }

    pool = await connectToControlSqlServer();
    await ensureAgentPublicWorkflowsTable(pool);
    const result = await pool.request().input('slug', sql.NVarChar, slug).query(`
      SELECT TOP 1 slug, title, workflow_json, updated_at
      FROM dbo.agent_public_workflows
      WHERE slug = @slug
    `);
    const row = result.recordset?.[0];
    if (!row) {
      return res.status(404).json({ ok: false, message: 'Published agent not found.' });
    }

    const workflow = parsePayload(row.workflow_json);
    return res.json({
      ok: true,
      agent: {
        slug: String(row.slug),
        title: String(row.title || ''),
        updatedAt: Number(row.updated_at || Date.now()),
        workflow,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load published agent.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};
