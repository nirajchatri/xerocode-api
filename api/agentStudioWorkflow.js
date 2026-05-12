import { runWorkflowLocally } from './lib/workflowRunOrder.js';

/**
 * POST /api/agent-studio/workflow/run
 * Validates graph shape and returns deterministic execution order + node summaries.
 * Does not call external HTTP/LLM/MCP — reserved for a future orchestration engine.
 */
export function postAgentStudioWorkflowRun(req, res) {
  try {
    const { workflowName, nodes, edges } = req.body || {};
    if (!Array.isArray(nodes) || !Array.isArray(edges)) {
      return res.status(400).json({ ok: false, error: 'Expected `nodes` and `edges` arrays.' });
    }
    const out = runWorkflowLocally(workflowName, nodes, edges);
    if (!out.ok) {
      return res.status(422).json({ ok: false, error: out.error, steps: out.steps, workflowName: out.workflowName });
    }
    return res.json({
      ok: true,
      workflowName: out.workflowName,
      steps: out.steps,
      context: {
        email: req.context?.email || null,
        tenantId: req.context?.tenantId ?? null,
      },
    });
  } catch (err) {
    console.error('postAgentStudioWorkflowRun', err);
    return res.status(500).json({ ok: false, error: err?.message || 'Run failed.' });
  }
}
