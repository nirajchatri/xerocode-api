import { Router } from 'express';
import {
  testMySqlConnection,
} from './connections/mysql.js';
import {
  getOrCreateUserAndTenantByEmail,
  getUserProfile,
  loginUser,
  loginWithGithub,
  loginWithGoogle,
  requestPasswordResetOtp,
  resetPasswordWithOtp,
  saveUserProfile,
  signupUser,
} from './controlDb/sqlserverAuth.js';
import {
  deleteAppRecord,
  deleteBlueprintApiRecord,
  deleteDashboardRecord,
  deleteAgentStudioRecord,
  deleteExternalApiRecord,
  getPublicDesignStudioPreview,
  getWorkspaceGuardrailsCatalog,
  saveWorkspaceGuardrailsCatalog,
  getPublicAppRecord,
  getPublicAppTableData,
  listSavedApps,
  listSavedBlueprintApis,
  listSavedDashboards,
  listSavedAgents,
  listSavedExternalApis,
  saveAppRecord,
  saveBlueprintApiRecord,
  saveDashboardRecord,
  saveAgentStudioRecord,
  publishDesignStudioPreview,
  saveExternalApiRecord,
} from './controlDb/sqlserverAppData.js';
import {
  deleteConnectionProfile,
  getMySqlTableData,
  getLlmConfigs,
  listConnectionProfiles,
  listAllWorkspaceConnectionProfiles,
  listMySqlConnectionTables,
  saveConnectionProfile,
  saveLlmConfig,
  updateConnectionProfile,
  issuePublicApiBearerToken,
  getStoredPublicApiBearerToken,
} from './controlDb/sqlserverConnections.js';
import { testDataSourceConnection } from './connections/testDispatch.js';
import {
  getConnectionTableData,
  getConnectionFkLookup,
  getConnectionTableForeignKeys,
  listConnectionTables,
  mutateConnectionTableData,
} from './connections/schemaRoutes.js';
import { chatWithLlm, testLlmConfig } from './llm/chat.js';
import {
  applyPublicApiJwtBearer,
  enforcePublicApiJwtConnectionScope,
  restrictPublicApiJwtRoutes,
} from './middleware/publicApiJwtAuth.js';
import {
  getBlueprintSlugSingleRecord,
  getBlueprintSlugTableData,
  listPublishedBlueprintRoutes,
  postBlueprintSlugMutate,
  syncApiBuilderSlugRoutes,
} from './apiBuilderSlugRoutes.js';
import { postAgentStudioWorkflowRun } from './agentStudioWorkflow.js';
import { postAgentStudioMcpTest } from './agentStudioMcpTest.js';

export const apiRouter = Router();

apiRouter.use(async (req, _res, next) => {
  const emailHeader = req.headers['x-user-email'] || req.headers['x-user-email'.toLowerCase()];
  const nameHeader = req.headers['x-user-name'] || req.headers['x-user-name'.toLowerCase()];
  const email = String(emailHeader || '').trim().toLowerCase();
  const fullName = String(nameHeader || '').trim();

  if (!email) {
    req.context = { userId: null, tenantId: null, email: null, fullName: null };
    return next();
  }
  const effectiveEmail = email;
  const effectiveName = fullName || email;

  try {
    const { userId, tenantId } = await getOrCreateUserAndTenantByEmail(effectiveEmail, effectiveName);
    req.context = { userId, tenantId, email: effectiveEmail, fullName: effectiveName };
  } catch (err) {
    // Best-effort: log and continue without context rather than failing all requests.
    console.error('Failed to resolve user/tenant context:', err);
    req.context = { userId: null, tenantId: null, email: effectiveEmail, fullName: effectiveName };
  }

  return next();
});

apiRouter.use(applyPublicApiJwtBearer);
apiRouter.use(restrictPublicApiJwtRoutes);

apiRouter.get('/health', (_req, res) => {
  res.json({ ok: true });
});

apiRouter.post('/agent-studio/workflow/run', postAgentStudioWorkflowRun);
apiRouter.post('/agent-studio/mcp/test', postAgentStudioMcpTest);

apiRouter.get('/profile', getUserProfile);
apiRouter.put('/profile', saveUserProfile);
apiRouter.post('/auth/signup', signupUser);
apiRouter.post('/auth/login', loginUser);
apiRouter.post('/auth/google', loginWithGoogle);
apiRouter.post('/auth/github', loginWithGithub);
apiRouter.post('/auth/forgot-password/request-otp', requestPasswordResetOtp);
apiRouter.post('/auth/forgot-password/reset', resetPasswordWithOtp);
apiRouter.post('/llm/chat', chatWithLlm);
apiRouter.get('/llm-config', getLlmConfigs);
apiRouter.put('/llm-config', saveLlmConfig);
apiRouter.post('/llm-config/test', testLlmConfig);
apiRouter.get('/apps', listSavedApps);
apiRouter.post('/design-studio/preview', publishDesignStudioPreview);
apiRouter.get('/public/design-studio/:slug', getPublicDesignStudioPreview);
apiRouter.get('/public/apps/:id', getPublicAppRecord);
apiRouter.get('/public/apps/:id/table-data', getPublicAppTableData);
apiRouter.post('/apps', saveAppRecord);
apiRouter.delete('/apps/:id', deleteAppRecord);
apiRouter.get('/dashboards', listSavedDashboards);
apiRouter.post('/dashboards', saveDashboardRecord);
apiRouter.delete('/dashboards/:id', deleteDashboardRecord);
apiRouter.get('/agents', listSavedAgents);
apiRouter.post('/agents', saveAgentStudioRecord);
apiRouter.delete('/agents/:id', deleteAgentStudioRecord);

/** Workspace user data in control DB (`xerocode`) — scoped by signed-in user (`x-user-email`). */
apiRouter.get('/workspace/guardrails-catalog', getWorkspaceGuardrailsCatalog);
apiRouter.put('/workspace/guardrails-catalog', saveWorkspaceGuardrailsCatalog);

apiRouter.get('/workspace/blueprint-apis', listSavedBlueprintApis);
apiRouter.post('/workspace/blueprint-apis', saveBlueprintApiRecord);
apiRouter.delete('/workspace/blueprint-apis/:id', deleteBlueprintApiRecord);
apiRouter.get('/workspace/external-apis', listSavedExternalApis);
apiRouter.post('/workspace/external-apis', saveExternalApiRecord);
apiRouter.delete('/workspace/external-apis/:id', deleteExternalApiRecord);

apiRouter.get('/connections/list', listConnectionProfiles);
apiRouter.get('/connections/list-all', listAllWorkspaceConnectionProfiles);
apiRouter.get('/connections/mysql/list', (req, res) => {
  req.query.connector = 'mysql';
  return listConnectionProfiles(req, res);
});

apiRouter.post('/connections/test', testDataSourceConnection);
apiRouter.post('/connections/mysql/test', testMySqlConnection);

/** All connector profiles are written to the configured control DB. */
apiRouter.post('/connections/save', saveConnectionProfile);
apiRouter.post('/connections/mysql/save', saveConnectionProfile);

apiRouter.put('/connections/:id', updateConnectionProfile);
apiRouter.delete('/connections/:id', deleteConnectionProfile);
apiRouter.put('/connections/mysql/:id', updateConnectionProfile);
apiRouter.delete('/connections/mysql/:id', deleteConnectionProfile);

apiRouter.get('/connections/mysql/:id/table-data', enforcePublicApiJwtConnectionScope, getMySqlTableData);
apiRouter.get('/connections/mysql/:id/tables', enforcePublicApiJwtConnectionScope, listMySqlConnectionTables);

apiRouter.get('/connections/:id/tables', enforcePublicApiJwtConnectionScope, listConnectionTables);
apiRouter.get('/connections/:id/table-data', enforcePublicApiJwtConnectionScope, getConnectionTableData);
apiRouter.get('/connections/:id/table-foreign-keys', enforcePublicApiJwtConnectionScope, getConnectionTableForeignKeys);
apiRouter.get('/connections/:id/fk-lookup', enforcePublicApiJwtConnectionScope, getConnectionFkLookup);
apiRouter.post('/connections/:id/table-data/mutate', enforcePublicApiJwtConnectionScope, mutateConnectionTableData);

/** Signed Bearer JWT for external API docs / Postman (scoped to tenant + saved connection profile). */
apiRouter.post('/public-api-token/issue', issuePublicApiBearerToken);
apiRouter.get('/workspace/public-api-token', getStoredPublicApiBearerToken);

apiRouter.post('/api-builder/sync-slugs', syncApiBuilderSlugRoutes);
apiRouter.get('/api-builder/published-routes', listPublishedBlueprintRoutes);

apiRouter.post('/:slug', postBlueprintSlugMutate);
/** Blueprint GET by published slug — two-param route must register before `/:slug`. */
apiRouter.get('/:slug/:recordKey', getBlueprintSlugSingleRecord);
/** Single-segment blueprint GET (e.g. /api/icl-invoice) — must stay last among slug routes. */
apiRouter.get('/:slug', getBlueprintSlugTableData);
