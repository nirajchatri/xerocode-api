import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { apiRouter } from '../api/index.js';
import { bootstrapControlDbTables } from '../api/controlDb/sqlserverConnections.js';
import { listReachableApiUrls, resolveLocalListenConfig } from '../config/apiEndpoints.js';

dotenv.config({ path: '.env' });
dotenv.config({ path: '.env.local', override: true });

const app = express();
const { host, port } = resolveLocalListenConfig();

app.use(cors());
/** Default express.json() limit (~100kb) rejects multimodal chat payloads (base64 images). */
app.use(express.json({ limit: '40mb' }));
app.use(express.urlencoded({ extended: true, limit: '40mb' }));
app.use('/api', apiRouter);
app.use((err, _req, res, _next) => {
  const message = err instanceof Error ? err.message : 'Internal server error';
  console.error('Unhandled API error:', err);
  if (res.headersSent) {
    return;
  }
  res.status(500).json({ ok: false, message });
});

const bootstrapControlDb = async () => {
  try {
    await bootstrapControlDbTables();
    console.log('Control DB bootstrap complete (connection_profiles, user_profile).');
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Control DB bootstrap failed: ${message}`);
  }
};

const server = app.listen(port, host, () => {
  const urls = listReachableApiUrls(host, port);
  console.log(`API server running at ${urls[0]}`);
  for (const url of urls.slice(1)) {
    console.log(`  Also reachable at ${url}`);
  }
  void bootstrapControlDb();
});

server.on('error', (error) => {
  if (error && typeof error === 'object' && 'code' in error && error.code === 'EADDRINUSE') {
    console.error(`Port ${port} is already in use. Stop the other process and start the API again.`);
    process.exit(1);
  }
  throw error;
});
