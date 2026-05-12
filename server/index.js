import os from 'node:os';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { apiRouter } from '../api/index.js';
import { bootstrapControlDbTables } from '../api/controlDb/sqlserverConnections.js';

dotenv.config({ path: '.env' });
dotenv.config({ path: '.env.local', override: true });

const app = express();
const port = Number(process.env.PORT || process.env.API_PORT || 8787);
const host = String(process.env.API_HOST || process.env.HOST || '127.0.0.1').trim() || '127.0.0.1';

const listReachableApiUrls = (bindHost, listenPort) => {
  const urls = new Set();
  if (bindHost && bindHost !== '0.0.0.0') {
    urls.add(`http://${bindHost}:${listenPort}`);
    return [...urls];
  }

  urls.add(`http://127.0.0.1:${listenPort}`);
  urls.add(`http://localhost:${listenPort}`);
  for (const iface of Object.values(os.networkInterfaces())) {
    if (!iface) continue;
    for (const addr of iface) {
      const family = typeof addr.family === 'string' ? addr.family : `IPv${addr.family}`;
      if (family === 'IPv4' && !addr.internal) {
        urls.add(`http://${addr.address}:${listenPort}`);
      }
    }
  }
  return [...urls];
};

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
