import sql from 'mssql';
import { buildSqlServerTlsOptions } from '../connections/hostUtils.js';

const defaultControlSqlServerConfig = () => {
  const env = process.env || {};
  const server = String(env.SQLSERVER_CONTROL_HOST || '172.31.11.96').trim();
  const options = buildSqlServerTlsOptions(server);
  options.encrypt = String(env.SQLSERVER_CONTROL_ENCRYPT || 'true').trim().toLowerCase() !== 'false';
  options.trustServerCertificate = String(env.SQLSERVER_CONTROL_TRUST_CERT || 'true').trim().toLowerCase() !== 'false';
  if (String(env.SQLSERVER_CONTROL_SERVER_NAME || '').trim()) {
    options.serverName = String(env.SQLSERVER_CONTROL_SERVER_NAME).trim();
  }
  return {
    user: String(env.SQLSERVER_CONTROL_USER || 'sa').trim(),
    password: String(env.SQLSERVER_CONTROL_PASSWORD || 'Tr@n$Form$34762186627#').trim(),
    server,
    port: Number(env.SQLSERVER_CONTROL_PORT || 1433) || 1433,
    database: String(env.SQLSERVER_CONTROL_DATABASE || 'xerocode').trim(),
    options,
    connectionTimeout: 20_000,
    requestTimeout: 20_000,
    pool: {
      max: Number(env.SQLSERVER_CONTROL_POOL_MAX || 10) || 10,
      min: 0,
      idleTimeoutMillis: 30_000,
    },
  };
};

/** @type {import('mssql').ConnectionPool | null} */
let controlPool = null;
/** @type {Promise<import('mssql').ConnectionPool> | null} */
let controlPoolConnecting = null;

const resetControlPool = () => {
  controlPool = null;
  controlPoolConnecting = null;
};

/**
 * Returns a shared control-database pool. Reuses connections across API requests instead of
 * opening/closing on every call (reduces timeouts and 502s under load).
 */
export const connectToControlSqlServer = async () => {
  if (controlPool?.connected) return controlPool;
  if (controlPoolConnecting) return controlPoolConnecting;

  controlPoolConnecting = (async () => {
    const pool = new sql.ConnectionPool(defaultControlSqlServerConfig());
    pool.on('error', (err) => {
      console.error('[control-sql] pool error', err);
      resetControlPool();
    });
    await pool.connect();
    controlPool = pool;
    return pool;
  })();

  try {
    return await controlPoolConnecting;
  } catch (error) {
    resetControlPool();
    throw error;
  } finally {
    controlPoolConnecting = null;
  }
};

/** Kept for call-site compatibility; the shared pool stays open for reuse. */
export const closeControlSqlServer = async (_pool) => {
  /* no-op */
};

export const shutdownControlSqlServer = async () => {
  const pool = controlPool;
  resetControlPool();
  if (!pool) return;
  try {
    await pool.close();
  } catch {
    /* ignore */
  }
};
