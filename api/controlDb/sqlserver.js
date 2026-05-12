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
  };
};

export const connectToControlSqlServer = async () => {
  const cfg = defaultControlSqlServerConfig();
  const pool = new sql.ConnectionPool(cfg);
  await pool.connect();
  return pool;
};

export const closeControlSqlServer = async (pool) => {
  if (!pool) return;
  try {
    await pool.close();
  } catch {
    /* ignore */
  }
};

