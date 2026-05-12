import { isIP } from 'node:net';

export const hostFallbacks = {};

export const normalizeHost = (rawHost) => {
  if (!rawHost || typeof rawHost !== 'string') {
    return '';
  }

  let normalized = rawHost.trim();

  if (normalized.startsWith('http://') || normalized.startsWith('https://')) {
    try {
      normalized = new URL(normalized).hostname;
    } catch {
      normalized = normalized.replace(/^https?:\/\//, '');
    }
  }

  normalized = normalized.split('/')[0].split('?')[0].split('#')[0];
  normalized = normalized.replace(/:\d+$/, '');
  normalized = normalized.toLowerCase().replace(/\.$/, '');

  return normalized;
};

/**
 * Store SQL Server address as entered (trim, strip tcp:), without hostname mangling.
 */
export const sqlServerHostForStorage = (rawHost) => {
  let s = String(rawHost ?? '').trim();
  if (!s) {
    return '';
  }
  return s.replace(/^tcp:/i, '').trim();
};

/**
 * Parse SQL Server host field for Tedious/mssql:
 * - tcp:server,port or server,port
 * - SERVER\INSTANCE (named instance; instance name is case-insensitive, preserved)
 * - Plain hostname or IP (optional port from second arg)
 */
export const parseSqlServerServerInput = (rawHost, formPort) => {
  let s = String(rawHost ?? '').trim().replace(/^tcp:/i, '').trim();
  if (!s) {
    return { server: '', instanceName: undefined, port: null };
  }

  let portFromComma = null;
  const commaIdx = s.lastIndexOf(',');
  if (commaIdx > 0) {
    const tail = s.slice(commaIdx + 1).trim();
    if (/^\d+$/.test(tail)) {
      portFromComma = Number(tail);
      s = s.slice(0, commaIdx).trim();
    }
  }

  let instanceName;
  const bs = s.indexOf('\\');
  if (bs >= 0) {
    instanceName = s.slice(bs + 1).trim() || undefined;
    s = s.slice(0, bs).trim();
  }

  const isIpv4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(s);
  if (!isIpv4) {
    s = s.toLowerCase().replace(/\.$/, '');
  }

  let port =
    portFromComma && Number.isFinite(portFromComma) && portFromComma > 0 ? portFromComma : null;
  if (!port && formPort !== undefined && formPort !== null && String(formPort).trim() !== '') {
    const n = Number(formPort);
    if (Number.isFinite(n) && n > 0) {
      port = n;
    }
  }

  return { server: s, instanceName, port };
};

export const buildSqlServerTlsOptions = (server, instanceName) => {
  const options = {
    encrypt: true,
    trustServerCertificate: true,
    ...(instanceName ? { instanceName } : {}),
  };
  if (isIP(String(server || '').trim())) {
    options.serverName = String(process.env.SQLSERVER_TLS_SERVER_NAME || 'localhost').trim();
  }
  return options;
};
