import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const configDir = path.dirname(fileURLToPath(import.meta.url));

const defaultApiEndpointsConfig = () => ({
  local: {
    bindHost: '127.0.0.1',
    host: '127.0.0.1',
    port: 8787,
    protocol: 'http',
  },
  production: {
    bindHost: '0.0.0.0',
    port: 8787,
    baseUrl: 'https://apis.xerocode.ai',
  },
});

const resolveApiEndpointsConfigPath = () => {
  const candidates = [
    process.env.XEROCODE_API_ENDPOINTS_CONFIG,
    path.join(configDir, 'api-endpoints.json'),
    path.resolve(configDir, '../config/api-endpoints.json'),
    path.resolve(configDir, '../../config/api-endpoints.json'),
  ]
    .filter(Boolean)
    .map((candidate) => path.resolve(String(candidate)));

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
};

let cachedConfig;
let cachedConfigPath;

export function apiEndpointsConfigPath() {
  if (cachedConfigPath === undefined) {
    cachedConfigPath = resolveApiEndpointsConfigPath();
  }
  return cachedConfigPath;
}

export function loadApiEndpointsConfig() {
  if (cachedConfig) {
    return cachedConfig;
  }

  const configPath = apiEndpointsConfigPath();
  if (!configPath) {
    cachedConfig = defaultApiEndpointsConfig();
    return cachedConfig;
  }

  try {
    cachedConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  } catch {
    cachedConfig = defaultApiEndpointsConfig();
  }
  return cachedConfig;
}

function normalizePort(value, fallback) {
  const port = Number(value);
  return Number.isFinite(port) && port > 0 ? port : fallback;
}

function originFromParts(protocol, host, port) {
  const scheme = String(protocol || 'http').trim() || 'http';
  const hostname = String(host || '127.0.0.1').trim() || '127.0.0.1';
  const listenPort = normalizePort(port, 8787);
  const defaultPort = scheme === 'https' ? 443 : 80;
  const portSuffix = listenPort === defaultPort ? '' : `:${listenPort}`;
  return `${scheme}://${hostname}${portSuffix}`;
}

export function resolveLocalListenConfig(env = process.env) {
  const cfg = loadApiEndpointsConfig();
  const local = cfg.local ?? {};
  const production = cfg.production ?? {};
  const isProduction = String(env.NODE_ENV || '').trim().toLowerCase() === 'production';
  const port = normalizePort(
    env.PORT ?? env.API_PORT ?? (isProduction ? production.port : undefined) ?? local.port,
    8787
  );
  const defaultBindHost = isProduction
    ? (production.bindHost ?? production.host ?? local.bindHost ?? '0.0.0.0')
    : (local.bindHost ?? local.host ?? '127.0.0.1');
  const host =
    String(env.API_HOST ?? env.HOST ?? defaultBindHost).trim() ||
    (isProduction ? '0.0.0.0' : '127.0.0.1');

  return { host, port };
}

export function resolveLocalApiOrigin(env = process.env) {
  const local = loadApiEndpointsConfig().local ?? {};
  const { host, port } = resolveLocalListenConfig(env);
  const configuredOrigin = String(env.VITE_API_PROXY_TARGET ?? '').trim();
  if (configuredOrigin) {
    return configuredOrigin.replace(/\/$/, '');
  }

  const protocol = String(local.protocol ?? 'http').trim() || 'http';
  const publicHost = String(local.host ?? host).trim() || host;
  return originFromParts(protocol, publicHost, port);
}

export function resolveProductionApiBaseUrl(env = process.env) {
  const fromEnv = String(env.VITE_API_BASE_URL ?? env.API_PUBLIC_BASE_URL ?? '').trim();
  if (fromEnv) {
    return fromEnv.replace(/\/$/, '');
  }

  const production = loadApiEndpointsConfig().production ?? {};
  if (production.sameOriginApi === true) {
    return undefined;
  }
  if (typeof production.baseUrl === 'string' && production.baseUrl.trim()) {
    return production.baseUrl.trim().replace(/\/$/, '');
  }

  const host = String(production.host ?? '').trim();
  if (!host) {
    return undefined;
  }

  if (/^https?:\/\//i.test(host)) {
    try {
      return new URL(host).origin.replace(/\/$/, '');
    } catch {
      return undefined;
    }
  }

  return originFromParts(production.protocol, host, production.port);
}

export function listReachableApiUrls(bindHost, listenPort) {
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
}
