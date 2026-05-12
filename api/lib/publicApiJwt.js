import crypto from 'crypto';

export const getPublicApiJwtSecret = () =>
  String(process.env.PUBLIC_API_JWT_SECRET || process.env.XEROCODE_PUBLIC_API_SECRET || '').trim() ||
  'dev-public-api-jwt-secret-change-me';

const base64UrlEncodeJson = (obj) =>
  Buffer.from(JSON.stringify(obj), 'utf8')
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');

const base64UrlDecodeToString = (b64url) => {
  let s = String(b64url || '').replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4) s += '=';
  return Buffer.from(s, 'base64').toString('utf8');
};

/** HS256 JWT used for datasource-scoped Bearer tokens (issued by POST /public-api-token/issue). */
export const signPublicApiJwt = (payload, secret = getPublicApiJwtSecret()) => {
  const header = { alg: 'HS256', typ: 'JWT' };
  const encodedHeader = base64UrlEncodeJson(header);
  const encodedPayload = base64UrlEncodeJson(payload);
  const data = `${encodedHeader}.${encodedPayload}`;
  const sig = crypto
    .createHmac('sha256', secret)
    .update(data)
    .digest('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
  return `${data}.${sig}`;
};

export const verifyPublicApiJwt = (token, secret = getPublicApiJwtSecret()) => {
  try {
    const parts = String(token || '').split('.');
    if (parts.length !== 3) return { ok: false, error: 'Malformed token.' };
    const [h, p, s] = parts;
    const data = `${h}.${p}`;
    const expected = crypto
      .createHmac('sha256', secret)
      .update(data)
      .digest('base64')
      .replace(/=/g, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');

    const sigBuf = Buffer.from(String(s), 'utf8');
    const expBuf = Buffer.from(expected, 'utf8');
    if (sigBuf.length !== expBuf.length) return { ok: false, error: 'Invalid signature.' };
    if (!crypto.timingSafeEqual(sigBuf, expBuf)) return { ok: false, error: 'Invalid signature.' };

    const header = JSON.parse(base64UrlDecodeToString(h));
    if (header.alg !== 'HS256') return { ok: false, error: 'Unsupported algorithm.' };

    const payload = JSON.parse(base64UrlDecodeToString(p));
    const now = Math.floor(Date.now() / 1000);
    if (payload.exp != null && Number(payload.exp) < now) {
      return { ok: false, error: 'Token expired.' };
    }
    return { ok: true, payload };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : 'Invalid token.' };
  }
};
