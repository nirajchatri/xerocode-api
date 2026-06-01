/** Platform operator who may manage users across all tenants when also a tenant admin. */
export const PLATFORM_SUPER_ADMIN_EMAIL = 'niraj@xerocode.ai';

export function normalizeAdminEmail(email) {
  return String(email || '').trim().toLowerCase();
}

export function isPlatformSuperAdminEmail(email) {
  return normalizeAdminEmail(email) === PLATFORM_SUPER_ADMIN_EMAIL;
}

/** True when signed-in user is tenant admin and the platform super-admin email. */
export function isPlatformSuperAdminSession(email, isTenantAdmin) {
  return Boolean(isTenantAdmin) && isPlatformSuperAdminEmail(email);
}
