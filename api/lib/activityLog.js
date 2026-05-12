import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from '../controlDb/sqlserver.js';

const safePayload = (payload) => {
  if (payload == null) return null;
  try {
    const raw = JSON.stringify(payload);
    if (raw.length > 12000) {
      return { truncated: true, preview: raw.slice(0, 12000) };
    }
    return JSON.parse(raw);
  } catch {
    return { serializationError: true };
  }
};

export const logActivity = async ({
  tenantId,
  userId,
  entityType,
  entityId,
  action,
  payload,
  userAgent,
  ipAddress,
}) => {
  if (!tenantId || !entityType || !entityId || !action) return;

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await pool
      .request()
      .input('tenantId', sql.Int, Number(tenantId))
      .input('userId', sql.Int, userId != null ? Number(userId) : null)
      .input('entityType', sql.NVarChar, String(entityType))
      .input('entityId', sql.NVarChar, String(entityId))
      .input('action', sql.NVarChar, String(action))
      .input('payload', sql.NVarChar(sql.MAX), JSON.stringify(safePayload(payload)))
      .input('userAgent', sql.NVarChar(255), userAgent ? String(userAgent).slice(0, 255) : null)
      .input('ipAddress', sql.NVarChar(64), ipAddress ? String(ipAddress).slice(0, 64) : null).query(`
        INSERT INTO dbo.activity_log
          (tenant_id, user_id, entity_type, entity_id, action, payload, user_agent, ip_address, created_at)
        VALUES
          (@tenantId, @userId, @entityType, @entityId, @action, @payload, @userAgent, @ipAddress, SYSDATETIME())
      `);
  } catch {
    // Best-effort audit trail; do not fail primary operation.
  } finally {
    await closeControlSqlServer(pool);
  }
};
