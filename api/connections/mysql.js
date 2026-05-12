import mysql from 'mysql2/promise';
import { hostFallbacks, normalizeHost } from './hostUtils.js';

export const testMySqlConnection = async (req, res) => {
  const { host, port: dbPort, database, username, password } = req.body ?? {};
  const sanitizedHost = normalizeHost(host);
  const directHost = hostFallbacks[sanitizedHost] || sanitizedHost;
  const rawPort = dbPort && String(dbPort).trim().length > 0 ? dbPort : 3306;
  const portNum = Number(rawPort);

  if (!sanitizedHost || !Number.isFinite(portNum) || portNum <= 0 || !database || !username || !password) {
    return res.status(400).json({
      ok: false,
      message: 'Missing required fields: host, port, database, username, password.',
    });
  }

  let connection;
  try {
    try {
      connection = await mysql.createConnection({
        host: directHost,
        port: portNum,
        database: String(database),
        user: String(username),
        password: String(password),
      });
    } catch (connectError) {
      const fallbackHost = hostFallbacks[sanitizedHost];
      const shouldRetryWithFallback =
        fallbackHost &&
        connectError &&
        typeof connectError === 'object' &&
        'code' in connectError &&
        connectError.code === 'ENOTFOUND';

      if (!shouldRetryWithFallback) {
        throw connectError;
      }

      connection = await mysql.createConnection({
        host: fallbackHost,
        port: portNum,
        database: String(database),
        user: String(username),
        password: String(password),
      });
    }

    return res.json({
      ok: true,
      message: 'Successfully connected to MySQL data source.',
    });
  } catch (error) {
    const err = error instanceof Error ? error : new Error('Unable to connect to MySQL data source.');
    return res.status(500).json({ ok: false, message: err.message });
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};

