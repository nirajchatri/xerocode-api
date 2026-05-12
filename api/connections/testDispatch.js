import sql from 'mssql';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import { normalizeConnectorType } from './connectorTypes.js';
import { buildSqlServerTlsOptions, normalizeHost, parseSqlServerServerInput } from './hostUtils.js';
import { testMySqlConnection } from './mysql.js';

const defaultPorts = {
  sqlserver: 1433,
  postgresql: 5432,
  mongodb: 27017,
};

const parsePort = (raw, fallback) => {
  const rawPort = raw !== undefined && raw !== null && String(raw).trim() !== '' ? raw : fallback;
  const portNum = Number(rawPort);
  return Number.isFinite(portNum) && portNum > 0 ? portNum : null;
};

export const testDataSourceConnection = async (req, res) => {
  const body = req.body ?? {};
  const type = normalizeConnectorType(body.connectorType);

  if (!type) {
    return res.status(400).json({
      ok: false,
      message: 'Invalid connector type. Use mysql, sqlserver, postgresql, or mongodb.',
    });
  }

  if (type === 'mysql') {
    return testMySqlConnection(req, res);
  }

  if (type === 'sqlserver') {
    const { host, port: dbPort, database, username, password } = body;
    const { server, instanceName, port: parsedPort } = parseSqlServerServerInput(host, dbPort);

    if (!server || !database || !username || password === undefined || password === null) {
      return res.status(400).json({
        ok: false,
        message: 'Missing required fields: host, database, username, password.',
      });
    }

    let portNum = parsedPort;
    if (!instanceName && (!portNum || !Number.isFinite(portNum))) {
      portNum = defaultPorts.sqlserver;
    }

    const options = buildSqlServerTlsOptions(server, instanceName);

    const config = {
      user: String(username),
      password: String(password),
      server,
      database: String(database),
      options,
      connectionTimeout: 12_000,
      requestTimeout: 12_000,
    };

    if (instanceName) {
      if (portNum && Number.isFinite(portNum) && portNum > 0) {
        config.port = portNum;
      }
    } else {
      config.port = portNum && Number.isFinite(portNum) && portNum > 0 ? portNum : defaultPorts.sqlserver;
    }

    try {
      await sql.connect(config);
      await sql.query`SELECT 1 AS ok`;
      await sql.close();
      return res.json({ ok: true, message: 'Successfully connected to SQL Server.' });
    } catch (error) {
      try {
        await sql.close();
      } catch {
        /* ignore */
      }
      const err = error instanceof Error ? error : new Error('SQL Server connection failed.');
      let message = err.message;
      const code = /** @type {{ code?: string }} */ (error).code;
      if (code === 'ENOTFOUND' || /ENOTFOUND/i.test(message)) {
        message = `${message} — Use a DNS name or IP that resolves on this machine. For a named instance, enter SERVER\\INSTANCE in the host field (backslash). If the port is not 1433, use HOST,PORT or set the port field.`;
      }
      return res.status(500).json({ ok: false, message });
    }
  }

  if (type === 'postgresql') {
    const { host, port: dbPort, database, username, password } = body;
    const sanitizedHost = normalizeHost(host);
    const portNum = parsePort(dbPort, defaultPorts.postgresql);
    if (!sanitizedHost || !portNum || !database || !username || password === undefined || password === null) {
      return res.status(400).json({
        ok: false,
        message: 'Missing required fields: host, port, database, username, password.',
      });
    }

    const client = new pg.Client({
      host: sanitizedHost,
      port: portNum,
      database: String(database),
      user: String(username),
      password: String(password),
      connectionTimeoutMillis: 12_000,
    });

    try {
      await client.connect();
      await client.query('SELECT 1');
      await client.end();
      return res.json({ ok: true, message: 'Successfully connected to PostgreSQL.' });
    } catch (error) {
      try {
        await client.end();
      } catch {
        /* ignore */
      }
      const err = error instanceof Error ? error : new Error('PostgreSQL connection failed.');
      return res.status(500).json({ ok: false, message: err.message });
    }
  }

  if (type === 'mongodb') {
    const { host, port: dbPort, database, username, password } = body;
    const dbName = String(database || '').trim();
    if (!dbName) {
      return res.status(400).json({ ok: false, message: 'Database name is required for MongoDB.' });
    }

    const hostStr = String(host || '').trim();
    let uri;
    if (/^mongodb(\+srv)?:\/\//i.test(hostStr)) {
      uri = hostStr;
    } else {
      const sanitizedHost = normalizeHost(hostStr);
      const portNum = parsePort(dbPort, defaultPorts.mongodb);
      if (!sanitizedHost || !portNum || password === undefined || password === null) {
        return res.status(400).json({
          ok: false,
          message: 'Missing required fields: host (or URI), port, database, username, password.',
        });
      }
      const user = encodeURIComponent(String(username));
      const pass = encodeURIComponent(String(password));
      uri = `mongodb://${user}:${pass}@${sanitizedHost}:${portNum}/${encodeURIComponent(dbName)}`;
    }

    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 12_000 });
    try {
      await client.connect();
      await client.db(dbName).command({ ping: 1 });
      await client.close();
      return res.json({ ok: true, message: 'Successfully connected to MongoDB.' });
    } catch (error) {
      try {
        await client.close();
      } catch {
        /* ignore */
      }
      const err = error instanceof Error ? error : new Error('MongoDB connection failed.');
      return res.status(500).json({ ok: false, message: err.message });
    }
  }

  return res.status(400).json({ ok: false, message: 'Unsupported connector.' });
};
