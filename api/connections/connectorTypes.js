const ALLOWED = new Set(['mysql', 'sqlserver', 'postgresql', 'mongodb']);

export const CONNECTOR_DEFAULT_PORTS = {
  mysql: 3306,
  sqlserver: 1433,
  postgresql: 5432,
  mongodb: 27017,
};

export const normalizeConnectorType = (raw) => {
  const c = String(raw || '')
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '');
  if (c === 'postgres' || c === 'pgsql') {
    return 'postgresql';
  }
  if (c === 'mongo' || c === 'mongodb' || c === 'mongodb+srv') {
    return 'mongodb';
  }
  if (c === 'mssql' || c === 'microsoftsqlserver') {
    return 'sqlserver';
  }
  if (c === 'mariadb') {
    return 'mysql';
  }
  return ALLOWED.has(c) ? c : null;
};
