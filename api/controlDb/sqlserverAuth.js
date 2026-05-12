import crypto from 'crypto';
import nodemailer from 'nodemailer';
import sql from 'mssql';
import { closeControlSqlServer, connectToControlSqlServer } from './sqlserver.js';

const hashPassword = (password) => {
  const salt = crypto.randomBytes(16).toString('hex');
  const digest = crypto.createHash('sha256').update(`${salt}:${String(password)}`, 'utf8').digest('hex');
  return `${salt}:${digest}`;
};

const verifyPassword = (password, storedHash) => {
  const parts = String(storedHash || '').split(':');
  if (parts.length !== 2) return false;
  const [salt, expected] = parts;
  const digest = crypto.createHash('sha256').update(`${salt}:${String(password)}`, 'utf8').digest('hex');
  return digest === expected;
};

const normalizeAuthResponseUser = (row, authProvider = 'local') => ({
  id: Number(row?.id) || 0,
  fullName: row?.full_name ?? '',
  email: row?.email ?? '',
  authProvider,
  avatarUrl: row?.avatar_url ?? '',
});

const generateNumericOtp = () => String(Math.floor(100000 + Math.random() * 900000));

const getOtpEmailConfig = (env = process.env) => {
  const host = String(env.SMTP_HOST || 'smtp.gmail.com').trim();
  const port = Number(env.SMTP_PORT || 465) || 465;
  const secure = String(env.SMTP_SECURE || '').trim() === 'true' || port === 465;
  const user = String(env.SMTP_USER || 'xerocode.ai@gmail.com').trim();
  const pass = String(env.SMTP_PASS || env.GMAIL_APP_PASSWORD || '').trim();
  const from = String(env.SMTP_FROM || user).trim();
  return { host, port, secure, user, pass, from, isConfigured: Boolean(pass) };
};

const sendOtpEmail = async ({ toEmail, otpCode, smtp = getOtpEmailConfig() }) => {
  if (!smtp.isConfigured) {
    throw new Error('SMTP password is missing. Set SMTP_PASS (or GMAIL_APP_PASSWORD).');
  }

  const transporter = nodemailer.createTransport({
    host: smtp.host,
    port: smtp.port,
    secure: smtp.secure,
    auth: { user: smtp.user, pass: smtp.pass },
  });
  await transporter.sendMail({
    from: smtp.from,
    to: toEmail,
    subject: 'Your OTP for password reset',
    text: `Your OTP is ${otpCode}. It is valid for 10 minutes.`,
    html: `<p>Your OTP is <strong>${otpCode}</strong>.</p><p>It is valid for 10 minutes.</p>`,
  });
};

const ensureSqlServerAuthTables = async (pool) => {
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.tenants', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.tenants (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
      );
    END
  `);

  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.user_profile (
        id INT PRIMARY KEY,
        tenant_id INT NULL,
        full_name NVARCHAR(255) NOT NULL DEFAULT '',
        email NVARCHAR(255) NOT NULL DEFAULT '',
        password_hash NVARCHAR(255) NULL,
        phone NVARCHAR(80) NULL,
        company NVARCHAR(255) NULL,
        role_title NVARCHAR(255) NULL,
        bio NVARCHAR(MAX) NULL,
        avatar_url NVARCHAR(MAX) NULL,
        slack_url NVARCHAR(MAX) NULL,
        discord_url NVARCHAR(MAX) NULL,
        linkedin_url NVARCHAR(MAX) NULL,
        x_url NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        updated_at DATETIME2 NULL
      );
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.user_profile', 'created_at') IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM sys.columns c
        INNER JOIN sys.tables t ON t.object_id = c.object_id
        INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'dbo'
          AND t.name = 'user_profile'
          AND c.name = 'created_at'
          AND c.default_object_id = 0
      )
    BEGIN
      ALTER TABLE dbo.user_profile ADD CONSTRAINT DF_user_profile_created_at DEFAULT SYSDATETIME() FOR created_at;
    END
  `);

  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL AND COL_LENGTH('dbo.user_profile', 'slack_url') IS NULL
      ALTER TABLE dbo.user_profile ADD slack_url NVARCHAR(MAX) NULL;
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL AND COL_LENGTH('dbo.user_profile', 'discord_url') IS NULL
      ALTER TABLE dbo.user_profile ADD discord_url NVARCHAR(MAX) NULL;
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL AND COL_LENGTH('dbo.user_profile', 'linkedin_url') IS NULL
      ALTER TABLE dbo.user_profile ADD linkedin_url NVARCHAR(MAX) NULL;
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL AND COL_LENGTH('dbo.user_profile', 'x_url') IS NULL
      ALTER TABLE dbo.user_profile ADD x_url NVARCHAR(MAX) NULL;
  `);

  await pool.request().query(`
    IF OBJECT_ID(N'dbo.password_reset_otp', N'U') IS NULL
    BEGIN
      CREATE TABLE dbo.password_reset_otp (
        id INT IDENTITY(1,1) PRIMARY KEY,
        email NVARCHAR(255) NOT NULL,
        otp_code NVARCHAR(10) NOT NULL,
        expires_at DATETIME2 NOT NULL,
        used BIT NOT NULL DEFAULT 0,
        created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
      );
      CREATE INDEX idx_password_reset_email ON dbo.password_reset_otp(email);
    END
  `);
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.password_reset_otp', N'U') IS NOT NULL
      AND COL_LENGTH('dbo.password_reset_otp', 'created_at') IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM sys.columns c
        INNER JOIN sys.tables t ON t.object_id = c.object_id
        INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'dbo'
          AND t.name = 'password_reset_otp'
          AND c.name = 'created_at'
          AND c.default_object_id = 0
      )
    BEGIN
      ALTER TABLE dbo.password_reset_otp
      ADD CONSTRAINT DF_password_reset_otp_created_at DEFAULT SYSDATETIME() FOR created_at;
    END
  `);
};

const nextUserId = async (pool) => {
  const result = await pool.request().query(`SELECT ISNULL(MAX(id), 0) + 1 AS next_id FROM dbo.user_profile`);
  return Number(result.recordset?.[0]?.next_id || 1);
};

const nextTenantId = async (pool) => {
  const result = await pool.request().query(`SELECT ISNULL(MAX(id), 0) + 1 AS next_id FROM dbo.tenants`);
  return Number(result.recordset?.[0]?.next_id || 1);
};

export const getOrCreateUserAndTenantByEmail = async (emailRaw, fullNameRaw) => {
  const email = String(emailRaw || '').trim().toLowerCase();
  const fullName = String(fullNameRaw || '').trim();
  if (!email) return { userId: null, tenantId: null };

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);

    const domain = email.includes('@') ? email.split('@')[1] : email;
    let tenantId = null;

    const tenantHit = await pool.request().input('name', sql.NVarChar, domain).query(`
      SELECT TOP 1 id FROM dbo.tenants WHERE name = @name
    `);
    if (tenantHit.recordset?.length) {
      tenantId = Number(tenantHit.recordset[0].id) || null;
    } else {
      const tenantNextId = await nextTenantId(pool);
      const tenantInsert = await pool
        .request()
        .input('id', sql.Int, tenantNextId)
        .input('name', sql.NVarChar, domain || 'default')
        .query(`
          INSERT INTO dbo.tenants (id, name, created_at) VALUES (@id, @name, SYSDATETIME());
          SELECT @id AS id;
        `);
      tenantId = Number(tenantInsert.recordset?.[0]?.id || 0) || null;
    }

    let userId = null;
    const userHit = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id FROM dbo.user_profile WHERE email = @email
    `);
    if (userHit.recordset?.length) {
      userId = Number(userHit.recordset[0].id) || null;
      await pool
        .request()
        .input('tenantId', sql.Int, tenantId)
        .input('userId', sql.Int, userId)
        .query(`UPDATE dbo.user_profile SET tenant_id = ISNULL(tenant_id, @tenantId), updated_at = SYSDATETIME() WHERE id = @userId`);
    } else {
      userId = await nextUserId(pool);
      await pool
        .request()
        .input('id', sql.Int, userId)
        .input('tenantId', sql.Int, tenantId)
        .input('fullName', sql.NVarChar, fullName || email)
        .input('email', sql.NVarChar, email).query(`
          INSERT INTO dbo.user_profile (id, tenant_id, full_name, email, created_at, updated_at)
          VALUES (@id, @tenantId, @fullName, @email, SYSDATETIME(), SYSDATETIME())
        `);
    }

    return { userId, tenantId };
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const signupUser = async (req, res) => {
  const fullName = String(req.body?.fullName ?? '').trim();
  const email = String(req.body?.email ?? '').trim().toLowerCase();
  const password = String(req.body?.password ?? '');
  if (!fullName) return res.status(400).json({ ok: false, message: 'Name is required.' });
  if (!email) return res.status(400).json({ ok: false, message: 'Email is required.' });
  if (password.length < 6) return res.status(400).json({ ok: false, message: 'Password must be at least 6 characters.' });

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);

    const existing = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id FROM dbo.user_profile WHERE email = @email
    `);
    if (existing.recordset?.length) {
      return res.status(409).json({ ok: false, message: 'User already exists. Please login.' });
    }

    const id = await nextUserId(pool);
    const passwordHash = hashPassword(password);
    await pool
      .request()
      .input('id', sql.Int, id)
      .input('fullName', sql.NVarChar, fullName)
      .input('email', sql.NVarChar, email)
      .input('passwordHash', sql.NVarChar, passwordHash).query(`
        INSERT INTO dbo.user_profile (id, full_name, email, password_hash, created_at, updated_at)
        VALUES (@id, @fullName, @email, @passwordHash, SYSDATETIME(), SYSDATETIME())
      `);
    await getOrCreateUserAndTenantByEmail(email, fullName);
    return res.json({ ok: true, message: 'Signup successful.', user: { id, fullName, email, authProvider: 'local', avatarUrl: '' } });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to signup.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const loginUser = async (req, res) => {
  const email = String(req.body?.email ?? '').trim().toLowerCase();
  const password = String(req.body?.password ?? '');
  if (!email || !password) return res.status(400).json({ ok: false, message: 'Email and password are required.' });

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const result = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id, full_name, email, password_hash, avatar_url
      FROM dbo.user_profile
      WHERE email = @email
    `);
    const row = result.recordset?.[0] || null;
    if (!row || !row.password_hash || !verifyPassword(password, row.password_hash)) {
      return res.status(401).json({ ok: false, message: 'Invalid email or password.' });
    }
    await getOrCreateUserAndTenantByEmail(email, row.full_name || email);
    return res.json({ ok: true, message: 'Login successful.', user: normalizeAuthResponseUser(row, 'local') });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to login.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const loginWithGoogle = async (req, res) => {
  const accessToken = String(req.body?.accessToken ?? '').trim();
  if (!accessToken) return res.status(400).json({ ok: false, message: 'Google access token is required.' });

  let pool;
  try {
    const googleRes = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    const googleProfile = await googleRes.json().catch(() => ({}));
    if (!googleRes.ok) return res.status(401).json({ ok: false, message: 'Google token validation failed.' });

    const email = String(googleProfile?.email ?? '').trim().toLowerCase();
    const fullName = String(googleProfile?.name ?? '').trim();
    const avatarUrl = String(googleProfile?.picture ?? '').trim();
    if (!email) return res.status(400).json({ ok: false, message: 'Google profile is missing required fields.' });

    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const existing = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id, full_name, email, avatar_url FROM dbo.user_profile WHERE email = @email
    `);
    if (!existing.recordset?.length) {
      const id = await nextUserId(pool);
      await pool
        .request()
        .input('id', sql.Int, id)
        .input('fullName', sql.NVarChar, fullName || email.split('@')[0])
        .input('email', sql.NVarChar, email)
        .input('avatarUrl', sql.NVarChar(sql.MAX), avatarUrl || null).query(`
          INSERT INTO dbo.user_profile (id, full_name, email, avatar_url, created_at, updated_at)
          VALUES (@id, @fullName, @email, @avatarUrl, SYSDATETIME(), SYSDATETIME())
        `);
    }
    const rowRes = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id, full_name, email, avatar_url FROM dbo.user_profile WHERE email = @email
    `);
    const row = rowRes.recordset?.[0] || null;
    await getOrCreateUserAndTenantByEmail(email, row?.full_name || fullName || email);
    return res.json({ ok: true, message: 'Google login successful.', user: normalizeAuthResponseUser(row, 'google') });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to login with Google.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const loginWithGithub = async (req, res) => {
  const code = String(req.body?.code ?? '').trim();
  const redirectUri = String(req.body?.redirectUri ?? '').trim();
  const env = process.env || {};
  const clientId = String(env.GITHUB_CLIENT_ID || env.VITE_GITHUB_CLIENT_ID || '').trim();
  const clientSecret = String(env.GITHUB_CLIENT_SECRET || '').trim();
  if (!code) return res.status(400).json({ ok: false, message: 'GitHub authorization code is required.' });
  if (!clientId || !clientSecret) {
    return res.status(500).json({ ok: false, message: 'GitHub OAuth is not configured. Set GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET.' });
  }

  let pool;
  try {
    const tokenRes = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ client_id: clientId, client_secret: clientSecret, code, redirect_uri: redirectUri || undefined }),
    });
    const tokenPayload = await tokenRes.json().catch(() => ({}));
    const accessToken = String(tokenPayload?.access_token || '').trim();
    if (!tokenRes.ok || !accessToken) {
      return res.status(401).json({ ok: false, message: 'GitHub token exchange failed.' });
    }

    const userRes = await fetch('https://api.github.com/user', {
      headers: { Authorization: `Bearer ${accessToken}`, Accept: 'application/vnd.github+json', 'User-Agent': 'xerocode-ai' },
    });
    const userPayload = await userRes.json().catch(() => ({}));
    if (!userRes.ok) return res.status(401).json({ ok: false, message: 'Unable to fetch GitHub user profile.' });

    let email = String(userPayload?.email || '').trim().toLowerCase();
    if (!email) {
      const emailsRes = await fetch('https://api.github.com/user/emails', {
        headers: { Authorization: `Bearer ${accessToken}`, Accept: 'application/vnd.github+json', 'User-Agent': 'xerocode-ai' },
      });
      const emailsPayload = await emailsRes.json().catch(() => []);
      if (emailsRes.ok && Array.isArray(emailsPayload)) {
        const primary = emailsPayload.find((item) => item?.primary && item?.verified) || emailsPayload[0];
        email = String(primary?.email || '').trim().toLowerCase();
      }
    }
    if (!email) return res.status(400).json({ ok: false, message: 'GitHub account email is unavailable.' });

    const fullName = String(userPayload?.name || '').trim() || String(userPayload?.login || '').trim() || email.split('@')[0];
    const avatarUrl = String(userPayload?.avatar_url || '').trim();

    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const existing = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id, full_name, email, avatar_url FROM dbo.user_profile WHERE email = @email
    `);
    if (!existing.recordset?.length) {
      const id = await nextUserId(pool);
      await pool
        .request()
        .input('id', sql.Int, id)
        .input('fullName', sql.NVarChar, fullName)
        .input('email', sql.NVarChar, email)
        .input('avatarUrl', sql.NVarChar(sql.MAX), avatarUrl || null).query(`
          INSERT INTO dbo.user_profile (id, full_name, email, avatar_url, created_at, updated_at)
          VALUES (@id, @fullName, @email, @avatarUrl, SYSDATETIME(), SYSDATETIME())
        `);
    }
    const rowRes = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id, full_name, email, avatar_url FROM dbo.user_profile WHERE email = @email
    `);
    const row = rowRes.recordset?.[0] || null;
    await getOrCreateUserAndTenantByEmail(email, row?.full_name || fullName || email);
    return res.json({ ok: true, message: 'GitHub login successful.', user: normalizeAuthResponseUser(row, 'github') });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to login with GitHub.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const requestPasswordResetOtp = async (req, res) => {
  const email = String(req.body?.email ?? '').trim().toLowerCase();
  if (!email) return res.status(400).json({ ok: false, message: 'Email is required.' });

  const isProduction = String(process.env.NODE_ENV || '').trim().toLowerCase() === 'production';
  const smtp = getOtpEmailConfig();
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const users = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id FROM dbo.user_profile WHERE email = @email
    `);
    if (!users.recordset?.length) {
      return res.status(404).json({ ok: false, message: 'No account found for this email.' });
    }

    if (isProduction && !smtp.isConfigured) {
      return res.status(503).json({
        ok: false,
        message: 'Password reset email is not configured on the API server. Set SMTP_PASS (or GMAIL_APP_PASSWORD).',
      });
    }

    const otpCode = generateNumericOtp();
    await pool
      .request()
      .input('email', sql.NVarChar, email)
      .input('otp', sql.NVarChar, otpCode).query(`
        INSERT INTO dbo.password_reset_otp (email, otp_code, expires_at, used, created_at)
        VALUES (@email, @otp, DATEADD(MINUTE, 10, SYSDATETIME()), 0, SYSDATETIME())
      `);

    if (!smtp.isConfigured) {
      return res.json({
        ok: true,
        message: 'OTP generated for development. Email delivery is not configured.',
        otp: otpCode,
      });
    }

    await sendOtpEmail({ toEmail: email, otpCode, smtp });
    return res.json({ ok: true, message: 'OTP sent to your registered email. It is valid for 10 minutes.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to generate OTP.';
    const status = /smtp|mail|email/i.test(message) ? 503 : 500;
    return res.status(status).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const resetPasswordWithOtp = async (req, res) => {
  const email = String(req.body?.email ?? '').trim().toLowerCase();
  const otp = String(req.body?.otp ?? '').trim();
  const newPassword = String(req.body?.newPassword ?? '');
  if (!email || !otp || !newPassword) {
    return res.status(400).json({ ok: false, message: 'Email, OTP, and new password are required.' });
  }
  if (newPassword.length < 6) {
    return res.status(400).json({ ok: false, message: 'New password must be at least 6 characters.' });
  }
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const otpRows = await pool
      .request()
      .input('email', sql.NVarChar, email)
      .input('otp', sql.NVarChar, otp).query(`
        SELECT TOP 1 id
        FROM dbo.password_reset_otp
        WHERE email = @email AND otp_code = @otp AND used = 0 AND expires_at >= SYSDATETIME()
        ORDER BY id DESC
      `);
    if (!otpRows.recordset?.length) {
      return res.status(400).json({ ok: false, message: 'Invalid or expired OTP.' });
    }
    const passwordHash = hashPassword(newPassword);
    const upd = await pool
      .request()
      .input('email', sql.NVarChar, email)
      .input('passwordHash', sql.NVarChar, passwordHash).query(`
        UPDATE dbo.user_profile SET password_hash = @passwordHash, updated_at = SYSDATETIME() WHERE email = @email
      `);
    if (!(upd.rowsAffected?.[0] > 0)) {
      return res.status(404).json({ ok: false, message: 'No account found for this email.' });
    }
    await pool.request().input('id', sql.Int, Number(otpRows.recordset[0].id)).query(`
      UPDATE dbo.password_reset_otp SET used = 1 WHERE id = @id
    `);
    return res.json({ ok: true, message: 'Password reset successful. Please login with new password.' });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to reset password.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const getUserProfile = async (_req, res) => {
  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const result = await pool.request().query(`
      SELECT TOP 1 id, full_name, email, phone, company, role_title, bio, avatar_url,
        slack_url, discord_url, linkedin_url, x_url
      FROM dbo.user_profile
      ORDER BY updated_at DESC, id DESC
    `);
    if (result.recordset?.length) {
      const row = result.recordset[0];
      return res.json({
        ok: true,
        profile: {
          id: Number(row.id) || 0,
          fullName: row.full_name ?? '',
          email: row.email ?? '',
          phone: row.phone ?? '',
          company: row.company ?? '',
          roleTitle: row.role_title ?? '',
          bio: row.bio ?? '',
          avatarUrl: row.avatar_url ?? '',
          slackUrl: row.slack_url ?? '',
          discordUrl: row.discord_url ?? '',
          linkedinUrl: row.linkedin_url ?? '',
          xUrl: row.x_url ?? '',
        },
      });
    }
    return res.json({
      ok: true,
      profile: {
        id: 0,
        fullName: '',
        email: '',
        phone: '',
        company: '',
        roleTitle: '',
        bio: '',
        avatarUrl: '',
        slackUrl: '',
        discordUrl: '',
        linkedinUrl: '',
        xUrl: '',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to load profile.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

export const saveUserProfile = async (req, res) => {
  const body = req.body ?? {};
  const fullName = String(body.fullName ?? '').trim();
  const email = String(body.email ?? '').trim().toLowerCase();
  const phone = String(body.phone ?? '').trim();
  const company = String(body.company ?? '').trim();
  const roleTitle = String(body.roleTitle ?? '').trim();
  const bio = String(body.bio ?? '').trim();
  const avatarUrl = String(body.avatarUrl ?? '').trim();
  const slackUrl = String(body.slackUrl ?? '').trim();
  const discordUrl = String(body.discordUrl ?? '').trim();
  const linkedinUrl = String(body.linkedinUrl ?? '').trim();
  const xUrl = String(body.xUrl ?? '').trim();
  if (!fullName) return res.status(400).json({ ok: false, message: 'Full name is required.' });
  if (!email) return res.status(400).json({ ok: false, message: 'Email is required.' });

  let pool;
  try {
    pool = await connectToControlSqlServer();
    await ensureSqlServerAuthTables(pool);
    const existing = await pool.request().input('email', sql.NVarChar, email).query(`
      SELECT TOP 1 id FROM dbo.user_profile WHERE email = @email
    `);
    let id = 0;
    if (existing.recordset?.length) {
      id = Number(existing.recordset[0].id) || 0;
      await pool
        .request()
        .input('id', sql.Int, id)
        .input('fullName', sql.NVarChar, fullName)
        .input('phone', sql.NVarChar, phone || null)
        .input('company', sql.NVarChar, company || null)
        .input('roleTitle', sql.NVarChar, roleTitle || null)
        .input('bio', sql.NVarChar(sql.MAX), bio || null)
        .input('avatarUrl', sql.NVarChar(sql.MAX), avatarUrl || null)
        .input('slackUrl', sql.NVarChar(sql.MAX), slackUrl || null)
        .input('discordUrl', sql.NVarChar(sql.MAX), discordUrl || null)
        .input('linkedinUrl', sql.NVarChar(sql.MAX), linkedinUrl || null)
        .input('xUrl', sql.NVarChar(sql.MAX), xUrl || null).query(`
          UPDATE dbo.user_profile
          SET full_name = @fullName, phone = @phone, company = @company, role_title = @roleTitle, bio = @bio, avatar_url = @avatarUrl,
              slack_url = @slackUrl, discord_url = @discordUrl, linkedin_url = @linkedinUrl, x_url = @xUrl,
              updated_at = SYSDATETIME()
          WHERE id = @id
        `);
    } else {
      id = await nextUserId(pool);
      await pool
        .request()
        .input('id', sql.Int, id)
        .input('fullName', sql.NVarChar, fullName)
        .input('email', sql.NVarChar, email)
        .input('phone', sql.NVarChar, phone || null)
        .input('company', sql.NVarChar, company || null)
        .input('roleTitle', sql.NVarChar, roleTitle || null)
        .input('bio', sql.NVarChar(sql.MAX), bio || null)
        .input('avatarUrl', sql.NVarChar(sql.MAX), avatarUrl || null)
        .input('slackUrl', sql.NVarChar(sql.MAX), slackUrl || null)
        .input('discordUrl', sql.NVarChar(sql.MAX), discordUrl || null)
        .input('linkedinUrl', sql.NVarChar(sql.MAX), linkedinUrl || null)
        .input('xUrl', sql.NVarChar(sql.MAX), xUrl || null).query(`
          INSERT INTO dbo.user_profile
            (id, full_name, email, phone, company, role_title, bio, avatar_url, slack_url, discord_url, linkedin_url, x_url, created_at, updated_at)
          VALUES
            (@id, @fullName, @email, @phone, @company, @roleTitle, @bio, @avatarUrl, @slackUrl, @discordUrl, @linkedinUrl, @xUrl, SYSDATETIME(), SYSDATETIME())
        `);
    }
    return res.json({
      ok: true,
      message: 'Profile saved.',
      profile: {
        id,
        fullName,
        email,
        phone,
        company,
        roleTitle,
        bio,
        avatarUrl,
        slackUrl,
        discordUrl,
        linkedinUrl,
        xUrl,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unable to save profile.';
    return res.status(500).json({ ok: false, message });
  } finally {
    await closeControlSqlServer(pool);
  }
};

