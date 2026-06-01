import nodemailer from 'nodemailer';

export const getSmtpConfig = (env = process.env) => {
  const host = String(env.SMTP_HOST || 'smtp.gmail.com').trim();
  const port = Number(env.SMTP_PORT || 465) || 465;
  const secure = String(env.SMTP_SECURE || '').trim() === 'true' || port === 465;
  const user = String(env.SMTP_USER || 'xerocode.ai@gmail.com').trim();
  const pass = String(env.SMTP_PASS || env.GMAIL_APP_PASSWORD || '').trim();
  const from = String(env.SMTP_FROM || user).trim();
  return { host, port, secure, user, pass, from, isConfigured: Boolean(pass) };
};

export const formatSmtpSendError = (error) => {
  const raw = error instanceof Error ? error.message : String(error);
  if (/application-specific password required/i.test(raw) || /534-5\.7\.9/i.test(raw)) {
    return 'Gmail rejected the SMTP login. Set SMTP_PASS to a Google app password for SMTP_USER, not your normal Gmail password.';
  }
  if (/invalid login|authentication failed|username and password not accepted/i.test(raw)) {
    return 'SMTP login failed. Verify SMTP_USER and SMTP_PASS. For Gmail, use an app password.';
  }
  return raw || 'Unable to send email.';
};

export const getStudioLoginUrl = (env = process.env) => {
  const url = String(env.STUDIO_APP_URL || env.APP_PUBLIC_URL || 'https://apps.xerocode.ai').trim();
  return url.replace(/\/$/, '');
};

export async function sendMail({ to, subject, text, html, smtp = getSmtpConfig() }) {
  if (!smtp.isConfigured) {
    throw new Error('SMTP password is missing. Set SMTP_PASS (or GMAIL_APP_PASSWORD).');
  }
  const transporter = nodemailer.createTransport({
    host: smtp.host,
    port: smtp.port,
    secure: smtp.secure,
    auth: { user: smtp.user, pass: smtp.pass },
  });
  try {
    await transporter.sendMail({
      from: smtp.from,
      to,
      subject,
      text,
      html,
    });
  } catch (error) {
    throw new Error(formatSmtpSendError(error));
  }
}

export async function sendWelcomeUserEmail({
  toEmail,
  fullName,
  email,
  password,
  invitedByName,
  smtp = getSmtpConfig(),
}) {
  const loginUrl = getStudioLoginUrl();
  const displayName = String(fullName || email).trim() || 'there';
  const inviter = String(invitedByName || 'Your workspace admin').trim();
  const subject = 'Welcome to XeroCode.ai — your workspace account is ready';

  const text = [
    `Hi ${displayName},`,
    '',
    `${inviter} created an account for you on XeroCode.ai.`,
    '',
    'XeroCode.ai is a no-code platform to build and run:',
    '• Agentic applications from your data',
    '• AI agents and workflows',
    '• Interactive dashboards',
    '• Automations across your stack',
    '',
    'Your login details:',
    `Login URL: ${loginUrl}`,
    `Email: ${email}`,
    `Password: ${password}`,
    '',
    'Sign in with the email and password above. We recommend changing your password after your first login.',
    '',
    'If you did not expect this email, contact your workspace administrator.',
    '',
    '— The XeroCode.ai team',
  ].join('\n');

  const html = `
    <div style="font-family:system-ui,-apple-system,Segoe UI,sans-serif;line-height:1.55;color:#0f172a;max-width:560px;">
      <p>Hi ${escapeHtml(displayName)},</p>
      <p><strong>${escapeHtml(inviter)}</strong> created an account for you on <strong>XeroCode.ai</strong>.</p>
      <p>XeroCode.ai is a no-code platform to build and run agentic applications, AI agents, interactive dashboards, and automations — without writing code.</p>
      <div style="margin:20px 0;padding:16px 18px;border:1px solid #e2e8f0;border-radius:12px;background:#f8fafc;">
        <p style="margin:0 0 10px;font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;">Login details</p>
        <p style="margin:0 0 8px;"><strong>Login URL:</strong> <a href="${escapeHtml(loginUrl)}">${escapeHtml(loginUrl)}</a></p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> ${escapeHtml(email)}</p>
        <p style="margin:0;"><strong>Password:</strong> ${escapeHtml(password)}</p>
      </div>
      <p style="margin:0 0 16px;">
        <a href="${escapeHtml(loginUrl)}" style="display:inline-block;background:#4f46e5;color:#fff;text-decoration:none;padding:10px 18px;border-radius:10px;font-weight:600;">Sign in to XeroCode.ai</a>
      </p>
      <p style="font-size:13px;color:#64748b;">We recommend changing your password after your first login. If you did not expect this email, contact your workspace administrator.</p>
      <p style="font-size:13px;color:#64748b;">— The XeroCode.ai team</p>
    </div>
  `;

  await sendMail({ to: toEmail, subject, text, html, smtp });
}

export async function sendOtpEmail({ toEmail, otpCode, smtp = getSmtpConfig() }) {
  const code = String(otpCode ?? '').trim();
  await sendMail({
    to: toEmail,
    subject: 'Your OTP for password reset',
    text: `Your OTP is ${code}. It is valid for 10 minutes.`,
    html: `<p>Your OTP is <strong>${escapeHtml(code)}</strong>.</p><p>It is valid for 10 minutes.</p>`,
    smtp,
  });
}

export async function sendSignupWelcomeEmail({
  toEmail,
  fullName,
  email,
  password,
  smtp = getSmtpConfig(),
}) {
  const loginUrl = getStudioLoginUrl();
  const displayName = String(fullName || email).trim() || 'there';
  const subject = 'Welcome to XeroCode.ai — your account is ready';

  const text = [
    `Hi ${displayName},`,
    '',
    'Thanks for signing up on XeroCode.ai.',
    '',
    'Your login details:',
    `Login URL: ${loginUrl}`,
    `Email: ${email}`,
    `Password: ${password}`,
    '',
    'Sign in with the email and password above. We recommend changing your password after your first login.',
    '',
    'If you did not create this account, contact support.',
    '',
    '— The XeroCode.ai team',
  ].join('\n');

  const html = `
    <div style="font-family:system-ui,-apple-system,Segoe UI,sans-serif;line-height:1.55;color:#0f172a;max-width:560px;">
      <p>Hi ${escapeHtml(displayName)},</p>
      <p>Thanks for signing up on <strong>XeroCode.ai</strong>.</p>
      <div style="margin:20px 0;padding:16px 18px;border:1px solid #e2e8f0;border-radius:12px;background:#f8fafc;">
        <p style="margin:0 0 10px;font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;color:#64748b;">Login details</p>
        <p style="margin:0 0 8px;"><strong>Login URL:</strong> <a href="${escapeHtml(loginUrl)}">${escapeHtml(loginUrl)}</a></p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> ${escapeHtml(email)}</p>
        <p style="margin:0;"><strong>Password:</strong> ${escapeHtml(password)}</p>
      </div>
      <p style="margin:0 0 16px;">
        <a href="${escapeHtml(loginUrl)}" style="display:inline-block;background:#4f46e5;color:#fff;text-decoration:none;padding:10px 18px;border-radius:10px;font-weight:600;">Sign in to XeroCode.ai</a>
      </p>
      <p style="font-size:13px;color:#64748b;">We recommend changing your password after your first login.</p>
      <p style="font-size:13px;color:#64748b;">— The XeroCode.ai team</p>
    </div>
  `;

  await sendMail({ to: toEmail, subject, text, html, smtp });
}

export async function sendPasswordChangedEmail({ toEmail, fullName, smtp = getSmtpConfig() }) {
  const loginUrl = getStudioLoginUrl();
  const displayName = String(fullName || toEmail).trim() || 'there';
  const subject = 'Your XeroCode.ai password was changed';

  const text = [
    `Hi ${displayName},`,
    '',
    'This confirms that the password for your XeroCode.ai account was changed successfully.',
    '',
    `Registered email: ${toEmail}`,
    `Login URL: ${loginUrl}`,
    '',
    'If you did not make this change, reset your password immediately or contact your workspace administrator.',
    '',
    '— The XeroCode.ai team',
  ].join('\n');

  const html = `
    <div style="font-family:system-ui,-apple-system,Segoe UI,sans-serif;line-height:1.55;color:#0f172a;max-width:560px;">
      <p>Hi ${escapeHtml(displayName)},</p>
      <p>The password for your <strong>XeroCode.ai</strong> account was changed successfully.</p>
      <p style="margin:12px 0;"><strong>Registered email:</strong> ${escapeHtml(toEmail)}</p>
      <p style="margin:0 0 16px;">
        <a href="${escapeHtml(loginUrl)}" style="display:inline-block;background:#4f46e5;color:#fff;text-decoration:none;padding:10px 18px;border-radius:10px;font-weight:600;">Sign in to XeroCode.ai</a>
      </p>
      <p style="font-size:13px;color:#64748b;">If you did not make this change, reset your password immediately or contact your workspace administrator.</p>
      <p style="font-size:13px;color:#64748b;">— The XeroCode.ai team</p>
    </div>
  `;

  await sendMail({ to: toEmail, subject, text, html, smtp });
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
