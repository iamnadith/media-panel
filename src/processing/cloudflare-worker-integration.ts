import 'server-only';

import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { ACTIVE_POSTGRES_URL, BASE_URL, POSTGRES_SSL_ENABLED } from '@/app/config';
import { query } from '@/platforms/postgres';
import { BACKEND_ORCHESTRATOR_BUNDLE } from './generated/backend-orchestrator-bundle';
import { getProcessingSettingsSafe } from './settings';

type StoredIntegration = {
  token_ciphertext: string; token_iv: string; token_tag: string
  secret_ciphertext: string; secret_iv: string; secret_tag: string
  account_id: string; worker_name: string; worker_url: string; hyperdrive_id: string
};
export type CloudflareWorkerIntegrationStatus = {
  configured: boolean; accountId?: string; workerName?: string; workerUrl?: string; hyperdriveId?: string
};
export type BackendOrchestratorIntegration = { baseUrl: string; sharedSecret: string };

const table = async () => {
  await query(`CREATE TABLE IF NOT EXISTS cloudflare_worker_integration (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id), token_ciphertext TEXT NOT NULL,
    token_iv TEXT NOT NULL, token_tag TEXT NOT NULL, secret_ciphertext TEXT,
    secret_iv TEXT, secret_tag TEXT, account_id TEXT, worker_name TEXT,
    worker_url TEXT, hyperdrive_id TEXT, updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now())`);
  for (const column of ['secret_ciphertext', 'secret_iv', 'secret_tag']) {
    await query(`ALTER TABLE cloudflare_worker_integration ADD COLUMN IF NOT EXISTS ${column} TEXT`);
  }
  await query('ALTER TABLE cloudflare_worker_integration ENABLE ROW LEVEL SECURITY');
  await query('REVOKE ALL ON cloudflare_worker_integration FROM PUBLIC');
  await query('REVOKE ALL ON cloudflare_worker_integration FROM anon, authenticated');
};
const key = () => {
  const secret = process.env.AUTH_SECRET || process.env.NEXTAUTH_SECRET;
  if (!secret) throw new Error('AUTH_SECRET is required to secure Cloudflare integration credentials');
  return createHash('sha256').update(`media-panel:cloudflare:${secret}`).digest();
};
const encrypt = (value: string) => {
  const iv = randomBytes(12); const cipher = createCipheriv('aes-256-gcm', key(), iv);
  return { ciphertext: Buffer.concat([cipher.update(value, 'utf8'), cipher.final()]).toString('base64'), iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64') };
};
const decrypt = (ciphertext: string, iv: string, tag: string) => {
  const cipher = createDecipheriv('aes-256-gcm', key(), Buffer.from(iv, 'base64')); cipher.setAuthTag(Buffer.from(tag, 'base64'));
  return Buffer.concat([cipher.update(Buffer.from(ciphertext, 'base64')), cipher.final()]).toString('utf8');
};
const cf = async (token: string, path: string, init?: RequestInit) => {
  const response = await fetch(`https://api.cloudflare.com/client/v4${path}`, { ...init, headers: { Authorization: `Bearer ${token}`, ...(init?.headers || {}) } });
  const body = await response.json().catch(() => ({})) as { success?: boolean; result?: any; errors?: Array<{ message?: string }> };
  if (!response.ok || !body.success) throw new Error(body.errors?.[0]?.message || `Cloudflare API returned ${response.status}`);
  return body.result;
};
const cfJson = (token: string, path: string, value: unknown, method = 'POST') => cf(token, path, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(value) });
const slug = (value: string) => value.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 45);
const publicUrl = (input?: string) => input?.trim() ? (/^https?:\/\//i.test(input) ? input.trim().replace(/\/+$/, '') : `https://${input.trim().replace(/\/+$/, '')}`) : undefined;
const getStored = async () => { await table(); return (await query<StoredIntegration>('SELECT * FROM cloudflare_worker_integration WHERE id=true')).rows[0]; };

export const getBackendOrchestratorIntegration = async (): Promise<BackendOrchestratorIntegration | undefined> => {
  const row = await getStored();
  if (row?.worker_url && row.secret_ciphertext && row.secret_iv && row.secret_tag) return { baseUrl: row.worker_url, sharedSecret: decrypt(row.secret_ciphertext, row.secret_iv, row.secret_tag) };
  return undefined;
};
export const getCloudflareWorkerIntegrationStatus = async (): Promise<CloudflareWorkerIntegrationStatus> => {
  const row = await getStored(); return row ? { configured: true, accountId: row.account_id, workerName: row.worker_name, workerUrl: row.worker_url, hyperdriveId: row.hyperdrive_id } : { configured: false };
};
const workerSecrets = async (secret: string) => {
  const settings = await getProcessingSettingsSafe();
  const values: Record<string, string | undefined> = {
    POSTGRES_URL: ACTIVE_POSTGRES_URL.trim(), DISABLE_POSTGRES_SSL: POSTGRES_SSL_ENABLED ? '0' : '1', MEDIA_PANEL_BASE_URL: publicUrl(process.env.NEXT_PUBLIC_DOMAIN) || BASE_URL, AUTOMATION_API_SECRET: secret, BACKEND_ORCHESTRATOR_SHARED_SECRET: secret,
    BACKEND_PROCESSOR_SHARED_SECRET: process.env.BACKEND_PROCESSOR_SHARED_SECRET?.trim(), DRIVE_STORAGE_BASE_URL: process.env.DRIVE_STORAGE_BASE_URL?.trim(), DRIVE_STORAGE_API_KEY: process.env.DRIVE_STORAGE_API_KEY?.trim(), DRIVE_STORAGE_PROJECT_ID: process.env.NEXT_PUBLIC_DRIVE_STORAGE_PROJECT_ID?.trim(), DRIVE_STORAGE_BUCKET: process.env.NEXT_PUBLIC_DRIVE_STORAGE_BUCKET?.trim(),
    R2_PUBLIC_BASE_URL: publicUrl(process.env.NEXT_PUBLIC_CLOUDFLARE_R2_PUBLIC_DOMAIN), R2_ACCOUNT_ID: process.env.NEXT_PUBLIC_CLOUDFLARE_R2_ACCOUNT_ID?.trim(), R2_BUCKET: process.env.NEXT_PUBLIC_CLOUDFLARE_R2_BUCKET?.trim(), R2_ACCESS_KEY_ID: process.env.CLOUDFLARE_R2_ACCESS_KEY?.trim(), R2_SECRET_ACCESS_KEY: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY?.trim(), UNIQUE_MEDIA_NAMES: process.env.NEXT_PUBLIC_UNIQUE_MEDIA_NAMES || '0',
    REGISTER_BATCH_SIZE: String(settings.registerBatchSize), MAX_REGISTER_PASSES: String(settings.maxRegisterPasses), STALE_PROCESSING_MINUTES: String(settings.staleProcessingMinutes), STALE_REGISTRATION_MINUTES: String(settings.staleRegistrationMinutes), REGISTRATION_HISTORY_DAYS: String(settings.registrationHistoryDays), BACKEND_PROCESSOR_POLL_INTERVAL_MS: String(settings.processorPollIntervalMs), BACKEND_PROCESSOR_IDLE_INTERVAL_MS: String(settings.processorIdleIntervalMs), BACKEND_PROCESSOR_HEARTBEAT_INTERVAL_MS: String(settings.processorHeartbeatIntervalMs), BACKEND_PROCESSOR_CLAIM_LIMIT: String(settings.processorClaimLimit),
  };
  if (!values.POSTGRES_URL) throw new Error('POSTGRES_URL must be configured before deploying the Worker');
  return Object.entries(values).filter(([, value]) => Boolean(value)).map(([name, text]) => ({ name, type: 'secret_text', text }));
};

export const provisionCloudflareWorkerIntegration = async ({ token, accountId }: { token: string; accountId?: string }) => {
  if (!token.trim()) throw new Error('Cloudflare API token is required'); await table();
  try { await cf(token, '/user/tokens/verify'); } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown Cloudflare API error';
    throw new Error(`Cloudflare rejected this API token: ${message}. Create an API token with Workers Scripts Write and Hyperdrive Write.`);
  }
  const accounts = accountId?.trim() ? [{ id: accountId.trim() }] : await cf(token, '/accounts?per_page=50');
  if (!Array.isArray(accounts) || accounts.length !== 1) throw new Error('Cloudflare account ID is needed only when the token can access more than one account');
  const account = accounts[0].id as string; const stored = await getStored(); const workerName = stored?.worker_name || `media-panel-orchestrator-${slug(account).slice(-8)}`;
  const database = new URL(ACTIVE_POSTGRES_URL); const hyperdriveName = `media-panel-${workerName}-postgres`;
  const hyperdrives = await cf(token, `/accounts/${account}/hyperdrive/configs?per_page=100`); let hyperdrive = Array.isArray(hyperdrives) ? hyperdrives.find(item => item.name === hyperdriveName) : undefined;
  if (!hyperdrive) hyperdrive = await cfJson(token, `/accounts/${account}/hyperdrive/configs`, { name: hyperdriveName, origin: { database: database.pathname.replace(/^\//, ''), host: database.hostname, password: decodeURIComponent(database.password), port: Number(database.port || 5432), scheme: 'postgres', user: decodeURIComponent(database.username) }, caching: { disabled: true }, origin_connection_limit: 5, mtls: { sslmode: 'require' } });
  let subdomain: string | undefined;
  try { subdomain = (await cf(token, `/accounts/${account}/workers/subdomain`)).subdomain; } catch { subdomain = `media-panel-${slug(account).slice(-8)}`; await cfJson(token, `/accounts/${account}/workers/subdomain`, { subdomain }, 'PUT'); }
  const secret = randomBytes(32).toString('base64url'); const bindings = await workerSecrets(secret); bindings.push({ name: 'HYPERDRIVE', type: 'hyperdrive', id: hyperdrive.id } as any);
  const form = new FormData(); form.set('metadata', JSON.stringify({ main_module: 'index.js', compatibility_date: '2026-05-05', compatibility_flags: ['nodejs_compat'], bindings })); form.set('index.js', new Blob([BACKEND_ORCHESTRATOR_BUNDLE], { type: 'application/javascript+module' }), 'index.js');
  await cf(token, `/accounts/${account}/workers/scripts/${workerName}`, { method: 'PUT', body: form });
  await cfJson(token, `/accounts/${account}/workers/scripts/${workerName}/subdomain`, { enabled: true, previews_enabled: false });
  await cfJson(token, `/accounts/${account}/workers/scripts/${workerName}/triggers`, { schedules: [{ cron: '* * * * *' }] }, 'PUT');
  const workerUrl = `https://${workerName}.${subdomain}.workers.dev`; const securedToken = encrypt(token.trim()); const securedSecret = encrypt(secret);
  await query(`INSERT INTO cloudflare_worker_integration (id, token_ciphertext, token_iv, token_tag, secret_ciphertext, secret_iv, secret_tag, account_id, worker_name, worker_url, hyperdrive_id, updated_at) VALUES (true,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now()) ON CONFLICT (id) DO UPDATE SET token_ciphertext=EXCLUDED.token_ciphertext, token_iv=EXCLUDED.token_iv, token_tag=EXCLUDED.token_tag, secret_ciphertext=EXCLUDED.secret_ciphertext, secret_iv=EXCLUDED.secret_iv, secret_tag=EXCLUDED.secret_tag, account_id=EXCLUDED.account_id, worker_name=EXCLUDED.worker_name, worker_url=EXCLUDED.worker_url, hyperdrive_id=EXCLUDED.hyperdrive_id, updated_at=now()`, [securedToken.ciphertext, securedToken.iv, securedToken.tag, securedSecret.ciphertext, securedSecret.iv, securedSecret.tag, account, workerName, workerUrl, hyperdrive.id]);
  return getCloudflareWorkerIntegrationStatus();
};
