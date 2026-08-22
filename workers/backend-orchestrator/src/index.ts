import { neon } from '@neondatabase/serverless';
import { AwsClient } from 'aws4fetch';
import { Client } from 'pg';

type ScheduledController = {
  cron: string
  scheduledTime: number
};

type ExecutionContext = {
  waitUntil: (promise: Promise<unknown>) => void
};

type R2ObjectLike = {
  key: string
  uploaded?: Date
  size?: number
};

type MediaRow = {
  id: string
  url: string
  extension: string
  poster_url: string | null
  preview_url: string | null
  transcode_status: string | null
};

type ClaimJobRow = {
  id: string
  url: string
  extension: string
  transcode_error: string | null
};

type CanonicalMediaRow = {
  id: string
  url: string
  extension: string
};

type RegistrationStatusRow = {
  url: string
  file_name?: string | null
  uploaded_at?: string | null
  status?: string | null
  source_url?: string | null
  original_file_name?: string | null
  title?: string | null
  media_id?: string | null
  extension?: string | null
  error_message?: string | null
};

type UploadRegistrationHintRow = {
  url: string
  original_file_name: string | null
  title: string | null
  updated_at?: string | Date | null
  created_at?: string | Date | null
};

export interface Env {
  POSTGRES_URL: string
  DISABLE_POSTGRES_SSL?: string
  MEDIA_PANEL_BASE_URL?: string
  AUTOMATION_API_SECRET?: string
  R2_PUBLIC_BASE_URL: string
  R2_ACCOUNT_ID: string
  R2_BUCKET: string
  R2_ACCESS_KEY_ID: string
  R2_SECRET_ACCESS_KEY: string
  DRIVE_STORAGE_BASE_URL?: string
  DRIVE_STORAGE_API_KEY?: string
  DRIVE_STORAGE_PROJECT_ID?: string
  DRIVE_STORAGE_BUCKET?: string
  UNIQUE_MEDIA_NAMES?: string
  NEXT_PUBLIC_UNIQUE_MEDIA_NAMES?: string
  BACKEND_ORCHESTRATOR_SHARED_SECRET?: string
  BACKEND_PROCESSOR_SHARED_SECRET?: string
  REGISTER_BATCH_SIZE?: string
  MAX_REGISTER_PASSES?: string
  STALE_PROCESSING_MINUTES?: string
  STALE_REGISTRATION_MINUTES?: string
  REGISTRATION_HISTORY_DAYS?: string
  BACKEND_PROCESSOR_POLL_INTERVAL_MS?: string
  BACKEND_PROCESSOR_IDLE_INTERVAL_MS?: string
  BACKEND_PROCESSOR_HEARTBEAT_INTERVAL_MS?: string
  BACKEND_PROCESSOR_CLAIM_LIMIT?: string
}

type RuntimeProcessingSettings = {
  orchestratorEnabled: boolean
  registrationEnabled: boolean
  videoProcessingEnabled: boolean
  registerBatchSize: number
  maxRegisterPasses: number
  staleProcessingMinutes: number
  staleRegistrationMinutes: number
  registrationHistoryDays: number
  processorPollIntervalMs: number
  processorIdleIntervalMs: number
  processorHeartbeatIntervalMs: number
  processorClaimLimit: number
};

let runtimeSettingsCache: {
  expiresAt: number
  settings: RuntimeProcessingSettings
} | undefined;
let hlsSchemaInitialization: Promise<void> | undefined;

const ensureHlsSchema = async (env: Env) => {
  if (hlsSchemaInitialization) return hlsSchemaInitialization;
  hlsSchemaInitialization = (async () => {
    const sql = sqlForEnv(env);
    await sql`ALTER TABLE media ADD COLUMN IF NOT EXISTS hls_manifest_url TEXT`;
    await sql`ALTER TABLE media ADD COLUMN IF NOT EXISTS hls_verified_at TIMESTAMP WITH TIME ZONE`;
    await sql`CREATE INDEX IF NOT EXISTS media_hls_reconciliation_idx
      ON media (hls_verified_at ASC NULLS FIRST, id ASC)
      WHERE media_type='video' AND transcode_status='ready'`;
  })().catch(error => {
    hlsSchemaInitialization = undefined;
    throw error;
  });
  return hlsSchemaInitialization;
};

const getRuntimeProcessingSettings = async (env: Env) => {
  if (runtimeSettingsCache && runtimeSettingsCache.expiresAt > Date.now()) {
    return runtimeSettingsCache.settings;
  }
  const defaults: RuntimeProcessingSettings = {
    orchestratorEnabled: true,
    registrationEnabled: true,
    videoProcessingEnabled: true,
    registerBatchSize: getNumber(env.REGISTER_BATCH_SIZE, 1, { min: 1, max: 100 }),
    maxRegisterPasses: getNumber(env.MAX_REGISTER_PASSES, 1, { min: 1, max: 20 }),
    staleProcessingMinutes: getNumber(env.STALE_PROCESSING_MINUTES, 2, { min: 1, max: 1440 }),
    staleRegistrationMinutes: getNumber(env.STALE_REGISTRATION_MINUTES, 5, { min: 1, max: 1440 }),
    registrationHistoryDays: getNumber(env.REGISTRATION_HISTORY_DAYS, 14, { min: 1, max: 365 }),
    processorPollIntervalMs: getNumber(env.BACKEND_PROCESSOR_POLL_INTERVAL_MS, 5000, { min: 1000, max: 300000 }),
    processorIdleIntervalMs: getNumber(env.BACKEND_PROCESSOR_IDLE_INTERVAL_MS, 5000, { min: 1000, max: 300000 }),
    processorHeartbeatIntervalMs: getNumber(env.BACKEND_PROCESSOR_HEARTBEAT_INTERVAL_MS, 5000, { min: 1000, max: 60000 }),
    processorClaimLimit: getNumber(env.BACKEND_PROCESSOR_CLAIM_LIMIT, 1, { min: 1, max: 3 }),
  };
  try {
    const sql = sqlForEnv(env);
    await sql`
      CREATE TABLE IF NOT EXISTS processing_configuration (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      )
    `;
    const rows = await sql`SELECT key, value FROM processing_configuration` as
      unknown as { key: string, value: string }[];
    const values = Object.fromEntries(rows.map(row => [row.key, row.value]));
    const enabled = (key: string, fallback: boolean) => values[key] === undefined
      ? fallback
      : values[key] === 'true' || values[key] === '1';
    const number = (key: string, fallback: number, min: number, max: number) => {
      const parsed = Number(values[key]);
      return Number.isFinite(parsed)
        ? Math.min(Math.max(Math.round(parsed), min), max)
        : fallback;
    };
    defaults.orchestratorEnabled = enabled('orchestratorEnabled', true);
    defaults.registrationEnabled = enabled('registrationEnabled', true);
    defaults.videoProcessingEnabled = enabled('videoProcessingEnabled', true);
    defaults.registerBatchSize = number('registerBatchSize', defaults.registerBatchSize, 1, 100);
    defaults.maxRegisterPasses = number('maxRegisterPasses', defaults.maxRegisterPasses, 1, 20);
    defaults.staleProcessingMinutes = number('staleProcessingMinutes', defaults.staleProcessingMinutes, 1, 1440);
    defaults.staleRegistrationMinutes = number('staleRegistrationMinutes', defaults.staleRegistrationMinutes, 1, 1440);
    defaults.registrationHistoryDays = number('registrationHistoryDays', defaults.registrationHistoryDays, 1, 365);
    defaults.processorPollIntervalMs = number('processorPollIntervalMs', defaults.processorPollIntervalMs, 1000, 300000);
    defaults.processorIdleIntervalMs = number('processorIdleIntervalMs', defaults.processorIdleIntervalMs, 1000, 300000);
    defaults.processorHeartbeatIntervalMs = number('processorHeartbeatIntervalMs', defaults.processorHeartbeatIntervalMs, 1000, 60000);
    defaults.processorClaimLimit = number('processorClaimLimit', defaults.processorClaimLimit, 1, 3);
  } catch (error) {
    console.warn('Using deployed processing configuration defaults', error);
  }
  runtimeSettingsCache = { expiresAt: Date.now() + 30_000, settings: defaults };
  return defaults;
};

const envWithRuntimeSettings = (
  env: Env,
  settings: RuntimeProcessingSettings,
): Env => ({
  ...env,
  REGISTER_BATCH_SIZE: String(settings.registerBatchSize),
  MAX_REGISTER_PASSES: String(settings.maxRegisterPasses),
  STALE_PROCESSING_MINUTES: String(settings.staleProcessingMinutes),
  STALE_REGISTRATION_MINUTES: String(settings.staleRegistrationMinutes),
  REGISTRATION_HISTORY_DAYS: String(settings.registrationHistoryDays),
});

const MEDIA_EXTENSIONS = new Set([
  'jpg',
  'jpeg',
  'png',
  'mp4',
  'mov',
  'webm',
  'mkv',
  'm4v',
  'avi',
  'ts',
  'm2ts',
  'mts',
  'mpg',
  'mpeg',
  'wmv',
  'flv',
  '3gp',
  'ogv',
]);
const VIDEO_EXTENSIONS = new Set([
  'mp4', 'mkv', 'mov', 'm4v', 'webm', 'avi', 'ts', 'm2ts', 'mts',
  'mpg', 'mpeg', 'wmv', 'flv', '3gp', 'ogv',
]);
const PRESERVED_VIDEO_EXTENSIONS = new Set(['mp4', 'mkv']);
const GENERATED_MEDIA_SUFFIX_REGEX =
  /-(sm|md|lg|poster|preview|stream|hls(?:-init|-(?:high|720p)-init)|subtitles(?:\.[a-z0-9_-]+)?)$/i;
const STALE_REGISTRATION_ERROR_MESSAGE =
  'Previous registration attempt stalled; queued for retry';
const MISSING_UPLOAD_ERROR_PREFIX = 'Upload not found in storage';
const WORKER_BUILD_ID = 'registration-retry-v37-hints';
// A scheduled Worker must finish promptly. Drive copies can become visible
// asynchronously, so persist the in-flight state and check again on the next
// minute instead of polling long enough to lose the registration lease.
export const DRIVE_COPY_VISIBILITY_ATTEMPTS = 3;
export const DRIVE_COPY_VISIBILITY_DELAY_MS = 2000;
export const DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS = 3;
export const DRIVE_COPY_REQUEST_TIMEOUT_MS = 15_000;
// A registration scan owns a global lease. Bound every Drive operation in its
// path so one stalled request cannot block all later scans indefinitely.
const REGISTRATION_STORAGE_TIMEOUT_MS = 30_000;
const DELETION_STORAGE_TIMEOUT_MS = 15_000;
const DELETION_MUTATION_TIMEOUT_MS = 45_000;
const DELETION_MUTATION_CONCURRENCY = 4;
// A single scheduled scan must not pin the isolate's shared in-flight promise
// forever. Individual database/storage calls are already bounded; this is a
// final recovery guard for a provider/runtime promise that never settles.
export const SCAN_WATCHDOG_TIMEOUT_MS = 120_000;

const encoder = new TextEncoder();

export const isAllowedStreamDerivativeKey = (key: string) =>
  /^[a-zA-Z0-9._@-]+-stream\.(mp4|webm)$/i.test(key);

export const isAllowedHlsDerivativeKey = (key: string) =>
  /^[a-zA-Z0-9._@-]+-hls(?:-(?:high|720p))?(?:\.m3u8|-init\.mp4|-[0-9]{5}\.m4s)$/i.test(key);

export const isAllowedProcessorUploadKey = (
  key: string,
  photoId: string,
) => isAllowedStreamDerivativeKey(key) ||
  isAllowedHlsDerivativeKey(key) ||
  key.split('/').pop()?.toLowerCase() === `${photoId.toLowerCase()}.mp4`;

type SubtitleManifestTrack = {
  src: string
  lang: string
  label: string
};

type SubtitleUploadMetadata = {
  fileName: string
  lang?: string
  label?: string
};

export const getValidSubtitleUploadMetadata = (
  fileNameBase: string,
  value: unknown,
  uploadedFileNames: string[],
) => {
  if (!Array.isArray(value)) { return [] as Required<SubtitleUploadMetadata>[]; }
  const uploaded = new Set(uploadedFileNames);
  const prefix = `${fileNameBase}-subtitles.`;
  return value.flatMap(item => {
    if (!item || typeof item !== 'object') { return []; }
    const candidate = item as SubtitleUploadMetadata;
    const fileName = candidate.fileName?.trim();
    if (
      !fileName ||
      !uploaded.has(fileName) ||
      !fileName.startsWith(prefix) ||
      !/^[a-z0-9_-]+\.vtt$/i.test(fileName.slice(prefix.length))
    ) {
      return [];
    }
    const token = fileName.slice(prefix.length, -4);
    const lang = (candidate.lang || token || 'und').trim().slice(0, 48);
    const label = (candidate.label || lang.toUpperCase() || 'Subtitle')
      .trim()
      .slice(0, 120);
    return [{ fileName, lang, label }];
  });
};

export const mergeSubtitleManifestTracks = (
  existing: SubtitleManifestTrack[],
  incoming: SubtitleManifestTrack[],
) => {
  const merged = new Map(existing.map(track => [track.src, track]));
  incoming.forEach(track => merged.set(track.src, track));
  return Array.from(merged.values());
};

export const detectStorageProvider = (env: Env) =>
  (
    env.DRIVE_STORAGE_BASE_URL &&
    env.DRIVE_STORAGE_API_KEY &&
    env.DRIVE_STORAGE_PROJECT_ID &&
    env.DRIVE_STORAGE_BUCKET
  )
    ? 'drive' as const
    : 'cloudflare-r2' as const;

const isDriveStorageEnabled = (env: Env) =>
  detectStorageProvider(env) === 'drive';

const driveObjectBaseUrl = (env: Env) =>
  `${env.DRIVE_STORAGE_BASE_URL!.replace(/\/+$/, '')}/${encodeURIComponent(env.DRIVE_STORAGE_BUCKET || '')}`;

const driveApiBaseUrl = (env: Env) => {
  try {
    return new URL(env.DRIVE_STORAGE_BASE_URL || '').origin;
  } catch {
    return '';
  }
};

const revalidateMediaPanel = async (env: Env, photoId?: string) => {
  const baseUrl = env.MEDIA_PANEL_BASE_URL?.trim().replace(/\/+$/, '');
  const secret = (
    env.AUTOMATION_API_SECRET || env.BACKEND_ORCHESTRATOR_SHARED_SECRET
  )?.trim();
  if (!baseUrl || !secret) { return; }

  await fetch(`${baseUrl}/api/processing/revalidate`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${secret}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(photoId ? { photoId } : {}),
    signal: AbortSignal.timeout(REGISTRATION_STORAGE_TIMEOUT_MS),
  }).catch(() => undefined);
};

const driveHeaders = (env: Env, extras?: Record<string, string>) => ({
  Authorization: `Bearer ${env.DRIVE_STORAGE_API_KEY}`,
  'X-Drive-Project': env.DRIVE_STORAGE_PROJECT_ID || '',
  'X-Drive-Bucket': env.DRIVE_STORAGE_BUCKET || '',
  ...(extras ?? {}),
});

const stableStorageReadHeaders = (env: Env) =>
  isDriveStorageEnabled(env) ? driveHeaders(env) : undefined;

const getNumber = (
  value: string | undefined,
  fallback: number,
  { min = 0, max = Number.MAX_SAFE_INTEGER } = {},
) => {
  const parsed = parseInt(value || '', 10);
  if (!Number.isFinite(parsed)) { return fallback; }
  return Math.min(Math.max(parsed, min), max);
};

export const runSafeRegistrationCommit = async ({
  prepareDestination,
  commitRegistration,
  cleanupSource,
  onCleanupError,
}: {
  prepareDestination: () => Promise<void>
  commitRegistration: () => Promise<void>
  cleanupSource: () => Promise<void>
  onCleanupError?: (error: unknown) => void
}) => {
  await prepareDestination();
  await commitRegistration();
  await cleanupSource().catch(error => {
    onCleanupError?.(error);
  });
};

export const isVerifiedStorageCopy = (
  sourceSize: number | undefined,
  destinationSize: number | undefined,
) => destinationSize !== undefined && (
  sourceSize === undefined || sourceSize === destinationSize
);

export const isProtectedRegistrationDestination = ({
  objectUrl,
  sourceUrl,
  expectedUrl,
  sourceExists,
}: {
  objectUrl: string
  sourceUrl: string | undefined
  expectedUrl: string | undefined
  sourceExists: boolean
}) => Boolean(
  sourceExists &&
  sourceUrl &&
  expectedUrl &&
  expectedUrl !== sourceUrl &&
  objectUrl === expectedUrl
);

export const shouldVerifyExistingRegistrationDestination = ({
  sourceKey,
  destinationKey,
  mediaId,
  trackedMediaId,
  targetRecordedAsRegistered,
}: {
  sourceKey: string
  destinationKey: string
  mediaId: string
  trackedMediaId: string | undefined
  targetRecordedAsRegistered: boolean
}) => destinationKey !== sourceKey && (
  targetRecordedAsRegistered || trackedMediaId === mediaId
);

export const shouldWaitForTrackedRegistrationDestination = ({
  shouldVerifyExistingTarget,
  registrationStatus,
  targetAlreadyRegistered,
}: {
  shouldVerifyExistingTarget: boolean
  registrationStatus: string | undefined
  targetAlreadyRegistered: boolean
}) => shouldVerifyExistingTarget &&
  registrationStatus === 'registering' &&
  !targetAlreadyRegistered;

export const waitForVerifiedStorageCopy = async ({
  sourceSize,
  readDestinationSize,
  attempts = 1,
  delayMs = 0,
  wait = sleep,
}: {
  sourceSize: number | undefined
  readDestinationSize: () => Promise<number | undefined>
  attempts?: number
  delayMs?: number
  wait?: (milliseconds: number) => Promise<void>
}) => {
  let destinationSize: number | undefined;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    destinationSize = await readDestinationSize();
    if (isVerifiedStorageCopy(sourceSize, destinationSize)) {
      return destinationSize;
    }
    if (attempt < attempts - 1 && delayMs > 0) {
      await wait(delayMs);
    }
  }
  return destinationSize;
};

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
    },
  });

const isAuthorized = (
  request: Request,
  secret: string | undefined,
) => {
  if (!secret) { return true; }
  const authorization = request.headers.get('authorization') || '';
  const [scheme, token] = authorization.split(/\s+/, 2);
  return scheme?.toLowerCase() === 'bearer' && token === secret;
};

const deriveTitleFromFileName = (fileName?: string) =>
  fileName
    ?.replace(/\.[^/.]+$/, '')
    .replace(/[-_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const toNaivePostgresString = (value: string) =>
  value.replace(
    /(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(\.\d+)?Z?$/,
    '$1 $2',
  );

const parseDateValue = (value?: string | Date | null) => {
  if (!value) { return undefined; }
  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed : undefined;
};

const sleep = (ms: number) =>
  new Promise(resolve => setTimeout(resolve, ms));

const getFileParts = (key: string) => {
  const normalized = key.split('?')[0] || '';
  const fileName = normalized.split('/').pop() || normalized;
  const lastDot = fileName.lastIndexOf('.');
  const fileNameBase = lastDot >= 0
    ? fileName.slice(0, lastDot)
    : fileName;
  const extension = lastDot >= 0
    ? fileName.slice(lastDot + 1).toLowerCase()
    : '';
  return {
    fileName,
    fileNameBase,
    extension,
  };
};

const safeDecodeURIComponent = (value: string) => {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
};

const canonicalizeStorageUrl = (url: string) => {
  const [withoutQuery] = url.split('?');
  try {
    const parsed = new URL(withoutQuery);
    const normalizedPath = parsed.pathname
      .split('/')
      .map(segment => encodeURIComponent(safeDecodeURIComponent(segment)))
      .join('/');
    return `${parsed.origin}${normalizedPath}`;
  } catch {
    return withoutQuery;
  }
};

const decodeStorageUrlPath = (url: string) => {
  const [withoutQuery] = url.split('?');
  try {
    const parsed = new URL(withoutQuery);
    const decodedPath = parsed.pathname
      .split('/')
      .map(segment => safeDecodeURIComponent(segment))
      .join('/');
    return `${parsed.origin}${decodedPath}`;
  } catch {
    return withoutQuery;
  }
};

const urlForKey = (env: Env, key: string) =>
  isDriveStorageEnabled(env)
    ? `${driveObjectBaseUrl(env)}/${key.split('/').map(encodeURIComponent).join('/')}`
    : `${env.R2_PUBLIC_BASE_URL.replace(/\/+$/, '')}/${key}`;

const keyFromStorageUrl = (env: Env, url: string) => {
  const [urlWithoutQuery] = url.split('?');
  const base = isDriveStorageEnabled(env)
    ? driveObjectBaseUrl(env)
    : env.R2_PUBLIC_BASE_URL.replace(/\/+$/, '');
  if (!base || !urlWithoutQuery.startsWith(base)) {
    return '';
  }
  return urlWithoutQuery.slice(base.length).replace(/^\/+/, '').split('/').map(decodeURIComponent).join('/');
};

const listDriveKeysForPrefix = async (
  env: Env,
  prefix: string,
  timeoutMs = DELETION_STORAGE_TIMEOUT_MS,
) => {
  const listUrl = new URL(`${driveApiBaseUrl(env)}/api/v1/storage/list`);
  listUrl.searchParams.set('projectId', env.DRIVE_STORAGE_PROJECT_ID || '');
  listUrl.searchParams.set('bucket', env.DRIVE_STORAGE_BUCKET || '');
  listUrl.searchParams.set('prefix', prefix);
  listUrl.searchParams.set('limit', '10000');
  const response = await fetch(listUrl.toString(), {
    headers: driveHeaders(env),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) {
    throw new Error(`Drive deletion list failed (${response.status})`);
  }
  const data = await response.json() as {
    objects?: Array<{ key?: string, fileName?: string, url?: string }>
  };
  return (data.objects || []).flatMap(item => {
    const itemKey = item.key || item.fileName ||
      (item.url ? keyFromStorageUrl(env, item.url) : '');
    return itemKey ? [itemKey] : [];
  });
};

const driveObjectExists = async (
  env: Env,
  key: string,
  timeoutMs?: number,
) => {
  const effectiveTimeoutMs = timeoutMs ?? REGISTRATION_STORAGE_TIMEOUT_MS;
  const response = await fetch(
    `${driveApiBaseUrl(env)}/api/v1/storage/object/${key.split('/').map(encodeURIComponent).join('/')}`,
    {
      method: 'HEAD',
      headers: driveHeaders(env),
      signal: AbortSignal.timeout(effectiveTimeoutMs),
    },
  );
  if (response.status === 404) { return false; }
  if (response.status === 405 || response.status === 501) {
    const keys = await listDriveKeysForPrefix(env, key, effectiveTimeoutMs);
    return keys.includes(key);
  }
  if (!response.ok) {
    throw new Error(`Drive source check failed (${response.status})`);
  }
  return true;
};

const storageObjectSize = async (env: Env, key: string) => {
  if (isDriveStorageEnabled(env)) {
    const response = await fetch(
      `${driveApiBaseUrl(env)}/api/v1/storage/object/${key.split('/').map(encodeURIComponent).join('/')}`,
      {
        method: 'HEAD',
        headers: driveHeaders(env),
        signal: AbortSignal.timeout(REGISTRATION_STORAGE_TIMEOUT_MS),
      },
    );
    if (!response.ok) { return undefined; }
    const contentLength = response.headers.get('content-length');
    if (contentLength === null) { return undefined; }
    const size = Number(contentLength);
    return Number.isFinite(size) && size >= 0 ? size : undefined;
  }

  try {
    const response = await r2Request(env, 'HEAD', key);
    const contentLength = response.headers.get('content-length');
    if (contentLength === null) { return undefined; }
    const size = Number(contentLength);
    return Number.isFinite(size) && size >= 0 ? size : undefined;
  } catch {
    return undefined;
  }
};

const finalizeDriveUpload = async (env: Env, key: string) => {
  const response = await fetch(
    `${driveApiBaseUrl(env)}/api/v1/storage/finalize`,
    {
      method: 'POST',
      headers: driveHeaders(env, {
        'Content-Type': 'application/json',
      }),
      signal: AbortSignal.timeout(REGISTRATION_STORAGE_TIMEOUT_MS),
      body: JSON.stringify({
        projectId: env.DRIVE_STORAGE_PROJECT_ID,
        bucket: env.DRIVE_STORAGE_BUCKET,
        key,
      }),
    },
  );
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(
      `Drive finalize failed (${response.status})${text ? `: ${text}` : ''}`,
    );
  }
};

const storageObjectExists = async (
  env: Env,
  key: string,
  timeoutMs?: number,
) => {
  if (isDriveStorageEnabled(env)) {
    return driveObjectExists(env, key, timeoutMs);
  }

  try {
    await r2Request(env, 'HEAD', key, {
      ...(timeoutMs ? { signal: AbortSignal.timeout(timeoutMs) } : {}),
    });
    return true;
  } catch (error) {
    if (/R2 request failed \(404\b/i.test(String(error))) { return false; }
    throw error;
  }
};

const createDriveSignedDownloadUrl = async (env: Env, key: string) => {
  const downloadUrl = new URL(`${driveApiBaseUrl(env)}/api/v1/files/download`);
  downloadUrl.searchParams.set('projectId', env.DRIVE_STORAGE_PROJECT_ID || '');
  downloadUrl.searchParams.set('bucket', env.DRIVE_STORAGE_BUCKET || '');
  downloadUrl.searchParams.set('key', key);
  downloadUrl.searchParams.set('expiresInSeconds', '900');
  const response = await fetch(downloadUrl.toString(), {
    headers: driveHeaders(env),
  });
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Drive download sign failed (${response.status})${text ? `: ${text}` : ''}`);
  }
  const data = await response.json() as { url?: string };
  if (!data.url) {
    throw new Error('Drive download sign failed: missing signed URL');
  }
  return data.url;
};

const areUniqueMediaNamesEnabled = (env: Env) =>
  env.UNIQUE_MEDIA_NAMES === '1' ||
  env.NEXT_PUBLIC_UNIQUE_MEDIA_NAMES === '1';

const GENERATED_MEDIA_ID_PATTERN = /^\d{12}$/;

const trimToUndefined = (value?: string | null) => {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
};

const isDriveTimeoutLikeError = (error: unknown) => {
  const message = error instanceof Error
    ? error.message
    : String(error ?? '');
  return (
    message.includes('(524)') ||
    /timeout/i.test(message) ||
    /timed out/i.test(message)
  );
};

export const isRecoverableDriveCopyError = (error: unknown) => {
  if (isDriveTimeoutLikeError(error)) { return true; }
  const message = error instanceof Error
    ? error.message
    : String(error ?? '');
  return (
    message.startsWith('Copied destination is not readable in storage:') ||
    message.startsWith('Copied destination size mismatch:')
  );
};

export const selectOldestRegistrationBatch = (
  pending: R2ObjectLike[],
  attemptedKeys: Set<string>,
  limit: number,
) => pending
  .filter(object => !attemptedKeys.has(object.key))
  .sort((left, right) => {
    const leftUploaded = left.uploaded?.getTime();
    const rightUploaded = right.uploaded?.getTime();
    const leftTime = Number.isFinite(leftUploaded)
      ? leftUploaded as number
      : Number.MAX_SAFE_INTEGER;
    const rightTime = Number.isFinite(rightUploaded)
      ? rightUploaded as number
      : Number.MAX_SAFE_INTEGER;
    return leftTime - rightTime || left.key.localeCompare(right.key);
  })
  .slice(0, Math.max(0, limit));

const isGeneratedMediaName = (fileName?: string | null) => {
  const normalized = trimToUndefined(fileName);
  return Boolean(
    normalized &&
    GENERATED_MEDIA_ID_PATTERN.test(getFileParts(normalized).fileNameBase),
  );
};

const getOriginalFileNameFromSourceUrl = (
  statusRow?: RegistrationStatusRow,
) => {
  const sourceUrl = trimToUndefined(statusRow?.source_url);
  if (!sourceUrl) { return undefined; }
  const sourceFileName = trimToUndefined(getFileParts(sourceUrl).fileName);
  if (!sourceFileName || isGeneratedMediaName(sourceFileName)) {
    return undefined;
  }
  return sourceFileName;
};

const resolveRegistrationSourceUrl = (
  statusRow: RegistrationStatusRow | undefined,
  fallbackUrl: string,
) => trimToUndefined(statusRow?.source_url) || fallbackUrl;

const resolveRegistrationOriginalFileName = ({
  hint,
  statusRow,
  fallbackFileName,
}: {
  hint?: UploadRegistrationHintRow
  statusRow?: RegistrationStatusRow
  fallbackFileName?: string
}) => {
  const hintFileName = trimToUndefined(hint?.original_file_name);
  if (hintFileName) { return hintFileName; }

  const statusOriginalFileName = trimToUndefined(statusRow?.original_file_name);
  if (statusOriginalFileName && !isGeneratedMediaName(statusOriginalFileName)) {
    return statusOriginalFileName;
  }

  const statusFileName = trimToUndefined(statusRow?.file_name);
  if (statusFileName && !isGeneratedMediaName(statusFileName)) {
    return statusFileName;
  }

  const sourceFileName = getOriginalFileNameFromSourceUrl(statusRow);
  if (sourceFileName) { return sourceFileName; }

  return trimToUndefined(fallbackFileName) || fallbackFileName;
};

const resolveRegistrationTitle = ({
  originalFileName,
  fallbackFileName,
}: {
  originalFileName?: string
  fallbackFileName?: string
}) => {
  return (
    deriveTitleFromFileName(trimToUndefined(originalFileName)) ||
    deriveTitleFromFileName(trimToUndefined(fallbackFileName)) ||
    deriveTitleFromFileName(fallbackFileName)
  );
};

type SqlQuery = (...parts: any[]) => Promise<unknown[]>;
const SUPABASE_CONNECT_TIMEOUT_MS = 10_000;
const SUPABASE_QUERY_TIMEOUT_MS = 20_000;
const SUPABASE_CONNECTION_RETRY_ATTEMPTS = 3;
const connectionStringWithoutSslMode = (value: string) => {
  try {
    const url = new URL(value);
    url.searchParams.delete('sslmode');
    return url.toString();
  } catch {
    return value;
  }
};
const isRetryableSupabaseConnectionError = (error: unknown) =>
  /connection terminated unexpectedly|connection reset|econnreset|socket closed/i
    .test(error instanceof Error ? error.message : String(error));
const describePostgresQuery = (text: string) => text
  .replace(/\s+/g, ' ')
  .trim()
  .slice(0, 240);

const supabaseSqlForEnv = (env: Env): SqlQuery => {
  const sql = async (strings: TemplateStringsArray, ...values: unknown[]) => {
    let text = strings[0] || '';
    for (let index = 1; index < strings.length; index += 1) {
      text += `$${index}${strings[index] || ''}`;
    }
    for (let attempt = 0; attempt < SUPABASE_CONNECTION_RETRY_ATTEMPTS; attempt += 1) {
      // Workers are stateless. Do not retain a pg Client or Pool between
      // events: the runtime may reclaim the socket after a prior invocation.
      const client = new Client({
        connectionString: connectionStringWithoutSslMode(env.POSTGRES_URL),
        ssl: env.DISABLE_POSTGRES_SSL === '1'
          ? false
          : { rejectUnauthorized: false },
        connectionTimeoutMillis: SUPABASE_CONNECT_TIMEOUT_MS,
        query_timeout: SUPABASE_QUERY_TIMEOUT_MS,
        statement_timeout: SUPABASE_QUERY_TIMEOUT_MS,
      });
      try {
        await client.connect();
        const result = await client.query({
          text,
          values,
          query_timeout: SUPABASE_QUERY_TIMEOUT_MS,
          statement_timeout: SUPABASE_QUERY_TIMEOUT_MS,
        });
        return result.rows;
      } catch (error) {
        if (
          attempt < SUPABASE_CONNECTION_RETRY_ATTEMPTS - 1 &&
          isRetryableSupabaseConnectionError(error)
        ) {
          await sleep(250 * (attempt + 1));
          continue;
        }
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(
          `Postgres query failed after ${attempt + 1} attempt(s): ` +
          `${describePostgresQuery(text)}; ${message}`,
          { cause: error },
        );
      } finally {
        await client.end().catch(() => undefined);
      }
    }
    return [];
  };
  return sql as SqlQuery;
};

const isSupabasePostgresUrl = (value: string) => {
  try {
    const hostname = new URL(value).hostname;
    return /(?:^|\.)supabase\.co$/i.test(hostname) ||
      /\.pooler\.supabase\.com$/i.test(hostname);
  } catch {
    return false;
  }
};

const sqlForEnv = (env: Env) =>
  isSupabasePostgresUrl(env.POSTGRES_URL)
    ? supabaseSqlForEnv(env)
    : neon(env.POSTGRES_URL);

type BackendActivity = {
  category: 'orchestrator' | 'registration' | 'processing' | 'processor'
  event: string
  status?: 'info' | 'success' | 'warning' | 'error'
  message: string
  mediaId?: string
  processorId?: string
  details?: Record<string, unknown>
};

let backendActivityLogTableReady: Promise<void> | undefined;
const ensureBackendActivityLogTable = async (env: Env) => {
  if (!backendActivityLogTableReady) {
    const sql = sqlForEnv(env);
    backendActivityLogTableReady = sql`
      CREATE TABLE IF NOT EXISTS backend_activity_log (
        id BIGSERIAL PRIMARY KEY,
        category VARCHAR(32) NOT NULL,
        event VARCHAR(64) NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'info',
        message TEXT NOT NULL,
        media_id TEXT,
        processor_id TEXT,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      )
    `.then(() => undefined);
  }
  try {
    await backendActivityLogTableReady;
  } catch (error) {
    backendActivityLogTableReady = undefined;
    throw error;
  }
};

const logBackendActivity = async (
  env: Env,
  activity: BackendActivity,
) => {
  try {
    await ensureBackendActivityLogTable(env);
    const sql = sqlForEnv(env);
    await sql`
      INSERT INTO backend_activity_log (
        category, event, status, message, media_id, processor_id, details
      ) VALUES (
        ${activity.category},
        ${activity.event},
        ${activity.status || 'info'},
        ${activity.message},
        ${activity.mediaId ?? null},
        ${activity.processorId ?? null},
        ${JSON.stringify(activity.details || {})}::jsonb
      )
    `;
  } catch (error) {
    console.warn('Unable to record backend activity', error);
  }
};

const getBackendActivityLogs = async (env: Env, limit: number) => {
  await ensureBackendActivityLogTable(env);
  const sql = sqlForEnv(env);
  const retentionDays = getNumber(env.REGISTRATION_HISTORY_DAYS, 14, {
    min: 1,
    max: 365,
  });
  await sql`
    DELETE FROM backend_activity_log
    WHERE created_at < now() - (${String(retentionDays)} || ' days')::interval
  `;
  return await sql`
    SELECT
      id, category, event, status, message, media_id, processor_id,
      details, created_at
    FROM backend_activity_log
    ORDER BY created_at DESC
    LIMIT ${limit}
  ` as unknown as Record<string, unknown>[];
};

export const stableMediaIdForUrl = async (
  url: string,
  uploaded?: Date,
  attempt = 0,
) => {
  const baseIdentity = uploaded
    ? `${decodeURIComponent(url).split('?')[0]}|${uploaded.toISOString()}`
    : decodeURIComponent(url).split('?')[0];
  const objectIdentity = attempt > 0
    ? `${baseIdentity}|collision-${attempt}`
    : baseIdentity;
  const hashBuffer = await crypto.subtle.digest(
    'SHA-256',
    encoder.encode(objectIdentity),
  );
  const hashHex = Array.from(new Uint8Array(hashBuffer))
    .map(byte => byte.toString(16).padStart(2, '0'))
    .join('');
  return (
    BigInt(`0x${hashHex.slice(0, 16)}`) % BigInt('1000000000000')
  ).toString().padStart(12, '0');
};

const mediaIdForObject = async (
  env: Env,
  key: string,
  uploaded?: Date,
  attempt = 0,
) => {
  const { fileNameBase } = getFileParts(key);
  if (
    attempt === 0 &&
    areUniqueMediaNamesEnabled(env) &&
    GENERATED_MEDIA_ID_PATTERN.test(fileNameBase)
  ) {
    return fileNameBase;
  }
  return stableMediaIdForUrl(urlForKey(env, key), uploaded, attempt);
};

export const findAvailableMediaId = async (
  candidateForAttempt: (attempt: number) => Promise<string>,
  occupiedIds: Set<string>,
) => {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const candidate = await candidateForAttempt(attempt);
    if (!occupiedIds.has(candidate)) {
      return candidate;
    }
  }
  throw new Error('Unable to allocate a unique media ID');
};

export const isDeferredSourceCleanupSafe = (
  sourceUploaded?: Date,
  mapUpdatedAt?: Date,
) => Boolean(
  sourceUploaded &&
  mapUpdatedAt &&
  sourceUploaded.getTime() <= mapUpdatedAt.getTime()
);

const sha256Hex = async (input: string | Uint8Array | ArrayBuffer) => {
  const bytes =
    typeof input === 'string'
      ? encoder.encode(input)
      : input instanceof ArrayBuffer
        ? new Uint8Array(input)
        : input;
  const hash = await crypto.subtle.digest(
    'SHA-256',
    bytes.buffer.slice(
      bytes.byteOffset,
      bytes.byteOffset + bytes.byteLength,
    ) as ArrayBuffer,
  );
  return Array.from(new Uint8Array(hash))
    .map(byte => byte.toString(16).padStart(2, '0'))
    .join('');
};

const encodeR2PathSegment = (value: string) =>
  encodeURIComponent(value).replace(/[!'()*]/g, ch =>
    `%${ch.charCodeAt(0).toString(16).toUpperCase()}`);

const canonicalUriForKey = (key = '') =>
  `/${key.split('/').map(encodeR2PathSegment).join('/')}`;

const r2Host = (env: Env) => `${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

const getR2Client = (env: Env) =>
  new AwsClient({
    accessKeyId: env.R2_ACCESS_KEY_ID,
    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
    service: 's3',
    region: 'auto',
  });

const decodeXmlEntities = (value: string) =>
  value
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, '\'')
    .replace(/&amp;/g, '&');

const r2Request = async (
  env: Env,
  method: string,
  key = '',
  {
    query = new URLSearchParams(),
    body,
    headers = {},
    signal,
  }: {
    query?: URLSearchParams
    body?: BodyInit | null
    headers?: Record<string, string>
    signal?: AbortSignal
  } = {},
) => {
  const host = r2Host(env);
  const queryString = query.toString();
  const url =
    `https://${host}/${env.R2_BUCKET}${canonicalUriForKey(key)}` +
    (queryString ? `?${queryString}` : '');
  const requestHeaders = new Headers(headers);
  if (!requestHeaders.has('x-amz-content-sha256')) {
    requestHeaders.set(
      'x-amz-content-sha256',
      body == null
        ? await sha256Hex('')
        : body instanceof ArrayBuffer
          ? await sha256Hex(body)
          : body instanceof Uint8Array
            ? await sha256Hex(body)
            : await sha256Hex(await new Response(body).arrayBuffer()),
    );
  }
  const response = await getR2Client(env).fetch(url, {
    method,
    headers: requestHeaders,
    body,
    signal,
  });
  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    throw new Error(
      `R2 request failed (${response.status} ${response.statusText})` +
      (errorText ? `: ${errorText}` : ''),
    );
  }
  return response;
};

const listAllObjects = async (env: Env) => {
  if (isDriveStorageEnabled(env)) {
    const listUrl = new URL(`${driveApiBaseUrl(env)}/api/v1/storage/list`);
    listUrl.searchParams.set('projectId', env.DRIVE_STORAGE_PROJECT_ID || '');
    listUrl.searchParams.set('bucket', env.DRIVE_STORAGE_BUCKET || '');
    listUrl.searchParams.set('limit', '200000');
    const response = await fetch(listUrl.toString(), {
      headers: driveHeaders(env),
      signal: AbortSignal.timeout(REGISTRATION_STORAGE_TIMEOUT_MS),
    });
    if (!response.ok) {
      throw new Error(`Drive list failed (${response.status})`);
    }
    const data = await response.json() as {
      objects?: Array<{ key: string, uploadedAt?: string | null, size?: number }>
    };
    return (data.objects || []).map(object => ({
      key: object.key,
      uploaded: object.uploadedAt ? new Date(object.uploadedAt) : undefined,
      size: typeof object.size === 'number' ? object.size : undefined,
    }));
  }

  const objects: R2ObjectLike[] = [];
  let continuationToken: string | undefined;

  while (true) {
    const query = new URLSearchParams({
      'list-type': '2',
      'max-keys': '1000',
    });
    if (continuationToken) {
      query.set('continuation-token', continuationToken);
    }

    const response = await r2Request(env, 'GET', '', { query });
    const xml = await response.text();

    const contents = Array.from<RegExpMatchArray>(
      xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g),
    );
    contents.forEach(match => {
      const content = match[1] || '';
      const keyMatch = content.match(/<Key>([\s\S]*?)<\/Key>/);
      if (!keyMatch?.[1]) { return; }
      const lastModifiedMatch =
        content.match(/<LastModified>([\s\S]*?)<\/LastModified>/);
      const sizeMatch = content.match(/<Size>(\d+)<\/Size>/);
      objects.push({
        key: decodeXmlEntities(keyMatch[1]),
        uploaded: lastModifiedMatch?.[1]
          ? new Date(lastModifiedMatch[1])
          : undefined,
        size: sizeMatch?.[1] ? Number(sizeMatch[1]) : undefined,
      });
    });

    const isTruncated = /<IsTruncated>true<\/IsTruncated>/.test(xml);
    const nextTokenMatch =
      xml.match(/<NextContinuationToken>([\s\S]*?)<\/NextContinuationToken>/);
    continuationToken = nextTokenMatch?.[1]
      ? decodeXmlEntities(nextTokenMatch[1])
      : undefined;
    if (!isTruncated || !continuationToken) {
      break;
    }
  }

  return objects;
};

const putObject = async (
  env: Env,
  key: string,
  value: ArrayBuffer,
  contentType: string,
) => {
  if (isDriveStorageEnabled(env)) {
    const response = await fetch(
      `${driveApiBaseUrl(env)}/api/v1/storage/object/${key.split('/').map(encodeURIComponent).join('/')}`,
      {
        method: 'PUT',
        headers: driveHeaders(env, {
          'Content-Type': contentType,
        }),
        body: value,
      },
    );
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      throw new Error(`Drive put failed (${response.status})${text ? `: ${text}` : ''}`);
    }
    return;
  }
  await r2Request(env, 'PUT', key, {
    body: value,
    headers: {
      'content-type': contentType,
    },
  });
};

const deleteObject = async (
  env: Env,
  key: string,
) => {
  if (isDriveStorageEnabled(env)) {
    const response = await fetch(
      `${driveApiBaseUrl(env)}/api/v1/storage/object/${key.split('/').map(encodeURIComponent).join('/')}`,
      {
        method: 'DELETE',
        // A media deletion is an explicit, authenticated destructive action.
        // Ask Drive to clear stale object-operation locks after the object is
        // deleted so an abandoned lock cannot permanently block cleanup.
        headers: driveHeaders(env, {
          'X-Drive-Force-Delete': 'true',
        }),
        signal: AbortSignal.timeout(DELETION_MUTATION_TIMEOUT_MS),
      },
    );
    if (!response.ok) {
      if (response.status === 404) { return; }
      const text = await response.text().catch(() => '');
      throw new Error(`Drive delete failed (${response.status})${text ? `: ${text}` : ''}`);
    }
    return;
  }
  await r2Request(env, 'DELETE', key, {
    signal: AbortSignal.timeout(DELETION_MUTATION_TIMEOUT_MS),
  });
};

export const deleteStorageKeyIfPresent = async ({
  exists,
  remove,
}: {
  exists: () => Promise<boolean>
  remove: () => Promise<void>
}) => {
  if (!await exists()) { return 'already-missing' as const; }
  try {
    await remove();
    return 'deleted' as const;
  } catch (error) {
    if (!await exists()) { return 'already-missing' as const; }
    throw error;
  }
};

type MediaDeletionQueueRow = {
  media_id: string
  title?: string | null
  urls?: unknown
  prefixes?: unknown
  attempts?: number
};

let mediaDeletionQueueTableReady: Promise<void> | undefined;
const ensureMediaDeletionQueueTable = async (env: Env) => {
  if (!mediaDeletionQueueTableReady) {
    const sql = sqlForEnv(env);
    mediaDeletionQueueTableReady = sql`
      CREATE TABLE IF NOT EXISTS media_deletion_queue (
        media_id TEXT PRIMARY KEY,
        title TEXT,
        urls JSONB NOT NULL DEFAULT '[]'::jsonb,
        prefixes JSONB NOT NULL DEFAULT '[]'::jsonb,
        status VARCHAR(16) NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        claimed_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      )
    `.then(() => undefined);
  }
  try {
    await mediaDeletionQueueTableReady;
  } catch (error) {
    mediaDeletionQueueTableReady = undefined;
    throw error;
  }
};

const stringArray = (value: unknown) => {
  if (Array.isArray(value)) {
    return value.filter((item): item is string => typeof item === 'string');
  }
  if (typeof value === 'string') {
    try {
      return stringArray(JSON.parse(value));
    } catch {
      return [];
    }
  }
  return [];
};

export const deletionKeyMatchesPrefix = (key: string, prefix: string) => {
  if (prefix.includes('/')) {
    const keyWithoutExtension = key.replace(/\.[^/.]+$/, '');
    return keyWithoutExtension === prefix ||
      keyWithoutExtension.startsWith(`${prefix}-`);
  }
  const { fileNameBase } = getFileParts(key);
  return fileNameBase === prefix || fileNameBase.startsWith(`${prefix}-`);
};

const deletionPrefixForKey = (key: string) => key.replace(/\.[^/.]+$/, '');

export const buildDeletionPrefixes = (
  mediaId: string,
  queuedPrefixes: string[],
  explicitKeys: string[],
) => {
  const explicitPrefixes = explicitKeys.map(deletionPrefixForKey);
  const explicitNestedBases = new Set(explicitPrefixes
    .filter(prefix => prefix.includes('/'))
    .map(prefix => getFileParts(prefix).fileNameBase));
  return Array.from(new Set([
    mediaId,
    ...explicitPrefixes,
    ...queuedPrefixes,
  ].filter(prefix => {
    if (!prefix) { return false; }
    if (prefix.includes('/')) { return true; }
    if (prefix === mediaId) { return true; }
    if (prefix !== mediaId && prefix.startsWith(`${mediaId}-`)) {
      return false;
    }
    return !explicitNestedBases.has(prefix);
  })));
};

const listObjectsForPrefix = async (env: Env, prefix: string) => {
  if (isDriveStorageEnabled(env)) {
    return listDriveKeysForPrefix(env, prefix);
  }

  const keys: string[] = [];
  let continuationToken: string | undefined;
  while (true) {
    const query = new URLSearchParams({
      'list-type': '2',
      'max-keys': '1000',
      prefix,
    });
    if (continuationToken) {
      query.set('continuation-token', continuationToken);
    }
    const response = await r2Request(env, 'GET', '', {
      query,
      signal: AbortSignal.timeout(DELETION_STORAGE_TIMEOUT_MS),
    });
    const xml = await response.text();
    for (const match of xml.matchAll(/<Key>([\s\S]*?)<\/Key>/g)) {
      if (match[1]) { keys.push(decodeXmlEntities(match[1])); }
    }
    const isTruncated = /<IsTruncated>true<\/IsTruncated>/.test(xml);
    const nextToken = xml.match(
      /<NextContinuationToken>([\s\S]*?)<\/NextContinuationToken>/,
    )?.[1];
    continuationToken = nextToken ? decodeXmlEntities(nextToken) : undefined;
    if (!isTruncated || !continuationToken) { break; }
  }
  return keys;
};

const claimMediaDeletion = async (env: Env) => {
  await ensureMediaDeletionQueueTable(env);
  const sql = sqlForEnv(env);
  return (await sql`
    WITH candidate AS (
      SELECT media_id
      FROM media_deletion_queue
      WHERE status IN ('pending', 'failed')
        OR (
          status='processing' AND
          claimed_at < now() - interval '5 minutes'
        )
      ORDER BY created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE media_deletion_queue queue
    SET
      status='processing',
      attempts=attempts + 1,
      error_message=NULL,
      claimed_at=now(),
      updated_at=now()
    FROM candidate
    WHERE queue.media_id=candidate.media_id
    RETURNING queue.*
  ` as unknown as MediaDeletionQueueRow[])[0];
};

const cleanupDeletedMediaRecords = async (
  env: Env,
  mediaId: string,
  urls: string[],
) => {
  const sql = sqlForEnv(env);
  await ensureRegistrationStatusTable(env);
  await ensureRegisteredUploadFileMapTable(env);
  await ensureUploadRegistrationHintsTable(env);
  const tables = await sql`
    SELECT
      to_regclass('public.album_media')::text AS album_media,
      to_regclass('public.auth_user_favorites')::text AS favorites
  ` as unknown as Array<{
    album_media?: string | null
    favorites?: string | null
  }>;
  if (tables[0]?.favorites) {
    await sql`DELETE FROM auth_user_favorites WHERE media_id=${mediaId}`;
  }
  if (tables[0]?.album_media) {
    await sql`DELETE FROM album_media WHERE media_id=${mediaId}`;
  }
  if (urls.length > 0) {
    await sql`
      DELETE FROM worker_registration_status
      WHERE media_id=${mediaId}
        OR url = ANY(${urls})
        OR source_url = ANY(${urls})
    `;
    await sql`
      DELETE FROM upload_registration_hints
      WHERE url = ANY(${urls})
    `;
  } else {
    await sql`DELETE FROM worker_registration_status WHERE media_id=${mediaId}`;
  }
  await sql`DELETE FROM registered_upload_file_map WHERE media_id=${mediaId}`;
  await sql`DELETE FROM media WHERE id=${mediaId}`;
};

const processMediaDeletion = async (
  env: Env,
  deletion: MediaDeletionQueueRow,
) => {
  const mediaId = deletion.media_id;
  const urls = stringArray(deletion.urls);
  const explicitKeys = urls
    .map(url => keyFromStorageUrl(env, url))
    .filter(Boolean);
  const prefixes = buildDeletionPrefixes(
    mediaId,
    stringArray(deletion.prefixes),
    explicitKeys,
  );

  await logBackendActivity(env, {
    category: 'orchestrator',
    event: 'deletion_started',
    status: 'info',
    message: `Deleting ${deletion.title || mediaId}`,
    mediaId,
    details: { prefixes, attempt: deletion.attempts || 1 },
  });

  try {
    const keys = new Set(explicitKeys);
    const listedPrefixKeys = await Promise.all(prefixes.map(async prefix => {
      try {
        return { prefix, keys: await listObjectsForPrefix(env, prefix) };
      } catch (error) {
        throw new Error(`Deletion discovery failed for ${prefix}: ${error}`);
      }
    }));
    for (const { prefix, keys: listedKeys } of listedPrefixKeys) {
      listedKeys
        .filter(key => deletionKeyMatchesPrefix(key, prefix))
        .forEach(key => keys.add(key));
    }

    // Drive and R2 deletes are idempotent. Use small parallel batches so slow
    // Drive bookkeeping does not multiply request time for related objects.
    const keysToDelete = Array.from(keys);
    for (
      let index = 0;
      index < keysToDelete.length;
      index += DELETION_MUTATION_CONCURRENCY
    ) {
      await Promise.all(keysToDelete
        .slice(index, index + DELETION_MUTATION_CONCURRENCY)
        .map(async key => {
          try {
            await deleteObject(env, key);
          } catch (error) {
            // A storage gateway can fail after the authoritative object delete
            // has already committed (for example, during lock/inventory
            // bookkeeping). Confirm the object itself before failing the
            // whole media deletion and retrying already-removed keys forever.
            try {
              if (!await storageObjectExists(
                env,
                key,
                DELETION_STORAGE_TIMEOUT_MS,
              )) {
                return;
              }
            } catch {
              // Preserve the original mutation failure when verification is
              // unavailable; a later queue attempt can safely retry it.
            }
            throw new Error(`Storage delete failed for ${key}: ${error}`);
          }
        }));
    }

    const remaining = new Set<string>();
    const remainingPrefixKeys = await Promise.all(prefixes.map(async prefix => {
      try {
        return { prefix, keys: await listObjectsForPrefix(env, prefix) };
      } catch (error) {
        throw new Error(`Deletion verification failed for ${prefix}: ${error}`);
      }
    }));
    for (const { prefix, keys: listedKeys } of remainingPrefixKeys) {
      listedKeys
        .filter(key => deletionKeyMatchesPrefix(key, prefix))
        .forEach(key => remaining.add(key));
    }
    if (remaining.size > 0) {
      throw new Error(
        `Storage cleanup incomplete: ${remaining.size} related object(s) remain`,
      );
    }

    await cleanupDeletedMediaRecords(env, mediaId, urls);
    const sql = sqlForEnv(env);
    await sql`DELETE FROM media_deletion_queue WHERE media_id=${mediaId}`;
    await logBackendActivity(env, {
      category: 'orchestrator',
      event: 'deletion_completed',
      status: 'success',
      message: `Deleted ${deletion.title || mediaId} and ${keys.size} object(s)`,
      mediaId,
      details: { deletedObjects: keys.size, prefixes },
    });
    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Deletion failed';
    const sql = sqlForEnv(env);
    await sql`
      UPDATE media_deletion_queue
      SET status='failed', error_message=${message}, updated_at=now()
      WHERE media_id=${mediaId}
    `;
    await logBackendActivity(env, {
      category: 'orchestrator',
      event: 'deletion_failed',
      status: 'error',
      message,
      mediaId,
      details: { attempt: deletion.attempts || 1 },
    });
    return false;
  }
};

const getDeletionQueueCounts = async (env: Env) => {
  await ensureMediaDeletionQueueTable(env);
  const sql = sqlForEnv(env);
  const rows = await sql`
    SELECT status, COUNT(*)::int AS count
    FROM media_deletion_queue
    GROUP BY status
  ` as unknown as Array<{ status: string, count: number }>;
  return rows.reduce<Record<string, number>>((counts, row) => {
    counts[row.status] = row.count;
    return counts;
  }, {});
};

let lastKnownQueuedDeletionPrefixes = new Set<string>();
const getQueuedDeletionPrefixes = async (env: Env) => {
  try {
    await ensureMediaDeletionQueueTable(env);
    const sql = sqlForEnv(env);
    const rows = await sql`
      SELECT prefixes FROM media_deletion_queue
    ` as unknown as Array<{ prefixes?: unknown }>;
    lastKnownQueuedDeletionPrefixes = new Set(
      rows.flatMap(row => stringArray(row.prefixes)),
    );
  } catch (error) {
    // A transient deletion-queue connection failure must not keep every
    // unrelated upload stuck in `detected`. Retain the most recently known
    // deletion prefixes so a previously queued deletion stays protected.
    console.warn(
      'Unable to read deletion queue; continuing registration with cached prefixes',
      error,
    );
  }
  return new Set(lastKnownQueuedDeletionPrefixes);
};

const drainMediaDeletionQueue = async (env: Env) => {
  let processed = 0;
  for (let index = 0; index < 50; index += 1) {
    const deletion = await claimMediaDeletion(env);
    if (!deletion) { break; }
    const completed = await processMediaDeletion(env, deletion);
    processed += 1;
    if (!completed) { break; }
  }
  return processed;
};

const copyObject = async (
  env: Env,
  sourceKey: string,
  destinationKey: string,
) => {
  if (isDriveStorageEnabled(env)) {
    try {
      const response = await fetch(
        `${driveApiBaseUrl(env)}/api/v1/storage/copy`,
        {
          method: 'POST',
          signal: AbortSignal.timeout(DRIVE_COPY_REQUEST_TIMEOUT_MS),
          headers: driveHeaders(env, {
            'Content-Type': 'application/json',
          }),
          body: JSON.stringify({
            projectId: env.DRIVE_STORAGE_PROJECT_ID,
            bucket: env.DRIVE_STORAGE_BUCKET,
            fromKey: sourceKey,
            toKey: destinationKey,
          }),
        },
      );
      if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new Error(
          `Drive copy failed (${response.status})${text ? `: ${text}` : ''}`,
        );
      }
      return;
    } catch (error) {
      if (
        isDriveTimeoutLikeError(error) &&
        await waitForDriveDestination(env, destinationKey)
      ) {
        return;
      }
      throw error;
    }
  }
  await r2Request(env, 'PUT', destinationKey, {
    headers: {
      'x-amz-copy-source':
        `${env.R2_BUCKET}/${canonicalUriForKey(sourceKey).slice(1)}`,
    },
  });
};

const copyAndVerifyObject = async (
  env: Env,
  sourceKey: string,
  destinationKey: string,
  expectedSize?: number,
) => {
  await copyObject(env, sourceKey, destinationKey);

  const sourceSize = expectedSize === undefined
    ? await storageObjectSize(env, sourceKey)
    : expectedSize;
  const destinationSize = await waitForVerifiedStorageCopy({
    sourceSize,
    readDestinationSize: () => storageObjectSize(env, destinationKey),
    attempts: isDriveStorageEnabled(env)
      ? DRIVE_COPY_VISIBILITY_ATTEMPTS
      : 1,
    delayMs: isDriveStorageEnabled(env)
      ? DRIVE_COPY_VISIBILITY_DELAY_MS
      : 0,
  });
  if (!isVerifiedStorageCopy(sourceSize, destinationSize)) {
    if (destinationSize === undefined) {
      throw new Error(
        `Copied destination is not readable in storage: ${destinationKey}`,
      );
    }
    throw new Error(
      `Copied destination size mismatch: source=${sourceSize} destination=${destinationSize}`,
    );
  }
};

const buildRegistrationKey = (
  env: Env,
  sourceKey: string,
  mediaId: string,
  extension: string,
) => {
  if (!areUniqueMediaNamesEnabled(env)) {
    return sourceKey;
  }

  const segments = sourceKey.split('/');
  segments[segments.length - 1] = `${mediaId}.${extension}`;
  return segments.join('/');
};

const getMediaRows = async (env: Env) => {
  const sql = sqlForEnv(env);
  return (await sql`
    SELECT id, url, extension, poster_url, preview_url, transcode_status
    FROM media
  `) as unknown as MediaRow[];
};

const PROCESSING_SOURCE_MISSING_ERROR =
  'Source file is missing from storage. Upload a replacement to retry.';

export const shouldMarkProcessingSourceMissing = ({
  status,
  sourceKey,
  isListed,
  exists,
}: {
  status?: string | null
  sourceKey: string
  isListed: boolean
  exists?: boolean
}) =>
  (status === 'pending' || status === 'processing') &&
  (!sourceKey || (!isListed && exists === false));

const processingSourceExists = async (env: Env, key: string) => {
  if (isDriveStorageEnabled(env)) {
    const keys = await listDriveKeysForPrefix(
      env,
      key,
      DELETION_STORAGE_TIMEOUT_MS,
    );
    return keys.includes(key);
  }
  return storageObjectExists(env, key, DELETION_STORAGE_TIMEOUT_MS);
};

const markProcessingSourceMissing = async (env: Env, mediaId: string) => {
  const sql = sqlForEnv(env);
  await sql`
    UPDATE media
    SET
      transcode_status='failed',
      transcode_error=${PROCESSING_SOURCE_MISSING_ERROR},
      updated_at=now()
    WHERE id=${mediaId}
      AND transcode_status IN ('pending', 'processing')
  `;
  await logBackendActivity(env, {
    category: 'processing',
    event: 'source_missing',
    status: 'error',
    message: PROCESSING_SOURCE_MISSING_ERROR,
    mediaId,
  });
  await revalidateMediaPanel(env, mediaId).catch(() => undefined);
};

const reconcileMissingProcessingSources = async (
  env: Env,
  rows: MediaRow[],
  listedKeys: Set<string>,
) => {
  let missing = 0;
  for (const row of rows) {
    if (row.transcode_status !== 'pending' &&
      row.transcode_status !== 'processing') {
      continue;
    }
    const sourceKey = keyFromStorageUrl(env, row.url);
    const isListed = Boolean(sourceKey && listedKeys.has(sourceKey));
    let exists: boolean | undefined;
    if (sourceKey && !isListed) {
      try {
        exists = await processingSourceExists(env, sourceKey);
      } catch (error) {
        console.warn('Skipping processing source reconciliation', {
          mediaId: row.id,
          sourceKey,
          error,
        });
        continue;
      }
    }
    if (shouldMarkProcessingSourceMissing({
      status: row.transcode_status,
      sourceKey,
      isListed,
      exists,
    })) {
      await markProcessingSourceMissing(env, row.id);
      missing += 1;
    }
  }
  return missing;
};

const countPendingVideos = async (env: Env) => {
  const sql = sqlForEnv(env);
  const rows = (await sql`
    SELECT COUNT(*)::int AS count
    FROM media
    WHERE transcode_status='pending'
  `) as unknown as { count: number }[];
  return rows[0]?.count ?? 0;
};

const getExpectedRegistrationKeyForHint = async (
  env: Env,
  hintKey: string,
) => {
  const { extension } = getFileParts(hintKey);
  if (!MEDIA_EXTENSIONS.has(extension)) {
    return undefined;
  }

  const mediaId = await mediaIdForObject(env, hintKey);
  const registrationKey = buildRegistrationKey(
    env,
    hintKey,
    mediaId,
    extension,
  );
  return registrationKey === hintKey ? undefined : registrationKey;
};

const getExpectedRegistrationUrlForStatusRow = async (
  env: Env,
  row: RegistrationStatusRow,
) => {
  const baseUrl = trimToUndefined(row.source_url) || trimToUndefined(row.url);
  if (!baseUrl) { return undefined; }
  const sourceKey = keyFromStorageUrl(env, baseUrl);
  if (!sourceKey) { return undefined; }
  const extension =
    trimToUndefined(row.extension) ||
    trimToUndefined(getFileParts(sourceKey).extension);
  if (!extension || !MEDIA_EXTENSIONS.has(extension)) {
    return undefined;
  }
  const mediaId = trimToUndefined(row.media_id) || await mediaIdForObject(
    env,
    sourceKey,
  );
  return urlForKey(
    env,
    buildRegistrationKey(env, sourceKey, mediaId, extension),
  );
};

const buildRegistrationStatusLookup = async (
  env: Env,
  rows: RegistrationStatusRow[],
) => {
  const byUrl = new Map<string, RegistrationStatusRow>();
  for (const row of rows) {
    byUrl.set(row.url, row);
    const sourceUrl = trimToUndefined(row.source_url);
    if (sourceUrl) {
      byUrl.set(sourceUrl, row);
    }
    const expectedUrl = await getExpectedRegistrationUrlForStatusRow(env, row)
      .catch(() => undefined);
    if (expectedUrl) {
      byUrl.set(expectedUrl, row);
    }
  }
  return byUrl;
};

const waitForDriveDestination = async (
  env: Env,
  destinationKey: string,
  {
    attempts = DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS,
    delayMs = DRIVE_COPY_VISIBILITY_DELAY_MS,
  }: {
    attempts?: number
    delayMs?: number
  } = {},
) => {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (await driveObjectExists(env, destinationKey).catch(() => false)) {
      return true;
    }
    if (attempt < attempts - 1) {
      await sleep(delayMs);
    }
  }
  return false;
};

let uploadRegistrationHintsTableReady: Promise<void> | undefined;
const ensureUploadRegistrationHintsTable = async (env: Env) => {
  if (!uploadRegistrationHintsTableReady) {
    const sql = sqlForEnv(env);
    uploadRegistrationHintsTableReady = sql`
      CREATE TABLE IF NOT EXISTS upload_registration_hints (
        url TEXT PRIMARY KEY,
        original_file_name TEXT,
        title TEXT,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      )
    `.then(() => undefined);
  }
  try {
    await uploadRegistrationHintsTableReady;
  } catch (error) {
    uploadRegistrationHintsTableReady = undefined;
    throw error;
  }
};

const ensureUploadRegistrationHintsColumnTypes = async (env: Env) => {
  // The CREATE statement above is the steady-state schema contract. Older
  // deployments already completed the one-time VARCHAR-to-TEXT migration;
  // repeating ALTER TABLE in every fresh Worker isolate wastes subrequests.
  await ensureUploadRegistrationHintsTable(env);
};

const getUploadRegistrationHints = async (env: Env, urls: string[]) => {
  if (urls.length === 0) {
    return new Map<string, UploadRegistrationHintRow>();
  }
  try {
    await ensureUploadRegistrationHintsTable(env);
    await ensureUploadRegistrationHintsColumnTypes(env);
  } catch (error) {
    console.warn('Upload registration hints unavailable; continuing without hints', error);
    return new Map<string, UploadRegistrationHintRow>();
  }
  const sql = sqlForEnv(env);
  const requestedUrls = new Map(urls.map(url => [url, {
    canonical: canonicalizeStorageUrl(url),
    decoded: decodeStorageUrlPath(url),
  }]));
  const lookupUrls = Array.from(new Set(
    Array.from(requestedUrls.values()).flatMap(({ canonical, decoded }) => [
      canonical,
      decoded,
    ]),
  ));
  const rows: UploadRegistrationHintRow[] = [];
  // Keep optional metadata lookups small. A large ANY(array) query can be
  // dropped by a Supabase pooler; losing hints must never stop registration.
  for (let offset = 0; offset < lookupUrls.length; offset += 200) {
    const chunk = lookupUrls.slice(offset, offset + 200);
    try {
      const chunkRows = (await sql`
        SELECT url, original_file_name, title
        FROM upload_registration_hints
        WHERE url = ANY(${chunk})
      `) as unknown as UploadRegistrationHintRow[];
      rows.push(...chunkRows);
    } catch (error) {
      console.warn('Upload registration hint chunk unavailable; continuing without it', {
        offset,
        error,
      });
    }
  }
  const rowsByUrl = new Map(rows.map(row => [row.url, row]));
  const rowsByCanonicalUrl = new Map(
    rows.map(row => [canonicalizeStorageUrl(row.url), row]),
  );
  const rowsByDecodedUrl = new Map(
    rows.map(row => [decodeStorageUrlPath(row.url), row]),
  );

  const resolved = new Map<string, UploadRegistrationHintRow>();
  urls.forEach(url => {
    const variants = requestedUrls.get(url);
    if (!variants) { return; }
    const row =
      rowsByUrl.get(url) ||
      rowsByUrl.get(variants.canonical) ||
      rowsByUrl.get(variants.decoded) ||
      rowsByCanonicalUrl.get(variants.canonical) ||
      rowsByDecodedUrl.get(variants.decoded);
    if (row) {
      resolved.set(url, row);
    }
  });
  return resolved;
};

const getPendingUploadRegistrationHints = async (env: Env) => {
  try {
    await ensureUploadRegistrationHintsTable(env);
    await ensureUploadRegistrationHintsColumnTypes(env);
    const sql = sqlForEnv(env);
    return (await sql`
      SELECT url, original_file_name, title, updated_at, created_at
      FROM upload_registration_hints
      ORDER BY updated_at DESC
    `) as unknown as UploadRegistrationHintRow[];
  } catch (error) {
    console.warn('Pending upload registration hints unavailable; continuing without hints', error);
    return [];
  }
};

const replaceUploadRegistrationHintUrl = async (
  env: Env,
  previousUrl: string,
  nextUrl: string,
) => {
  if (previousUrl === nextUrl) { return; }
  await ensureUploadRegistrationHintsTable(env);
  await ensureUploadRegistrationHintsColumnTypes(env);
  const sql = sqlForEnv(env);
  const rows = (await sql`
    SELECT url, original_file_name, title
    FROM upload_registration_hints
    WHERE url = ANY(${[previousUrl, nextUrl]})
  `) as unknown as UploadRegistrationHintRow[];
  const previousRow = rows.find(row => row.url === previousUrl);
  if (!previousRow) { return; }
  const nextRow = rows.find(row => row.url === nextUrl);
  const originalFileName =
    trimToUndefined(previousRow.original_file_name) ||
    trimToUndefined(nextRow?.original_file_name);
  const title =
    trimToUndefined(previousRow.title) ||
    trimToUndefined(nextRow?.title);
  await sql`
    INSERT INTO upload_registration_hints (url, original_file_name, title)
    VALUES (${nextUrl}, ${originalFileName ?? null}, ${title ?? null})
    ON CONFLICT (url) DO UPDATE SET
      original_file_name=COALESCE(
        EXCLUDED.original_file_name,
        upload_registration_hints.original_file_name
      ),
      title=COALESCE(EXCLUDED.title, upload_registration_hints.title),
      updated_at=now()
  `;
  await sql`
    DELETE FROM upload_registration_hints
    WHERE url=${previousUrl}
  `;
};

const clearUploadRegistrationHint = async (env: Env, url: string) => {
  return clearUploadRegistrationHints(env, [url]);
};

const clearUploadRegistrationHints = async (env: Env, urls: string[]) => {
  const uniqueUrls = Array.from(new Set(urls.filter(Boolean)));
  if (uniqueUrls.length === 0) { return; }
  await ensureUploadRegistrationHintsTable(env);
  await ensureUploadRegistrationHintsColumnTypes(env);
  const sql = sqlForEnv(env);
  await sql`
    DELETE FROM upload_registration_hints
    WHERE url = ANY(${uniqueUrls})
  `;
};

const clearTrackedRegistration = async (
  env: Env,
  urls: Array<string | undefined>,
) => {
  const trackedUrls = Array.from(new Set(
    urls
      .map(url => trimToUndefined(url))
      .filter((url): url is string => Boolean(url)),
  ));
  if (trackedUrls.length === 0) { return; }

  await ensureRegistrationStatusTable(env);
  await ensureUploadRegistrationHintsTable(env);
  await ensureUploadRegistrationHintsColumnTypes(env);
  const sql = sqlForEnv(env);
  await sql`
    DELETE FROM worker_registration_status
    WHERE url = ANY(${trackedUrls})
      OR source_url = ANY(${trackedUrls})
  `;
  await sql`
    DELETE FROM upload_registration_hints
    WHERE url = ANY(${trackedUrls})
  `;
};

let registrationStatusTableReady: Promise<void> | undefined;
const ensureRegistrationStatusTable = async (env: Env) => {
  if (!registrationStatusTableReady) {
    const sql = sqlForEnv(env);
    registrationStatusTableReady = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS worker_registration_status (
          url TEXT PRIMARY KEY,
          file_name TEXT,
          uploaded_at TIMESTAMP WITH TIME ZONE,
          status VARCHAR(32) NOT NULL,
          source_url TEXT,
          original_file_name TEXT,
          title TEXT,
          media_id TEXT,
          extension TEXT,
          error_message TEXT,
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
      `;
    })();
  }
  try {
    await registrationStatusTableReady;
  } catch (error) {
    registrationStatusTableReady = undefined;
    throw error;
  }
};

const clearStaleRegistrationStatuses = async (env: Env) => {
  const sql = sqlForEnv(env);
  const minutes = getNumber(env.STALE_REGISTRATION_MINUTES, 15, {
    min: 1,
    max: 24 * 60,
  });
  // Remove the misleading legacy marker from rows that were never claimed.
  // Their normal state is detected, and they remain eligible for FIFO work.
  await sql`
    UPDATE worker_registration_status
    SET error_message=NULL
    WHERE status='detected'
      AND error_message=${STALE_REGISTRATION_ERROR_MESSAGE}
  `;
  await sql`
    UPDATE worker_registration_status
    SET
      status='detected',
      error_message=${STALE_REGISTRATION_ERROR_MESSAGE},
      updated_at=now()
    -- A detected row has not started an attempt. Rewriting it as "stalled"
    -- made an idle backlog look like every file had failed. Only recover a
    -- file that was actually claimed and left in registering.
    WHERE status='registering'
      AND updated_at < now() - (${String(minutes)} || ' minutes')::interval
  `;
};

const clearOldCompletedRegistrationStatuses = async (env: Env) => {
  const sql = sqlForEnv(env);
  const days = getNumber(env.REGISTRATION_HISTORY_DAYS, 14, {
    min: 1,
    max: 365,
  });
  await sql`
    DELETE FROM worker_registration_status
    WHERE status IN ('registered', 'error')
      AND updated_at < now() - (${String(days)} || ' days')::interval
  `;
};

const clearResolvedRegistrationStatuses = async (env: Env) => {
  const sql = sqlForEnv(env);
  await sql`
    DELETE FROM worker_registration_status s
    WHERE EXISTS (
      SELECT 1
      FROM media m
      WHERE (s.media_id IS NOT NULL AND m.id=s.media_id)
        OR m.url=s.url
        OR (s.source_url IS NOT NULL AND m.url=s.source_url)
    )
      OR EXISTS (
        SELECT 1
        FROM registered_upload_file_map f
        WHERE (s.media_id IS NOT NULL AND f.media_id=s.media_id)
          OR f.stored_url=s.url
          OR (
            s.source_url IS NOT NULL
            AND (f.source_url=s.source_url OR f.stored_url=s.source_url)
          )
      )
  `;
};

const getRegistrationStatusRows = async (env: Env) => {
  await ensureRegistrationStatusTable(env);
  const sql = sqlForEnv(env);
  return (await sql`
    SELECT
      url,
      file_name,
      uploaded_at,
      status,
      source_url,
      original_file_name,
      title,
      media_id,
      extension,
      error_message
    FROM worker_registration_status
  `) as unknown as RegistrationStatusRow[];
};

const getTrackedRegistrationStatuses = async (env: Env) => {
  await ensureRegistrationStatusTable(env);
  await ensureRegisteredUploadFileMapTable(env);
  await clearStaleRegistrationStatuses(env);
  await clearResolvedRegistrationStatuses(env);
  await clearOldCompletedRegistrationStatuses(env);
  return getRegistrationStatusRows(env);
};

const setRegistrationStatus = async (
  env: Env,
  row: RegistrationStatusWrite,
) => upsertRegistrationStatuses(env, [row]);

type RegistrationStatusWrite = {
  url: string
  fileName?: string
  uploadedAt?: string
  status: 'detected' | 'registering' | 'registered' | 'error'
  sourceUrl?: string
  originalFileName?: string
  title?: string
  mediaId?: string
  extension?: string
  errorMessage?: string
};

type HlsArtifactMetadata = {
  key?: string
  size?: number
  contentType?: string
};

const REGISTRATION_STATUS_WRITE_BATCH_SIZE = 25;
const upsertRegistrationStatusBatch = async (
  env: Env,
  rows: RegistrationStatusWrite[],
) => {
  const payload = JSON.stringify(rows.map(({
    url,
    fileName,
    uploadedAt,
    status,
    sourceUrl,
    originalFileName,
    title,
    mediaId,
    extension,
    errorMessage,
  }) => ({
    url,
    file_name: fileName ?? null,
    uploaded_at: uploadedAt ?? null,
    status,
    source_url: sourceUrl ?? null,
    original_file_name: originalFileName ?? null,
    title: title ?? null,
    media_id: mediaId ?? null,
    extension: extension ?? null,
    error_message: errorMessage ?? null,
  })));
  const sql = sqlForEnv(env);
  await sql`
    WITH incoming AS (
      SELECT *
      FROM jsonb_to_recordset(${payload}::jsonb) AS row(
        url TEXT,
        file_name TEXT,
        uploaded_at TIMESTAMP WITH TIME ZONE,
        status VARCHAR(32),
        source_url TEXT,
        original_file_name TEXT,
        title TEXT,
        media_id TEXT,
        extension TEXT,
        error_message TEXT
      )
    )
    INSERT INTO worker_registration_status (
      url,
      file_name,
      uploaded_at,
      status,
      source_url,
      original_file_name,
      title,
      media_id,
      extension,
      error_message
    )
    SELECT
      url,
      file_name,
      uploaded_at,
      status,
      source_url,
      original_file_name,
      title,
      media_id,
      extension,
      error_message
    FROM incoming
    ON CONFLICT (url) DO UPDATE SET
      file_name=COALESCE(EXCLUDED.file_name, worker_registration_status.file_name),
      uploaded_at=COALESCE(EXCLUDED.uploaded_at, worker_registration_status.uploaded_at),
      source_url=COALESCE(EXCLUDED.source_url, worker_registration_status.source_url),
      original_file_name=COALESCE(EXCLUDED.original_file_name, worker_registration_status.original_file_name),
      title=COALESCE(EXCLUDED.title, worker_registration_status.title),
      media_id=COALESCE(EXCLUDED.media_id, worker_registration_status.media_id),
      extension=COALESCE(EXCLUDED.extension, worker_registration_status.extension),
      error_message=CASE
        WHEN worker_registration_status.status='registering'
          AND EXCLUDED.status='detected'
          THEN worker_registration_status.error_message
        ELSE EXCLUDED.error_message
      END,
      status=CASE
        WHEN worker_registration_status.status='registering'
          AND EXCLUDED.status='detected'
          THEN worker_registration_status.status
        ELSE EXCLUDED.status
      END,
      updated_at=now()
  `;
};

const upsertRegistrationStatuses = async (
  env: Env,
  rows: RegistrationStatusWrite[],
) => {
  for (
    let offset = 0;
    offset < rows.length;
    offset += REGISTRATION_STATUS_WRITE_BATCH_SIZE
  ) {
    await upsertRegistrationStatusBatch(
      env,
      rows.slice(offset, offset + REGISTRATION_STATUS_WRITE_BATCH_SIZE),
    );
  }
};

const clearRegistrationStatus = async (env: Env, url: string) => {
  const sql = sqlForEnv(env);
  await sql`
    DELETE FROM worker_registration_status
    WHERE url=${url} OR source_url=${url}
  `;
};

const clearRegistrationTrackingAfterSuccess = async (
  env: Env,
  {
    mediaId,
    urls,
  }: {
    mediaId: string
    urls: string[]
  },
) => {
  const sql = sqlForEnv(env);
  const uniqueUrls = Array.from(new Set(urls.filter(Boolean)));
  await sql`
    DELETE FROM worker_registration_status
    WHERE media_id=${mediaId}
      OR url = ANY(${uniqueUrls})
      OR source_url = ANY(${uniqueUrls})
  `;
};

const replaceRegistrationStatusUrl = async (
  env: Env,
  previousUrl: string,
  nextUrl: string,
) => {
  if (previousUrl === nextUrl) {
    await setRegistrationStatus(env, { url: nextUrl, status: 'registering' });
    return;
  }
  const sql = sqlForEnv(env);
  await sql`
    INSERT INTO worker_registration_status (
      url,
      file_name,
      uploaded_at,
      status,
      source_url,
      original_file_name,
      title,
      media_id,
      extension,
      error_message
    )
    SELECT
      ${nextUrl},
      file_name,
      uploaded_at,
      'registering',
      source_url,
      original_file_name,
      title,
      media_id,
      extension,
      NULL
    FROM worker_registration_status
    WHERE url=${previousUrl}
    ON CONFLICT (url) DO UPDATE SET
      file_name=COALESCE(EXCLUDED.file_name, worker_registration_status.file_name),
      uploaded_at=COALESCE(EXCLUDED.uploaded_at, worker_registration_status.uploaded_at),
      status='registering',
      source_url=COALESCE(EXCLUDED.source_url, worker_registration_status.source_url),
      original_file_name=COALESCE(EXCLUDED.original_file_name, worker_registration_status.original_file_name),
      title=COALESCE(EXCLUDED.title, worker_registration_status.title),
      media_id=COALESCE(EXCLUDED.media_id, worker_registration_status.media_id),
      extension=COALESCE(EXCLUDED.extension, worker_registration_status.extension),
      error_message=NULL,
      updated_at=now()
  `;
  await sql`
    DELETE FROM worker_registration_status
    WHERE url=${previousUrl}
  `;
  await setRegistrationStatus(env, {
    url: nextUrl,
    status: 'registering',
  });
};

const syncDetectedStatuses = async (
  env: Env,
  pending: R2ObjectLike[],
  registrationRowsByUrl: Map<string, RegistrationStatusRow>,
) => {
  const pendingUrls = pending.map(object => urlForKey(env, object.key));
  const hintsByUrl = await getUploadRegistrationHints(env, pendingUrls);
  const pendingByUrl = new Map<string, {
    fileName: string
    uploadedAt?: string
    sourceUrl: string
    title?: string
  }>();

  pending.forEach(object => {
    const pendingUrl = urlForKey(env, object.key);
    const hint = hintsByUrl.get(pendingUrl);
    const statusRow = registrationRowsByUrl.get(pendingUrl);
    const originalFileName =
      resolveRegistrationOriginalFileName({
        hint,
        statusRow,
        fallbackFileName: getFileParts(object.key).fileName,
      }) || getFileParts(object.key).fileName;
    pendingByUrl.set(pendingUrl, {
      fileName: originalFileName,
      uploadedAt: object.uploaded?.toISOString(),
      sourceUrl: resolveRegistrationSourceUrl(statusRow, pendingUrl),
      title: resolveRegistrationTitle({
        originalFileName,
        fallbackFileName: getFileParts(object.key).fileName,
      }),
    });
  });

  const newStatuses = Array.from(pendingByUrl.entries())
    .filter(([url]) => !registrationRowsByUrl.has(url))
    .map(([url, pendingRow]) => ({
      url,
      fileName: pendingRow.fileName,
      uploadedAt: pendingRow.uploadedAt,
      status: 'detected',
      sourceUrl: pendingRow.sourceUrl,
      originalFileName: pendingRow.fileName,
      title: pendingRow.title,
      errorMessage: undefined,
    }));
  await upsertRegistrationStatuses(env, newStatuses);
};

const requeueRegistrationStatuses = async (env: Env, urls: string[]) => {
  const uniqueUrls = Array.from(new Set(urls
    .map(url => trimToUndefined(url))
    .filter((url): url is string => Boolean(url))));
  if (uniqueUrls.length === 0) { return 0; }
  await ensureRegistrationStatusTable(env);
  const sql = sqlForEnv(env);
  const rows = await sql`
    UPDATE worker_registration_status
    SET
      status='detected',
      error_message=NULL,
      updated_at=now()
    WHERE url = ANY(${uniqueUrls})
      OR source_url = ANY(${uniqueUrls})
    RETURNING url
  ` as unknown as Array<{ url: string }>;
  return rows.length;
};

const retryStaleProcessing = async (env: Env) => {
  const sql = sqlForEnv(env);
  const minutes = getNumber(env.STALE_PROCESSING_MINUTES, 15, {
    min: 1,
    max: 24 * 60,
  });
  const rows = await sql`
    UPDATE media
    SET
      transcode_status='pending',
      transcode_error='Previous processing attempt stalled; queued for retry',
      updated_at=now()
    WHERE transcode_status='processing'
      AND updated_at < now() - (${String(minutes)} || ' minutes')::interval
    RETURNING id
  ` as unknown as { id: string }[];
  await Promise.all(rows.map(row => logBackendActivity(env, {
    category: 'processing',
    event: 'job_requeued',
    status: 'warning',
    message: 'Stalled processing job was returned to the pending queue',
    mediaId: row.id,
  })));
};

let registeredUploadFileMapTableReady: Promise<void> | undefined;
const ensureRegisteredUploadFileMapTable = async (env: Env) => {
  if (!registeredUploadFileMapTableReady) {
    const sql = sqlForEnv(env);
    registeredUploadFileMapTableReady = sql`
      CREATE TABLE IF NOT EXISTS registered_upload_file_map (
        media_id TEXT PRIMARY KEY,
        original_file_name TEXT NOT NULL,
        stored_file_name TEXT NOT NULL,
        stored_url TEXT NOT NULL,
        source_url TEXT NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      )
    `.then(() => undefined);
  }
  try {
    await registeredUploadFileMapTableReady;
  } catch (error) {
    registeredUploadFileMapTableReady = undefined;
    throw error;
  }
};

const getRegisteredUploadFileMapRows = async (env: Env) => {
  const sql = sqlForEnv(env);
  await ensureRegisteredUploadFileMapTable(env);
  return (await sql`
    SELECT media_id, stored_url, source_url, updated_at
    FROM registered_upload_file_map
    WHERE stored_url<>source_url
  `) as unknown as Array<{
    media_id: string
    stored_url: string
    source_url: string
    updated_at: Date | string
  }>;
};

const upsertRegisteredUploadFileMap = async (
  env: Env,
  {
    mediaId,
    originalFileName,
    storedFileName,
    storedUrl,
    sourceUrl,
  }: {
    mediaId: string
    originalFileName: string
    storedFileName: string
    storedUrl: string
    sourceUrl: string
  },
) => {
  const sql = sqlForEnv(env);
  await ensureRegisteredUploadFileMapTable(env);
  await sql`
    INSERT INTO registered_upload_file_map (
      media_id,
      original_file_name,
      stored_file_name,
      stored_url,
      source_url
    )
    VALUES (
      ${mediaId},
      ${originalFileName},
      ${storedFileName},
      ${storedUrl},
      ${sourceUrl}
    )
    ON CONFLICT (media_id) DO UPDATE SET
      original_file_name=EXCLUDED.original_file_name,
      stored_file_name=EXCLUDED.stored_file_name,
      stored_url=EXCLUDED.stored_url,
      source_url=CASE
        WHEN registered_upload_file_map.source_url<>
          registered_upload_file_map.stored_url
          THEN registered_upload_file_map.source_url
        ELSE EXCLUDED.source_url
      END,
      updated_at=now()
  `;
};

let scanLeaseTableReady: Promise<void> | undefined;
const ensureScanLeaseTable = async (env: Env) => {
  if (!scanLeaseTableReady) {
    const sql = sqlForEnv(env);
    scanLeaseTableReady = sql`
      CREATE TABLE IF NOT EXISTS worker_scan_lease (
        lock_name TEXT PRIMARY KEY,
        lease_token TEXT NOT NULL,
        lease_until TIMESTAMP WITH TIME ZONE NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      )
    `.then(() => undefined);
  }
  try {
    await scanLeaseTableReady;
  } catch (error) {
    scanLeaseTableReady = undefined;
    throw error;
  }
};

const getScanLeaseMinutes = (env: Env) => getNumber(
  env.STALE_REGISTRATION_MINUTES,
  5,
  { min: 1, max: 60 },
);

const acquireScanLease = async (env: Env) => {
  await ensureScanLeaseTable(env);
  const leaseToken = crypto.randomUUID();
  const sql = sqlForEnv(env);
  const rows = (await sql`
    INSERT INTO worker_scan_lease (
      lock_name,
      lease_token,
      lease_until,
      updated_at
    ) VALUES (
      'registration',
      ${leaseToken},
      now() + (${String(getScanLeaseMinutes(env))} || ' minutes')::interval,
      now()
    )
    ON CONFLICT (lock_name) DO UPDATE SET
      lease_token=EXCLUDED.lease_token,
      lease_until=EXCLUDED.lease_until,
      updated_at=now()
    WHERE worker_scan_lease.lease_until < now()
    RETURNING lease_token
  `) as unknown as { lease_token: string }[];
  return rows[0]?.lease_token === leaseToken ? leaseToken : undefined;
};

const renewScanLease = async (env: Env, leaseToken: string) => {
  const sql = sqlForEnv(env);
  const rows = (await sql`
    UPDATE worker_scan_lease
    SET
      lease_until=now() +
        (${String(getScanLeaseMinutes(env))} || ' minutes')::interval,
      updated_at=now()
    WHERE lock_name='registration'
      AND lease_token=${leaseToken}
    RETURNING lease_token
  `) as unknown as { lease_token: string }[];
  return rows[0]?.lease_token === leaseToken;
};

const releaseScanLease = async (env: Env, leaseToken: string) => {
  const sql = sqlForEnv(env);
  await sql`
    DELETE FROM worker_scan_lease
    WHERE lock_name='registration'
      AND lease_token=${leaseToken}
  `;
};

const upsertMediaRow = async (
  env: Env,
  {
    id,
    url,
    extension,
    mediaType,
    title,
    transcodeStatus,
    transcodeError,
    aspectRatio,
    takenAt,
    takenAtNaive,
  }: {
    id: string
    url: string
    extension: string
    mediaType: 'photo' | 'video'
    title?: string
    transcodeStatus?: string
    transcodeError?: string
    aspectRatio?: number
    takenAt: string
    takenAtNaive: string
  },
) => {
  const sql = sqlForEnv(env);
  await sql`DELETE FROM media WHERE url=${url} AND id<>${id}`;
  await sql`
    INSERT INTO media (
      id,
      url,
      extension,
      media_type,
      title,
      tags,
      transcode_status,
      transcode_error,
      aspect_ratio,
      exclude_from_feeds,
      hidden,
      taken_at,
      taken_at_naive
    ) VALUES (
      ${id},
      ${url},
      ${extension},
      ${mediaType},
      ${title},
      ${[]},
      ${transcodeStatus ?? null},
      ${transcodeError ?? null},
      ${aspectRatio ?? (mediaType === 'video' ? 16 / 9 : 1.5)},
      ${false},
      ${false},
      ${takenAt},
      ${takenAtNaive}
    )
    ON CONFLICT (id) DO UPDATE SET
      url=EXCLUDED.url,
      extension=EXCLUDED.extension,
      media_type=EXCLUDED.media_type,
      title=CASE
        WHEN NULLIF(media.title, '') IS NOT NULL
          AND media.title !~ '^[0-9]{12}(?:[-_].*)?$'
          THEN media.title
        ELSE EXCLUDED.title
      END,
      transcode_status=EXCLUDED.transcode_status,
      transcode_error=EXCLUDED.transcode_error,
      aspect_ratio=EXCLUDED.aspect_ratio,
      updated_at=now()
  `;
};

const scanAndRegisterWithLease = async (
  env: Env,
  leaseToken: string,
) => {
  await ensureRegisteredUploadFileMapTable(env);
  await ensureRegistrationStatusTable(env);
  await clearStaleRegistrationStatuses(env);
  await clearResolvedRegistrationStatuses(env);
  await clearOldCompletedRegistrationStatuses(env);
  await retryStaleProcessing(env);

  const registerBatchSize = getNumber(env.REGISTER_BATCH_SIZE, 1, {
    min: 1,
    max: 10,
  });
  const maxRegisterPasses = getNumber(env.MAX_REGISTER_PASSES, 1, {
    min: 1,
    max: 10,
  });
  const staleRegistrationMinutes = getNumber(env.STALE_REGISTRATION_MINUTES, 15, {
    min: 1,
    max: 24 * 60,
  });

  let registered = 0;
  let registrationRemaining = 0;
  let missingProcessingSources = 0;
  let passes = 0;
  const attemptedRegistrationKeys = new Set<string>();

  for (let pass = 0; pass < maxRegisterPasses; pass += 1) {
    // Do not fan out direct Postgres connections here.  A large backlog used
    // to make this single scan open several pooler connections at once, which
    // can terminate the scan before any item reaches `registering`.
    // Storage listing can overlap the first query, but database work remains
    // deliberately serial and bounded regardless of backlog size.
    const listedObjectsPromise = listAllObjects(env);
    const rows = await getMediaRows(env);
    const hintRows = await getPendingUploadRegistrationHints(env);
    const registrationRows = await getRegistrationStatusRows(env);
    const registeredFileMaps = await getRegisteredUploadFileMapRows(env);
    const queuedDeletionPrefixes = await getQueuedDeletionPrefixes(env);
    const listedObjects = await listedObjectsPromise;
    const objects = listedObjects.filter(object =>
      !Array.from(queuedDeletionPrefixes).some(prefix =>
        deletionKeyMatchesPrefix(object.key, prefix)));
    const objectsByKey = new Map(objects.map(object => [object.key, object]));
    if (pass === 0) {
      missingProcessingSources = await reconcileMissingProcessingSources(
        env,
        rows,
        new Set(objectsByKey.keys()),
      );
    }
    const safelyCleanedSourceKeys = new Set<string>();
    for (const fileMap of registeredFileMaps) {
      const sourceKey = keyFromStorageUrl(env, fileMap.source_url);
      const storedKey = keyFromStorageUrl(env, fileMap.stored_url);
      if (!sourceKey || !storedKey || sourceKey === storedKey) { continue; }
      const sourceObject = objectsByKey.get(sourceKey);
      const storedObject = objectsByKey.get(storedKey);
      if (
        !sourceObject ||
        !storedObject ||
        !isDeferredSourceCleanupSafe(
          sourceObject.uploaded,
          parseDateValue(fileMap.updated_at),
        ) ||
        !isVerifiedStorageCopy(sourceObject.size, storedObject.size)
      ) {
        continue;
      }
      try {
        await deleteObject(env, sourceKey);
        safelyCleanedSourceKeys.add(sourceKey);
      } catch (error) {
        console.warn('Deferred registered source cleanup failed', {
          mediaId: fileMap.media_id,
          sourceKey,
          storedKey,
          error,
        });
      }
    }
    const registrationRowsByUrl = await buildRegistrationStatusLookup(
      env,
      registrationRows,
    );
    const protectedRegistrationDestinationUrls = new Set<string>();
    await Promise.all(registrationRows.map(async row => {
      const sourceUrl = trimToUndefined(row.source_url) || trimToUndefined(row.url);
      if (!sourceUrl) { return; }
      const sourceKey = keyFromStorageUrl(env, sourceUrl);
      if (!sourceKey || !objectsByKey.has(sourceKey)) { return; }
      const expectedUrl = await getExpectedRegistrationUrlForStatusRow(env, row)
        .catch(() => undefined);
      if (expectedUrl && isProtectedRegistrationDestination({
        objectUrl: expectedUrl,
        sourceUrl,
        expectedUrl,
        sourceExists: true,
      })) {
        protectedRegistrationDestinationUrls.add(expectedUrl);
      }
    }));

    const knownUrls = new Set<string>();
    rows.forEach(row => {
      knownUrls.add(row.url);
      if (row.poster_url) { knownUrls.add(row.poster_url); }
      if (row.preview_url) { knownUrls.add(row.preview_url); }
    });

    const pending = objects.filter(object => {
      if (safelyCleanedSourceKeys.has(object.key)) { return false; }
      const { fileNameBase, extension } = getFileParts(object.key);
      if (!MEDIA_EXTENSIONS.has(extension)) { return false; }
      if (GENERATED_MEDIA_SUFFIX_REGEX.test(fileNameBase)) { return false; }
      const objectUrl = urlForKey(env, object.key);
      if (knownUrls.has(objectUrl)) { return false; }
      // A failed or not-yet-visible copy may already exist under the generated
      // name. While its original source still exists, only retry the source;
      // never register that generated destination as a second upload.
      if (protectedRegistrationDestinationUrls.has(objectUrl)) { return false; }
      return true;
    });

    const pendingObjectByKey = new Map<string, R2ObjectLike>();
    pending.forEach(object => {
      pendingObjectByKey.set(object.key, object);
    });

    const now = Date.now();
    for (const hint of hintRows) {
      const hintKey = keyFromStorageUrl(env, hint.url);
      if (!hintKey) { continue; }

      const canonicalHintUrl = urlForKey(env, hintKey);
      if (canonicalHintUrl !== hint.url) {
        await replaceUploadRegistrationHintUrl(
          env,
          hint.url,
          canonicalHintUrl,
        ).catch(() => undefined);
      }

      const { fileName, fileNameBase, extension } = getFileParts(hintKey);
      if (!MEDIA_EXTENSIONS.has(extension)) { continue; }
      if (GENERATED_MEDIA_SUFFIX_REGEX.test(fileNameBase)) { continue; }

      const uploadedAtDate =
        parseDateValue(hint.updated_at) ??
        parseDateValue(hint.created_at);
      const uploadedAt = uploadedAtDate?.toISOString();
      const existingRegistration =
        registrationRowsByUrl.get(canonicalHintUrl) ||
        registrationRowsByUrl.get(hint.url);
      const originalFileName =
        resolveRegistrationOriginalFileName({
          hint,
          statusRow: existingRegistration,
          fallbackFileName: fileName,
        }) || fileName;
      const title = resolveRegistrationTitle({
        originalFileName,
        fallbackFileName: fileName,
      });
      const sourceUrl = resolveRegistrationSourceUrl(
        existingRegistration,
        canonicalHintUrl,
      );

      if (knownUrls.has(canonicalHintUrl)) {
        await clearTrackedRegistration(env, [
          canonicalHintUrl,
          hint.url,
          sourceUrl,
        ]).catch(() => undefined);
        continue;
      }

      if (pendingObjectByKey.has(hintKey)) {
        continue;
      }

      const recoveredRegistrationKey = await getExpectedRegistrationKeyForHint(
        env,
        hintKey,
      );
      if (
        recoveredRegistrationKey &&
        await storageObjectExists(env, recoveredRegistrationKey)
      ) {
        const recoveredRegistrationUrl = urlForKey(env, recoveredRegistrationKey);
        await replaceRegistrationStatusUrl(
          env,
          canonicalHintUrl,
          recoveredRegistrationUrl,
        ).catch(() => undefined);
        await replaceUploadRegistrationHintUrl(
          env,
          canonicalHintUrl,
          recoveredRegistrationUrl,
        ).catch(() => undefined);
        pendingObjectByKey.set(recoveredRegistrationKey, {
          key: recoveredRegistrationKey,
          uploaded: uploadedAtDate,
        });
        continue;
      }

      let finalizeErrorMessage: string | undefined;
      if (isDriveStorageEnabled(env)) {
        try {
          await finalizeDriveUpload(env, hintKey);
        } catch (error) {
          finalizeErrorMessage =
            error instanceof Error
              ? error.message
              : String(error ?? 'Drive finalize failed');
        }
      }

      if (await storageObjectExists(env, hintKey)) {
        pendingObjectByKey.set(hintKey, {
          key: hintKey,
          uploaded: uploadedAtDate,
        });
        continue;
      }

      const shouldMarkError = Boolean(
        finalizeErrorMessage ||
        existingRegistration?.status === 'error' ||
        (
          uploadedAtDate
            ? now - uploadedAtDate.getTime() >= staleRegistrationMinutes * 60 * 1000
            : true
        ),
      );
      await setRegistrationStatus(env, {
        url: canonicalHintUrl,
        fileName: originalFileName,
        uploadedAt,
        status: shouldMarkError ? 'error' : 'detected',
        sourceUrl,
        originalFileName,
        title,
        extension,
        errorMessage: shouldMarkError
          ? (
            finalizeErrorMessage
              ? `${MISSING_UPLOAD_ERROR_PREFIX} after finalize attempt: ${finalizeErrorMessage}`
              : `${MISSING_UPLOAD_ERROR_PREFIX}; finalize or re-upload the file`
          )
          : undefined,
      });
    }

    const pendingUploads = Array.from(pendingObjectByKey.values());

    await syncDetectedStatuses(env, pendingUploads, registrationRowsByUrl);
    registrationRemaining = pendingUploads.length;
    passes += 1;
    if (pendingUploads.length === 0) { break; }

    const batch = selectOldestRegistrationBatch(
      pendingUploads,
      attemptedRegistrationKeys,
      registerBatchSize,
    );
    if (batch.length === 0) {
      registrationRemaining = pendingUploads.length;
      break;
    }
    const batchHintUrls = batch.map(object => urlForKey(env, object.key));
    const hintsByUrl = await getUploadRegistrationHints(env, batchHintUrls);
    await upsertRegistrationStatuses(env, batch.map(object => {
      const currentUrl = urlForKey(env, object.key);
      const uploadHint = hintsByUrl.get(currentUrl);
      const existingRegistration = registrationRowsByUrl.get(currentUrl);
      const fileParts = getFileParts(object.key);
      const originalFileName =
        resolveRegistrationOriginalFileName({
          hint: uploadHint,
          statusRow: existingRegistration,
          fallbackFileName: fileParts.fileName,
        }) || fileParts.fileName;
      return {
        url: currentUrl,
        fileName: originalFileName,
        uploadedAt: object.uploaded?.toISOString(),
        status: 'registering',
        sourceUrl: resolveRegistrationSourceUrl(
          existingRegistration,
          currentUrl,
        ),
        originalFileName,
        title: resolveRegistrationTitle({
          originalFileName,
          fallbackFileName: fileParts.fileName,
        }),
        extension: fileParts.extension,
        errorMessage: undefined,
      } satisfies RegistrationStatusWrite;
    }));
    for (const object of batch) {
      if (!await renewScanLease(env, leaseToken)) {
        throw new Error('Worker registration scan lease was lost');
      }
      attemptedRegistrationKeys.add(object.key);
      const sourceUrl = urlForKey(env, object.key);
      let registrationUrl = sourceUrl;
      const { fileName, extension } = getFileParts(object.key);
      const sourceFileName = fileName;
      const sourceUploadedAt = object.uploaded?.toISOString();
      const uploadHint = hintsByUrl.get(sourceUrl);
      const existingRegistration = registrationRowsByUrl.get(sourceUrl);
      const persistedSourceUrl = resolveRegistrationSourceUrl(
        existingRegistration,
        sourceUrl,
      );
      const originalFileName =
        resolveRegistrationOriginalFileName({
          hint: uploadHint,
          statusRow: existingRegistration,
          fallbackFileName: sourceFileName,
        }) || sourceFileName;
      const registrationTitle = resolveRegistrationTitle({
        originalFileName,
        fallbackFileName: sourceFileName,
      });
      let mediaId = trimToUndefined(existingRegistration?.media_id);
      let registrationPhase: 'allocating' | 'preparing' | 'committing' =
        'allocating';
      try {
        mediaId = mediaId || await findAvailableMediaId(
          attempt => mediaIdForObject(
            env,
            object.key,
            object.uploaded,
            attempt,
          ),
          new Set(rows.map(row => row.id)),
        );
        const registrationKey = buildRegistrationKey(
          env,
          object.key,
          mediaId,
          extension,
        );
        const targetRegistrationUrl = urlForKey(env, registrationKey);
        const existingMediaForId = rows.find(row => row.id === mediaId);
        const targetRecordedAsRegistered =
          registrationKey !== object.key &&
          (
            existingMediaForId?.url === targetRegistrationUrl ||
            knownUrls.has(targetRegistrationUrl)
          );
        const shouldVerifyExistingTarget =
          shouldVerifyExistingRegistrationDestination({
            sourceKey: object.key,
            destinationKey: registrationKey,
            mediaId,
            trackedMediaId: trimToUndefined(existingRegistration?.media_id),
            targetRecordedAsRegistered,
          });
        const listedTargetSize = objectsByKey.get(registrationKey)?.size;
        const recordedTargetSize = shouldVerifyExistingTarget
          ? isVerifiedStorageCopy(object.size, listedTargetSize)
            ? listedTargetSize
            : await waitForVerifiedStorageCopy({
              sourceSize: object.size,
              readDestinationSize: () => storageObjectSize(env, registrationKey),
              attempts: isDriveStorageEnabled(env)
                ? DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS
                : 1,
              delayMs: isDriveStorageEnabled(env)
                ? DRIVE_COPY_VISIBILITY_DELAY_MS
                : 0,
            })
          : undefined;
        const targetAlreadyRegistered = shouldVerifyExistingTarget &&
          isVerifiedStorageCopy(object.size, recordedTargetSize);
        if (shouldWaitForTrackedRegistrationDestination({
          shouldVerifyExistingTarget,
          registrationStatus: existingRegistration?.status,
          targetAlreadyRegistered,
        })) {
          throw new Error(
            `Copied destination is not readable in storage: ${registrationKey}`,
          );
        }
        const shouldUpsertMediaRow = existingMediaForId?.url !== targetRegistrationUrl;
        if (targetAlreadyRegistered) {
          registrationUrl = targetRegistrationUrl;
        }
        if (registrationKey !== object.key) {
          registrationUrl = targetRegistrationUrl;
        }
        const registeredFileName = getFileParts(registrationUrl).fileName;
        await setRegistrationStatus(env, {
          url: sourceUrl,
          status: 'registering',
          sourceUrl: persistedSourceUrl,
          mediaId,
        });
        await logBackendActivity(env, {
          category: 'registration',
          event: 'registration_started',
          status: 'info',
          message: `Registering ${originalFileName}`,
          mediaId,
          details: {
            phase: 'preparing',
            fileName: originalFileName,
            extension,
            uploadedAt: sourceUploadedAt,
            sourceUrl,
            targetUrl: registrationUrl,
            sourceSize: object.size,
            storageProvider: detectStorageProvider(env),
          },
        });
        registrationPhase = 'preparing';
        await runSafeRegistrationCommit({
          // Keep the original object until the generated-name destination is
          // verified and the media row plus filename map are both committed.
          prepareDestination: async () => {
            if (registrationKey !== object.key && !targetAlreadyRegistered) {
              await copyAndVerifyObject(
                env,
                object.key,
                registrationKey,
                object.size,
              );
            }
          },
          commitRegistration: async () => {
            registrationPhase = 'committing';
            if (shouldUpsertMediaRow) {
              const mediaType = VIDEO_EXTENSIONS.has(extension) ? 'video' : 'photo';
              const uploadedAt = object.uploaded?.toISOString() || new Date().toISOString();
              await upsertMediaRow(env, {
                id: mediaId,
                url: registrationUrl,
                extension,
                mediaType,
                title: registrationTitle,
                transcodeStatus: mediaType === 'video' ? 'pending' : undefined,
                transcodeError: mediaType === 'video'
                  ? 'Queued for background processing'
                  : undefined,
                aspectRatio: mediaType === 'video' ? 16 / 9 : 1.5,
                takenAt: uploadedAt,
                takenAtNaive: toNaivePostgresString(uploadedAt),
              });
            }
            await upsertRegisteredUploadFileMap(env, {
              mediaId,
              originalFileName,
              storedFileName: registeredFileName,
              storedUrl: registrationUrl,
              sourceUrl,
            });
          },
          cleanupSource: async () => {
            if (registrationUrl !== sourceUrl) {
              await deleteObject(env, object.key);
            }
          },
          onCleanupError: cleanupError => {
            console.warn('Source cleanup deferred after safe registration', {
              sourceUrl,
              registrationUrl,
              cleanupError,
            });
          },
        });
        await clearRegistrationTrackingAfterSuccess(env, {
          mediaId,
          urls: [
            registrationUrl,
            sourceUrl,
            persistedSourceUrl,
          ].filter((url): url is string => Boolean(url)),
        });
        await clearUploadRegistrationHints(env, [
          registrationUrl,
          sourceUrl,
          persistedSourceUrl,
        ].filter((url): url is string => Boolean(url))).catch(() => undefined);
        await logBackendActivity(env, {
          category: 'registration',
          event: 'media_registered',
          status: 'success',
          message: `Registered ${originalFileName}`,
          mediaId,
          details: {
            phase: 'completed',
            fileName: originalFileName,
            extension,
            uploadedAt: sourceUploadedAt,
            sourceUrl,
            storedUrl: registrationUrl,
            storageProvider: detectStorageProvider(env),
          },
        });
        await revalidateMediaPanel(env, mediaId).catch(error => {
          console.error('Media panel revalidation failed after registration', {
            mediaId,
            sourceUrl,
            registrationUrl,
            error,
          });
        });
        knownUrls.add(registrationUrl);
        registered += 1;
      } catch (error) {
        const isRecoverableCopyDelay =
          isDriveStorageEnabled(env) &&
          isRecoverableDriveCopyError(error);
        const logRegistrationIssue = isRecoverableCopyDelay
          ? console.warn
          : console.error;
        logRegistrationIssue(
          isRecoverableCopyDelay
            ? 'Drive copy still completing for object'
            : 'Registration failed for object', {
          sourceUrl,
          registrationUrl,
          error,
        });
        await setRegistrationStatus(env, {
          url: sourceUrl,
          fileName: originalFileName,
          uploadedAt: sourceUploadedAt,
          status: isRecoverableCopyDelay ? 'registering' : 'error',
          sourceUrl: persistedSourceUrl,
          originalFileName,
          title: registrationTitle,
          extension,
          errorMessage: isRecoverableCopyDelay
            ? undefined
            : error instanceof Error
              ? error.message
              : String(error ?? 'Registration failed'),
        }).catch(() => undefined);
        await logBackendActivity(env, {
          category: 'registration',
          event: isRecoverableCopyDelay
            ? 'registration_waiting_for_storage'
            : 'registration_failed',
          status: isRecoverableCopyDelay ? 'warning' : 'error',
          message: isRecoverableCopyDelay
            ? `Waiting for Drive copy of ${originalFileName}`
            : error instanceof Error
              ? error.message
              : String(error ?? 'Registration failed'),
          mediaId,
          details: {
            phase: registrationPhase,
            fileName: originalFileName,
            extension,
            uploadedAt: sourceUploadedAt,
            sourceUrl,
            targetUrl: registrationUrl,
            error: error instanceof Error ? error.message : String(error),
          },
        });
        continue;
      }
    }

    registrationRemaining = Math.max(pendingUploads.length - batch.length, 0);
    if (batch.length === 0) { break; }
  }

  return {
    registered,
    registrationPasses: passes,
    registrationRemaining,
    pendingVideos: await countPendingVideos(env),
    missingProcessingSources,
    scanSkipped: false,
  };
};

const scanAndRegister = async (env: Env) => {
  const leaseToken = await acquireScanLease(env);
  if (!leaseToken) {
    return {
      registered: 0,
      registrationPasses: 0,
      registrationRemaining: 0,
      pendingVideos: await countPendingVideos(env),
      missingProcessingSources: 0,
      scanSkipped: true,
    };
  }
  try {
    return await scanAndRegisterWithLease(env, leaseToken);
  } finally {
    await releaseScanLease(env, leaseToken).catch(error => {
      console.warn('Failed to release worker scan lease', error);
    });
  }
};

const claimVideoJobs = async (env: Env, limit: number) => {
  // Reclaim abandoned leases even when the scheduled scan has not run yet.
  // Active processors keep updated_at fresh through their heartbeat requests.
  await retryStaleProcessing(env);

  const sql = sqlForEnv(env);
  const rows = (await sql`
    WITH cte AS (
      SELECT id
      FROM media
      WHERE transcode_status='pending'
        AND media_type='video'
      ORDER BY created_at ASC, id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT ${limit}
    )
    UPDATE media p
    SET transcode_status='processing', updated_at=now()
    WHERE p.id IN (SELECT id FROM cte)
    RETURNING p.id, p.url, p.extension, p.transcode_error
  `) as unknown as ClaimJobRow[];

  const jobs = await Promise.all(rows.map(async row => {
    const sourceKey = keyFromStorageUrl(env, row.url);
    let sourceExists = false;
    try {
      sourceExists = Boolean(
        sourceKey && await processingSourceExists(env, sourceKey),
      );
    } catch (error) {
      const sql = sqlForEnv(env);
      await sql`
        UPDATE media
        SET
          transcode_status='pending',
          transcode_error=${`Unable to verify processing source: ${error instanceof Error ? error.message : String(error)}`},
          updated_at=now()
        WHERE id=${row.id}
          AND transcode_status='processing'
      `;
      return undefined;
    }
    if (!sourceExists) {
      await markProcessingSourceMissing(env, row.id);
      return undefined;
    }
    const sourceUrl = isDriveStorageEnabled(env) && sourceKey
      ? await createDriveSignedDownloadUrl(env, sourceKey)
      : row.url;
    const fileNameBase = getFileParts(row.url).fileNameBase;
    return {
      photoId: row.id,
      sourceUrl,
      sourceKey,
      fileNameBase,
      extension: row.extension,
      processingReason: row.transcode_error || undefined,
      canonicalOutputKey: sourceKey &&
        !PRESERVED_VIDEO_EXTENSIONS.has(row.extension.toLowerCase())
        ? sourceKey.replace(/\.[^/.]+$/, '.mp4')
        : undefined,
    };
  }));
  const readyJobs = jobs
    .filter((job): job is NonNullable<typeof job> => Boolean(job));
  await Promise.all(readyJobs.map(job => logBackendActivity(env, {
    category: 'processing',
    event: 'job_claimed',
    status: 'info',
    message: 'Backend Processor claimed the video job',
    mediaId: job.photoId,
  })));
  return readyJobs;
};

const heartbeatVideoJob = async (
  env: Env,
  body: { photoId?: string, note?: string },
) => {
  const photoId = body.photoId?.trim();
  if (!photoId) {
    return json(400, { error: 'photoId is required' });
  }

  const sql = sqlForEnv(env);
  await sql`
    UPDATE media
    SET
      updated_at=now(),
      transcode_error=${body.note || 'Video processing in progress'}
    WHERE id=${photoId}
      AND transcode_status='processing'
  `;

  return json(200, { success: true });
};

const proxyVideoStreamMultipartUpload = async (
  env: Env,
  body: Record<string, unknown>,
) => {
  if (!isDriveStorageEnabled(env)) {
    return json(503, { error: 'Direct stream upload requires Drive storage' });
  }
  const key = typeof body.key === 'string' ? body.key.trim() : '';
  const photoId = typeof body.photoId === 'string' ? body.photoId.trim() : '';
  const action = typeof body.action === 'string' ? body.action.trim() : '';
  if (!photoId || !isAllowedProcessorUploadKey(key, photoId)) {
    return json(400, { error: 'Invalid processor upload key' });
  }
  if (!['start', 'part', 'complete', 'abort'].includes(action)) {
    return json(400, { error: 'Invalid multipart action' });
  }
  const response = await fetch(
    `${driveApiBaseUrl(env)}/api/v1/storage/multipart`,
    {
      method: 'POST',
      headers: driveHeaders(env, { 'Content-Type': 'application/json' }),
      body: JSON.stringify({
        ...body,
        action,
        key,
        projectId: env.DRIVE_STORAGE_PROJECT_ID,
        bucket: env.DRIVE_STORAGE_BUCKET,
      }),
    },
  );
  const data = await response.json().catch(() => ({}));
  return json(response.status, data);
};

const uploadProcessorObject = async (env: Env, formData: FormData) => {
  const photoId = formData.get('photoId')?.toString().trim();
  const key = formData.get('key')?.toString().trim() || '';
  const contentType = formData.get('contentType')?.toString().trim() || 'application/octet-stream';
  const file = formData.get('file');
  if (!photoId || !(file instanceof File) || !isAllowedHlsDerivativeKey(key)) {
    return json(400, { error: 'photoId, HLS key, and file are required' });
  }
  const sql = sqlForEnv(env);
  const rows = await sql`SELECT url FROM media WHERE id=${photoId} LIMIT 1` as unknown as Array<{ url: string }>;
  const base = rows[0]?.url ? getFileParts(rows[0].url).fileNameBase : '';
  if (!base || !key.startsWith(`${base}-hls`)) {
    return json(400, { error: 'HLS key does not match the media source' });
  }
  await putObject(env, key, await file.arrayBuffer(), contentType);
  // Drive can acknowledge the write before its object HEAD endpoint sees the
  // new artifact. HLS uploads are published one object at a time, so reuse
  // the same bounded visibility wait as other Drive destinations rather than
  // failing the entire video job on the first stale read.
  const size = await waitForVerifiedStorageCopy({
    sourceSize: file.size,
    readDestinationSize: () => storageObjectSize(env, key),
    attempts: isDriveStorageEnabled(env)
      ? DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS
      : 1,
    delayMs: isDriveStorageEnabled(env)
      ? DRIVE_COPY_VISIBILITY_DELAY_MS
      : 0,
  });
  if (size === undefined || size !== file.size) {
    return json(409, { error: 'HLS artifact is not fully readable in storage' });
  }
  return json(200, { success: true, key, size, url: urlForKey(env, key) });
};

const verifyHlsArtifacts = async (
  env: Env,
  fileNameBase: string,
  manifestKey: string,
  rawArtifacts: unknown,
) => {
  if (!isAllowedHlsDerivativeKey(manifestKey) || manifestKey !== `${fileNameBase}-hls.m3u8`) {
    throw new Error('Invalid HLS manifest key');
  }
  if (!Array.isArray(rawArtifacts) || rawArtifacts.length < 2) {
    throw new Error('HLS artifact list is incomplete');
  }
  const artifacts = rawArtifacts.map(item => {
    const value = item as HlsArtifactMetadata;
    const key = value.key?.trim() || '';
    const size = Number(value.size);
    if (!isAllowedHlsDerivativeKey(key) || !key.startsWith(`${fileNameBase}-hls`) ||
        !Number.isFinite(size) || size <= 0) {
      throw new Error(`Invalid HLS artifact metadata: ${key}`);
    }
    return { key, size };
  });
  const byKey = new Map(artifacts.map(artifact => [artifact.key, artifact]));
  const manifest = byKey.get(manifestKey);
  if (!manifest) throw new Error('HLS manifest is missing from artifact list');
  for (const artifact of artifacts) {
    const stored = await storageObjectSize(env, artifact.key);
    if (!isVerifiedStorageCopy(artifact.size, stored)) {
      throw new Error(`HLS artifact is not fully readable in storage: ${artifact.key}`);
    }
  }
  const response = await fetch(urlForKey(env, manifestKey), {
    cache: 'no-store',
    headers: stableStorageReadHeaders(env),
  });
  if (!response.ok) throw new Error('HLS manifest is not readable in storage');
  const manifestText = await response.text();
  if (!/#EXTM3U/.test(manifestText) || !/#EXT-X-STREAM-INF:/i.test(manifestText)) throw new Error('HLS master manifest is incomplete');
  const masterUris = manifestText.split(/\r?\n/).filter(line => line.trim() && !line.trim().startsWith('#'));
  const variantArtifacts = masterUris.map(uri => artifacts.find(candidate => urlForKey(env, candidate.key) === uri));
  if (variantArtifacts.some(artifact => !artifact || !/-(?:high|720p)\.m3u8$/i.test(artifact!.key))) throw new Error('HLS master references an unverified rendition');
  for (const variant of variantArtifacts) {
    const response = await fetch(urlForKey(env, variant!.key), {
      cache: 'no-store',
      headers: stableStorageReadHeaders(env),
    });
    if (!response.ok) throw new Error(`HLS rendition is not readable: ${variant!.key}`);
    const text = await response.text();
    if (!/#EXTM3U/.test(text) || !/#EXT-X-PLAYLIST-TYPE:VOD/i.test(text) || !/#EXT-X-MAP:/i.test(text) || !/#EXT-X-ENDLIST/i.test(text)) throw new Error(`HLS rendition is incomplete: ${variant!.key}`);
    const uris = Array.from(text.matchAll(/#EXT-X-MAP:.*?URI="([^"]+)"/gi), match => match[1]).concat(text.split(/\r?\n/).filter(line => line.trim() && !line.trim().startsWith('#')));
    for (const uri of Array.from(new Set(uris))) {
      const artifact = artifacts.find(candidate => urlForKey(env, candidate.key) === uri);
      if (!uri || !artifact || !/-(?:high|720p)-(?:init\.mp4|[0-9]{5}\.m4s)$/i.test(artifact.key)) throw new Error(`HLS rendition references an unverified artifact: ${uri}`);
    }
  }
  return urlForKey(env, manifestKey);
};

// Reconciliation is intentionally idempotent rather than cursor-based: a
// worker crash can leave any artifact missing, and the next claim pass safely
// returns that media to the normal pending FIFO queue.
const reconcileMissingHlsArtifacts = async (env: Env) => {
  await ensureHlsSchema(env);
  const sql = sqlForEnv(env);
  const rows = await sql`
    WITH candidates AS (
      SELECT id
      FROM media
      WHERE media_type='video' AND transcode_status='ready'
        AND (hls_manifest_url IS NULL OR hls_verified_at IS NULL OR
          hls_verified_at < now() - interval '15 minutes')
      ORDER BY hls_verified_at ASC NULLS FIRST, id ASC
      LIMIT 12
      FOR UPDATE SKIP LOCKED
    )
    UPDATE media AS m
    SET hls_verified_at=now()
    FROM candidates
    WHERE m.id=candidates.id
    RETURNING m.id, m.url, m.hls_manifest_url, m.hls_verified_at
  ` as unknown as Array<{ id: string, url: string, hls_manifest_url?: string | null }>;
  for (const row of rows) {
    let missing = !row.hls_manifest_url;
    if (!missing) {
      const manifestKey = keyFromStorageUrl(env, row.hls_manifest_url!);
      const hlsBase = manifestKey.replace(/\.m3u8$/i, '');
      if (!manifestKey || urlForKey(env, manifestKey) !== row.hls_manifest_url ||
          !isAllowedHlsDerivativeKey(manifestKey)) {
        missing = true;
      }
      const manifestResponse = await fetch(row.hls_manifest_url!, {
        cache: 'no-store',
        headers: stableStorageReadHeaders(env),
      }).catch(() => undefined);
      if (!missing && !manifestResponse?.ok) {
        missing = manifestResponse?.status === 404 || !manifestResponse;
      } else if (!missing) {
        const text = await manifestResponse.text();
        const renditionUrls = text.split(/\r?\n/).filter(line => line.trim() && !line.trim().startsWith('#'));
        if (!/#EXTM3U/.test(text) || !/#EXT-X-STREAM-INF:/i.test(text) || renditionUrls.length < 1) missing = true;
        for (const renditionUrl of renditionUrls) {
          const renditionKey = keyFromStorageUrl(env, renditionUrl);
          if (!renditionKey || urlForKey(env, renditionKey) !== renditionUrl || !/-(?:high|720p)\.m3u8$/i.test(renditionKey) || !renditionKey.startsWith(hlsBase) || await storageObjectSize(env, renditionKey) === undefined) { missing = true; break; }
          const renditionResponse = await fetch(renditionUrl, {
            cache: 'no-store',
            headers: stableStorageReadHeaders(env),
          }).catch(() => undefined);
          if (!renditionResponse?.ok) { missing = true; break; }
          const renditionText = await renditionResponse.text();
          const artifactUrls = Array.from(renditionText.matchAll(/#EXT-X-MAP:.*?URI="([^"]+)"/gi), match => match[1]).concat(renditionText.split(/\r?\n/).filter(line => line.trim() && !line.trim().startsWith('#')));
          for (const uri of Array.from(new Set(artifactUrls))) {
            const key = keyFromStorageUrl(env, uri);
            if (!key || urlForKey(env, key) !== uri || !isAllowedHlsDerivativeKey(key) || !key.startsWith(hlsBase) || await storageObjectSize(env, key) === undefined) { missing = true; break; }
          }
          if (missing) break;
        }
      }
    }
    if (missing) {
      await sql`
        UPDATE media
        SET transcode_status='pending',
            hls_verified_at=NULL,
            transcode_error='HLS VOD artifact backfill required',
            updated_at=now()
        WHERE id=${row.id} AND transcode_status='ready'
      `;
    } else {
      await sql`
        UPDATE media SET hls_verified_at=now()
        WHERE id=${row.id} AND transcode_status='ready'
      `;
    }
  }
};

const commitCanonicalVideo = async (
  env: Env,
  body: Record<string, unknown>,
) => {
  const photoId = typeof body.photoId === 'string' ? body.photoId.trim() : '';
  const key = typeof body.key === 'string' ? body.key.trim() : '';
  const expectedSize = Number(body.size);
  if (!photoId || !key || !Number.isFinite(expectedSize) || expectedSize <= 0) {
    return json(400, { error: 'photoId, key, and size are required' });
  }
  if (!isAllowedProcessorUploadKey(key, photoId)) {
    return json(400, { error: 'Invalid canonical media key' });
  }
  const sql = sqlForEnv(env);
  const mediaRows = await sql`
    SELECT id, url, extension
    FROM media
    WHERE id=${photoId}
    LIMIT 1
  ` as unknown as CanonicalMediaRow[];
  const media = mediaRows[0];
  if (!media || PRESERVED_VIDEO_EXTENSIONS.has(media.extension.toLowerCase())) {
    return json(409, { error: 'Media does not require canonical MP4 conversion' });
  }
  const sourceKey = keyFromStorageUrl(env, media.url);
  const expectedKey = sourceKey.replace(/\.[^/.]+$/, '.mp4');
  if (!sourceKey || key !== expectedKey) {
    return json(400, { error: 'Canonical MP4 key does not match the media source' });
  }
  const storedSize = await storageObjectSize(env, key);
  if (!isVerifiedStorageCopy(expectedSize, storedSize)) {
    return json(409, { error: 'Canonical MP4 is not fully readable in storage' });
  }
  await ensureRegisteredUploadFileMapTable(env);
  const maps = await sql`
    SELECT original_file_name
    FROM registered_upload_file_map
    WHERE media_id=${photoId}
    LIMIT 1
  ` as unknown as Array<{ original_file_name: string }>;
  const originalFileName = maps[0]?.original_file_name ||
    getFileParts(media.url).fileName;
  const canonicalUrl = urlForKey(env, key);
  await sql.transaction(tx => [
    tx`
      UPDATE media
      SET url=${canonicalUrl}, extension='mp4', updated_at=now()
      WHERE id=${photoId} AND url=${media.url}
    `,
    tx`
      INSERT INTO registered_upload_file_map (
        media_id, original_file_name, stored_file_name, stored_url, source_url
      ) VALUES (
        ${photoId}, ${originalFileName}, ${getFileParts(key).fileName},
        ${canonicalUrl}, ${media.url}
      )
      ON CONFLICT (media_id) DO UPDATE SET
        original_file_name=EXCLUDED.original_file_name,
        stored_file_name=EXCLUDED.stored_file_name,
        stored_url=EXCLUDED.stored_url,
        source_url=EXCLUDED.source_url,
        updated_at=now()
    `,
  ]);
  if (sourceKey !== key) {
    await deleteObject(env, sourceKey).catch(error => {
      console.warn('Canonical MP4 committed; source cleanup deferred', {
        photoId,
        sourceKey,
        key,
        error,
      });
    });
  }
  await revalidateMediaPanel(env, photoId).catch(() => undefined);
  return json(200, { success: true, url: canonicalUrl });
};

const completeVideoJob = async (
  env: Env,
  formData: FormData,
) => {
  const photoId = formData.get('photoId')?.toString().trim();
  const fileNameBase = formData.get('fileNameBase')?.toString().trim();
  if (!photoId || !fileNameBase) {
    return json(400, { error: 'photoId and fileNameBase are required' });
  }

  const metadataRaw = formData.get('metadata')?.toString();
  const metadata = metadataRaw
    ? JSON.parse(metadataRaw) as {
      durationSeconds?: number
      frameRate?: number
      mediaWidth?: number
      mediaHeight?: number
    }
    : {};
  const poster = formData.get('poster');
  const preview = formData.get('preview');
  const subtitleFiles = formData.getAll('subtitles')
    .filter((value): value is File => value instanceof File);
  const subtitleMetadataRaw = formData.get('subtitleTracks')?.toString();
  const subtitleMetadata = getValidSubtitleUploadMetadata(
    fileNameBase,
    subtitleMetadataRaw ? JSON.parse(subtitleMetadataRaw) : [],
    subtitleFiles.map(file => file.name),
  );
  let posterUrl: string | undefined;
  let previewUrl: string | undefined;

  if (poster instanceof File) {
    const key = `${fileNameBase}-poster.jpg`;
    await putObject(
      env,
      key,
      await poster.arrayBuffer(),
      poster.type || 'image/jpeg',
    );
    posterUrl = urlForKey(env, key);
  }

  if (preview instanceof File) {
    const extension = getFileParts(preview.name).extension || 'mp4';
    const key = `${fileNameBase}-preview.${extension}`;
    await putObject(
      env,
      key,
      await preview.arrayBuffer(),
      preview.type || 'video/mp4',
    );
    previewUrl = urlForKey(env, key);
  }

  const newSubtitleTracks: SubtitleManifestTrack[] = [];
  for (const track of subtitleMetadata) {
    const file = subtitleFiles.find(candidate => candidate.name === track.fileName);
    if (!file) { continue; }
    await putObject(
      env,
      track.fileName,
      await file.arrayBuffer(),
      file.type || 'text/vtt',
    );
    newSubtitleTracks.push({
      src: urlForKey(env, track.fileName),
      lang: track.lang,
      label: track.label,
    });
  }

  if (newSubtitleTracks.length > 0) {
    const manifestKey = `${fileNameBase}-subtitles.json`;
    const existingManifest = await fetch(urlForKey(env, manifestKey), {
      cache: 'no-store',
      headers: stableStorageReadHeaders(env),
    })
      .then(async response => response.ok
        ? await response.json() as { tracks?: SubtitleManifestTrack[] }
        : undefined)
      .catch(() => undefined);
    const existingTracks = Array.isArray(existingManifest?.tracks)
      ? existingManifest.tracks.filter(track =>
        Boolean(track?.src && track?.lang && track?.label))
      : [];
    const tracks = mergeSubtitleManifestTracks(
      existingTracks,
      newSubtitleTracks,
    );
    await putObject(
      env,
      manifestKey,
      encoder.encode(JSON.stringify({ tracks })).buffer,
      'application/json',
    );
  }

  const sql = sqlForEnv(env);
  await ensureHlsSchema(env);
  await sql`
    UPDATE media
    SET
      poster_url=${posterUrl ?? null},
      preview_url=${previewUrl ?? null},
      hls_manifest_url=NULL,
      hls_verified_at=NULL,
      duration_seconds=${metadata.durationSeconds ?? null},
      frame_rate=${metadata.frameRate ?? null},
      media_width=${metadata.mediaWidth ?? null},
      media_height=${metadata.mediaHeight ?? null},
      aspect_ratio=${(
        metadata.mediaWidth &&
        metadata.mediaHeight
      ) ? metadata.mediaWidth / metadata.mediaHeight : 16 / 9},
      transcode_status='ready',
      transcode_error=NULL,
      updated_at=now()
    WHERE id=${photoId}
  `;

  await logBackendActivity(env, {
    category: 'processing',
    event: 'job_completed',
    status: 'success',
    message: 'Video processing completed',
    mediaId: photoId,
    details: {
      posterGenerated: Boolean(posterUrl),
      previewGenerated: Boolean(previewUrl),
      subtitleTracks: newSubtitleTracks.length,
    },
  });

  await revalidateMediaPanel(env, photoId);

  return json(200, {
    success: true,
    posterUrl,
    previewUrl,
    subtitleTracks: newSubtitleTracks.length,
  });
};

const failVideoJob = async (
  env: Env,
  body: { photoId?: string, transcodeError?: string },
) => {
  const photoId = body.photoId?.trim();
  if (!photoId) {
    return json(400, { error: 'photoId is required' });
  }

  const sql = sqlForEnv(env);
  const errorMessage = body.transcodeError || 'Background processing failed';
  const retryDownload = shouldRetryInterruptedJob(errorMessage);
  await sql`
    UPDATE media
    SET
      transcode_status=${retryDownload ? 'pending' : 'failed'},
      transcode_error=${retryDownload
        ? `Retryable processing interruption; queued for retry: ${errorMessage}`
        : errorMessage},
      updated_at=now()
    WHERE id=${photoId}
  `;

  await logBackendActivity(env, {
    category: 'processing',
    event: retryDownload ? 'job_requeued' : 'job_failed',
    status: retryDownload ? 'warning' : 'error',
    message: retryDownload
      ? 'Interrupted video job was returned to the pending queue'
      : errorMessage,
    mediaId: photoId,
    details: { error: errorMessage },
  });

  await revalidateMediaPanel(env, photoId);

  return json(200, { success: true });
};

export const shouldRetryInterruptedJob = (errorMessage: string) =>
    /source download stalled|fetch failed|processor interrupted/i
      .test(errorMessage);

const heartbeatProcessor = async (
  env: Env,
  body: { processorId?: string, platform?: string, state?: string },
) => {
  const processorId = body.processorId?.trim().slice(0, 120);
  if (!processorId) { return json(400, { error: 'processorId is required' }); }
  const sql = sqlForEnv(env);
  await sql`
    CREATE TABLE IF NOT EXISTS video_processor_presence (
      processor_id TEXT PRIMARY KEY,
      platform TEXT,
      state TEXT,
      last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
      started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )
  `;
  const presenceRows = await sql`
    WITH previous AS MATERIALIZED (
      SELECT state, last_seen_at
      FROM video_processor_presence
      WHERE processor_id=${processorId}
    )
    INSERT INTO video_processor_presence (
      processor_id, platform, state, last_seen_at
    ) VALUES (
      ${processorId}, ${body.platform || 'unknown'},
      ${body.state || 'idle'}, now()
    )
    ON CONFLICT (processor_id) DO UPDATE SET
      platform=EXCLUDED.platform,
      state=EXCLUDED.state,
      last_seen_at=now()
    RETURNING
      (SELECT state FROM previous) AS previous_state,
      (SELECT last_seen_at FROM previous) AS previous_last_seen_at
  ` as unknown as Array<{
    previous_state?: string | null
    previous_last_seen_at?: string | Date | null
  }>;
  const previous = presenceRows[0];
  const previousLastSeen = previous?.previous_last_seen_at
    ? new Date(previous.previous_last_seen_at).getTime()
    : 0;
  const state = body.state || 'idle';
  const wasOffline = !previousLastSeen ||
    Date.now() - previousLastSeen > 2 * 60 * 1000;
  if (wasOffline || previous?.previous_state !== state) {
    await logBackendActivity(env, {
      category: 'processor',
      event: wasOffline ? 'processor_connected' : 'processor_state_changed',
      status: 'info',
      message: wasOffline
        ? 'Backend Processor connected'
        : `Backend Processor state changed to ${state}`,
      processorId,
      details: { platform: body.platform || 'unknown', state },
    });
  }
  return json(200, { ok: true });
};

const status = async (env: Env) => {
  const sql = sqlForEnv(env);
  const [
    rows,
    processors,
    activeJobs,
    deletionQueue,
    registrationSnapshotRows,
  ] = await Promise.all([
    sql`
      SELECT transcode_status, COUNT(*)::int AS count
      FROM media
      WHERE transcode_status IN ('pending', 'processing', 'failed')
      GROUP BY transcode_status
    ` as unknown as Promise<Array<{
      transcode_status: string | null
      count: number
    }>>,
    sql`
      SELECT processor_id, platform, state, last_seen_at, started_at
      FROM video_processor_presence
      WHERE last_seen_at > now() - interval '2 minutes'
      ORDER BY last_seen_at DESC
    `.catch(() => []) as Promise<Record<string, unknown>[]>,
    sql`
      SELECT id, title, transcode_status, transcode_error, updated_at
      FROM media
      WHERE transcode_status IN ('pending', 'processing', 'failed')
      ORDER BY updated_at DESC
      LIMIT 20
    ` as unknown as Promise<Record<string, unknown>[]>,
    getDeletionQueueCounts(env),
    sql`
      SELECT
        (COUNT(*) FILTER (WHERE status='detected'))::int AS detected,
        (COUNT(*) FILTER (WHERE status='registering'))::int AS registering,
        (COUNT(*) FILTER (WHERE status='error'))::int AS error,
        COUNT(*)::int AS total,
        COALESCE((
          SELECT jsonb_agg(
            to_jsonb(job)
            ORDER BY job.uploaded_at ASC NULLS LAST, job.updated_at ASC, job.url ASC
          )
          FROM (
            SELECT
              url,
              file_name,
              original_file_name,
              title,
              status,
              media_id,
              extension,
              error_message,
              uploaded_at,
              updated_at
            FROM worker_registration_status
            WHERE status IN ('detected', 'registering', 'error')
            ORDER BY uploaded_at ASC NULLS LAST, updated_at ASC, url ASC
            LIMIT 50
          ) job
        ), '[]'::jsonb) AS jobs
      FROM worker_registration_status
      WHERE status IN ('detected', 'registering', 'error')
    ` as unknown as Promise<Array<{
      detected: number
      registering: number
      error: number
      total: number
      jobs: Record<string, unknown>[]
    }>>,
  ]);
  const counts = rows.reduce<Record<string, number>>((acc, row) => {
    acc[row.transcode_status || 'unknown'] = row.count;
    return acc;
  }, {});
  const registrationSnapshot = registrationSnapshotRows[0] || {
    detected: 0,
    registering: 0,
    error: 0,
    total: 0,
    jobs: [],
  };
  return {
    ...counts,
    processors,
    activeJobs,
    deletionQueue,
    registrationQueue: {
      detected: registrationSnapshot.detected,
      registering: registrationSnapshot.registering,
      error: registrationSnapshot.error,
      total: registrationSnapshot.total,
    },
    registrationJobs: registrationSnapshot.jobs,
    build: WORKER_BUILD_ID,
    storageProvider: detectStorageProvider(env),
    checkedAt: new Date().toISOString(),
  };
};

let scanInFlight: Promise<Awaited<ReturnType<typeof scanAndRegister>>> | undefined;
let deletionDrainInFlight: Promise<number> | undefined;

const startDeletionDrain = (env: Env) => {
  if (deletionDrainInFlight) {
    return { started: false, promise: deletionDrainInFlight };
  }
  const promise = drainMediaDeletionQueue(env).finally(() => {
    if (deletionDrainInFlight === promise) {
      deletionDrainInFlight = undefined;
    }
  });
  deletionDrainInFlight = promise;
  return { started: true, promise };
};

const startScan = (
  env: Env,
  { shareInFlight = true }: { shareInFlight?: boolean } = {},
) => {
  // Scheduled events are independent invocations. The database lease is the
  // cross-invocation concurrency guard; sharing a promise here could let one
  // hung isolate suppress every later cron run indefinitely.
  if (shareInFlight && scanInFlight) {
    return { started: false, promise: scanInFlight };
  }
  const promise = (async () => {
    await logBackendActivity(env, {
      category: 'orchestrator',
      event: 'scan_started',
      status: 'info',
      message: 'Storage scan started',
      details: { storageProvider: detectStorageProvider(env) },
    });
    try {
      let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
      const watchdog = new Promise<never>((_, reject) => {
        timeoutHandle = setTimeout(() => {
          reject(new Error(
            `Registration scan watchdog exceeded ${SCAN_WATCHDOG_TIMEOUT_MS}ms`,
          ));
        }, SCAN_WATCHDOG_TIMEOUT_MS);
      });
      let result: Awaited<ReturnType<typeof scanAndRegister>>;
      try {
        result = await Promise.race([scanAndRegister(env), watchdog]);
      } finally {
        if (timeoutHandle) { clearTimeout(timeoutHandle); }
      }
      await logBackendActivity(env, {
        category: 'orchestrator',
        event: 'scan_completed',
        status: 'success',
        message: 'Storage scan completed',
        details: result,
      });
      return result;
    } catch (error) {
      await logBackendActivity(env, {
        category: 'orchestrator',
        event: 'scan_failed',
        status: 'error',
        message: error instanceof Error ? error.message : 'Storage scan failed',
      });
      throw error;
    }
  })().finally(() => {
    if (shareInFlight && scanInFlight === promise) {
      scanInFlight = undefined;
    }
  });
  if (shareInFlight) {
    scanInFlight = promise;
  }
  return { started: true, promise };
};

const scheduleScan = (env: Env, ctx: ExecutionContext) => {
  const scan = startScan(env);
  if (scan.started) {
    ctx.waitUntil(scan.promise);
  }
  return scan.started;
};

export default {
  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ) {
    await logBackendActivity(env, {
      category: 'orchestrator',
      event: 'scheduled_triggered',
      status: 'info',
      message: 'Cloudflare scheduled registration run triggered',
      details: {
        cron: controller.cron,
        scheduledTime: controller.scheduledTime,
      },
    });
    const settings = await getRuntimeProcessingSettings(env);
    // Cleanup is independent of registration. It can hit a slow database or
    // storage operation, so it must never hold the scheduled registration
    // path hostage. Keep it alive separately and start the FIFO scan now.
    const deletionDrain = startDeletionDrain(env);
    if (deletionDrain.started) {
      ctx.waitUntil(deletionDrain.promise.catch((error) => {
        console.warn(
          'Deletion queue drain failed; continuing scheduled registration scan',
          error,
        );
      }));
    }
    if (!settings.orchestratorEnabled || !settings.registrationEnabled) {
      return;
    }
    const scan = startScan(
      envWithRuntimeSettings(env, settings),
      { shareInFlight: false },
    );
    if (scan.started) {
      ctx.waitUntil(scan.promise.catch(error => {
        console.warn('Scheduled registration scan failed', error);
      }));
    }
  },

  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
      return json(200, { ok: true, build: WORKER_BUILD_ID });
    }

    if (url.pathname === '/status') {
      if (!isAuthorized(request, env.BACKEND_ORCHESTRATOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      try {
        const [settings, snapshot] = await Promise.all([
          getRuntimeProcessingSettings(env),
          status(env),
        ]);
        return json(200, { ...snapshot, settings });
      } catch (error: any) {
        return json(500, { error: error?.message || 'Status failed' });
      }
    }

    if (url.pathname === '/logs' && request.method === 'GET') {
      if (!isAuthorized(request, env.BACKEND_ORCHESTRATOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      const rawLimit = parseInt(url.searchParams.get('limit') || '200', 10);
      const limit = Number.isFinite(rawLimit)
        ? Math.min(Math.max(rawLimit, 1), 500)
        : 200;
      try {
        return json(200, {
          logs: await getBackendActivityLogs(env, limit),
          checkedAt: new Date().toISOString(),
        });
      } catch (error: any) {
        return json(500, { error: error?.message || 'Activity logs failed' });
      }
    }

    if (url.pathname === '/deletions/run' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_ORCHESTRATOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      const drain = startDeletionDrain(env);
      ctx.waitUntil(drain.promise.catch((error) => {
        console.warn('Deletion queue drain failed', error);
      }));
      return json(drain.started ? 202 : 200, {
        triggered: true,
        started: drain.started,
      });
    }

    const settings = await getRuntimeProcessingSettings(env);
    const runtimeEnv = envWithRuntimeSettings(env, settings);

    if (url.pathname === '/registration/retry' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_ORCHESTRATOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      const body = await request.json().catch(() => ({})) as {
        url?: unknown
        sourceUrl?: unknown
      };
      const urls = [body.url, body.sourceUrl]
        .filter((value): value is string => typeof value === 'string');
      if (urls.length === 0) {
        return json(400, { error: 'A registration URL is required' });
      }
      try {
        const requeued = await requeueRegistrationStatuses(runtimeEnv, urls);
        const scanQueued = settings.orchestratorEnabled && settings.registrationEnabled
          ? scheduleScan(runtimeEnv, ctx)
          : false;
        return json(scanQueued ? 202 : 200, {
          requeued,
          triggered: scanQueued,
          statusMessage: requeued > 0
            ? 'Registration requeued for worker retry'
            : 'Worker scan requested',
        });
      } catch (error: any) {
        return json(500, { error: error?.message || 'Unable to retry registration' });
      }
    }

    if (url.pathname === '/run' || url.pathname === '/scan') {
      if (!isAuthorized(request, env.BACKEND_ORCHESTRATOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      if (!settings.orchestratorEnabled || !settings.registrationEnabled) {
        return json(200, { triggered: false, disabled: true });
      }
      try {
        const trackedRegistrations = await getTrackedRegistrationStatuses(runtimeEnv);
        const activeRegistrations = trackedRegistrations
          .filter(({ status }) => status === 'registering');
        if (url.pathname === '/scan') {
          const scan = startScan(runtimeEnv);
          const result = await scan.promise;
          return json(200, {
            triggered: true,
            scanStarted: scan.started,
            registeringUrls: activeRegistrations.map(({ url }) => url),
            ...result,
          });
        }
        const scanQueued = scheduleScan(runtimeEnv, ctx);
        return json(scanQueued ? 202 : 200, {
          triggered: true,
          registeringUrls: activeRegistrations.map(({ url }) => url),
        });
      } catch (error: any) {
        return json(500, { error: error?.message || 'Scan failed' });
      }
    }

    if (url.pathname === '/jobs/claim') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      if (!settings.orchestratorEnabled || !settings.videoProcessingEnabled) {
        return json(200, { claimed: 0, jobs: [], disabled: true });
      }
      const rawLimit = parseInt(url.searchParams.get('limit') || '1', 10);
      const limit = Number.isFinite(rawLimit)
        ? Math.min(Math.max(rawLimit, 1), 3)
        : 1;
      try {
        const jobs = await claimVideoJobs(runtimeEnv, limit);
        return json(200, {
          claimed: jobs.length,
          pendingVideos: await countPendingVideos(env),
          jobs,
        });
      } catch (error: any) {
        return json(500, { error: error?.message || 'Claim failed' });
      }
    }

    if (url.pathname === '/jobs/config' && request.method === 'GET') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      return json(200, {
        pollIntervalMs: settings.processorPollIntervalMs,
        idleIntervalMs: settings.processorIdleIntervalMs,
        heartbeatIntervalMs: settings.processorHeartbeatIntervalMs,
        claimLimit: settings.processorClaimLimit,
        enabled: settings.orchestratorEnabled && settings.videoProcessingEnabled,
      });
    }

    if (url.pathname === '/processors/heartbeat' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      return heartbeatProcessor(
        runtimeEnv,
        await request.json().catch(() => ({})) as Record<string, string>,
      );
    }

    if (url.pathname === '/jobs/complete' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      try {
        const formData = await request.formData();
        return await completeVideoJob(env, formData);
      } catch (error: any) {
        return json(500, { error: error?.message || 'Complete failed' });
      }
    }

    if (
      url.pathname === '/jobs/storage/multipart' &&
      request.method === 'POST'
    ) {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      try {
        const body = await request.json().catch(() => ({})) as
          Record<string, unknown>;
        return await proxyVideoStreamMultipartUpload(env, body);
      } catch (error: any) {
        return json(500, { error: error?.message || 'Stream upload failed' });
      }
    }

    if (
      url.pathname === '/jobs/storage/status' &&
      request.method === 'GET'
    ) {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      const key = url.searchParams.get('key')?.trim() || '';
      if (!isAllowedStreamDerivativeKey(key) && !isAllowedHlsDerivativeKey(key)) {
        return json(400, { error: 'Invalid stream derivative key' });
      }
      const size = await storageObjectSize(env, key);
      return json(200, {
        exists: size !== undefined,
        size,
        url: size !== undefined ? urlForKey(env, key) : undefined,
      });
    }
    if (url.pathname === '/jobs/storage/upload' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      try {
        return await uploadProcessorObject(env, await request.formData());
      } catch (error: any) {
        return json(500, { error: error?.message || 'HLS upload failed' });
      }
    }
    if (url.pathname === '/jobs/canonical/commit' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      return commitCanonicalVideo(
        env,
        await request.json().catch(() => ({})) as Record<string, unknown>,
      );
    }

    if (url.pathname === '/jobs/fail' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      try {
        const body = await request.json().catch(() => ({})) as {
          photoId?: string
          transcodeError?: string
        };
        return await failVideoJob(env, body);
      } catch (error: any) {
        return json(500, { error: error?.message || 'Fail failed' });
      }
    }

    if (url.pathname === '/jobs/heartbeat' && request.method === 'POST') {
      if (!isAuthorized(request, env.BACKEND_PROCESSOR_SHARED_SECRET)) {
        return json(401, { error: 'Unauthorized' });
      }
      try {
        const body = await request.json().catch(() => ({})) as {
          photoId?: string
          note?: string
        };
        return await heartbeatVideoJob(env, body);
      } catch (error: any) {
        return json(500, { error: error?.message || 'Heartbeat failed' });
      }
    }

    return json(404, { error: 'Not found' });
  },
};
