import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  DRIVE_COPY_VISIBILITY_ATTEMPTS,
  DRIVE_COPY_VISIBILITY_DELAY_MS,
  DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS,
  buildDeletionPrefixes,
  deletionKeyMatchesPrefix,
  deleteStorageKeyIfPresent,
  detectStorageProvider,
  findAvailableMediaId,
  getValidSubtitleUploadMetadata,
  isDeferredSourceCleanupSafe,
  isAllowedStreamDerivativeKey,
  isAllowedHlsDerivativeKey,
  isAllowedProcessorUploadKey,
  isProtectedRegistrationDestination,
  isRecoverableDriveCopyError,
  isVerifiedStorageCopy,
  mergeSubtitleManifestTracks,
  runSafeRegistrationCommit,
  selectOldestRegistrationBatch,
  shouldMarkProcessingSourceMissing,
  shouldRetryInterruptedJob,
  shouldVerifyExistingRegistrationDestination,
  shouldWaitForTrackedRegistrationDestination,
  stableMediaIdForUrl,
  waitForVerifiedStorageCopy,
} from '../src/index.ts';

const workerSource = await readFile(
  new URL('../src/index.ts', import.meta.url),
  'utf8',
);

test('storage provider is detected without a preference variable', () => {
  assert.equal(detectStorageProvider({
    DRIVE_STORAGE_BASE_URL: 'https://drive.example/storage',
    DRIVE_STORAGE_API_KEY: 'drive-key',
    DRIVE_STORAGE_PROJECT_ID: 'project',
    DRIVE_STORAGE_BUCKET: 'media',
  }), 'drive');
  assert.equal(detectStorageProvider({
    R2_PUBLIC_BASE_URL: 'https://media.example',
    R2_ACCOUNT_ID: 'account',
    R2_BUCKET: 'media',
    R2_ACCESS_KEY_ID: 'r2-key',
    R2_SECRET_ACCESS_KEY: 'r2-secret',
  }), 'cloudflare-r2');
  assert.equal(detectStorageProvider({
    DRIVE_STORAGE_BASE_URL: 'https://drive.example/storage',
    R2_PUBLIC_BASE_URL: 'https://media.example',
  }), 'cloudflare-r2');
});

test('large registration backlogs are selected one FIFO batch at a time', () => {
  const pending = Array.from({ length: 100 }, (_, index) => ({
    key: `uploads/file-${String(index).padStart(3, '0')}.mp4`,
    uploaded: new Date(Date.UTC(2026, 7, 21, 0, index)),
  })).reverse();

  assert.deepEqual(
    selectOldestRegistrationBatch(pending, new Set(), 3).map(row => row.key),
    [
      'uploads/file-000.mp4',
      'uploads/file-001.mp4',
      'uploads/file-002.mp4',
    ],
  );
  assert.deepEqual(
    selectOldestRegistrationBatch(
      pending,
      new Set(['uploads/file-000.mp4']),
      1,
    ).map(row => row.key),
    ['uploads/file-001.mp4'],
  );
});

test('video processing claims the oldest pending upload first', () => {
  const claimStart = workerSource.indexOf('const claimVideoJobs');
  const claimEnd = workerSource.indexOf('const getProcessorJobs', claimStart);
  const source = workerSource.slice(claimStart, claimEnd);

  assert.match(source, /ORDER BY created_at ASC, id ASC/);
  assert.doesNotMatch(source, /created_at DESC/);
});

test('detected and registering status transitions use batch database writes', () => {
  const syncStart = workerSource.indexOf('const syncDetectedStatuses');
  const syncEnd = workerSource.indexOf('const retryStaleProcessing', syncStart);
  const syncSource = workerSource.slice(syncStart, syncEnd);
  assert.match(syncSource, /upsertRegistrationStatuses/);
  assert.match(syncSource, /filter\(\(\[url\]\) => !registrationRowsByUrl\.has\(url\)\)/);
  assert.doesNotMatch(syncSource, /Promise\.all/);

  assert.match(workerSource, /jsonb_to_recordset/);
  assert.match(workerSource, /REGISTRATION_STATUS_WRITE_BATCH_SIZE = 25/);
  assert.match(workerSource, /rows\.slice\(offset, offset \+ REGISTRATION_STATUS_WRITE_BATCH_SIZE\)/);
  assert.doesNotMatch(
    workerSource,
    /Promise\.all\(batch\.map\(object => \{[\s\S]*?status: 'registering'/,
  );
});

test('only an in-progress registration is recovered as stalled', () => {
  const staleStart = workerSource.indexOf('const clearStaleRegistrationStatuses');
  const staleEnd = workerSource.indexOf('const clearOldCompletedRegistrationStatuses', staleStart);
  const staleSource = workerSource.slice(staleStart, staleEnd);

  assert.match(staleSource, /WHERE status='registering'/);
  assert.doesNotMatch(staleSource, /status IN \('detected', 'registering'\)/);
  assert.match(staleSource, /WHERE status='detected'[\s\S]*?error_message=\$\{STALE_REGISTRATION_ERROR_MESSAGE\}/);
});

test('a registration scan does not fan out direct database connections for a backlog', () => {
  const scanStart = workerSource.indexOf('const scanAndRegisterWithLease');
  const scanEnd = workerSource.indexOf('const scanAndRegister =', scanStart);
  const source = workerSource.slice(scanStart, scanEnd);

  assert.match(source, /const storageScanCursor = await getStorageScanCursor\(env\)/);
  assert.match(source, /listedPageForScan \?\? await listStorageObjectPage/);
  assert.match(source, /await saveStorageScanCursor\(env, listedPage\.nextCursor\)/);
  assert.match(source, /getMediaRowsForUrls\(env, candidateUrls\)/);
  assert.match(source, /getRegistrationStatusRowsForUrls/);
  assert.match(source, /getRegisteredUploadFileMapRowsForUrls/);
  assert.match(source, /const queuedDeletionPrefixes = await getQueuedDeletionPrefixes\(env\)/);
  assert.doesNotMatch(source, /Promise\.all\(\[\s*listStorageObjectPage\(env\)/);
});

test('registration consumes the durable queue FIFO across inventory pages', () => {
  const scanStart = workerSource.indexOf('const scanAndRegisterWithLease');
  const scanEnd = workerSource.indexOf('const scanAndRegister =', scanStart);
  const source = workerSource.slice(scanStart, scanEnd);

  assert.match(source, /getQueuedRegistrationStatusRows/);
  assert.match(source, /queuedUploads/);
  assert.match(source, /selectOldestRegistrationBatch\(\s*queuedUploads/);
  assert.match(workerSource, /WHERE status='detected'[\s\S]*?ORDER BY uploaded_at ASC NULLS LAST, created_at ASC, url ASC/);
});

test('status polling does not fan out database connections', () => {
  const statusStart = workerSource.indexOf('const status = async');
  const statusEnd = workerSource.indexOf('let scanInFlight', statusStart);
  const source = workerSource.slice(statusStart, statusEnd);

  assert.doesNotMatch(source, /Promise\.all/);
  assert.doesNotMatch(source, /getDeletionQueueCounts\(env\)/);
  assert.match(source, /Keep the[\s\S]*admin snapshot to one session/);
  assert.match(source, /COALESCE\(\([\s\S]*?registration_snapshot/);
});

test('registration status and logs expose file-level queue progress', () => {
  assert.match(workerSource, /registrationQueue:/);
  assert.match(workerSource, /registrationJobs:/);
  assert.match(workerSource, /event: 'registration_started'/);
  assert.match(workerSource, /event: isRecoverableCopyDelay[\s\S]*?'registration_waiting_for_storage'/);
  assert.match(workerSource, /phase: registrationPhase/);
});

test('Drive registration I/O is deadline-bound so a scan lease cannot stick forever', () => {
  assert.match(workerSource, /const REGISTRATION_STORAGE_TIMEOUT_MS = 30_000/);
  assert.match(workerSource, /SCAN_WATCHDOG_TIMEOUT_MS = 120_000/);
  assert.match(workerSource, /DRIVE_COPY_VISIBILITY_ATTEMPTS = 3/);
  assert.match(workerSource, /DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS = 3/);
  assert.match(workerSource, /DRIVE_COPY_VISIBILITY_DELAY_MS = 2000/);

  const listStart = workerSource.indexOf('const listStorageObjectPage');
  const listEnd = workerSource.indexOf('const putObject', listStart);
  assert.match(
    workerSource.slice(listStart, listEnd),
    /signal: AbortSignal\.timeout\(REGISTRATION_STORAGE_TIMEOUT_MS\)/,
  );

  const finalizeStart = workerSource.indexOf('const finalizeDriveUpload');
  const finalizeEnd = workerSource.indexOf('const storageObjectExists', finalizeStart);
  assert.match(
    workerSource.slice(finalizeStart, finalizeEnd),
    /signal: AbortSignal\.timeout\(REGISTRATION_STORAGE_TIMEOUT_MS\)/,
  );

  const objectSizeStart = workerSource.indexOf('const storageObjectSize');
  const objectSizeEnd = workerSource.indexOf('const finalizeDriveUpload', objectSizeStart);
  assert.match(
    workerSource.slice(objectSizeStart, objectSizeEnd),
    /signal: AbortSignal\.timeout\(REGISTRATION_STORAGE_TIMEOUT_MS\)/,
  );
});

test('a hung scan cannot pin the shared scheduled scan promise forever', () => {
  assert.match(workerSource, /Promise\.race\(\[scanAndRegister\(env\), watchdog\]\)/);
  assert.match(workerSource, /Registration scan watchdog exceeded/);
  assert.match(workerSource, /ctx\.waitUntil\(scan\.promise\.catch/);
});

test('scheduled scans do not share an in-memory promise across cron events', () => {
  assert.match(workerSource, /startScan\([\s\S]*?\{ shareInFlight: false \}/);
  assert.match(workerSource, /if \(shareInFlight && scanInFlight\)/);
});

test('stalled registration rows are requeued instead of left as permanent errors', () => {
  const staleStart = workerSource.indexOf('const clearStaleRegistrationStatuses');
  const staleEnd = workerSource.indexOf('const clearOldCompletedRegistrationStatuses', staleStart);
  const source = workerSource.slice(staleStart, staleEnd);

  assert.match(source, /status='detected'/);
  assert.match(workerSource, /Previous registration attempt stalled; queued for retry/);
  assert.doesNotMatch(source, /status='error'/);
});

test('manual retries explicitly requeue the matching registration record', () => {
  const retryStart = workerSource.indexOf("url.pathname === '/registration/retry'");
  const retryEnd = workerSource.indexOf("if (url.pathname === '/run'", retryStart);
  const source = workerSource.slice(retryStart, retryEnd);

  assert.match(source, /requeueRegistrationStatuses/);
  assert.match(source, /scheduleScan/);
  assert.match(workerSource, /status='detected'/);
});

test('Supabase scans use a fresh bounded client and retry a dropped connection', () => {
  assert.match(workerSource, /new Client\(/);
  assert.match(workerSource, /connectionTimeoutMillis: SUPABASE_CONNECT_TIMEOUT_MS/);
  assert.match(workerSource, /query_timeout: SUPABASE_QUERY_TIMEOUT_MS/);
  assert.match(workerSource, /isRetryableSupabaseConnectionError/);
  assert.match(workerSource, /SUPABASE_CONNECTION_RETRY_ATTEMPTS = 3/);
  assert.match(workerSource, /await client\.end\(\)\.catch/);
  assert.match(workerSource, /Postgres query failed after/);
  assert.match(workerSource, /describePostgresQuery/);
  assert.doesNotMatch(workerSource, /new Pool\(/);
});

test('optional upload hints cannot abort a registration scan', () => {
  assert.match(workerSource, /lookupUrls\.length; offset \+= 200/);
  assert.match(workerSource, /Upload registration hints unavailable; continuing without hints/);
  assert.match(workerSource, /Upload registration hint chunk unavailable; continuing without it/);
});

test('a deletion queue drain cannot block the scheduled registration scan', () => {
  const scheduledStart = workerSource.indexOf('async scheduled(');
  const scheduledEnd = workerSource.indexOf('async fetch(', scheduledStart);
  const source = workerSource.slice(scheduledStart, scheduledEnd);

  assert.match(source, /const deletionDrain = startDeletionDrain\(env\)/);
  assert.match(source, /ctx\.waitUntil\(deletionDrain\.promise\.catch/);
  assert.doesNotMatch(source, /await startDeletionDrain/);
  assert.match(source, /continuing scheduled registration scan/);
  assert.match(source, /startScan\([\s\S]*?envWithRuntimeSettings\(env, settings\)[\s\S]*?shareInFlight: false/);
  assert.match(workerSource, /ctx\.waitUntil\(drain\.promise\.catch/);
  assert.match(workerSource, /continuing registration with cached prefixes/);
});

test('active processing rows are failed only after storage confirms missing', () => {
  assert.equal(shouldMarkProcessingSourceMissing({
    status: 'pending',
    sourceKey: 'uploads/video.mp4',
    isListed: false,
    exists: false,
  }), true);
  assert.equal(shouldMarkProcessingSourceMissing({
    status: 'processing',
    sourceKey: 'uploads/video.mp4',
    isListed: true,
  }), false);
  assert.equal(shouldMarkProcessingSourceMissing({
    status: 'pending',
    sourceKey: 'uploads/video.mp4',
    isListed: false,
    exists: undefined,
  }), false);
  assert.equal(shouldMarkProcessingSourceMissing({
    status: 'ready',
    sourceKey: '',
    isListed: false,
  }), false);
});

test('deletion prefixes cover nested media derivatives without sibling IDs', () => {
  assert.equal(deletionKeyMatchesPrefix(
    'uploads/token/123456789012-preview.mp4',
    'uploads/token/123456789012',
  ), true);
  assert.equal(deletionKeyMatchesPrefix(
    'uploads/token/123456789012-subtitles.json',
    'uploads/token/123456789012',
  ), true);
  assert.equal(deletionKeyMatchesPrefix(
    'uploads/token/1234567890123.mp4',
    'uploads/token/123456789012',
  ), false);
});

test('deletion treats an already-missing source as successfully removed', async () => {
  let removeCalls = 0;
  const result = await deleteStorageKeyIfPresent({
    exists: async () => false,
    remove: async () => { removeCalls += 1; },
  });
  assert.equal(result, 'already-missing');
  assert.equal(removeCalls, 0);
});

test('deletion removes redundant nested and derivative prefix scans', () => {
  assert.deepEqual(buildDeletionPrefixes(
    '464439787784',
    [
      '464439787784',
      '464439787784-poster',
      '464439787784-preview',
      'source-name',
    ],
    [
      'uploads/token/464439787784.mp4',
      '464439787784-poster.jpg',
      '464439787784-preview.mp4',
      'uploads/token/source-name.mp4',
    ],
  ), [
    '464439787784',
    'uploads/token/464439787784',
    'uploads/token/source-name',
  ]);
});

test('processor termination returns the claimed job to the retry queue', () => {
  assert.equal(
    shouldRetryInterruptedJob('Processor interrupted by SIGTERM'),
    true,
  );
  assert.equal(shouldRetryInterruptedJob('Unsupported video codec'), false);
});

test('processor stream uploads are restricted to safe derivative keys', () => {
  assert.equal(isAllowedStreamDerivativeKey('124399888136-stream.mp4'), true);
  assert.equal(isAllowedStreamDerivativeKey('show-name-stream.webm'), true);
  assert.equal(isAllowedStreamDerivativeKey('../source.mkv'), false);
  assert.equal(isAllowedStreamDerivativeKey('124399888136-preview.mp4'), false);
});

test('HLS VOD artifacts use stable, flat derivative keys', () => {
  assert.equal(isAllowedHlsDerivativeKey('movie-hls.m3u8'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-init.mp4'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-00001.m4s'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-high.m3u8'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-high-init.mp4'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-high-00001.m4s'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-720p.m3u8'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-720p-init.mp4'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-720p-00001.m4s'), true);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls/segment-00001.m4s'), false);
  assert.equal(isAllowedHlsDerivativeKey('movie-hls-random-init.mp4'), false);
});

test('HLS reconciliation is bounded and requires canonical delivery URLs', () => {
  const reconcileStart = workerSource.indexOf('const reconcileMissingHlsArtifacts');
  const reconcileEnd = workerSource.indexOf('const commitCanonicalVideo', reconcileStart);
  const source = workerSource.slice(reconcileStart, reconcileEnd);
  assert.match(source, /LIMIT 12/);
  assert.match(source, /FOR UPDATE SKIP LOCKED/);
  assert.match(source, /urlForKey\(env, key\) !== uri/);
  assert.match(source, /hls_verified_at/);
});

test('canonical processor uploads keep the media ID and plain mp4 name', () => {
  assert.equal(isAllowedProcessorUploadKey(
    'uploads/folder/123456789012.mp4',
    '123456789012',
  ), true);
  assert.equal(isAllowedProcessorUploadKey(
    'uploads/folder/123456789012-converted.mp4',
    '123456789012',
  ), false);
  assert.equal(isAllowedProcessorUploadKey(
    'uploads/folder/other.mp4',
    '123456789012',
  ), false);
});

test('subtitle upload metadata keeps named multi-track files and rejects unrelated paths', () => {
  assert.deepEqual(getValidSubtitleUploadMetadata(
    '124399888136',
    [
      { fileName: '124399888136-subtitles.eng.vtt', lang: 'eng', label: 'English Full' },
      { fileName: '124399888136-subtitles.eng-2.vtt', lang: 'eng', label: 'English Signs' },
      { fileName: '../outside.vtt', lang: 'eng', label: 'Unsafe' },
    ],
    [
      '124399888136-subtitles.eng.vtt',
      '124399888136-subtitles.eng-2.vtt',
      '../outside.vtt',
    ],
  ), [
    { fileName: '124399888136-subtitles.eng.vtt', lang: 'eng', label: 'English Full' },
    { fileName: '124399888136-subtitles.eng-2.vtt', lang: 'eng', label: 'English Signs' },
  ]);
});

test('new extracted subtitle metadata updates matching tracks and preserves manual tracks', () => {
  assert.deepEqual(mergeSubtitleManifestTracks(
    [
      { src: 'https://storage/1-subtitles.eng.vtt', lang: 'eng', label: 'Old English' },
      { src: 'https://storage/1-subtitles.custom.vtt', lang: 'custom', label: 'Custom' },
    ],
    [{ src: 'https://storage/1-subtitles.eng.vtt', lang: 'eng', label: 'English Full' }],
  ), [
    { src: 'https://storage/1-subtitles.eng.vtt', lang: 'eng', label: 'English Full' },
    { src: 'https://storage/1-subtitles.custom.vtt', lang: 'custom', label: 'Custom' },
  ]);
});

test('media ID allocation retries instead of overwriting an occupied ID', async () => {
  const candidates = ['111111111111', '222222222222'];
  const mediaId = await findAvailableMediaId(
    async attempt => candidates[attempt],
    new Set(['111111111111']),
  );

  assert.equal(mediaId, '222222222222');
});

test('re-uploading the same storage key creates a different media identity', async () => {
  const url = 'https://storage.example/uploads/repeated-name.mp4';
  const first = await stableMediaIdForUrl(url, new Date('2026-08-06T10:00:00Z'));
  const second = await stableMediaIdForUrl(url, new Date('2026-08-06T11:00:00Z'));

  assert.notEqual(first, second);
});

test('deferred cleanup never deletes a newer re-upload at the same key', () => {
  const mapUpdatedAt = new Date('2026-08-06T10:30:00Z');
  assert.equal(isDeferredSourceCleanupSafe(
    new Date('2026-08-06T10:00:00Z'),
    mapUpdatedAt,
  ), true);
  assert.equal(isDeferredSourceCleanupSafe(
    new Date('2026-08-06T11:00:00Z'),
    mapUpdatedAt,
  ), false);
});

test('an existing generated destination is trusted only when its size matches', () => {
  assert.equal(isVerifiedStorageCopy(100, 100), true);
  assert.equal(isVerifiedStorageCopy(undefined, 100), true);
  assert.equal(isVerifiedStorageCopy(100, 99), false);
  assert.equal(isVerifiedStorageCopy(100, undefined), false);
});

test('a matching destination size from the scan listing is sufficient', () => {
  assert.equal(isVerifiedStorageCopy(68, 68), true);
  assert.equal(isVerifiedStorageCopy(68, 0), false);
});

test('copy verification tolerates delayed Drive destination visibility', async () => {
  const observedSizes = [undefined, undefined, 68];
  let waits = 0;
  const destinationSize = await waitForVerifiedStorageCopy({
    sourceSize: 68,
    readDestinationSize: async () => observedSizes.shift(),
    attempts: 3,
    delayMs: 1,
    wait: async () => { waits += 1; },
  });

  assert.equal(destinationSize, 68);
  assert.equal(waits, 2);
});

test('Drive copy verification is short and resumable', () => {
  const coveredDelay =
    (DRIVE_COPY_VISIBILITY_ATTEMPTS - 1) * DRIVE_COPY_VISIBILITY_DELAY_MS;
  assert.ok(coveredDelay > 0 && coveredDelay <= 5_000);
});

test('a retry checks a tracked destination without holding the scan lease', () => {
  const coveredDelay =
    (DRIVE_RETRY_TARGET_VISIBILITY_ATTEMPTS - 1) *
    DRIVE_COPY_VISIBILITY_DELAY_MS;
  assert.ok(coveredDelay > 0 && coveredDelay <= 5_000);
});

test('an in-flight tracked Drive copy is not started a second time', () => {
  assert.equal(shouldWaitForTrackedRegistrationDestination({
    shouldVerifyExistingTarget: true,
    registrationStatus: 'registering',
    targetAlreadyRegistered: false,
  }), true);
  assert.equal(shouldWaitForTrackedRegistrationDestination({
    shouldVerifyExistingTarget: true,
    registrationStatus: 'registering',
    targetAlreadyRegistered: true,
  }), false);
  assert.equal(shouldWaitForTrackedRegistrationDestination({
    shouldVerifyExistingTarget: true,
    registrationStatus: 'error',
    targetAlreadyRegistered: false,
  }), false);
});

test('delayed Drive copy visibility remains recoverable', () => {
  assert.equal(isRecoverableDriveCopyError(new Error(
    'Copied destination is not readable in storage: uploads/123.mkv',
  )), true);
  assert.equal(isRecoverableDriveCopyError(new Error(
    'Copied destination size mismatch: source=100 destination=50',
  )), true);
  assert.equal(isRecoverableDriveCopyError(new Error(
    'Drive copy failed (403)',
  )), false);
});

test('a generated retry destination is not treated as a separate upload', () => {
  const sourceUrl = 'https://storage.example/staging/Original.png';
  const expectedUrl = 'https://storage.example/staging/123456789012.png';
  assert.equal(isProtectedRegistrationDestination({
    objectUrl: expectedUrl,
    sourceUrl,
    expectedUrl,
    sourceExists: true,
  }), true);
  assert.equal(isProtectedRegistrationDestination({
    objectUrl: expectedUrl,
    sourceUrl,
    expectedUrl,
    sourceExists: false,
  }), false);
});

test('a retry reuses the generated destination tied to its tracked media ID', () => {
  assert.equal(shouldVerifyExistingRegistrationDestination({
    sourceKey: 'staging/Original.png',
    destinationKey: 'staging/123456789012.png',
    mediaId: '123456789012',
    trackedMediaId: '123456789012',
    targetRecordedAsRegistered: false,
  }), true);
  assert.equal(shouldVerifyExistingRegistrationDestination({
    sourceKey: 'staging/Original.png',
    destinationKey: 'staging/123456789012.png',
    mediaId: '123456789012',
    trackedMediaId: '999999999999',
    targetRecordedAsRegistered: false,
  }), false);
});

test('registration prepares, commits, then cleans up in order', async () => {
  const events = [];

  await runSafeRegistrationCommit({
    prepareDestination: async () => { events.push('prepared'); },
    commitRegistration: async () => { events.push('committed'); },
    cleanupSource: async () => { events.push('cleaned'); },
  });

  assert.deepEqual(events, ['prepared', 'committed', 'cleaned']);
});

test('registration never deletes the source when destination preparation fails', async () => {
  let committed = false;
  let cleaned = false;

  await assert.rejects(() => runSafeRegistrationCommit({
    prepareDestination: async () => { throw new Error('copy failed'); },
    commitRegistration: async () => { committed = true; },
    cleanupSource: async () => { cleaned = true; },
  }), /copy failed/);

  assert.equal(committed, false);
  assert.equal(cleaned, false);
});

test('registration never deletes the source when the database commit fails', async () => {
  let cleaned = false;

  await assert.rejects(() => runSafeRegistrationCommit({
    prepareDestination: async () => undefined,
    commitRegistration: async () => { throw new Error('database failed'); },
    cleanupSource: async () => { cleaned = true; },
  }), /database failed/);

  assert.equal(cleaned, false);
});

test('a cleanup failure does not undo a safely committed registration', async () => {
  let cleanupError;

  await runSafeRegistrationCommit({
    prepareDestination: async () => undefined,
    commitRegistration: async () => undefined,
    cleanupSource: async () => { throw new Error('delete failed'); },
    onCleanupError: error => { cleanupError = error; },
  });

  assert.match(cleanupError.message, /delete failed/);
});
