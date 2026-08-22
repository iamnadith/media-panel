import {
  copyFile,
  deleteFile,
  getFileNamePartsFromStorageUrl,
  moveFile,
  putFile,
  sanitizeStorageFileNameBase,
  deleteFilesWithPrefix,
  getCurrentStorageUrlsForPrefix,
  generateMediaStorageId,
} from '@/platforms/storage';
import { removeGpsData, resizeImageToBytes } from '../server';
import {
  getOptimizedMediaFileMeta,
} from '.';
import { MediaType, TranscodeStatus } from '..';
import { promises as fs, existsSync } from 'fs';
import os from 'os';
import path from 'path';
import sleep from '@/utility/sleep';
import {
  GENERATE_STREAM_DERIVATIVES,
  UNIQUE_MEDIA_NAMES,
} from '@/app/config';
import { hasProcessingOrchestrator } from '@/processing/orchestrator';

const VIDEO_EXTENSIONS = new Set([
  'mp4', 'mkv', 'mov', 'm4v', 'webm', 'avi', 'ts', 'm2ts', 'mts',
  'mpg', 'mpeg', 'wmv', 'flv', '3gp', 'ogv',
]);
const PREVIEW_DURATION_SECONDS = 10;
const PREVIEW_MAX_DURATION_RATIO = 0.3;
const PREVIEW_MAX_WIDTH = 720;
const GENERATED_MEDIA_ID_PATTERN = /^\d{12}$/;

type FfmpegModule = typeof import('fluent-ffmpeg');

type VideoTooling = {
  ffmpeg?: FfmpegModule
  ffmpegBinary?: string
  ffprobeBinary?: string
  canProcess: boolean
};

let videoToolingPromise: Promise<VideoTooling> | undefined;

const loadVideoTooling = async (): Promise<VideoTooling> => {
  try {
    const [{ default: ffmpegModule }, ffmpegStaticModule, ffprobeStaticModule] = await Promise.all([
      import('fluent-ffmpeg').catch(() => ({ default: undefined as unknown as FfmpegModule })),
      import('ffmpeg-static').catch(() => ({ default: undefined as unknown as string | undefined })),
      import('ffprobe-static').catch(() => ({ default: undefined as unknown as string | { path?: string } | undefined })),
    ]);

    const ffmpeg = ffmpegModule ?? undefined;

    let ffmpegBinary: string | undefined;
    const ffmpegStaticValue = (ffmpegStaticModule as { default?: string }).default ?? ffmpegStaticModule;
    if (typeof ffmpegStaticValue === 'string' && existsSync(ffmpegStaticValue)) {
      ffmpegBinary = ffmpegStaticValue;
    }

    let ffprobeBinary: string | undefined;
    const ffprobeStaticValue = (ffprobeStaticModule as { default?: string | { path?: string } }).default ?? ffprobeStaticModule;
    if (typeof ffprobeStaticValue === 'string' && existsSync(ffprobeStaticValue)) {
      ffprobeBinary = ffprobeStaticValue;
    } else if (ffprobeStaticValue && typeof ffprobeStaticValue === 'object' && 'path' in ffprobeStaticValue) {
      const candidate = ffprobeStaticValue.path;
      if (candidate && existsSync(candidate)) {
        ffprobeBinary = candidate;
      }
    }

    if (ffmpeg && ffmpegBinary) {
      ffmpeg.setFfmpegPath(ffmpegBinary);
    }
    if (ffmpeg && ffprobeBinary) {
      ffmpeg.setFfprobePath(ffprobeBinary);
    }

    const canProcess = Boolean(
      (ffmpeg as unknown) &&
      (ffmpegBinary ?? '') &&
      (ffprobeBinary ?? ''),
    );

    return {
      ffmpeg,
      ffmpegBinary,
      ffprobeBinary,
      canProcess,
    };
  } catch (error) {
    console.warn('Video tooling unavailable', error);
    return {
      canProcess: false,
    };
  }
};

const getVideoTooling = () => {
  videoToolingPromise = videoToolingPromise ?? loadVideoTooling();
  return videoToolingPromise;
};

const conversionQueueState: {
  tail: Promise<void>
  inFlight: Map<string, Promise<ConvertUploadToMediaResult>>
} = {
  tail: Promise.resolve(),
  inFlight: new Map(),
};

const enqueueConversion = <T>(task: () => Promise<T>) => {
  const next = conversionQueueState.tail.then(() => task());
  conversionQueueState.tail = next.then(
    () => undefined,
    () => undefined,
  );
  return next;
};

const conversionKeyForArgs = ({
  uploadUrl,
  preferredFileNameBase,
  mediaType,
  overwriteTarget,
  deferTranscode,
  skipVideoProcessing,
  registerOnly,
}: {
  uploadUrl: string
  originalFileName?: string
  preferredFileNameBase?: string
  mediaType?: MediaType
  overwriteTarget?: {
    url: string
    posterUrl?: string
    previewUrl?: string
  }
  deferTranscode?: boolean
  skipVideoProcessing?: boolean
  registerOnly?: boolean
}) => {
  const [normalizedUrl] = decodeURIComponent(uploadUrl ?? '').split('?');
  const { fileName } = getFileNamePartsFromStorageUrl(normalizedUrl);
  return [
    normalizedUrl,
    fileName,
    preferredFileNameBase ?? '',
    mediaType ?? '',
    overwriteTarget?.url ?? '',
    deferTranscode ? 'defer' : 'process',
    skipVideoProcessing ? 'transfer-only' : 'full',
    registerOnly ? 'register-only' : 'convert',
  ].join('::');
};

type RetryOptions = {
  attempts?: number
  delayMs?: number
  label?: string
};

const withRetry = async <T>(
  operation: () => Promise<T>,
  { attempts = 3, delayMs = 400, label }: RetryOptions = {},
): Promise<T> => {
  let lastError: unknown;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt === attempts) {
        throw error;
      }
      if (label) {
        console.warn(`Retrying ${label} (${attempt}/${attempts})`, error);
      }
      await sleep(delayMs * attempt);
    }
  }
  throw lastError ?? new Error('Unknown retry error');
};

type VideoMetadata = {
  durationSeconds?: number
  frameRate?: number
  mediaWidth?: number
  mediaHeight?: number
};

const getPreviewWindow = (durationSeconds?: number) => {
  const safeDuration = (
    typeof durationSeconds === 'number' &&
    Number.isFinite(durationSeconds) &&
    durationSeconds > 0
  ) ? durationSeconds : 0;
  const desiredStart = 60;
  const desiredEnd = 300;
  const windowStart = safeDuration >= desiredStart
    ? desiredStart
    : Math.max(safeDuration * 0.5, 0);
  const windowEnd = safeDuration >= desiredEnd
    ? desiredEnd
    : Math.max(windowStart, safeDuration);
  const midpoint = Math.max(
    windowStart + (windowEnd - windowStart) / 2,
    0,
  );
  const previewDuration = safeDuration > 0
    ? Math.min(
      PREVIEW_DURATION_SECONDS,
      Math.max(1, safeDuration * PREVIEW_MAX_DURATION_RATIO),
    )
    : PREVIEW_DURATION_SECONDS;
  const previewSeek = Math.min(
    Math.max(midpoint - previewDuration / 2, 0),
    Math.max(safeDuration - previewDuration, 0),
  );

  return {
    midpoint,
    previewSeek,
    previewDuration,
  };
};

type ConvertUploadToMediaArgs = {
  uploadUrl: string
  fileBytes?: ArrayBuffer
  shouldStripGpsData?: boolean
  shouldDeleteOrigin?: boolean
  mediaType?: MediaType
  originalFileName?: string
  overwriteTarget?: {
    url: string
    posterUrl?: string
    previewUrl?: string
  }
  preferredFileNameBase?: string
  deferTranscode?: boolean
  skipVideoProcessing?: boolean
  processingError?: string
  registerOnly?: boolean
};

export type ConvertUploadToMediaResult = VideoMetadata & {
  url: string
  mediaType: MediaType
  posterUrl?: string
  previewUrl?: string
  transcodeStatus?: TranscodeStatus
  transcodeError?: string
};

const withTempDir = async <T>(callback: (tempDir: string) => Promise<T>) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'exif-media-'));
  try {
    return await callback(tempDir);
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
  }
};

const probeVideo = async (
  inputPath: string,
  ffmpegInstance: FfmpegModule,
): Promise<VideoMetadata> =>
  new Promise((resolve, reject) => {
    ffmpegInstance(inputPath).ffprobe((error, data) => {
      if (error) {
        reject(error);
        return;
      }
      const videoStream = data.streams?.find(stream => stream.codec_type === 'video');
      const parseMaybeNumber = (v: unknown): number | undefined => {
        if (typeof v === 'number') { return Number.isFinite(v) ? v : undefined; }
        if (typeof v === 'string') {
          const n = parseFloat(v);
          return Number.isFinite(n) ? n : undefined;
        }
        return undefined;
      };
      const durationSeconds =
        parseMaybeNumber((videoStream as any)?.duration) ??
        parseMaybeNumber((data as any)?.format?.duration);
      const frameRateRaw = videoStream?.avg_frame_rate;
      let frameRate: number | undefined;
      if (frameRateRaw && frameRateRaw !== '0/0') {
        const [numerator, denominator] = frameRateRaw.split('/');
        const numeratorNumber = Number(numerator);
        const denominatorNumber = Number(denominator);
        frameRate = denominatorNumber !== 0
          ? numeratorNumber / denominatorNumber
          : undefined;
      }
      resolve({
        durationSeconds,
        frameRate,
        mediaWidth: videoStream?.width,
        mediaHeight: videoStream?.height,
      });
    });
  });

const generateVideoDerivatives = async ({
  tempDir,
  inputPath,
  fileNameBase,
  durationSeconds,
  ffmpegInstance,
}: {
  tempDir: string
  inputPath: string
  fileNameBase: string
  durationSeconds?: number
  ffmpegInstance: FfmpegModule
}) => {
  const posterFilePath = path.join(tempDir, `${fileNameBase}-poster.jpg`);
  // We'll choose preview extension dynamically based on successful codec
  let previewFilePath = path.join(tempDir, `${fileNameBase}-preview.mp4`);
  const {
    midpoint,
    previewDuration,
    previewSeek,
  } = getPreviewWindow(durationSeconds);
  // Generate poster (JPEG)
  await new Promise<void>((resolve, reject) => {
    ffmpegInstance(inputPath)
      .seekInput(midpoint)
      .frames(1)
      .outputOptions(['-qscale:v 2'])
      .output(posterFilePath)
      .on('end', () => resolve())
      .on('error', reject)
      .run();
  });
  // Try to create a short preview clip with fallbacks
  const tryPreview = async (): Promise<string | undefined> => {
    // Attempt MP4 (H.264)
    try {
      await new Promise<void>((resolve, reject) => {
        ffmpegInstance(inputPath)
          .seekInput(previewSeek)
          .duration(previewDuration)
          .videoCodec('libx264')
          .noAudio()
          .outputOptions([
            '-movflags', 'faststart',
            '-preset', 'veryfast',
            '-pix_fmt', 'yuv420p',
            `-vf`,
            `scale='min(${PREVIEW_MAX_WIDTH},iw)':-2`,
          ])
          .output(previewFilePath)
          .on('end', () => resolve())
          .on('error', reject)
          .run();
      });
      return previewFilePath;
    } catch {}

    // Attempt MP4 (H.265)
    try {
      previewFilePath = path.join(tempDir, `${fileNameBase}-preview.mp4`);
      await new Promise<void>((resolve, reject) => {
        ffmpegInstance(inputPath)
          .seekInput(previewSeek)
          .duration(previewDuration)
          .videoCodec('libx265')
          .noAudio()
          .outputOptions([
            '-movflags', 'faststart',
            '-preset', 'veryfast',
            '-pix_fmt', 'yuv420p',
            `-vf`,
            `scale='min(${PREVIEW_MAX_WIDTH},iw)':-2`,
          ])
          .output(previewFilePath)
          .on('end', () => resolve())
          .on('error', reject)
          .run();
      });
      return previewFilePath;
    } catch {}

    // Attempt WebM (VP9)
    try {
      previewFilePath = path.join(tempDir, `${fileNameBase}-preview.webm`);
      await new Promise<void>((resolve, reject) => {
        ffmpegInstance(inputPath)
          .seekInput(previewSeek)
          .duration(previewDuration)
          .videoCodec('libvpx-vp9')
          .noAudio()
          .outputOptions([
            `-vf`,
            `scale='min(${PREVIEW_MAX_WIDTH},iw)':-2`,
            '-deadline', 'realtime',
          ])
          .output(previewFilePath)
          .on('end', () => resolve())
          .on('error', reject)
          .run();
      });
      return previewFilePath;
    } catch {}

    return undefined;
  };

  const preview = await tryPreview().catch(() => undefined);
  return {
    posterFilePath,
    previewFilePath: preview,
  };
};

// Build and store a JSON manifest of available subtitle sidecars for a given base
const buildAndStoreSubtitlesManifest = async (fileNameBase: string) => {
  const existing = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
  const tracks = existing
    .filter(({ fileName }) => /-subtitles(\.[a-zA-Z0-9_-]+)?\.vtt$/i.test(fileName))
    .map(({ fileName, url }) => {
      const match = fileName.match(/-subtitles\.([a-zA-Z0-9_-]+)\.vtt$/i);
      const lang = match?.[1] || 'default';
      return { src: url, lang, label: lang.toUpperCase() };
    });
  const manifest = JSON.stringify({ tracks }, null, 0);
  await withRetry(
    () => putFile(Buffer.from(manifest), `${fileNameBase}-subtitles.json`),
    { label: `store subtitles manifest ${fileNameBase}` },
  ).catch(() => undefined);
};

const getVideoSidecarUrls = async (fileNameBase: string) => {
  const existing = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
  const posterUrl = existing.find(({ url }) => {
    const { fileName } = getFileNamePartsFromStorageUrl(url);
    return fileName.toLowerCase() === `${fileNameBase}-poster.jpg`.toLowerCase();
  })?.url;
  const previewUrl = existing.find(({ url }) => {
    const { fileName } = getFileNamePartsFromStorageUrl(url);
    return /^.+-preview\.(mp4|webm)$/i.test(fileName) &&
      fileName.toLowerCase().startsWith(`${fileNameBase}-preview.`);
  })?.url;

  return {
    posterUrl,
    previewUrl,
  };
};

// Convert arbitrary subtitle uploads to WebVTT and store
export const storeUploadedSubtitles = async ({
  fileNameBase,
  files,
  lang,
}: {
  fileNameBase: string
  files: { name: string, bytes: ArrayBuffer }[]
  lang: string
}) => {
  const videoTooling = await getVideoTooling();
  if (!videoTooling.ffmpeg) {
    throw new Error('Video processing unavailable');
  }
  await withTempDir(async (tempDir) => {
    const written: string[] = [];
    for (const f of files) {
      const local = path.join(tempDir, f.name);
      await fs.writeFile(local, Buffer.from(f.bytes));
      written.push(local);
    }
    let primary = written[0];
    const idx = written.find(p => p.toLowerCase().endsWith('.idx'));
    if (idx) { primary = idx; }
    const outVtt = path.join(tempDir, `${fileNameBase}-subtitles.${lang}.vtt`);
    await new Promise<void>((resolve, reject) => {
      videoTooling.ffmpeg!(primary)
        .noVideo()
        .noAudio()
        .format('webvtt')
        .output(outVtt)
        .on('end', () => resolve())
        .on('error', reject)
        .run();
    });
    const vttBuffer = await fs.readFile(outVtt);
    await withRetry(
      () => putFile(vttBuffer, `${fileNameBase}-subtitles.${lang}.vtt`),
      { label: `store uploaded subtitles ${fileNameBase} (${lang})` },
    );
  });
  await buildAndStoreSubtitlesManifest(fileNameBase);
  return `${fileNameBase}-subtitles.${lang}.vtt`;
};

const storeOptimizedMedia = async (
  url: string,
  fileBytes: ArrayBuffer,
) => {
  const { fileNameBase } = getFileNamePartsFromStorageUrl(url);
  const optimizedMediaFileMeta = getOptimizedMediaFileMeta(fileNameBase);
  for (const { fileName, size, quality } of optimizedMediaFileMeta) {
    const resized = await resizeImageToBytes(fileBytes, size, quality);
    await withRetry(
      () => putFile(resized, fileName),
      { label: `put optimized photo ${fileName}` },
    );
  }
  return url;
};

const determineMediaType = (
  fileExtension?: string,
  explicitMediaType?: MediaType,
): MediaType =>
  explicitMediaType
    ? explicitMediaType
    : fileExtension && VIDEO_EXTENSIONS.has(fileExtension.toLowerCase())
      ? 'video'
      : 'photo';

const isGeneratedMediaBase = (base?: string) =>
  Boolean(base && GENERATED_MEDIA_ID_PATTERN.test(base));

const shouldReuseMediaBase = (base?: string) =>
  Boolean(base);

const buildMediaBaseWithId = (sourceBase?: string) => {
  if (UNIQUE_MEDIA_NAMES) { return generateMediaStorageId(); }
  return sanitizeStorageFileNameBase(sourceBase, 'media');
};

const convertUploadToMediaInternal = async ({
  uploadUrl,
  fileBytes: providedFileBytes,
  shouldStripGpsData,
  shouldDeleteOrigin = true,
  mediaType: explicitMediaType,
  originalFileName,
  overwriteTarget,
  preferredFileNameBase,
  deferTranscode,
  skipVideoProcessing,
  processingError,
  registerOnly,
}: ConvertUploadToMediaArgs): Promise<ConvertUploadToMediaResult> => {
  const {
    fileExtension,
    fileName: uploadFileName,
  } = getFileNamePartsFromStorageUrl(uploadUrl);
  const extensionFromUpload = fileExtension?.toLowerCase() ?? '';
  const fallbackBaseFromUpload = uploadFileName
    ? sanitizeStorageFileNameBase(
      uploadFileName.replace(/\.[^/.]+$/, ''),
    )
    : undefined;
  const providedOriginalFileName = originalFileName?.trim();
  const baseFromOriginal = providedOriginalFileName
    ? sanitizeStorageFileNameBase(
      providedOriginalFileName.replace(/\.[^/.]+$/, ''),
    )
    : undefined;
  const extensionFromOriginal = providedOriginalFileName
    ?.split('.')
    .pop()
    ?.toLowerCase();
  const mediaType = determineMediaType(extensionFromUpload, explicitMediaType);
  const extension = (
    extensionFromUpload ||
    extensionFromOriginal ||
    (mediaType === 'photo' ? 'jpg' : 'mp4')
  ).toLowerCase();
  const overwriteTargetFile = overwriteTarget?.url
    ? getFileNamePartsFromStorageUrl(overwriteTarget.url)
    : undefined;
  const sourceFileNameBase =
    baseFromOriginal ||
    fallbackBaseFromUpload ||
    'media';
  if (
    !UNIQUE_MEDIA_NAMES &&
    baseFromOriginal &&
    !overwriteTargetFile?.fileNameBase
  ) {
    const existingSourceBaseFiles =
      await getCurrentStorageUrlsForPrefix(baseFromOriginal).catch(() => []);
    if (existingSourceBaseFiles.some(item => {
      if (item.url === uploadUrl) { return false; }
      const { fileNameBase } = getFileNamePartsFromStorageUrl(item.url);
      return (
        fileNameBase === baseFromOriginal ||
        fileNameBase.startsWith(`${baseFromOriginal}-`)
      );
    })) {
      throw new Error(`Duplicate media file already exists: ${baseFromOriginal}.${extension}`);
    }
  }
  let fileNameBase = shouldReuseMediaBase(preferredFileNameBase)
    ? preferredFileNameBase!
    : shouldReuseMediaBase(overwriteTargetFile?.fileNameBase)
      ? overwriteTargetFile!.fileNameBase
      : buildMediaBaseWithId(sourceFileNameBase);
  const shouldPreferExplicitPreferredBase =
    Boolean(
      preferredFileNameBase &&
      shouldReuseMediaBase(preferredFileNameBase) &&
      preferredFileNameBase !== overwriteTargetFile?.fileNameBase,
    );
  if (shouldPreferExplicitPreferredBase) {
    fileNameBase = preferredFileNameBase!;
  }
  const resolveUniqueDestination = async (
    base: string,
    ext: string,
  ) => {
    const normalizedBase = sanitizeStorageFileNameBase(base);
    const normalizedExtension = ext.toLowerCase();
    const isCandidateTaken = async (base: string, fileNameToCheck: string) => {
      const existing = await getCurrentStorageUrlsForPrefix(base).catch(() => []);
      const existingNames = new Set(
        existing.map(({ fileName }) => fileName.toLowerCase()),
      );
      const baseLower = base.toLowerCase();
      return (
        existingNames.has(fileNameToCheck.toLowerCase()) ||
        existing.some(item => {
          const { fileNameBase } = getFileNamePartsFromStorageUrl(item.url);
          const itemBaseLower = fileNameBase.toLowerCase();
          return (
            itemBaseLower === baseLower ||
            itemBaseLower.startsWith(`${baseLower}-`)
          );
        })
      );
    };
    let candidateBase = normalizedBase;
    let candidateFileName = `${candidateBase}.${normalizedExtension}`;
    let attempt = 0;
    while (
      await isCandidateTaken(candidateBase, candidateFileName) &&
      attempt < 30
    ) {
      if (!UNIQUE_MEDIA_NAMES) {
        throw new Error(`Duplicate media file already exists: ${candidateFileName}`);
      }
      candidateBase = buildMediaBaseWithId(sourceFileNameBase);
      candidateFileName = `${candidateBase}.${normalizedExtension}`;
      attempt += 1;
    }
    if (await isCandidateTaken(candidateBase, candidateFileName)) {
      throw new Error(`Unable to resolve unique storage filename for ${normalizedBase}`);
    }
    return {
      fileNameBase: candidateBase,
      fileName: candidateFileName,
    };
  };

  let fileName: string;
  if (UNIQUE_MEDIA_NAMES && preferredFileNameBase && shouldReuseMediaBase(preferredFileNameBase)) {
    fileNameBase = preferredFileNameBase;
    fileName = `${fileNameBase}.${extension}`;
  } else if (
    !shouldPreferExplicitPreferredBase &&
    overwriteTargetFile?.fileName &&
    shouldReuseMediaBase(overwriteTargetFile.fileNameBase)
  ) {
    fileNameBase = overwriteTargetFile.fileNameBase;
    fileName = overwriteTargetFile.fileName;
  } else if (preferredFileNameBase && shouldReuseMediaBase(preferredFileNameBase)) {
    fileNameBase = preferredFileNameBase;
    fileName = `${fileNameBase}.${extension}`;
  } else {
    const {
      fileNameBase: uniqueFileNameBase,
      fileName: uniqueFileName,
    } = await resolveUniqueDestination(fileNameBase, extension);
    fileNameBase = uniqueFileNameBase;
    fileName = uniqueFileName;
  }

  const prefixesToClear = new Set<string>();
  const originFileNameBase = getFileNamePartsFromStorageUrl(uploadUrl)
    .fileNameBase;
  if (preferredFileNameBase && preferredFileNameBase !== fileNameBase) {
    prefixesToClear.add(preferredFileNameBase);
  }
  if (overwriteTargetFile?.fileNameBase && overwriteTargetFile.fileNameBase !== fileNameBase) {
    prefixesToClear.add(overwriteTargetFile.fileNameBase);
  }
  const cleanupStalePrefixes = async () => {
    for (const prefix of prefixesToClear) {
      if (prefix) {
        const staleFiles =
          await getCurrentStorageUrlsForPrefix(prefix).catch(() => []);
        await Promise.all(staleFiles
          .filter(item => {
            const { fileNameBase } = getFileNamePartsFromStorageUrl(item.url);
            return (
              fileNameBase === prefix ||
              fileNameBase.startsWith(`${prefix}-`)
            );
          })
          .map(({ url }) => deleteFile(url).catch(() => undefined)));
      }
    }
  };
  const renameExistingSidecars = async () => {
    if (!shouldRenameUniqueStorageObject) { return; }

    const originBase = overwriteTargetFile?.fileNameBase ?? originFileNameBase;
    if (!originBase || originBase === fileNameBase) { return; }

    const existing = await getCurrentStorageUrlsForPrefix(originBase).catch(() => []);
    await Promise.all(existing.map(async ({ url: existingUrl }) => {
      const { fileName: existingFileName } =
        getFileNamePartsFromStorageUrl(existingUrl);
      if (existingFileName.toLowerCase() === uploadFileName.toLowerCase()) {
        return;
      }
      if (!existingFileName.startsWith(`${originBase}-`)) {
        return;
      }

      const destinationFileName =
        `${fileNameBase}${existingFileName.slice(originBase.length)}`;
      await withRetry(
        () => moveFile(existingUrl, destinationFileName),
        { label: `rename storage sidecar ${destinationFileName}`, attempts: 2 },
      ).catch(() => undefined);
    }));
  };

  const shouldTransferOnlyVideo =
    mediaType === 'video' && Boolean(skipVideoProcessing);
  const shouldFetchBytesFromHost =
    mediaType === 'photo' ||
    Boolean(shouldStripGpsData) ||
    !shouldTransferOnlyVideo;
  const fileBytes = providedFileBytes ??
    (registerOnly || !shouldFetchBytesFromHost
      ? undefined
      : await withRetry(async () => {
        const response = await fetch(uploadUrl);
        if (!response.ok) {
          throw new Error(`Failed to download upload (${response.status})`);
        }
        return response.arrayBuffer();
      }, {
        attempts: 8,
        delayMs: 1000,
        label: `download upload ${fileName}`,
      }));

  if (!fileBytes && shouldFetchBytesFromHost && !registerOnly) {
    throw new Error(`Unable to retrieve bytes for upload ${uploadUrl}`);
  }

  const isUploadAlreadyAtDestination =
    uploadFileName.toLowerCase() === fileName.toLowerCase();
  const shouldMoveUniqueStorageObject =
    UNIQUE_MEDIA_NAMES &&
    !isUploadAlreadyAtDestination;
  if (shouldMoveUniqueStorageObject && originFileNameBase !== fileNameBase) {
    prefixesToClear.add(originFileNameBase);
  }
  const shouldRenameUniqueStorageObject =
    shouldMoveUniqueStorageObject &&
    (
      shouldDeleteOrigin ||
      overwriteTarget?.url === uploadUrl
    );

  const assertUniqueDestination = (destinationUrl: string) => {
    if (!shouldMoveUniqueStorageObject) { return; }

    const { fileName: destinationFileName } =
      getFileNamePartsFromStorageUrl(destinationUrl);
    if (destinationFileName.toLowerCase() !== fileName.toLowerCase()) {
      throw new Error(
        `Unique media rename failed: expected ${fileName}, got ${destinationFileName}`,
      );
    }
  };

  const transferUploadToDestination = () =>
    shouldDeleteOrigin || shouldMoveUniqueStorageObject
      ? moveFile(uploadUrl, fileName)
      : copyFile(uploadUrl, fileName);

  if (registerOnly) {
    let url = uploadUrl;
    let transferFailed = false;
    if (!isUploadAlreadyAtDestination) {
      try {
        url = await withRetry(
          transferUploadToDestination,
          { label: `transfer media upload ${fileName}` },
        );
      } catch (error) {
        transferFailed = true;
        console.warn(
          `Storage transfer failed during register-only flow for ${fileName}: ` +
          `${(error as Error)?.message ?? error}`,
        );
      }
    }
    if (!transferFailed) {
      assertUniqueDestination(url);
    }
    // Large object moves can timeout even when storage eventually completes.
    // Only run sidecar/prefix cleanup after a confirmed transfer so we never
    // delete the original upload key on a transient transfer failure.
    if (!transferFailed) {
      await renameExistingSidecars();
      await cleanupStalePrefixes();
    }
    return {
      url,
      mediaType,
      transcodeStatus: 'pending',
      transcodeError: 'Processing queued',
    };
  }

  if (mediaType === 'photo') {
    let destinationUrl: string;
    if (shouldStripGpsData) {
      const fileWithoutGps = await removeGpsData(fileBytes as ArrayBuffer);
      destinationUrl = await withRetry(
        () => putFile(fileWithoutGps, fileName),
        { label: `store stripped photo ${fileName}` },
      );
      if (shouldDeleteOrigin || shouldRenameUniqueStorageObject) {
        await withRetry(
          () => deleteFile(uploadUrl),
          { label: `cleanup original upload ${fileName}`, attempts: 2 },
        ).catch(() => undefined);
      }
    } else {
      if (overwriteTarget?.url && overwriteTarget.url !== uploadUrl) {
        await deleteFile(overwriteTarget.url).catch(() => undefined);
      }
      destinationUrl = isUploadAlreadyAtDestination
        ? uploadUrl
        : await withRetry(
          transferUploadToDestination,
          { label: `transfer photo upload ${fileName}` },
        );
    }
    assertUniqueDestination(destinationUrl);
    await renameExistingSidecars();
    const url = await storeOptimizedMedia(destinationUrl, fileBytes as ArrayBuffer);
    await cleanupStalePrefixes();
    return {
      url,
      mediaType,
      transcodeStatus: 'ready',
    };
  }

  const url = isUploadAlreadyAtDestination
    ? uploadUrl
    : await withRetry(
      transferUploadToDestination,
      { label: `transfer media upload ${fileName}` },
    );
  assertUniqueDestination(url);
  await renameExistingSidecars();
  await cleanupStalePrefixes();

  if (shouldTransferOnlyVideo) {
    return {
      url,
      mediaType,
      posterUrl: undefined,
      previewUrl: undefined,
      transcodeStatus: 'failed',
      transcodeError:
        processingError ||
        'Video metadata generation failed; manual sync required',
    };
  }

  const videoTooling = await getVideoTooling();
  const canProcessVideo = videoTooling.canProcess && Boolean(videoTooling.ffmpeg);
  const shouldQueueForExternalProcessor =
    !canProcessVideo && await hasProcessingOrchestrator();
  const shouldTranscodeNow = !deferTranscode && canProcessVideo;

  let posterUrl: string | undefined;
  let previewUrl: string | undefined;
  let transcodeStatus: ConvertUploadToMediaResult['transcodeStatus'] =
    shouldTranscodeNow
      ? 'processing'
      : ((canProcessVideo || shouldQueueForExternalProcessor) ? 'pending' : 'failed');
  let transcodeError: string | undefined =
    shouldTranscodeNow
      ? undefined
      : (
        canProcessVideo
          ? 'Transcoding deferred'
          : (
            shouldQueueForExternalProcessor
              ? 'Queued for background processing'
              : 'Video processing unavailable'
          )
      );
  let metadata: VideoMetadata = {};

  if (canProcessVideo && videoTooling.ffmpeg) {
    try {
      metadata = await withTempDir(async (tempDir) => {
        const inputPath = path.join(tempDir, `source.${extension || 'mp4'}`);
        await fs.writeFile(inputPath, Buffer.from(fileBytes as ArrayBuffer));
        const probed = await probeVideo(inputPath, videoTooling.ffmpeg!);
        if (!shouldTranscodeNow) {
          return probed;
        }
        try {
          const { posterFilePath, previewFilePath } = await generateVideoDerivatives({
            tempDir,
            inputPath,
            fileNameBase,
            durationSeconds: probed.durationSeconds,
            ffmpegInstance: videoTooling.ffmpeg!,
          });
          const posterBuffer = await fs.readFile(posterFilePath);
          posterUrl = await withRetry(
            () => putFile(posterBuffer, `${fileNameBase}-poster.jpg`),
            { label: `store video poster ${fileNameBase}` },
          );
          if (previewFilePath) {
            try {
              const previewBuffer = await fs.readFile(previewFilePath);
              const ext = path.extname(previewFilePath).replace(/^\./, '') || 'mp4';
              previewUrl = await withRetry(
                () => putFile(previewBuffer, `${fileNameBase}-preview.${ext}`),
                { label: `store video preview ${fileNameBase}` },
              );
            } catch (err) {
              // Keep going if preview fails; poster is sufficient
              transcodeError = (err as any)?.message || transcodeError;
            }
          } else {
            transcodeError = transcodeError || 'Video preview generation failed';
          }
          if (GENERATE_STREAM_DERIVATIVES) {
            try {
              const streamMp4Path = path.join(tempDir, `${fileNameBase}-stream.mp4`);
              await new Promise<void>((resolve, reject) => {
                videoTooling.ffmpeg!(inputPath)
                  .outputOptions(['-movflags', 'faststart'])
                  .videoCodec('libx264')
                  .audioCodec('aac')
                  .outputOptions([
                    '-preset', 'veryfast',
                    '-crf', '23',
                    '-pix_fmt', 'yuv420p',
                  ])
                  .output(streamMp4Path)
                  .on('end', () => resolve())
                  .on('error', reject)
                  .run();
              });
              const streamMp4Buffer = await fs.readFile(streamMp4Path);
              await withRetry(
                () => putFile(streamMp4Buffer, `${fileNameBase}-stream.mp4`),
                { label: `store stream mp4 ${fileNameBase}` },
              );
            } catch {
              try {
                const streamWebmPath = path.join(tempDir, `${fileNameBase}-stream.webm`);
                await new Promise<void>((resolve, reject) => {
                  videoTooling.ffmpeg!(inputPath)
                    .videoCodec('libvpx-vp9')
                    .audioCodec('libopus')
                    .outputOptions(['-deadline', 'realtime', '-crf', '35'])
                    .output(streamWebmPath)
                    .on('end', () => resolve())
                    .on('error', reject)
                    .run();
                });
                const streamWebmBuffer = await fs.readFile(streamWebmPath);
                await withRetry(
                  () => putFile(streamWebmBuffer, `${fileNameBase}-stream.webm`),
                  { label: `store stream webm ${fileNameBase}` },
                );
              } catch {
                // ignore
              }
            }
          }
          // Attempt to extract first subtitle track as WebVTT sidecar
          try {
            // Probe for subtitle streams and languages
            await new Promise<void>((resolve, reject) => {
              videoTooling.ffmpeg!(inputPath).ffprobe(async (err, data) => {
                if (err) { reject(err); return; }
                const all = (data.streams || []);
                // Collect only subtitle streams and assign a 0..n-1 subtitle index
                const subs = all
                  .map((s: any, i: number) => ({ s, overallIdx: i }))
                  .filter(({ s }) => s?.codec_type === 'subtitle')
                  .map(({ s }, subIdx) => ({ s, subIdx }));
                for (const { subIdx, s } of subs) {
                  const lang: string = ((s as any)?.tags?.language || `track${subIdx}`).toString();
                  const out = path.join(tempDir, `${fileNameBase}-subtitles.${lang}.vtt`);
                  try {
                    await new Promise<void>((r, rej) => {
                      videoTooling.ffmpeg!(inputPath)
                        .outputOptions(['-map', `0:s:${subIdx}`, '-c:s', 'webvtt'])
                        .noVideo()
                        .noAudio()
                        .format('webvtt')
                        .output(out)
                        .on('end', () => r())
                        .on('error', rej)
                        .run();
                    });
                    const buf = await fs.readFile(out);
                    await withRetry(
                      () => putFile(buf, `${fileNameBase}-subtitles.${lang}.vtt`),
                      { label: `store subtitles ${fileNameBase} (${lang})` },
                    );
                  } catch {
                    // ignore individual subtitle extraction failures
                  }
                }
                resolve();
              });
            });
            await buildAndStoreSubtitlesManifest(fileNameBase);
          } catch {
            // No subtitles extracted; ignore
          }
          transcodeStatus = posterUrl && previewUrl ? 'ready' : 'failed';
        } catch (error: any) {
          // If poster or preview generation fails, mark as failed but try to continue
          transcodeStatus = 'failed';
          transcodeError = error?.message || 'Video derivative generation failed';
        }
        return probed;
      });
    } catch (error: any) {
      transcodeStatus = 'failed';
      transcodeError = error?.message || 'Video processing failed';
    }
  }

  const existingSidecars = await getVideoSidecarUrls(fileNameBase);
  posterUrl = posterUrl ?? existingSidecars.posterUrl;
  previewUrl = previewUrl ?? existingSidecars.previewUrl;
  if (transcodeStatus === 'failed' && posterUrl && previewUrl) {
    transcodeStatus = 'ready';
    transcodeError = undefined;
  }

  return {
    url,
    mediaType,
    posterUrl: transcodeStatus === 'failed' ? undefined : posterUrl,
    previewUrl: transcodeStatus === 'failed' ? undefined : previewUrl,
    transcodeStatus,
    transcodeError,
    ...metadata,
  };
};

export const convertUploadToMedia = async (
  args: ConvertUploadToMediaArgs,
): Promise<ConvertUploadToMediaResult> => {
  const key = conversionKeyForArgs(args);
  const inFlight = conversionQueueState.inFlight.get(key);
  if (inFlight) {
    return inFlight;
  }

  const promise = enqueueConversion(() => convertUploadToMediaInternal(args));
  const wrapped = promise.finally(() => {
    conversionQueueState.inFlight.delete(key);
  });
  conversionQueueState.inFlight.set(key, wrapped);
  return wrapped;
};

export const convertUploadToImage = async (
  args: Parameters<typeof convertUploadToMedia>[0],
) =>
  convertUploadToMedia({ ...args, mediaType: 'photo' })
    .then(result => result.url);
