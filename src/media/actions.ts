'use server';

import {
  insertMedia,
  deleteMediaCategoryGlobally,
  deleteMediaContentTypeGlobally,
  deleteMediaPerformerGlobally,
  deleteMediaTagGlobally,
  deleteMediaStudioGlobally,
  updateMedia,
  renameMediaTagGlobally,
  getMedia,
  addTagsToMedia,
  getUniqueTags,
  deleteMediaRecipeGlobally,
  renameMediaRecipeGlobally,
  getMediaNeedingRecipeTitleCount,
  updateColorDataForMedia,
  getColorDataForMedia,
  getMediaByStorageUrl,
  getMediaByFileNameBase,
  consolidateDuplicateMediaRecords,
  cleanupExactDuplicateMediaRecords,
  getUniqueCategories,
  getUniquePerformers,
  getUniqueStudios,
  getUniqueVideoContentTypes,
} from '@/media/query';
import { MediaQueryOptions, areOptionsSensitive } from '@/db';
import {
  MediaFormData,
} from './form';
import { redirect } from 'next/navigation';
import {
  getMediaCached,
  revalidateAdminPaths,
  revalidateAllKeysAndPaths,
  revalidateCategoriesKey,
  revalidateContentTypesKey,
  revalidateMedia,
  revalidateMediaKey,
  revalidatePerformersKey,
  revalidateRecipesKey,
  revalidateStudiosKey,
  revalidateTagsKey,
} from '@/media/cache';
import {
  PATH_ADMIN_CONTENT_TYPES,
  PATH_ADMIN_MEDIA,
  PATH_ADMIN_PERFORMERS,
  PATH_ADMIN_RECIPES,
  PATH_ADMIN_STUDIOS,
  PATH_ADMIN_TAGS,
  PATH_ROOT,
  pathForMedia,
  pathForTag,
} from '@/app/path';
import {
  blurImageFromUrl,
  convertFormDataToMediaDbInsertAndLookupRecipeTitle,
  extractImageDataFromBlobPath,
  propagateRecipeTitleIfNecessary,
} from './server';
import { TAG_FAVS, Tags, isMediaFav, isTagFavs, isTagPrivate } from '@/tag';
import {
  convertMediaToMediaDbInsert,
  MediaType,
  Media,
  MediaDbInsert,
  normalizeTitle,
} from '.';
import { runAuthenticatedAdminServerAction } from '@/auth/server';
import { AiImageQuery, getAiImageQuery, getAiTextFieldsToGenerate } from './ai';
import { streamOpenAiImageQuery } from '@/platforms/openai';
import {
  AI_TEXT_AUTO_GENERATED_FIELDS,
  AI_CONTENT_GENERATION_ENABLED,
  BLUR_ENABLED,
  UNIQUE_MEDIA_NAMES,
} from '@/app/config';
import { generateAiImageQueries } from './ai/server';
import { createStreamableValue } from '@ai-sdk/rsc';
import {
  convertUploadToMedia,
  type ConvertUploadToMediaResult,
} from './storage/server';
import { UrlAddStatus } from '@/admin/upload';
import { convertStringToArray } from '@/utility/string';
import { generateMediaNanoid } from '@/utility/nanoid';
import {
  generateLocalNaivePostgresString,
  generateLocalPostgresString,
} from '@/utility/date';
import { after } from 'next/server';
import { createHash } from 'crypto';
import {
  getColorFieldsForImageUrl,
  getColorFieldsForMediaDbInsert,
} from '@/media/color/server';
import { shouldBackfillMediaStorage } from './update/server';
import { getAlbumTitlesFromFormData } from '@/album/form';
import {
  addAlbumTitlesToMedia,
  createAlbumsAndGetIds,
  upgradeTagToAlbum,
} from '@/album/server';
import { addMediaAlbumIds, getAlbumTitlesForMedia } from '@/album/query';
import {
  copyFile,
  deleteFile,
  getCurrentStorageUrlsForPrefix,
  sanitizeStorageFileNameBase,
  getFileNamePartsFromStorageUrl,
  moveFile,
  putFile,
} from '@/platforms/storage';
import {
  addSubtitleLanguage,
  deleteSubtitleLanguage,
  getSubtitleLanguages,
} from '@/subtitle/query';
import { storeUploadedSubtitles } from './storage/server';
import { isVirtualStorageVideoId } from './storage/virtual';
import { getStorageUrlsForMedia } from './storage';
import { withPostgresAdvisoryLock } from '@/platforms/postgres';
import {
  hasProcessingOrchestrator,
  triggerDeletionOrchestrator,
  triggerProcessingOrchestrator,
} from '@/processing/orchestrator';
import {
  clearWorkerRegistrationStatusForUrl,
} from '@/admin/processing/server';
import { enqueueMediaDeletion } from './deletion';
import {
  driveCreatePresignedDownload,
  driveKeyFromUrl,
  isUrlFromDrive,
} from '@/platforms/storage/drive-gateway';

const VIDEO_FILE_EXTENSIONS = [
  'mp4', 'mkv', 'mov', 'm4v', 'webm', 'avi', 'ts', 'm2ts', 'mts',
  'mpg', 'mpeg', 'wmv', 'flv', '3gp', 'ogv',
] as const;
const VIDEO_EXTENSION_SET = new Set<string>(VIDEO_FILE_EXTENSIONS);
const GENERATED_MEDIA_ID_PATTERN = /^\d{12}$/;
const GENERATED_MEDIA_TITLE_PATTERN = /^\d{12}(?:[-_].*)?$/;
const uploadProcessingPromises = new Map<string, Promise<void>>();
let uploadProcessingQueue: Promise<void> = Promise.resolve();

const generateUniqueMediaId = async () => {
  for (let attempt = 0; attempt < 30; attempt += 1) {
    const id = generateMediaNanoid();
    if (!await getMedia(id, true)) { return id; }
  }
  throw new Error('Unable to generate a unique media id.');
};

const generateStableUploadMediaId = (url: string) =>
  (BigInt(`0x${createHash('sha256')
    .update(decodeURIComponent(url).split('?')[0])
    .digest('hex')
    .slice(0, 16)}`) % BigInt('1000000000000'))
    .toString()
    .padStart(12, '0');

const getUploadProcessingIdentity = (
  url: string,
  _originalFileName?: string,
) => {
  const decodedUrl = decodeURIComponent(url ?? '').split('?')[0];
  const { fileNameBase } = getFileNamePartsFromStorageUrl(decodedUrl);
  if (GENERATED_MEDIA_ID_PATTERN.test(fileNameBase)) {
    return fileNameBase;
  }
  return decodedUrl;
};

const getAdvisoryLockParts = (key: string) => {
  const hash = createHash('sha256').update(key).digest();
  return [
    hash.readInt32BE(0),
    hash.readInt32BE(4),
  ] as const;
};

const withUploadProcessingLock = <T>(
  url: string,
  originalFileName: string | undefined,
  callback: () => Promise<T>,
) => {
  const lockKey = getUploadProcessingIdentity(url, originalFileName);
  const [lockA, lockB] = getAdvisoryLockParts(lockKey);
  return withPostgresAdvisoryLock(lockA, lockB, callback);
};

const deriveTitleFromOriginalFileName = (fileName?: string | null) =>
  fileName
    ?.replace(/\.[^/.]+$/, '')
    .replace(/[-_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const isGeneratedMediaTitle = (title?: string | null) =>
  Boolean(title && GENERATED_MEDIA_TITLE_PATTERN.test(title.trim()));

const normalizeUploadTitle = (title?: string | null) => {
  const normalized = normalizeTitle(title);
  return isGeneratedMediaTitle(normalized) ? undefined : normalized;
};

const titleForMediaProcessing = (
  existingTitle?: string | null,
  ...candidateTitles: (string | null | undefined)[]
) =>
  normalizeUploadTitle(existingTitle) ??
  candidateTitles.map(normalizeUploadTitle).find(Boolean);

const collectMediaMatches = async ({
  ids = [],
  urls = [],
  fileNameBases: _fileNameBases = [],
}: {
  ids?: (string | undefined)[]
  urls?: (string | undefined)[]
  fileNameBases?: (string | undefined)[]
}) => {
  // A filename is presentation metadata, not object identity. Different
  // uploads may legitimately use the same original filename or title. Keep
  // accepting this argument while older call sites are migrated, but only
  // match records by their exact media ID or exact storage URL.
  void _fileNameBases;
  const matchingMediaMap = new Map<string, Media>();
  await Promise.all([
    ...Array.from(new Set(ids.filter((id): id is string => Boolean(id))))
      .map(async id => {
        const media = await getMedia(id, true);
        if (media) { matchingMediaMap.set(media.id, media); }
      }),
    ...Array.from(new Set(urls.filter((url): url is string => Boolean(url))))
      .map(async url => {
        const mediaItems = await getMediaByStorageUrl(url);
        mediaItems.forEach(media => matchingMediaMap.set(media.id, media));
      }),
  ]);
  return Array.from(matchingMediaMap.values());
};

export const findProcessedUploadMedia = async (
  url: string,
  originalFileName?: string,
) => {
  const { fileNameBase } = getFileNamePartsFromStorageUrl(url);
  const generatedFileNameMediaId = GENERATED_MEDIA_ID_PATTERN.test(fileNameBase)
    ? fileNameBase
    : undefined;
  const stableUploadMediaId = UNIQUE_MEDIA_NAMES
    ? generateStableUploadMediaId(url)
    : undefined;
  const originalBase = originalFileName
    ? sanitizeStorageFileNameBase(originalFileName.replace(/\.[^/.]+$/, ''))
    : undefined;
  const matches = await collectMediaMatches({
    ids: [generatedFileNameMediaId, stableUploadMediaId],
    urls: [url],
    fileNameBases: [
      generatedFileNameMediaId,
      stableUploadMediaId,
      originalBase,
    ],
  });
  const { canonical } = splitCanonicalMedia(matches, [
    generatedFileNameMediaId,
    stableUploadMediaId,
  ]);
  return canonical;
};

const splitCanonicalMedia = (
  mediaItems: Media[],
  preferredIds: (string | undefined)[] = [],
) => {
  const preferredIdSet = new Set(
    preferredIds.filter((id): id is string => Boolean(id)),
  );
  const canonical =
    mediaItems.find(({ id }) => preferredIdSet.has(id)) ??
    mediaItems[0];
  return {
    canonical,
    duplicates: mediaItems.filter(({ id }) => id !== canonical?.id),
  };
};

const deleteDuplicateMediaRecords = async (
  canonicalId: string,
  duplicates: Media[],
) => {
  if (duplicates.length === 0) { return; }
  await consolidateDuplicateMediaRecords(
    canonicalId,
    duplicates.map(({ id }) => id),
  );
};

const saveMediaRecord = async (
  photo: MediaDbInsert,
  preferredIds: (string | undefined)[] = [],
  extraMatches?: {
    urls?: (string | undefined)[]
    fileNameBases?: (string | undefined)[]
    ids?: (string | undefined)[]
  },
) => {
  const urlOwner = (await getMediaByStorageUrl(photo.url))[0];
  const fileNameBase = getFileNamePartsFromStorageUrl(photo.url).fileNameBase;
  const initialMatches = await collectMediaMatches({
    ids: [photo.id, urlOwner?.id, ...(extraMatches?.ids ?? [])],
    urls: [photo.url, ...(extraMatches?.urls ?? [])],
    fileNameBases: [fileNameBase, ...(extraMatches?.fileNameBases ?? [])],
  });
  const {
    canonical: existingMedia,
    duplicates: duplicateMedia,
  } = splitCanonicalMedia(initialMatches, [
    urlOwner?.id,
    ...preferredIds,
    photo.id,
  ]);

  if (existingMedia) {
    photo.id = existingMedia.id;
    await deleteDuplicateMediaRecords(photo.id, duplicateMedia);
    await updateMedia(photo);
    return photo.id;
  }

  const insertedId = await insertMedia(photo);
  if (insertedId) {
    photo.id = insertedId;
  }

  const postSaveMatches = await collectMediaMatches({
    ids: [photo.id, ...(extraMatches?.ids ?? [])],
    urls: [photo.url, ...(extraMatches?.urls ?? [])],
    fileNameBases: [fileNameBase, ...(extraMatches?.fileNameBases ?? [])],
  });
  const { duplicates } = splitCanonicalMedia(postSaveMatches, [
    photo.id,
    ...preferredIds,
  ]);
  await deleteDuplicateMediaRecords(photo.id, duplicates);
  return photo.id;
};

const runOncePerUpload = async (
  key: string,
  task: () => Promise<void>,
) => {
  const existing = uploadProcessingPromises.get(key);
  if (existing) {
    await existing;
    return 'joined' as const;
  }

  const promise = task().finally(() => {
    uploadProcessingPromises.delete(key);
  });
  uploadProcessingPromises.set(key, promise);
  await promise;
  return 'processed' as const;
};

const enqueueUploadProcessing = async (task: () => Promise<void>) => {
  const queued = uploadProcessingQueue
    .catch(() => undefined)
    .then(task);
  uploadProcessingQueue = queued.then(
    () => undefined,
    () => undefined,
  );
  await queued;
};

const getPreferredFileNameBaseForExistingMedia = (
  photo: Pick<MediaDbInsert, 'id' | 'url'>,
  originalFileName?: string,
) => {
  const existingFileNameBase =
    getFileNamePartsFromStorageUrl(photo.url).fileNameBase;
  if (GENERATED_MEDIA_ID_PATTERN.test(existingFileNameBase)) {
    return existingFileNameBase;
  }

  if (UNIQUE_MEDIA_NAMES) {
    return photo.id;
  }

  return sanitizeStorageFileNameBase(
    (originalFileName?.replace(/\.[^/.]+$/, '') ||
      existingFileNameBase ||
      ''),
  );
};

const mediaTypeFromExtension = (
  extension?: string | null,
  fallback: MediaType = 'photo',
): MediaType =>
  extension && VIDEO_EXTENSION_SET.has(extension.toLowerCase())
    ? 'video'
    : fallback;

const applyConvertedMediaToMedia = (
  photo: MediaDbInsert,
  converted: ConvertUploadToMediaResult,
) => {
  photo.url = converted.url;
  photo.mediaType = converted.mediaType;
  photo.posterUrl = converted.mediaType === 'video'
    ? converted.posterUrl ?? photo.posterUrl
    : converted.posterUrl;
  photo.previewUrl = converted.mediaType === 'video'
    ? converted.previewUrl ?? photo.previewUrl
    : converted.previewUrl;
  photo.durationSeconds = converted.durationSeconds;
  photo.frameRate = converted.frameRate;
  photo.mediaWidth = converted.mediaWidth;
  photo.mediaHeight = converted.mediaHeight;
  photo.transcodeStatus = converted.transcodeStatus;
  photo.transcodeError = converted.transcodeError;
  if (converted.mediaType === 'video' && converted.transcodeStatus === 'failed') {
    photo.posterUrl = undefined;
    photo.previewUrl = undefined;
  }
  if (converted.mediaType === 'photo') {
    photo.transcodeStatus = undefined;
    photo.transcodeError = undefined;
    photo.posterUrl = undefined;
    photo.previewUrl = undefined;
    photo.durationSeconds = undefined;
    photo.frameRate = undefined;
    photo.mediaWidth = undefined;
    photo.mediaHeight = undefined;
  }
  if (
    converted.mediaType === 'video' &&
    converted.mediaWidth &&
    converted.mediaHeight
  ) {
    photo.aspectRatio = converted.mediaWidth / converted.mediaHeight;
    photo.blurData = undefined;
  } else if (converted.mediaType === 'video') {
    photo.aspectRatio = 16 / 9;
    photo.blurData = undefined;
  }
};

// Private actions

export const createMediaAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const shouldStripGpsData = formData.get('shouldStripGpsData') === 'true';
    const originalFileNameFromForm =
      formData.get('uploadOriginalFileName')?.toString();
    const originalTitleFromFileName =
      deriveTitleFromOriginalFileName(originalFileNameFromForm);

    const photo =
      await convertFormDataToMediaDbInsertAndLookupRecipeTitle(formData);
    if (!formData.get('id')) {
      photo.id = await generateUniqueMediaId();
    }

    const generatedTitleFromUploadFileName =
      deriveTitleFromOriginalFileName(
        getFileNamePartsFromStorageUrl(photo.url).fileName,
      );
    const photoTitleIsGenerated = Boolean(
      photo.title &&
      (
        GENERATED_MEDIA_TITLE_PATTERN.test(photo.title) ||
        photo.title === generatedTitleFromUploadFileName
      ),
    );

    if ((!photo.title || photoTitleIsGenerated) && originalTitleFromFileName) {
      photo.title = originalTitleFromFileName;
    }
    const normalizedTitle = normalizeTitle(photo.title);
    photo.title = normalizedTitle ?? photo.title ?? undefined;

    const albumTitles = getAlbumTitlesFromFormData(formData);

    const extensionLower = photo.extension?.toLowerCase();
    const mediaTypeForUpload = extensionLower &&
      VIDEO_EXTENSION_SET.has(extensionLower)
      ? 'video'
      : photo.mediaType;
    const convertedMedia = await convertUploadToMedia({
      uploadUrl: photo.url,
      shouldStripGpsData,
      originalFileName: originalFileNameFromForm,
      mediaType: mediaTypeForUpload,
      preferredFileNameBase: UNIQUE_MEDIA_NAMES
        ? photo.id
        : undefined,
    });
    const convertedFileNameBase =
      getFileNamePartsFromStorageUrl(convertedMedia.url).fileNameBase;
    const originalFileNameBase = originalFileNameFromForm
      ? sanitizeStorageFileNameBase(
        originalFileNameFromForm.replace(/\.[^/.]+$/, ''),
      )
      : undefined;
    if (UNIQUE_MEDIA_NAMES && convertedFileNameBase !== photo.id) {
      throw new Error(
        `Unique media creation failed: expected ${photo.id}, ` +
        `got ${convertedFileNameBase}`,
      );
    }
    if (
      !UNIQUE_MEDIA_NAMES &&
      originalFileNameBase &&
      convertedFileNameBase !== originalFileNameBase
    ) {
      throw new Error(
        `Media creation failed: expected ${originalFileNameBase}, ` +
        `got ${convertedFileNameBase}`,
      );
    }
    
    if (convertedMedia?.url) {
      applyConvertedMediaToMedia(photo, convertedMedia);
      await saveMediaRecord(photo, [photo.id]);
      await addAlbumTitlesToMedia(albumTitles, photo.id, false);
      await propagateRecipeTitleIfNecessary(formData, photo);
      revalidateAllKeysAndPaths();
      redirect(PATH_ADMIN_MEDIA);
    }
  });

export const checkUploadConflictAction = async ({
  originalFileName,
  mediaType,
  extension,
}: {
  originalFileName?: string
  mediaType?: MediaType
  extension?: string
}) =>
  runAuthenticatedAdminServerAction(async () => {
    if (UNIQUE_MEDIA_NAMES) {
      return {
        exists: false,
      };
    }
    if (!originalFileName) {
      return {
        exists: false,
      };
    }

    const baseFromOriginal = sanitizeStorageFileNameBase(
      originalFileName.replace(/\.[^/.]+$/, ''),
    );
    if (!baseFromOriginal) {
      return {
        exists: false,
      };
    }

    const extensionLower = (
      extension ||
      originalFileName.split('.').pop() ||
      (mediaType === 'video' ? 'mp4' : 'jpg')
    ).toLowerCase();
    const candidateFileName = `${baseFromOriginal}.${extensionLower}`;

    const finalUrls =
      await getCurrentStorageUrlsForPrefix(baseFromOriginal).catch(() => []);

    const findExistingMatch = async () => {
      const exactMatch = finalUrls.find(item =>
        item.fileName.toLowerCase() === candidateFileName,
      );
      if (exactMatch) { return exactMatch; }

      for (const item of finalUrls) {
        const photos = await getMediaByStorageUrl(item.url);
        if (photos.length > 0) {
          return item;
        }
      }
      return undefined;
    };

    const existingItem = await findExistingMatch();

    let existingMediaId: string | undefined;
    let existingPosterUrl: string | undefined;
    let existingPreviewUrl: string | undefined;
    let existingUrl: string | undefined;
    if (existingItem?.url) {
      const existingMediaMatches = await getMediaByStorageUrl(existingItem.url);
      if (existingMediaMatches.length > 0) {
        const [existingMedia] = existingMediaMatches;
        existingMediaId = existingMedia.id;
        existingPosterUrl = existingMedia.posterUrl;
        existingPreviewUrl = existingMedia.previewUrl;
        existingUrl = existingMedia.url;
      } else {
        existingUrl = existingItem.url;
      }
    }

    if (!existingMediaId) {
      const photoByBase = await getMediaByFileNameBase(baseFromOriginal);
      if (photoByBase) {
        existingMediaId = photoByBase.id;
        existingPosterUrl = photoByBase.posterUrl;
        existingPreviewUrl = photoByBase.previewUrl;
        existingUrl = photoByBase.url;
      }
    }

    const exists = Boolean(existingItem) || Boolean(existingMediaId);

    return {
      exists,
      existingUrl,
      existingFileName: existingItem?.fileName,
      existingMediaId,
      existingPosterUrl,
      existingPreviewUrl,
    };
  });

const addUpload = async ({
  url,
  title: _title,
  originalFileName,
  albumIds = [],
  tags: _tags,
  favorite,
  hidden,
  excludeFromFeeds,
  takenAtLocal,
  takenAtNaiveLocal,
  uniqueTags: _uniqueTags,
  preferredFileNameBase,
  overwriteMediaId,
  overwriteTargetUrls,
  onStreamUpdate,
  onFinish,
  shouldRevalidateAllKeysAndPaths,
}: {
  url: string
  title?: string
  originalFileName?: string
  albumIds?: string[]
  tags?: string
  favorite?: string
  hidden?: string
  excludeFromFeeds?: string
  takenAtLocal: string
  takenAtNaiveLocal: string
  uniqueTags?: Tags
  preferredFileNameBase?: string
  overwriteMediaId?: string
  overwriteTargetUrls?: {
    url: string
    posterUrl?: string
    previewUrl?: string
  }
  onStreamUpdate?: (
    statusMessage: string,
    status?: UrlAddStatus['status'],
  ) => void
  onFinish?: (url: string) => void
  shouldRevalidateAllKeysAndPaths?: boolean
}) => {
  const uploadProcessingKey = getUploadProcessingIdentity(url, originalFileName);

  const processingResult = await runOncePerUpload(uploadProcessingKey, () =>
    enqueueUploadProcessing(() =>
      withUploadProcessingLock(url, originalFileName, async () => {
        await cleanupExactDuplicateMediaRecords();
        if (!overwriteMediaId && !overwriteTargetUrls?.url) {
          const existingProcessedMedia =
            await findProcessedUploadMedia(url, originalFileName);
          if (existingProcessedMedia) {
            onStreamUpdate?.('Already added', 'added');
            onFinish?.(url);
            return;
          }
        }

        onStreamUpdate?.('Transferring to media storage');

        const {
          fileExtension,
          fileName,
          fileNameBase: uploadFileNameBase,
        } = getFileNamePartsFromStorageUrl(url);
        const extensionFromOriginal = originalFileName
          ?.split('.')
          .pop()
          ?.toLowerCase();
        const extensionLower =
          (fileExtension || extensionFromOriginal || '').toLowerCase();
        const mediaType = mediaTypeFromExtension(extensionLower);
        const lockedGeneratedFileNameBase =
          GENERATED_MEDIA_ID_PATTERN.test(uploadFileNameBase)
            ? uploadFileNameBase
            : undefined;
        const derivedBaseFromOriginal = originalFileName
          ? sanitizeStorageFileNameBase(
            originalFileName.replace(/\.[^/.]+$/, ''),
          )
          : undefined;
        const overwriteExistingMedia =
          (overwriteMediaId
            ? await getMedia(overwriteMediaId, true)
            : lockedGeneratedFileNameBase
              ? await getMedia(lockedGeneratedFileNameBase, true)
              : undefined) ??
          (lockedGeneratedFileNameBase
            ? await getMediaByFileNameBase(lockedGeneratedFileNameBase)
            : undefined);
        const overwriteTargetFromArgs =
          overwriteTargetUrls?.url
            ? getFileNamePartsFromStorageUrl(overwriteTargetUrls.url)
            : undefined;
        const resolvedPreferredFileNameBase = UNIQUE_MEDIA_NAMES
          ? overwriteExistingMedia?.id ?? preferredFileNameBase
          : overwriteExistingMedia
            ? (
              preferredFileNameBase ??
              derivedBaseFromOriginal ??
              getFileNamePartsFromStorageUrl(overwriteExistingMedia.url)
                .fileNameBase
            )
            : preferredFileNameBase ??
              derivedBaseFromOriginal ??
              overwriteTargetFromArgs?.fileNameBase;
        const stableUploadMediaId = UNIQUE_MEDIA_NAMES
          ? generateStableUploadMediaId(url)
          : undefined;
        const mediaId = overwriteExistingMedia?.id ??
          lockedGeneratedFileNameBase ??
          stableUploadMediaId ??
          await generateUniqueMediaId();
        const conversionPreferredFileNameBase = UNIQUE_MEDIA_NAMES
          ? (
            overwriteExistingMedia?.id ??
            lockedGeneratedFileNameBase ??
            mediaId
          )
          : (overwriteExistingMedia || overwriteTargetUrls)
            ? (
              resolvedPreferredFileNameBase ??
              derivedBaseFromOriginal ??
              overwriteTargetFromArgs?.fileNameBase
            )
            : undefined;

        const convertedMedia = await convertUploadToMedia({
          uploadUrl: url,
          shouldDeleteOrigin: true,
          originalFileName,
          mediaType,
          overwriteTarget: overwriteExistingMedia
            ? {
              url: overwriteExistingMedia.url,
              posterUrl: overwriteExistingMedia.posterUrl,
              previewUrl: overwriteExistingMedia.previewUrl,
            }
            : overwriteTargetUrls,
          preferredFileNameBase: conversionPreferredFileNameBase,
          registerOnly: true,
        });

        const convertedFileNameBase =
          getFileNamePartsFromStorageUrl(convertedMedia.url).fileNameBase;
        if (
          UNIQUE_MEDIA_NAMES &&
          convertedFileNameBase !== conversionPreferredFileNameBase
        ) {
          throw new Error(
            `Unique media registration failed: expected ` +
            `${conversionPreferredFileNameBase}, got ${convertedFileNameBase}`,
          );
        }
        if (
          !UNIQUE_MEDIA_NAMES &&
          derivedBaseFromOriginal &&
          convertedFileNameBase !== derivedBaseFromOriginal
        ) {
          throw new Error(
            `Media registration failed: expected ${derivedBaseFromOriginal}, ` +
            `got ${convertedFileNameBase}`,
          );
        }
        const matchingMedia = await collectMediaMatches({
          ids: [
            overwriteExistingMedia?.id,
            mediaId,
            stableUploadMediaId,
            lockedGeneratedFileNameBase,
          ],
          urls: [
            url,
            convertedMedia.url,
            overwriteTargetUrls?.url,
          ],
          fileNameBases: [
            convertedFileNameBase,
            uploadFileNameBase,
            derivedBaseFromOriginal,
            resolvedPreferredFileNameBase,
            overwriteTargetFromArgs?.fileNameBase,
          ],
        });
        const existingMediaWithConvertedUrl = matchingMedia.find(media =>
          media.url === convertedMedia.url);
        const {
          canonical: existingMedia,
          duplicates: duplicateMedia,
        } = splitCanonicalMedia(matchingMedia, [
          existingMediaWithConvertedUrl?.id,
          mediaId,
          stableUploadMediaId,
          lockedGeneratedFileNameBase,
          overwriteExistingMedia?.id,
        ]);
        const originalTitleFromFileName =
          deriveTitleFromOriginalFileName(originalFileName);
        const submittedTitle = _title?.trim();
        const generatedTitleFromUploadFileName =
          deriveTitleFromOriginalFileName(fileName);
        const submittedTitleIsGenerated = Boolean(
          submittedTitle &&
          (
            isGeneratedMediaTitle(submittedTitle) ||
            submittedTitle === generatedTitleFromUploadFileName
          ),
        );
        const tagList = convertStringToArray(_tags, false) ?? [];
        const tags = favorite === 'true'
          ? Array.from(new Set([...tagList, TAG_FAVS]))
          : tagList;
        const title = titleForMediaProcessing(
          existingMedia?.title,
          originalTitleFromFileName,
          submittedTitleIsGenerated ? undefined : submittedTitle,
          deriveTitleFromOriginalFileName(fileName),
        );
        const photo: MediaDbInsert = {
          id: existingMedia?.id ?? mediaId,
          url: convertedMedia.url,
          extension: extensionLower ||
            getFileNamePartsFromStorageUrl(convertedMedia.url).fileExtension,
          mediaType: convertedMedia.mediaType,
          title,
          tags: tags.length > 0
            ? tags
            : existingMedia?.tags ?? overwriteExistingMedia?.tags,
          posterUrl: undefined,
          previewUrl: undefined,
          transcodeStatus: 'pending',
          transcodeError: 'Processing queued',
          aspectRatio: existingMedia?.aspectRatio ??
            overwriteExistingMedia?.aspectRatio ??
            (convertedMedia.mediaType === 'video' ? 16 / 9 : 1.5),
          hidden: hidden === 'true' ||
            existingMedia?.hidden ||
            overwriteExistingMedia?.hidden,
          excludeFromFeeds: excludeFromFeeds === 'true' ||
            existingMedia?.excludeFromFeeds ||
            overwriteExistingMedia?.excludeFromFeeds,
          takenAt: existingMedia?.takenAt?.toISOString?.() ?? takenAtLocal,
          takenAtNaive: existingMedia?.takenAtNaive ?? takenAtNaiveLocal,
        };

        if (existingMedia) {
          photo.priorityOrder = existingMedia.priorityOrder ?? undefined;
          await deleteDuplicateMediaRecords(photo.id, duplicateMedia);
          await saveMediaRecord(photo, [
            existingMedia.id,
            mediaId,
            stableUploadMediaId,
          ], {
            urls: [url, convertedMedia.url, overwriteTargetUrls?.url],
            fileNameBases: [
              convertedFileNameBase,
              uploadFileNameBase,
              derivedBaseFromOriginal,
              resolvedPreferredFileNameBase,
              overwriteTargetFromArgs?.fileNameBase,
            ],
          });
        } else {
          await saveMediaRecord(photo, [
            mediaId,
            stableUploadMediaId,
            lockedGeneratedFileNameBase,
          ], {
            ids: [overwriteExistingMedia?.id],
            urls: [url, convertedMedia.url, overwriteTargetUrls?.url],
            fileNameBases: [
              convertedFileNameBase,
              uploadFileNameBase,
              derivedBaseFromOriginal,
              resolvedPreferredFileNameBase,
              overwriteTargetFromArgs?.fileNameBase,
            ],
          });
        }
        if (albumIds.length > 0) {
          await addMediaAlbumIds([photo.id], albumIds);
        }
        if (photo.mediaType === 'video' && await hasProcessingOrchestrator()) {
          after(() => triggerProcessingOrchestrator().catch(error => {
            console.error('Failed to trigger Backend Orchestrator', error);
            return false;
          }));
        }
        await cleanupExactDuplicateMediaRecords();
        if (shouldRevalidateAllKeysAndPaths) {
          after(revalidateAllKeysAndPaths);
        }
        onFinish?.(url);
        onStreamUpdate?.('Queued', 'added');
      })));

  if (processingResult === 'joined') {
    const existingProcessedMedia =
      await findProcessedUploadMedia(url, originalFileName);
    if (existingProcessedMedia) {
      onStreamUpdate?.('Already added', 'added');
      onFinish?.(url);
      return;
    }

    throw new Error('Upload processing is still pending');
  }

  return;
};

export const addUploadAction = async (args: Parameters<typeof addUpload>[0]) =>
  runAuthenticatedAdminServerAction(() =>
    addUpload({
      ...args,
      shouldRevalidateAllKeysAndPaths:
        args.shouldRevalidateAllKeysAndPaths ?? true,
    }),
  );

export const registerUploadForAutomation = async (
  args: Parameters<typeof addUpload>[0],
) =>
  await addUpload({
    ...args,
    shouldRevalidateAllKeysAndPaths:
      args.shouldRevalidateAllKeysAndPaths ?? true,
  });

const addFailedVideoUploadFromStorage = async ({
  url,
  title,
  originalFileName,
  albumIds,
  hidden,
  favorite,
  excludeFromFeeds,
  takenAtLocal,
  takenAtNaiveLocal,
  preferredFileNameBase,
  overwriteMediaId,
  overwriteTargetUrls,
  errorMessage,
}: {
  url: string
  title?: string
  originalFileName?: string
  albumIds?: string[]
  hidden?: string
  favorite?: string
  excludeFromFeeds?: string
  takenAtLocal: string
  takenAtNaiveLocal: string
  preferredFileNameBase?: string
  overwriteMediaId?: string
  overwriteTargetUrls?: {
    url: string
    posterUrl?: string
    previewUrl?: string
  }
  errorMessage?: string
}) => {
  const {
    fileExtension,
    fileName,
    fileNameBase,
  } = getFileNamePartsFromStorageUrl(url);
  const extension = fileExtension?.toLowerCase() ?? '';
  if (!VIDEO_EXTENSION_SET.has(extension)) {
    throw new Error(errorMessage || 'Processing failed');
  }

  const resolvedOriginalFileName = originalFileName;
  const originalTitleFromFileName =
    deriveTitleFromOriginalFileName(resolvedOriginalFileName);
  const submittedTitle = title?.trim();
  const generatedTitleFromUploadFileName =
    deriveTitleFromOriginalFileName(getFileNamePartsFromStorageUrl(url).fileName);
  const submittedTitleIsGenerated = Boolean(
    submittedTitle &&
    (
      isGeneratedMediaTitle(submittedTitle) ||
      submittedTitle === generatedTitleFromUploadFileName
    ),
  );
  const originalBase = resolvedOriginalFileName
    ? sanitizeStorageFileNameBase(
      resolvedOriginalFileName.replace(/\.[^/.]+$/, ''),
    )
    : undefined;
  const preferredBase = preferredFileNameBase
    ? sanitizeStorageFileNameBase(preferredFileNameBase)
    : undefined;
  const overwriteTargetFromArgs = overwriteTargetUrls?.url
    ? getFileNamePartsFromStorageUrl(overwriteTargetUrls.url)
    : undefined;
  const stableUploadMediaId = UNIQUE_MEDIA_NAMES
    ? generateStableUploadMediaId(url)
    : undefined;
  const generatedFileNameMediaId = GENERATED_MEDIA_ID_PATTERN.test(fileNameBase)
    ? fileNameBase
    : undefined;
  const initialMatches = await collectMediaMatches({
    ids: [overwriteMediaId, generatedFileNameMediaId, stableUploadMediaId],
    urls: [url, overwriteTargetUrls?.url],
    fileNameBases: [
      fileNameBase,
      originalBase,
      preferredBase,
      overwriteTargetFromArgs?.fileNameBase,
    ],
  });
  const { canonical: overwriteExistingMedia } = splitCanonicalMedia(
    initialMatches,
    [overwriteMediaId, generatedFileNameMediaId, stableUploadMediaId],
  );
  const mediaId = overwriteExistingMedia?.id ??
    overwriteMediaId ??
    generatedFileNameMediaId ??
    stableUploadMediaId ??
    await generateUniqueMediaId();
  const overwriteTargetFileName = overwriteExistingMedia?.url
    ? getFileNamePartsFromStorageUrl(overwriteExistingMedia.url).fileName
    : overwriteTargetFromArgs?.fileName;
  const destinationFileName = overwriteTargetFileName ??
    `${preferredBase || mediaId}.${extension}`;
  let finalUrl = url;
  let finalTransferError: string | undefined;
  if (fileName.toLowerCase() !== destinationFileName.toLowerCase()) {
    try {
      finalUrl = await moveFile(url, destinationFileName);
    } catch (error: any) {
      finalTransferError = error?.message || 'Media storage transfer failed';
      finalUrl = url;
    }
  }
  let finalFileNameBase = getFileNamePartsFromStorageUrl(finalUrl).fileNameBase;
  const saveMatches = await collectMediaMatches({
    ids: [
      overwriteExistingMedia?.id,
      mediaId,
      overwriteMediaId,
      generatedFileNameMediaId,
      stableUploadMediaId,
    ],
    urls: [url, finalUrl, overwriteTargetUrls?.url],
    fileNameBases: [
      fileNameBase,
      finalFileNameBase,
      originalBase,
      preferredBase,
      overwriteTargetFromArgs?.fileNameBase,
    ],
  });
  const {
    canonical: existingMediaForSave,
    duplicates: duplicateMediaForSave,
  } = splitCanonicalMedia(saveMatches, [
    saveMatches.find(media => media.url === finalUrl)?.id,
    mediaId,
    overwriteExistingMedia?.id,
    overwriteMediaId,
    generatedFileNameMediaId,
    stableUploadMediaId,
  ]);
  if (finalTransferError && existingMediaForSave?.url) {
    finalUrl = existingMediaForSave.url;
    finalFileNameBase = getFileNamePartsFromStorageUrl(finalUrl).fileNameBase;
  }
  const photo: MediaDbInsert = {
    id: mediaId,
    url: finalUrl,
    extension,
    mediaType: 'video',
    title: titleForMediaProcessing(
      existingMediaForSave?.title ?? overwriteExistingMedia?.title,
      originalTitleFromFileName,
      submittedTitleIsGenerated ? undefined : submittedTitle,
      deriveTitleFromOriginalFileName(fileName),
    ),
    tags: existingMediaForSave?.tags ?? overwriteExistingMedia?.tags,
    posterUrl: undefined,
    previewUrl: undefined,
    transcodeStatus: 'failed',
    transcodeError: finalTransferError
      ? `${errorMessage || 'Processing failed'}; ${finalTransferError}`
      : errorMessage || 'Processing failed',
    aspectRatio: existingMediaForSave?.aspectRatio ??
      overwriteExistingMedia?.aspectRatio ??
      16 / 9,
    hidden: hidden === 'true' ||
      existingMediaForSave?.hidden ||
      overwriteExistingMedia?.hidden,
    excludeFromFeeds: excludeFromFeeds === 'true' ||
      existingMediaForSave?.excludeFromFeeds ||
      overwriteExistingMedia?.excludeFromFeeds,
    takenAt: takenAtLocal,
    takenAtNaive: takenAtNaiveLocal,
  };

  try {
    if (existingMediaForSave) {
      photo.id = existingMediaForSave.id;
      await deleteDuplicateMediaRecords(photo.id, duplicateMediaForSave);
      await saveMediaRecord(photo, [
        existingMediaForSave.id,
        mediaId,
        overwriteMediaId,
        generatedFileNameMediaId,
        stableUploadMediaId,
      ], {
        urls: [url, photo.url, finalUrl, overwriteTargetUrls?.url],
        fileNameBases: [
          fileNameBase,
          finalFileNameBase,
          getFileNamePartsFromStorageUrl(photo.url).fileNameBase,
          originalBase,
          preferredBase,
          overwriteTargetFromArgs?.fileNameBase,
        ],
      });
    } else {
      await saveMediaRecord(photo, [
        mediaId,
        overwriteMediaId,
        generatedFileNameMediaId,
        stableUploadMediaId,
      ], {
        urls: [url, finalUrl, overwriteTargetUrls?.url],
        fileNameBases: [
          fileNameBase,
          finalFileNameBase,
          originalBase,
          preferredBase,
          overwriteTargetFromArgs?.fileNameBase,
        ],
      });
    }
  } catch (error: any) {
    const existingByOriginalUrl = (await getMediaByStorageUrl(url))[0];
    if (!existingByOriginalUrl) {
      throw error;
    }
    photo.id = existingByOriginalUrl.id;
    photo.url = existingByOriginalUrl.url;
    photo.transcodeError = [
      photo.transcodeError,
      error?.message,
    ].filter(Boolean).join('; ');
    await saveMediaRecord(photo, [photo.id], {
      urls: [url],
      fileNameBases: [
        getFileNamePartsFromStorageUrl(photo.url).fileNameBase,
      ],
    });
  }
  if (albumIds?.length) {
    await addMediaAlbumIds([photo.id], albumIds);
  }
  const postSaveMatches = await collectMediaMatches({
    ids: [photo.id, mediaId, overwriteMediaId, generatedFileNameMediaId, stableUploadMediaId],
    urls: [url, photo.url, finalUrl, overwriteTargetUrls?.url],
    fileNameBases: [
      fileNameBase,
      finalFileNameBase,
      getFileNamePartsFromStorageUrl(photo.url).fileNameBase,
      originalBase,
      preferredBase,
      overwriteTargetFromArgs?.fileNameBase,
    ],
  });
  const { duplicates } = splitCanonicalMedia(postSaveMatches, [photo.id]);
  await deleteDuplicateMediaRecords(photo.id, duplicates);
  await cleanupExactDuplicateMediaRecords();
};

export const addFailedUploadAction = async ({
  url,
  title,
  originalFileName,
  preferredFileNameBase,
  overwriteMediaId,
  overwriteTargetUrls,
  errorMessage,
}: {
  url: string
  title?: string
  originalFileName?: string
  preferredFileNameBase?: string
  overwriteMediaId?: string
  overwriteTargetUrls?: {
    url: string
    posterUrl?: string
    previewUrl?: string
  }
  errorMessage?: string
}) =>
  runAuthenticatedAdminServerAction(async () => {
    await withUploadProcessingLock(url, originalFileName, () =>
      addFailedVideoUploadFromStorage({
        url,
        title,
        originalFileName,
        takenAtLocal: generateLocalPostgresString(),
        takenAtNaiveLocal: generateLocalNaivePostgresString(),
        preferredFileNameBase,
        overwriteMediaId,
        overwriteTargetUrls,
        errorMessage,
      }));

    try {
      revalidateAllKeysAndPaths();
    } catch (error) {
      console.error('Failed to revalidate after failed upload insert', error);
    }

    return {
      status: 'added' as const,
      statusMessage: 'Added',
    };
  });

export const addUploadsAction = async ({
  uploadUrls,
  uploadTitles,
  uploadOriginalFileNames,
  shouldRevalidateAllKeysAndPaths = true,
  albumTitles,
  tags,
  favorite,
  hidden,
  excludeFromFeeds,
  takenAtLocal,
  takenAtNaiveLocal,
  uploadOverwriteMediaIds,
  uploadOverwriteTargetUrls,
  uploadPreferredFileNameBases,
}: Omit<
  Parameters<typeof addUpload>[0],
  'url' | 'onStreamUpdate' | 'onFinish' | 'albumIds' | 'originalFileName'
> & {
  uploadUrls: string[]
  uploadTitles: string[]
  uploadOriginalFileNames?: (string | undefined)[]
  shouldRevalidateAllKeysAndPaths?: boolean
  albumTitles?: string[]
  uploadOverwriteMediaIds?: (string | undefined)[]
  uploadOverwriteTargetUrls?: ({
    url: string
    posterUrl?: string
    previewUrl?: string
  } | undefined)[]
  uploadPreferredFileNameBases?: (string | undefined)[]
}) =>
  runAuthenticatedAdminServerAction(async () => {
    const PROGRESS_TASK_COUNT = 3;

    const addedUploadUrls: string[] = [];
    let currentUploadUrl = '';
    let progress = 0;

    const stream = createStreamableValue<Omit<UrlAddStatus, 'fileName'>>();

    const streamUpdate = (
      statusMessage: string,
      status: UrlAddStatus['status'] = 'adding',
    ) =>
      stream.update({
        url: currentUploadUrl,
        status,
        statusMessage,
        progress: ++progress / PROGRESS_TASK_COUNT,
      });

    const albumIds = albumTitles
      ? await createAlbumsAndGetIds(albumTitles)
      : [];

    (async () => {
      try {
        for (const [index, url] of uploadUrls.entries()) {
          currentUploadUrl = url;
          progress = 0;
          const title = uploadTitles[index];
          const originalFileName = uploadOriginalFileNames?.[index];
          const overwriteMediaId = uploadOverwriteMediaIds?.[index];
          const overwriteTargetUrls = uploadOverwriteTargetUrls?.[index];
          const preferredFileNameBase = uploadPreferredFileNameBases?.[index];
          streamUpdate('Transferring to media storage');

          try {
            await addUpload({
              url,
              title,
              originalFileName,
              albumIds,
              tags,
              favorite,
              hidden,
              excludeFromFeeds,
              takenAtLocal,
              takenAtNaiveLocal,
              preferredFileNameBase,
              overwriteMediaId,
              overwriteTargetUrls,
              onStreamUpdate: streamUpdate,
              onFinish: () => {
                addedUploadUrls.push(url);
              },
            });
          } catch (error: any) {
            streamUpdate(
              error?.message || 'Failed to register upload',
              'error',
            );
          }
        };
      } catch (error: any) {
        // eslint-disable-next-line max-len
        stream.error(`${error.message} (${addedUploadUrls.length} of ${uploadUrls.length} photos successfully added)`);
      }
      stream.done();
    })();

    if (shouldRevalidateAllKeysAndPaths) {
      after(revalidateAllKeysAndPaths);
    }

    return stream.value;
  });

export const updateMediaAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo =
      await convertFormDataToMediaDbInsertAndLookupRecipeTitle(formData);
    const existingPhoto = await getMedia(photo.id, true);
    if (!existingPhoto) {
      throw new Error('Media not found');
    }

    const normalizedUpdatedTitle = normalizeTitle(photo.title);
    photo.title = normalizedUpdatedTitle ?? photo.title ?? undefined;

    const albumTitles = getAlbumTitlesFromFormData(formData);
    await addAlbumTitlesToMedia(albumTitles, photo.id);

    await updateMedia(photo)
      .then(() => propagateRecipeTitleIfNecessary(formData, photo));

    revalidateAllKeysAndPaths();

    redirect(PATH_ADMIN_MEDIA);
  });

export const updateMediaQuickMetaAction = async ({
  photoId,
  title,
  tags,
  albumTitles,
}: {
  photoId: string
  title?: string
  tags?: string
  albumTitles?: string
}) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId, true);
    if (!photo) {
      return {
        success: false,
        error: 'Media not found',
      } as const;
    }

    photo.title = normalizeTitle(photo.title) ?? photo.title ?? undefined;

    const trimmedTitle = title?.trim() ?? '';
    const normalizedInputTitle =
      title !== undefined
        ? normalizeTitle(trimmedTitle) ?? ''
        : undefined;
    const existingTitleForComparison = photo.title?.trim() ?? '';
    const hasTitleChange =
      title !== undefined &&
      existingTitleForComparison !== (normalizedInputTitle ?? existingTitleForComparison);
    if (hasTitleChange) {
      photo.title = normalizedInputTitle && normalizedInputTitle.length > 0
        ? normalizedInputTitle
        : undefined;
    }

    let hasTagsChange = false;
    if (tags !== undefined) {
      const parsedTags = convertStringToArray(tags) ?? [];
      const normalizedTags = Array.from(
        new Set(parsedTags.map(tag => tag.trim()).filter(Boolean)),
      );
      const reservedTags = photo.tags.filter(tag =>
        isTagFavs(tag) || isTagPrivate(tag),
      );
      const mergedTags = [
        ...reservedTags,
        ...normalizedTags.filter(tag => !reservedTags.includes(tag)),
      ];
      const existingTags = photo.tags ?? [];
      hasTagsChange = existingTags.length !== mergedTags.length ||
        existingTags.some((tag, index) => tag !== mergedTags[index]);
      if (hasTagsChange) {
        photo.tags = mergedTags;
      }
    }

    let hasAlbumsChange = false;
    let dedupedAlbumTitles: string[] = [];
    if (albumTitles !== undefined) {
      const rawAlbumTitles = convertStringToArray(albumTitles, false) ?? [];
      const seen = new Set<string>();
      const normalize = (value: string) => value.trim().toLocaleLowerCase();
      dedupedAlbumTitles = rawAlbumTitles.reduce<string[]>((acc, title) => {
        const trimmedTitle = title.trim();
        if (!trimmedTitle) { return acc; }
        const normalized = normalize(trimmedTitle);
        if (seen.has(normalized)) { return acc; }
        seen.add(normalized);
        acc.push(trimmedTitle);
        return acc;
      }, []);
      const existingAlbumTitles = await getAlbumTitlesForMedia(photoId);
      const existingNormalized = new Set(
        existingAlbumTitles.map(value => normalize(value)),
      );
      hasAlbumsChange =
        existingNormalized.size !== seen.size ||
        Array.from(existingNormalized).some(value => !seen.has(value));
      if (hasAlbumsChange) {
        await addAlbumTitlesToMedia(dedupedAlbumTitles, photoId);
      }
    }

    if (!hasTitleChange && !hasTagsChange && !hasAlbumsChange) {
      return {
        success: true,
        updated: false,
      } as const;
    }

    if (hasTitleChange || hasTagsChange) {
      await updateMedia(convertMediaToMediaDbInsert(photo));
    }
    revalidateMedia(photoId);

    return {
      success: true,
      updated: true,
    } as const;
  });

export const getUniqueVideoLibraryOptionsAction = async () =>
  runAuthenticatedAdminServerAction(async () => {
    const [performers, studios, contentTypes, categories] = await Promise.all([
      getUniquePerformers(),
      getUniqueStudios(),
      getUniqueVideoContentTypes(),
      getUniqueCategories(),
    ]);
    return {
      performers,
      studios,
      contentTypes,
      categories,
    };
  });

export const tagMultipleMediaAction = async (
  tags: string,
  photoIds: string[],
) =>
  runAuthenticatedAdminServerAction(async () => {
    await addTagsToMedia(
      convertStringToArray(tags, false) ?? [],
      photoIds,
    );
    revalidateAllKeysAndPaths();
  });

export const toggleFavoriteMediaAction = async (
  photoId: string,
  shouldRedirect?: boolean,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId);
    if (photo) {
      const { tags } = photo;
      photo.tags = isMediaFav(photo)
        ? tags.filter(tag => !isTagFavs(tag))
        : [...tags, TAG_FAVS];
      await updateMedia(convertMediaToMediaDbInsert(photo));
      revalidateAllKeysAndPaths();
      if (shouldRedirect) {
        redirect(pathForMedia({ photo: photoId }));
      }
    }
  });

export const togglePrivateMediaAction = async (
  photoId: string,
  redirectPath?: string,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId, true);
    if (photo) {
      photo.hidden = !photo.hidden;
      await updateMedia(convertMediaToMediaDbInsert(photo));
      revalidateAllKeysAndPaths();
    }
    if (redirectPath) { redirect(redirectPath); }
  });

export const deleteMediaItemsAction = async (photoIds: string[]) =>
  runAuthenticatedAdminServerAction(async () => {
    let queued = 0;
    for (const photoId of photoIds) {
      const photo = await getMedia(photoId, true);
      if (photo) {
        await enqueueMediaDeletion(photo);
        queued += 1;
      }
    }
    if (queued > 0) {
      after(() => triggerDeletionOrchestrator().catch(error => {
        console.error('Failed to trigger Backend Orchestrator deletion queue', error);
      }));
    }
    revalidateAllKeysAndPaths();
  }, 'delete');

export const deleteMediaAction = async (
  photoId: string,
  photoUrl: string,
  shouldRedirect?: boolean,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId, true);
    await enqueueMediaDeletion(photo ?? {
      id: photoId,
      title: photoId,
      url: photoUrl,
      posterUrl: undefined,
      previewUrl: undefined,
    });
    after(() => triggerDeletionOrchestrator().catch(error => {
      console.error('Failed to trigger Backend Orchestrator deletion queue', error);
    }));
    revalidateAllKeysAndPaths();
    if (shouldRedirect) {
      redirect(PATH_ROOT);
    }
  }, 'delete');

export const deleteMediaTagGloballyFormAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const tag = formData.get('tag') as string;
    await deleteMediaTagGlobally(tag);
    revalidateMediaKey();
    revalidateAdminPaths();
  }, 'delete');

export const deleteMediaCategoryGloballyFormAction = async (
  formData: FormData,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const category = formData.get('category') as string;
    await deleteMediaCategoryGlobally(category);
    revalidateMediaKey();
    revalidateCategoriesKey();
    revalidateAdminPaths();
  }, 'delete');

export const deleteMediaStudioGloballyFormAction = async (
  formData: FormData,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const studio = formData.get('studio') as string;
    await deleteMediaStudioGlobally(studio);
    revalidateMediaKey();
    revalidateStudiosKey();
    revalidateAdminPaths();
    redirect(PATH_ADMIN_STUDIOS);
  }, 'delete');

export const deleteMediaPerformerGloballyFormAction = async (
  formData: FormData,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const performer = formData.get('performer') as string;
    await deleteMediaPerformerGlobally(performer);
    revalidateMediaKey();
    revalidatePerformersKey();
    revalidateAdminPaths();
    redirect(PATH_ADMIN_PERFORMERS);
  }, 'delete');

export const deleteMediaContentTypeGloballyFormAction = async (
  formData: FormData,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const contentType = formData.get('contentType') as string;
    await deleteMediaContentTypeGlobally(contentType);
    revalidateMediaKey();
    revalidateContentTypesKey();
    revalidateAdminPaths();
    redirect(PATH_ADMIN_CONTENT_TYPES);
  }, 'delete');

export const deleteMediaTagGloballyAction = async (
  tag: string,
  currentPath?: string,
) =>
  runAuthenticatedAdminServerAction(async () => {
    await deleteMediaTagGlobally(tag);
    revalidateAllKeysAndPaths();
    if (currentPath === pathForTag(tag)) {
      redirect(PATH_ROOT);
    }
  }, 'delete');

export const renameMediaTagGloballyAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const tag = formData.get('tag') as string;
    const updatedTag = formData.get('updatedTag') as string;

    if (tag && updatedTag && tag !== updatedTag) {
      await renameMediaTagGlobally(tag, updatedTag);
      revalidateMediaKey();
      revalidateTagsKey();
      redirect(PATH_ADMIN_TAGS);
    }
  });

export const upgradeTagToAlbumAction = async (tag: string) =>
  runAuthenticatedAdminServerAction(async () =>
    upgradeTagToAlbum(tag).then(revalidateAllKeysAndPaths),
  );

export const getMediaNeedingRecipeTitleCountAction = async (
  recipeData: string,
  film: string,
  photoIdToExclude?: string,
) =>
  runAuthenticatedAdminServerAction(async () =>
    await getMediaNeedingRecipeTitleCount(
      recipeData,
      film,
      photoIdToExclude,
    ),
  );

export const storeColorDataForMediaAction = async (photoId: string) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId, true);
    if (photo) {
      const colorFields = await getColorFieldsForImageUrl(
        photo.url,
        photo.colorData,
      );
      if (colorFields) {
        await updateMedia(convertMediaToMediaDbInsert({
          ...photo,
          ...colorFields,
        }));
      }
      revalidateMedia(photo.id);
    }
  });

export const recalculateColorDataForAllMediaAction = async () =>
  runAuthenticatedAdminServerAction(async () => {
    const photos = await getColorDataForMedia();
    for (const { id, url, colorData: _colorData } of photos) {
      const colorFields = await getColorFieldsForMediaDbInsert(url, _colorData);
      if (colorFields && colorFields.colorSort) {
        await updateColorDataForMedia(
          id,
          colorFields.colorData,
          colorFields.colorSort,
        );
      }
    }
  });

export const deleteMediaRecipeGloballyAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const recipe = formData.get('recipe') as string;

    await deleteMediaRecipeGlobally(recipe);

    revalidateMediaKey();
    revalidateAdminPaths();
  }, 'delete');

export const renameMediaRecipeGloballyAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const recipe = formData.get('recipe') as string;
    const updatedRecipe = formData.get('updatedRecipe') as string;

    if (recipe && updatedRecipe && recipe !== updatedRecipe) {
      await renameMediaRecipeGlobally(recipe, updatedRecipe);
      revalidateMediaKey();
      revalidateRecipesKey();
      redirect(PATH_ADMIN_RECIPES);
    }
  });

export const deleteUploadsAction = async (urls: string[]) =>
  runAuthenticatedAdminServerAction(async () => {
    await Promise.all(urls.map(async url => {
      try {
        await deleteFile(url);
      } catch (error) {
        console.warn('Upload storage delete failed during cleanup', {
          url,
          error,
        });
      } finally {
        await clearWorkerRegistrationStatusForUrl(url);
      }
    }));
    if (urls.length > 1) {
      // Only refresh state when deleting multiple uploads
      revalidateAdminPaths();
    }
  }, 'delete');

// Accessed from admin photo edit page
// will not update blur data
export const getExifDataAction = async (
  url: string,
): Promise<Partial<MediaFormData>> =>
  runAuthenticatedAdminServerAction(async () => {
    const { formDataFromExif } = await extractImageDataFromBlobPath(url);
    if (formDataFromExif) {
      return formDataFromExif;
    } else {
      return {};
    }
  });

const processQueuedMedia = async (photo: Media) => {
  const {
    formDataFromExif,
    imageResizedBase64,
    shouldStripGpsData,
    fileBytes,
  } = await extractImageDataFromBlobPath(photo.url, {
    includeInitialMediaFields: true,
    generateBlurData: BLUR_ENABLED,
    generateResizedImage:
      AI_CONTENT_GENERATION_ENABLED && photo.mediaType !== 'video',
  });

  if (!formDataFromExif) {
    throw new Error('Metadata generation failed');
  }

  const extensionLower = (
    formDataFromExif.extension ||
    photo.extension ||
    getFileNamePartsFromStorageUrl(photo.url).fileExtension
  )?.toLowerCase();
  const mediaType = photo.mediaType === 'video'
    ? 'video'
    : mediaTypeFromExtension(extensionLower, photo.mediaType);
  const originalFileNameFromExif =
    typeof formDataFromExif.uploadOriginalFileName === 'string'
      ? formDataFromExif.uploadOriginalFileName
      : undefined;

  let aiTitle: string | undefined;
  let aiCaption: string | undefined;
  let aiTags: string | undefined;
  let semantic: string | undefined;
  if (
    AI_CONTENT_GENERATION_ENABLED &&
    mediaType !== 'video' &&
    imageResizedBase64
  ) {
    const uniqueTags = await getUniqueTags();
    const aiGenerated = await generateAiImageQueries({
      imageBase64: imageResizedBase64,
      textFieldsToGenerate: getAiTextFieldsToGenerate(
        AI_TEXT_AUTO_GENERATED_FIELDS,
        Boolean(photo.title || formDataFromExif.title),
        Boolean(photo.caption || formDataFromExif.caption),
        Boolean(photo.tags?.length || formDataFromExif.tags),
      ),
      existingTitle: photo.title || formDataFromExif.title,
      uniqueTags,
    });
    aiTitle = aiGenerated.title;
    aiCaption = aiGenerated.caption;
    aiTags = aiGenerated.tags;
    semantic = aiGenerated.semantic;
  }

  const convertedMediaResult = await convertUploadToMedia({
    uploadUrl: photo.url,
    fileBytes,
    shouldStripGpsData,
    shouldDeleteOrigin: false,
    mediaType,
    originalFileName: originalFileNameFromExif,
    deferTranscode: false,
    overwriteTarget: {
      url: photo.url,
      posterUrl: photo.posterUrl,
      previewUrl: photo.previewUrl,
    },
    preferredFileNameBase: getPreferredFileNameBaseForExistingMedia(
      photo,
      originalFileNameFromExif,
    ),
  });

  const form: Partial<MediaFormData> = {
    ...formDataFromExif,
    id: photo.id,
    url: convertedMediaResult.url,
    mediaType,
    title: titleForMediaProcessing(
      photo.title,
      formDataFromExif.title,
      aiTitle,
    ),
    caption: photo.caption || formDataFromExif.caption || aiCaption,
    tags: photo.tags?.length
      ? photo.tags.join(',')
      : formDataFromExif.tags || aiTags,
    semanticDescription:
      photo.semanticDescription ||
      formDataFromExif.semanticDescription ||
      semantic,
    hidden: photo.hidden ? 'true' : 'false',
    excludeFromFeeds: photo.excludeFromFeeds ? 'true' : 'false',
    takenAt:
      formDataFromExif.takenAt ||
      photo.takenAt.toISOString(),
    takenAtNaive:
      formDataFromExif.takenAtNaive ||
      photo.takenAtNaive,
  };
  const photoFormDbInsert =
    await convertFormDataToMediaDbInsertAndLookupRecipeTitle(form);
  photoFormDbInsert.priorityOrder = photo.priorityOrder ?? undefined;
  applyConvertedMediaToMedia(photoFormDbInsert, convertedMediaResult);
  if (convertedMediaResult.transcodeStatus === 'ready') {
    photoFormDbInsert.transcodeStatus = 'ready';
    photoFormDbInsert.transcodeError = undefined;
  } else if (
    convertedMediaResult.transcodeStatus === 'pending' ||
    convertedMediaResult.transcodeStatus === 'processing'
  ) {
    photoFormDbInsert.transcodeStatus = convertedMediaResult.transcodeStatus;
    photoFormDbInsert.transcodeError = convertedMediaResult.transcodeError;
  } else {
    photoFormDbInsert.transcodeStatus = 'failed';
    photoFormDbInsert.transcodeError =
      convertedMediaResult.transcodeError ||
      'Processing failed';
  }

  await updateMedia(photoFormDbInsert);
};

const shouldForceVideoProcessingOnSync = async (photo: Media) => {
  const extensionLower =
    photo.extension?.toLowerCase() ||
    getFileNamePartsFromStorageUrl(photo.url).fileExtension?.toLowerCase();
  const isVideo = photo.mediaType === 'video' ||
    (extensionLower ? VIDEO_EXTENSION_SET.has(extensionLower) : false);

  if (!isVideo) { return false; }

  if (
    photo.transcodeStatus === 'pending' ||
    photo.transcodeStatus === 'processing' ||
    photo.transcodeStatus === 'failed'
  ) {
    return true;
  }

  return shouldBackfillMediaStorage(photo);
};

// Accessed from admin photo table, will:
// - update EXIF data
// - anonymize storage url if necessary
// - strip GPS data if necessary
// - update blur data (or destroy if blur is disabled)
// - generate AI text data, if enabled, and auto-generated fields are empty
export const syncMediaForAutomation = async (
  photoId: string,
  _options: {
    isBatch?: boolean,
    updateMode?: boolean,
  } = {},
) => {
    const photo = await getMedia(photoId ?? '', true);

    if (photo) {
      if (await shouldForceVideoProcessingOnSync(photo)) {
        await processQueuedMedia(photo);
        revalidateAllKeysAndPaths();
        return;
      }

      if (isVirtualStorageVideoId(photo.id)) {
        await addUpload({
          url: photo.url,
          title: photo.title,
          originalFileName: getFileNamePartsFromStorageUrl(photo.url).fileName,
          takenAtLocal: photo.takenAt.toISOString(),
          takenAtNaiveLocal: photo.takenAtNaive,
        });
        revalidateAllKeysAndPaths();
        return;
      }

      const {
        formDataFromExif,
        shouldStripGpsData,
        fileBytes,
      } = await extractImageDataFromBlobPath(photo.url, {
        includeInitialMediaFields: false,
        generateBlurData: false,
        generateResizedImage: false,
        updateColorFields: false,
      });

      let urlToDelete: string | undefined;
      let convertedMediaResult: ConvertUploadToMediaResult | undefined;
      if (formDataFromExif) {
        const extensionLowerFromExif = formDataFromExif.extension?.toLowerCase();
        const extensionLower = extensionLowerFromExif ||
          photo.extension?.toLowerCase();
        const isVideoByExtension = extensionLower
          ? VIDEO_EXTENSION_SET.has(extensionLower)
          : false;
        if (isVideoByExtension) {
          formDataFromExif.mediaType = 'video';
        }
        const inferredMediaType =
          photo.mediaType === 'video' || isVideoByExtension
            ? 'video'
            : photo.mediaType;
        const originalFileNameFromExif =
          typeof formDataFromExif.uploadOriginalFileName === 'string'
            ? formDataFromExif.uploadOriginalFileName
            : undefined;
        const shouldForceVideoRefresh = inferredMediaType === 'video';

        if (
          shouldForceVideoRefresh ||
          await shouldBackfillMediaStorage(photo) ||
          shouldStripGpsData
        ) {
          const preferredFileNameBaseForSync =
            getPreferredFileNameBaseForExistingMedia(
              photo,
              originalFileNameFromExif,
            );
          convertedMediaResult = await convertUploadToMedia({
            uploadUrl: photo.url,
            fileBytes,
            shouldStripGpsData,
            shouldDeleteOrigin: false,
            mediaType: inferredMediaType,
            originalFileName: originalFileNameFromExif,
            deferTranscode: false,
            // Overwrite the current file instead of generating a new suffixed one
            overwriteTarget: {
              url: photo.url,
              posterUrl: photo.posterUrl,
              previewUrl: photo.previewUrl,
            },
            preferredFileNameBase: preferredFileNameBaseForSync,
          });
          if (
            convertedMediaResult?.url &&
            convertedMediaResult.url !== photo.url
          ) {
            urlToDelete = photo.url;
          }
        }

        const photoFormDbInsert = convertMediaToMediaDbInsert(photo);
        if (convertedMediaResult) {
          applyConvertedMediaToMedia(photoFormDbInsert, convertedMediaResult);
        }

        await updateMedia(photoFormDbInsert)
          .then(async () => {
            if (urlToDelete) { await deleteFile(urlToDelete); }
          });

        revalidateAllKeysAndPaths();
      }
    }
  };

export const syncMediaAction = async (
  photoId: string,
  options: {
    isBatch?: boolean,
    updateMode?: boolean,
  } = {},
) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId ?? '', true);
    if (!photo) { return; }

    if (
      await hasProcessingOrchestrator() &&
      await shouldForceVideoProcessingOnSync(photo)
    ) {
      const queuedPhoto = convertMediaToMediaDbInsert(photo);
      queuedPhoto.transcodeStatus = 'pending';
      queuedPhoto.transcodeError = 'Queued for background processing';
      await updateMedia(queuedPhoto);
      after(() => triggerProcessingOrchestrator().catch(error => {
        console.error('Failed to trigger Backend Orchestrator', error);
        return false;
      }));
      revalidateAllKeysAndPaths();
      return;
    }

    await syncMediaForAutomation(photoId, options);
  });

export const syncMediaItemsAction = async (photosToSync: {
  photoId: string,
  onlySyncColorData?: boolean,
}[]) =>
  runAuthenticatedAdminServerAction(async () => {
    for (const { photoId, onlySyncColorData } of photosToSync) {
      await (onlySyncColorData
        ? storeColorDataForMediaAction(photoId)
        : syncMediaAction(photoId, { isBatch: true }));
    }
    revalidateAllKeysAndPaths();
  });

export const clearCacheAction = async () =>
  runAuthenticatedAdminServerAction(revalidateAllKeysAndPaths);

// Add or replace subtitle sidecars for a photo
type SubtitleManifestTrack = {
  src: string
  lang: string
  label?: string
};

const readSubtitleManifestTracks = async (
  fileNameBase: string,
): Promise<SubtitleManifestTrack[]> => {
  const files = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
  const manifestUrl = files.find(({ fileName }) =>
    fileName.toLowerCase() === `${fileNameBase}-subtitles.json`.toLowerCase())?.url;
  if (!manifestUrl) { return []; }
  const readableUrl = isUrlFromDrive(manifestUrl)
    ? (await driveCreatePresignedDownload(driveKeyFromUrl(manifestUrl))).url
    : manifestUrl;
  return fetch(readableUrl, { cache: 'no-store' })
    .then(async response => response.ok
      ? (await response.json() as { tracks?: SubtitleManifestTrack[] }).tracks || []
      : [])
    .catch(() => []);
};

const subtitleFileNameFromUrl = (url: string) =>
  getFileNamePartsFromStorageUrl(url).fileName;

const rebuildSubtitleManifest = async (
  fileNameBase: string,
  previousTracks: SubtitleManifestTrack[],
  overrides: Record<string, { lang: string, label: string }> = {},
) => {
  const existingByFileName = new Map(previousTracks.map(track => [
    subtitleFileNameFromUrl(track.src).toLowerCase(),
    track,
  ]));
  const remaining = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
  const tracks = remaining
    .filter(({ fileName }) => /-subtitles(\.[a-zA-Z0-9_-]+)?\.vtt$/i.test(fileName))
    .map(({ fileName, url }) => {
      const match = fileName.match(/-subtitles\.([a-zA-Z0-9_-]+)\.vtt$/i);
      const fallbackLang = match?.[1] || 'default';
      const override = overrides[fileName.toLowerCase()];
      const previous = existingByFileName.get(fileName.toLowerCase());
      const lang = override?.lang || previous?.lang || fallbackLang;
      return {
        src: url,
        lang,
        label: override?.label || previous?.label || lang.toUpperCase(),
      };
    });
  await putFile(
    Buffer.from(JSON.stringify({ tracks })),
    `${fileNameBase}-subtitles.json`,
  );
};

export const addSubtitlesAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const photoId = formData.get('photoId')?.toString() || '';
    const lang = (formData.get('subtitleLang')?.toString() || 'default').trim() || 'default';
    const label = (formData.get('subtitleLabel')?.toString() || '').trim();
    const files = (formData.getAll('subtitleFiles') as unknown as File[]);
    if (!photoId || files.length === 0) { return; }
    const photo = await getMedia(photoId, true);
    if (!photo) { return; }
    const { fileNameBase } = getFileNamePartsFromStorageUrl(photo.url);
    const payload = await Promise.all(files.map(async f => ({ name: f.name, bytes: await f.arrayBuffer() })));
    const previousTracks = await readSubtitleManifestTracks(fileNameBase);
    const storedFileName = await storeUploadedSubtitles({ fileNameBase, files: payload, lang });
    await rebuildSubtitleManifest(fileNameBase, previousTracks, {
      [storedFileName.toLowerCase()]: {
        lang,
        label: label || lang.toUpperCase(),
      },
    });
    after(revalidateAllKeysAndPaths);
    return;
  });

export const deleteSubtitleAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const photoId = formData.get('photoId')?.toString() || '';
    const fileName = formData.get('subtitleFileName')?.toString() || '';
    if (!photoId || !fileName) { return; }
    const photo = await getMedia(photoId, true);
    if (!photo) { return; }
    // Delete the subtitle file across all storages and rebuild manifest
    const { fileNameBase } = getFileNamePartsFromStorageUrl(photo.url);
    const previousTracks = await readSubtitleManifestTracks(fileNameBase);
    const all = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
    const targets = all.filter(({ fileName: fn }) => fn.toLowerCase() === fileName.toLowerCase());
    await Promise.all(targets.map(({ url }) => deleteFile(url).catch(() => undefined)));
    await rebuildSubtitleManifest(fileNameBase, previousTracks);
    after(revalidateAllKeysAndPaths);
    return;
  }, 'delete');

// Rename a subtitle sidecar by changing its language code
export const updateSubtitleTrackAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const photoId = formData.get('photoId')?.toString() || '';
    const fileName = formData.get('subtitleFileName')?.toString() || '';
    const newLangRaw = formData.get('subtitleNewLang')?.toString() || '';
    const newLabel = (formData.get('subtitleNewLabel')?.toString() || '').trim();
    if (!photoId || !fileName || !newLangRaw || !newLabel) { return; }
    const newLang = newLangRaw.trim();
    // Allow alphanumerics, dash and underscore
    if (!/^[a-zA-Z0-9_-]{1,48}$/.test(newLang) || newLabel.length > 120) { return; }
    const photo = await getMedia(photoId, true);
    if (!photo) { return; }
    const { fileNameBase } = getFileNamePartsFromStorageUrl(photo.url);
    const newFileName = `${fileNameBase}-subtitles.${newLang}.vtt`;
    const previousTracks = await readSubtitleManifestTracks(fileNameBase);
    const all = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
    if (fileName.toLowerCase() !== newFileName.toLowerCase()) {
      const collision = all.some(({ fileName: candidate }) =>
        candidate.toLowerCase() === newFileName.toLowerCase());
      if (collision) { throw new Error('A subtitle track already uses that language code.'); }
      const targets = all.filter(({ fileName: fn }) => fn.toLowerCase() === fileName.toLowerCase());
      await Promise.all(targets.map(async ({ url }) => {
        const copied = await copyFile(url, newFileName);
        if (copied) { await deleteFile(url); }
      }));
    }
    await rebuildSubtitleManifest(fileNameBase, previousTracks, {
      [newFileName.toLowerCase()]: { lang: newLang, label: newLabel },
    });
    after(revalidateAllKeysAndPaths);
    return;
  });

// Subtitle language list: fetch, add, delete
export const getSubtitleLanguagesAction = async () =>
  runAuthenticatedAdminServerAction(async () => getSubtitleLanguages());

export const addSubtitleLanguageAction = async (code: string, label?: string) =>
  runAuthenticatedAdminServerAction(async () => {
    const normalized = (code || '').trim();
    if (!/^[a-zA-Z0-9_-]{1,16}$/.test(normalized)) { return; }
    await addSubtitleLanguage(normalized, label);
    return true;
  });

export const deleteSubtitleLanguageAction = async (code: string) =>
  runAuthenticatedAdminServerAction(async () => {
    const normalized = (code || '').trim();
    if (!/^[a-zA-Z0-9_-]{1,16}$/.test(normalized)) { return; }
    await deleteSubtitleLanguage(normalized);
    return true;
  }, 'delete');

// Delete any single storage asset under the photo's base (safety: not the main file)
export const deleteStorageAssetAction = async (formData: FormData) =>
  runAuthenticatedAdminServerAction(async () => {
    const photoId = formData.get('photoId')?.toString() || '';
    const assetFileName = formData.get('assetFileName')?.toString() || '';
    if (!photoId || !assetFileName) { return; }
    const photo = await getMedia(photoId, true);
    if (!photo) { return; }
    const { fileName: mainFileName, fileNameBase } = getFileNamePartsFromStorageUrl(photo.url);
    if (assetFileName === mainFileName) {
      throw new Error('Refusing to delete primary media file.');
    }
    const previousSubtitleTracks = /-subtitles(\.[a-zA-Z0-9_-]+)?\.vtt$/i
      .test(assetFileName)
      ? await readSubtitleManifestTracks(fileNameBase)
      : [];
    // Delete this asset across all configured storages
    const all = await getCurrentStorageUrlsForPrefix(fileNameBase).catch(() => []);
    const targets = all.filter(({ fileName }) => fileName.toLowerCase() === assetFileName.toLowerCase());
    await Promise.all(targets.map(({ url }) => deleteFile(url).catch(() => undefined)));
    // If deleting a subtitle, rebuild manifest
    if (/-subtitles(\.[a-zA-Z0-9_-]+)?\.vtt$/i.test(assetFileName)) {
      await rebuildSubtitleManifest(fileNameBase, previousSubtitleTracks);
    }
    after(revalidateAllKeysAndPaths);
    return;
  }, 'delete');

export const streamAiImageQueryAction = async (
  imageBase64: string,
  query: AiImageQuery,
  existingTitle?: string,
) =>
  runAuthenticatedAdminServerAction(async () => {
    const existingTags = await getUniqueTags();
    return streamOpenAiImageQuery(
      imageBase64,
      getAiImageQuery(query, existingTitle, existingTags),
    );
  });

export const getImageBlurAction = async (url: string) =>
  runAuthenticatedAdminServerAction(() => blurImageFromUrl(url));

export const getStorageUrlsForMediaAction = async (photoId: string) =>
  runAuthenticatedAdminServerAction(async () => {
    const photo = await getMedia(photoId, true);
    if (!photo) { return undefined; }
    return getStorageUrlsForMedia(photo);
  });

// Public/Private actions

export const getMediaAction = async (
  options: MediaQueryOptions,
  warmOnly?: boolean,
) => {
  if (warmOnly) {
    return [];
  } else {
    return areOptionsSensitive(options)
      ? runAuthenticatedAdminServerAction(() => getMedia(options))
      : getMedia(options);
  }
};

export const getMediaCachedAction = async (
  options: MediaQueryOptions,
  warmOnly?: boolean,
) => {
  if (warmOnly) {
    return [];
  } else {
    return areOptionsSensitive(options)
      ? runAuthenticatedAdminServerAction(() => getMediaCached(options))
      : getMediaCached(options);
  }
};

// Public actions

export const searchMediaAction = async (query: string) =>
  getMedia({ query, limit: 25 })
    .catch(e => {
      console.error('Could not query photos', e);
      return [] as Media[];
    });
