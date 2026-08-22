'use server';

import { revalidatePath } from 'next/cache';
import { PATH_ADMIN_CONFIGURATION } from '@/app/path';
import { runAuthenticatedAdminServerAction } from '@/auth/server';
import { parseProcessingSettings } from './settings-schema';
import { saveProcessingSettings } from './settings';
import { provisionCloudflareWorkerIntegration } from './cloudflare-worker-integration';

export type ProcessingSettingsActionState = {
  saved?: boolean
  error?: string
};

export const saveProcessingSettingsAction = async (
  _state: ProcessingSettingsActionState,
  formData: FormData,
): Promise<ProcessingSettingsActionState> =>
  runAuthenticatedAdminServerAction(async () => {
    try {
      const values: Record<string, unknown> = Object.fromEntries(
        formData.entries(),
      );
      values.orchestratorEnabled = formData.has('orchestratorEnabled');
      values.registrationEnabled = formData.has('registrationEnabled');
      values.videoProcessingEnabled = formData.has('videoProcessingEnabled');
      const settings = parseProcessingSettings(values);
      await saveProcessingSettings(settings);
      revalidatePath(PATH_ADMIN_CONFIGURATION);
      return { saved: true };
    } catch (error) {
      return {
        error: error instanceof Error
          ? error.message
          : 'Unable to save settings',
      };
    }
  }, 'manage-configuration');

export const deployCloudflareWorkerAction = async (
  _state: ProcessingSettingsActionState,
  formData: FormData,
): Promise<ProcessingSettingsActionState> =>
  runAuthenticatedAdminServerAction(async () => {
    try {
      await provisionCloudflareWorkerIntegration({
        token: String(formData.get('cloudflareApiToken') || ''),
        accountId: String(formData.get('cloudflareAccountId') || '') || undefined,
      });
      revalidatePath(PATH_ADMIN_CONFIGURATION);
      return { saved: true };
    } catch (error) {
      return { error: error instanceof Error ? error.message : 'Worker deployment failed' };
    }
  }, 'manage-configuration');
