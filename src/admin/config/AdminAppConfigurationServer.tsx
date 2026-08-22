import AdminAppConfigurationClient from './AdminAppConfigurationClient';
import { APP_CONFIGURATION } from '@/app/config';
import { testConnectionsAction } from '@/admin/actions';
import { generateAuthSecret } from '@/auth';
import { getProcessingSettingsSafe } from '@/processing/settings';
import { getSiteAccessSettingsSafe } from '@/auth/site-access';
import { getCloudflareWorkerIntegrationStatus } from '@/processing/cloudflare-worker-integration';

export default async function AdminAppConfigurationServer({
  simplifiedView,
}: {
  simplifiedView?: boolean
}) {
  const [
    connectionErrors,
    secret,
    processingSettings,
    siteAccessSettings,
    cloudflareWorkerIntegration,
  ] = await Promise.all([
    testConnectionsAction().catch(() => ({})),
    generateAuthSecret(),
    getProcessingSettingsSafe(),
    getSiteAccessSettingsSafe(),
    getCloudflareWorkerIntegrationStatus().catch(() => ({ configured: false })),
  ]);

  return (
    <AdminAppConfigurationClient {...{
      ...APP_CONFIGURATION,
      ...connectionErrors,
      secret,
      processingSettings,
      siteAccessSettings,
      cloudflareWorkerIntegration,
      simplifiedView,
    }} />
  );
}
