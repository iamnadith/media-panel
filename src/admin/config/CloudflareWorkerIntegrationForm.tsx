'use client';

import { useActionState } from 'react';
import clsx from 'clsx/lite';
import { deployCloudflareWorkerAction, type ProcessingSettingsActionState } from '@/processing/settings-actions';
import type { CloudflareWorkerIntegrationStatus } from '@/processing/cloudflare-worker-integration';

const initialState: ProcessingSettingsActionState = {};

export default function CloudflareWorkerIntegrationForm({ status }: { status: CloudflareWorkerIntegrationStatus }) {
  const [state, action, pending] = useActionState(deployCloudflareWorkerAction, initialState);
  return <form action={action} className="space-y-3">
    <div>
      <div className="font-bold text-main">Cloudflare Worker</div>
      <p className="text-sm text-dim">The panel uploads this release&apos;s orchestrator code, creates Hyperdrive, generates the private key, enables the workers.dev URL, and creates the one-minute schedule. The token needs Workers Scripts Edit and Hyperdrive Edit.</p>
    </div>
    {status.configured && <div className="rounded-md bg-dim px-3 py-2 text-sm text-main">Deployed: {status.workerUrl}<br />Hyperdrive: {status.hyperdriveId}</div>}
    <label className="block text-sm font-medium text-main">Cloudflare API token<input required name="cloudflareApiToken" type="password" autoComplete="off" className="mt-1 w-full rounded-md border border-medium bg-main px-3 py-2 text-main" /></label>
    <label className="block text-sm font-medium text-main">Cloudflare account ID <span className="font-normal text-dim">(only needed if the token has multiple accounts)</span><input name="cloudflareAccountId" autoComplete="off" className="mt-1 w-full rounded-md border border-medium bg-main px-3 py-2 text-main" /></label>
    <div className="flex items-center justify-between gap-3"><span className={clsx('text-sm', state.error ? 'text-red-600' : 'text-dim')}>{state.error || (state.saved ? 'Worker deployed and connected' : '')}</span><button type="submit" disabled={pending} className="rounded-md bg-main px-4 py-2 text-sm font-medium text-inverse hover:opacity-85 disabled:cursor-wait disabled:opacity-60">{pending ? 'Deploying…' : status.configured ? 'Redeploy Worker' : 'Deploy Worker'}</button></div>
  </form>;
}
