import { getBackendOrchestratorIntegration } from './cloudflare-worker-integration';
import { getProcessingSettingsSafe } from './settings';

export const hasProcessingOrchestrator = async () => Boolean(await getBackendOrchestratorIntegration());
export type ProcessingOrchestratorRunResult = { triggered: boolean; registeringUrls?: string[] };
const call = async (pathname: string, init?: RequestInit) => {
  const integration = await getBackendOrchestratorIntegration();
  if (!integration) return undefined;
  return fetch(`${integration.baseUrl.replace(/\/+$/, '')}${pathname}`, { ...init, headers: { Authorization: `Bearer ${integration.sharedSecret}`, ...(init?.headers || {}) } });
};
export const runProcessingOrchestrator = async () => {
  const settings = await getProcessingSettingsSafe();
  if (!settings.orchestratorEnabled || !settings.registrationEnabled) return { triggered: false } satisfies ProcessingOrchestratorRunResult;
  const response = await call('/run', { method: 'POST' });
  if (!response) return { triggered: false } satisfies ProcessingOrchestratorRunResult;
  if (!response.ok) throw new Error(await response.text().catch(() => '') || `Backend Orchestrator failed (${response.status})`);
  return { triggered: true, ...await response.json().catch(() => ({})) } satisfies ProcessingOrchestratorRunResult;
};
export const retryWorkerRegistration = async ({ url, sourceUrl }: { url: string; sourceUrl?: string }) => {
  const response = await call('/registration/retry', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ url, sourceUrl }) });
  if (!response) return { triggered: false };
  if (!response.ok) throw new Error(await response.text().catch(() => '') || `Registration retry failed (${response.status})`);
  return { triggered: true, ...await response.json().catch(() => ({})) };
};
export const triggerProcessingOrchestrator = async () => (await runProcessingOrchestrator()).triggered;
export const triggerDeletionOrchestrator = async () => {
  const response = await call('/deletions/run', { method: 'POST' });
  if (!response) return false;
  if (!response.ok) throw new Error(await response.text().catch(() => '') || `Backend deletion queue failed (${response.status})`);
  return true;
};
