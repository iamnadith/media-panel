import { NextResponse } from 'next/server';
import { isSessionAuthorized } from '@/auth/api';
import { getBackendOrchestratorIntegration } from '@/processing/cloudflare-worker-integration';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

const STATUS_TIMEOUT_MS = 20_000;

const isTimeoutError = (error: unknown) =>
  error instanceof Error && (
    error.name === 'TimeoutError' ||
    error.name === 'AbortError' ||
    /timed? out|timeout|aborted/i.test(error.message)
  );

export async function GET() {
  if (!await isSessionAuthorized('edit')) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const integration = await getBackendOrchestratorIntegration();
  if (!integration) {
    return NextResponse.json({ configured: false });
  }
  try {
    const response = await fetch(
      `${integration.baseUrl.replace(/\/+$/, '')}/status`,
      {
        headers: {
          Authorization: `Bearer ${integration.sharedSecret}`,
        },
        cache: 'no-store',
        signal: AbortSignal.timeout(STATUS_TIMEOUT_MS),
      },
    );
    const data = await response.json().catch(() => ({}));
    return NextResponse.json(
      response.ok
        ? {
          configured: true,
          connected: true,
          checkedAt: new Date().toISOString(),
          ...data,
        }
        : {
          configured: true,
          connected: false,
          checkedAt: new Date().toISOString(),
          errorCode: 'upstream',
          error: data.error || `Backend Orchestrator returned ${response.status}`,
        },
      { status: response.ok ? 200 : 502 },
    );
  } catch (error) {
    const timedOut = isTimeoutError(error);
    return NextResponse.json({
      configured: true,
      connected: false,
      checkedAt: new Date().toISOString(),
      errorCode: timedOut ? 'timeout' : 'connection',
      error: timedOut
        ? 'Status check timed out'
        : error instanceof Error ? error.message : 'Connection failed',
    }, { status: 502 });
  }
}
