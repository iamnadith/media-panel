import { NextRequest, NextResponse } from 'next/server';
import { isSessionAuthorized } from '@/auth/api';
import { getBackendOrchestratorIntegration } from '@/processing/cloudflare-worker-integration';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

export async function GET(request: NextRequest) {
  if (!await isSessionAuthorized('edit')) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const integration = await getBackendOrchestratorIntegration();
  if (!integration) {
    return NextResponse.json({ configured: false, logs: [] });
  }

  const rawLimit = Number(request.nextUrl.searchParams.get('limit') || 200);
  const limit = Number.isFinite(rawLimit)
    ? Math.min(Math.max(Math.round(rawLimit), 1), 500)
    : 200;

  try {
    const response = await fetch(
      `${integration.baseUrl.replace(/\/+$/, '')}/logs?limit=${limit}`,
      {
        headers: {
          Authorization: `Bearer ${integration.sharedSecret}`,
        },
        cache: 'no-store',
        signal: AbortSignal.timeout(20_000),
      },
    );
    const data = await response.json().catch(() => ({}));
    return NextResponse.json(
      response.ok
        ? { configured: true, connected: true, ...data }
        : {
          configured: true,
          connected: false,
          error: data.error || `Backend Orchestrator returned ${response.status}`,
          logs: [],
        },
      { status: response.ok ? 200 : 502 },
    );
  } catch (error) {
    const timedOut = error instanceof Error && (
      error.name === 'TimeoutError' ||
      error.name === 'AbortError' ||
      /timed? out|timeout|aborted/i.test(error.message)
    );
    return NextResponse.json({
      configured: true,
      connected: false,
      error: timedOut
        ? 'Activity log request timed out'
        : error instanceof Error ? error.message : 'Connection failed',
      logs: [],
    }, { status: 502 });
  }
}
