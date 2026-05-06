import { NextResponse } from "next/server";
import {
  adapterSetOperatorPilotSendConfig,
  OperatorBackendUnavailableError,
} from "@/lib/backend-adapter";
import type { PilotSendConfigInput } from "@/lib/api";

export const runtime = "nodejs";

function errorResponse(err: unknown) {
  const message = err instanceof Error ? err.message : "Unexpected error.";
  const status =
    err instanceof OperatorBackendUnavailableError ? 503 : 500;
  return NextResponse.json({ message }, { status });
}

export async function PUT(
  req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  try {
    const { jobId } = await params;
    const body = (await req.json().catch(() => ({}))) as PilotSendConfigInput;
    const result = await adapterSetOperatorPilotSendConfig(jobId, body);
    return NextResponse.json(result);
  } catch (err) {
    return errorResponse(err);
  }
}
