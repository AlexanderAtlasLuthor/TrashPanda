import { NextResponse } from "next/server";
import {
  adapterSetOperatorRetryQueueAutoRetry,
  OperatorBackendUnavailableError,
} from "@/lib/backend-adapter";

export const runtime = "nodejs";

function errorResponse(err: unknown) {
  const message = err instanceof Error ? err.message : "Unexpected error.";
  const status =
    err instanceof OperatorBackendUnavailableError ? 503 : 500;
  return NextResponse.json({ message }, { status });
}

export async function PATCH(
  req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  try {
    const { jobId } = await params;
    const { searchParams } = new URL(req.url);
    const enabled = searchParams.get("enabled") === "true";
    const result = await adapterSetOperatorRetryQueueAutoRetry(jobId, enabled);
    return NextResponse.json(result);
  } catch (err) {
    return errorResponse(err);
  }
}
