import { NextResponse } from "next/server";
import {
  adapterRunOperatorPreflight,
  OperatorBackendUnavailableError,
} from "@/lib/backend-adapter";
import type { RunPreflightInput } from "@/lib/api";

export const runtime = "nodejs";

function errorResponse(err: unknown) {
  const message = err instanceof Error ? err.message : "Unexpected error.";
  const status =
    err instanceof OperatorBackendUnavailableError ? 503 : 500;
  return NextResponse.json({ message }, { status });
}

export async function POST(req: Request) {
  try {
    const body = (await req.json().catch(() => ({}))) as RunPreflightInput;
    const result = await adapterRunOperatorPreflight(body);
    return NextResponse.json(result);
  } catch (err) {
    return errorResponse(err);
  }
}
