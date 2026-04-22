import {
  adapterGetReviewDecisions,
  adapterSaveReviewDecisions,
} from "@/lib/backend-adapter";
import type { ReviewDecision } from "@/lib/types";
import { NextResponse } from "next/server";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  try {
    const { jobId } = await params;
    const data = await adapterGetReviewDecisions(jobId);
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}

export async function POST(
  req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  try {
    const { jobId } = await params;
    const body = await req.json().catch(() => ({}));
    const decisions = (body?.decisions ?? {}) as Record<string, ReviewDecision>;
    const data = await adapterSaveReviewDecisions(jobId, decisions);
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
