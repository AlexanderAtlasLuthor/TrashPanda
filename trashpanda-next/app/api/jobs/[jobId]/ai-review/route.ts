import { adapterAIReviewSuggestions, AIDisabledError } from "@/lib/backend-adapter";
import { NextResponse } from "next/server";

export const runtime = "nodejs";

export async function POST(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  try {
    const { jobId } = await params;
    const data = await adapterAIReviewSuggestions(jobId);
    return NextResponse.json(data);
  } catch (err) {
    if (err instanceof AIDisabledError) {
      return NextResponse.json({ message: err.message }, { status: 503 });
    }
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
