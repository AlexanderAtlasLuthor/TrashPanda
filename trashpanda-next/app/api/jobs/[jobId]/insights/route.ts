import { NextResponse } from "next/server";
import { adapterGetJobInsights } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  try {
    const data = await adapterGetJobInsights(jobId);
    return NextResponse.json(data);
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unexpected error fetching insights.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
