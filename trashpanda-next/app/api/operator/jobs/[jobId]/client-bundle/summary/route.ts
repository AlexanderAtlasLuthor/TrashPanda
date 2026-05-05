import { NextResponse } from "next/server";
import { adapterGetClientBundleSummary } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  try {
    const summary = await adapterGetClientBundleSummary(jobId);
    return NextResponse.json(summary, {
      headers: { "cache-control": "no-store" },
    });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unable to load bundle summary.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
