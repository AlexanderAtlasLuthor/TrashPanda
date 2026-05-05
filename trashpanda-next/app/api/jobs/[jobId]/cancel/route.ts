import { NextResponse } from "next/server";
import { adapterCancelJob } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function POST(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  try {
    const result = await adapterCancelJob(jobId);
    if (!result) {
      return NextResponse.json({ message: "Job not found" }, { status: 404 });
    }
    return NextResponse.json(result);
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unexpected error cancelling job.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
