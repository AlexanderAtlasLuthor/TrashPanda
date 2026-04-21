import { NextResponse } from "next/server";
import { adapterGetJob } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  try {
    const result = await adapterGetJob(jobId);
    if (!result) {
      return NextResponse.json({ message: "Job not found" }, { status: 404 });
    }
    return NextResponse.json(result);
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unexpected error fetching job.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
