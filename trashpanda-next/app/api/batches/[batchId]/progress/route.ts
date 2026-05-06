import { NextRequest, NextResponse } from "next/server";
import { adapterGetBatchProgress } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: NextRequest,
  { params }: { params: { batchId: string } },
) {
  try {
    const progress = await adapterGetBatchProgress(params.batchId);
    if (progress === null) {
      return NextResponse.json({ message: "Batch not found" }, { status: 404 });
    }
    return NextResponse.json(progress);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
