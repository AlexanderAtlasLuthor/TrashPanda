import { NextRequest, NextResponse } from "next/server";
import { adapterGetBatchStatusDoc } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: NextRequest,
  { params }: { params: { batchId: string } },
) {
  try {
    const doc = await adapterGetBatchStatusDoc(params.batchId);
    if (doc === null) {
      return NextResponse.json({ message: "Batch not found" }, { status: 404 });
    }
    return NextResponse.json(doc);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
