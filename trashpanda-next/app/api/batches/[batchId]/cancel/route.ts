import { NextRequest, NextResponse } from "next/server";
import { adapterCancelBatch } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function POST(
  _req: NextRequest,
  { params }: { params: { batchId: string } },
) {
  try {
    const result = await adapterCancelBatch(params.batchId);
    return NextResponse.json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    // Backend returns 404 / 409 / 503 with JSON; we surface as 500
    // here for simplicity and let the UI display the message. The
    // BatchProgressPanel renders it inline in the error box.
    return NextResponse.json({ message }, { status: 500 });
  }
}
