import { NextRequest, NextResponse } from "next/server";
import { adapterListBatches } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(_req: NextRequest) {
  try {
    const list = await adapterListBatches();
    return NextResponse.json(list);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
