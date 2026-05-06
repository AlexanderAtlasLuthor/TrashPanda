import { NextRequest } from "next/server";
import { adapterDownloadBatchBundle } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(
  _req: NextRequest,
  { params }: { params: { batchId: string } },
) {
  const upstream = await adapterDownloadBatchBundle(params.batchId);
  // Pass through the binary stream + the Content-Disposition header
  // so the browser saves the zip with the right filename.
  const headers = new Headers();
  const ct = upstream.headers.get("content-type");
  if (ct) headers.set("content-type", ct);
  const cd = upstream.headers.get("content-disposition");
  if (cd) headers.set("content-disposition", cd);
  return new Response(upstream.body, {
    status: upstream.status,
    headers,
  });
}
