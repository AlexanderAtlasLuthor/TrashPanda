import { adapterDownloadClientBundle } from "@/lib/backend-adapter";

export const runtime = "nodejs";

/**
 * Streams the curated "Send to client" ZIP straight back to the
 * browser. Body, content-type and Content-Disposition all flow
 * through unchanged so the client picks up the same friendly
 * filename the backend chose
 * (``<input>_clean_<YYYY-MM-DD>.zip``).
 */
export async function GET(
  _req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  const upstream = await adapterDownloadClientBundle(jobId);

  // Forward the upstream response as-is. Don't parse, don't reconstruct
  // the filename, don't fall back to a different endpoint — the same
  // contract as the legacy operator client-package download.
  const headers = new Headers(upstream.headers);
  return new Response(upstream.body, {
    status: upstream.status,
    headers,
  });
}
