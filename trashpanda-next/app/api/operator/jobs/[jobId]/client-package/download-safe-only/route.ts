import { adapterDownloadOperatorClientPackageSafeOnly } from "@/lib/backend-adapter";

export const runtime = "nodejs";

// V2.10.8.3 — BFF route for the safe-only partial client-package
// download. Reads the operator override header from the inbound
// request and forwards it to the backend adapter unchanged. The
// adapter returns the raw backend Response (200 application/zip on
// the happy path, 409 application/json on any blocked gate); we
// surface that Response verbatim so Content-Disposition, the
// audience header, and the V2.10.8.3 advisory headers
// (X-TrashPanda-Delivery-Mode, X-TrashPanda-Ready-For-Client,
// X-TrashPanda-Ready-For-Client-Partial) all flow through.
//
// Distinct from /client-package/download — that is the full-delivery
// channel and ignores the override header. This route MUST NOT fall
// back to it.
export async function GET(
  req: Request,
  { params }: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await params;
  const overrideHeader =
    req.headers.get("x-trashpanda-operator-override") ?? "";
  return adapterDownloadOperatorClientPackageSafeOnly(jobId, overrideHeader);
}
