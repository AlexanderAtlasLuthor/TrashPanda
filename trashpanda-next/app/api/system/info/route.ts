import { NextResponse } from "next/server";
import { adapterGetSystemInfo } from "@/lib/backend-adapter";

export const runtime = "nodejs";

/**
 * Surfaces backend deployment metadata to the operator UI:
 *
 *   - backend_label / deployment ("racknerd" / "vps" or "local")
 *   - adapter_mode ("proxy" via tunnel vs in-memory "mock")
 *   - operator_token_configured (true when the BFF can authenticate)
 *   - wall_clock_seconds, smtp_default_dry_run, auth_enabled
 *
 * Used by ``Topbar.tsx`` to render the "VPS via tunnel" badge so the
 * operator can never confuse a local-mock run with the real VPS run.
 */
export async function GET() {
  try {
    const info = await adapterGetSystemInfo();
    return NextResponse.json(info, {
      headers: { "cache-control": "no-store" },
    });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unable to fetch system info.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
