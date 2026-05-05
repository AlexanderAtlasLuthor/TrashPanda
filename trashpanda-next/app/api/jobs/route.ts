import { NextRequest, NextResponse } from "next/server";
import { adapterGetJobList, adapterStartJob } from "@/lib/backend-adapter";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const raw = parseInt(url.searchParams.get("limit") ?? "20", 10);
    const limit = isNaN(raw) ? 20 : Math.min(Math.max(1, raw), 100);
    const list = await adapterGetJobList(limit);
    return NextResponse.json(list);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unexpected error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}

export async function POST(req: NextRequest) {
  try {
    const form = await req.formData();
    const file = form.get("file");
    if (!(file instanceof File)) {
      return NextResponse.json(
        { message: "Missing 'file' field in form data." },
        { status: 400 },
      );
    }

    // Basic guardrails. Real validation lives in Python.
    const name = file.name.toLowerCase();
    if (!name.endsWith(".csv") && !name.endsWith(".xlsx")) {
      return NextResponse.json(
        { message: "Only .csv and .xlsx files are accepted." },
        { status: 400 },
      );
    }
    // 100MB soft cap here; backend enforces real limit.
    if (file.size > 100 * 1024 * 1024) {
      return NextResponse.json(
        { message: "File exceeds 100 MB limit." },
        { status: 400 },
      );
    }

    // Optional config_path Form field. Only accept strings; silently
    // ignore other types (e.g. File posted under the wrong field). Empty
    // / whitespace strings are treated as absent so HomeDashboard's
    // `<UploadDropzone />` (which never sets config_path) keeps its
    // exact pre-V2.10.7 behavior.
    const rawConfigPath = form.get("config_path");
    const configPath =
      typeof rawConfigPath === "string" && rawConfigPath.trim().length > 0
        ? rawConfigPath.trim()
        : undefined;

    const result = await adapterStartJob(
      file,
      configPath ? { config_path: configPath } : undefined,
    );
    return NextResponse.json(result, { status: 201 });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unexpected upload error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
