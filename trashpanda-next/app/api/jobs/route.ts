import { NextRequest, NextResponse } from "next/server";
import { adapterStartJob } from "@/lib/backend-adapter";

export const runtime = "nodejs";

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

    const result = await adapterStartJob(file);
    return NextResponse.json(result, { status: 201 });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unexpected upload error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
