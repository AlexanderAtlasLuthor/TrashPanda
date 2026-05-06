import { NextRequest, NextResponse } from "next/server";
import { adapterUploadBatch } from "@/lib/backend-adapter";

export const runtime = "nodejs";

function parseInt32(form: FormData, key: string): number | undefined {
  const raw = form.get(key);
  if (typeof raw !== "string" || raw.trim() === "") return undefined;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) ? n : undefined;
}

function parseBool(form: FormData, key: string): boolean | undefined {
  const raw = form.get(key);
  if (typeof raw !== "string") return undefined;
  const v = raw.trim().toLowerCase();
  if (v === "true" || v === "1") return true;
  if (v === "false" || v === "0") return false;
  return undefined;
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
    const name = file.name.toLowerCase();
    if (!name.endsWith(".csv") && !name.endsWith(".xlsx")) {
      return NextResponse.json(
        { message: "Only .csv and .xlsx files are accepted." },
        { status: 400 },
      );
    }
    // 200MB soft cap (batches are bigger than single jobs).
    if (file.size > 200 * 1024 * 1024) {
      return NextResponse.json(
        { message: "File exceeds 200 MB limit." },
        { status: 400 },
      );
    }

    const result = await adapterUploadBatch(file, {
      chunk_size: parseInt32(form, "chunk_size"),
      threshold_rows: parseInt32(form, "threshold_rows"),
      allow_partial: parseBool(form, "allow_partial"),
      cleanup: parseBool(form, "cleanup"),
    });
    return NextResponse.json(result, { status: 201 });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Unexpected upload error.";
    return NextResponse.json({ message }, { status: 500 });
  }
}
