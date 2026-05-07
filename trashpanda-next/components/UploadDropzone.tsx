"use client";

import { useRef, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { ApiError, uploadBatch, uploadFile } from "@/lib/api";
import styles from "./UploadDropzone.module.css";

const ACCEPTED_EXTENSIONS = [".csv", ".xlsx"] as const;
const MAX_SIZE = 200 * 1024 * 1024;

// V2.10.18 — files this large or larger are auto-routed to the
// /batches/upload path (auto-chunked, OOM-safe). 7 MB is a
// conservative heuristic: ~50k rows at ~150 B/row. The backend
// re-counts rows server-side and falls through to the single-job
// path if the actual count is below threshold_rows, so the
// classification is always correct.
const BATCH_THRESHOLD_BYTES = 7 * 1024 * 1024;

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function validateFile(file: File): string | null {
  const name = file.name.toLowerCase();
  const okExt = ACCEPTED_EXTENSIONS.some((e) => name.endsWith(e));
  if (!okExt) {
    return `Unsupported format. Accepts ${ACCEPTED_EXTENSIONS.join(", ")}.`;
  }
  if (file.size > MAX_SIZE) {
    return `File exceeds ${formatBytes(MAX_SIZE)} limit.`;
  }
  if (file.size === 0) {
    return "File is empty.";
  }
  return null;
}

interface UploadDropzoneProps {
  /**
   * Optional override for the post-upload redirect path. When omitted,
   * the dropzone routes to `/results/{jobId}` (the client-facing
   * default). Operator surfaces pass a function that returns
   * `/operator/jobs/{jobId}` so the same upload UX can drop the
   * operator straight into the Package + Gate page.
   */
  redirectTo?: (jobId: string) => string;
  /** Optional override for the call-to-action label on the file card. */
  ctaLabel?: string;
  /**
   * Optional config_path forwarded to the backend's `POST /jobs`
   * (multipart Form field). Only attached when non-empty after trim,
   * so HomeDashboard's prop-less `<UploadDropzone />` keeps its
   * pre-V2.10.7 behavior identical. Operator launches set this to the
   * config_path of the preflight that gated the launch.
   */
  configPath?: string | null;
}

export function UploadDropzone({
  redirectTo,
  ctaLabel,
  configPath,
}: UploadDropzoneProps = {}) {
  const router = useRouter();
  const inputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleFile = useCallback((next: File | null) => {
    setError(null);
    if (!next) {
      setFile(null);
      return;
    }
    const validation = validateFile(next);
    if (validation) {
      setError(validation);
      setFile(null);
      return;
    }
    setFile(next);
  }, []);

  const onDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragging(false);
    const dropped = e.dataTransfer.files?.[0];
    if (dropped) handleFile(dropped);
  };

  const onDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragging(true);
  };

  const onDragLeave = () => setDragging(false);

  const onPick = () => inputRef.current?.click();

  const onInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleFile(e.target.files?.[0] ?? null);
    // reset so selecting the same file again re-triggers change
    e.target.value = "";
  };

  const onSubmit = async () => {
    if (!file || submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      // Large files take the auto-chunked batch path. The backend
      // re-counts rows and falls through to a single-job run when
      // the file is actually small (e.g. wide CSV columns); the
      // routing decision here is just the heuristic to avoid the
      // OOM-prone single-job path for clearly-large inputs.
      if (file.size >= BATCH_THRESHOLD_BYTES) {
        const { batch_id } = await uploadBatch(file);
        router.push(`/batches/${encodeURIComponent(batch_id)}`);
        return;
      }
      const trimmedConfig = configPath?.trim();
      const { job_id } = await (trimmedConfig
        ? uploadFile(file, { config_path: trimmedConfig })
        : uploadFile(file));
      const target = redirectTo
        ? redirectTo(job_id)
        : `/results/${encodeURIComponent(job_id)}`;
      router.push(target);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.message);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError("Upload failed. Try again.");
      }
      setSubmitting(false);
    }
  };

  const onKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      onPick();
    }
  };

  return (
    <div className={styles.wrap}>
      {!file ? (
        <div
          role="button"
          tabIndex={0}
          className={[styles.dropzone, dragging && styles.dropzoneDragging]
            .filter(Boolean)
            .join(" ")}
          onClick={onPick}
          onKeyDown={onKeyDown}
          onDrop={onDrop}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          aria-label="Upload CSV or XLSX file"
        >
          <div className={styles.text}>
            <div className={styles.title}>
              FEED THE <span className={styles.hl}>PANDA</span>
            </div>
            <div className={styles.sub}>
              Drop <span className={styles.key}>.csv</span>
              <span className={styles.key}>.xlsx</span> &nbsp;·&nbsp; up to
              200 MB &nbsp;·&nbsp; large files auto-batch
            </div>
          </div>
          <button
            className={styles.btnPrimary}
            onClick={(e) => {
              e.stopPropagation();
              onPick();
            }}
            type="button"
          >
            SELECT FILE
          </button>
          <input
            ref={inputRef}
            type="file"
            accept={ACCEPTED_EXTENSIONS.join(",")}
            onChange={onInputChange}
            className={styles.hidden}
            aria-hidden
          />
        </div>
      ) : (
        <div className={styles.fileCard}>
          <div className={styles.fileInfo}>
            <div className={styles.fileName}>{file.name}</div>
            <div className={styles.fileSize}>
              <span className={styles.fileBadge}>
                {file.name.toLowerCase().endsWith(".xlsx") ? "XLSX" : "CSV"}
              </span>
              {formatBytes(file.size)} · ready to ingest
            </div>
          </div>
          <button
            className={styles.btnGhost}
            onClick={() => handleFile(null)}
            disabled={submitting}
            type="button"
          >
            REMOVE
          </button>
          <button
            className={styles.btnPrimary}
            onClick={onSubmit}
            disabled={submitting}
            type="button"
          >
            {submitting
              ? "STARTING..."
              : file.size >= BATCH_THRESHOLD_BYTES
                ? "START BATCH"
                : (ctaLabel ?? "START CLEANING")}
          </button>
        </div>
      )}

      {file && file.size >= BATCH_THRESHOLD_BYTES && (
        <div className={styles.batchBanner}>
          <strong>Large file detected ({formatBytes(file.size)}).</strong>{" "}
          This will be auto-split into chunks and processed as a batch
          (OOM-safe). You'll see per-chunk progress on the next page.
        </div>
      )}

      {error && <div className={styles.inlineError}>⚠ {error}</div>}
    </div>
  );
}
