/**
 * Contract types. These MUST stay in sync with the Python backend's
 * JobResult serialization (e.g. pydantic model or dataclass -> dict).
 *
 * Source of truth: `run_cleaning_job(...) -> JobResult` in the Python layer.
 * If the backend contract changes, update this file first, then ripple through UI.
 */

export type JobStatus = "queued" | "running" | "completed" | "failed";

export interface JobError {
  error_type: string;
  message: string;
  details?: Record<string, unknown> | null;
}

export interface JobSummary {
  total_input_rows?: number | null;
  total_valid?: number | null;
  total_review?: number | null;
  total_invalid_or_bounce_risk?: number | null;
  duplicates_removed?: number | null;
  typo_corrections?: number | null;
  disposable_emails?: number | null;
  placeholder_or_fake_emails?: number | null;
  role_based_emails?: number | null;
}

export interface JobArtifacts {
  run_dir?: string | null;
  technical_csvs?: Record<string, string | null>;
  client_outputs?: Record<string, string | null>;
  reports?: Record<string, string | null>;
}

export interface JobResult {
  job_id: string;
  status: JobStatus;
  input_filename?: string | null;
  run_dir?: string | null;
  summary?: JobSummary | null;
  artifacts?: JobArtifacts | null;
  error?: JobError | null;
  started_at?: string | null;
  finished_at?: string | null;
}

export interface JobLogs {
  job_id: string;
  lines: string[];
}

export interface JobListItem {
  job_id: string;
  input_filename?: string | null;
  status: JobStatus;
  started_at?: string | null;
  finished_at?: string | null;
}

export interface JobList {
  jobs: JobListItem[];
}

export type ReviewReason = "catch-all" | "role-based" | "no-smtp";
export type ReviewConfidence = "low" | "medium";
export type ReviewDecision = "approved" | "removed";

export interface ReviewEmail {
  id: string;
  email: string;
  domain: string;
  reason: ReviewReason;
  confidence: ReviewConfidence;
}

export interface ReviewQueue {
  job_id: string;
  total: number;
  emails: ReviewEmail[];
}

/**
 * Known client output filenames the backend is guaranteed to produce on success.
 * Keys match artifact keys the backend returns; labels are UI-friendly.
 */
export const CLIENT_OUTPUT_MANIFEST: ReadonlyArray<{
  key: string;
  filename: string;
  label: string;
  description: string;
  severity: "ok" | "warn" | "bad" | "info";
}> = [
  {
    key: "valid_emails",
    filename: "valid_emails.xlsx",
    label: "Ready to send",
    description: "Safe to use in your campaigns.",
    severity: "ok",
  },
  {
    key: "review_emails",
    filename: "review_emails.xlsx",
    label: "Needs attention",
    description: "May require manual review before sending.",
    severity: "warn",
  },
  {
    key: "invalid_or_bounce_risk",
    filename: "invalid_or_bounce_risk.xlsx",
    label: "Do not use",
    description: "High risk of bounce or invalid address.",
    severity: "bad",
  },
  {
    key: "summary_report",
    filename: "summary_report.xlsx",
    label: "Summary report",
    description: "Full pipeline breakdown and per-stage counts.",
    severity: "info",
  },
];
