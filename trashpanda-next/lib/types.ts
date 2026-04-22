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

export interface ReviewEmailFlags {
  role_based?: boolean;
  catch_all?: boolean;
  smtp_unverified?: boolean;
  typo_corrected?: boolean;
  domain_mismatch?: boolean;
}

export interface ReviewEmail {
  id: string;
  email: string;
  domain: string;
  reason: ReviewReason;
  confidence: ReviewConfidence;
  classification_bucket?: string;
  friendly_reason?: string;
  risk?: string;
  recommended_action?: string;
  flags?: ReviewEmailFlags;

  // ── V2 Deliverability Intelligence (all optional; absent on legacy runs).
  bucket_v2?: string;
  bucket_label?: string;
  confidence_v2?: number;
  confidence_tier?: "high" | "medium" | "low";
  final_action?: string;
  final_action_label?: string;
  decision_reason?: string;
  decision_note?: string;
  decision_confidence?: number;
  deliverability_probability?: number;
  deliverability_label?: string;
  deliverability_factors?: string;
  human_reason?: string;
  human_risk?: string;
  human_recommendation?: string;
  historical_label?: string;
  historical_label_friendly?: string;
  confidence_adjustment_applied?: boolean;
  possible_catch_all?: boolean;
  catch_all_confidence?: number;
  catch_all_reason?: string;
  review_subclass?: string;
  smtp_tested?: boolean;
  smtp_confirmed_valid?: boolean;
  smtp_suspicious?: boolean;
  smtp_result?: string;
  smtp_code?: string;
  smtp_confidence?: number;
  reason_codes_v2?: string;
}

export interface ReviewQueue {
  job_id: string;
  total: number;
  emails: ReviewEmail[];
}

export interface ReviewDecisions {
  job_id: string;
  decisions: Record<string, ReviewDecision>;
}

export interface TypoCorrection {
  original: string;
  corrected: string;
  email: string;
}

export interface TypoCorrections {
  job_id: string;
  total: number;
  corrections: TypoCorrection[];
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
  {
    key: "approved_original_format",
    filename: "approved_original_format.xlsx",
    label: "Approved records (original format)",
    description: "Approved rows with the exact column layout from your original input file.",
    severity: "ok",
  },
];

// ── V2 Deliverability Intelligence payload ─────────────────────────────────

export interface InsightRow extends ReviewEmail {
  source: "valid" | "review" | "invalid";
  reason_codes?: string;
}

export interface InsightDomain {
  domain: string;
  count: number;
  avg_deliverability: number | null;
  historical_label: string | null;
  catch_all_count: number;
  smtp_suspicious_count: number;
  valid: number;
  review: number;
  invalid: number;
}

export interface InsightsResponse {
  job_id: string;
  v2_available: boolean;
  totals: { all: number; valid: number; review: number; invalid: number };
  confidence_tiers: { high: number; medium: number; low: number; unknown: number };
  final_actions: Record<string, number>;
  catch_all_count: number;
  smtp_tested_count: number;
  smtp_suspicious_count: number;
  domain_intelligence: {
    reliable: InsightDomain[];
    risky: InsightDomain[];
    unstable: InsightDomain[];
    catch_all_suspected: InsightDomain[];
  };
  rows: InsightRow[];
}
