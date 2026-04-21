/**
 * Mock adapter. Simulates the Python backend in-memory so the frontend
 * works end-to-end before the HTTP layer is built.
 *
 * When TRASHPANDA_BACKEND_URL is set, the route handlers bypass this and
 * proxy directly. This file is only used in dev.
 */

import type { JobList, JobLogs, JobResult, ReviewQueue, ReviewEmail, ReviewReason, ReviewConfidence } from "./types";

interface StoredJob {
  result: JobResult;
  /** epoch ms when the job was created */
  createdAt: number;
}

// Module-level map. In Next.js dev, module state persists across requests
// within the same Node process. Not safe for multi-instance production,
// but this adapter is only a dev stub.
const jobs = new Map<string, StoredJob>();

function msAgo(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

const _SEED: Array<{
  id: string;
  filename: string;
  status: "completed" | "failed";
  daysAgo: number;
  rows?: number;
}> = [
  { id: "job_hist_001", filename: "newsletter_Q1_2025.csv",  status: "completed", daysAgo: 3,  rows: 48200  },
  { id: "job_hist_002", filename: "crm_export_march.xlsx",   status: "completed", daysAgo: 7,  rows: 12840  },
  { id: "job_hist_003", filename: "leads_WY_dataset.csv",    status: "failed",    daysAgo: 14                },
  { id: "job_hist_004", filename: "contacts_batch_12.csv",   status: "completed", daysAgo: 30, rows: 91500  },
];

for (const seed of _SEED) {
  const startMs = msAgo(seed.daysAgo);
  const startedAt = new Date(startMs).toISOString();
  const finishedAt = new Date(startMs + 29_700).toISOString();
  jobs.set(seed.id, {
    createdAt: startMs,
    result: {
      job_id: seed.id,
      status: seed.status,
      input_filename: seed.filename,
      run_dir: `/runs/${seed.id}`,
      started_at: startedAt,
      finished_at: finishedAt,
      summary: seed.status === "completed" && seed.rows ? {
        total_input_rows: seed.rows,
        total_valid: Math.floor(seed.rows * 0.65),
        total_review: Math.floor(seed.rows * 0.08),
        total_invalid_or_bounce_risk: Math.floor(seed.rows * 0.27),
        duplicates_removed: Math.floor(seed.rows * 0.06),
        typo_corrections: Math.floor(seed.rows * 0.035),
      } : null,
      artifacts: seed.status === "completed" ? {
        run_dir: `/runs/${seed.id}`,
        client_outputs: {
          valid_emails: "valid_emails.xlsx",
          review_emails: "review_emails.xlsx",
          invalid_or_bounce_risk: "invalid_or_bounce_risk.xlsx",
          summary_report: "summary_report.xlsx",
        },
        technical_csvs: undefined,
        reports: undefined,
      } : null,
      error: seed.status === "failed" ? {
        error_type: "ParseError",
        message: "Could not determine email column from uploaded file.",
      } : null,
    },
  });
}

function genId(): string {
  // readable, short, no external deps
  const ts = Date.now().toString(36);
  const rnd = Math.random().toString(36).slice(2, 8);
  return `job_${ts}_${rnd}`;
}

export function createMockJob(filename: string): string {
  const jobId = genId();
  const now = new Date().toISOString();
  jobs.set(jobId, {
    createdAt: Date.now(),
    result: {
      job_id: jobId,
      status: "queued",
      input_filename: filename,
      run_dir: `/runs/${jobId}`,
      started_at: now,
      finished_at: null,
      summary: null,
      artifacts: null,
      error: null,
    },
  });
  return jobId;
}

/**
 * Return the current state, advancing the mock state machine based on age.
 *   0 - 2s   : queued
 *   2 - 7s   : running
 *   > 7s     : completed
 * Rolls a 5% chance of "failed" on first completion read (deterministic per job).
 */
export function getMockJob(jobId: string): JobResult | null {
  const stored = jobs.get(jobId);
  if (!stored) return null;

  const ageMs = Date.now() - stored.createdAt;
  const current = stored.result;

  // Terminal states don't advance.
  if (current.status === "completed" || current.status === "failed") {
    return current;
  }

  if (ageMs < 2000) {
    return { ...current, status: "queued" };
  }
  if (ageMs < 7000) {
    const next: JobResult = { ...current, status: "running" };
    stored.result = next;
    return next;
  }

  // Transition to terminal. Deterministic: hash job id to a fixed outcome.
  const roll = hashString(jobId) % 20;
  if (roll === 0) {
    const failed: JobResult = {
      ...current,
      status: "failed",
      finished_at: new Date().toISOString(),
      error: {
        error_type: "ParseError",
        message:
          "Could not parse uploaded file. Expected CSV or XLSX with at least one email column.",
      },
    };
    stored.result = failed;
    return failed;
  }

  const completed: JobResult = {
    ...current,
    status: "completed",
    finished_at: new Date().toISOString(),
    summary: {
      total_input_rows: 114398,
      total_valid: 75112,
      total_review: 9284,
      total_invalid_or_bounce_risk: 30002,
      duplicates_removed: 7284,
      typo_corrections: 4112,
      disposable_emails: 842,
      placeholder_or_fake_emails: 317,
      role_based_emails: 1903,
    },
    artifacts: {
      run_dir: current.run_dir,
      client_outputs: {
        valid_emails: "valid_emails.xlsx",
        review_emails: "review_emails.xlsx",
        invalid_or_bounce_risk: "invalid_or_bounce_risk.xlsx",
        summary_report: "summary_report.xlsx",
      },
      // Keys match app/api_boundary.py _TECHNICAL_CSV_NAMES and _REPORT_NAMES
      technical_csvs: {
        clean_high_confidence: "clean_high_confidence.csv",
        review_medium_confidence: "review_medium_confidence.csv",
        removed_invalid: "removed_invalid.csv",
      },
      reports: {
        processing_report_json: "processing_report.json",
        processing_report_csv: "processing_report.csv",
        domain_summary: "domain_summary.csv",
        typo_corrections: "typo_corrections.csv",
        duplicate_summary: "duplicate_summary.csv",
      },
    },
  };
  stored.result = completed;
  return completed;
}

function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

const _MOCK_LOG_SEQUENCE = [
  "2024-01-01 00:00:00 | INFO | [TIMING] Pipeline START",
  "2024-01-01 00:00:00 | INFO | Starting email cleaner ingestion run.",
  "2024-01-01 00:00:00 | INFO | Discovered supported files: 1",
  "2024-01-01 00:00:00 | INFO | Loaded typo map with 2184 entries.",
  "2024-01-01 00:00:00 | INFO | Processing input.csv",
  "2024-01-01 00:00:01 | INFO | [TIMING] stage=HeaderNormalizationStage chunk=0 rows=50000 elapsed=0.012s",
  "2024-01-01 00:00:01 | INFO | [TIMING] stage=EmailSyntaxValidationStage chunk=0 rows=50000 elapsed=0.341s",
  "2024-01-01 00:00:01 | INFO | [TIMING] stage=TypoCorrectionStage chunk=0 rows=50000 elapsed=0.082s",
  "2024-01-01 00:00:02 | INFO | [TIMING] stage=DNSEnrichmentStage chunk=0 rows=50000 elapsed=14.231s",
  "2024-01-01 00:00:16 | INFO | [TIMING] stage=ScoringStage chunk=0 rows=50000 elapsed=0.421s",
  "2024-01-01 00:00:16 | INFO | [TIMING] stage=DedupeStage chunk=0 rows=50000 elapsed=0.218s",
  "2024-01-01 00:00:17 | INFO | [TIMING] stage=StagingPersistenceStage chunk=0 rows=50000 elapsed=1.123s",
  "2024-01-01 00:00:17 | INFO | [TIMING] chunk=0 rows=50000 elapsed=18.4s dns_new=4821 dns_cached=0",
  "2024-01-01 00:00:17 | INFO | Processed chunk 0 from input.csv | rows=50000 valid_emails=44312",
  "2024-01-01 00:00:18 | INFO | [TIMING] stage=DNSEnrichmentStage chunk=1 rows=50000 elapsed=5.812s",
  "2024-01-01 00:00:24 | INFO | [TIMING] stage=StagingPersistenceStage chunk=1 rows=50000 elapsed=1.089s",
  "2024-01-01 00:00:24 | INFO | [TIMING] chunk=1 rows=50000 elapsed=8.2s dns_new=312 dns_cached=4509",
  "2024-01-01 00:00:24 | INFO | [TIMING] chunk=2 rows=14398 elapsed=3.1s dns_new=48 dns_cached=5085",
  "2024-01-01 00:00:24 | INFO | Pipeline run complete | files=1 chunks=3 rows=114398",
  "2024-01-01 00:00:24 | INFO | [TIMING] Materialize START",
  "2024-01-01 00:00:24 | INFO | [TIMING] Materialize iter_rows START (staging→CSV)",
  "2024-01-01 00:00:27 | INFO | [TIMING] Materialize iter_rows DONE elapsed=3.2s rows=114398",
  "2024-01-01 00:00:27 | INFO | [TIMING] Materialize reports DONE elapsed=0.4s",
  "2024-01-01 00:00:27 | INFO | [TIMING] Materialize xlsx START",
  "2024-01-01 00:00:31 | INFO | [TIMING] Materialize xlsx DONE elapsed=4.1s",
  "2024-01-01 00:00:31 | INFO | [TIMING] Materialize DONE elapsed=7.7s",
  "2024-01-01 00:00:31 | INFO | [TIMING] Pipeline DONE elapsed=29.7s",
];

/**
 * Return mock log lines that progressively reveal over the simulated run lifetime.
 * Lines are revealed proportionally based on elapsed time so the panel looks alive.
 */
export function getMockJobLogs(jobId: string, limit: number): JobLogs {
  const stored = jobs.get(jobId);
  if (!stored) return { job_id: jobId, lines: [] };

  const ageMs = Date.now() - stored.createdAt;
  // Reveal all lines over 12 seconds of simulated run time.
  const fraction = Math.min(1, ageMs / 12000);
  const count = Math.max(0, Math.floor(_MOCK_LOG_SEQUENCE.length * fraction));
  const lines = _MOCK_LOG_SEQUENCE.slice(0, count).slice(-limit);
  return { job_id: jobId, lines };
}

export function getMockJobList(limit: number): JobList {
  const sortedIds = Array.from(jobs.entries())
    .sort((a, b) => b[1].createdAt - a[1].createdAt)
    .slice(0, limit)
    .map(([id]) => id);

  const items = sortedIds
    .map((id) => getMockJob(id))
    .filter((r): r is JobResult => r !== null)
    .map((r) => ({
      job_id: r.job_id,
      input_filename: r.input_filename,
      status: r.status,
      started_at: r.started_at,
      finished_at: r.finished_at,
    }));
  return { jobs: items };
}

// ── Review queue mock generator ──────────────────────────────────────────

const _ROLES = ["info","admin","contact","sales","support","hello","billing",
  "hr","dev","help","office","team","press","webmaster","noreply","mail",
  "marketing","newsletter","careers","jobs","data","it","security","ops"];
const _CATCHALL = ["acme-enterprise.io","nexus-solutions.co","vertex-group.biz",
  "alphatech.ventures","globalcorp.co","betaholdings.io","omnisystems.biz",
  "stellargroup.co","apexinc.io","meridian.co","paradigm-labs.biz",
  "axiomtech.io","crescendo.ventures","synergy-co.biz","horizon-ent.io"];
const _NOSMTP = ["freelance.design","creative-studio.biz","digitalcraft.io",
  "webpilot.co","theagency.design","projecthive.biz","buildersco.io",
  "crafted.co","pixelsmith.biz","devshop.io"];
const _REGULAR = ["company.com","techfirm.net","startup.io","agency.co",
  "webgroup.biz","digitalteam.net","cloudsvc.io","appco.net"];
const _FIRST = ["alex","sarah","mike","emily","rob","jessica","david",
  "jennifer","carlos","anna","pierre","liu","ahmed","priya","tom",
  "jordan","chris","taylor","morgan","sam","riley","casey","dana","blake"];
const _LAST = ["smith","jones","garcia","mueller","dupont","chen","kowalski",
  "hernandez","kim","patel","wilson","brown","davis","martin","thompson",
  "white","jackson","harris","lee","nguyen","clark","rodriguez","walker"];

function _seedRand(str: string): () => number {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  let s = h;
  return () => {
    s ^= s << 13; s ^= s >>> 17; s ^= s << 5;
    return (s >>> 0) / 4294967296;
  };
}

function _pick<T>(arr: T[], r: () => number): T {
  return arr[Math.floor(r() * arr.length)];
}

export function getMockReviewEmails(jobId: string): ReviewQueue {
  const rand = _seedRand(jobId);
  const emails: ReviewEmail[] = [];
  for (let i = 0; i < 200; i++) {
    const roll = rand();
    let reason: ReviewReason;
    let email: string;
    let domain: string;
    if (roll < 0.44) {
      reason = "catch-all";
      domain = _pick(_CATCHALL, rand);
      email = `${_pick(_FIRST, rand)}.${_pick(_LAST, rand)}@${domain}`;
    } else if (roll < 0.79) {
      reason = "role-based";
      domain = _pick(_REGULAR, rand);
      email = `${_pick(_ROLES, rand)}@${domain}`;
    } else {
      reason = "no-smtp";
      domain = _pick(_NOSMTP, rand);
      email = `${_pick(_FIRST, rand)}@${domain}`;
    }
    const confidence: ReviewConfidence = rand() < 0.4 ? "low" : "medium";
    emails.push({ id: `${jobId}_r${i}`, email, domain, reason, confidence });
  }
  return { job_id: jobId, total: 200, emails };
}

/**
 * Produce an empty "downloaded" file so the download buttons actually
 * resolve in the mock adapter. Real backend will stream the real xlsx.
 */
export function mockArtifactResponse(
  jobId: string,
  key: string,
): { filename: string; body: string } | null {
  const job = jobs.get(jobId);
  if (!job) return null;
  const filename = job.result.artifacts?.client_outputs?.[key];
  if (!filename) return null;
  const body = `TrashPanda mock artifact\nJob: ${jobId}\nKey: ${key}\nFilename: ${filename}\n\nThis is a dev placeholder. The real backend will stream the .xlsx.`;
  return { filename, body };
}
