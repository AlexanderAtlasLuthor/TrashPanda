import type { ReviewActionBreakdown, ReviewBreakdown } from "@/lib/api";
import {
  RESULTS_COPY,
  type ReviewActionKey,
  type ReviewSubdivisionKey,
} from "@/lib/copy";
import type { JobSummary } from "@/lib/types";
import styles from "./ClassificationBreakdown.module.css";

interface Props {
  summary: JobSummary | null | undefined;
  reviewBreakdown?: ReviewBreakdown | null;
  /**
   * V2.10.10.b — action-oriented review breakdown ("what should I do
   * with this row"). When present, takes precedence over the
   * decision-reason ``reviewBreakdown`` because it tells the operator
   * *what to do*, not *why the engine routed the row*.
   */
  reviewActionBreakdown?: ReviewActionBreakdown | null;
}

// V2.10.10 — render the review subdivisions in a stable order so a
// row that disappears in a later run doesn't reshuffle the rest. The
// ordering matches "rescatability" (most rescatable first → most
// dangerous last) so the operator's eye lands on cold-start B2B and
// drifts down to high-risk-domain.
const REVIEW_SUBDIVISION_ORDER: readonly ReviewSubdivisionKey[] = [
  "review_cold_start_b2b",
  "review_smtp_inconclusive",
  "review_medium_probability",
  "review_catch_all",
  "review_domain_high_risk",
];

// V2.10.10.b — action-oriented order. ``second_pass_candidates`` is
// surfaced separately below the per-action rows because it overlaps
// with ready_probable + low_risk + timeout_retry — listing it inline
// would double-count.
//
// V2.10.11 — ``review_ready_probable`` (Tier 2) leads the list
// because it is the most-rescatable cohort (probability ≥ 0.70,
// almost-confirmed) and a customer scanning the breakdown should
// see "good news" first.
const REVIEW_ACTION_ORDER: readonly ReviewActionKey[] = [
  "review_ready_probable",
  "review_low_risk",
  "review_timeout_retry",
  "review_catch_all_consumer",
  "review_high_risk",
  "do_not_send",
];

type Tone = "ok" | "warn" | "bad";

interface ReasonItem {
  label: string;
  detail?: string;
  count?: number | null;
}

interface CardProps {
  tone: Tone;
  title: string;
  count: number | null | undefined;
  sectionLabel: string;
  reasons: ReasonItem[];
}

function fmt(n: number): string {
  return n.toLocaleString("en-US");
}

function CardIcon({ tone }: { tone: Tone }) {
  if (tone === "ok") {
    return (
      <svg viewBox="0 0 14 14" aria-hidden>
        <polyline points="1.5 7.5 5.5 11.5 12.5 2.5" />
      </svg>
    );
  }
  if (tone === "warn") {
    return (
      <svg viewBox="0 0 14 14" aria-hidden>
        <path d="M7 1.5L13 12.5H1Z" />
        <line x1="7" y1="5.5" x2="7" y2="8.5" />
        <circle cx="7" cy="10.5" r="0.7" fill="currentColor" stroke="none" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 14 14" aria-hidden>
      <line x1="2" y1="2" x2="12" y2="12" />
      <line x1="12" y1="2" x2="2" y2="12" />
    </svg>
  );
}

function CategoryCard({ tone, title, count, sectionLabel, reasons }: CardProps) {
  return (
    <div className={`${styles.card} ${styles[tone]}`}>
      <div className={styles.cardHeader}>
        <div className={styles.cardTitleRow}>
          <span className={styles.cardIcon}>
            <CardIcon tone={tone} />
          </span>
          <span className={styles.cardTitle}>{title}</span>
          {count != null && (
            <span className={styles.cardCount}>{fmt(count)}</span>
          )}
        </div>
      </div>

      <div className={styles.cardBody}>
        <div className={styles.sectionLabel}>{sectionLabel}</div>
        <ul className={styles.reasons}>
          {reasons.map((r, i) => (
            <li key={i} className={styles.reason}>
              <div className={styles.reasonLeft}>
                <span className={`${styles.bullet} ${styles[`bullet_${tone}`]}`} />
                <div>
                  <div className={styles.reasonLabel}>{r.label}</div>
                  {r.detail && (
                    <div className={styles.reasonDetail}>{r.detail}</div>
                  )}
                </div>
              </div>
              {r.count != null && r.count > 0 && (
                <span className={styles.reasonCount}>{fmt(r.count)}</span>
              )}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export function ClassificationBreakdown({
  summary,
  reviewBreakdown,
  reviewActionBreakdown,
}: Props) {
  if (!summary) return null;

  // V2.10.10.b — prefer the action-oriented breakdown when available.
  // It tells the operator *what to do* with each cohort, which is the
  // headline question after a run with a low ready_count. Fall back
  // to the decision-reason breakdown, then to the legacy placeholder
  // list, depending on what the API returned.
  const actionReasons: ReasonItem[] = [];
  if (reviewActionBreakdown) {
    for (const key of REVIEW_ACTION_ORDER) {
      const count = reviewActionBreakdown[key];
      if (typeof count !== "number" || count <= 0) continue;
      const copy = RESULTS_COPY.reviewActions[key];
      actionReasons.push({
        label: copy.title,
        detail: copy.hint,
        count,
      });
    }
  }

  const breakdownReasons: ReasonItem[] = [];
  if (reviewBreakdown) {
    for (const key of REVIEW_SUBDIVISION_ORDER) {
      const count = reviewBreakdown[key];
      if (typeof count !== "number" || count <= 0) continue;
      const copy = RESULTS_COPY.reviewSubdivisions[key];
      breakdownReasons.push({
        label: copy.title,
        detail: copy.hint,
        count,
      });
    }
  }

  // Selection precedence: action > decision_reason > legacy placeholder.
  let reviewReasons: ReasonItem[];
  let reviewSectionLabel: string;
  if (actionReasons.length > 0) {
    reviewReasons = actionReasons;
    reviewSectionLabel = "What to do with each cohort";
  } else if (breakdownReasons.length > 0) {
    reviewReasons = breakdownReasons;
    reviewSectionLabel = "Subdivision (per decision_reason)";
  } else {
    reviewSectionLabel = "Common reasons";
    reviewReasons = [
      {
        label: "Catch-all domain",
        detail: "Server accepts any address — can't confirm delivery",
      },
      {
        label: "No SMTP confirmation",
        detail: "Mailbox couldn't be verified at send time",
      },
      {
        label: "Role-based address",
        detail: "Shared inboxes like info@, admin@, support@",
        count: summary.role_based_emails,
      },
    ];
  }

  const secondPassCount =
    reviewActionBreakdown?.second_pass_candidates ?? null;

  return (
    <div className={styles.wrapper}>
      <div className={styles.sectionHeading}>// Why each email was classified</div>
      <div className={styles.grid}>
        <CategoryCard
          tone="ok"
          title="Confirmed safe-only"
          count={summary.total_valid}
          sectionLabel="What was verified"
          reasons={[
            { label: "Valid email syntax and format" },
            { label: "Domain exists and accepts email" },
            { label: "SMTP-confirmed or trusted consumer provider" },
            { label: "No catch-all / cold-start cap fired" },
          ]}
        />
        <CategoryCard
          tone="warn"
          title="Require review"
          count={summary.total_review}
          sectionLabel={reviewSectionLabel}
          reasons={reviewReasons}
        />
        <CategoryCard
          tone="bad"
          title="Do not use"
          count={summary.total_invalid_or_bounce_risk}
          sectionLabel="Why these were flagged"
          reasons={[
            {
              label: "Invalid or non-existent domain",
              detail: "DNS lookup returned no valid mail server",
            },
            {
              label: "Disposable address",
              detail: "Temporary throwaway service detected",
              count: summary.disposable_emails,
            },
            {
              label: "Fake or placeholder",
              detail: "Patterns like test@test.com, noreply@, etc.",
              count: summary.placeholder_or_fake_emails,
            },
            {
              label: "SMTP rejected",
              detail: "Mail server explicitly rejected the address",
            },
          ]}
        />
      </div>

      {typeof secondPassCount === "number" && secondPassCount > 0 && (
        <div className={styles.secondPassNote}>
          <strong>{fmt(secondPassCount)}</strong> review rows are
          flagged as <em>second-pass candidates</em> — see{" "}
          <code>second_pass_candidates.xlsx</code> in the package.
          These are the rescatable cohort (low-risk + timeout / blocked
          / temp-fail). A live SMTP retry from a warmer egress, or a
          paid third-party probe, typically recovers a meaningful
          share of them.
        </div>
      )}
    </div>
  );
}
