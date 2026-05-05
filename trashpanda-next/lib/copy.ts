/**
 * Centralized customer-facing copy strings.
 *
 * V2.10.10 — extracted from `ExecutiveSummary.tsx`,
 * `MetricsCards.tsx`, `SendToClientButton.tsx`, and
 * `ClassificationBreakdown.tsx`. The audit of WY_small revealed that
 * the previous "8.3% READY TO SEND" copy did not explain what the
 * other 79.3% actually meant, which made an in-policy result look
 * like a destruction event. Centralizing the strings here keeps
 * tone consistent across components and gives us a single place to
 * iterate on wording (and to add future i18n).
 *
 * Rules:
 *   - Never claim a row is "invalid" when the engine routed it to
 *     `manual_review` — those rows are unconfirmed, not bad.
 *   - Never claim a row is "ready" without qualifying that "ready"
 *     means SMTP / safety-cap-confirmed safe-only delivery.
 *   - Always offer the operator a clear next action (review the
 *     breakdown, re-clean stricter, etc.) when the headline is bad.
 */

export const RESULTS_COPY = {
  /** Main page banner when delivery is partial / not fully ready. */
  partialDeliveryBanner:
    "Partial delivery — the operator review gate flagged warnings. " +
    "Review rows are not invalid; they are unconfirmed. The breakdown " +
    "below tells you which subset is safe to use after second-pass.",

  /** Headline tile labels. */
  readyTile: {
    label: "Confirmed safe for immediate use",
    helper: "Ready to drop straight into your campaign tool.",
  },
  reviewTile: {
    label: "Require manual review",
    helper:
      "Mostly unconfirmed B2B / catch-all consumer providers. " +
      "See breakdown below.",
  },
  removedTile: {
    label: "High-risk removed",
    helper: "Hard syntax / MX failures, duplicates, disposable.",
  },
  scannedTile: {
    label: "Emails scanned",
    helper: "Total rows processed.",
  },

  /** Small caption shown next to the SEND TO CLIENT card. */
  sendToClientCaption: (
    safe: number,
    review: number,
    removed: number,
  ): string =>
    `${safe} confirmed safe-only · ${review} require review · ${removed} do not use`,

  /** Per-subdivision copy for the review breakdown card. */
  reviewSubdivisions: {
    review_cold_start_b2b: {
      title: "Unconfirmed B2B / unknown domains",
      hint: "Often rescatable with second-pass / live SMTP retry.",
    },
    review_smtp_inconclusive: {
      title: "SMTP inconclusive",
      hint: "MX exists but probe was blocked / timed out / dry-run.",
    },
    review_catch_all: {
      title: "Catch-all consumer",
      hint: "Yahoo / AOL / Verizon-class. Cannot be confirmed without sending.",
    },
    review_medium_probability: {
      title: "Medium probability",
      hint: "Score 0.50–0.80. Mixed signals.",
    },
    review_domain_high_risk: {
      title: "High-risk domain",
      hint: "Disposable / suspicious shape. Treat as do-not-send.",
    },
  } as const,

  /**
   * V2.10.10.b — action-oriented breakdown copy. Each entry maps to a
   * filtered XLSX in the package; ``second_pass_candidates`` is a
   * rolled-up file (low_risk + timeout_retry).
   */
  reviewActions: {
    review_low_risk: {
      title: "Low-risk · second-pass candidate",
      hint:
        "Good signals (probability ≥ 0.55, valid syntax / MX, not " +
        "disposable). SMTP didn't confirm but a retry from a warmer " +
        "egress should rescue most.",
      tone: "ok" as const,
    },
    review_timeout_retry: {
      title: "Timeout / blocked · operational retry",
      hint:
        "MX is alive but the probe was blocked, timed out, or " +
        "soft-failed. Retry with a different egress or after a backoff.",
      tone: "ok" as const,
    },
    review_catch_all_consumer: {
      title: "Catch-all consumer (Yahoo / AOL / Verizon-class)",
      hint:
        "Domain accepts any address — no automated probe can confirm " +
        "deliverability without sending. Do NOT auto-send to cold " +
        "campaigns.",
      tone: "warn" as const,
    },
    review_high_risk: {
      title: "High-risk · low confidence",
      hint:
        "Low probability or multiple negative signals. Not enough " +
        "evidence to send. Inspect manually before including.",
      tone: "warn" as const,
    },
    do_not_send: {
      title: "Do not send",
      hint:
        "Disposable / suspicious-shape domain. The probability scoring " +
        "would have approved them but the domain itself is poison.",
      tone: "bad" as const,
    },
    second_pass_candidates: {
      title: "Second-pass candidates (rolled-up)",
      hint:
        "Union of low_risk + timeout_retry. Send these to a live SMTP " +
        "verifier or a paid third-party probe to recover the rescatable " +
        "share before campaign launch.",
      tone: "ok" as const,
    },
  } as const,

  /** Extra-strict CTA copy. */
  extraStrict: {
    title: "Generate Extra-Strict bundle",
    description:
      "Drops Yahoo / AOL / Verizon catch-all automatically. " +
      "Use this if your previous delivery had a high bounce rate.",
    cta: "Generate Extra-Strict",
  },

  /** SMTP runtime caption shown next to the headline tiles. */
  smtpCaption: (
    attempted: number | undefined,
    seen: number | undefined,
    valid: number | undefined,
    inconclusive: number | undefined,
    enabled: boolean | undefined,
    dryRun: boolean | undefined,
  ): string => {
    if (!enabled) {
      return "SMTP verification disabled — relying on offline signals only.";
    }
    if (dryRun) {
      return "SMTP ran in dry-run mode — every candidate was treated as inconclusive.";
    }
    if (
      typeof attempted !== "number" ||
      typeof seen !== "number" ||
      seen === 0
    ) {
      return "SMTP coverage unavailable.";
    }
    const validStr = typeof valid === "number" ? valid.toLocaleString() : "—";
    const inconclusiveStr =
      typeof inconclusive === "number" ? inconclusive.toLocaleString() : "—";
    return `SMTP: ${attempted.toLocaleString()} of ${seen.toLocaleString()} candidates probed · ${validStr} confirmed valid · ${inconclusiveStr} inconclusive.`;
  },
} as const;

export type ReviewSubdivisionKey =
  keyof typeof RESULTS_COPY.reviewSubdivisions;

export type ReviewActionKey =
  keyof typeof RESULTS_COPY.reviewActions;
