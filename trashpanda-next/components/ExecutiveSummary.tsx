import type { JobSummary } from "@/lib/types";
import styles from "./ExecutiveSummary.module.css";

interface Props {
  summary: JobSummary | null | undefined;
}

type Tone = "ok" | "warn" | "bad";

function pct(
  num: number | null | undefined,
  denom: number | null | undefined,
): number | null {
  if (num == null || !denom || denom === 0) return null;
  return (num / denom) * 100;
}

function fmt(n: number, decimals = 1): string {
  const s = n.toFixed(decimals);
  return s.endsWith(".0") ? s.slice(0, -2) : s;
}

function fmtCount(n: number): string {
  return n.toLocaleString("en-US");
}

function getTone(validPct: number | null): Tone {
  if (validPct === null) return "ok";
  if (validPct > 95) return "ok";
  if (validPct >= 80) return "warn";
  return "bad";
}

function getHeadline(validPct: number | null): string {
  if (validPct === null) return "Your list has been processed";
  if (validPct > 95) return "Your list is in excellent shape";
  if (validPct >= 80) return "Your list is in good shape";
  return "Your list needs attention";
}

function ToneIcon({ tone }: { tone: Tone }) {
  if (tone === "ok") {
    return (
      <svg viewBox="0 0 16 16" aria-hidden>
        <polyline points="2 9 6 13 14 3" />
      </svg>
    );
  }
  if (tone === "warn") {
    return (
      <svg viewBox="0 0 16 16" aria-hidden>
        <path d="M8 2L15 14H1Z" />
        <line x1="8" y1="6.5" x2="8" y2="10" />
        <circle cx="8" cy="12" r="0.8" fill="currentColor" stroke="none" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 16 16" aria-hidden>
      <circle cx="8" cy="8" r="6" />
      <line x1="8" y1="5" x2="8" y2="9.5" />
      <circle cx="8" cy="11.5" r="0.8" fill="currentColor" stroke="none" />
    </svg>
  );
}

export function ExecutiveSummary({ summary }: Props) {
  const total = summary?.total_input_rows ?? null;
  const valid = summary?.total_valid ?? null;
  const review = summary?.total_review ?? null;
  const invalid = summary?.total_invalid_or_bounce_risk ?? null;

  const validPct = pct(valid, total);
  const invalidPct = pct(invalid, total);

  if (total == null && valid == null) return null;

  const tone = getTone(validPct);
  const headline = getHeadline(validPct);

  const improvementPct =
    invalidPct !== null && invalidPct > 0
      ? Math.round(invalidPct * 10) / 10
      : null;

  const metrics: Array<{ value: string; label: string; color: string }> = [];
  if (validPct !== null) {
    metrics.push({
      value: `${fmt(validPct)}%`,
      label: "ready to send",
      color: "var(--neon)",
    });
  }
  if (invalidPct !== null && invalidPct > 0) {
    metrics.push({
      value: `${fmt(invalidPct)}%`,
      label: "high-risk removed",
      color: "var(--danger)",
    });
  }
  if (review !== null && review > 0) {
    metrics.push({
      value: fmtCount(review),
      label: "need review",
      color: "var(--warn)",
    });
  }

  return (
    <div className={`${styles.panel} ${styles[tone]}`}>
      <div className={styles.top}>
        <div className={styles.headlineRow}>
          <span className={styles.statusIcon}>
            <ToneIcon tone={tone} />
          </span>
          <h2 className={styles.headline}>{headline}</h2>
        </div>
        {total !== null && (
          <div className={styles.totalBadge}>
            {fmtCount(total)} rows processed
          </div>
        )}
      </div>

      {metrics.length > 0 && (
        <div className={styles.metricsRow}>
          {metrics.map((m, i) => (
            <div key={i} className={styles.metric}>
              <div
                className={styles.metricValue}
                style={{ color: m.color }}
              >
                {m.value}
              </div>
              <div className={styles.metricLabel}>{m.label}</div>
            </div>
          ))}
        </div>
      )}

      {improvementPct !== null && (
        <div className={styles.estimateBar}>
          <span className={styles.estimateText}>
            Estimated deliverability improvement
          </span>
          <span className={styles.estimateValue}>+{fmt(improvementPct)}%</span>
        </div>
      )}
    </div>
  );
}
