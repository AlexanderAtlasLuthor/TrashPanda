import styles from "./IssuesList.module.css";

interface IssueLike {
  severity?: string | null;
  code?: string | null;
  message?: string | null;
}

interface IssuesListProps {
  issues?: IssueLike[] | null;
  emptyLabel?: string;
}

const SEVERITY_ORDER: Record<string, number> = {
  block: 0,
  warn: 1,
};

function severityRank(value: string | null | undefined): number {
  const key = (value ?? "").toString().trim().toLowerCase();
  if (key in SEVERITY_ORDER) return SEVERITY_ORDER[key];
  return 99;
}

function toneFor(value: string | null | undefined): string {
  const key = (value ?? "").toString().trim().toLowerCase();
  if (key === "block") return "block";
  if (key === "warn" || key === "warning") return "warn";
  if (key === "info") return "info";
  return "muted";
}

/**
 * Presentational list of operator/package issues. Groups by severity
 * (block first, warn second, anything else last) and shows the issue's
 * code + message. NEVER computes readiness from the list.
 */
export function IssuesList({ issues, emptyLabel }: IssuesListProps) {
  const list = issues ?? [];
  if (list.length === 0) {
    return (
      <div className={styles.empty}>
        {emptyLabel ?? "No issues reported."}
      </div>
    );
  }

  const sorted = [...list].sort(
    (a, b) => severityRank(a.severity) - severityRank(b.severity),
  );

  return (
    <ul className={styles.list}>
      {sorted.map((issue, idx) => {
        const tone = toneFor(issue.severity);
        const code = (issue.code ?? "").toString().trim();
        const message = (issue.message ?? "").toString().trim();
        const severityLabel = (issue.severity ?? "info")
          .toString()
          .trim()
          .toUpperCase();
        return (
          <li
            key={`${code || "issue"}-${idx}`}
            className={[styles.item, styles[tone]].join(" ")}
            data-tone={tone}
          >
            <span className={styles.severity}>{severityLabel}</span>
            {code && <span className={styles.code}>{code}</span>}
            <span className={styles.message}>
              {message || "(no message provided)"}
            </span>
          </li>
        );
      })}
    </ul>
  );
}
