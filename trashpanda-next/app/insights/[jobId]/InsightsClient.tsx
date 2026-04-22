"use client";

import Link from "next/link";
import { useEffect, useMemo, useState, useCallback } from "react";
import { Topbar } from "@/components/Topbar";
import { getJobInsights } from "@/lib/api";
import type { InsightRow, InsightsResponse, InsightDomain } from "@/lib/types";
import styles from "./Insights.module.css";

// ─── Copy mappings (product-friendly) ───────────────────────────────────────

const FINAL_ACTION_COPY: Record<string, { label: string; tone: "ok" | "warn" | "bad" }> = {
  auto_approve:  { label: "Auto-approved",  tone: "ok"   },
  manual_review: { label: "Manual review",  tone: "warn" },
  auto_reject:   { label: "Auto-rejected",  tone: "bad"  },
};

const BUCKET_COPY: Record<string, { label: string; tone: "ok" | "warn" | "bad" }> = {
  valid:                   { label: "Ready to send",   tone: "ok"   },
  review:                  { label: "Needs attention", tone: "warn" },
  invalid:                 { label: "Do not use",      tone: "bad"  },
  invalid_or_bounce_risk:  { label: "Do not use",      tone: "bad"  },
};

const SOURCE_LABEL: Record<InsightRow["source"], string> = {
  valid:   "Ready",
  review:  "Review",
  invalid: "Removed",
};

const HISTORICAL_COPY: Record<string, { label: string; tone: "ok" | "warn" | "bad" | "info" }> = {
  reliable:             { label: "Historically reliable", tone: "ok"   },
  risky:                { label: "Historically risky",    tone: "bad"  },
  unstable:             { label: "Historically unstable", tone: "warn" },
  catch_all_suspected:  { label: "Catch-all suspected",   tone: "warn" },
  unknown:              { label: "No history",            tone: "info" },
};

// ─── Filter types ──────────────────────────────────────────────────────────

type FilterKey =
  | "all"
  | "ready"
  | "review"
  | "removed"
  | "high_conf"
  | "low_conf"
  | "catch_all"
  | "smtp_tested"
  | "smtp_suspicious"
  | "risky_history"
  | "auto_rejected"
  | "manual_review";

const FILTERS: { key: FilterKey; label: string }[] = [
  { key: "all",             label: "All" },
  { key: "ready",           label: "Ready to send" },
  { key: "review",          label: "Needs attention" },
  { key: "removed",         label: "Do not use" },
  { key: "high_conf",       label: "High confidence" },
  { key: "low_conf",        label: "Low confidence" },
  { key: "catch_all",       label: "Catch-all" },
  { key: "smtp_tested",     label: "SMTP tested" },
  { key: "smtp_suspicious", label: "SMTP suspicious" },
  { key: "risky_history",   label: "Historically risky" },
  { key: "manual_review",   label: "Manual review" },
  { key: "auto_rejected",   label: "Auto-rejected" },
];

const PAGE_SIZE = 100;

// ─── Helpers ───────────────────────────────────────────────────────────────

function fmtPct(n: number, total: number): string {
  if (!total) return "0%";
  return `${((n / total) * 100).toFixed(1).replace(/\.0$/, "")}%`;
}

function fmtProb(p: number | undefined | null): string {
  if (p == null) return "—";
  return `${Math.round(p * 100)}%`;
}

function fmtCount(n: number): string {
  return n.toLocaleString("en-US");
}

function matchesFilter(row: InsightRow, f: FilterKey): boolean {
  switch (f) {
    case "all":             return true;
    case "ready":           return row.source === "valid";
    case "review":          return row.source === "review";
    case "removed":         return row.source === "invalid";
    case "high_conf":       return row.confidence_tier === "high";
    case "low_conf":        return row.confidence_tier === "low";
    case "catch_all":       return !!row.possible_catch_all;
    case "smtp_tested":     return !!row.smtp_tested;
    case "smtp_suspicious": return !!row.smtp_suspicious;
    case "risky_history":   return row.historical_label === "risky" || row.historical_label === "catch_all_suspected";
    case "manual_review":   return row.final_action === "manual_review";
    case "auto_rejected":   return row.final_action === "auto_reject";
  }
}

// ─── Main component ────────────────────────────────────────────────────────

interface Props {
  jobId: string;
  initial: InsightsResponse | null;
  inputFilename: string | null;
}

export function InsightsClient({ jobId, initial, inputFilename }: Props) {
  const [data, setData] = useState<InsightsResponse | null>(initial);
  const [loading, setLoading] = useState(initial === null);
  const [error, setError] = useState<string | null>(null);

  const [filter, setFilter] = useState<FilterKey>("all");
  const [search, setSearch] = useState("");
  const [visible, setVisible] = useState(PAGE_SIZE);
  const [selectedRow, setSelectedRow] = useState<InsightRow | null>(null);

  useEffect(() => {
    if (initial) return;
    let cancelled = false;
    getJobInsights(jobId)
      .then((r) => { if (!cancelled) setData(r); })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : "Fetch error"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [jobId, initial]);

  const rows = data?.rows ?? [];

  // Pre-compute filter counts so chip counters are live.
  const filterCounts = useMemo(() => {
    const counts: Record<FilterKey, number> = {
      all: 0, ready: 0, review: 0, removed: 0,
      high_conf: 0, low_conf: 0, catch_all: 0,
      smtp_tested: 0, smtp_suspicious: 0, risky_history: 0,
      auto_rejected: 0, manual_review: 0,
    };
    for (const r of rows) {
      for (const f of FILTERS) {
        if (matchesFilter(r, f.key)) counts[f.key]++;
      }
    }
    return counts;
  }, [rows]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return rows.filter((r) => {
      if (!matchesFilter(r, filter)) return false;
      if (q && !r.email.toLowerCase().includes(q) && !r.domain.toLowerCase().includes(q)) return false;
      return true;
    });
  }, [rows, filter, search]);

  const onChipClick = useCallback((f: FilterKey) => {
    setFilter(f);
    setVisible(PAGE_SIZE);
  }, []);

  // ─── Render ──────────────────────────────────────────────────────────────

  const meta = [
    ...(inputFilename ? [{ label: "FILE", value: inputFilename }] : []),
    { label: "JOB", value: jobId.slice(0, 14) + (jobId.length > 14 ? "…" : "") },
  ];

  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["WORKSPACE", "INSIGHTS"]}
          title="DELIVERABILITY/INTELLIGENCE"
          titleSlice="/"
          subtitle={inputFilename ?? undefined}
          meta={meta}
        />
      </div>

      {loading && !data && (
        <div className="fade-up">
          <div className={styles.panel}>
            <div className={styles.panelBody}>
              <div className={styles.skeletonBlock} style={{ height: 18, width: "40%", marginBottom: 12 }} />
              <div className={styles.skeletonBlock} style={{ height: 14, width: "70%", marginBottom: 8 }} />
              <div className={styles.skeletonBlock} style={{ height: 14, width: "55%" }} />
            </div>
          </div>
        </div>
      )}

      {error && !data && (
        <div className="fade-up">
          <div className={styles.panel}>
            <div className={styles.panelBody} style={{ color: "var(--warn)", fontFamily: "var(--font-mono)", fontSize: 12 }}>
              ⚠ {error}
            </div>
          </div>
        </div>
      )}

      {data && !data.v2_available && (
        <div className="fade-up">
          <div className={styles.panel}>
            <div className={styles.emptyState}>
              <h3>Advanced intelligence not available for this run</h3>
              <p>
                This job was processed before the advanced deliverability layer was enabled,
                or the V2 engine did not produce rich signals for these records.
              </p>
              <p className="hint">
                Re-run this list to unlock per-record confidence, catch-all detection,
                SMTP probe results and domain reputation history.
              </p>
              <Link
                href={`/results/${encodeURIComponent(jobId)}`}
                style={{
                  display: "inline-block",
                  marginTop: 8,
                  padding: "8px 18px",
                  fontFamily: "var(--font-mono)",
                  fontSize: 11,
                  letterSpacing: 1.5,
                  textTransform: "uppercase",
                  color: "var(--neon)",
                  border: "1px solid var(--stroke-strong)",
                  borderRadius: 2,
                  textDecoration: "none",
                }}
              >
                ← Back to results
              </Link>
            </div>
          </div>
        </div>
      )}

      {data && data.v2_available && (
        <>
          <div className="fade-up">
            <ExecutiveHero data={data} />
          </div>

          <div className="fade-up">
            <Panel title="Domain Intelligence" badge={`${Object.values(data.domain_intelligence).reduce((s, l) => s + l.length, 0)} domains`}>
              <DomainIntelligence domains={data.domain_intelligence} />
            </Panel>
          </div>

          <div className="fade-up">
            <Panel
              title="Record-level intelligence"
              badge={`${fmtCount(filtered.length)} of ${fmtCount(rows.length)}`}
            >
              <div className={styles.searchRow}>
                <input
                  className={styles.searchInput}
                  placeholder="Filter by email or domain…"
                  value={search}
                  onChange={(e) => { setSearch(e.target.value); setVisible(PAGE_SIZE); }}
                />
                <Link
                  href={`/review/${encodeURIComponent(jobId)}`}
                  className={styles.whyBtn}
                  style={{ textDecoration: "none" }}
                >
                  → Open review queue
                </Link>
              </div>

              <div className={styles.filters} style={{ marginBottom: 14 }}>
                {FILTERS.map((f) => (
                  <button
                    key={f.key}
                    type="button"
                    onClick={() => onChipClick(f.key)}
                    className={[styles.chip, filter === f.key && styles.chipActive].filter(Boolean).join(" ")}
                  >
                    {f.label}
                    <span className={styles.chipCount}>{filterCounts[f.key]}</span>
                  </button>
                ))}
              </div>

              <EnrichedTable
                rows={filtered.slice(0, visible)}
                onRowClick={setSelectedRow}
              />

              {filtered.length > visible && (
                <button
                  type="button"
                  className={styles.loadMoreBtn}
                  onClick={() => setVisible((v) => v + PAGE_SIZE)}
                >
                  Load {Math.min(PAGE_SIZE, filtered.length - visible)} more ·
                  {" "}{fmtCount(filtered.length - visible)} left
                </button>
              )}
            </Panel>
          </div>
        </>
      )}

      {selectedRow && (
        <WhyModal row={selectedRow} onClose={() => setSelectedRow(null)} />
      )}
    </>
  );
}

// ─── Sub-components ────────────────────────────────────────────────────────

function Panel({ title, badge, children }: { title: string; badge?: string; children: React.ReactNode }) {
  return (
    <div className={styles.panel}>
      <div className={styles.panelHeader}>
        <div className={styles.panelTitle}>// {title}</div>
        {badge && <div className={styles.panelBadge}>{badge}</div>}
      </div>
      <div className={styles.panelBody}>{children}</div>
    </div>
  );
}

function ExecutiveHero({ data }: { data: InsightsResponse }) {
  const total = data.totals.all;
  const ready = data.totals.valid;
  const review = data.totals.review;
  const removed = data.totals.invalid;
  const readyPct = total ? (ready / total) * 100 : 0;

  let headline = "Your list has been analysed with the advanced engine";
  if (readyPct >= 85) {
    headline = "Most of this list looks safe to use";
  } else if (readyPct >= 60) {
    headline = "A solid core is ready, some records need attention";
  } else if (readyPct > 0) {
    headline = "Significant cleanup needed before sending";
  }

  return (
    <div className={styles.heroPanel}>
      <div className={styles.heroHeader}>
        <div>
          <h2 className={styles.heroTitle}>
            EXECUTIVE<span className={styles.heroSlice}>/</span>SUMMARY
          </h2>
          <div className={styles.heroTag}>// Deliverability intelligence</div>
        </div>
        <div className={styles.heroBadge}>V2 ENGINE · LIVE</div>
      </div>

      <p className={styles.heroHeadline}>
        <strong>{headline}.</strong>{" "}
        Out of <strong>{fmtCount(total)}</strong> records,{" "}
        <strong>{fmtCount(ready)}</strong> ({fmtPct(ready, total)}) are ready to send,{" "}
        <strong>{fmtCount(review)}</strong> ({fmtPct(review, total)}) need attention, and{" "}
        <strong>{fmtCount(removed)}</strong> ({fmtPct(removed, total)}) should not be used.
      </p>

      <div className={styles.heroMetrics}>
        <HeroMetric tone="ok"  value={fmtPct(data.confidence_tiers.high, total)}   label="High confidence"   sub={fmtCount(data.confidence_tiers.high)} />
        <HeroMetric tone="warn" value={fmtPct(data.confidence_tiers.medium, total)} label="Medium confidence" sub={fmtCount(data.confidence_tiers.medium)} />
        <HeroMetric tone="bad" value={fmtPct(data.confidence_tiers.low, total)}    label="Low confidence"    sub={fmtCount(data.confidence_tiers.low)} />
        <HeroMetric tone="ok"  value={fmtCount(data.final_actions.auto_approve ?? 0)} label="Auto-approved" sub="Passed all checks" />
        <HeroMetric tone="warn" value={fmtCount(data.final_actions.manual_review ?? 0)} label="Manual review" sub="Needs human eyes" />
        <HeroMetric tone="bad" value={fmtCount(data.final_actions.auto_reject ?? 0)}  label="Auto-rejected" sub="Blocked by engine" />
        <HeroMetric tone="info" value={fmtCount(data.catch_all_count)}               label="Catch-all"     sub="Domains accepting everything" />
        <HeroMetric tone="info" value={fmtCount(data.smtp_tested_count)}             label="SMTP tested"   sub={`${fmtCount(data.smtp_suspicious_count)} suspicious`} />
      </div>
    </div>
  );
}

function HeroMetric({ tone, value, label, sub }: { tone: "ok" | "warn" | "bad" | "info" | "steel"; value: string; label: string; sub?: string }) {
  return (
    <div className={[styles.heroMetric, styles[tone]].filter(Boolean).join(" ")}>
      <div className={styles.metricValue}>{value}</div>
      <div className={styles.metricLabel}>// {label}</div>
      {sub && <div className={styles.metricSub}>{sub}</div>}
    </div>
  );
}

function EnrichedTable({ rows, onRowClick }: { rows: InsightRow[]; onRowClick: (r: InsightRow) => void }) {
  if (rows.length === 0) {
    return (
      <div style={{ padding: "30px 8px", textAlign: "center", color: "var(--ink-low)", fontFamily: "var(--font-mono)", fontSize: 11 }}>
        No records match the current filter.
      </div>
    );
  }
  return (
    <div className={styles.tableWrap}>
      <table className={styles.table}>
        <thead>
          <tr>
            <th>Email</th>
            <th>Bucket</th>
            <th>Confidence</th>
            <th>Suggested action</th>
            <th>Domain history</th>
            <th>SMTP</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <TableRow key={r.id} row={r} onClick={() => onRowClick(r)} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TableRow({ row, onClick }: { row: InsightRow; onClick: () => void }) {
  const bucketKey = (row.bucket_v2 ?? row.source).toLowerCase();
  const bucketCopy = BUCKET_COPY[bucketKey] ?? { label: row.bucket_label ?? SOURCE_LABEL[row.source], tone: row.source === "valid" ? "ok" : row.source === "review" ? "warn" : "bad" as const };

  const action = row.final_action ? FINAL_ACTION_COPY[row.final_action] : null;

  const histCopy = row.historical_label ? HISTORICAL_COPY[row.historical_label] : null;

  const confPct = row.confidence_v2 != null ? Math.round(row.confidence_v2 * 100) : null;
  const confTier = row.confidence_tier ?? "medium";

  return (
    <tr onClick={onClick}>
      <td>
        <div className={styles.emailCell}>{row.email}</div>
        <div className={styles.domainCell}>{row.domain}</div>
      </td>
      <td>
        <span className={`${styles.tag} ${styles["tag" + cap(bucketCopy.tone)]}`}>
          {bucketCopy.label}
        </span>
      </td>
      <td>
        {confPct !== null ? (
          <div className={styles.confBar}>
            <div className={styles.confBarTrack}>
              <div className={`${styles.confBarFill} ${styles[confTier]}`} style={{ width: `${confPct}%` }} />
            </div>
            <span className={styles.confBarLabel}>{confPct}%</span>
          </div>
        ) : (
          <span className={`${styles.tag} ${styles.tagSteel}`}>No signal</span>
        )}
      </td>
      <td>
        {action ? (
          <span className={`${styles.tag} ${styles["tag" + cap(action.tone)]}`}>{action.label}</span>
        ) : (
          <span className={`${styles.sourceBadge} ${styles["source" + cap(row.source)]}`}>
            {SOURCE_LABEL[row.source]}
          </span>
        )}
        {row.review_subclass && (
          <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--ink-low)", marginTop: 3 }}>
            {row.review_subclass}
          </div>
        )}
      </td>
      <td>
        {histCopy ? (
          <span className={`${styles.tag} ${styles["tag" + cap(histCopy.tone)]}`}>{histCopy.label}</span>
        ) : (
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--ink-low)" }}>—</span>
        )}
        {row.possible_catch_all && (
          <div style={{ marginTop: 4 }}>
            <span className={`${styles.tag} ${styles.tagWarn}`}>Catch-all</span>
          </div>
        )}
      </td>
      <td>
        {row.smtp_tested ? (
          <span className={`${styles.tag} ${row.smtp_suspicious ? styles.tagBad : row.smtp_confirmed_valid ? styles.tagOk : styles.tagSteel}`}>
            {row.smtp_result ?? (row.smtp_suspicious ? "Suspicious" : row.smtp_confirmed_valid ? "Valid" : "Tested")}
          </span>
        ) : (
          <span style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--ink-low)" }}>Not tested</span>
        )}
      </td>
      <td>
        <button type="button" className={styles.whyBtn} onClick={(e) => { e.stopPropagation(); onClick(); }}>
          Why?
        </button>
      </td>
    </tr>
  );
}

function cap(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function WhyModal({ row, onClose }: { row: InsightRow; onClose: () => void }) {
  useEffect(() => {
    const onEsc = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onEsc);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onEsc);
      document.body.style.overflow = prev;
    };
  }, [onClose]);

  const bucketKey = (row.bucket_v2 ?? row.source).toLowerCase();
  const bucketCopy = BUCKET_COPY[bucketKey] ?? { label: row.bucket_label ?? SOURCE_LABEL[row.source], tone: "info" as const };
  const action = row.final_action ? FINAL_ACTION_COPY[row.final_action] : null;
  const histCopy = row.historical_label ? HISTORICAL_COPY[row.historical_label] : null;

  const details: { label: string; value: React.ReactNode; muted?: boolean }[] = [];

  details.push({
    label: "Classification",
    value: (
      <span className={`${styles.tag} ${styles["tag" + cap(bucketCopy.tone)]}`}>{bucketCopy.label}</span>
    ),
  });
  if (action) {
    details.push({
      label: "Engine decision",
      value: <span className={`${styles.tag} ${styles["tag" + cap(action.tone)]}`}>{action.label}</span>,
    });
  }
  if (row.confidence_v2 != null) {
    details.push({
      label: "Confidence",
      value: `${Math.round(row.confidence_v2 * 100)}% (${row.confidence_tier ?? "—"})`,
    });
  }
  if (row.deliverability_probability != null) {
    details.push({
      label: "Deliverability probability",
      value: `${fmtProb(row.deliverability_probability)}${row.deliverability_label ? ` · ${row.deliverability_label}` : ""}`,
    });
  }
  if (histCopy) {
    details.push({
      label: "Domain history",
      value: <span className={`${styles.tag} ${styles["tag" + cap(histCopy.tone)]}`}>{histCopy.label}</span>,
    });
  }
  if (row.possible_catch_all) {
    details.push({
      label: "Catch-all detection",
      value: `Yes${row.catch_all_confidence != null ? ` · ${Math.round(row.catch_all_confidence * 100)}% confidence` : ""}${row.catch_all_reason ? ` · ${row.catch_all_reason}` : ""}`,
    });
  }
  if (row.smtp_tested) {
    const parts: string[] = [];
    if (row.smtp_result) parts.push(row.smtp_result);
    if (row.smtp_code) parts.push(`code ${row.smtp_code}`);
    if (row.smtp_confidence != null) parts.push(`${Math.round(row.smtp_confidence * 100)}% conf`);
    details.push({
      label: "SMTP probe",
      value: (
        <>
          {row.smtp_confirmed_valid ? "Confirmed valid" : row.smtp_suspicious ? "Suspicious response" : "Tested"}
          {parts.length > 0 && <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--ink-mid)", marginTop: 3 }}>{parts.join(" · ")}</div>}
        </>
      ),
    });
  }
  if (row.review_subclass) {
    details.push({ label: "Review subclass", value: row.review_subclass, muted: true });
  }
  if (row.decision_reason) {
    details.push({ label: "Decision reason", value: row.decision_reason, muted: true });
  }
  if (row.decision_note) {
    details.push({ label: "Decision note", value: row.decision_note, muted: true });
  }
  if (row.deliverability_factors) {
    details.push({ label: "Signals considered", value: row.deliverability_factors, muted: true });
  }
  if (row.confidence_adjustment_applied) {
    details.push({ label: "Adjusted by history", value: "Yes — the score was modified based on this domain's past behavior.", muted: true });
  }
  if (row.reason_codes_v2) {
    details.push({ label: "Raw reason codes", value: row.reason_codes_v2, muted: true });
  }

  return (
    <div className={styles.modalBackdrop} onClick={onClose}>
      <div className={styles.modalCard} onClick={(e) => e.stopPropagation()}>
        <div className={styles.modalHeader}>
          <div className={styles.modalTitle}>
            WHY<span className="slice" style={{ color: "var(--neon)" }}>/</span>THIS EMAIL IS HERE
          </div>
          <button className={styles.modalClose} onClick={onClose}>ESC</button>
        </div>
        <div className={styles.modalBody}>
          <div className={styles.modalEmailBlock}>
            <div className={styles.modalEmailAddr}>{row.email}</div>
            <div className={styles.modalDomain}>Domain · {row.domain}</div>
          </div>

          {row.human_reason && (
            <div className={styles.modalSection}>
              <div className={styles.modalSectionTitle}>// In plain English</div>
              <div className={styles.detailValue} style={{ lineHeight: 1.6 }}>
                {row.human_reason}
              </div>
              {row.human_risk && (
                <div className={styles.detailValue} style={{ color: "var(--warn)", fontSize: 12 }}>
                  ⚠ {row.human_risk}
                </div>
              )}
              {row.human_recommendation && (
                <div className={styles.detailValue} style={{ color: "var(--neon)", fontSize: 12 }}>
                  → {row.human_recommendation}
                </div>
              )}
            </div>
          )}

          <div className={styles.modalSection}>
            <div className={styles.modalSectionTitle}>// Engine signals</div>
            <div className={styles.detailGrid}>
              {details.map((d, i) => (
                <div key={i} className={styles.detailRow}>
                  <div className={styles.detailLabel}>{d.label}</div>
                  <div className={`${styles.detailValue} ${d.muted ? styles.muted : ""}`}>{d.value}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Domain intelligence ───────────────────────────────────────────────────

function DomainIntelligence({ domains }: { domains: InsightsResponse["domain_intelligence"] }) {
  const groups: { key: keyof InsightsResponse["domain_intelligence"]; label: string; tone: "ok" | "warn" | "bad" | "info"; desc: string }[] = [
    { key: "reliable",            label: "Historically reliable", tone: "ok",   desc: "Domains that consistently deliver."  },
    { key: "risky",               label: "Historically risky",    tone: "bad",  desc: "Low deliverability on past runs."   },
    { key: "unstable",            label: "Unstable",              tone: "warn", desc: "Mixed signals or intermittent SMTP." },
    { key: "catch_all_suspected", label: "Catch-all suspected",   tone: "info", desc: "Accept anything; delivery unclear." },
  ];

  return (
    <div className={styles.domainGrid}>
      {groups.map((g) => {
        const list = domains[g.key];
        return (
          <div key={g.key} className={styles.domainGroup}>
            <div className={`${styles.domainGroupTitle} ${styles[g.tone]}`}>
              ● {g.label} · {list.length}
            </div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--ink-low)", marginBottom: 10 }}>
              {g.desc}
            </div>
            {list.length === 0 ? (
              <div className={styles.emptyGroup}>No domains in this group.</div>
            ) : (
              list.map((d: InsightDomain) => (
                <div key={d.domain} className={styles.domainRow}>
                  <div>
                    <div className={styles.domainName}>{d.domain}</div>
                    <div className={styles.domainStats}>
                      {d.avg_deliverability != null
                        ? `${Math.round(d.avg_deliverability * 100)}% deliverability`
                        : "No probability"} ·
                      {" "}{d.count} records
                      {d.catch_all_count > 0 && ` · ${d.catch_all_count} catch-all`}
                    </div>
                  </div>
                  <div className={styles.domainVolume}>{d.count}</div>
                </div>
              ))
            )}
          </div>
        );
      })}
    </div>
  );
}
