"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import {
  getReviewDecisions,
  getReviewEmails,
  reviewExportUrl,
  saveReviewDecisions,
} from "@/lib/api";
import { Topbar } from "@/components/Topbar";
import type { ReviewDecision, ReviewEmail, ReviewReason } from "@/lib/types";
import styles from "./ReviewQueue.module.css";

const REASON_LABELS: Record<ReviewReason, string> = {
  "catch-all": "Catch-all",
  "role-based": "Role-based",
  "no-smtp":    "No SMTP",
};

const PAGE_SIZE = 50;

// ── Sub-components ────────────────────────────────────────────────────────

function ReasonPill({ reason }: { reason: ReviewReason }) {
  const cls = reason === "catch-all"
    ? styles.pillCatchAll
    : reason === "role-based"
      ? styles.pillRoleBased
      : styles.pillNoSmtp;
  return <span className={`${styles.pill} ${cls}`}>{REASON_LABELS[reason]}</span>;
}

function SkeletonRow() {
  return (
    <tr className={styles.row}>
      {[40, 220, 120, 70, 160].map((w, i) => (
        <td key={i} className={styles.td}>
          <div className={styles.skeletonCell} style={{ width: w }} />
        </td>
      ))}
    </tr>
  );
}

interface RowProps {
  email: ReviewEmail;
  decision: ReviewDecision | null;
  selected: boolean;
  expanded: boolean;
  onSelect: () => void;
  onDecide: (d: ReviewDecision) => void;
  onUndo: () => void;
  onToggleDetails: () => void;
}

function EmailRow({
  email, decision, selected, expanded,
  onSelect, onDecide, onUndo, onToggleDetails,
}: RowProps) {
  const rowCls = [
    styles.row,
    decision === "approved" && styles.rowApproved,
    decision === "removed"  && styles.rowRemoved,
    selected                && styles.rowSelected,
  ].filter(Boolean).join(" ");

  return (
    <>
      <tr className={rowCls}>
        <td className={`${styles.td} ${styles.tdCheck}`}>
          <input
            type="checkbox"
            className={styles.checkbox}
            checked={selected}
            onChange={onSelect}
            aria-label={`Select ${email.email}`}
          />
        </td>
        <td className={`${styles.td} ${styles.tdEmail}`}>
          <span className={styles.emailAddr}>{email.email}</span>
          <button
            type="button"
            className={styles.whyBtn}
            onClick={onToggleDetails}
            aria-expanded={expanded}
            aria-label="Why this email is here"
          >
            {expanded ? "Hide details" : "Why?"}
          </button>
        </td>
        <td className={styles.td}>
          <ReasonPill reason={email.reason} />
        </td>
        <td className={styles.td}>
          <span className={`${styles.conf} ${email.confidence === "low" ? styles.confLow : styles.confMed}`}>
            {email.confidence.toUpperCase()}
          </span>
        </td>
        <td className={`${styles.td} ${styles.tdActions}`}>
          {decision ? (
            <div className={styles.decidedRow}>
              <span className={decision === "approved" ? styles.decidedApproved : styles.decidedRemoved}>
                {decision === "approved" ? "→ Approved" : "✕ Removed"}
              </span>
              <button className={styles.undoBtn} onClick={onUndo} type="button">
                undo
              </button>
            </div>
          ) : (
            <div className={styles.actionBtns}>
              <button
                className={styles.btnApprove}
                onClick={() => onDecide("approved")}
                type="button"
              >
                Approve
              </button>
              <button
                className={styles.btnRemove}
                onClick={() => onDecide("removed")}
                type="button"
              >
                Reject
              </button>
            </div>
          )}
        </td>
      </tr>
      {expanded && (
        <tr className={styles.detailRow}>
          <td colSpan={5} className={styles.detailCell}>
            <div className={styles.detailGrid}>
              <div>
                <div className={styles.detailLabel}>Classification</div>
                <div className={styles.detailValue}>
                  {email.classification_bucket ?? "Needs attention"}
                </div>
              </div>
              <div>
                <div className={styles.detailLabel}>Reason</div>
                <div className={styles.detailValue}>
                  {email.human_reason ?? email.friendly_reason ?? REASON_LABELS[email.reason]}
                </div>
              </div>
              <div>
                <div className={styles.detailLabel}>Risk</div>
                <div className={styles.detailValue}>
                  {email.human_risk ?? email.risk ?? "Deliverability could not be fully verified."}
                </div>
              </div>
              <div>
                <div className={styles.detailLabel}>Recommendation</div>
                <div className={styles.detailValue}>
                  {email.human_recommendation ?? email.recommended_action ?? "Review manually before sending."}
                </div>
              </div>
              {email.deliverability_probability != null && (
                <div>
                  <div className={styles.detailLabel}>Deliverability probability</div>
                  <div className={styles.detailValue}>
                    {Math.round(email.deliverability_probability * 100)}%
                    {email.deliverability_label ? ` · ${email.deliverability_label}` : ""}
                  </div>
                </div>
              )}
              {email.confidence_v2 != null && (
                <div>
                  <div className={styles.detailLabel}>Engine confidence</div>
                  <div className={styles.detailValue}>
                    {Math.round(email.confidence_v2 * 100)}%
                    {email.confidence_tier ? ` (${email.confidence_tier})` : ""}
                  </div>
                </div>
              )}
              {email.final_action_label && (
                <div>
                  <div className={styles.detailLabel}>Engine decision</div>
                  <div className={styles.detailValue}>{email.final_action_label}</div>
                </div>
              )}
              {email.historical_label_friendly && (
                <div>
                  <div className={styles.detailLabel}>Domain history</div>
                  <div className={styles.detailValue}>{email.historical_label_friendly}</div>
                </div>
              )}
              {email.possible_catch_all && (
                <div>
                  <div className={styles.detailLabel}>Catch-all</div>
                  <div className={styles.detailValue}>
                    Yes
                    {email.catch_all_confidence != null ? ` · ${Math.round(email.catch_all_confidence * 100)}% confidence` : ""}
                  </div>
                </div>
              )}
              {email.smtp_tested && (
                <div>
                  <div className={styles.detailLabel}>SMTP probe</div>
                  <div className={styles.detailValue}>
                    {email.smtp_confirmed_valid
                      ? "Confirmed valid"
                      : email.smtp_suspicious
                        ? "Suspicious response"
                        : (email.smtp_result ?? "Tested")}
                  </div>
                </div>
              )}
              {email.review_subclass && (
                <div>
                  <div className={styles.detailLabel}>Subclass</div>
                  <div className={styles.detailValue}>{email.review_subclass}</div>
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ── Main component ────────────────────────────────────────────────────────

export function ReviewQueueClient({ jobId }: { jobId: string }) {
  const [emails, setEmails]         = useState<ReviewEmail[]>([]);
  const [loading, setLoading]       = useState(true);
  const [decisions, setDecisions]   = useState<Record<string, ReviewDecision>>({});
  const [selected, setSelected]     = useState<Set<string>>(new Set());
  const [search, setSearch]         = useState("");
  const [reasonFilter, setReasonFilter] = useState<ReviewReason | "all">("all");
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [decisionsLoaded, setDecisionsLoaded] = useState(false);

  // Fetch emails + decisions (backend first, localStorage fallback)
  useEffect(() => {
    let cancelled = false;
    getReviewEmails(jobId)
      .then((q) => { if (!cancelled) setEmails(q.emails); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });

    getReviewDecisions(jobId)
      .then((d) => {
        if (cancelled) return;
        const fromBackend = d.decisions ?? {};
        if (Object.keys(fromBackend).length > 0) {
          setDecisions(fromBackend);
        } else {
          try {
            const saved = localStorage.getItem(`tp:review:${jobId}`);
            if (saved) setDecisions(JSON.parse(saved) as Record<string, ReviewDecision>);
          } catch { /* ignore */ }
        }
      })
      .catch(() => {
        try {
          const saved = localStorage.getItem(`tp:review:${jobId}`);
          if (saved && !cancelled) setDecisions(JSON.parse(saved) as Record<string, ReviewDecision>);
        } catch { /* ignore */ }
      })
      .finally(() => { if (!cancelled) setDecisionsLoaded(true); });

    return () => { cancelled = true; };
  }, [jobId]);

  // Persist decisions — localStorage (instant) + backend (debounced)
  useEffect(() => {
    if (!decisionsLoaded) return;
    try { localStorage.setItem(`tp:review:${jobId}`, JSON.stringify(decisions)); }
    catch { /* ignore */ }
    const handle = setTimeout(() => {
      saveReviewDecisions(jobId, decisions).catch(() => { /* best effort */ });
    }, 500);
    return () => clearTimeout(handle);
  }, [jobId, decisions, decisionsLoaded]);

  // Derived lists
  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return emails.filter((e) => {
      const matchSearch = !q || e.email.includes(q) || e.domain.includes(q);
      const matchReason = reasonFilter === "all" || e.reason === reasonFilter;
      return matchSearch && matchReason;
    });
  }, [emails, search, reasonFilter]);

  const visible = filtered.slice(0, visibleCount);

  const counts = useMemo(() => {
    const approved = Object.values(decisions).filter((d) => d === "approved").length;
    const removed  = Object.values(decisions).filter((d) => d === "removed").length;
    return { total: emails.length, approved, removed, pending: emails.length - approved - removed };
  }, [emails.length, decisions]);

  // Actions
  const decide = useCallback((id: string, d: ReviewDecision) => {
    setDecisions((prev) => ({ ...prev, [id]: d }));
    setSelected((prev) => { const s = new Set(prev); s.delete(id); return s; });
  }, []);

  const undecide = useCallback((id: string) => {
    setDecisions((prev) => { const n = { ...prev }; delete n[id]; return n; });
  }, []);

  const bulkDecide = useCallback((ids: string[], d: ReviewDecision) => {
    setDecisions((prev) => {
      const n = { ...prev };
      ids.forEach((id) => { n[id] = d; });
      return n;
    });
    setSelected(new Set());
  }, []);

  const toggleSelect = useCallback((id: string) => {
    setSelected((prev) => {
      const s = new Set(prev);
      s.has(id) ? s.delete(id) : s.add(id);
      return s;
    });
  }, []);

  const allVisibleSelected = visible.length > 0 &&
    visible.every((e) => selected.has(e.id));

  const toggleSelectAll = useCallback(() => {
    setSelected((prev) => {
      const s = new Set(prev);
      if (visible.every((e) => s.has(e.id))) {
        visible.forEach((e) => s.delete(e.id));
      } else {
        visible.forEach((e) => s.add(e.id));
      }
      return s;
    });
  }, [visible]);

  const pendingFiltered = useMemo(
    () => filtered.filter((e) => !decisions[e.id]),
    [filtered, decisions],
  );

  const bulkApproveAll = useCallback(() => {
    const ids = pendingFiltered.map((e) => e.id);
    if (ids.length) bulkDecide(ids, "approved");
  }, [pendingFiltered, bulkDecide]);

  const bulkRemoveAll = useCallback(() => {
    const ids = pendingFiltered.map((e) => e.id);
    if (ids.length) bulkDecide(ids, "removed");
  }, [pendingFiltered, bulkDecide]);

  return (
    <>
      {/* ── Header ──────────────────────────────────────────────── */}
      <div className="fade-up">
        <Topbar
          breadcrumb={[
            "WORKSPACE",
            { label: "RESULTS", href: `/results/${encodeURIComponent(jobId)}` },
            "REVIEW QUEUE",
          ]}
          title="REVIEW/QUEUE"
          titleSlice="/"
          meta={[
            { label: "JOB", value: jobId.slice(0, 14) + (jobId.length > 14 ? "…" : "") },
            { label: "TOTAL", value: String(counts.total) },
          ]}
        />
      </div>

      {/* ── Stats bar ───────────────────────────────────────────── */}
      <div className={`${styles.statsBar} fade-up`}>
        <div className={styles.stat}>
          <div className={styles.statValue}>{counts.total}</div>
          <div className={styles.statLabel}>Total</div>
        </div>
        <div className={`${styles.stat} ${styles.statPending}`}>
          <div className={styles.statValue}>{counts.pending}</div>
          <div className={styles.statLabel}>Pending</div>
        </div>
        <div className={`${styles.stat} ${styles.statApproved}`}>
          <div className={styles.statValue}>{counts.approved}</div>
          <div className={styles.statLabel}>→ Approved</div>
        </div>
        <div className={`${styles.stat} ${styles.statRemoved}`}>
          <div className={styles.statValue}>{counts.removed}</div>
          <div className={styles.statLabel}>✕ Removed</div>
        </div>
      </div>

      {/* ── Controls ────────────────────────────────────────────── */}
      <div className={`${styles.controls} fade-up`}>
        <input
          className={styles.searchInput}
          type="text"
          placeholder="Search email or domain…"
          value={search}
          onChange={(e) => { setSearch(e.target.value); setVisibleCount(PAGE_SIZE); }}
        />
        <select
          className={styles.filterSelect}
          value={reasonFilter}
          onChange={(e) => { setReasonFilter(e.target.value as ReviewReason | "all"); setVisibleCount(PAGE_SIZE); }}
        >
          <option value="all">All reasons</option>
          <option value="catch-all">Catch-all</option>
          <option value="role-based">Role-based</option>
          <option value="no-smtp">No SMTP</option>
        </select>
        <div className={styles.quickBtns}>
          {pendingFiltered.length > 0 && (
            <>
              <button className={styles.quickApprove} onClick={bulkApproveAll} type="button">
                Approve all ({pendingFiltered.length})
              </button>
              <button className={styles.quickRemove} onClick={bulkRemoveAll} type="button">
                Remove all ({pendingFiltered.length})
              </button>
            </>
          )}
        </div>
      </div>

      {/* ── Table ───────────────────────────────────────────────── */}
      <div className={`${styles.panel} fade-up`}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th className={`${styles.th} ${styles.thCheck}`}>
                <input
                  type="checkbox"
                  className={styles.checkbox}
                  checked={allVisibleSelected}
                  onChange={toggleSelectAll}
                  aria-label="Select all"
                />
              </th>
              <th className={styles.th}>Email</th>
              <th className={styles.th}>Reason</th>
              <th className={styles.th}>Conf.</th>
              <th className={styles.th}>Action</th>
            </tr>
          </thead>
          <tbody>
            {loading
              ? Array.from({ length: 8 }, (_, i) => <SkeletonRow key={i} />)
              : visible.length === 0
                ? (
                  <tr>
                    <td colSpan={5} className={styles.emptyCell}>
                      No emails match your filters.
                    </td>
                  </tr>
                )
                : visible.map((e) => (
                  <EmailRow
                    key={e.id}
                    email={e}
                    decision={decisions[e.id] ?? null}
                    selected={selected.has(e.id)}
                    expanded={expandedId === e.id}
                    onSelect={() => toggleSelect(e.id)}
                    onDecide={(d) => decide(e.id, d)}
                    onUndo={() => undecide(e.id)}
                    onToggleDetails={() =>
                      setExpandedId((prev) => (prev === e.id ? null : e.id))
                    }
                  />
                ))
            }
          </tbody>
        </table>

        {!loading && filtered.length > visibleCount && (
          <div className={styles.tableFooter}>
            <span className={styles.footerCount}>
              Showing {visibleCount} of {filtered.length}
            </span>
            <button
              className={styles.loadMoreBtn}
              onClick={() => setVisibleCount((n) => n + PAGE_SIZE)}
              type="button"
            >
              Load {Math.min(PAGE_SIZE, filtered.length - visibleCount)} more
            </button>
          </div>
        )}
      </div>

      {/* ── Footer actions ──────────────────────────────────────── */}
      <div
        className="fade-up"
        style={{
          marginBottom: 40,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          flexWrap: "wrap",
        }}
      >
        <Link href={`/results/${encodeURIComponent(jobId)}`} className={styles.backLink}>
          ← Back to results
        </Link>
        <a
          href={reviewExportUrl(jobId)}
          className={styles.finalExportBtn}
          download
        >
          ↓ Download final list (after review)
        </a>
      </div>

      {/* ── Floating bulk action bar ─────────────────────────────── */}
      {selected.size > 0 && (
        <div className={styles.bulkBar}>
          <span className={styles.bulkCount}>{selected.size} selected</span>
          <button
            className={styles.bulkApprove}
            onClick={() => bulkDecide([...selected], "approved")}
            type="button"
          >
            Approve {selected.size}
          </button>
          <button
            className={styles.bulkRemove}
            onClick={() => bulkDecide([...selected], "removed")}
            type="button"
          >
            Remove {selected.size}
          </button>
          <button
            className={styles.bulkClear}
            onClick={() => setSelected(new Set())}
            type="button"
          >
            Clear
          </button>
        </div>
      )}
    </>
  );
}
