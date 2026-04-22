"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import {
  getAIReviewSuggestions,
  getReviewDecisions,
  getReviewEmails,
  reviewExportUrl,
  saveReviewDecisions,
  type AIReviewSuggestion,
} from "@/lib/api";
import { Topbar } from "@/components/Topbar";
import type { ReviewDecision, ReviewEmail, ReviewReason } from "@/lib/types";
import styles from "./ReviewQueue.module.css";

type AISuggestionMap = Record<string, AIReviewSuggestion>;
type AIState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "ready"; suggestions: AISuggestionMap }
  | { status: "error"; message: string };

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

function AISuggestionBadge({ suggestion }: { suggestion: AIReviewSuggestion }) {
  const cls =
    suggestion.decision === "approve"
      ? styles.aiBadgeApprove
      : suggestion.decision === "reject"
        ? styles.aiBadgeReject
        : styles.aiBadgeUncertain;
  const label =
    suggestion.decision === "approve"
      ? "Approve"
      : suggestion.decision === "reject"
        ? "Reject"
        : "Uncertain";
  const pct = Math.round(suggestion.confidence * 100);
  return (
    <span
      className={`${styles.aiBadge} ${cls}`}
      title={suggestion.reasoning}
      aria-label={`AI suggests ${label}, ${pct}% confidence. ${suggestion.reasoning}`}
    >
      AI: {label} · {pct}%
    </span>
  );
}

interface RowProps {
  email: ReviewEmail;
  decision: ReviewDecision | null;
  selected: boolean;
  expanded: boolean;
  suggestion: AIReviewSuggestion | null;
  onSelect: () => void;
  onDecide: (d: ReviewDecision) => void;
  onUndo: () => void;
  onToggleDetails: () => void;
}

function EmailRow({
  email, decision, selected, expanded, suggestion,
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
          {suggestion && <AISuggestionBadge suggestion={suggestion} />}
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

// ── Domain group header ──────────────────────────────────────────────────

interface GroupBlockProps {
  group: {
    domain: string;
    emails: ReviewEmail[];
    pendingCount: number;
    reasons: Record<ReviewReason, number>;
    dominantReason: ReviewReason;
  };
  expanded: boolean;
  onToggle: () => void;
  onApproveAll: () => void;
  onRemoveAll: () => void;
  decisions: Record<string, ReviewDecision>;
  selected: Set<string>;
  expandedId: string | null;
  suggestions: AISuggestionMap | null;
  onSelect: (id: string) => void;
  onDecide: (id: string, d: ReviewDecision) => void;
  onUndo: (id: string) => void;
  onToggleDetails: (id: string) => void;
}

function GroupBlock({
  group,
  expanded,
  onToggle,
  onApproveAll,
  onRemoveAll,
  decisions,
  selected,
  expandedId,
  suggestions,
  onSelect,
  onDecide,
  onUndo,
  onToggleDetails,
}: GroupBlockProps) {
  const { domain, emails, pendingCount, reasons, dominantReason } = group;

  // Build a short human summary: "No SMTP", "Catch-all + Role-based", etc.
  const reasonSummary = (Object.entries(reasons) as [ReviewReason, number][])
    .filter(([, n]) => n > 0)
    .sort((a, b) => b[1] - a[1])
    .map(([r]) => REASON_LABELS[r])
    .join(" · ");

  return (
    <>
      <tr className={`${styles.groupRow} ${expanded ? styles.groupRowOpen : ""}`}>
        <td className={styles.groupCell} colSpan={5}>
          <div className={styles.groupRowInner}>
            <button
              type="button"
              className={styles.groupToggle}
              onClick={onToggle}
              aria-expanded={expanded}
              aria-label={`${expanded ? "Collapse" : "Expand"} ${domain}`}
            >
              <span
                className={`${styles.groupCaret} ${expanded ? styles.groupCaretOpen : ""}`}
                aria-hidden
              >
                ▸
              </span>
              <span className={styles.groupDomain}>{domain}</span>
              <span className={styles.groupCount}>
                {emails.length} email{emails.length !== 1 ? "s" : ""}
              </span>
              <ReasonPill reason={dominantReason} />
              {reasonSummary !== REASON_LABELS[dominantReason] && (
                <span className={styles.groupReasonExtra}>{reasonSummary}</span>
              )}
              {pendingCount < emails.length && (
                <span className={styles.groupDecided}>
                  {emails.length - pendingCount} decided
                </span>
              )}
            </button>
            {pendingCount > 0 && (
              <div className={styles.groupActions}>
                <button
                  type="button"
                  className={styles.groupApprove}
                  onClick={onApproveAll}
                >
                  Approve {pendingCount}
                </button>
                <button
                  type="button"
                  className={styles.groupRemove}
                  onClick={onRemoveAll}
                >
                  Remove {pendingCount}
                </button>
              </div>
            )}
          </div>
        </td>
      </tr>
      {expanded &&
        emails.map((e) => (
          <EmailRow
            key={e.id}
            email={e}
            decision={decisions[e.id] ?? null}
            selected={selected.has(e.id)}
            expanded={expandedId === e.id}
            suggestion={suggestions?.[e.id] ?? null}
            onSelect={() => onSelect(e.id)}
            onDecide={(d) => onDecide(e.id, d)}
            onUndo={() => onUndo(e.id)}
            onToggleDetails={() => onToggleDetails(e.id)}
          />
        ))}
    </>
  );
}

// ── Main component ────────────────────────────────────────────────────────

type ViewMode = "grouped" | "flat";

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
  // Default to grouped: a queue of 21 "No SMTP / Medium" rows compresses
  // into ~5 domain buckets and the repeating pattern becomes obvious.
  const [viewMode, setViewMode]     = useState<ViewMode>("grouped");
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [aiState, setAiState]       = useState<AIState>({ status: "idle" });

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

  // Group filtered emails by domain for the default "grouped" view.
  // Each group carries the dominant reason + counts by reason so we can
  // render a one-line summary that collapses N rows of the same kind.
  interface DomainGroup {
    domain: string;
    emails: ReviewEmail[];
    pendingCount: number;
    reasons: Record<ReviewReason, number>;
    dominantReason: ReviewReason;
  }

  const groups = useMemo<DomainGroup[]>(() => {
    const byDomain = new Map<string, ReviewEmail[]>();
    for (const e of filtered) {
      const arr = byDomain.get(e.domain);
      if (arr) arr.push(e);
      else byDomain.set(e.domain, [e]);
    }
    const list: DomainGroup[] = [];
    for (const [domain, groupEmails] of byDomain.entries()) {
      const reasons = { "catch-all": 0, "role-based": 0, "no-smtp": 0 } as Record<ReviewReason, number>;
      let pendingCount = 0;
      for (const e of groupEmails) {
        reasons[e.reason] = (reasons[e.reason] ?? 0) + 1;
        if (!decisions[e.id]) pendingCount += 1;
      }
      const dominantReason = (Object.entries(reasons) as [ReviewReason, number][])
        .sort((a, b) => b[1] - a[1])[0][0];
      list.push({ domain, emails: groupEmails, pendingCount, reasons, dominantReason });
    }
    // Biggest pending buckets first — that's where bulk action pays off.
    list.sort((a, b) => b.pendingCount - a.pendingCount || b.emails.length - a.emails.length);
    return list;
  }, [filtered, decisions]);

  const toggleGroup = useCallback((domain: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(domain)) next.delete(domain);
      else next.add(domain);
      return next;
    });
  }, []);

  const approveGroup = useCallback((group: DomainGroup) => {
    const ids = group.emails.filter((e) => !decisions[e.id]).map((e) => e.id);
    if (ids.length) bulkDecide(ids, "approved");
  }, [bulkDecide, decisions]);

  const removeGroup = useCallback((group: DomainGroup) => {
    const ids = group.emails.filter((e) => !decisions[e.id]).map((e) => e.id);
    if (ids.length) bulkDecide(ids, "removed");
  }, [bulkDecide, decisions]);

  // AI review — one call for the whole queue. Re-runnable, cached on the
  // server side (system prompt). Disabled while loading.
  const runAIReview = useCallback(async () => {
    setAiState({ status: "loading" });
    try {
      const result = await getAIReviewSuggestions(jobId);
      const map: AISuggestionMap = {};
      for (const s of result.suggestions) map[s.id] = s;
      setAiState({ status: "ready", suggestions: map });
    } catch (err) {
      const message = err instanceof Error ? err.message : "AI review failed.";
      setAiState({ status: "error", message });
    }
  }, [jobId]);

  const aiSuggestions =
    aiState.status === "ready" ? aiState.suggestions : null;

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
        <div
          className={styles.viewToggle}
          role="tablist"
          aria-label="View mode"
        >
          <button
            type="button"
            role="tab"
            aria-selected={viewMode === "grouped"}
            className={`${styles.viewBtn} ${viewMode === "grouped" ? styles.viewBtnActive : ""}`}
            onClick={() => setViewMode("grouped")}
          >
            By domain
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={viewMode === "flat"}
            className={`${styles.viewBtn} ${viewMode === "flat" ? styles.viewBtnActive : ""}`}
            onClick={() => setViewMode("flat")}
          >
            Flat list
          </button>
        </div>
        <div className={styles.quickBtns}>
          <button
            type="button"
            className={styles.aiRunBtn}
            onClick={runAIReview}
            disabled={aiState.status === "loading" || emails.length === 0}
            title="Stack-rank the queue with Claude Haiku — approve/reject/uncertain suggestions per row."
          >
            {aiState.status === "loading"
              ? "Running AI…"
              : aiState.status === "ready"
                ? "Re-run AI review"
                : "Run AI review"}
          </button>
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
      {aiState.status === "error" && (
        <div className={`${styles.aiNotice} fade-up`} role="alert">
          AI review failed: {aiState.message}
        </div>
      )}

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
            {loading ? (
              Array.from({ length: 8 }, (_, i) => <SkeletonRow key={i} />)
            ) : filtered.length === 0 ? (
              <tr>
                <td colSpan={5} className={styles.emptyCell}>
                  No emails match your filters.
                </td>
              </tr>
            ) : viewMode === "grouped" ? (
              groups.map((g) => (
                <GroupBlock
                  key={g.domain}
                  group={g}
                  expanded={expandedGroups.has(g.domain)}
                  onToggle={() => toggleGroup(g.domain)}
                  onApproveAll={() => approveGroup(g)}
                  onRemoveAll={() => removeGroup(g)}
                  decisions={decisions}
                  selected={selected}
                  expandedId={expandedId}
                  suggestions={aiSuggestions}
                  onSelect={toggleSelect}
                  onDecide={decide}
                  onUndo={undecide}
                  onToggleDetails={(id) =>
                    setExpandedId((prev) => (prev === id ? null : id))
                  }
                />
              ))
            ) : (
              visible.map((e) => (
                <EmailRow
                  key={e.id}
                  email={e}
                  decision={decisions[e.id] ?? null}
                  selected={selected.has(e.id)}
                  expanded={expandedId === e.id}
                  suggestion={aiSuggestions?.[e.id] ?? null}
                  onSelect={() => toggleSelect(e.id)}
                  onDecide={(d) => decide(e.id, d)}
                  onUndo={() => undecide(e.id)}
                  onToggleDetails={() =>
                    setExpandedId((prev) => (prev === e.id ? null : e.id))
                  }
                />
              ))
            )}
          </tbody>
        </table>

        {!loading && viewMode === "flat" && filtered.length > visibleCount && (
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

        {!loading && viewMode === "grouped" && groups.length > 0 && (
          <div className={styles.tableFooter}>
            <span className={styles.footerCount}>
              {groups.length} domain{groups.length !== 1 ? "s" : ""} · {filtered.length} email
              {filtered.length !== 1 ? "s" : ""}
            </span>
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
