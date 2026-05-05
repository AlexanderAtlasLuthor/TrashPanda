import type { OperatorReviewSummary } from "@/lib/types";
import { StatusBadge } from "./StatusBadge";
import { IssuesList } from "./IssuesList";
import {
  OperatorEmptyState,
  OperatorErrorState,
  OperatorLoadingState,
} from "./OperatorPanelStates";
import { OPERATOR_SAFETY_COPY } from "./SafetyBanner";
import styles from "./OperatorReviewPanel.module.css";

interface OperatorReviewPanelProps {
  review: OperatorReviewSummary | null;
  loading?: boolean;
  error?: string | null;
  reviewStaleSinceBuild?: boolean;
  onRunGate: () => Promise<void> | void;
}

export function OperatorReviewPanel({
  review,
  loading,
  error,
  reviewStaleSinceBuild,
  onRunGate,
}: OperatorReviewPanelProps) {
  const buttonLabel = review ? "Re-run gate" : "Run gate";
  const isWarn =
    (review?.status ?? "").toString().trim().toLowerCase() === "warn";

  return (
    <section className={styles.panel} aria-label="Operator review gate">
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// OPERATOR REVIEW GATE</div>
          <div className={styles.title}>Delivery readiness</div>
        </div>
        <div className={styles.headMeta}>
          <StatusBadge status={review?.status ?? "missing"} />
          {review?.generated_at && (
            <span className={styles.timestamp}>{review.generated_at}</span>
          )}
        </div>
      </header>

      {loading && (
        <OperatorLoadingState message="Running review gate…" />
      )}

      {error && !loading && (
        <OperatorErrorState
          title="Could not load the review summary."
          message={error}
        />
      )}

      {!loading && !error && !review && (
        <OperatorEmptyState
          title="Operator review has not run yet."
          message="Run the gate after building the client package."
        />
      )}

      {!loading && !error && review && (
        <>
          <div
            className={[
              styles.readyRow,
              review.ready_for_client === true ? styles.readyTrue : styles.readyFalse,
            ].join(" ")}
            data-ready={review.ready_for_client ? "true" : "false"}
          >
            <span className={styles.readyLabel}>ready_for_client:</span>
            <span className={styles.readyValue}>
              {review.ready_for_client === true ? "true" : "false"}
            </span>
          </div>

          {isWarn && (
            <div className={styles.warnNote}>{OPERATOR_SAFETY_COPY[0]}</div>
          )}

          {reviewStaleSinceBuild && (
            <div className={styles.staleNote}>
              Package was rebuilt — re-run the gate before delivery.
            </div>
          )}

          {(review.reviewed_files !== null && review.reviewed_files !== undefined) ||
          (review.blocked_files !== null && review.blocked_files !== undefined) ||
          (review.warnings_count !== null && review.warnings_count !== undefined) ? (
            <div className={styles.statsRow}>
              {review.reviewed_files !== null &&
                review.reviewed_files !== undefined && (
                  <div className={styles.stat}>
                    <div className={styles.statValue}>
                      {review.reviewed_files}
                    </div>
                    <div className={styles.statLabel}>Reviewed files</div>
                  </div>
                )}
              {review.blocked_files !== null &&
                review.blocked_files !== undefined && (
                  <div className={styles.stat}>
                    <div className={[styles.statValue, styles.bad].join(" ")}>
                      {review.blocked_files}
                    </div>
                    <div className={styles.statLabel}>Blocked files</div>
                  </div>
                )}
              {review.warnings_count !== null &&
                review.warnings_count !== undefined && (
                  <div className={styles.stat}>
                    <div className={[styles.statValue, styles.warn].join(" ")}>
                      {review.warnings_count}
                    </div>
                    <div className={styles.statLabel}>Warnings</div>
                  </div>
                )}
            </div>
          ) : null}

          <div className={styles.section}>
            <div className={styles.sectionLabel}>
              Issues ({(review.issues ?? []).length})
            </div>
            <IssuesList
              issues={review.issues}
              emptyLabel="No issues from the gate."
            />
          </div>
        </>
      )}

      <footer className={styles.footer}>
        <button
          type="button"
          className={styles.gateBtn}
          disabled={!!loading}
          onClick={() => {
            void onRunGate();
          }}
        >
          {loading ? "Working…" : buttonLabel}
        </button>
      </footer>
    </section>
  );
}
