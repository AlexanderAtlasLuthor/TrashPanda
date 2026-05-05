import type {
  ClientPackageManifest,
  ClientPackageWarning,
} from "@/lib/types";
import { ArtifactAudienceBadge } from "./ArtifactAudienceBadge";
import { StatusBadge } from "./StatusBadge";
import { IssuesList } from "./IssuesList";
import {
  OperatorEmptyState,
  OperatorErrorState,
  OperatorLoadingState,
} from "./OperatorPanelStates";
import styles from "./ClientPackageCard.module.css";

interface ClientPackageCardProps {
  jobStatus?: string | null;
  manifest: ClientPackageManifest | null;
  loading?: boolean;
  error?: string | null;
  reviewStaleSinceBuild?: boolean;
  onBuildPackage: () => Promise<void> | void;
}

function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes)) return "—";
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  return `${mb.toFixed(2)} MB`;
}

function warningsToIssueLikes(
  warnings: ClientPackageWarning[] | undefined,
): Array<{ severity?: string | null; code?: string | null; message?: string | null }> {
  if (!warnings) return [];
  return warnings.map((w) => ({
    severity: w.severity ?? "warn",
    code: w.code,
    message: w.message,
  }));
}

export function ClientPackageCard({
  jobStatus,
  manifest,
  loading,
  error,
  reviewStaleSinceBuild,
  onBuildPackage,
}: ClientPackageCardProps) {
  const buildLabel = manifest ? "Rebuild package" : "Build package";
  const jobStatusKnown = jobStatus !== undefined && jobStatus !== null;
  // Disable only when job status is known AND it's not "completed".
  // If the status is unknown, allow the click but show a caveat below
  // — the backend will reject if the pipeline isn't done.
  const buildDisabled =
    !!loading || (jobStatusKnown && jobStatus !== "completed");

  const includedFiles = manifest?.files_included ?? [];
  const excludedFiles = manifest?.files_excluded ?? [];
  const warnings = manifest?.warnings ?? [];

  return (
    <section className={styles.panel} aria-label="Client package">
      <header className={styles.header}>
        <div className={styles.titleWrap}>
          <div className={styles.eyebrow}>// CLIENT PACKAGE</div>
          <div className={styles.title}>
            {manifest?.package_name ?? "Client delivery package"}
          </div>
        </div>
        <div className={styles.headMeta}>
          <StatusBadge status={manifest?.status ?? "missing"} />
          {manifest?.generated_at && (
            <span className={styles.timestamp}>
              {manifest.generated_at}
            </span>
          )}
        </div>
      </header>

      {loading && (
        <OperatorLoadingState message="Building / refreshing client package…" />
      )}

      {error && !loading && (
        <OperatorErrorState
          title="Could not load the package."
          message={error}
        />
      )}

      {!loading && !error && !manifest && (
        <OperatorEmptyState
          title="No client package built yet."
          message="Build the client_delivery_package before running the operator review gate."
        />
      )}

      {!loading && !error && manifest && (
        <>
          {reviewStaleSinceBuild && (
            <div className={styles.staleNote}>
              Package was rebuilt — re-run the gate before delivery.
            </div>
          )}

          <div className={styles.section}>
            <div className={styles.sectionHead}>
              <span className={styles.sectionLabel}>
                Included ({includedFiles.length})
              </span>
            </div>
            {includedFiles.length === 0 ? (
              <div className={styles.empty}>No files in the package.</div>
            ) : (
              <ul className={styles.fileList}>
                {includedFiles.map((file) => (
                  <li key={file.filename} className={styles.fileRow}>
                    <span className={styles.filename}>{file.filename}</span>
                    <ArtifactAudienceBadge audience={file.audience} />
                    <span className={styles.size}>
                      {formatBytes(file.size_bytes)}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {excludedFiles.length > 0 && (
            <details className={styles.section}>
              <summary className={styles.sectionHeadCollapsible}>
                <span className={styles.sectionLabel}>
                  Excluded ({excludedFiles.length})
                </span>
              </summary>
              <ul className={styles.fileList}>
                {excludedFiles.map((file, idx) => (
                  <li
                    key={`${file.filename}-${idx}`}
                    className={styles.fileRow}
                  >
                    <span className={styles.filename}>{file.filename}</span>
                    <ArtifactAudienceBadge audience={file.audience} />
                    <span className={styles.reason}>
                      {file.reason ?? "—"}
                    </span>
                  </li>
                ))}
              </ul>
            </details>
          )}

          <div className={styles.section}>
            <div className={styles.sectionHead}>
              <span className={styles.sectionLabel}>
                Warnings ({warnings.length})
              </span>
            </div>
            <IssuesList
              issues={warningsToIssueLikes(warnings)}
              emptyLabel="No warnings."
            />
          </div>
        </>
      )}

      <footer className={styles.footer}>
        {!jobStatusKnown && (
          <div className={styles.caveat}>
            Job status unknown — build may fail if the pipeline has not finished.
          </div>
        )}
        <button
          type="button"
          className={styles.buildBtn}
          disabled={buildDisabled}
          onClick={() => {
            void onBuildPackage();
          }}
        >
          {loading ? "Working…" : buildLabel}
        </button>
      </footer>
    </section>
  );
}
