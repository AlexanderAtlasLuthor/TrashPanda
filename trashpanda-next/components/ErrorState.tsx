import Link from "next/link";
import type { JobError } from "@/lib/types";
import styles from "./ErrorState.module.css";

interface ErrorStateProps {
  error: JobError | null | undefined;
  jobId?: string;
  retryHref?: string;
}

export function ErrorState({ error, jobId, retryHref = "/" }: ErrorStateProps) {
  const message =
    error?.message ??
    "Something went wrong while cleaning this file. Try uploading it again.";
  const errorType = error?.error_type ?? "UnknownError";

  return (
    <div className={styles.panel}>
      <div className={styles.head}>
        <div className={styles.icon}>!</div>
        <div className={styles.titleWrap}>
          <div className={styles.subtitle}>// JOB FAILED</div>
          <div className={styles.title}>The panda couldn&apos;t clean this one</div>
        </div>
      </div>

      <div className={styles.message}>{message}</div>

      <div className={styles.errorMeta}>
        <div className={styles.metaKey}>Error type</div>
        <div className={styles.metaVal}>{errorType}</div>
        {jobId && (
          <>
            <div className={styles.metaKey}>Job ID</div>
            <div className={styles.metaVal}>{jobId}</div>
          </>
        )}
      </div>

      <div className={styles.actions}>
        <Link href={retryHref} className={styles.btnPrimary}>
          TRY AGAIN
        </Link>
        <Link href="/" className={styles.btnGhost}>
          BACK TO CONSOLE
        </Link>
      </div>
    </div>
  );
}
