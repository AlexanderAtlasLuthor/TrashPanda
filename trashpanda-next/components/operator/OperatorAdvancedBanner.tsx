"use client";

import Link from "next/link";
import styles from "./OperatorAdvancedBanner.module.css";

/**
 * Sticky "you probably want Home" warning surfaced at the top of
 * every /operator/* page. The screens here use a dense, jargon-heavy
 * layout (preflight inputs, audience contracts, manifest gates, etc.)
 * that is intentionally meant for the auditor / founder workflow,
 * NOT for "I just want to clean a list".
 *
 * Without this banner first-time operators land on /operator/preflight
 * via the sidebar, see "// PREFLIGHT INPUT" + "/data/incoming/...csv"
 * placeholders, and freeze. The banner explicitly redirects them to
 * Home where the giant Send-to-client button lives.
 */
export function OperatorAdvancedBanner() {
  return (
    <div className={styles.banner} role="note">
      <div className={styles.iconWrap} aria-hidden>
        ⚠
      </div>
      <div className={styles.copy}>
        <div className={styles.headline}>
          You probably don&apos;t need this page.
        </div>
        <div className={styles.body}>
          The Operator Console is the advanced audit surface — manifests,
          gates, SMTP runtime, partial deliveries. To clean a list and
          get a ZIP for your client, go to{" "}
          <Link href="/" className={styles.homeLink}>
            Home
          </Link>{" "}
          and drop your file. The big green{" "}
          <strong>Send to client</strong> button on the results page does
          everything this section does, in one click.
        </div>
      </div>
      <Link href="/" className={styles.cta}>
        ← Back to Home
      </Link>
    </div>
  );
}
