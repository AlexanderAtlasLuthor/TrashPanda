import type { JobArtifacts } from "@/lib/types";
import { CLIENT_OUTPUT_MANIFEST } from "@/lib/types";
import { artifactDownloadUrl } from "@/lib/api";
import styles from "./DownloadArtifacts.module.css";

interface DownloadArtifactsProps {
  jobId: string;
  artifacts: JobArtifacts | null | undefined;
}

export function DownloadArtifacts({ jobId, artifacts }: DownloadArtifactsProps) {
  const clientOutputs = artifacts?.client_outputs ?? {};
  const technical = artifacts?.technical_csvs ?? {};
  const reports = artifacts?.reports ?? {};

  // Merge technical_csvs + reports into a single "technical" list.
  const technicalEntries: Array<{ key: string; filename: string }> = [
    ...Object.entries(technical)
      .filter(([, v]) => !!v)
      .map(([key, filename]) => ({ key, filename: filename as string })),
    ...Object.entries(reports)
      .filter(([, v]) => !!v)
      .map(([key, filename]) => ({ key, filename: filename as string })),
  ];

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.title}>DOWNLOADS</div>
        <div className={styles.badge}>
          {CLIENT_OUTPUT_MANIFEST.length} client outputs
        </div>
      </div>

      <div className={styles.body}>
        {CLIENT_OUTPUT_MANIFEST.map((item) => {
          const filename = clientOutputs[item.key] ?? item.filename;
          const available = !!clientOutputs[item.key];
          const itemClass = [
            styles.item,
            styles[item.severity],
            !available && styles.itemDisabled,
          ]
            .filter(Boolean)
            .join(" ");

          return (
            <a
              key={item.key}
              href={available ? artifactDownloadUrl(jobId, item.key) : undefined}
              download={filename}
              className={itemClass}
              aria-disabled={!available}
            >
              <div className={styles.icon}>XLSX</div>
              <div className={styles.info}>
                <div className={styles.label}>{item.label}</div>
                <div className={styles.filename}>{filename}</div>
                <div className={styles.description}>{item.description}</div>
              </div>
              <div className={styles.arrow}>
                <svg viewBox="0 0 24 24" aria-hidden>
                  <path d="M12 3v12M6 9l6 6 6-6" />
                  <path d="M3 21h18" />
                </svg>
              </div>
            </a>
          );
        })}

        {technicalEntries.length > 0 && (
          <div className={styles.technical} style={{ gridColumn: "1 / -1" }}>
            <div className={styles.techLabel}>// Technical outputs</div>
            <div className={styles.techList}>
              {technicalEntries.map((t) => (
                <a
                  key={t.key}
                  href={artifactDownloadUrl(jobId, t.key)}
                  download={t.filename}
                  className={styles.techItem}
                >
                  {t.filename}
                </a>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
