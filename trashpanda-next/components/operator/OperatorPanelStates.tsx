import styles from "./OperatorPanelStates.module.css";

interface OperatorEmptyStateProps {
  title: string;
  message?: string;
}

export function OperatorEmptyState({ title, message }: OperatorEmptyStateProps) {
  return (
    <div className={[styles.state, styles.empty].join(" ")} role="status">
      <div className={styles.title}>{title}</div>
      {message && <div className={styles.message}>{message}</div>}
    </div>
  );
}

interface OperatorErrorStateProps {
  title?: string;
  message: string;
}

export function OperatorErrorState({
  title = "Could not load.",
  message,
}: OperatorErrorStateProps) {
  return (
    <div className={[styles.state, styles.error].join(" ")} role="alert">
      <div className={styles.title}>{title}</div>
      <div className={styles.message}>{message}</div>
    </div>
  );
}

interface OperatorLoadingStateProps {
  message?: string;
}

export function OperatorLoadingState({
  message = "Loading…",
}: OperatorLoadingStateProps) {
  return (
    <div className={[styles.state, styles.loading].join(" ")} role="status">
      <div className={styles.spinner} aria-hidden="true" />
      <div className={styles.message}>{message}</div>
    </div>
  );
}
