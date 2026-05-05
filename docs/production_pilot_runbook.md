# TrashPanda V2 Production Pilot Runbook

> **Audience:** operator running a real V2 deliverability pilot in production.
> **Source of truth for client delivery:** `app.client_package_builder.build_client_delivery_package(...)`.
> Everything else is operator-facing.

---

## 1. Purpose

This runbook is the safe execution path for a V2 pilot run. It ties
together preflight, pipeline execution, client package assembly, the
operator review gate, and post-campaign feedback ingestion — and tells
you what is and isn't safe to share with the end client.

The goal is simple: **never send an operator-only artifact to a client,
and never deliver a package the operator review gate has not blessed
as `ready`.**

## 2. When to use this runbook

- Before each new client list, especially first-time pilots.
- After a config change that affects SMTP, classification thresholds,
  or rollout profile.
- When you are not 100% sure of the right `--config` profile to use.
- Whenever you would otherwise be tempted to copy the `/results/{job_id}`
  payload and forward it to a client.

## 3. Preflight checklist

Run the V2.9.2 preflight before the cleaning job:

```python
from app.api_boundary import run_rollout_preflight

result = run_rollout_preflight(
    input_path="path/to/list.csv",
    operator_confirmed_large_run=False,
    smtp_port_verified=False,
)
```

A `block` result must STOP the run. A `warn` result must be reviewed —
do not bypass it without an explicit operator confirmation.

Items the preflight checks:

- input file exists and is readable
- input row count vs the configured large-run cap
- SMTP port reachable when `smtp_probe.enabled`
- rollout config profile is sane

## 4. Recommended pilot size

- **First run on a new client list:** ≤ 500 rows.
- **Second pilot after operator review = ready:** up to a few thousand.
- **Full-volume run:** only after at least one full preflight → run →
  review → feedback cycle has cleared.

Do NOT skip these steps for "just a small list" — the safeguards exist
so the small list can't quietly become a big one over time.

## 5. SMTP prerequisites

If you turn SMTP probing on for the run, every one of these must be true:

- `smtp_probe.enabled = true` in the chosen rollout profile.
- A valid `sender_address` configured.
- `max_per_domain` cap is set and > 0.
- Outbound port 25 is reachable from the runner.
- You have explicit permission to probe the destination domains.

> **Do not run uncapped live SMTP on a large list.** Uncapped SMTP from
> a fresh IP gets you blocklisted and burns the sender reputation of
> every future campaign. Always cap, always rate-limit.

If any of those is unclear, set `smtp_probe.enabled = false` for the
pilot and rely on V2 classification + history alone.

## 6. Recommended rollout config

Use the V2.9.1 rollout profile that matches the run:

- `rollout/safe_pilot.yaml` (or equivalent low-risk preset) for first runs.
- Default config (`configs/default.yaml`) only for runs the operator has
  manually validated.

Profile choice is the single biggest lever for safe behavior — make it
deliberate, not accidental.

## 7. Run sequence

The complete operator flow is:

1. **Preflight** — `run_rollout_preflight(input_path)`.
2. **Pipeline** — `run_cleaning_job(input_path, output_root)` (V2 path).
3. **Build client package** — `build_client_delivery_package(run_dir)`.
4. **Operator review gate** — `run_operator_review_gate(run_dir)`.
5. **Deliver only the client package** — and only if status is `ready`.
6. **(Post-campaign)** — `ingest_bounce_feedback(...)` and the V2.9.8
   feedback domain intel preview.

Skip step 4 at your peril. The gate exists because reading raw counts
from the pipeline result is not enough to judge a delivery is safe.

## 8. Build client package

```python
from app.api_boundary import build_client_package_for_job

payload = build_client_package_for_job(run_dir)
# payload["package_dir"] is what you ship.
```

The builder is the **delivery source of truth**. It reads
`is_client_safe_artifact(...)` for every file in the run directory and
copies only `client_safe` artifacts into
`<run_dir>/client_delivery_package/`. It also writes
`client_package_manifest.json` describing what was included, what was
excluded, and any warnings.

What the package can contain (when present):

- `valid_emails.xlsx`
- `review_emails.xlsx`
- `invalid_or_bounce_risk.xlsx`
- `duplicate_emails.xlsx`
- `hard_fail_emails.xlsx`
- `summary_report.xlsx`
- `approved_original_format.xlsx`
- `client_package_manifest.json`

If a file you expect to see is in `files_excluded` of the manifest, the
contract says it is not client-safe. Trust the contract — do not
manually re-add it.

## 9. Run operator review gate

```python
from app.api_boundary import run_operator_review_for_job

decision = run_operator_review_for_job(run_dir)
# decision["status"] in {"ready", "warn", "block"}
# decision["ready_for_client"] is True ONLY when status == "ready"
```

The gate writes `operator_review_summary.json` next to the run.

| `status` | `ready_for_client` | Action |
|---|---|---|
| `ready` | `True` | Safe to deliver the client package. |
| `warn` | `False` | **Not ready_for_client.** Review every issue before deciding. |
| `block` | `False` | STOP. Do not deliver. Resolve the block(s) and re-run. |

A warn status is not ready_for_client. Do not promote a `warn`
package to `ready` without first explaining each warning to the operator
of record and getting explicit override authorization.

## 10. What to send to the client

Only the contents of `<run_dir>/client_delivery_package/`. Nothing else.

In particular:

- Send the directory or a zip of the directory.
- Do not link the operator UI or the `/results/{job_id}` URL.
- Do not paste the raw pipeline result JSON.
- Do not include any `.json` or `.csv` from the run dir except those
  inside the package.

## 11. What not to send to the client

The following artifacts exist in the run dir but are **operator-only**
or **technical debug** per the V2.9.5 artifact contract:

**Operator-only**
- `v2_deliverability_summary.json`, `v2_reason_breakdown.csv`,
  `v2_domain_risk_summary.csv`, `v2_probability_distribution.csv`
- `smtp_runtime_summary.json`
- `artifact_consistency.json`
- `operator_review_summary.json`
- `feedback_domain_intel_preview.json`
- `processing_report.json`, `processing_report.csv`, `domain_summary.csv`

**Technical debug**
- `clean_high_confidence.csv`, `review_medium_confidence.csv`
- `removed_invalid.csv`, `removed_duplicates.csv`, `removed_hard_fail.csv`
- `typo_corrections.csv`, `duplicate_summary.csv`

**Internal-only**
- `staging.sqlite3`
- `runtime/history/*.sqlite`, `runtime/feedback/*.sqlite`
- Any `*.log`, `*.tmp`, `logs/` directory

V2 reports are operator-only — they exist to help the operator make
delivery and routing decisions, not to brief the client.

Special note on the operator UI: **the `/results/{job_id}` HTTP
endpoint is the operator UI's view, not the client delivery contract.**
Some keys it lists (e.g. `processing_report_*`, `domain_summary`,
`typo_corrections`, `duplicate_summary`) are operator-only or
technical-debug per the contract. Never share that URL or its payload
with a client.

## 12. If safe export is empty

It is possible — and acceptable — for `approved_original_format.xlsx`
to be absent if no rows in the input cleared as safe.

When that happens:

- The client package builder emits warning
  `approved_original_format_absent`.
- The operator review gate raises warning `approved_original_absent`,
  and possibly `safe_count_zero` if the underlying valid count is 0.
- Status will be `warn`, not `ready`.

Do not deliver an empty package to plug the gap. Escalate to the
operator of record:

- Was the input list correct?
- Was the rollout profile too strict?
- Are there review-cohort rows that should be hand-graded into
  `approved_original_format.xlsx`?

## 13. How to ingest feedback after campaign

After the client runs the campaign, ingest the bounce/delivery outcomes:

```python
from app.api_boundary import ingest_bounce_feedback

summary = ingest_bounce_feedback("path/to/bounce_outcomes.csv")
```

Feedback is stored in `runtime/feedback/bounce_outcomes.sqlite`
(V2.7) and is independent from the cleaning pipeline. The feedback
store is **not** read by the active classification path in V2.9.x —
it accumulates safely until a future subphase wires it in.

## 14. How to generate feedback domain intel preview

V2.9.8 adds an out-of-band preview that shows how V2.7 feedback
**would** shape V2.6 domain intelligence in a future run, without
changing classification today:

```python
from app.api_boundary import build_feedback_domain_intel_preview_for_job

preview = build_feedback_domain_intel_preview_for_job(
    "runtime/feedback/bounce_outcomes.sqlite",
    output_dir="<run_dir>",
)
```

The preview is written to `feedback_domain_intel_preview.json` (operator-only).

> **The feedback preview does not affect decisions yet.** It is purely
> informational. Reading it should change how an operator chooses
> rollout profiles and review thresholds, not the runtime classifier.

## 15. Known caveats

- **Operator UI is not the client delivery contract.** The
  `/results/{job_id}` endpoint surfaces operator-only and technical-debug
  artifacts. See section 11.
- **V2 risk metrics in the operator review gate** read both flat top-level
  and nested V2.8 shapes (`catch_all_summary.*`,
  `domain_intelligence_summary.*`). If you produce custom V2 summary
  files, match one of those shapes.
- **`approved_original_format.xlsx` may be absent** when no rows clear
  as safe. This is a warning condition, not a failure mode (section 12).
- **The feedback preview does not yet feed `domain_intel_cache`.** A
  future subphase will wire it in; today it is preview-only.
- **SMTP probing** is opt-in per rollout profile. Never run uncapped
  live SMTP (section 5).
- **Windows file handles on corrupt SQLite stores** were a real bleed
  through V2.9.8; V2.9.9 fixed `BounceOutcomeStore.__init__` to close
  the handle on schema-init failure.

## 16. Rollback / stop conditions

STOP the pilot — do not deliver — when any of these is true:

- Preflight returned `block`.
- Operator review gate returned `block`.
- `artifact_consistency.materialized_outputs_mutated_after_reports == true`.
- `client_package_contains_non_client_safe` issue surfaced.
- SMTP coverage < 80% on a run that was supposed to use SMTP probing.
- The run dir is missing `client_delivery_package/` or
  `client_package_manifest.json`.
- Anyone is unsure whether a file is client-safe — assume operator-only
  and verify via `is_client_safe_artifact(...)`.

When you STOP, leave the run dir intact for forensics. Do not delete
artifacts before the operator of record has reviewed them.

---

*Generated for the TrashPanda V2.9.9 housekeeping cut. Update when
classification thresholds, the artifact contract, or the rollout config
profiles change.*
