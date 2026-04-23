# Core Product Definition

## Purpose

This document defines the product value that exists today in this repository.
It exists to protect the working product engine during the SaaS transformation.

This document is descriptive, not aspirational. It describes what the system
currently does, based on the codebase.

## What the product does today

TrashPanda is currently an advanced internal tool for contact-data hygiene and
email validation. It accepts CSV/XLSX contact datasets, processes them through
a staged pipeline, classifies records into usable/review/remove buckets, and
generates client-facing exports and technical reports.

The product value today is not authentication, billing, organizations, or SaaS
administration. Those layers do not exist yet.

The value today is the processing engine.

## Real customer value that exists today

The following capabilities create real value today:

- Ingesting CSV and XLSX files.
- Detecting and normalizing email/contact fields.
- Validating email syntax and structural quality.
- Extracting and evaluating email domains.
- Looking up DNS/MX signals.
- Suggesting conservative domain typo corrections.
- Classifying rows into output buckets.
- Deduplicating email records.
- Generating reviewable explanations and reasons.
- Producing client-facing XLSX exports.
- Producing technical CSV outputs and reports.
- Surfacing a review queue for ambiguous records.
- Surfacing deliverability/intelligence-style annotations for processed jobs.

## Core modules

The current product engine is primarily implemented in these modules:

| Area | Repo location | Role |
|---|---|---|
| Pipeline orchestration | `app/pipeline.py` | Main engine entry point through `EmailCleaningPipeline` |
| Stage engine | `app/engine/` | Generic stage execution, context, payloads, and stage abstractions |
| Stage implementations | `app/engine/stages/` | Preprocessing, email processing, enrichment, scoring, postprocessing |
| Input handling | `app/io_utils.py` | CSV/XLSX reading, chunked input handling, encoding fallback |
| Configuration | `app/config.py`, `configs/default.yaml` | Engine configuration defaults and runtime knobs |
| Normalization | `app/normalizers.py` | Contact/email/value normalization behavior |
| Validation rules | `app/validators.py`, `app/email_rules.py`, `app/rules.py` | Syntax and rule-level validation |
| DNS/MX enrichment | `app/dns_utils.py` | Domain DNS/MX signal collection |
| Deduplication | `app/dedupe.py` | Duplicate detection and winner selection |
| Baseline scoring | `app/scoring.py` | Existing deterministic scoring and bucket logic |
| Scoring V2 | `app/scoring_v2/` | Newer explainable scoring layer |
| Validation V2 | `app/validation_v2/` | Domain intelligence, SMTP-related signals, history, probability, decisions |
| Staging | `app/storage.py` | Per-run staging database for pipeline materialization |
| Client exports | `app/client_output.py` | Client-facing XLSX output generation |
| Reporting | `app/reporting.py` | Processing reports and technical summaries |
| API boundary | `app/api_boundary.py` | Boundary that calls the engine and serializes job results |

## Pipeline stages

The pipeline is stage-based. The exact implementation lives in `app/pipeline.py`
and `app/engine/stages/`.

The current staged flow includes these product behaviors:

1. Header normalization.
2. Structural validation.
3. Value normalization.
4. Technical metadata attachment.
5. Email syntax validation.
6. Domain extraction.
7. Typo correction/suggestion.
8. Domain comparison.
9. DNS enrichment.
10. Typo suggestion validation.
11. Baseline scoring.
12. V2 scoring annotation/comparison.
13. Completeness evaluation.
14. Email normalization.
15. Deduplication.
16. Staging persistence.
17. Materialization into output files.
18. Report/export generation.

These stages are core product behavior. Their ordering and semantics should not
be casually changed during SaaS conversion.

## Output structure

The system currently produces both technical and client-facing outputs.

### Technical outputs

Technical outputs are generated under a per-run output directory. They include
CSV/report artifacts such as:

- Clean/high-confidence records.
- Review/medium-confidence records.
- Removed/invalid or bounce-risk records.
- Processing reports.
- Domain summaries.
- Typo correction reports.
- Duplicate summaries.

The exact artifact collection is exposed through `collect_job_artifacts` and
the artifact endpoints in `app/server.py`.

### Client-facing outputs

Client-facing output generation is handled by `app/client_output.py`.

The current client output set includes:

- `valid_emails.xlsx`
- `review_emails.xlsx`
- `invalid_or_bounce_risk.xlsx`
- `summary_report.xlsx`
- `approved_original_format.xlsx` when original-format export can be produced

The client output contract is a core product surface and should be preserved
during SaaS transformation.

## Review queue meaning

The review queue represents records that the engine does not consider safe to
auto-approve or auto-remove without human judgment.

Review records may include:

- Role-based addresses.
- Catch-all or possible catch-all cases.
- No-SMTP or uncertain technical signals.
- Medium-confidence records.
- Records with typo suggestions.
- Records with conflicting or incomplete validation signals.

The review queue is surfaced through:

- Backend endpoint: `GET /jobs/{job_id}/review` in `app/server.py`.
- Frontend screen: `trashpanda-next/app/review/[jobId]/ReviewQueueClient.tsx`.

Review decisions currently exist as local job-level state. The review workflow
is valuable, but its current persistence and security model are temporary.

## V2 annotations and decision layers

The V2 validation, probability, and decision layers are product signals, not
SaaS platform infrastructure.

Important current behavior from `configs/default.yaml`:

- Typo correction defaults to `suggest_only`.
- History adjustments annotate by default and do not flip buckets by default.
- Probability is enabled and produces annotations.
- Decision engine is enabled, but bucket override is disabled by default.

This means the system currently favors conservative annotation over aggressive
automatic reclassification. That is a core product principle and should be
protected.

## What must not be casually rewritten

The following must not be casually rewritten during SaaS conversion:

- `EmailCleaningPipeline` orchestration.
- Stage ordering and stage semantics.
- Baseline scoring behavior in `app/scoring.py`.
- Deduplication rules in `app/dedupe.py`.
- Conservative typo suggestion behavior.
- DNS/MX enrichment behavior.
- Client-facing bucket semantics.
- Export file names and business meaning.
- Review queue meaning.
- Existing API boundary concept in `app/api_boundary.py`.
- Validation V2 signal generation, except through deliberate product work.

SaaS transformation should wrap, persist, secure, and operationalize the engine.
It should not erase or casually replace the current engine.

## Non-goals of this document

This document does not define:

- Authentication design.
- Tenant schema.
- Billing implementation.
- Production deployment architecture.
- Worker implementation.
- Database migrations.

Those belong to later SaaS design documents.
