# Transformation Rules

## Purpose

This document defines strict rules for transforming the current internal tool
into a SaaS product without damaging the working engine.

These rules are enforceable constraints for future work.

## Rule 1: Do not casually rewrite core engine logic

The engine is the valuable part of the current system.

Do not casually rewrite:

- `app/pipeline.py`
- `app/engine/`
- `app/scoring.py`
- `app/scoring_v2/`
- `app/validation_v2/`
- `app/dedupe.py`
- `app/dns_utils.py`
- `app/typo_suggestions.py`
- `app/client_output.py`
- `app/reporting.py`

Changes to these areas must be product-driven and deliberate, not incidental to
SaaS infrastructure work.

## Rule 2: Replace infrastructure behind stable contracts

Temporary infrastructure should be replaced behind stable boundaries.

The current best boundary is `app/api_boundary.py`.

The SaaS transformation should preserve the engine-call contract while changing
how jobs, files, artifacts, users, and organizations are managed around it.

## Rule 3: Do not mix SaaS concerns into the Engine

The Engine must not own:

- Authentication.
- Authorization.
- Organizations.
- Billing.
- Plan limits.
- User roles.
- Sessions.
- Tenant membership.
- Customer support state.

The Platform owns those concerns.

## Rule 4: Do not duplicate engine decisions in the Platform

The Platform must not reimplement:

- Email validation rules.
- Scoring rules.
- Deduplication rules.
- Typo suggestion rules.
- Bucket classification.
- Export semantics.

The Platform may display these outputs. It must not become a second engine.

## Rule 5: Preserve working flows during transformation

These flows must remain protected:

- Upload.
- Job creation.
- Processing.
- Status/results access.
- Review queue.
- Review decisions.
- Artifact/export download.
- Insights display.

Any SaaS transformation that breaks these flows without an intentional
replacement is invalid.

## Rule 6: Mock behavior must stay clearly marked as mock

`trashpanda-next/lib/mock-adapter.ts` is development-only behavior.

Mock jobs, mock logs, mock review data, and mock artifacts must never be treated
as production data or product proof.

## Rule 7: Preview pages must not be treated as real modules

The following are not real SaaS modules today:

- Domain audit.
- Lead discovery.
- Pipelines.

They must not be used as evidence that those capabilities exist.

## Rule 8: Do not call local runtime storage SaaS storage

The directories `runtime/`, `output/`, and `logs/` are local runtime surfaces.

They are not durable SaaS storage. They are not tenant-isolated storage. They
are not customer-safe artifact storage by default.

## Rule 9: Do not expose internal artifacts as customer artifacts

Customer-facing exports must be explicitly identified.

Internal runtime files, staging databases, logs, debug reports, and technical
side effects must not be downloadable merely because they exist in a run
directory.

## Rule 10: No global job access in SaaS

Access by `job_id` alone is not a valid SaaS access model.

Any future SaaS route or API must include an authorization boundary before
showing job status, results, review data, logs, or artifacts.

## Rule 11: Public request input must not choose server config

The current `config_path` behavior is a local/internal convenience.

It must not be part of a public SaaS upload contract.

## Rule 12: Product terms must stay honest

The system must not claim:

- Guaranteed inbox existence.
- Guaranteed non-bounce.
- Perfect deliverability.
- Enterprise-grade validation infrastructure.
- Completed domain audit.
- Completed lead discovery.
- Completed automated pipelines.

The README is already explicit that the system does not guarantee inbox
existence or perfect deliverability. SaaS transformation must preserve that
honesty.

## Rule 13: Protect conservative defaults

The current defaults favor safe, explainable behavior:

- Typo correction defaults to suggestion, not automatic mutation.
- History and decision layers annotate by default.
- Bucket override is disabled by default.
- SMTP probing is disabled/dry-run by default.

Do not loosen these defaults as an incidental SaaS change.

## Rule 14: Documentation must distinguish current state from target state

Every transformation document must distinguish:

- What exists today.
- What is temporary.
- What is a future target.
- What is explicitly out of scope.

Confusing these categories is a transformation risk.
