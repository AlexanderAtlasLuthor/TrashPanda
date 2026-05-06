"""V2.2 — SMTP Verification stage.

Runs inside the chunk pipeline between ``ScoringComparisonStage`` and
``DecisionStage`` so the V2 decision can see SMTP evidence before
computing ``final_action``. This is the production-path counterpart of
the post-pass orchestrator in :mod:`app.validation_v2.smtp_integration`,
which V2.1 left in place as an annotation-only fallback.

What the stage does
-------------------

For every row in the chunk:

  * Decide whether the row is an SMTP candidate (canonical structurally,
    syntax-valid, MX present, email + domain populated, not a V1 hard
    fail). Non-candidates short-circuit with ``smtp_status=not_tested``.
  * Probe the candidate's address through the configured probe function
    (``app.validation_v2.smtp_probe.probe_email_smtplib`` in production,
    ``probe_email_dry_run`` when ``smtp_probe.dry_run=true``, or an
    injected mock in tests).
  * Cache results per email_normalized for the duration of the run. A
    duplicate row in a later chunk gets the same SMTP fields as its
    canonical sibling without a second probe.
  * Normalize the underlying :class:`SMTPResult` into the canonical V2.2
    status vocabulary: ``valid | invalid | blocked | timeout | temp_fail
    | catch_all_possible | not_tested | error``.

What the stage does *not* do
----------------------------

  * No catch-all decision logic — that's V2.3. We surface
    ``catch_all_possible`` and let ``DecisionStage`` route those to
    review, but no "is it really catch-all?" probability lives here.
  * No bounce ingestion / feedback loop — that's V2.4+.
  * No client export changes.

Test safety
-----------

The default probe function is selected from config (live or dry-run).
Tests in this repo are protected by an autouse fixture in
``conftest.py`` that monkey-patches the live probe so an accidental
production-default config can never touch the network in CI.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import pandas as pd

from ...db.smtp_retry_queue import (
    DEFAULT_RETRY_SCHEDULE_MINUTES,
    SMTPRetryQueue,
    open_for_run,
)
from ...smtp_runtime import (
    compute_retry_backoff_seconds,
    get_or_create_smtp_runtime_summary,
)
from ...validation_v2.email_send_history import (
    EmailSendHistoryStore,
    EmailSendRecord,
)
from ...validation_v2.services.domain_intelligence import provider_family_for
from ...validation_v2.smtp_probe import (
    SMTPResult,
    probe_email_dry_run,
    probe_email_smtplib,
)
from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage


_LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Canonical V2.2 status vocabulary                                            #
# --------------------------------------------------------------------------- #


# These strings are the contract the rest of V2 reads from CSVs and
# from the DecisionStage. They are intentionally narrow so downstream
# code does not need to interpret protocol-level codes itself.
SMTP_STATUS_VALID = "valid"
SMTP_STATUS_INVALID = "invalid"
SMTP_STATUS_BLOCKED = "blocked"
SMTP_STATUS_TIMEOUT = "timeout"
SMTP_STATUS_TEMP_FAIL = "temp_fail"
SMTP_STATUS_CATCH_ALL_POSSIBLE = "catch_all_possible"
SMTP_STATUS_NOT_TESTED = "not_tested"
SMTP_STATUS_ERROR = "error"


SMTP_STATUSES: frozenset[str] = frozenset({
    SMTP_STATUS_VALID,
    SMTP_STATUS_INVALID,
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_CATCH_ALL_POSSIBLE,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_ERROR,
})


# Columns this stage appends. Existing legacy ``smtp_*`` columns from
# the pre-V2.2 post-pass (``app.validation_v2.smtp_integration``) are
# preserved by name where they overlap; the new canonical columns are
# the V2.2 contract.
SMTP_VERIFICATION_OUTPUT_COLUMNS: tuple[str, ...] = (
    "smtp_tested",
    "smtp_was_candidate",
    "smtp_status",
    "smtp_result",
    "smtp_response_code",
    "smtp_response_type",
    "smtp_confidence",
    "smtp_confirmed_valid",
    "smtp_suspicious",
    "smtp_error",
    # V2.10.11 — preserve the SMTP server's exact response text on
    # every probed row, not just on errors. ``smtp_error`` only
    # populates for {blocked, timeout, error, temp_fail}; auditing
    # 4xx greylisting / 5xx policy rejections needs the full message
    # regardless of status. Empty string for non-probed rows.
    "smtp_response_message",
    # Cross-run dedup audit columns. ``smtp_from_history=True``
    # marks "ya verificada en una corrida anterior" — the SMTP
    # status / verdict came from EmailSendHistoryStore, not from a
    # network probe. ``smtp_history_send_count`` is the running
    # total of times this address has been recorded (1 on the very
    # first probe of a fresh run, ≥2 once the dedup layer kicks in).
    "smtp_from_history",
    "smtp_history_send_count",
)


# --------------------------------------------------------------------------- #
# Coercion helpers (shared with DecisionStage style)                          #
# --------------------------------------------------------------------------- #


def _coerce_bool(val: Any) -> bool:
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    try:
        return bool(val)
    except (TypeError, ValueError):
        return False


def _coerce_str(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val)
    return s if s.lower() not in {"nan", "none"} else ""


# --------------------------------------------------------------------------- #
# Result normalization                                                        #
# --------------------------------------------------------------------------- #


# Substrings that indicate the server actively refused us for policy
# reasons rather than a mailbox-level rejection. Conservative — we err
# toward ``blocked`` only when the message clearly says so, otherwise
# the response code drives the verdict.
_BLOCKED_KEYWORDS: tuple[str, ...] = (
    "policy",
    "refused",
    "blocked",
    "blacklist",
    "denied",
    "spamhaus",
    "barracuda",
    "rejected by policy",
    "access denied",
)


# Substrings that indicate a socket/network timeout when no SMTP code
# is present. Same conservative treatment as above.
_TIMEOUT_KEYWORDS: tuple[str, ...] = (
    "timeout",
    "timed out",
    "connection refused",
    "connection reset",
    "no route to host",
    "name or service not known",
    "name resolution",
)


def normalize_smtp_status(result: SMTPResult) -> str:
    """Map an :class:`SMTPResult` onto the canonical V2.2 vocabulary.

    Priority order (first match wins):

      1. ``is_catch_all_like`` → ``catch_all_possible``.
      2. ``success`` → ``valid``.
      3. 5xx mailbox rejection → ``invalid``.
      4. 4xx transient → ``temp_fail``.
      5. blocked-policy keywords with no useful code → ``blocked``.
      6. timeout-style messages with no code → ``timeout``.
      7. Anything else → ``error``.
    """
    if result.is_catch_all_like:
        return SMTP_STATUS_CATCH_ALL_POSSIBLE
    if result.success:
        return SMTP_STATUS_VALID

    code = result.response_code
    msg_lower = (result.response_message or "").lower()

    # 5xx — permanent rejection. 550 is the canonical "mailbox does not
    # exist" code, but 5xx in general is a hard fail.
    if isinstance(code, int) and 500 <= code < 600:
        # Some servers use 550/553 with a policy message even though the
        # code class says "mailbox bad". Only re-classify when the
        # message is unambiguous.
        if any(k in msg_lower for k in _BLOCKED_KEYWORDS):
            return SMTP_STATUS_BLOCKED
        return SMTP_STATUS_INVALID

    # 4xx — transient; retry-eligible.
    if isinstance(code, int) and 400 <= code < 500:
        return SMTP_STATUS_TEMP_FAIL

    # No code — derive from the message.
    if any(k in msg_lower for k in _TIMEOUT_KEYWORDS):
        return SMTP_STATUS_TIMEOUT
    if any(k in msg_lower for k in _BLOCKED_KEYWORDS):
        return SMTP_STATUS_BLOCKED

    # The dry-run probe falls in here ("dry_run" message, no code).
    # Surface it as ``error`` so DecisionStage caps these at review;
    # the candidate flag tells consumers that the row WAS a candidate.
    return SMTP_STATUS_ERROR


def smtp_status_to_model_smtp_result(status: str) -> str:
    """Translate canonical V2.2 status into the probability model's vocabulary.

    The probability model in :mod:`app.validation_v2.probability.row_model`
    accepts ``deliverable | undeliverable | catch_all | inconclusive |
    not_tested``. This helper keeps that mapping in one place so both
    DecisionStage and post-pass consumers stay in sync.
    """
    if status == SMTP_STATUS_VALID:
        return "deliverable"
    if status == SMTP_STATUS_INVALID:
        return "undeliverable"
    if status == SMTP_STATUS_CATCH_ALL_POSSIBLE:
        return "catch_all"
    if status in (
        SMTP_STATUS_BLOCKED,
        SMTP_STATUS_TIMEOUT,
        SMTP_STATUS_TEMP_FAIL,
        SMTP_STATUS_ERROR,
    ):
        return "inconclusive"
    return "not_tested"


def _confidence_from_status(status: str) -> float:
    """Per-row SMTP confidence number, kept compatible with the legacy column.

    Numbers picked to be informative without being authoritative —
    DecisionStage applies the actual routing rules.
    """
    return {
        SMTP_STATUS_VALID: 0.90,
        SMTP_STATUS_INVALID: 0.85,
        SMTP_STATUS_CATCH_ALL_POSSIBLE: 0.40,
        SMTP_STATUS_BLOCKED: 0.30,
        SMTP_STATUS_TIMEOUT: 0.20,
        SMTP_STATUS_TEMP_FAIL: 0.20,
        SMTP_STATUS_ERROR: 0.10,
        SMTP_STATUS_NOT_TESTED: 0.0,
    }.get(status, 0.0)


# --------------------------------------------------------------------------- #
# Per-run cache                                                               #
# --------------------------------------------------------------------------- #


class SMTPCache:
    """Per-run cache keyed by ``email_normalized``.

    Mirrors the :class:`app.dns_utils.DnsCache` shape so future stages
    have a familiar surface. The cache is created in ``pipeline.run`` and
    handed to the stage via ``context.extras["smtp_cache"]``.
    """

    def __init__(self) -> None:
        self._store: dict[str, SMTPResult] = {}
        self.probes_executed: int = 0
        self.cache_hits: int = 0

    def get(self, email_normalized: str) -> SMTPResult | None:
        return self._store.get(email_normalized)

    def set(self, email_normalized: str, result: SMTPResult) -> None:
        self._store[email_normalized] = result
        self.probes_executed += 1

    def __contains__(self, email_normalized: str) -> bool:
        return email_normalized in self._store

    def __len__(self) -> int:
        return len(self._store)


# --------------------------------------------------------------------------- #
# Candidate selection                                                         #
# --------------------------------------------------------------------------- #


def is_smtp_candidate(row: dict[str, Any]) -> bool:
    """Return True iff the row is an SMTP probe candidate.

    Mirrors the eligibility list in the V2.2 prompt:

      * not V1 hard_fail
      * syntax_valid is true
      * has_mx_record is true
      * email present
      * corrected_domain (or domain) present

    Canonicality is not checked here because dedupe runs *after* SMTP
    in the chunk pipeline; instead, the per-run cache deduplicates
    probes by email_normalized so a duplicate pair shares one probe.
    """
    if _coerce_bool(row.get("hard_fail")):
        return False
    if not _coerce_bool(row.get("syntax_valid")):
        return False
    if not _coerce_bool(row.get("has_mx_record")):
        return False
    email = _coerce_str(row.get("email"))
    if "@" not in email:
        return False
    domain = _coerce_str(row.get("corrected_domain") or row.get("domain"))
    if not domain:
        return False
    return True


# --------------------------------------------------------------------------- #
# Probe-fn selection                                                          #
# --------------------------------------------------------------------------- #


# The probe contract: ``(email, *, sender, timeout, ...) -> SMTPResult``.
ProbeFn = Callable[..., SMTPResult]


def _resolve_probe_fn(
    context: PipelineContext,
    injected: ProbeFn | None,
) -> tuple[ProbeFn, bool]:
    """Pick the probe function and return whether it is the live one.

    Resolution order:
      1. The function injected at construction time wins. Tests use this
         to pass deterministic mocks.
      2. If ``smtp_probe.dry_run`` is true (or absent), use the dry-run
         probe.
      3. Otherwise use the live ``probe_email_smtplib``.
    """
    if injected is not None:
        return injected, False

    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    dry_run = True
    if smtp_cfg is not None:
        dry_run = bool(getattr(smtp_cfg, "dry_run", True))
    if dry_run:
        return probe_email_dry_run, False
    return probe_email_smtplib, True


def _smtp_probe_kwargs(context: PipelineContext) -> dict[str, Any]:
    """Build the probe kwargs from ``context.config.smtp_probe``."""
    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    if smtp_cfg is None:
        return {"sender": "trashpanda-probe@localhost", "timeout": 4.0}
    return {
        "sender": str(getattr(smtp_cfg, "sender_address", "trashpanda-probe@localhost")),
        "timeout": float(getattr(smtp_cfg, "timeout_seconds", 4.0)),
    }


def _smtp_probe_enabled(context: PipelineContext) -> bool:
    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    if smtp_cfg is None:
        return True
    return bool(getattr(smtp_cfg, "enabled", True))


def _max_candidates_per_run(context: PipelineContext) -> int | None:
    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    if smtp_cfg is None:
        return None
    raw = getattr(smtp_cfg, "max_candidates_per_run", None)
    if raw is None:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _rate_limit_per_second(context: PipelineContext) -> float:
    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    if smtp_cfg is None:
        return 0.0
    try:
        return float(getattr(smtp_cfg, "rate_limit_per_second", 0.0))
    except (TypeError, ValueError):
        return 0.0


# V2.10.11 — in-process retry knobs. ``retry_temp_failures`` and
# ``max_retries`` already live on ``SMTPProbeConfig`` but were never
# consumed by the stage; reading them here finally activates the
# `compute_retry_backoff_seconds` plumbing that ``app.smtp_runtime``
# documents but didn't drive.
def _retry_temp_failures(context: PipelineContext) -> bool:
    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    if smtp_cfg is None:
        return False
    return bool(getattr(smtp_cfg, "retry_temp_failures", False))


# Hard upper bound on in-process retries — keeps the chunk worker
# from stalling on a flaky MX. ``max_retries`` from config is
# clamped to this value silently. P2's persistent retry queue
# handles the long-tail (15-30min greylisting) that exceeds this
# in-process budget.
_MAX_IN_PROCESS_RETRIES: int = 3


def _max_retries_in_process(context: PipelineContext) -> int:
    cfg = getattr(context, "config", None)
    smtp_cfg = getattr(cfg, "smtp_probe", None) if cfg is not None else None
    if smtp_cfg is None:
        return 0
    try:
        n = int(getattr(smtp_cfg, "max_retries", 0))
    except (TypeError, ValueError):
        return 0
    if n <= 0:
        return 0
    return min(n, _MAX_IN_PROCESS_RETRIES)


# --------------------------------------------------------------------------- #
# Email send history (cross-run dedup) helpers                                #
# --------------------------------------------------------------------------- #


def _email_send_history_cfg(context: PipelineContext):
    cfg = getattr(context, "config", None)
    return getattr(cfg, "email_send_history", None) if cfg is not None else None


def _resolve_email_send_history_store(
    context: PipelineContext,
) -> EmailSendHistoryStore | None:
    """Return the per-run :class:`EmailSendHistoryStore` or None.

    Lookup order (so tests retain full control):
      1. ``context.extras["email_send_history_store"]`` — a store
         already opened by the test or by ``pipeline.run`` is reused
         verbatim. Tests that want to exercise the dedup path inject
         an in-memory store here.
      2. ``context.config.email_send_history`` — opens a real SQLite
         file lazily, caches it in ``context.extras`` for the duration
         of the run, and returns it. Only fires when the run carries a
         real ``run_context`` (i.e. a production / API-driven run);
         legacy stage tests use a bare ``PipelineContext`` and must
         not have a real on-disk DB created underneath them.
      3. None — when the config block is missing, ``enabled=False``,
         or there is no run_context.
    """
    extras = getattr(context, "extras", None)
    if extras is not None:
        existing = extras.get("email_send_history_store")
        if existing is not None:
            return existing  # type: ignore[no-any-return]

    cfg = _email_send_history_cfg(context)
    if cfg is None or not bool(getattr(cfg, "enabled", False)):
        return None

    # Only open the on-disk store for real runs. Legacy unit tests
    # construct a bare ``PipelineContext()`` (no run_context) and
    # exercise the stage directly — opening a default-pathed SQLite
    # file there would (a) leak state between tests and (b) create
    # ``runtime/history/email_send_history.sqlite`` in the test cwd.
    # Tests that care about cross-run dedup pass an explicit store
    # via ``context.extras["email_send_history_store"]`` (path 1).
    if getattr(context, "run_context", None) is None:
        return None

    db_path = str(getattr(cfg, "sqlite_path", "") or "").strip()
    if not db_path:
        return None

    try:
        store = EmailSendHistoryStore(db_path)
        store._ensure_connection()  # noqa: SLF001 — surface schema errors early
    except Exception as exc:  # pragma: no cover - defensive
        _LOGGER.warning(
            "email_send_history store unavailable at %s: %s", db_path, exc
        )
        return None

    if extras is not None:
        extras["email_send_history_store"] = store
    return store


def _email_history_ttl_days(context: PipelineContext) -> int | None:
    cfg = _email_send_history_cfg(context)
    if cfg is None:
        return None
    raw = getattr(cfg, "ttl_days", None)
    if raw is None:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _email_history_force_resend(context: PipelineContext) -> bool:
    cfg = _email_send_history_cfg(context)
    if cfg is None:
        return False
    return bool(getattr(cfg, "force_resend", False))


def _smtp_result_from_history(record: EmailSendRecord) -> SMTPResult:
    """Reconstruct an :class:`SMTPResult` from a persisted record.

    The four canonical booleans (``success``, ``is_catch_all_like``,
    ``inconclusive``) plus the response code/message are preserved
    verbatim so :func:`normalize_smtp_status` and the column emitter
    behave identically on cache replay vs. a live probe.
    """
    return SMTPResult(
        success=bool(record.last_was_success),
        response_code=record.last_response_code,
        response_message=record.last_response_message or "",
        is_catch_all_like=bool(record.last_is_catch_all),
        inconclusive=bool(record.last_inconclusive),
    )


# Statuses that signal an operationally-transient failure where a
# short backoff + retry on the same connection sequence is worth
# trying. ``error`` is intentionally excluded because it covers
# the dry-run probe and probe exceptions where retry would just
# re-trigger the same condition.
_IN_PROCESS_RETRY_STATUSES: frozenset[str] = frozenset({
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_BLOCKED,
})


# Statuses persisted to the deferred retry queue (P2) when the
# in-process retry budget (P1) is exhausted. Mirrors
# ``_IN_PROCESS_RETRY_STATUSES`` because the same outcomes that are
# worth retrying within seconds are worth retrying within minutes /
# hours from a different egress.
_DEFERRED_RETRY_STATUSES: frozenset[str] = frozenset({
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_BLOCKED,
})


def _resolve_run_dir(context: PipelineContext):
    """Return the per-run output directory or ``None`` if absent.

    Tests run with a bare ``PipelineContext()`` and no ``run_context``;
    the SMTP stage must continue to work in that mode (no enqueue).
    """
    rc = getattr(context, "run_context", None)
    return getattr(rc, "run_dir", None) if rc is not None else None


# --------------------------------------------------------------------------- #
# Stage                                                                       #
# --------------------------------------------------------------------------- #


_NOT_TESTED_RESULT = SMTPResult(
    success=False,
    response_code=None,
    response_message="not_a_candidate",
    is_catch_all_like=False,
    inconclusive=True,
)


class SMTPVerificationStage(Stage):
    """Probe SMTP candidates and write canonical SMTP columns.

    Candidates are determined by :func:`is_smtp_candidate`. Non-candidates
    short-circuit to ``smtp_status=not_tested`` and the probe function is
    not called for them.

    The stage uses a per-run :class:`SMTPCache` (stored in
    ``context.extras["smtp_cache"]``) so duplicate emails across files
    or chunks are probed exactly once. A configurable
    ``rate_limit_per_second`` and ``max_candidates_per_run`` provide
    safety nets without limiting candidate scope to a sample.

    For tests, callers can inject a deterministic probe function via the
    ``probe_fn`` constructor argument; the live ``probe_email_smtplib``
    is otherwise selected when ``smtp_probe.dry_run`` is false.
    """

    name = "smtp_verification"
    requires = (
        "email",
        "syntax_valid",
        "has_mx_record",
        "corrected_domain",
        "hard_fail",
    )
    produces = SMTP_VERIFICATION_OUTPUT_COLUMNS

    def __init__(
        self,
        probe_fn: ProbeFn | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        clock_fn: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._injected_probe_fn = probe_fn
        self._sleep_fn = sleep_fn
        self._clock_fn = clock_fn

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        runtime_summary = get_or_create_smtp_runtime_summary(context)
        if not _smtp_probe_enabled(context):
            for _ in range(len(payload.frame)):
                runtime_summary.record_not_tested()
            return payload.with_frame(_write_not_tested(payload.frame))

        cache: SMTPCache = context.extras.setdefault("smtp_cache", SMTPCache())
        probe_fn, _is_live = _resolve_probe_fn(context, self._injected_probe_fn)
        probe_kwargs = _smtp_probe_kwargs(context)
        history_store = _resolve_email_send_history_store(context)
        history_ttl_days = _email_history_ttl_days(context)
        history_force_resend = _email_history_force_resend(context)
        max_candidates = _max_candidates_per_run(context)
        rate_limit = _rate_limit_per_second(context)
        min_interval = 1.0 / rate_limit if rate_limit > 0 else 0.0
        retry_enabled = _retry_temp_failures(context)
        max_retries = _max_retries_in_process(context) if retry_enabled else 0

        # V2.10.11 — lazily open the per-run retry queue. Only created
        # if at least one row needs enqueueing. The queue is opened on
        # first use, kept open for the chunk's lifetime, and closed in
        # the ``finally`` block so a probe exception cannot leak the
        # SQLite handle.
        retry_queue: SMTPRetryQueue | None = None
        run_dir = _resolve_run_dir(context)
        run_id = (
            getattr(context.run_context, "run_id", "")
            if getattr(context, "run_context", None) is not None
            else ""
        )

        frame = payload.frame
        rows = frame.to_dict(orient="records")

        out: dict[str, list[Any]] = {
            col: [] for col in SMTP_VERIFICATION_OUTPUT_COLUMNS
        }

        last_probe_at: float | None = None
        for row in rows:
            candidate = is_smtp_candidate(row)
            if not candidate:
                runtime_summary.record_not_tested()
                _emit_not_tested(out, was_candidate=False)
                continue

            runtime_summary.record_candidate_seen()
            email = _coerce_str(row.get("email")).strip()
            email_normalized = _coerce_str(
                row.get("email_normalized")
            ).strip().lower() or email.lower()

            cached = cache.get(email_normalized)
            if cached is not None:
                cache.cache_hits += 1
                runtime_summary.record_status(normalize_smtp_status(cached))
                _emit_from_result(out, cached, was_candidate=True)
                continue

            # Cross-run dedup. When the persistent send-history store
            # has a fresh record for this email (within ``ttl_days``)
            # we replay its result instead of opening a new SMTP
            # handshake. The replayed result is also cached per-run
            # so subsequent identical rows in this run hit the cheap
            # in-memory path. ``force_resend`` flips this off without
            # disabling writes — the operator can re-validate every
            # address and the new outcome will overwrite the stored one.
            if (
                history_store is not None
                and not history_force_resend
            ):
                try:
                    record = history_store.lookup_fresh(
                        email_normalized, history_ttl_days
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    _LOGGER.debug(
                        "email_send_history lookup failed for %s: %s",
                        email_normalized, exc,
                    )
                    record = None
                if record is not None:
                    replay = _smtp_result_from_history(record)
                    cache.set(email_normalized, replay)
                    history_store.history_hits += 1
                    runtime_summary.record_status(normalize_smtp_status(replay))
                    _emit_from_result(
                        out,
                        replay,
                        was_candidate=True,
                        from_history=True,
                        history_send_count=record.send_count,
                    )
                    continue

            # Run-level safety net: stop probing past the configured
            # ceiling but still emit canonical columns. The remaining
            # candidates land as ``not_tested``-but-was-candidate, so
            # DecisionStage caps them at review (correct fallback for
            # an exhausted budget).
            if (
                max_candidates is not None
                and cache.probes_executed >= max_candidates
            ):
                runtime_summary.record_not_tested(skipped_by_cap=True)
                _emit_not_tested(out, was_candidate=True)
                continue

            # Best-effort rate limit between live probes.
            if min_interval > 0 and last_probe_at is not None:
                elapsed = self._clock_fn() - last_probe_at
                if elapsed < min_interval:
                    self._sleep_fn(min_interval - elapsed)

            def _do_probe() -> "SMTPResult":
                try:
                    return probe_fn(email, **probe_kwargs)
                except Exception as exc:  # pragma: no cover - defensive
                    _LOGGER.debug("smtp probe exception for %s: %s", email, exc)
                    return SMTPResult(
                        success=False,
                        response_code=None,
                        response_message=f"probe_exception: {exc}"[:200],
                        is_catch_all_like=False,
                        inconclusive=True,
                    )

            result = _do_probe()
            last_probe_at = self._clock_fn()

            # V2.10.11 — in-process retry of operationally transient
            # outcomes (4xx temp_fail / timeout / blocked). 5xx hard
            # rejections and the dry-run "error" status are NOT
            # retried — they're either terminal or wouldn't change
            # behaviour. Each retry respects the per-run rate limit
            # and the configured backoff so a stampede of 4xx
            # responses can't drown the destination MX.
            if retry_enabled and max_retries > 0:
                attempt = 0
                while attempt < max_retries:
                    status = normalize_smtp_status(result)
                    if status not in _IN_PROCESS_RETRY_STATUSES:
                        break
                    attempt += 1
                    backoff = compute_retry_backoff_seconds(attempt)
                    if min_interval > 0:
                        backoff = max(backoff, min_interval)
                    if backoff > 0:
                        self._sleep_fn(backoff)
                    runtime_summary.smtp_retries_executed += 1
                    result = _do_probe()
                    last_probe_at = self._clock_fn()

            cache.set(email_normalized, result)
            final_status = normalize_smtp_status(result)
            runtime_summary.record_probe_attempt(final_status)
            _emit_from_result(out, result, was_candidate=True)

            # Persist the outcome so future runs (over the same data
            # or a copy of it) can skip this address. We deliberately
            # write for **every** probe-fn invocation other than the
            # dry-run sentinel: injected mocks in tests want their
            # outcomes recorded so integration tests can pin the
            # write path, and live probes obviously do too. The
            # dry-run probe is identified by its sentinel
            # ``response_message="dry_run"`` and explicitly skipped
            # so a sanity pass with ``smtp_probe.dry_run=true`` never
            # poisons the store.
            if (
                history_store is not None
                and (result.response_message or "").strip() != "dry_run"
            ):
                try:
                    history_store.record(
                        email_normalized=email_normalized,
                        domain=_coerce_str(
                            row.get("corrected_domain") or row.get("domain")
                        ).lower(),
                        status=final_status,
                        smtp_result=result.verdict,
                        response_code=(
                            int(result.response_code)
                            if result.response_code is not None
                            else None
                        ),
                        response_message=result.response_message or "",
                        was_success=bool(result.success),
                        is_catch_all=bool(result.is_catch_all_like),
                        inconclusive=bool(result.inconclusive),
                        run_id=run_id,
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    _LOGGER.debug(
                        "email_send_history record failed for %s: %s",
                        email_normalized, exc,
                    )

            # V2.10.11 — persistent retry queue enqueue. Only triggers
            # for outcomes still operationally transient AFTER the
            # in-process retry budget. ``run_dir`` is None in unit
            # tests that drive the stage with a bare PipelineContext,
            # so enqueue is silently skipped there.
            if (
                run_dir is not None
                and final_status in _DEFERRED_RETRY_STATUSES
            ):
                if retry_queue is None:
                    try:
                        retry_queue = open_for_run(run_dir)
                    except Exception as exc:  # pragma: no cover - defensive
                        _LOGGER.warning(
                            "smtp retry queue unavailable for %s: %s",
                            run_dir, exc,
                        )
                        retry_queue = None
                if retry_queue is not None:
                    try:
                        domain = _coerce_str(
                            row.get("corrected_domain") or row.get("domain")
                        ).lower()
                        try:
                            source_row = int(row.get("source_row_number") or 0)
                        except (TypeError, ValueError):
                            source_row = 0
                        retry_queue.enqueue(
                            job_id=run_id,
                            source_row=source_row,
                            email=email,
                            domain=domain,
                            provider_family=provider_family_for(domain),
                            last_status=final_status,
                            last_response_code=(
                                int(result.response_code)
                                if result.response_code is not None
                                else None
                            ),
                            last_response_message=result.response_message or "",
                            schedule_minutes=DEFAULT_RETRY_SCHEDULE_MINUTES,
                        )
                    except Exception as exc:  # pragma: no cover - defensive
                        _LOGGER.debug(
                            "smtp retry queue enqueue failed for %s: %s",
                            email, exc,
                        )

        if retry_queue is not None:
            try:
                retry_queue.close()
            except Exception:  # pragma: no cover - defensive
                pass

        new_frame = frame.copy()
        for col, values in out.items():
            new_frame[col] = values
        return payload.with_frame(new_frame)


# --------------------------------------------------------------------------- #
# Column-emit helpers                                                         #
# --------------------------------------------------------------------------- #


def _emit_not_tested(
    out: dict[str, list[Any]],
    *,
    was_candidate: bool,
) -> None:
    """Append a canonical ``not_tested`` row to the output buffer."""
    out["smtp_tested"].append(False)
    out["smtp_was_candidate"].append(was_candidate)
    out["smtp_status"].append(SMTP_STATUS_NOT_TESTED)
    out["smtp_result"].append("not_tested")
    out["smtp_response_code"].append(None)
    out["smtp_response_type"].append(None)
    out["smtp_confidence"].append(0.0)
    out["smtp_confirmed_valid"].append(False)
    out["smtp_suspicious"].append(False)
    out["smtp_error"].append(None)
    out["smtp_response_message"].append("")
    out["smtp_from_history"].append(False)
    out["smtp_history_send_count"].append(0)


def _emit_from_result(
    out: dict[str, list[Any]],
    result: SMTPResult,
    *,
    was_candidate: bool,
    from_history: bool = False,
    history_send_count: int = 0,
) -> None:
    status = normalize_smtp_status(result)
    out["smtp_tested"].append(True)
    out["smtp_was_candidate"].append(was_candidate)
    out["smtp_status"].append(status)
    out["smtp_result"].append(result.verdict)
    out["smtp_response_code"].append(
        int(result.response_code) if result.response_code is not None else None
    )
    out["smtp_response_type"].append(
        _response_type_for(result.response_code, result.response_message)
    )
    out["smtp_confidence"].append(round(_confidence_from_status(status), 3))
    out["smtp_confirmed_valid"].append(status == SMTP_STATUS_VALID)
    out["smtp_suspicious"].append(
        status in (SMTP_STATUS_INVALID, SMTP_STATUS_CATCH_ALL_POSSIBLE)
    )
    out["smtp_error"].append(
        result.response_message if status in (
            SMTP_STATUS_BLOCKED,
            SMTP_STATUS_TIMEOUT,
            SMTP_STATUS_ERROR,
            SMTP_STATUS_TEMP_FAIL,
        ) else None
    )
    # V2.10.11 — preserve the response message on every probed row so
    # 4xx greylisting / 5xx policy rejections / blocked-keyword
    # detection are auditable downstream without re-walking
    # ``smtp_error`` (which is None for valid / catch_all_possible).
    out["smtp_response_message"].append(result.response_message or "")
    out["smtp_from_history"].append(bool(from_history))
    out["smtp_history_send_count"].append(int(history_send_count))


def _response_type_for(
    code: int | None,
    message: str | None = None,
) -> str | None:
    """Return a coarse class for an SMTP response.

    V2.10.11 expanded the vocabulary so the retry queue / classifier
    can filter without re-parsing:

    * ``success``   — 2xx (probably 250 OK).
    * ``transient`` — 4xx — retry-eligible (greylist, rate limit).
    * ``policy``    — 5xx where the message names a known
                      policy/refusal keyword. NOT a mailbox failure;
                      retry from a different egress may help.
    * ``permanent`` — 5xx mailbox rejection (no policy keyword).
                      Hard fail; no retry.
    * ``timeout``   — no code + timeout-shaped message.
    * ``network``   — no code + no message of any kind.

    Older callers passing only ``code`` see the same values for 2xx /
    4xx as before (``success`` / ``transient``), so the existing
    contract — including the V2.2 test that pins ``smtp_response_type
    == "success"`` for 2xx — remains intact.
    """

    msg_lower = (message or "").lower()

    if code is None:
        if not msg_lower:
            return None
        if any(k in msg_lower for k in _TIMEOUT_KEYWORDS):
            return "timeout"
        if any(k in msg_lower for k in _BLOCKED_KEYWORDS):
            return "policy"
        return "network"

    if 200 <= code < 300:
        return "success"
    if 400 <= code < 500:
        return "transient"
    if 500 <= code < 600:
        if any(k in msg_lower for k in _BLOCKED_KEYWORDS):
            return "policy"
        return "permanent"
    return "other"


def _write_not_tested(frame: pd.DataFrame) -> pd.DataFrame:
    """Populate the canonical SMTP columns with ``not_tested`` for every row.

    Used when ``smtp_probe.enabled=false`` so DecisionStage / materialize
    still see the column contract even on a fully-disabled run.
    """
    out = frame.copy()
    n = len(out)
    out["smtp_tested"] = [False] * n
    out["smtp_was_candidate"] = [False] * n
    out["smtp_status"] = [SMTP_STATUS_NOT_TESTED] * n
    out["smtp_result"] = ["not_tested"] * n
    out["smtp_response_code"] = [None] * n
    out["smtp_response_type"] = [None] * n
    out["smtp_confidence"] = [0.0] * n
    out["smtp_confirmed_valid"] = [False] * n
    out["smtp_suspicious"] = [False] * n
    out["smtp_error"] = [None] * n
    out["smtp_response_message"] = [""] * n
    out["smtp_from_history"] = [False] * n
    out["smtp_history_send_count"] = [0] * n
    return out


__all__ = [
    "SMTPVerificationStage",
    "SMTPCache",
    "SMTP_STATUSES",
    "SMTP_STATUS_VALID",
    "SMTP_STATUS_INVALID",
    "SMTP_STATUS_BLOCKED",
    "SMTP_STATUS_TIMEOUT",
    "SMTP_STATUS_TEMP_FAIL",
    "SMTP_STATUS_CATCH_ALL_POSSIBLE",
    "SMTP_STATUS_NOT_TESTED",
    "SMTP_STATUS_ERROR",
    "SMTP_VERIFICATION_OUTPUT_COLUMNS",
    "is_smtp_candidate",
    "normalize_smtp_status",
    "smtp_status_to_model_smtp_result",
]
