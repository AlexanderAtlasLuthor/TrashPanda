"""V2.3 — Catch-all Detection stage.

Promotes catch-all detection from "annotation surfaced inside SMTP
result" into a first-class row-level decision signal. Runs after
:class:`SMTPVerificationStage` and before :class:`DecisionStage` so
the decision can read canonical catch-all fields directly without
re-deriving them from ``smtp_status``.

Sources of evidence (priority order)
------------------------------------

1. **SMTP random-recipient signal** — already produced by
   ``probe_email_smtplib`` via the piggyback random RCPT trick. We
   detect it by reading ``smtp_status == catch_all_possible`` on the
   row (V2.2 already normalized ``is_catch_all_like``).
2. **SMTP valid + non-catch-all** — when the probe accepted the real
   address but rejected the random one, the domain is NOT catch-all.
   Evidence: ``smtp_status == valid``.
3. **SMTP inconclusive / not tested** — we don't know.
4. *Reserved for V2.4+:* historical detector corroboration to upgrade
   ``possible`` → ``confirmed``.

What this stage produces
------------------------

Six canonical catch-all columns on every row:

    catch_all_tested      bool
    catch_all_status      one of CATCH_ALL_STATUSES
    catch_all_flag        bool — True iff status indicates risk
    catch_all_confidence  float in [0, 1]
    catch_all_method      "smtp_probe_signal" | "history" | "not_tested" | …
    catch_all_reason      short machine-readable token

Per-domain cache
----------------

Catch-all is a **domain-level** behaviour, so the stage caches the
classification by ``corrected_domain`` (or ``domain``). When two rows
in the same chunk share a domain they get identical catch-all fields
without re-classification. The cache lives on
``context.extras["catch_all_cache"]`` so it persists across chunks.

What this stage does NOT do
---------------------------

  * No new probing — relies entirely on SMTP fields populated upstream.
  * No history reads — the existing history-driven detector at
    :mod:`app.validation_v2.catch_all` remains the post-pass /
    annotation backbone and is unchanged.
  * No client export changes.
  * No bounce ingestion.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..context import PipelineContext
from ..payload import ChunkPayload
from ..stage import Stage
from .smtp_verification import (
    SMTP_STATUS_BLOCKED,
    SMTP_STATUS_CATCH_ALL_POSSIBLE,
    SMTP_STATUS_ERROR,
    SMTP_STATUS_INVALID,
    SMTP_STATUS_NOT_TESTED,
    SMTP_STATUS_TEMP_FAIL,
    SMTP_STATUS_TIMEOUT,
    SMTP_STATUS_VALID,
)


# --------------------------------------------------------------------------- #
# Canonical V2.3 status vocabulary                                            #
# --------------------------------------------------------------------------- #


CATCH_ALL_STATUS_CONFIRMED = "confirmed_catch_all"
CATCH_ALL_STATUS_POSSIBLE = "possible_catch_all"
CATCH_ALL_STATUS_NOT = "not_catch_all"
CATCH_ALL_STATUS_UNKNOWN = "unknown"
CATCH_ALL_STATUS_NOT_TESTED = "not_tested"
CATCH_ALL_STATUS_ERROR = "error"


CATCH_ALL_STATUSES: frozenset[str] = frozenset({
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_POSSIBLE,
    CATCH_ALL_STATUS_NOT,
    CATCH_ALL_STATUS_UNKNOWN,
    CATCH_ALL_STATUS_NOT_TESTED,
    CATCH_ALL_STATUS_ERROR,
})


# Statuses that block ``auto_approve``. ``unknown`` is excluded
# deliberately — see DecisionStage rule 4 (candidate-without-valid-SMTP
# already covers the "we don't know" case via SMTP fields).
CATCH_ALL_RISK_STATUSES: frozenset[str] = frozenset({
    CATCH_ALL_STATUS_CONFIRMED,
    CATCH_ALL_STATUS_POSSIBLE,
})


# Methods recorded on the ``catch_all_method`` column. Kept narrow so a
# typo elsewhere surfaces as a routing inconsistency.
CATCH_ALL_METHOD_SMTP_PROBE = "smtp_probe_signal"
CATCH_ALL_METHOD_SMTP_VALID = "smtp_valid_no_random_accept"
CATCH_ALL_METHOD_HISTORY = "history"
CATCH_ALL_METHOD_NOT_TESTED = "not_tested"
CATCH_ALL_METHOD_INCONCLUSIVE = "inconclusive_smtp"


CATCH_ALL_DETECTION_OUTPUT_COLUMNS: tuple[str, ...] = (
    "catch_all_tested",
    "catch_all_status",
    "catch_all_flag",
    "catch_all_confidence",
    "catch_all_method",
    "catch_all_reason",
)


# --------------------------------------------------------------------------- #
# Coercion helpers                                                            #
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
# Detection result + classification                                           #
# --------------------------------------------------------------------------- #


class CatchAllClassification:
    """Lightweight value-object holding the six canonical fields.

    Implemented as a regular class (not a dataclass) so the slots
    layout is explicit and the column order stays in lock-step with
    :data:`CATCH_ALL_DETECTION_OUTPUT_COLUMNS`.
    """

    __slots__ = (
        "tested",
        "status",
        "flag",
        "confidence",
        "method",
        "reason",
    )

    def __init__(
        self,
        *,
        tested: bool,
        status: str,
        flag: bool,
        confidence: float,
        method: str,
        reason: str,
    ) -> None:
        self.tested = tested
        self.status = status
        self.flag = flag
        self.confidence = round(float(confidence), 3)
        self.method = method
        self.reason = reason

    @classmethod
    def not_tested(cls, reason: str = "not_a_candidate") -> "CatchAllClassification":
        return cls(
            tested=False,
            status=CATCH_ALL_STATUS_NOT_TESTED,
            flag=False,
            confidence=0.0,
            method=CATCH_ALL_METHOD_NOT_TESTED,
            reason=reason,
        )

    @classmethod
    def unknown(cls, reason: str = "smtp_inconclusive") -> "CatchAllClassification":
        return cls(
            tested=True,
            status=CATCH_ALL_STATUS_UNKNOWN,
            flag=False,
            confidence=0.0,
            method=CATCH_ALL_METHOD_INCONCLUSIVE,
            reason=reason,
        )


# --------------------------------------------------------------------------- #
# Candidate selection                                                         #
# --------------------------------------------------------------------------- #


def is_catch_all_candidate(row: dict[str, Any]) -> bool:
    """Return True iff catch-all detection should run for this row.

    Mirrors the eligibility list in the V2.3 prompt:

      * not V1 hard fail
      * syntax_valid is true
      * has_mx_record is true
      * email present and contains ``@``
      * domain populated

    SMTP-invalid mailbox rows are still candidates because the SMTP
    probe may have surfaced a catch-all signal *before* the mailbox
    rejection — we want to record that. In practice the upstream stage
    short-circuits these, so this branch is mostly defensive.
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
# SMTP-driven classification                                                  #
# --------------------------------------------------------------------------- #


def derive_catch_all_from_smtp(
    smtp_status: str,
    smtp_was_candidate: bool,
) -> CatchAllClassification:
    """Map the V2.2 canonical SMTP status onto V2.3 catch-all fields.

    Rules (first match wins):

      * ``catch_all_possible``     → ``possible_catch_all``, flag=True,
                                     method=smtp_probe_signal.
      * ``valid``                  → ``not_catch_all`` (probe succeeded
                                     and random RCPT was rejected),
                                     flag=False,
                                     method=smtp_valid_no_random_accept.
      * ``invalid``                → ``not_tested`` (we proved the
                                     mailbox is dead but never reached a
                                     useful catch-all assertion).
      * ``blocked / timeout / temp_fail / error``
                                   → ``unknown`` — SMTP didn't tell us.
      * ``not_tested``             → ``not_tested``.

    The ``smtp_was_candidate`` flag distinguishes "we never tried" (the
    row failed the candidate filter) from "we tried and got nothing
    useful" (server timed out, etc.).
    """
    if smtp_status == SMTP_STATUS_CATCH_ALL_POSSIBLE:
        return CatchAllClassification(
            tested=True,
            status=CATCH_ALL_STATUS_POSSIBLE,
            flag=True,
            confidence=0.75,
            method=CATCH_ALL_METHOD_SMTP_PROBE,
            reason="random_rcpt_accepted",
        )

    if smtp_status == SMTP_STATUS_VALID:
        return CatchAllClassification(
            tested=True,
            status=CATCH_ALL_STATUS_NOT,
            flag=False,
            confidence=0.85,
            method=CATCH_ALL_METHOD_SMTP_VALID,
            reason="random_rcpt_rejected",
        )

    if smtp_status == SMTP_STATUS_INVALID:
        return CatchAllClassification.not_tested(
            reason="smtp_invalid_no_catch_all_evidence"
        )

    if smtp_status in (
        SMTP_STATUS_BLOCKED,
        SMTP_STATUS_TIMEOUT,
        SMTP_STATUS_TEMP_FAIL,
        SMTP_STATUS_ERROR,
    ):
        if smtp_was_candidate:
            return CatchAllClassification.unknown(
                reason=f"smtp_{smtp_status}"
            )
        return CatchAllClassification.not_tested(
            reason="smtp_inconclusive_non_candidate"
        )

    # smtp_status == not_tested or anything else.
    return CatchAllClassification.not_tested()


# --------------------------------------------------------------------------- #
# Per-domain cache                                                            #
# --------------------------------------------------------------------------- #


class CatchAllCache:
    """Per-domain cache of the catch-all classification for one run.

    Catch-all is a domain-level property: two rows on the same domain
    should share the same classification. The cache is keyed by the
    lowercased ``corrected_domain``. Stored on
    ``context.extras["catch_all_cache"]`` for the duration of one run.
    """

    def __init__(self) -> None:
        self._store: dict[str, CatchAllClassification] = {}
        self.classifications_computed: int = 0
        self.cache_hits: int = 0

    def get(self, domain: str) -> CatchAllClassification | None:
        return self._store.get(domain)

    def set(self, domain: str, value: CatchAllClassification) -> None:
        self._store[domain] = value
        self.classifications_computed += 1

    def __contains__(self, domain: str) -> bool:
        return domain in self._store

    def __len__(self) -> int:
        return len(self._store)


# --------------------------------------------------------------------------- #
# Stage                                                                       #
# --------------------------------------------------------------------------- #


def _catch_all_enabled(context: PipelineContext) -> bool:
    cfg = getattr(context, "config", None)
    cfg_block = getattr(cfg, "catch_all", None) if cfg is not None else None
    if cfg_block is None:
        return True
    return bool(getattr(cfg_block, "enabled", True))


def _domain_for_row(row: dict[str, Any]) -> str:
    domain = _coerce_str(
        row.get("corrected_domain") or row.get("domain")
    ).strip().lower()
    return domain


class CatchAllDetectionStage(Stage):
    """Emit canonical catch-all fields per row using upstream SMTP evidence.

    Position in the chunk engine:

        SMTPVerificationStage → **CatchAllDetectionStage** → DecisionStage

    No new network calls. The piggyback random-RCPT probe inside
    ``probe_email_smtplib`` is the single source of catch-all evidence;
    this stage just normalizes it into a first-class column contract
    and applies a per-domain cache so two rows on the same domain agree.
    """

    name = "catch_all_detection"
    requires = (
        "syntax_valid",
        "has_mx_record",
        "hard_fail",
        "smtp_status",
        "smtp_was_candidate",
        "corrected_domain",
        "email",
    )
    produces = CATCH_ALL_DETECTION_OUTPUT_COLUMNS

    def run(self, payload: ChunkPayload, context: PipelineContext) -> ChunkPayload:
        if not _catch_all_enabled(context):
            return payload.with_frame(_write_not_tested_all(payload.frame))

        cache: CatchAllCache = context.extras.setdefault(
            "catch_all_cache", CatchAllCache()
        )

        frame = payload.frame
        rows = frame.to_dict(orient="records")

        out: dict[str, list[Any]] = {
            col: [] for col in CATCH_ALL_DETECTION_OUTPUT_COLUMNS
        }

        for row in rows:
            if not is_catch_all_candidate(row):
                _emit(out, CatchAllClassification.not_tested())
                continue

            domain = _domain_for_row(row)
            cached = cache.get(domain) if domain else None
            if cached is not None:
                cache.cache_hits += 1
                _emit(out, cached)
                continue

            smtp_status = _coerce_str(row.get("smtp_status")) or SMTP_STATUS_NOT_TESTED
            smtp_was_candidate = _coerce_bool(row.get("smtp_was_candidate"))
            classification = derive_catch_all_from_smtp(
                smtp_status, smtp_was_candidate
            )

            # Only cache classifications that meaningfully describe the
            # domain. ``not_tested`` from a non-candidate row is a
            # row-level skip, not a domain-level fact, so we don't cache
            # it (a later candidate row on the same domain might still
            # produce a real classification).
            if classification.status != CATCH_ALL_STATUS_NOT_TESTED and domain:
                cache.set(domain, classification)

            _emit(out, classification)

        new_frame = frame.copy()
        for col, values in out.items():
            new_frame[col] = values
        return payload.with_frame(new_frame)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _emit(
    out: dict[str, list[Any]],
    c: CatchAllClassification,
) -> None:
    out["catch_all_tested"].append(c.tested)
    out["catch_all_status"].append(c.status)
    out["catch_all_flag"].append(c.flag)
    out["catch_all_confidence"].append(c.confidence)
    out["catch_all_method"].append(c.method)
    out["catch_all_reason"].append(c.reason)


def _write_not_tested_all(frame: pd.DataFrame) -> pd.DataFrame:
    """Populate canonical catch-all columns with ``not_tested`` for every row.

    Used when ``catch_all.enabled=false`` so DecisionStage and
    materialization always see the column contract.
    """
    out = frame.copy()
    n = len(out)
    out["catch_all_tested"] = [False] * n
    out["catch_all_status"] = [CATCH_ALL_STATUS_NOT_TESTED] * n
    out["catch_all_flag"] = [False] * n
    out["catch_all_confidence"] = [0.0] * n
    out["catch_all_method"] = [CATCH_ALL_METHOD_NOT_TESTED] * n
    out["catch_all_reason"] = ["disabled_by_config"] * n
    return out


__all__ = [
    "CATCH_ALL_DETECTION_OUTPUT_COLUMNS",
    "CATCH_ALL_METHOD_HISTORY",
    "CATCH_ALL_METHOD_INCONCLUSIVE",
    "CATCH_ALL_METHOD_NOT_TESTED",
    "CATCH_ALL_METHOD_SMTP_PROBE",
    "CATCH_ALL_METHOD_SMTP_VALID",
    "CATCH_ALL_RISK_STATUSES",
    "CATCH_ALL_STATUSES",
    "CATCH_ALL_STATUS_CONFIRMED",
    "CATCH_ALL_STATUS_ERROR",
    "CATCH_ALL_STATUS_NOT",
    "CATCH_ALL_STATUS_NOT_TESTED",
    "CATCH_ALL_STATUS_POSSIBLE",
    "CATCH_ALL_STATUS_UNKNOWN",
    "CatchAllCache",
    "CatchAllClassification",
    "CatchAllDetectionStage",
    "derive_catch_all_from_smtp",
    "is_catch_all_candidate",
]
