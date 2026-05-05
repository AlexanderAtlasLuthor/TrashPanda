"""V2.9.5 — Artifact classification contract.

Defines the explicit *audience* of every artifact the pipeline can
produce so packaging/delivery code can reason about what is safe to
hand to a client and what is operator-only.

This module is a contract layer only. It does **not**:

* change V2 classification logic,
* change SMTP behaviour,
* change catch-all behaviour,
* change domain intelligence,
* change export routing,
* change report generation,
* implement a client package builder,
* implement an operator review gate.

Audiences:

* ``client_safe``       — safe to include in a normal client delivery
                          package.
* ``operator_only``     — useful for the operator/founder/admin, not
                          part of default client delivery.
* ``technical_debug``   — useful for debugging/auditing pipeline
                          internals.
* ``internal_only``     — never include in any client-facing surface
                          (databases, runtime stores, logs, temp
                          files). Conservative default for unknown
                          artifacts.
"""

from __future__ import annotations

from typing import Iterable


# --------------------------------------------------------------------------- #
# Audience constants
# --------------------------------------------------------------------------- #

ARTIFACT_AUDIENCE_CLIENT_SAFE: str = "client_safe"
ARTIFACT_AUDIENCE_OPERATOR_ONLY: str = "operator_only"
ARTIFACT_AUDIENCE_TECHNICAL_DEBUG: str = "technical_debug"
ARTIFACT_AUDIENCE_INTERNAL_ONLY: str = "internal_only"

ARTIFACT_AUDIENCES: tuple[str, ...] = (
    ARTIFACT_AUDIENCE_CLIENT_SAFE,
    ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    ARTIFACT_AUDIENCE_INTERNAL_ONLY,
)


# --------------------------------------------------------------------------- #
# Audience mapping by artifact key
#
# Keys match the canonical artifact keys used by ``app.api_boundary``
# (``_TECHNICAL_CSV_NAMES``, ``_CLIENT_OUTPUT_NAMES``, ``_REPORT_NAMES``).
# Filenames are also accepted by the lookup helpers below for the small
# number of internal artifacts (DB files, runtime stores) that are not
# tracked as keys.
# --------------------------------------------------------------------------- #

_AUDIENCE_BY_KEY: dict[str, str] = {
    # ---- client_safe: default client delivery package -------------------- #
    "valid_emails": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    "review_emails": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    "invalid_or_bounce_risk": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    "summary_report": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    "approved_original_format": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    "duplicate_emails": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    "hard_fail_emails": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    # V2.10.8.2 — safe-only partial delivery anchor file.
    "safe_only_delivery_note": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    # Always-on README that names the PRIMARY artifact for the customer.
    "client_readme": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    # Filename stem alias — the README ships as ``README_CLIENT.txt``
    # (caps for visibility in file managers). The lowercase stem is
    # matched here so the artifact-contract resolver classifies it as
    # ``client_safe`` without renaming the on-disk file.
    "readme_client": ARTIFACT_AUDIENCE_CLIENT_SAFE,
    # ---- operator_only: operator/founder/admin reports ------------------- #
    "processing_report_json": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "processing_report_csv": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    # V2.9.9 — both processing_report.{json,csv} share stem
    # ``processing_report``; the alias keeps stem-based filename
    # lookup correct (otherwise the bare filename falls through to
    # the conservative ``internal_only`` default).
    "processing_report": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "domain_summary": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "v2_deliverability_summary": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "v2_reason_breakdown": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "v2_domain_risk_summary": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "v2_probability_distribution": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "smtp_runtime_summary": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "artifact_consistency": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "operator_review_summary": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    "feedback_domain_intel_preview": ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    # ---- technical_debug: pipeline internals ----------------------------- #
    "clean_high_confidence": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    "review_medium_confidence": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    "removed_invalid": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    "removed_duplicates": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    "removed_hard_fail": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    "typo_corrections": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    "duplicate_summary": ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
}

# Public-facing alias of the mapping. Defensive copy on access.
ARTIFACT_AUDIENCE_BY_KEY: dict[str, str] = dict(_AUDIENCE_BY_KEY)


# --------------------------------------------------------------------------- #
# V2.10.8.2 — Safe-only delivery allowlist
#
# A *strict subset* of the client_safe set. Safe-only delivery is the
# partial-delivery channel: when the full run is not ready_for_client
# but a safe subset exists, only these artifacts may be handed out.
# Notably excludes review_emails, invalid_or_bounce_risk,
# duplicate_emails, and hard_fail_emails — which remain client_safe
# for the controlled full delivery path.
# --------------------------------------------------------------------------- #

_SAFE_ONLY_DELIVERY_KEYS: frozenset[str] = frozenset(
    {
        "valid_emails",
        "approved_original_format",
        "summary_report",
        "safe_only_delivery_note",
    }
)


# --------------------------------------------------------------------------- #
# Filename suffix / substring rules for non-key internal artifacts.
#
# These cover items the pipeline / runtime writes that are NOT tracked
# as canonical artifact keys — DB files, runtime stores, logs, etc.
# Anything matched here is ``internal_only``.
# --------------------------------------------------------------------------- #

_INTERNAL_FILENAME_SUFFIXES: tuple[str, ...] = (
    ".sqlite",
    ".sqlite3",
    ".db",
    ".log",
    ".tmp",
    ".temp",
    ".bak",
)

_INTERNAL_PATH_FRAGMENTS: tuple[str, ...] = (
    "runtime/history",
    "runtime/feedback",
    "runtime/uploads",
    "runtime/jobs",
    "logs/",
)


# --------------------------------------------------------------------------- #
# Lookup helpers
# --------------------------------------------------------------------------- #


def _normalize(key_or_filename: str) -> str:
    """Lowercase + strip whitespace; preserve path separators."""
    if key_or_filename is None:
        return ""
    return str(key_or_filename).strip().replace("\\", "/")


def _strip_extension(name: str) -> str:
    """Return the stem for a path-like value (last segment, no suffix)."""
    last = name.rsplit("/", 1)[-1]
    if "." in last:
        return last.rsplit(".", 1)[0]
    return last


def get_artifact_audience(key_or_filename: str) -> str:
    """Return the audience for an artifact key or filename.

    Resolution order:

    1. Exact match against a canonical artifact key.
    2. Filename stem match against a canonical artifact key
       (e.g. ``valid_emails.xlsx`` → ``valid_emails``).
    3. Internal-suffix / internal-path rule
       (DB files, runtime stores, logs, temp files).
    4. Conservative default: ``internal_only``.

    Unknown artifacts are NEVER classified as ``client_safe``.
    """
    if not key_or_filename:
        return ARTIFACT_AUDIENCE_INTERNAL_ONLY

    raw = _normalize(key_or_filename)

    # 1) direct key match
    if raw in _AUDIENCE_BY_KEY:
        return _AUDIENCE_BY_KEY[raw]

    # 2) filename stem match (handles ``valid_emails.xlsx`` etc.)
    stem = _strip_extension(raw)
    if stem in _AUDIENCE_BY_KEY:
        return _AUDIENCE_BY_KEY[stem]

    # 2b) case-insensitive stem match — for protocol filenames whose
    # on-disk casing differs from the canonical lowercase artifact key
    # (e.g. ``SAFE_ONLY_DELIVERY_NOTE.txt``). Additive: every previously
    # matching path returned in step 1 or 2 already.
    stem_lower = stem.lower()
    if stem_lower != stem and stem_lower in _AUDIENCE_BY_KEY:
        return _AUDIENCE_BY_KEY[stem_lower]

    # 3) internal heuristics
    lower = raw.lower()
    for suffix in _INTERNAL_FILENAME_SUFFIXES:
        if lower.endswith(suffix):
            return ARTIFACT_AUDIENCE_INTERNAL_ONLY
    for fragment in _INTERNAL_PATH_FRAGMENTS:
        if fragment in lower:
            return ARTIFACT_AUDIENCE_INTERNAL_ONLY

    # 4) conservative default
    return ARTIFACT_AUDIENCE_INTERNAL_ONLY


def is_client_safe_artifact(key_or_filename: str) -> bool:
    """Return ``True`` only when the artifact is explicitly client-safe."""
    return get_artifact_audience(key_or_filename) == ARTIFACT_AUDIENCE_CLIENT_SAFE


def is_safe_only_artifact(key_or_filename: str) -> bool:
    """Return ``True`` only when the artifact is in the safe-only subset.

    The safe-only subset is a strict subset of ``client_safe``: an
    artifact that is ``client_safe`` but not in the safe-only allowlist
    (such as ``review_emails`` or ``invalid_or_bounce_risk``) may still
    appear in a full client delivery package, but must NEVER appear in
    a safe-only partial delivery.

    Resolution mirrors :func:`get_artifact_audience` so callers may
    pass either an artifact key or a filename.
    """
    if not key_or_filename:
        return False

    raw = _normalize(key_or_filename)

    # Resolve to a canonical key by the same precedence as the
    # audience lookup: direct, stem, then lowercased stem.
    candidate: str | None = None
    if raw in _AUDIENCE_BY_KEY:
        candidate = raw
    else:
        stem = _strip_extension(raw)
        if stem in _AUDIENCE_BY_KEY:
            candidate = stem
        else:
            stem_lower = stem.lower()
            if stem_lower in _AUDIENCE_BY_KEY:
                candidate = stem_lower

    if candidate is None:
        return False
    if candidate not in _SAFE_ONLY_DELIVERY_KEYS:
        return False
    # Must additionally be client_safe — keeps the two contracts aligned.
    return _AUDIENCE_BY_KEY[candidate] == ARTIFACT_AUDIENCE_CLIENT_SAFE


def list_artifacts_by_audience(audience: str) -> tuple[str, ...]:
    """Return the tuple of canonical keys mapped to ``audience``.

    Raises ``ValueError`` for an unknown audience so callers cannot
    silently shadow a typo into an empty list.
    """
    if audience not in ARTIFACT_AUDIENCES:
        raise ValueError(
            f"unknown artifact audience: {audience!r}; "
            f"expected one of {ARTIFACT_AUDIENCES}"
        )
    return tuple(
        sorted(key for key, value in _AUDIENCE_BY_KEY.items() if value == audience)
    )


def known_artifact_keys() -> tuple[str, ...]:
    """Return all canonical artifact keys with an explicit audience."""
    return tuple(sorted(_AUDIENCE_BY_KEY.keys()))


def iter_known_audiences(keys: Iterable[str]) -> dict[str, str]:
    """Map an iterable of keys to their audiences (lookup convenience)."""
    return {key: get_artifact_audience(key) for key in keys}


__all__ = [
    "ARTIFACT_AUDIENCES",
    "ARTIFACT_AUDIENCE_BY_KEY",
    "ARTIFACT_AUDIENCE_CLIENT_SAFE",
    "ARTIFACT_AUDIENCE_INTERNAL_ONLY",
    "ARTIFACT_AUDIENCE_OPERATOR_ONLY",
    "ARTIFACT_AUDIENCE_TECHNICAL_DEBUG",
    "get_artifact_audience",
    "is_client_safe_artifact",
    "is_safe_only_artifact",
    "iter_known_audiences",
    "known_artifact_keys",
    "list_artifacts_by_audience",
]
