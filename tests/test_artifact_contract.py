"""V2.9.5 — artifact classification contract tests."""

from __future__ import annotations

import pytest

from app.api_boundary import (
    _CLIENT_OUTPUT_NAMES,
    _REPORT_NAMES,
    _TECHNICAL_CSV_NAMES,
)
from app.artifact_contract import (
    ARTIFACT_AUDIENCE_BY_KEY,
    ARTIFACT_AUDIENCE_CLIENT_SAFE,
    ARTIFACT_AUDIENCE_INTERNAL_ONLY,
    ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    ARTIFACT_AUDIENCE_TECHNICAL_DEBUG,
    ARTIFACT_AUDIENCES,
    get_artifact_audience,
    is_client_safe_artifact,
    is_safe_only_artifact,
    known_artifact_keys,
    list_artifacts_by_audience,
)


# --------------------------------------------------------------------------- #
# Test 1 — All expected client-safe artifacts classified
# --------------------------------------------------------------------------- #


def test_client_safe_artifacts_classified() -> None:
    expected_client_safe = (
        "approved_original_format",
        "valid_emails",
        "review_emails",
        "invalid_or_bounce_risk",
        "duplicate_emails",
        "hard_fail_emails",
        "summary_report",
        # V2.10.10 — review-bucket subdivisions by decision_reason.
        "review_cold_start_b2b",
        "review_smtp_inconclusive",
        "review_catch_all",
        "review_medium_probability",
        "review_domain_high_risk",
        # V2.10.10.b — action-oriented review classification.
        "review_ready_probable",
        "review_low_risk",
        "review_timeout_retry",
        "review_catch_all_consumer",
        "review_high_risk",
        "do_not_send",
        "second_pass_candidates",
        # V2.10.12 — pilot send / bounce-proven verification.
        "delivery_verified",
        "pilot_send_candidates",
        "pilot_hard_bounces",
        "pilot_soft_bounces",
        "pilot_blocked_or_deferred",
        "pilot_summary_report",
        "updated_do_not_send",
    )
    for key in expected_client_safe:
        assert get_artifact_audience(key) == ARTIFACT_AUDIENCE_CLIENT_SAFE, (
            f"{key!r} should be client_safe"
        )


# --------------------------------------------------------------------------- #
# Test 2 — Operator-only artifacts classified
# --------------------------------------------------------------------------- #


def test_operator_only_artifacts_classified() -> None:
    expected_operator_only = (
        "v2_deliverability_summary",
        "v2_reason_breakdown",
        "v2_domain_risk_summary",
        "v2_probability_distribution",
        "smtp_runtime_summary",
        "artifact_consistency",
        "processing_report_json",
        "processing_report_csv",
        "domain_summary",
    )
    for key in expected_operator_only:
        assert get_artifact_audience(key) == ARTIFACT_AUDIENCE_OPERATOR_ONLY, (
            f"{key!r} should be operator_only"
        )


# --------------------------------------------------------------------------- #
# Test 3 — Technical debug artifacts classified
# --------------------------------------------------------------------------- #


def test_technical_debug_artifacts_classified() -> None:
    expected_technical_debug = (
        "clean_high_confidence",
        "review_medium_confidence",
        "removed_invalid",
        "removed_duplicates",
        "removed_hard_fail",
        "typo_corrections",
        "duplicate_summary",
    )
    for key in expected_technical_debug:
        assert get_artifact_audience(key) == ARTIFACT_AUDIENCE_TECHNICAL_DEBUG, (
            f"{key!r} should be technical_debug"
        )


# --------------------------------------------------------------------------- #
# Test 4 — Unknown artifacts default to internal_only
# --------------------------------------------------------------------------- #


def test_unknown_artifacts_default_internal_only() -> None:
    unknown = (
        "staging.sqlite3",
        "some_random_file.tmp",
        "runtime/feedback/bounce_outcomes.sqlite",
        "runtime/history/domain_history.sqlite",
        "logs/pipeline.log",
        "no_such_artifact",
        "",
        "totally.made.up.artifact",
    )
    for name in unknown:
        assert get_artifact_audience(name) == ARTIFACT_AUDIENCE_INTERNAL_ONLY, (
            f"{name!r} should default to internal_only"
        )


# --------------------------------------------------------------------------- #
# Test 5 — is_client_safe_artifact
# --------------------------------------------------------------------------- #


def test_is_client_safe_artifact() -> None:
    assert is_client_safe_artifact("valid_emails") is True
    assert is_client_safe_artifact("approved_original_format") is True
    # operator_only
    assert is_client_safe_artifact("v2_deliverability_summary") is False
    assert is_client_safe_artifact("smtp_runtime_summary") is False
    assert is_client_safe_artifact("artifact_consistency") is False
    # technical_debug
    assert is_client_safe_artifact("clean_high_confidence") is False
    assert is_client_safe_artifact("removed_invalid") is False
    # unknown
    assert is_client_safe_artifact("staging.sqlite3") is False
    assert is_client_safe_artifact("totally_unknown_file") is False
    assert is_client_safe_artifact("") is False


# --------------------------------------------------------------------------- #
# Test 6 — list_artifacts_by_audience
# --------------------------------------------------------------------------- #


def test_list_by_audience_client_safe() -> None:
    listed = list_artifacts_by_audience(ARTIFACT_AUDIENCE_CLIENT_SAFE)
    expected_subset = {
        "valid_emails",
        "review_emails",
        "invalid_or_bounce_risk",
        "summary_report",
        "approved_original_format",
        "duplicate_emails",
        "hard_fail_emails",
    }
    assert expected_subset.issubset(set(listed))
    # everything returned must actually be client_safe
    for key in listed:
        assert get_artifact_audience(key) == ARTIFACT_AUDIENCE_CLIENT_SAFE


def test_list_by_audience_operator_only() -> None:
    listed = list_artifacts_by_audience(ARTIFACT_AUDIENCE_OPERATOR_ONLY)
    expected_subset = {
        "v2_deliverability_summary",
        "v2_reason_breakdown",
        "v2_domain_risk_summary",
        "v2_probability_distribution",
        "smtp_runtime_summary",
        "artifact_consistency",
        "processing_report_json",
        "processing_report_csv",
        "domain_summary",
    }
    assert expected_subset.issubset(set(listed))


def test_list_by_audience_technical_debug() -> None:
    listed = list_artifacts_by_audience(ARTIFACT_AUDIENCE_TECHNICAL_DEBUG)
    expected_subset = {
        "clean_high_confidence",
        "review_medium_confidence",
        "removed_invalid",
        "removed_duplicates",
        "removed_hard_fail",
        "typo_corrections",
        "duplicate_summary",
    }
    assert expected_subset.issubset(set(listed))


def test_list_by_audience_invalid_raises() -> None:
    with pytest.raises(ValueError):
        list_artifacts_by_audience("not_a_real_audience")


# --------------------------------------------------------------------------- #
# Test 7 — Contract covers discoverable artifacts
# --------------------------------------------------------------------------- #


def test_contract_covers_all_discoverable_artifacts() -> None:
    """Every key exposed by api_boundary discovery must have an explicit
    audience entry. Conservative default exists for unknowns, but the
    discoverable surface must be intentional, not implicit."""
    discoverable_keys = (
        set(_TECHNICAL_CSV_NAMES.keys())
        | set(_CLIENT_OUTPUT_NAMES.keys())
        | set(_REPORT_NAMES.keys())
    )
    mapped_keys = set(known_artifact_keys())
    missing = discoverable_keys - mapped_keys
    assert not missing, (
        f"discoverable artifacts without explicit audience: {sorted(missing)}"
    )


def test_no_discoverable_artifact_falls_back_to_internal_only() -> None:
    discoverable_keys = (
        set(_TECHNICAL_CSV_NAMES.keys())
        | set(_CLIENT_OUTPUT_NAMES.keys())
        | set(_REPORT_NAMES.keys())
    )
    leaked_to_internal = [
        key
        for key in discoverable_keys
        if get_artifact_audience(key) == ARTIFACT_AUDIENCE_INTERNAL_ONLY
    ]
    assert not leaked_to_internal, (
        f"discoverable artifacts unintentionally classified internal_only: "
        f"{sorted(leaked_to_internal)}"
    )


# --------------------------------------------------------------------------- #
# Test 8 — Public report keys do not include non-client-safe artifacts
#
# Scoped guardrail: V2.8 / V2.9.3 / V2.9.4 deliberately kept the V2
# deliverability reports, the SMTP runtime summary, and the artifact
# consistency metadata out of the public report listing. This test
# locks in that contract — any future addition that violates it must
# update the contract or the listing on purpose.
#
# The legacy V1 reports (processing_report_*, domain_summary,
# typo_corrections, duplicate_summary) are *currently* exposed via the
# generic operator results route and are grandfathered. The V2.9.5
# contract still labels them operator_only / technical_debug so future
# client-package work can route around them; no server route is
# changed in this subphase.
# --------------------------------------------------------------------------- #


_NEVER_PUBLIC_KEYS: tuple[str, ...] = (
    "v2_deliverability_summary",
    "v2_reason_breakdown",
    "v2_domain_risk_summary",
    "v2_probability_distribution",
    "smtp_runtime_summary",
    "artifact_consistency",
)


def test_public_report_keys_exclude_v2_and_runtime_metadata() -> None:
    from app.server import _PUBLIC_REPORT_KEYS

    public = set(_PUBLIC_REPORT_KEYS)
    leaked = [k for k in _NEVER_PUBLIC_KEYS if k in public]
    assert not leaked, (
        f"non-client-safe artifacts leaked into _PUBLIC_REPORT_KEYS: {leaked}"
    )


def test_public_report_keys_have_explicit_audience() -> None:
    from app.server import _PUBLIC_REPORT_KEYS

    for key in _PUBLIC_REPORT_KEYS:
        assert key in ARTIFACT_AUDIENCE_BY_KEY, (
            f"public report key {key!r} has no explicit audience in contract"
        )


# --------------------------------------------------------------------------- #
# Test 9 — No runtime behaviour change
#
# Importing the contract module must not perturb api_boundary
# discovery. We re-run a minimal smoke test of the artifact name maps.
# --------------------------------------------------------------------------- #


def test_artifact_discovery_still_works() -> None:
    # Discovery name maps are still well-formed dicts of str→str
    assert isinstance(_TECHNICAL_CSV_NAMES, dict)
    assert isinstance(_CLIENT_OUTPUT_NAMES, dict)
    assert isinstance(_REPORT_NAMES, dict)
    assert all(isinstance(v, str) and v for v in _TECHNICAL_CSV_NAMES.values())
    assert all(isinstance(v, str) and v for v in _CLIENT_OUTPUT_NAMES.values())
    assert all(isinstance(v, str) and v for v in _REPORT_NAMES.values())

    # Sanity: contract module exposes the audiences tuple and constants
    assert ARTIFACT_AUDIENCE_CLIENT_SAFE in ARTIFACT_AUDIENCES
    assert ARTIFACT_AUDIENCE_OPERATOR_ONLY in ARTIFACT_AUDIENCES
    assert ARTIFACT_AUDIENCE_TECHNICAL_DEBUG in ARTIFACT_AUDIENCES
    assert ARTIFACT_AUDIENCE_INTERNAL_ONLY in ARTIFACT_AUDIENCES


# --------------------------------------------------------------------------- #
# Bonus: filename resolution (suffix and stem)
# --------------------------------------------------------------------------- #


def test_filename_stem_resolves_to_key_audience() -> None:
    # filenames map to their key's audience via stem matching
    assert (
        get_artifact_audience("valid_emails.xlsx") == ARTIFACT_AUDIENCE_CLIENT_SAFE
    )
    assert (
        get_artifact_audience("approved_original_format.xlsx")
        == ARTIFACT_AUDIENCE_CLIENT_SAFE
    )
    assert (
        get_artifact_audience("v2_deliverability_summary.json")
        == ARTIFACT_AUDIENCE_OPERATOR_ONLY
    )
    assert (
        get_artifact_audience("artifact_consistency.json")
        == ARTIFACT_AUDIENCE_OPERATOR_ONLY
    )
    assert (
        get_artifact_audience("clean_high_confidence.csv")
        == ARTIFACT_AUDIENCE_TECHNICAL_DEBUG
    )


def test_internal_path_fragments_classified_internal_only() -> None:
    samples = (
        "runtime/feedback/bounce_outcomes.sqlite",
        "runtime/history/domain_history.sqlite",
        "runtime/uploads/foo.csv",
        "runtime/jobs/job_42/whatever",
    )
    for name in samples:
        assert get_artifact_audience(name) == ARTIFACT_AUDIENCE_INTERNAL_ONLY


# --------------------------------------------------------------------------- #
# V2.10.8.2 — Safe-only allowlist
#
# The safe-only subset is a *strict subset* of client_safe used for
# the partial-delivery channel. The contract must keep:
#
#     client_safe ≠ safe_only
#
# i.e. review/rejected/duplicate/hard-fail XLSXs remain client_safe
# (they belong in the full delivery package) but must NEVER pass the
# safe-only filter.
# --------------------------------------------------------------------------- #


def test_safe_only_allows_strict_safe_subset() -> None:
    # Filenames the safe-only delivery channel may carry.
    assert is_safe_only_artifact("valid_emails.xlsx") is True
    assert is_safe_only_artifact("approved_original_format.xlsx") is True
    assert is_safe_only_artifact("summary_report.xlsx") is True
    assert is_safe_only_artifact("SAFE_ONLY_DELIVERY_NOTE.txt") is True
    # Bare keys must also resolve.
    assert is_safe_only_artifact("valid_emails") is True
    assert is_safe_only_artifact("approved_original_format") is True
    assert is_safe_only_artifact("summary_report") is True
    assert is_safe_only_artifact("safe_only_delivery_note") is True


def test_safe_only_excludes_client_safe_review_and_rejected() -> None:
    """client_safe ≠ safe_only — the partial channel is stricter."""
    # These remain client_safe so the *full* delivery still ships them …
    assert is_client_safe_artifact("review_emails.xlsx") is True
    assert is_client_safe_artifact("invalid_or_bounce_risk.xlsx") is True
    assert is_client_safe_artifact("duplicate_emails.xlsx") is True
    assert is_client_safe_artifact("hard_fail_emails.xlsx") is True
    # … but NONE of them may pass the safe-only filter.
    assert is_safe_only_artifact("review_emails.xlsx") is False
    assert is_safe_only_artifact("invalid_or_bounce_risk.xlsx") is False
    assert is_safe_only_artifact("duplicate_emails.xlsx") is False
    assert is_safe_only_artifact("hard_fail_emails.xlsx") is False
    # Same for bare keys.
    assert is_safe_only_artifact("review_emails") is False
    assert is_safe_only_artifact("invalid_or_bounce_risk") is False


def test_safe_only_excludes_operator_and_internal_artifacts() -> None:
    operator_or_debug = (
        "operator_review_summary.json",
        "smtp_runtime_summary.json",
        "v2_deliverability_summary.json",
        "artifact_consistency.json",
        "processing_report.json",
        "domain_summary.csv",
        "clean_high_confidence.csv",
        "removed_invalid.csv",
    )
    for name in operator_or_debug:
        assert is_safe_only_artifact(name) is False, name


def test_safe_only_excludes_unknown_filenames() -> None:
    unknown = (
        "unknown.xlsx",
        "totally_made_up_artifact",
        "staging.sqlite3",
        "",
    )
    for name in unknown:
        assert is_safe_only_artifact(name) is False, name


def test_safe_only_delivery_note_is_client_safe() -> None:
    """The note itself must classify as client_safe — the V2.9.7 gate
    re-validates every file in the package against the client_safe
    contract, so a non-client-safe note would block the package."""
    assert (
        get_artifact_audience("SAFE_ONLY_DELIVERY_NOTE.txt")
        == ARTIFACT_AUDIENCE_CLIENT_SAFE
    )
    assert is_client_safe_artifact("SAFE_ONLY_DELIVERY_NOTE.txt") is True
    assert is_safe_only_artifact("SAFE_ONLY_DELIVERY_NOTE.txt") is True


def test_safe_only_subset_is_strict_subset_of_client_safe() -> None:
    """Every safe-only artifact must ALSO be client_safe — the two
    contracts can never diverge."""
    sample_safe_only_keys = (
        "valid_emails",
        "approved_original_format",
        "summary_report",
        "safe_only_delivery_note",
    )
    for key in sample_safe_only_keys:
        assert is_safe_only_artifact(key) is True
        assert is_client_safe_artifact(key) is True
