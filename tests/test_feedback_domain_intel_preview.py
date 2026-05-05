"""V2.9.8 — feedback bridge readiness preview tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from app import feedback_domain_intel_preview as preview_module
from app.api_boundary import (
    build_client_package_for_job,
    build_feedback_domain_intel_preview_for_job,
    collect_job_artifacts,
)
from app.artifact_contract import (
    ARTIFACT_AUDIENCE_OPERATOR_ONLY,
    get_artifact_audience,
    is_client_safe_artifact,
)
from app.feedback_domain_intel_preview import (
    FeedbackDomainIntelPreviewRecord,
    FeedbackDomainIntelPreviewResult,
    WARNING_FEEDBACK_STORE_EMPTY,
    WARNING_FEEDBACK_STORE_MISSING,
    build_feedback_domain_intel_preview,
)
from app.validation_v2.feedback import (
    BounceOutcomeStore,
    DomainBounceAggregate,
)


# --------------------------------------------------------------------------- #
# Fixture helpers
# --------------------------------------------------------------------------- #


def _make_store(path: Path, aggregates: list[DomainBounceAggregate]) -> None:
    """Create a fresh BounceOutcomeStore at ``path`` with the given rows."""
    store = BounceOutcomeStore(path)
    try:
        for agg in aggregates:
            store.upsert_aggregate(agg)
    finally:
        store.close()


def _good_aggregate(domain: str = "good.com") -> DomainBounceAggregate:
    # 20 obs, all delivered → low risk → known_good
    return DomainBounceAggregate(
        domain=domain,
        total_observations=20,
        delivered_count=20,
    )


def _risky_aggregate(domain: str = "risky.com") -> DomainBounceAggregate:
    # 20 obs, 8 hard bounces (rate 0.40 ≥ 0.20) → high → known_risky
    return DomainBounceAggregate(
        domain=domain,
        total_observations=20,
        delivered_count=10,
        hard_bounce_count=8,
        soft_bounce_count=2,
    )


def _cold_start_aggregate(domain: str = "newdomain.com") -> DomainBounceAggregate:
    # 2 obs (< min=5) → cold_start
    return DomainBounceAggregate(
        domain=domain,
        total_observations=2,
        delivered_count=2,
    )


def _medium_aggregate(domain: str = "medium.com") -> DomainBounceAggregate:
    # 20 obs, 2 hard bounces (rate 0.10, between 0.08 and 0.20) → medium → unknown
    return DomainBounceAggregate(
        domain=domain,
        total_observations=20,
        delivered_count=18,
        hard_bounce_count=2,
    )


# --------------------------------------------------------------------------- #
# Test 1 — Missing store returns warning
# --------------------------------------------------------------------------- #


def test_missing_store_returns_warning(tmp_path: Path) -> None:
    nonexistent = tmp_path / "absent.sqlite"
    assert not nonexistent.exists()

    result = build_feedback_domain_intel_preview(nonexistent)

    assert result.feedback_available is False
    assert WARNING_FEEDBACK_STORE_MISSING in result.warnings
    assert result.records == ()
    assert result.total_domains == 0
    # The constructor must not have created the store as a side effect.
    assert not nonexistent.exists()


# --------------------------------------------------------------------------- #
# Test 2 — Empty store returns warning
# --------------------------------------------------------------------------- #


def test_empty_store_returns_warning(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(store_path, aggregates=[])
    assert store_path.is_file()

    result = build_feedback_domain_intel_preview(store_path)

    assert result.feedback_available is False
    assert WARNING_FEEDBACK_STORE_EMPTY in result.warnings
    assert result.records == ()
    assert result.total_domains == 0


# --------------------------------------------------------------------------- #
# Test 3 — Preview records generated
# --------------------------------------------------------------------------- #


def test_preview_records_generated(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(
        store_path,
        aggregates=[
            _good_aggregate("good.com"),
            _risky_aggregate("risky.com"),
            _cold_start_aggregate("new.com"),
        ],
    )

    result = build_feedback_domain_intel_preview(store_path)

    assert result.feedback_available is True
    assert result.total_domains == 3
    by_domain = {r.domain: r for r in result.records}
    assert by_domain["good.com"].behavior_class == "known_good"
    assert by_domain["good.com"].risk_level == "low"
    assert by_domain["good.com"].cold_start is False
    assert by_domain["risky.com"].behavior_class == "known_risky"
    assert by_domain["risky.com"].risk_level == "high"
    assert by_domain["risky.com"].cold_start is False
    assert by_domain["new.com"].behavior_class == "cold_start"
    assert by_domain["new.com"].cold_start is True


# --------------------------------------------------------------------------- #
# Test 4 — Summary counts correct
# --------------------------------------------------------------------------- #


def test_summary_counts_correct(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(
        store_path,
        aggregates=[
            _good_aggregate("a-good.com"),
            _good_aggregate("b-good.com"),
            _risky_aggregate("c-risky.com"),
            _cold_start_aggregate("d-new.com"),
            _medium_aggregate("e-medium.com"),  # behavior_class=unknown
        ],
    )

    result = build_feedback_domain_intel_preview(store_path)

    assert result.feedback_available is True
    assert result.total_domains == 5
    assert result.known_good_count == 2
    assert result.known_risky_count == 1
    assert result.cold_start_count == 1
    assert result.unknown_count == 1
    # 20 + 20 + 20 + 2 + 20
    assert result.total_observations == 82


# --------------------------------------------------------------------------- #
# Test 5 — Sorting deterministic
# --------------------------------------------------------------------------- #


def test_sorting_is_deterministic(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(
        store_path,
        aggregates=[
            # Mixed order on disk; preview must order them.
            DomainBounceAggregate(
                domain="z-cold.com", total_observations=2, delivered_count=2
            ),  # cold_start, 2 obs
            DomainBounceAggregate(
                domain="m-risky-small.com",
                total_observations=10,
                delivered_count=4,
                hard_bounce_count=4,
            ),  # known_risky, 10 obs
            DomainBounceAggregate(
                domain="a-good-small.com",
                total_observations=5,
                delivered_count=5,
            ),  # known_good, 5 obs
            DomainBounceAggregate(
                domain="b-risky-big.com",
                total_observations=20,
                delivered_count=10,
                hard_bounce_count=8,
            ),  # known_risky, 20 obs
            DomainBounceAggregate(
                domain="x-medium.com",
                total_observations=20,
                delivered_count=18,
                hard_bounce_count=2,
            ),  # unknown, 20 obs
        ],
    )

    result = build_feedback_domain_intel_preview(store_path)

    domains_in_order = [r.domain for r in result.records]
    # Expected order:
    #   1. b-risky-big.com   (known_risky, 20)
    #   2. m-risky-small.com (known_risky, 10)
    #   3. a-good-small.com  (known_good, 5)
    #   4. z-cold.com        (cold_start, 2)
    #   5. x-medium.com      (unknown, 20)
    assert domains_in_order == [
        "b-risky-big.com",
        "m-risky-small.com",
        "a-good-small.com",
        "z-cold.com",
        "x-medium.com",
    ]


def test_sort_tiebreak_domain_ascending(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    # Two known_risky domains with the same obs count → tiebreak is domain ascending.
    _make_store(
        store_path,
        aggregates=[
            DomainBounceAggregate(
                domain="zeta.com",
                total_observations=20,
                delivered_count=10,
                hard_bounce_count=8,
            ),
            DomainBounceAggregate(
                domain="alpha.com",
                total_observations=20,
                delivered_count=10,
                hard_bounce_count=8,
            ),
        ],
    )

    result = build_feedback_domain_intel_preview(store_path)
    assert [r.domain for r in result.records] == ["alpha.com", "zeta.com"]


# --------------------------------------------------------------------------- #
# Test 6 — JSON report written
# --------------------------------------------------------------------------- #


def test_json_report_written_when_output_dir_provided(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(
        store_path,
        aggregates=[_good_aggregate(), _risky_aggregate()],
    )
    output_dir = tmp_path / "run"

    result = build_feedback_domain_intel_preview(store_path, output_dir=output_dir)

    assert result.output_path is not None
    assert result.output_path.is_file()
    payload = json.loads(result.output_path.read_text(encoding="utf-8"))

    required = {
        "report_version",
        "generated_at",
        "feedback_store_path",
        "output_path",
        "feedback_available",
        "total_domains",
        "total_observations",
        "known_good_count",
        "known_risky_count",
        "cold_start_count",
        "unknown_count",
        "records",
        "warnings",
    }
    missing = required - set(payload.keys())
    assert not missing, f"missing JSON fields: {missing}"
    assert payload["report_version"] == "v2.9.8"
    assert payload["feedback_available"] is True
    assert isinstance(payload["records"], list)


def test_no_output_when_output_dir_omitted(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(store_path, aggregates=[_good_aggregate()])

    result = build_feedback_domain_intel_preview(store_path)

    assert result.output_path is None


# --------------------------------------------------------------------------- #
# Test 7 — Boundary callable JSON-friendly
# --------------------------------------------------------------------------- #


def test_boundary_callable_json_friendly(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(
        store_path,
        aggregates=[_good_aggregate(), _risky_aggregate(), _cold_start_aggregate()],
    )
    output_dir = tmp_path / "run"

    payload = build_feedback_domain_intel_preview_for_job(
        store_path, output_dir=output_dir
    )

    assert isinstance(payload, dict)
    json.dumps(payload)  # round-trip
    assert isinstance(payload["feedback_store_path"], str)
    assert isinstance(payload["output_path"], str)
    for r in payload["records"]:
        assert isinstance(r["domain"], str)


# --------------------------------------------------------------------------- #
# Test 8 — Artifact discovery includes preview
# --------------------------------------------------------------------------- #


def test_artifact_discovery_includes_preview(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(store_path, aggregates=[_good_aggregate()])
    run_dir = tmp_path / "run"
    build_feedback_domain_intel_preview(store_path, output_dir=run_dir)

    artifacts = collect_job_artifacts(run_dir)

    expected = run_dir / "feedback_domain_intel_preview.json"
    assert artifacts.reports.feedback_domain_intel_preview == expected
    assert expected.is_file()


# --------------------------------------------------------------------------- #
# Test 9 — Artifact contract classifies preview as operator_only
# --------------------------------------------------------------------------- #


def test_contract_classifies_preview_as_operator_only() -> None:
    assert (
        get_artifact_audience("feedback_domain_intel_preview")
        == ARTIFACT_AUDIENCE_OPERATOR_ONLY
    )
    # Filename stem also resolves correctly.
    assert (
        get_artifact_audience("feedback_domain_intel_preview.json")
        == ARTIFACT_AUDIENCE_OPERATOR_ONLY
    )
    assert is_client_safe_artifact("feedback_domain_intel_preview") is False
    assert is_client_safe_artifact("feedback_domain_intel_preview.json") is False


# --------------------------------------------------------------------------- #
# Test 10 — Client package excludes preview
# --------------------------------------------------------------------------- #


def test_client_package_excludes_preview(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # Minimal client-safe fixture so the builder has something to copy.
    pd.DataFrame({"email": ["a@b.com"]}).to_excel(
        run_dir / "valid_emails.xlsx", sheet_name="emails", index=False
    )
    pd.DataFrame({"email": []}).to_excel(
        run_dir / "review_emails.xlsx", sheet_name="emails", index=False
    )
    pd.DataFrame({"email": []}).to_excel(
        run_dir / "invalid_or_bounce_risk.xlsx", sheet_name="emails", index=False
    )
    # Build the preview into the same run dir.
    store_path = tmp_path / "feedback.sqlite"
    _make_store(store_path, aggregates=[_good_aggregate()])
    build_feedback_domain_intel_preview(store_path, output_dir=run_dir)
    assert (run_dir / "feedback_domain_intel_preview.json").is_file()

    pkg_payload = build_client_package_for_job(run_dir)

    pkg_dir = Path(pkg_payload["package_dir"])
    assert not (pkg_dir / "feedback_domain_intel_preview.json").exists()
    excluded = {x["filename"] for x in pkg_payload["files_excluded"]}
    assert "feedback_domain_intel_preview.json" in excluded
    excluded_entry = next(
        x for x in pkg_payload["files_excluded"]
        if x["filename"] == "feedback_domain_intel_preview.json"
    )
    assert excluded_entry["audience"] == "operator_only"


# --------------------------------------------------------------------------- #
# Test 11 — Does not affect pipeline
# --------------------------------------------------------------------------- #


def test_pipeline_does_not_import_preview() -> None:
    from app import pipeline as pipeline_mod

    src = Path(pipeline_mod.__file__).read_text(encoding="utf-8")
    assert "feedback_domain_intel_preview" not in src
    assert "build_feedback_domain_intel_preview" not in src


def test_domain_intelligence_stage_does_not_import_preview() -> None:
    from app.engine.stages import postprocessing  # noqa: F401  (stage host)

    # DomainIntelligenceStage lives under app.engine.stages — scan its dir.
    stages_dir = (
        Path(__file__).resolve().parent.parent / "app" / "engine" / "stages"
    )
    for py in stages_dir.glob("*.py"):
        src = py.read_text(encoding="utf-8")
        assert "feedback_domain_intel_preview" not in src, (
            f"preview leaked into stage: {py.name}"
        )


# --------------------------------------------------------------------------- #
# Test 12 — No live network
# --------------------------------------------------------------------------- #


def test_no_network_imports_in_preview() -> None:
    src = Path(preview_module.__file__).read_text(encoding="utf-8")
    forbidden_tokens = (
        "import socket",
        "from socket",
        "import smtplib",
        "from smtplib",
        "import urllib.request",
        "from urllib.request",
        "import requests",
        "from requests",
        "import http.client",
        "from http.client",
    )
    for token in forbidden_tokens:
        assert token not in src, f"preview must not contain {token!r}"


# --------------------------------------------------------------------------- #
# Result-object sanity
# --------------------------------------------------------------------------- #


def test_result_dataclass_shape(tmp_path: Path) -> None:
    store_path = tmp_path / "feedback.sqlite"
    _make_store(store_path, aggregates=[_good_aggregate(), _risky_aggregate()])

    result = build_feedback_domain_intel_preview(store_path)

    assert isinstance(result, FeedbackDomainIntelPreviewResult)
    assert all(
        isinstance(r, FeedbackDomainIntelPreviewRecord) for r in result.records
    )
    assert isinstance(result.records, tuple)
    assert isinstance(result.warnings, tuple)


def test_does_not_mutate_store(tmp_path: Path) -> None:
    """The preview must be a pure read; the store row count is unchanged."""
    store_path = tmp_path / "feedback.sqlite"
    aggregates = [_good_aggregate("a.com"), _risky_aggregate("b.com")]
    _make_store(store_path, aggregates=aggregates)

    # Snapshot row count before the preview.
    conn = sqlite3.connect(str(store_path))
    before = conn.execute(
        "SELECT COUNT(*) FROM bounce_outcomes_aggregate"
    ).fetchone()[0]
    conn.close()

    build_feedback_domain_intel_preview(
        store_path, output_dir=tmp_path / "run"
    )

    conn = sqlite3.connect(str(store_path))
    after = conn.execute(
        "SELECT COUNT(*) FROM bounce_outcomes_aggregate"
    ).fetchone()[0]
    conn.close()

    assert before == after == 2
