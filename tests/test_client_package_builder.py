"""V2.9.6 — client delivery package builder tests."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app import client_package_builder
from app.api_boundary import (
    _CLIENT_OUTPUT_NAMES,
    _REPORT_NAMES,
    _TECHNICAL_CSV_NAMES,
    build_client_package_for_job,
    collect_job_artifacts,
)
from app.client_package_builder import (
    ClientPackageFile,
    ClientPackageResult,
    build_client_delivery_package,
)


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #


def _write_xlsx(path: Path, rows: int) -> None:
    df = pd.DataFrame({"email": [f"user{i}@example.com" for i in range(rows)]})
    df.to_excel(path, sheet_name="emails", index=False)


def _write_summary_xlsx(path: Path) -> None:
    totals = pd.DataFrame({"metric": ["total_input_rows"], "value": [10]})
    with pd.ExcelWriter(path) as writer:
        totals.to_excel(writer, sheet_name="totals", index=False)


def _populate_run_dir(
    run_dir: Path,
    *,
    include_approved_original: bool = True,
    include_random: bool = False,
) -> None:
    """Create a realistic mix of artifacts in ``run_dir``."""
    # client_safe XLSX
    _write_xlsx(run_dir / "valid_emails.xlsx", rows=5)
    _write_xlsx(run_dir / "review_emails.xlsx", rows=3)
    _write_xlsx(run_dir / "invalid_or_bounce_risk.xlsx", rows=2)
    _write_xlsx(run_dir / "duplicate_emails.xlsx", rows=1)
    _write_xlsx(run_dir / "hard_fail_emails.xlsx", rows=1)
    _write_summary_xlsx(run_dir / "summary_report.xlsx")
    if include_approved_original:
        _write_xlsx(run_dir / "approved_original_format.xlsx", rows=8)

    # operator_only / technical_debug / internal_only — must be excluded
    (run_dir / "v2_deliverability_summary.json").write_text("{}", encoding="utf-8")
    (run_dir / "v2_reason_breakdown.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "v2_domain_risk_summary.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "v2_probability_distribution.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "smtp_runtime_summary.json").write_text("{}", encoding="utf-8")
    (run_dir / "artifact_consistency.json").write_text("{}", encoding="utf-8")
    (run_dir / "processing_report.json").write_text("{}", encoding="utf-8")
    (run_dir / "processing_report.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "domain_summary.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "clean_high_confidence.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "review_medium_confidence.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "removed_invalid.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "removed_duplicates.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "removed_hard_fail.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "typo_corrections.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "duplicate_summary.csv").write_text("a,b\n", encoding="utf-8")
    (run_dir / "staging.sqlite3").write_bytes(b"\x00\x01")
    (run_dir / "pipeline.log").write_text("log\n", encoding="utf-8")
    (run_dir / "logs").mkdir(exist_ok=True)
    (run_dir / "logs" / "deep.log").write_text("deep\n", encoding="utf-8")

    if include_random:
        (run_dir / "random_debug_dump.csv").write_text("x,y\n", encoding="utf-8")


_CLIENT_SAFE_FILENAMES: tuple[str, ...] = (
    "valid_emails.xlsx",
    "review_emails.xlsx",
    "invalid_or_bounce_risk.xlsx",
    "duplicate_emails.xlsx",
    "hard_fail_emails.xlsx",
    "summary_report.xlsx",
    "approved_original_format.xlsx",
)

_NEVER_INCLUDED_FILENAMES: tuple[str, ...] = (
    "v2_deliverability_summary.json",
    "v2_reason_breakdown.csv",
    "v2_domain_risk_summary.csv",
    "v2_probability_distribution.csv",
    "smtp_runtime_summary.json",
    "artifact_consistency.json",
    "processing_report.json",
    "processing_report.csv",
    "domain_summary.csv",
    "clean_high_confidence.csv",
    "review_medium_confidence.csv",
    "removed_invalid.csv",
    "removed_duplicates.csv",
    "removed_hard_fail.csv",
    "typo_corrections.csv",
    "duplicate_summary.csv",
    "staging.sqlite3",
    "pipeline.log",
)


# --------------------------------------------------------------------------- #
# Test 1 — Package includes only client-safe artifacts
# --------------------------------------------------------------------------- #


def test_package_includes_only_client_safe_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    result = build_client_delivery_package(run_dir)

    package_dir = result.package_dir
    assert package_dir.is_dir()

    # All client-safe files present (when source existed).
    for name in _CLIENT_SAFE_FILENAMES:
        assert (package_dir / name).is_file(), f"missing client-safe file: {name}"

    # No non-client-safe files leaked.
    for name in _NEVER_INCLUDED_FILENAMES:
        assert not (package_dir / name).exists(), (
            f"non-client-safe file leaked into package: {name}"
        )

    # Subdirectories from run_dir not copied.
    assert not (package_dir / "logs").exists()


# --------------------------------------------------------------------------- #
# Test 2 — Approved original absent warning
# --------------------------------------------------------------------------- #


def test_approved_original_absent_warns_but_succeeds(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir, include_approved_original=False)

    result = build_client_delivery_package(run_dir)

    assert result.package_dir.is_dir()
    assert not (result.package_dir / "approved_original_format.xlsx").exists()
    codes = {w.code for w in result.warnings}
    assert "approved_original_format_absent" in codes
    # Other client-safe files still made it into the package.
    assert (result.package_dir / "valid_emails.xlsx").is_file()


# --------------------------------------------------------------------------- #
# Test 3 — Manifest is written
# --------------------------------------------------------------------------- #


def test_manifest_is_written_with_required_fields(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    result = build_client_delivery_package(run_dir)

    manifest_path = result.manifest_path
    assert manifest_path.is_file()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    required = {
        "report_version",
        "generated_at",
        "source_run_dir",
        "package_dir",
        "files_included",
        "files_excluded",
        "warnings",
        "safe_count",
        "review_count",
        "rejected_count",
    }
    missing = required - set(payload.keys())
    assert not missing, f"manifest missing fields: {missing}"
    assert payload["report_version"] == "v2.9.6"
    assert isinstance(payload["files_included"], list)
    assert isinstance(payload["files_excluded"], list)
    assert isinstance(payload["warnings"], list)


# --------------------------------------------------------------------------- #
# Test 4 — Manifest excludes non-client-safe artifacts
# --------------------------------------------------------------------------- #


def test_manifest_excludes_non_client_safe(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    result = build_client_delivery_package(run_dir)
    excluded_filenames = {item["filename"] for item in result.files_excluded}

    # Sample of expected exclusions across audiences.
    expected_subset = {
        "v2_deliverability_summary.json",
        "smtp_runtime_summary.json",
        "artifact_consistency.json",
        "processing_report.json",
        "domain_summary.csv",
        "clean_high_confidence.csv",
        "removed_invalid.csv",
        "typo_corrections.csv",
        "staging.sqlite3",
    }
    assert expected_subset.issubset(excluded_filenames), (
        f"missing exclusions: {expected_subset - excluded_filenames}"
    )

    # Every excluded entry has reason=not_client_safe.
    for item in result.files_excluded:
        assert item["reason"] == "not_client_safe", item


# --------------------------------------------------------------------------- #
# Test 5 — Counts extracted
# --------------------------------------------------------------------------- #


def test_counts_extracted_from_xlsx(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    result = build_client_delivery_package(run_dir)

    # Matches the rows we wrote in _populate_run_dir.
    assert result.safe_count == 5
    assert result.review_count == 3
    assert result.rejected_count == 2


# --------------------------------------------------------------------------- #
# Test 6 — Count extraction failure does not crash
# --------------------------------------------------------------------------- #


def test_corrupt_xlsx_count_unavailable_warning(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)
    # Corrupt valid_emails.xlsx so pandas cannot read it.
    (run_dir / "valid_emails.xlsx").write_bytes(b"this is not an xlsx file")

    result = build_client_delivery_package(run_dir)

    assert result.safe_count is None
    assert result.review_count == 3  # other counts still work
    assert result.rejected_count == 2
    codes = [w.code for w in result.warnings]
    assert "count_unavailable" in codes


# --------------------------------------------------------------------------- #
# Test 7 — Unknown artifact not included
# --------------------------------------------------------------------------- #


def test_unknown_artifact_not_included_and_marked_internal(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir, include_random=True)

    result = build_client_delivery_package(run_dir)

    package_dir = result.package_dir
    assert not (package_dir / "random_debug_dump.csv").exists()

    excluded_for_random = [
        item for item in result.files_excluded
        if item["filename"] == "random_debug_dump.csv"
    ]
    assert len(excluded_for_random) == 1
    assert excluded_for_random[0]["audience"] == "internal_only"
    assert excluded_for_random[0]["reason"] == "not_client_safe"


# --------------------------------------------------------------------------- #
# Test 8 — Builder uses artifact contract, not _PUBLIC_REPORT_KEYS
# --------------------------------------------------------------------------- #


def test_builder_uses_artifact_contract(monkeypatch, tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    # Patch is_client_safe_artifact in the builder to refuse everything.
    # If the builder uses the contract, nothing gets copied into the package.
    monkeypatch.setattr(
        client_package_builder,
        "is_client_safe_artifact",
        lambda key_or_filename: False,
    )

    result = build_client_delivery_package(run_dir)

    assert result.files_included == ()
    # Only the manifest itself should sit in the package directory.
    actual_files = [p.name for p in result.package_dir.iterdir() if p.is_file()]
    assert actual_files == ["client_package_manifest.json"]


def test_builder_does_not_import_public_report_keys() -> None:
    """Source-level guard: the builder must not depend on the legacy
    ``_PUBLIC_REPORT_KEYS`` list as a delivery filter."""
    src = Path(client_package_builder.__file__).read_text(encoding="utf-8")
    assert "_PUBLIC_REPORT_KEYS" not in src


# --------------------------------------------------------------------------- #
# Test 9 — Boundary callable returns JSON-friendly dict
# --------------------------------------------------------------------------- #


def test_boundary_returns_json_friendly_dict(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    payload = build_client_package_for_job(run_dir)

    assert isinstance(payload, dict)
    # Must round-trip through json with no custom encoder.
    serialised = json.dumps(payload)
    reloaded = json.loads(serialised)
    assert reloaded["report_version"] == "v2.9.6"
    # No dataclass leakage: every nested object is a primitive.
    for f in reloaded["files_included"]:
        assert isinstance(f["package_path"], str)
        assert isinstance(f["source_path"], str)


# --------------------------------------------------------------------------- #
# Test 10 — Existing artifact discovery unaffected
# --------------------------------------------------------------------------- #


def test_collect_job_artifacts_still_works(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    artifacts = collect_job_artifacts(run_dir)
    assert artifacts.run_dir == run_dir
    assert artifacts.client_outputs.valid_emails == run_dir / "valid_emails.xlsx"
    assert (
        artifacts.reports.smtp_runtime_summary
        == run_dir / "smtp_runtime_summary.json"
    )

    # Discoverable artifact name maps unchanged.
    assert isinstance(_TECHNICAL_CSV_NAMES, dict)
    assert isinstance(_CLIENT_OUTPUT_NAMES, dict)
    assert isinstance(_REPORT_NAMES, dict)


# --------------------------------------------------------------------------- #
# Test 11 — No live network
# --------------------------------------------------------------------------- #


def test_no_network_imports_in_builder() -> None:
    src = Path(client_package_builder.__file__).read_text(encoding="utf-8")
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
        assert token not in src, f"builder must not contain {token!r}"


# --------------------------------------------------------------------------- #
# Result-object sanity
# --------------------------------------------------------------------------- #


def test_result_object_shape(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    result = build_client_delivery_package(run_dir)

    assert isinstance(result, ClientPackageResult)
    assert all(isinstance(f, ClientPackageFile) for f in result.files_included)
    assert isinstance(result.files_excluded, tuple)
    assert isinstance(result.warnings, tuple)


def test_custom_output_dir_honoured(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    custom = tmp_path / "deliveries" / "client_pkg"
    result = build_client_delivery_package(run_dir, output_dir=custom)

    assert result.package_dir == custom.resolve()
    assert (custom / "valid_emails.xlsx").is_file()
    assert (custom / "client_package_manifest.json").is_file()
    # default subdir was not used.
    assert not (run_dir / "client_delivery_package").exists()


def test_package_idempotent_on_rerun(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    first = build_client_delivery_package(run_dir)
    second = build_client_delivery_package(run_dir)

    assert first.package_dir == second.package_dir
    # Re-running does not duplicate or break files.
    for f in second.files_included:
        assert f.package_path.is_file()


# --------------------------------------------------------------------------- #
# V2.9.9 — atomic manifest write + legacy-key rejection
# --------------------------------------------------------------------------- #


def test_manifest_atomic_write_leaves_no_temp_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    result = build_client_delivery_package(run_dir)

    assert result.manifest_path.is_file()
    # No leftover temp file from atomic write helper.
    leftover = result.package_dir / f".{result.manifest_path.name}.tmp"
    assert not leftover.exists(), f"temp file leaked: {leftover}"
    # Manifest is valid JSON, not a partial write.
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["report_version"] == "v2.9.6"


def test_manifest_write_failure_leaves_no_corrupt_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)

    # Force os.replace inside atomic_write_json to fail. The helper's
    # finally block must clean up the temp file even on failure, and the
    # final manifest path must not exist as a corrupt half-written file.
    from app import atomic_io

    real_replace = atomic_io.os.replace

    def _failing_replace(src, dst):  # type: ignore[no-untyped-def]
        raise OSError("simulated rename failure")

    monkeypatch.setattr(atomic_io.os, "replace", _failing_replace)

    with pytest.raises(OSError):
        build_client_delivery_package(run_dir)

    # Restore for sanity (test cleanup) — pytest will undo monkeypatch
    # automatically, but assertions below run before that.
    pkg_dir = run_dir / "client_delivery_package"
    if pkg_dir.is_dir():
        manifest = pkg_dir / "client_package_manifest.json"
        # Either no manifest at all, or a stale one from a prior run —
        # what we must NOT see is a temp file dangling.
        leftover = pkg_dir / ".client_package_manifest.json.tmp"
        assert not leftover.exists(), (
            f"atomic write left temp file behind: {leftover}"
        )

    # Sanity: rolling back the monkeypatch restores normal behavior.
    monkeypatch.setattr(atomic_io.os, "replace", real_replace)
    result = build_client_delivery_package(run_dir)
    assert result.manifest_path.is_file()


def test_legacy_public_keys_rejected_by_package_builder(
    tmp_path: Path,
) -> None:
    """V2.9.9: documents that legacy ``_PUBLIC_REPORT_KEYS`` items
    classified operator_only / technical_debug are NEVER copied into
    the client package, even though they are exposed by the operator
    UI's results endpoint."""
    from app.server import _PUBLIC_REPORT_KEYS

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # Minimal client-safe set so the package can still be built.
    pd.DataFrame({"email": ["a@b.com"]}).to_excel(
        run_dir / "valid_emails.xlsx", sheet_name="emails", index=False
    )
    pd.DataFrame({"email": []}).to_excel(
        run_dir / "review_emails.xlsx", sheet_name="emails", index=False
    )
    pd.DataFrame({"email": []}).to_excel(
        run_dir / "invalid_or_bounce_risk.xlsx", sheet_name="emails", index=False
    )
    # Drop every legacy public report into the run dir alongside.
    legacy_files = {
        "processing_report_json": "processing_report.json",
        "processing_report_csv": "processing_report.csv",
        "domain_summary": "domain_summary.csv",
        "typo_corrections": "typo_corrections.csv",
        "duplicate_summary": "duplicate_summary.csv",
    }
    for filename in legacy_files.values():
        (run_dir / filename).write_text("placeholder", encoding="utf-8")

    result = build_client_delivery_package(run_dir)

    excluded_filenames = {x["filename"] for x in result.files_excluded}
    for key, filename in legacy_files.items():
        assert filename in excluded_filenames, (
            f"legacy public key {key!r} ({filename}) was not excluded"
        )
        entry = next(
            x for x in result.files_excluded if x["filename"] == filename
        )
        assert entry["reason"] == "not_client_safe"
        assert entry["audience"] in {"operator_only", "technical_debug"}
        assert not (result.package_dir / filename).exists()

    # And the public-keys list itself remains the operator UI list,
    # not the client delivery contract — we are not changing it here.
    assert "processing_report_json" in _PUBLIC_REPORT_KEYS
    assert "domain_summary" in _PUBLIC_REPORT_KEYS


# --------------------------------------------------------------------------- #
# V2.10.8.2 — Safe-only delivery note + manifest extension
# --------------------------------------------------------------------------- #


_SAFE_ONLY_NOTE_FILENAME = "SAFE_ONLY_DELIVERY_NOTE.txt"


def _build_partial_run(
    run_dir: Path,
    *,
    valid_rows: int,
    review_rows: int,
    rejected_rows: int,
    include_approved_original: bool = True,
) -> None:
    """Build a run whose row mix exercises the partial-delivery rule."""
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_xlsx(run_dir / "valid_emails.xlsx", rows=valid_rows)
    _write_xlsx(run_dir / "review_emails.xlsx", rows=review_rows)
    _write_xlsx(run_dir / "invalid_or_bounce_risk.xlsx", rows=rejected_rows)
    _write_summary_xlsx(run_dir / "summary_report.xlsx")
    if include_approved_original:
        _write_xlsx(
            run_dir / "approved_original_format.xlsx",
            rows=max(valid_rows, 1),
        )


def test_safe_only_note_created_when_partial_applies(tmp_path: Path) -> None:
    """WY-100 shape: warn-only run with safe rows AND review/rejected rows."""
    run_dir = tmp_path / "run"
    _build_partial_run(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
    )

    result = build_client_delivery_package(run_dir)

    note = result.package_dir / _SAFE_ONLY_NOTE_FILENAME
    assert note.is_file()
    body = note.read_text(encoding="utf-8")
    # Every line called out by the spec must be present verbatim.
    for snippet in (
        "This is a safe-only partial delivery package.",
        "The full run is NOT ready_for_client.",
        "safe_count: 7",
        "review_count: 59",
        "rejected_count: 34",
        "delivery_mode: safe_only_partial",
    ):
        assert snippet in body, f"missing line in note: {snippet!r}"
    # Sanity: the note ends with a trailing newline, per spec.
    assert body.endswith("\n")


def test_safe_only_note_appears_in_main_files_included(tmp_path: Path) -> None:
    """The note is itself client_safe; the main manifest must list it."""
    run_dir = tmp_path / "run"
    _build_partial_run(run_dir, valid_rows=7, review_rows=3, rejected_rows=0)

    result = build_client_delivery_package(run_dir)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    note_entries = [
        f for f in manifest["files_included"]
        if f["filename"] == _SAFE_ONLY_NOTE_FILENAME
    ]
    assert len(note_entries) == 1, manifest["files_included"]
    entry = note_entries[0]
    assert entry["key"] == "safe_only_delivery_note"
    assert entry["audience"] == "client_safe"


def test_safe_only_delivery_block_supported(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _build_partial_run(
        run_dir,
        valid_rows=7,
        review_rows=59,
        rejected_rows=34,
    )

    result = build_client_delivery_package(run_dir)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    block = manifest["safe_only_delivery"]
    assert block["supported"] is True
    assert block["note_filename"] == _SAFE_ONLY_NOTE_FILENAME
    assert block["safe_count"] == 7
    assert block["review_count"] == 59
    assert block["rejected_count"] == 34


def test_safe_only_files_included_excludes_review_and_rejected(
    tmp_path: Path,
) -> None:
    """The safe-only sub-list is a strict subset — review/rejected/etc.
    XLSXs may sit in the main package but must never appear here."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)  # safe=5, review=3, rejected=2 → partial applies

    result = build_client_delivery_package(run_dir)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    block = manifest["safe_only_delivery"]
    assert block["supported"] is True
    safe_only_filenames = {f["filename"] for f in block["files_included"]}

    expected_included = {
        "valid_emails.xlsx",
        "approved_original_format.xlsx",
        "summary_report.xlsx",
        _SAFE_ONLY_NOTE_FILENAME,
    }
    assert expected_included <= safe_only_filenames, (
        f"safe-only must include {expected_included - safe_only_filenames}"
    )

    must_be_excluded = {
        "review_emails.xlsx",
        "invalid_or_bounce_risk.xlsx",
        "duplicate_emails.xlsx",
        "hard_fail_emails.xlsx",
    }
    leaked = must_be_excluded & safe_only_filenames
    assert not leaked, f"safe-only leaked review/rejected files: {leaked}"

    # And the strict subset is exactly what the spec says — no surprises.
    assert safe_only_filenames == expected_included, (
        f"unexpected safe-only files: {safe_only_filenames ^ expected_included}"
    )


def test_safe_only_note_absent_when_safe_count_zero(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _build_partial_run(
        run_dir,
        valid_rows=0,
        review_rows=3,
        rejected_rows=2,
    )

    result = build_client_delivery_package(run_dir)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert not (result.package_dir / _SAFE_ONLY_NOTE_FILENAME).exists()
    block = manifest["safe_only_delivery"]
    assert block["supported"] is False
    assert block["note_filename"] is None
    assert block["files_included"] == []


def test_safe_only_note_absent_when_no_review_or_rejected(tmp_path: Path) -> None:
    """No review and no rejected ⇒ the full package IS the delivery —
    a safe-only side-channel would be redundant."""
    run_dir = tmp_path / "run"
    _build_partial_run(
        run_dir,
        valid_rows=5,
        review_rows=0,
        rejected_rows=0,
    )

    result = build_client_delivery_package(run_dir)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert not (result.package_dir / _SAFE_ONLY_NOTE_FILENAME).exists()
    block = manifest["safe_only_delivery"]
    assert block["supported"] is False
    assert block["note_filename"] is None
    assert block["files_included"] == []


def test_full_package_keeps_review_and_rejected_when_partial_applies(
    tmp_path: Path,
) -> None:
    """Regression guard: V2.10.8.2 must NOT remove review/rejected
    XLSXs from the main client delivery package — only the safe-only
    sub-list filters them out."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _populate_run_dir(run_dir)  # partial applies

    result = build_client_delivery_package(run_dir)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    main_filenames = {f["filename"] for f in manifest["files_included"]}
    # Full delivery still carries these client_safe files.
    assert "review_emails.xlsx" in main_filenames
    assert "invalid_or_bounce_risk.xlsx" in main_filenames
    assert "duplicate_emails.xlsx" in main_filenames
    assert "hard_fail_emails.xlsx" in main_filenames

    # And — still — none of them appear in the safe-only sub-list.
    safe_only_filenames = {
        f["filename"] for f in manifest["safe_only_delivery"]["files_included"]
    }
    assert "review_emails.xlsx" not in safe_only_filenames
    assert "invalid_or_bounce_risk.xlsx" not in safe_only_filenames
    assert "duplicate_emails.xlsx" not in safe_only_filenames
    assert "hard_fail_emails.xlsx" not in safe_only_filenames


def test_stale_safe_only_note_is_removed_on_rebuild(tmp_path: Path) -> None:
    """If a previous build produced the note but a rebuild's counts no
    longer satisfy the partial rule, the disk must agree with the
    manifest (note absent, supported=false)."""
    run_dir = tmp_path / "run"
    _build_partial_run(run_dir, valid_rows=7, review_rows=3, rejected_rows=2)
    first = build_client_delivery_package(run_dir)
    assert (first.package_dir / _SAFE_ONLY_NOTE_FILENAME).is_file()

    # Rewrite review/rejected to zero rows → partial no longer applies.
    _write_xlsx(run_dir / "review_emails.xlsx", rows=0)
    _write_xlsx(run_dir / "invalid_or_bounce_risk.xlsx", rows=0)

    second = build_client_delivery_package(run_dir)
    manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))

    assert not (second.package_dir / _SAFE_ONLY_NOTE_FILENAME).exists()
    assert manifest["safe_only_delivery"]["supported"] is False
    assert manifest["safe_only_delivery"]["note_filename"] is None
