"""Tests for the V2-extras client_package_builder additions:

  - always-on README_CLIENT.txt inside the delivery package
  - PRIMARY artifact pointer in the manifest + ``primary: true`` flag
    on the recommended row in ``files_included``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app.client_package_builder import build_client_delivery_package


def _write_xlsx(path: Path, rows: int = 1) -> None:
    df = pd.DataFrame({"email": [f"x{i}@example-corp.com" for i in range(rows)]})
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False)


@pytest.fixture()
def populated_run(tmp_path: Path) -> Path:
    """A minimal "run dir" with the client-safe XLSXs the builder expects."""

    _write_xlsx(tmp_path / "valid_emails.xlsx", rows=3)
    _write_xlsx(tmp_path / "review_emails.xlsx", rows=2)
    _write_xlsx(tmp_path / "invalid_or_bounce_risk.xlsx", rows=1)
    _write_xlsx(tmp_path / "summary_report.xlsx", rows=0)
    _write_xlsx(tmp_path / "approved_original_format.xlsx", rows=3)
    return tmp_path


def _load_manifest(run_dir: Path) -> dict:
    package_dir = run_dir / "client_delivery_package"
    return json.loads(
        (package_dir / "client_package_manifest.json").read_text(encoding="utf-8")
    )


class TestPrimaryAndReadme:
    def test_readme_is_written_into_package(self, populated_run: Path) -> None:
        result = build_client_delivery_package(populated_run)
        readme = result.package_dir / "README_CLIENT.txt"
        assert readme.is_file()
        body = readme.read_text(encoding="utf-8")
        assert "USE THIS FILE FIRST" in body
        # Must name the actually-shipped file rather than a placeholder.
        assert "approved_original_format.xlsx" in body

    def test_readme_listed_as_client_safe_in_manifest(
        self, populated_run: Path
    ) -> None:
        build_client_delivery_package(populated_run)
        manifest = _load_manifest(populated_run)
        readme_entry = next(
            (
                f
                for f in manifest["files_included"]
                if f["filename"] == "README_CLIENT.txt"
            ),
            None,
        )
        assert readme_entry is not None
        assert readme_entry["audience"] == "client_safe"
        assert readme_entry["key"] == "client_readme"

    def test_primary_artifact_block_points_at_approved_original_format(
        self, populated_run: Path
    ) -> None:
        build_client_delivery_package(populated_run)
        manifest = _load_manifest(populated_run)
        primary = manifest.get("primary_artifact")
        assert primary is not None
        assert primary["key"] == "approved_original_format"
        assert primary["filename"] == "approved_original_format.xlsx"
        assert primary["label"] == "Recommended download"

    def test_primary_flag_set_on_recommended_row_only(
        self, populated_run: Path
    ) -> None:
        build_client_delivery_package(populated_run)
        manifest = _load_manifest(populated_run)
        primaries = [
            f["filename"]
            for f in manifest["files_included"]
            if f.get("primary") is True
        ]
        assert primaries == ["approved_original_format.xlsx"]

    def test_falls_back_to_valid_emails_when_original_format_absent(
        self, tmp_path: Path
    ) -> None:
        _write_xlsx(tmp_path / "valid_emails.xlsx", rows=1)
        _write_xlsx(tmp_path / "review_emails.xlsx", rows=0)
        _write_xlsx(tmp_path / "invalid_or_bounce_risk.xlsx", rows=0)
        _write_xlsx(tmp_path / "summary_report.xlsx", rows=0)

        build_client_delivery_package(tmp_path)
        manifest = _load_manifest(tmp_path)
        assert manifest["primary_artifact"]["key"] == "valid_emails"

    def test_readme_filename_is_recorded_at_top_level(
        self, populated_run: Path
    ) -> None:
        build_client_delivery_package(populated_run)
        manifest = _load_manifest(populated_run)
        assert manifest["readme_filename"] == "README_CLIENT.txt"
