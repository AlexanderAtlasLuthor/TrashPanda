"""Tests for the one-click client-bundle and extra-strict endpoints."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Iterator

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import server


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _write_xlsx(path: Path, rows: int = 1) -> None:
    df = pd.DataFrame(
        {"email": [f"x{i}@example-corp.com" for i in range(rows)]}
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    import csv

    fields = sorted({k for row in rows for k in row.keys()} | {"email"})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _seed_run_dir(tmp_path: Path, job_id: str) -> Path:
    """Build a tiny but realistic run dir for the bundle endpoints."""

    run_dir = tmp_path / "runtime" / "jobs" / job_id / "run_2026_01_01"
    run_dir.mkdir(parents=True, exist_ok=True)

    # XLSX artifacts (the package builder reads these and copies them).
    _write_xlsx(run_dir / "valid_emails.xlsx", rows=3)
    _write_xlsx(run_dir / "review_emails.xlsx", rows=1)
    _write_xlsx(run_dir / "invalid_or_bounce_risk.xlsx", rows=1)
    _write_xlsx(run_dir / "summary_report.xlsx", rows=0)
    _write_xlsx(run_dir / "approved_original_format.xlsx", rows=3)

    # CSVs the extra-strict cleaner consumes.
    _write_csv(
        run_dir / "clean_high_confidence.csv",
        [
            {
                "email": "alice@example-corp.com",
                "smtp_status": "valid",
                "deliverability_probability": "0.95",
                "final_action": "auto_approve",
            },
        ],
    )
    _write_csv(
        run_dir / "review_medium_confidence.csv",
        [
            {
                "email": "someone@yahoo.com",
                "smtp_status": "not_tested",
                "deliverability_probability": "0.85",
                "final_action": "manual_review",
            },
        ],
    )
    _write_csv(run_dir / "removed_invalid.csv", [])

    return run_dir


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    monkeypatch.delenv("TRASHPANDA_OPERATOR_TOKEN", raising=False)
    monkeypatch.delenv("TRASHPANDA_OPERATOR_TOKENS", raising=False)
    with TestClient(server.app) as c:
        yield c
    server.JOB_STORE.clear()


# --------------------------------------------------------------------------- #
# /client-bundle/summary                                                       #
# --------------------------------------------------------------------------- #


class TestClientBundleSummary:
    def test_returns_available_when_run_dir_has_safe_rows(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        _seed_run_dir(tmp_path, "job_demo")
        res = client.get(
            "/api/operator/jobs/job_demo/client-bundle/summary",
        )
        assert res.status_code == 200
        body = res.json()
        assert body["available"] is True
        assert body["safe_count"] == 3
        assert body["primary_filename"] == "approved_original_format.xlsx"
        assert body["delivery_mode"] in {"full", "safe_only_partial"}
        assert body["download_filename"] is not None

    def test_returns_unavailable_when_run_dir_missing(
        self, client: TestClient
    ) -> None:
        res = client.get(
            "/api/operator/jobs/nope/client-bundle/summary",
        )
        # _resolve_run_dir raises 404 when the run dir doesn't exist.
        assert res.status_code in (404, 409, 500)


# --------------------------------------------------------------------------- #
# /client-bundle/download                                                      #
# --------------------------------------------------------------------------- #


class TestClientBundleDownload:
    def test_zip_contains_only_curated_files(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        _seed_run_dir(tmp_path, "job_demo")
        res = client.get(
            "/api/operator/jobs/job_demo/client-bundle/download",
        )
        assert res.status_code == 200
        assert res.headers["content-type"].startswith("application/zip")

        cd = res.headers.get("content-disposition", "")
        assert ".zip" in cd
        assert "_clean_" in cd  # friendly filename pattern

        zf = zipfile.ZipFile(io.BytesIO(res.content))
        names = set(zf.namelist())
        # PRIMARY artifact must be present.
        assert "approved_original_format.xlsx" in names
        # README is generated by the package builder and must be in
        # the curated bundle so the customer knows what to use first.
        assert "README_CLIENT.txt" in names
        # No technical / debug artifacts leaked into the bundle.
        for forbidden in (
            "clean_high_confidence.csv",
            "review_medium_confidence.csv",
            "removed_invalid.csv",
            "operator_review_summary.json",
            "client_package_manifest.json",
        ):
            assert forbidden not in names, f"{forbidden} leaked into bundle"

    def test_404_when_run_dir_missing(self, client: TestClient) -> None:
        res = client.get(
            "/api/operator/jobs/ghost/client-bundle/download",
        )
        assert res.status_code in (404, 409)


# --------------------------------------------------------------------------- #
# /extra-strict/download                                                       #
# --------------------------------------------------------------------------- #


class TestExtraStrictDownload:
    def test_zip_contains_six_extra_strict_files(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        _seed_run_dir(tmp_path, "job_demo")
        res = client.get(
            "/api/operator/jobs/job_demo/extra-strict/download",
        )
        assert res.status_code == 200
        assert res.headers["content-type"].startswith("application/zip")
        cd = res.headers.get("content-disposition", "")
        assert "_extrastrict_" in cd

        zf = zipfile.ZipFile(io.BytesIO(res.content))
        names = set(zf.namelist())
        # Yahoo address is routed to review_catch_all by the cleaner.
        assert "clean_final_extra_strict.xlsx" in names
        assert "review_catch_all.xlsx" in names
        assert "removed_extra_risk.xlsx" in names
        assert "rejected_structural.xlsx" in names
        assert "cleaning_summary.txt" in names
        assert "README_CLIENT.txt" in names


# --------------------------------------------------------------------------- #
# Auth still gates these endpoints when a token is configured                  #
# --------------------------------------------------------------------------- #


class TestBundleAuth:
    def test_401_without_token_when_configured(
        self,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _seed_run_dir(tmp_path, "job_demo")
        monkeypatch.setenv("TRASHPANDA_OPERATOR_TOKEN", "secret")
        res = client.get(
            "/api/operator/jobs/job_demo/client-bundle/summary",
        )
        assert res.status_code == 401

    def test_200_with_correct_token(
        self,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _seed_run_dir(tmp_path, "job_demo")
        monkeypatch.setenv("TRASHPANDA_OPERATOR_TOKEN", "secret")
        res = client.get(
            "/api/operator/jobs/job_demo/client-bundle/summary",
            headers={"Authorization": "Bearer secret"},
        )
        assert res.status_code == 200
