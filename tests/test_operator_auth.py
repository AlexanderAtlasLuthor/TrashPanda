"""Tests for the operator bearer-token dependency."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from app import auth, server


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    server.JOB_STORE.clear()
    monkeypatch.setattr(server, "RUNTIME_ROOT", tmp_path / "runtime")
    with TestClient(server.app) as c:
        yield c
    server.JOB_STORE.clear()


# --------------------------------------------------------------------------- #
# Loader semantics                                                             #
# --------------------------------------------------------------------------- #


class TestTokenLoader:
    def test_no_env_means_auth_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(auth.ENV_VAR_PRIMARY, raising=False)
        monkeypatch.delenv(auth.ENV_VAR_LIST, raising=False)
        assert auth.auth_enabled() is False

    def test_primary_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "abc")
        monkeypatch.delenv(auth.ENV_VAR_LIST, raising=False)
        assert auth.auth_enabled() is True

    def test_list_concatenation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "primary")
        monkeypatch.setenv(auth.ENV_VAR_LIST, "second , third,")
        # Internal helper: ensure all three tokens are recognised.
        tokens = auth._load_configured_tokens()  # noqa: SLF001
        assert set(tokens) == {"primary", "second", "third"}


# --------------------------------------------------------------------------- #
# HTTP behaviour on operator endpoints                                         #
# --------------------------------------------------------------------------- #


class TestOperatorAuthHttp:
    def test_no_token_configured_means_no_auth_required(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(auth.ENV_VAR_PRIMARY, raising=False)
        monkeypatch.delenv(auth.ENV_VAR_LIST, raising=False)
        # Operator job lookup should not 401 when auth is off.
        res = client.get("/api/operator/jobs/job_does_not_exist")
        assert res.status_code != 401
        assert res.status_code != 403

    def test_missing_token_returns_401(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "secret-token")
        res = client.get("/api/operator/jobs/job_x")
        assert res.status_code == 401
        body = res.json()
        error = body.get("error") or body
        assert error["error_type"] == "operator_auth_required"
        # WWW-Authenticate header should advertise Bearer scheme.
        assert "Bearer" in res.headers.get("www-authenticate", "")

    def test_wrong_token_returns_403(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "secret-token")
        res = client.get(
            "/api/operator/jobs/job_x",
            headers={"Authorization": "Bearer wrong"},
        )
        assert res.status_code == 403
        body = res.json()
        error = body.get("error") or body
        assert error["error_type"] == "operator_auth_invalid"

    def test_correct_token_passes(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "secret-token")
        res = client.get(
            "/api/operator/jobs/job_x",
            headers={"Authorization": "Bearer secret-token"},
        )
        # Auth passed: a missing-job 404 (or any non-auth status) is OK.
        assert res.status_code not in (401, 403)

    def test_x_header_alternative_passes(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "secret-token")
        res = client.get(
            "/api/operator/jobs/job_x",
            headers={"X-TrashPanda-Operator-Token": "secret-token"},
        )
        assert res.status_code not in (401, 403)

    def test_rotation_via_tokens_list(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "new-token")
        monkeypatch.setenv(auth.ENV_VAR_LIST, "old-token")
        # Old token must still pass during rollout.
        res_old = client.get(
            "/api/operator/jobs/job_x",
            headers={"Authorization": "Bearer old-token"},
        )
        res_new = client.get(
            "/api/operator/jobs/job_x",
            headers={"Authorization": "Bearer new-token"},
        )
        assert res_old.status_code not in (401, 403)
        assert res_new.status_code not in (401, 403)
