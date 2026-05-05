"""Tests for /healthz and /system/info."""

from __future__ import annotations

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


class TestHealthz:
    def test_returns_ok(self, client: TestClient) -> None:
        res = client.get("/healthz")
        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "ok"
        assert "started_at" in body
        assert isinstance(body["uptime_seconds"], (int, float))
        assert "wall_clock_seconds" in body

    def test_does_not_require_auth_even_when_configured(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Liveness checks must work even when operator auth is enforced —
        # they live on the public ``app``, never on the operator router.
        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "abc")
        res = client.get("/healthz")
        assert res.status_code == 200


class TestSystemInfo:
    def test_returns_minimal_payload(self, client: TestClient) -> None:
        res = client.get("/system/info")
        assert res.status_code == 200
        body = res.json()
        assert "backend_label" in body
        assert "deployment" in body
        assert "auth_enabled" in body
        assert "wall_clock_seconds" in body
        assert "smtp_default_dry_run" in body

    def test_auth_enabled_reflects_env(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(auth.ENV_VAR_PRIMARY, raising=False)
        monkeypatch.delenv(auth.ENV_VAR_LIST, raising=False)
        assert client.get("/system/info").json()["auth_enabled"] is False

        monkeypatch.setenv(auth.ENV_VAR_PRIMARY, "x")
        assert client.get("/system/info").json()["auth_enabled"] is True

    def test_deployment_label_reflects_env(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TRASHPANDA_DEPLOYMENT", "vps")
        monkeypatch.setenv("TRASHPANDA_BACKEND_LABEL", "racknerd")
        body = client.get("/system/info").json()
        assert body["deployment"] == "vps"
        assert body["backend_label"] == "racknerd"
