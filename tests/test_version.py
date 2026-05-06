"""V2.10.15 — version resolution tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from app import version as version_mod


@pytest.fixture(autouse=True)
def _clear_cache():
    version_mod.reset_cache()
    yield
    version_mod.reset_cache()


class TestEnvOverride:
    def test_env_override_short_circuits(self, monkeypatch):
        monkeypatch.setenv(
            "TRASHPANDA_VERSION_OVERRIDE",
            "deadbeefcafebabe1234567890",
        )
        info = version_mod.get_version()
        assert info.commit == "deadbeefcafebabe1234567890"
        assert info.short_commit == "deadbee"
        assert info.source == "env_override"
        assert info.dirty is False


class TestVersionFilePath:
    def test_reads_version_file_from_repo_root(
        self, monkeypatch, tmp_path: Path,
    ):
        # Make _repo_root point at a temp dir that contains a VERSION
        # file. Also pin cwd to a directory WITHOUT a VERSION so the
        # cwd-first lookup misses and the repo-root one wins.
        repo = tmp_path / "fake_repo"
        repo.mkdir()
        (repo / "VERSION").write_text("abc1234567890\n")
        monkeypatch.setattr(version_mod, "_repo_root", lambda: repo)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TRASHPANDA_VERSION_OVERRIDE", raising=False)

        info = version_mod.get_version()
        assert info.commit == "abc1234567890"
        assert info.short_commit == "abc1234"
        assert info.source == "version_file"

    def test_version_file_with_trailing_whitespace_is_trimmed(
        self, monkeypatch, tmp_path: Path,
    ):
        repo = tmp_path / "fake_repo"
        repo.mkdir()
        # A VERSION file written by `git rev-parse HEAD > VERSION`
        # ends with a newline. Plus tolerate optional human comment.
        (repo / "VERSION").write_text("abc1234   # deployed 2026-05-06\n")
        monkeypatch.setattr(version_mod, "_repo_root", lambda: repo)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TRASHPANDA_VERSION_OVERRIDE", raising=False)

        info = version_mod.get_version()
        assert info.commit == "abc1234"

    def test_empty_version_file_falls_through(
        self, monkeypatch, tmp_path: Path,
    ):
        repo = tmp_path / "fake_repo"
        repo.mkdir()
        (repo / "VERSION").write_text("")
        monkeypatch.setattr(version_mod, "_repo_root", lambda: repo)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TRASHPANDA_VERSION_OVERRIDE", raising=False)
        # No git in a brand new tmp_path, no env override → unknown.
        monkeypatch.setattr(version_mod, "_git_call", lambda *a, **k: None)

        info = version_mod.get_version()
        assert info.commit == version_mod.VERSION_UNKNOWN
        assert info.source == "unknown"


class TestGitFallback:
    def test_git_fallback_when_no_version_file(
        self, monkeypatch, tmp_path: Path,
    ):
        repo = tmp_path / "fake_repo"
        repo.mkdir()  # no VERSION file
        monkeypatch.setattr(version_mod, "_repo_root", lambda: repo)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TRASHPANDA_VERSION_OVERRIDE", raising=False)

        # Stub _git_call to mimic a git repo.
        calls: list[list[str]] = []

        def fake_git(args, *, cwd):
            calls.append(args)
            if args == ["rev-parse", "HEAD"]:
                return "fedcba9876543210"
            if args == ["rev-parse", "--abbrev-ref", "HEAD"]:
                return "main"
            if args == ["status", "--porcelain"]:
                return ""
            return None

        monkeypatch.setattr(version_mod, "_git_call", fake_git)

        info = version_mod.get_version()
        assert info.commit == "fedcba9876543210"
        assert info.short_commit == "fedcba9"
        assert info.branch == "main"
        assert info.dirty is False
        assert info.source == "git"
        assert ["rev-parse", "HEAD"] in calls

    def test_git_fallback_marks_dirty_when_uncommitted(
        self, monkeypatch, tmp_path: Path,
    ):
        repo = tmp_path / "fake_repo"
        repo.mkdir()
        monkeypatch.setattr(version_mod, "_repo_root", lambda: repo)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TRASHPANDA_VERSION_OVERRIDE", raising=False)

        def fake_git(args, *, cwd):
            if args == ["rev-parse", "HEAD"]:
                return "abcdef1"
            if args == ["rev-parse", "--abbrev-ref", "HEAD"]:
                return "feature/foo"
            if args == ["status", "--porcelain"]:
                return " M app/server.py"
            return None

        monkeypatch.setattr(version_mod, "_git_call", fake_git)

        info = version_mod.get_version()
        assert info.dirty is True
        assert info.branch == "feature/foo"


class TestUnknownFallback:
    def test_unknown_when_no_signal(self, monkeypatch, tmp_path: Path):
        repo = tmp_path / "fake_repo"
        repo.mkdir()
        monkeypatch.setattr(version_mod, "_repo_root", lambda: repo)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("TRASHPANDA_VERSION_OVERRIDE", raising=False)
        monkeypatch.setattr(version_mod, "_git_call", lambda *a, **k: None)

        info = version_mod.get_version()
        assert info.commit == version_mod.VERSION_UNKNOWN
        assert info.short_commit == version_mod.VERSION_UNKNOWN
        assert info.source == "unknown"


class TestSerialization:
    def test_to_dict_has_stable_shape(self, monkeypatch):
        monkeypatch.setenv("TRASHPANDA_VERSION_OVERRIDE", "1234567abcdef")
        info = version_mod.get_version()
        d = info.to_dict()
        assert set(d.keys()) == {
            "commit", "short_commit", "branch", "dirty", "source",
        }
        # All values JSON-safe (str / bool).
        assert isinstance(d["commit"], str)
        assert isinstance(d["dirty"], bool)


class TestStartupBanner:
    def test_log_startup_banner_emits_a_record(self, monkeypatch, caplog):
        import logging
        monkeypatch.setenv("TRASHPANDA_VERSION_OVERRIDE", "abcdef0")
        with caplog.at_level(logging.INFO, logger="app.version"):
            version_mod.log_startup_banner()
        assert any("trashpanda revision" in r.message for r in caplog.records)
        assert any("abcdef0" in r.message for r in caplog.records)
