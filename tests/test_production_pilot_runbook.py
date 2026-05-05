"""V2.9.9 — production pilot runbook tests.

Verifies the operator-facing runbook exists and contains the required
phrases. The runbook is the operator's source of truth for safe pilot
execution; it must be discoverable and must call out the non-negotiable
delivery rules explicitly.
"""

from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNBOOK_PATH = PROJECT_ROOT / "docs" / "production_pilot_runbook.md"


def test_runbook_file_exists() -> None:
    assert RUNBOOK_PATH.is_file(), (
        f"production pilot runbook missing at {RUNBOOK_PATH}"
    )


def test_runbook_has_title() -> None:
    text = RUNBOOK_PATH.read_text(encoding="utf-8")
    assert "TrashPanda V2 Production Pilot Runbook" in text


@pytest.mark.parametrize(
    "phrase",
    [
        "preflight",
        "client package",
        "operator review",
        "approved_original_format.xlsx",
        "uncapped live SMTP",
        "feedback",
        "not ready_for_client",
        "/results/{job_id}",
    ],
)
def test_runbook_contains_required_phrase(phrase: str) -> None:
    text = RUNBOOK_PATH.read_text(encoding="utf-8")
    assert phrase in text, (
        f"runbook missing required phrase: {phrase!r}"
    )


def test_runbook_lists_required_sections() -> None:
    text = RUNBOOK_PATH.read_text(encoding="utf-8")
    expected_sections = [
        "Purpose",
        "When to use this runbook",
        "Preflight checklist",
        "Recommended pilot size",
        "SMTP prerequisites",
        "Recommended rollout config",
        "Run sequence",
        "Build client package",
        "Run operator review gate",
        "What to send to the client",
        "What not to send to the client",
        "If safe export is empty",
        "How to ingest feedback after campaign",
        "How to generate feedback domain intel preview",
        "Known caveats",
        "Rollback / stop conditions",
    ]
    for section in expected_sections:
        assert section in text, f"runbook missing section: {section!r}"
