"""Pytest bootstrap for local vendored dependencies + V2.2 SMTP safety net.

Two responsibilities:

  1. Prepend the local ``.vendor_py`` directory so vendored deps load
     before site-packages.
  2. Install an autouse fixture that monkey-patches the live SMTP probe
     (``app.validation_v2.smtp_probe.probe_email_smtplib`` and the
     re-export inside the V2.2 stage module) to a deterministic offline
     stub. Production defaults in ``configs/default.yaml`` set
     ``smtp_probe.enabled=true`` and ``smtp_probe.dry_run=false`` so the
     pipeline could otherwise open a real socket during tests; this
     fixture guarantees that *cannot* happen in CI.

Tests that want a specific SMTP outcome should construct
``SMTPVerificationStage(probe_fn=...)`` with their own mock or
monkey-patch the stub via ``monkeypatch.setattr`` inside the test.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


_VENDOR_PATH = Path(__file__).resolve().parent / ".vendor_py"
if _VENDOR_PATH.exists():
    sys.path.insert(0, str(_VENDOR_PATH))


def _safe_offline_probe(email: str, **_kwargs: object):
    """Deterministic offline replacement for ``probe_email_smtplib``.

    Imported lazily so the vendor path is already on ``sys.path`` by the
    time we touch ``app.validation_v2``. Returns ``inconclusive=True``
    so any test that doesn't explicitly mock the probe still gets a
    safe, predictable result instead of a network call.
    """
    from app.validation_v2.smtp_probe import SMTPResult

    return SMTPResult(
        success=False,
        response_code=None,
        response_message="conftest_offline_stub",
        is_catch_all_like=False,
        inconclusive=True,
    )


@pytest.fixture(autouse=True)
def _block_live_smtp(monkeypatch: pytest.MonkeyPatch) -> None:
    """Autouse safety net: every test runs with the live probe disabled.

    Patches every known import path of ``probe_email_smtplib``: the
    canonical module and the V2.2 stage's local re-export. Tests that
    inject ``probe_fn`` directly are unaffected; tests that don't pass a
    mock get the offline stub above.
    """
    from app.validation_v2 import smtp_probe as _smtp_probe_mod

    monkeypatch.setattr(
        _smtp_probe_mod, "probe_email_smtplib", _safe_offline_probe
    )

    # The V2.2 stage imports ``probe_email_smtplib`` at module load time,
    # so patch the bound name on the stage module too.
    try:
        from app.engine.stages import smtp_verification as _v22_stage_mod

        monkeypatch.setattr(
            _v22_stage_mod, "probe_email_smtplib", _safe_offline_probe
        )
    except Exception:
        # The stage module is optional during very early imports; if it
        # isn't loaded yet the canonical patch above is enough.
        pass
