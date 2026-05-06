"""V2.10.13 — defensive_clean smoke tests.

The script wraps the existing pipeline with ``smtp_probe.enabled =
False`` and emits the customer bundle. We do not exercise the full
pipeline here (that needs fixtures the wider test suite owns) — we
verify the two invariants that make the script honest:

  1. The flag flip actually takes effect on the SMTP-verification
     gating function (config object is mutable).
  2. The customer bundle path runs end-to-end with no SMTP artifacts
     present (defensive-only output).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app.config import SMTPProbeConfig
from app.customer_bundle import (
    CLEAN_DELIVERABLE_CSV,
    CUSTOMER_BUNDLE_DIRNAME,
    HIGH_RISK_REMOVED_CSV,
    REVIEW_PROVIDER_LIMITED_CSV,
    emit_customer_bundle,
)
from app.engine.stages.smtp_verification import _smtp_probe_enabled
from app.pilot_send.evidence import SMTP_EVIDENCE_REPORT_FILENAME


class TestSMTPProbeFlagIsHonored:
    def test_setting_enabled_false_disables_the_stage_gate(self):
        # The gate function in smtp_verification reads
        # config.smtp_probe.enabled. If our defensive_clean override
        # is silently dropped (e.g. the config dataclass is frozen),
        # this would still return True and SMTP would still run.
        cfg = SimpleNamespace(smtp_probe=SMTPProbeConfig())
        cfg.smtp_probe.enabled = True
        ctx = SimpleNamespace(config=cfg)
        assert _smtp_probe_enabled(ctx) is True

        cfg.smtp_probe.enabled = False
        assert _smtp_probe_enabled(ctx) is False

    def test_smtp_probe_config_is_mutable(self):
        # Defensive-only relies on this. If someone freezes the
        # dataclass, this test catches it before defensive runs
        # silently send SMTP traffic.
        c = SMTPProbeConfig()
        c.enabled = False
        assert c.enabled is False


class TestDefensiveModeOutputsBundle:
    def test_bundle_emits_four_csvs_with_no_pilot_artifacts(
        self, tmp_path: Path,
    ):
        # Simulate a defensive-only pipeline run: only the technical
        # CSVs exist (no pilot xlsx). The customer bundle must still
        # produce the four customer-facing CSVs so the operator has
        # a deliverable to hand the buyer.
        pd.DataFrame({"email": ["a@x.com", "b@x.com"]}).to_csv(
            tmp_path / "clean_high_confidence.csv", index=False,
        )
        pd.DataFrame({"email": ["bad@invalid"]}).to_csv(
            tmp_path / "removed_invalid.csv", index=False,
        )

        result = emit_customer_bundle(tmp_path)

        bundle_dir = tmp_path / CUSTOMER_BUNDLE_DIRNAME
        for filename in (
            CLEAN_DELIVERABLE_CSV,
            REVIEW_PROVIDER_LIMITED_CSV,
            HIGH_RISK_REMOVED_CSV,
            SMTP_EVIDENCE_REPORT_FILENAME,
        ):
            assert (bundle_dir / filename).is_file(), filename

        # No pilot ran → no review_provider_limited rows, no SMTP
        # evidence rows. clean_deliverable carries the technical-CSV
        # rows; high_risk_removed carries the syntax/MX failures.
        assert result.counts["review_provider_limited"] == 0
        assert result.counts["smtp_evidence_report"] == 0
        assert result.counts["clean_deliverable"] == 2
        assert result.counts["high_risk_removed"] == 1
