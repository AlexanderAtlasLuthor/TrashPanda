from __future__ import annotations

from pathlib import Path


def test_send_to_client_button_has_smtp_pending_copy() -> None:
    src = Path("trashpanda-next/components/SendToClientButton.tsx").read_text(
        encoding="utf-8"
    )

    pending_pos = src.find('summary.delivery_state === "smtp_verification_pending"')
    blocked_pos = src.find("No rows are safe to send yet")

    assert pending_pos != -1
    assert blocked_pos != -1
    assert pending_pos < blocked_pos
    assert "SMTP verification has not run yet" in src
    assert "Rerun with production SMTP config" in src
