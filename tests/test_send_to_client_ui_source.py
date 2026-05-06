from __future__ import annotations

from pathlib import Path


def test_send_to_client_button_surfaces_blocking_states() -> None:
    src = Path("trashpanda-next/components/SendToClientButton.tsx").read_text(
        encoding="utf-8"
    )

    pending_pos  = src.find('summary.delivery_state === "smtp_verification_pending"')
    cleaning_pos = src.find('summary.delivery_state === "cleaning_completed"')

    assert pending_pos  != -1
    assert cleaning_pos != -1
    assert pending_pos < cleaning_pos
    assert "SMTP verification has not run yet"          in src
    assert "Rerun with production SMTP config"          in src
    assert "Delivery readiness still needs verification" in src
