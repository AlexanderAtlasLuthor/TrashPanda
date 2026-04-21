"""Classification rules for controlled SMTP probe results."""

from __future__ import annotations

from .smtp_result import SMTPProbeResult


SMTP_STATUS_VALID = "valid"
SMTP_STATUS_INVALID = "invalid"
SMTP_STATUS_UNCERTAIN = "uncertain"
SMTP_STATUS_NOT_ATTEMPTED = "not_attempted"


class SMTPResultClassifier:
    """Convert a raw SMTP result into conservative signal flags."""

    def classify(self, result: SMTPProbeResult) -> dict[str, object]:
        if result.error_type:
            return _uncertain(f"smtp_error:{result.error_type}")

        if result.code == 250:
            return {
                "smtp_valid": True,
                "smtp_invalid": False,
                "smtp_uncertain": False,
                "smtp_status": SMTP_STATUS_VALID,
                "classification_reason": "smtp_250_accepted",
            }

        if result.code == 550:
            return {
                "smtp_valid": False,
                "smtp_invalid": True,
                "smtp_uncertain": False,
                "smtp_status": SMTP_STATUS_INVALID,
                "classification_reason": "smtp_550_rejected",
            }

        if result.code is not None and 400 <= result.code < 500:
            return _uncertain("smtp_4xx_temporary_response")

        return _uncertain("smtp_response_uncertain")


def _uncertain(reason: str) -> dict[str, object]:
    return {
        "smtp_valid": False,
        "smtp_invalid": False,
        "smtp_uncertain": True,
        "smtp_status": SMTP_STATUS_UNCERTAIN,
        "classification_reason": reason,
    }


__all__ = [
    "SMTPResultClassifier",
    "SMTP_STATUS_VALID",
    "SMTP_STATUS_INVALID",
    "SMTP_STATUS_UNCERTAIN",
    "SMTP_STATUS_NOT_ATTEMPTED",
]
