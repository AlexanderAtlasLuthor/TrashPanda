"""Result model for the controlled SMTP sampler."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SMTPProbeResult:
    """Single SMTP probe outcome.

    The sampler stops at the RCPT TO response and records only the
    observable signal. It never sends DATA or attempts delivery.
    """

    success: bool
    code: int | None
    message: str | None
    latency_ms: float | None
    error_type: str | None


__all__ = ["SMTPProbeResult"]
