"""Bounded retry decisions for controlled SMTP probing."""

from __future__ import annotations

from dataclasses import dataclass

from ..interfaces import RetryStrategy
from .smtp_result import SMTPProbeResult


@dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    delay_ms: int | None
    reason: str


class IntelligentRetryStrategy(RetryStrategy):
    """Selective one-retry strategy for transient SMTP outcomes."""

    def __init__(self, *, delay_ms: int = 250) -> None:
        if delay_ms < 0:
            raise ValueError("delay_ms must be >= 0")
        self.delay_ms = int(delay_ms)

    def evaluate(self, result: SMTPProbeResult) -> RetryDecision:
        if result.error_type in {"timeout", "connection_error"}:
            return RetryDecision(True, self.delay_ms, result.error_type)

        if result.code is not None and 400 <= result.code < 500:
            return RetryDecision(True, self.delay_ms, "temporary_4xx")

        if result.code == 250:
            return RetryDecision(False, None, "smtp_250_final")

        if result.code == 550:
            return RetryDecision(False, None, "smtp_550_hard_fail")

        return RetryDecision(False, None, "not_retryable")

    def decide(self, probe_result: dict[str, object]) -> dict[str, object]:
        result = SMTPProbeResult(
            success=bool(probe_result.get("success")),
            code=probe_result.get("code"),  # type: ignore[arg-type]
            message=probe_result.get("message"),  # type: ignore[arg-type]
            latency_ms=probe_result.get("latency_ms"),  # type: ignore[arg-type]
            error_type=probe_result.get("error_type"),  # type: ignore[arg-type]
        )
        decision = self.evaluate(result)
        return {
            "retry": decision.should_retry,
            "delay_ms": decision.delay_ms,
            "reason": decision.reason,
            "attempts_remaining": 1 if decision.should_retry else 0,
        }


__all__ = ["RetryDecision", "IntelligentRetryStrategy"]
