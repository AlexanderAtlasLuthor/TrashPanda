"""Strictly controlled SMTP sampler client.

This client performs at most one SMTP connection attempt and stops
after the RCPT TO response. It never sends message DATA, never retries,
and has no parallel execution behavior.
"""

from __future__ import annotations

import smtplib
import socket
import time
from typing import Callable

from ..interfaces import SMTPProbeClient
from ..request import ValidationRequest
from .smtp_result import SMTPProbeResult


ERROR_TIMEOUT = "timeout"
ERROR_CONNECTION = "connection_error"
ERROR_PROTOCOL = "protocol_error"


class SafeSMTPProbeClient(SMTPProbeClient):
    """Low-risk SMTP probe client.

    MX resolution is intentionally outside this class. Tests and callers
    may inject ``mx_resolver`` or provide ``request.metadata["mx_host"]``.
    If neither is present, the request domain is used as the host.
    """

    def __init__(
        self,
        *,
        timeout_seconds: float = 2.5,
        smtp_factory: Callable[..., smtplib.SMTP] | None = None,
        mx_resolver: Callable[[ValidationRequest], str | None] | None = None,
        port: int = 25,
        mail_from: str = "probe@invalid.local",
        helo_host: str = "localhost",
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.timeout_seconds = float(timeout_seconds)
        self._smtp_factory = smtp_factory or smtplib.SMTP
        self._mx_resolver = mx_resolver
        self._port = int(port)
        self._mail_from = mail_from
        self._helo_host = helo_host

    def probe(self, request: ValidationRequest) -> SMTPProbeResult:
        start = time.monotonic()
        server = None
        try:
            host = self._resolve_host(request)
            server = self._smtp_factory(
                host=host,
                port=self._port,
                timeout=self.timeout_seconds,
            )
            server.helo(self._helo_host)
            server.mail(self._mail_from)
            code, message = server.rcpt(request.email)
            return SMTPProbeResult(
                success=True,
                code=_coerce_code(code),
                message=_coerce_message(message),
                latency_ms=_elapsed_ms(start),
                error_type=None,
            )
        except (socket.timeout, TimeoutError) as exc:
            return _error_result(start, ERROR_TIMEOUT, exc)
        except (OSError, smtplib.SMTPConnectError) as exc:
            return _error_result(start, ERROR_CONNECTION, exc)
        except smtplib.SMTPException as exc:
            return _error_result(start, ERROR_PROTOCOL, exc)
        finally:
            if server is not None:
                try:
                    server.quit()
                except Exception:
                    pass

    def _resolve_host(self, request: ValidationRequest) -> str:
        if self._mx_resolver is not None:
            host = self._mx_resolver(request)
            if host:
                return host

        metadata = request.metadata
        for key in ("mx_host", "smtp_host"):
            value = metadata.get(key)
            if isinstance(value, str) and value:
                return value

        mx_hosts = metadata.get("mx_hosts")
        if isinstance(mx_hosts, (tuple, list)) and mx_hosts:
            first = mx_hosts[0]
            if isinstance(first, str) and first:
                return first

        return request.domain


def _elapsed_ms(start: float) -> float:
    return (time.monotonic() - start) * 1000.0


def _coerce_code(code: object) -> int | None:
    try:
        return int(code)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _coerce_message(message: object) -> str | None:
    if message is None:
        return None
    if isinstance(message, bytes):
        return message.decode("utf-8", errors="replace")
    return str(message)


def _error_result(
    start: float,
    error_type: str,
    exc: BaseException,
) -> SMTPProbeResult:
    return SMTPProbeResult(
        success=False,
        code=None,
        message=str(exc) or None,
        latency_ms=_elapsed_ms(start),
        error_type=error_type,
    )


__all__ = [
    "SafeSMTPProbeClient",
    "ERROR_TIMEOUT",
    "ERROR_CONNECTION",
    "ERROR_PROTOCOL",
]
