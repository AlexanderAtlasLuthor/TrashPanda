"""V2.10.12 — Pilot send launch orchestrator.

Top-level entry point that ties the selector, tracker, and sender
together. Called by the operator endpoint ``POST
/jobs/{id}/pilot-send/launch`` after the operator has supplied a
:class:`PilotSendConfig` with ``authorization_confirmed=True``.

Steps (atomic per row):

1. Validate the config (template complete, return-path-domain set,
   authorization_confirmed, batch size within ``max_batch_size``).
2. Generate a fresh ``batch_id`` (UUID) and select up to ``batch_size``
   candidates via :func:`app.pilot_send.selector.select_candidates`.
3. For each candidate: write the tracker row in ``pending_send``
   with a freshly-generated VERP token, then call the sender. On
   success, transition the tracker row to ``sent`` with the
   message-id.
4. On any per-row failure (refused recipient, MX unreachable, smtp
   exception): record the outcome in the tracker as
   ``verdict_ready`` with the appropriate verdict (hard_bounce /
   blocked / soft_bounce / unknown) so the operator sees the row
   without waiting for IMAP.

The orchestrator is **operator-triggered**, **synchronous**, and
**idempotent on retry** (the tracker's ``UNIQUE (job_id, batch_id,
email)`` prevents double-send if the operator hits launch twice).

Tests inject a fake transport via ``smtp_factory`` so no real
network call happens.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from ..db.pilot_send_tracker import (
    PILOT_TRACKER_FILENAME,
    open_for_run,
    PilotCounts,
    VERDICT_HARD_BOUNCE,
    VERDICT_BLOCKED,
    VERDICT_INFRA_BLOCKED,
    VERDICT_PROVIDER_DEFERRED,
    VERDICT_SOFT_BOUNCE,
    VERDICT_UNKNOWN,
)
from .bounce_parser import _is_infra_block, _is_provider_deferred
from .config import PilotSendConfig, read_pilot_config
from .selector import PilotCandidate, select_candidates
from .sender import PilotSendOutcome, SMTPSender, SMTPTransport
from .verp import (
    DEFAULT_VERP_LOCAL_PART,
    encode_verp_token,
    new_verp_token,
)


_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class LaunchResult:
    """Outcome of one ``launch_pilot`` call."""

    run_dir: Path
    batch_id: str
    candidates_selected: int
    candidates_added: int
    sent: int
    failed: int
    counts: PilotCounts
    error: str | None = None
    failures: list[PilotSendOutcome] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


def _validate(config: PilotSendConfig, *, batch_size: int) -> str | None:
    """Return a short reason code on failure, ``None`` when ready."""
    if not config.authorization_confirmed:
        return "authorization_required"
    if not config.template.is_complete():
        return "template_incomplete"
    if not config.return_path_domain.strip():
        return "return_path_domain_missing"
    if batch_size <= 0:
        return "batch_size_must_be_positive"
    if batch_size > config.max_batch_size:
        return "batch_size_exceeds_max"
    return None


# --------------------------------------------------------------------------- #
# Per-outcome verdict mapping
# --------------------------------------------------------------------------- #


def _check_sender_reputation() -> None:
    """Opt-in pre-pilot sender reputation check.

    When ``TRASHPANDA_SENDING_IP`` is set, look up the latest
    reputation snapshots for that IP and emit a warning log if the
    aggregate signal is red. Never blocks the pilot — the operator
    sees the warning and decides. Unset env var = no-op (preserves
    test behavior and back-compat for existing deployments).
    """
    import os

    ip = (os.environ.get("TRASHPANDA_SENDING_IP") or "").strip()
    if not ip:
        return
    try:
        from ..sender_reputation import is_safe_to_pilot, open_store

        with open_store() as conn:
            decision = is_safe_to_pilot(conn, ip)
    except Exception as exc:  # pragma: no cover - defensive
        logging.getLogger(__name__).debug(
            "sender_reputation: skipped (%s)", exc,
        )
        return

    log = logging.getLogger(__name__)
    if decision.overall_status == "red":
        log.warning(
            "pre-pilot reputation check: ip=%s overall=RED — "
            "consider delaying. reasons: %s",
            ip, "; ".join(decision.reasons),
        )
    elif decision.overall_status == "yellow":
        log.warning(
            "pre-pilot reputation check: ip=%s overall=yellow — "
            "monitor closely. reasons: %s",
            ip, "; ".join(decision.reasons),
        )
    else:
        log.info(
            "pre-pilot reputation check: ip=%s overall=%s",
            ip, decision.overall_status,
        )


def _send_failure_verdict(outcome: PilotSendOutcome) -> str:
    """Translate a sender failure into a tracker verdict.

    A 5xx ``rcpt_refused`` is a hard bounce (the destination MX
    explicitly rejected the address). A 4xx counts as
    ``soft_bounce``. Everything else (network error, all-mx-failed)
    is ``unknown`` — the operator can re-launch later.
    """
    code = outcome.smtp_response_code
    raw = outcome.smtp_response_message or ""
    # Belt-and-suspenders: also scan ``outcome.error`` (the formatted
    # exception repr from sender.py) so the classifier survives any
    # upstream format change that puts the body text only in ``error``
    # and not in ``smtp_response_message``. Both fields carry the
    # same body for SMTPSenderRefused; we OR the two checks so
    # whichever one has the diagnostic text wins.
    error_str = outcome.error or ""
    if _is_infra_block(raw) or _is_infra_block(error_str):
        return VERDICT_INFRA_BLOCKED
    if _is_provider_deferred(raw) or _is_provider_deferred(error_str):
        return VERDICT_PROVIDER_DEFERRED
    if isinstance(code, int):
        if 500 <= code < 600:
            msg = (raw + " " + error_str).lower()
            if any(
                k in msg
                for k in (
                    "blocked", "policy", "spam", "blacklist",
                    "denied", "spamhaus", "barracuda",
                )
            ):
                return VERDICT_BLOCKED
            return VERDICT_HARD_BOUNCE
        if 400 <= code < 500:
            return VERDICT_SOFT_BOUNCE
    return VERDICT_UNKNOWN


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #


def launch_pilot(
    run_dir: str | Path,
    *,
    job_id: str,
    batch_size: int,
    config: PilotSendConfig | None = None,
    smtp_factory: Callable[..., SMTPTransport] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    now: datetime | None = None,
) -> LaunchResult:
    """Select + send a pilot batch. Returns a :class:`LaunchResult`.

    Parameters
    ----------
    run_dir:
        Per-run output directory (carries the action XLSX files
        from V2.10.11 and the tracker / config).
    job_id:
        Operator-assigned job identifier; persisted on every tracker
        row so finalize / poll can scope by job.
    batch_size:
        Cap on the number of recipients sent. Hard-clamped to
        ``config.max_batch_size``.
    config / smtp_factory / sleep_fn:
        Tests inject. Defaults read ``pilot_send_config.json`` and
        use ``smtplib.SMTP`` directly.

    Never raises. Validation failures land in
    ``LaunchResult.error`` so the operator endpoint can render a
    400 with a useful reason.
    """
    run_dir_path = Path(run_dir)
    cfg = config or read_pilot_config(run_dir_path)

    error = _validate(cfg, batch_size=batch_size)
    if error is not None:
        return LaunchResult(
            run_dir=run_dir_path,
            batch_id="",
            candidates_selected=0,
            candidates_added=0,
            sent=0,
            failed=0,
            counts=PilotCounts(),
            error=error,
        )

    # Optional pre-pilot reputation check. Strictly opt-in via the
    # TRASHPANDA_SENDING_IP env var; unset = no check (preserves test
    # behavior). Result is logged but never blocks — the operator
    # decides whether to proceed when reputation is red.
    _check_sender_reputation()

    batch_id = uuid.uuid4().hex[:12]
    candidates = select_candidates(run_dir_path, batch_size=batch_size)
    if not candidates:
        return LaunchResult(
            run_dir=run_dir_path,
            batch_id=batch_id,
            candidates_selected=0,
            candidates_added=0,
            sent=0,
            failed=0,
            counts=PilotCounts(),
            error="no_candidates_found",
        )

    sender_kwargs: dict = {}
    if smtp_factory is not None:
        sender_kwargs["smtp_factory"] = smtp_factory
    elif cfg.relay.is_configured():
        # V2.10.13 — operator opted into relay mode. The factory
        # handles AUTH; SMTPSender skips MX resolution.
        sender_kwargs["relay_config"] = cfg.relay
    if sleep_fn is not None:
        sender_kwargs["sleep_fn"] = sleep_fn
    sender = SMTPSender(**sender_kwargs)

    candidates_added = 0
    sent = 0
    failed = 0
    failures: list[PilotSendOutcome] = []

    with closing(open_for_run(run_dir_path)) as tracker:
        for candidate in candidates:
            token = new_verp_token()
            ok = tracker.add_candidate(
                job_id=job_id,
                batch_id=batch_id,
                source_row=candidate.source_row,
                email=candidate.email,
                domain=candidate.domain,
                provider_family=candidate.provider_family,
                verp_token=token,
                now=now,
            )
            if not ok:
                # Already enqueued in a previous batch — skip silently.
                continue
            candidates_added += 1

            try:
                return_path = encode_verp_token(
                    token,
                    return_path_domain=cfg.return_path_domain,
                    local_part=DEFAULT_VERP_LOCAL_PART,
                )
            except ValueError as exc:
                # Misconfigured envelope — record as unknown so the
                # row stays auditable and move on.
                tracker.record_dsn(
                    token,
                    dsn_status=VERDICT_UNKNOWN,
                    dsn_diagnostic=f"envelope_error:{exc}"[:300],
                    now=now,
                )
                failed += 1
                continue

            outcome = sender.send_one(
                recipient=candidate.email,
                verp_token=token,
                return_path=return_path,
                sender_name=cfg.template.sender_name,
                sender_address=cfg.template.sender_address,
                subject=cfg.template.subject,
                body_text=cfg.template.body_text,
                body_html=cfg.template.body_html,
                reply_to=cfg.template.reply_to or None,
            )

            if outcome.sent:
                tracker.mark_sent(
                    _row_id_for_token(tracker, token),
                    message_id=outcome.message_id,
                    now=now,
                )
                sent += 1
            else:
                # Per-row send failure → record as verdict_ready
                # immediately so the operator sees the row without
                # waiting for IMAP.
                verdict = _send_failure_verdict(outcome)
                tracker.record_dsn(
                    token,
                    dsn_status=verdict,
                    dsn_diagnostic=(
                        outcome.error
                        or outcome.smtp_response_message
                        or "send_failed"
                    )[:300],
                    dsn_smtp_code=(
                        str(outcome.smtp_response_code)
                        if outcome.smtp_response_code is not None
                        else None
                    ),
                    now=now,
                )
                # ``record_dsn`` only updates rows in ``sent``; for
                # failed rows that never reached ``sent``, force the
                # transition by writing through the tracker's update
                # statement directly.
                _force_verdict_for_token(
                    tracker,
                    token,
                    verdict=verdict,
                    diagnostic=(
                        outcome.error
                        or outcome.smtp_response_message
                        or "send_failed"
                    ),
                    smtp_code=(
                        str(outcome.smtp_response_code)
                        if outcome.smtp_response_code is not None
                        else None
                    ),
                    now=now,
                )
                failed += 1
                failures.append(outcome)

        counts = tracker.counts(batch_id=batch_id)

    return LaunchResult(
        run_dir=run_dir_path,
        batch_id=batch_id,
        candidates_selected=len(candidates),
        candidates_added=candidates_added,
        sent=sent,
        failed=failed,
        counts=counts,
        failures=failures,
    )


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #


def _row_id_for_token(tracker, verp_token: str) -> int:
    row = tracker.by_token(verp_token)
    if row is None:  # pragma: no cover - shouldn't happen
        raise RuntimeError(f"tracker row vanished for token {verp_token}")
    return row.id


def _force_verdict_for_token(
    tracker,
    verp_token: str,
    *,
    verdict: str,
    diagnostic: str | None,
    smtp_code: str | None,
    now: datetime | None,
) -> None:
    """Set ``state=verdict_ready`` + populate dsn fields for a token
    that may not be in ``state=sent`` yet (per-row send failure).

    Direct UPDATE so the state machine still moves forward on
    failures that happen before ``mark_sent``.
    """
    ts = now or datetime.now(tz=timezone.utc)
    iso = ts.isoformat()
    tracker._conn.execute(  # type: ignore[attr-defined]
        """
        UPDATE pilot_send_tracker
        SET state = 'verdict_ready',
            dsn_status = ?,
            dsn_diagnostic = ?,
            dsn_smtp_code = ?,
            dsn_received_at = ?,
            updated_at = ?
        WHERE verp_token = ? AND state = 'pending_send'
        """,
        (
            verdict,
            (diagnostic or "")[:500],
            smtp_code,
            iso,
            iso,
            verp_token,
        ),
    )
    tracker._conn.commit()  # type: ignore[attr-defined]


__all__ = [
    "LaunchResult",
    "launch_pilot",
]
