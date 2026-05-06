"""V2.10.12 — IMAP bounce poller tests.

The IMAP transport is injectable via ``imap_factory`` so tests
never open a real socket. The fake client below mimics
``imaplib.IMAP4_SSL`` for the methods the poller calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.db.pilot_send_tracker import (
    open_for_run as open_tracker,
    VERDICT_HARD_BOUNCE,
)
from app.pilot_send.bounce_poller import poll_bounces
from app.pilot_send.config import (
    IMAPCredentials,
    PilotMessageTemplate,
    PilotSendConfig,
    write_pilot_config,
)


# --------------------------------------------------------------------- #
# Fake IMAP client
# --------------------------------------------------------------------- #


def _multipart_dsn(token: str, *, action: str = "failed",
                   status: str = "5.1.1") -> bytes:
    return (
        "From: postmaster@destination.example.com\r\n"
        f"To: postmaster@bounces.acme.com\r\n"
        "Subject: Mail delivery failed\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/report; report-type=delivery-status; "
        'boundary="bdy"\r\n'
        "\r\n"
        "--bdy\r\n"
        "Content-Type: text/plain\r\n\r\n"
        "Bounced.\r\n"
        "--bdy\r\n"
        "Content-Type: message/delivery-status\r\n\r\n"
        f"Action: {action}\r\n"
        f"Status: {status}\r\n"
        "Diagnostic-Code: smtp; 550 5.1.1 No such user\r\n"
        f"Original-Recipient: rfc822; bounce+{token}@bounces.acme.com\r\n"
        "Final-Recipient: rfc822; recipient@example.com\r\n"
        "\r\n"
        "--bdy--\r\n"
    ).encode("utf-8")


class _FakeIMAP:
    def __init__(self, *, messages: dict[bytes, bytes]):
        self.messages = messages
        self.login_called = False
        self.select_called = False
        self.seen: set[bytes] = set()
        self.logout_called = False

    def login(self, user, password):
        self.login_called = True
        return ("OK", [b""])

    def select(self, mailbox):
        self.select_called = True
        return ("OK", [str(len(self.messages)).encode()])

    def search(self, charset, *criteria):
        ids = b" ".join(self.messages.keys())
        return ("OK", [ids])

    def fetch(self, message_set, message_parts):
        if message_set not in self.messages:
            return ("NO", [b""])
        # imaplib's fetch returns ``[(envelope, body)]`` tuples.
        return (
            "OK",
            [(b"meta", self.messages[message_set]), b")"],
        )

    def store(self, message_set, command, flags):
        if "\\Seen" in flags:
            self.seen.add(message_set)
        return ("OK", [b""])

    def close(self):
        return ("OK", [b""])

    def logout(self):
        self.logout_called = True
        return ("BYE", [b""])


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #


@pytest.fixture
def run_dir_with_tracker(tmp_path: Path):
    """Run dir with a tracker DB that has 1 sent row pending DSN."""
    cfg = PilotSendConfig(
        template=PilotMessageTemplate(
            subject="x", body_text="y", sender_address="s@acme.com",
        ),
        imap=IMAPCredentials(
            host="imap.acme.com",
            username="bounces",
            password_env="MOCK_PW",
        ),
        return_path_domain="bounces.acme.com",
        authorization_confirmed=True,
    )
    write_pilot_config(tmp_path, cfg)

    with open_tracker(tmp_path) as tracker:
        tracker.add_candidate(
            job_id="j1",
            batch_id="b1",
            source_row=1,
            email="alice@example.com",
            domain="example.com",
            provider_family="corporate_unknown",
            verp_token="aaa",
        )
        row = tracker.snapshot()[0]
        tracker.mark_sent(row.id, message_id="<msg@x>")
    return tmp_path


# --------------------------------------------------------------------- #
# Happy path + edge cases
# --------------------------------------------------------------------- #


class TestPollBounces:
    def test_no_tracker_returns_zero(self, tmp_path: Path):
        result = poll_bounces(tmp_path)
        assert result.fetched == 0
        assert result.parsed == 0

    def test_password_missing_returns_zero(
        self, run_dir_with_tracker: Path
    ):
        # No env var lookup → returns "" → poller skips.
        result = poll_bounces(
            run_dir_with_tracker,
            password_lookup=lambda creds: "",
        )
        assert result.fetched == 0

    def test_imap_not_configured_returns_zero(self, tmp_path: Path):
        cfg = PilotSendConfig()  # default has no host/username
        write_pilot_config(tmp_path, cfg)
        # Tracker exists.
        with open_tracker(tmp_path) as tracker:
            tracker.add_candidate(
                job_id="j", batch_id="b", source_row=1,
                email="a@x.com", domain="x.com",
                provider_family="corporate_unknown",
                verp_token="t",
            )
        result = poll_bounces(tmp_path, password_lookup=lambda c: "pw")
        assert result.fetched == 0

    def test_happy_path_matches_token(
        self, run_dir_with_tracker: Path
    ):
        fake = _FakeIMAP(
            messages={b"1": _multipart_dsn("aaa")},
        )
        result = poll_bounces(
            run_dir_with_tracker,
            imap_factory=lambda creds: fake,
            password_lookup=lambda creds: "pw",
        )
        assert result.fetched == 1
        assert result.parsed == 1
        assert result.matched == 1
        # Tracker now shows verdict_ready.
        with open_tracker(run_dir_with_tracker) as tracker:
            row = tracker.by_token("aaa")
        assert row is not None
        assert row.dsn_status == VERDICT_HARD_BOUNCE
        assert fake.logout_called is True

    def test_unknown_token_increments_unmatched(
        self, run_dir_with_tracker: Path
    ):
        fake = _FakeIMAP(
            messages={b"1": _multipart_dsn("zzz")},
        )
        result = poll_bounces(
            run_dir_with_tracker,
            imap_factory=lambda creds: fake,
            password_lookup=lambda creds: "pw",
        )
        assert result.matched == 0
        assert result.unmatched_tokens == 1

    def test_mark_seen_default(
        self, run_dir_with_tracker: Path
    ):
        fake = _FakeIMAP(
            messages={b"1": _multipart_dsn("aaa")},
        )
        poll_bounces(
            run_dir_with_tracker,
            imap_factory=lambda creds: fake,
            password_lookup=lambda creds: "pw",
        )
        assert b"1" in fake.seen

    def test_mark_seen_false_skips_flag(
        self, run_dir_with_tracker: Path
    ):
        fake = _FakeIMAP(
            messages={b"1": _multipart_dsn("aaa")},
        )
        poll_bounces(
            run_dir_with_tracker,
            imap_factory=lambda creds: fake,
            password_lookup=lambda creds: "pw",
            mark_seen=False,
        )
        assert fake.seen == set()

    def test_connect_failure_returns_zero(
        self, run_dir_with_tracker: Path
    ):
        def boom(creds):
            raise ConnectionError("imap unreachable")

        result = poll_bounces(
            run_dir_with_tracker,
            imap_factory=boom,
            password_lookup=lambda creds: "pw",
        )
        assert result.fetched == 0
