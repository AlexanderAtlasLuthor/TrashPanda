"""V2.10.12 — pilot send config persistence tests."""

from __future__ import annotations

import json
from pathlib import Path

from app.db.pilot_send_tracker import PILOT_CONFIG_FILENAME
from app.pilot_send.config import (
    IMAPCredentials,
    PilotMessageTemplate,
    PilotSendConfig,
    RelayConfig,
    read_pilot_config,
    write_pilot_config,
)


class TestPilotMessageTemplate:
    def test_empty_template_is_incomplete(self):
        assert PilotMessageTemplate().is_complete() is False

    def test_template_needs_subject_body_and_sender(self):
        t = PilotMessageTemplate(
            subject="Hello",
            body_text="Hi",
            sender_address="sender@acme.com",
        )
        assert t.is_complete() is True

    def test_missing_sender_makes_incomplete(self):
        t = PilotMessageTemplate(subject="x", body_text="y")
        assert t.is_complete() is False


class TestIMAPCredentials:
    def test_default_not_configured(self):
        assert IMAPCredentials().is_configured() is False

    def test_configured_when_host_and_user_set(self):
        creds = IMAPCredentials(host="imap.acme.com", username="bounces")
        assert creds.is_configured() is True


class TestRelayConfig:
    """V2.10.13 — relay metadata persistence + activation predicate."""

    def test_default_relay_not_configured(self):
        assert RelayConfig().is_configured() is False

    def test_host_set_marks_configured(self):
        assert RelayConfig(host="relay.example.com").is_configured() is True

    def test_blank_host_not_configured(self):
        assert RelayConfig(host="   ").is_configured() is False

    def test_relay_round_trip(self, tmp_path: Path):
        cfg = PilotSendConfig(
            relay=RelayConfig(
                host="relay.example.com",
                port=2525,
                username="bob",
                password_env="MY_RELAY_PWD",
                use_starttls=False,
            ),
        )
        write_pilot_config(tmp_path, cfg)
        loaded = read_pilot_config(tmp_path)
        assert loaded.relay.host == "relay.example.com"
        assert loaded.relay.port == 2525
        assert loaded.relay.username == "bob"
        assert loaded.relay.password_env == "MY_RELAY_PWD"
        assert loaded.relay.use_starttls is False

    def test_relay_password_never_persisted(self, tmp_path: Path):
        """Defense in depth: only ``password_env`` (var name) is on
        disk; no ``password`` key leaks into the JSON."""
        cfg = PilotSendConfig(
            relay=RelayConfig(
                host="relay.example.com",
                username="bob",
                password_env="MY_RELAY_PWD",
            ),
        )
        path = write_pilot_config(tmp_path, cfg)
        parsed = json.loads(path.read_text(encoding="utf-8"))
        assert "password" not in parsed.get("relay", {})
        assert parsed["relay"]["password_env"] == "MY_RELAY_PWD"


class TestPilotSendConfigPredicates:
    def test_default_not_ready(self):
        assert PilotSendConfig().is_ready_to_send() is False

    def test_complete_config_with_auth_is_ready(self):
        cfg = PilotSendConfig(
            template=PilotMessageTemplate(
                subject="x", body_text="y",
                sender_address="sender@acme.com",
            ),
            return_path_domain="bounces.acme.com",
            authorization_confirmed=True,
        )
        assert cfg.is_ready_to_send() is True

    def test_unauthorized_blocks_ready(self):
        cfg = PilotSendConfig(
            template=PilotMessageTemplate(
                subject="x", body_text="y",
                sender_address="sender@acme.com",
            ),
            return_path_domain="bounces.acme.com",
            authorization_confirmed=False,
        )
        assert cfg.is_ready_to_send() is False


class TestRoundTrip:
    def test_write_then_read(self, tmp_path: Path):
        cfg = PilotSendConfig(
            template=PilotMessageTemplate(
                subject="Hello",
                body_text="Body",
                body_html="<p>Body</p>",
                sender_address="sender@acme.com",
                sender_name="Acme",
                reply_to="reply@acme.com",
            ),
            imap=IMAPCredentials(
                host="imap.acme.com",
                port=993,
                use_ssl=True,
                username="bounces",
                folder="INBOX",
            ),
            return_path_domain="bounces.acme.com",
            wait_window_hours=72,
            expiry_hours=200,
            max_batch_size=50,
            authorization_confirmed=True,
            authorization_note="approved by user",
        )
        write_pilot_config(tmp_path, cfg)
        loaded = read_pilot_config(tmp_path)
        assert loaded.template.subject == "Hello"
        assert loaded.template.body_html == "<p>Body</p>"
        assert loaded.imap.host == "imap.acme.com"
        assert loaded.return_path_domain == "bounces.acme.com"
        assert loaded.wait_window_hours == 72
        assert loaded.expiry_hours == 200
        assert loaded.max_batch_size == 50
        assert loaded.authorization_confirmed is True
        assert loaded.authorization_note == "approved by user"

    def test_missing_file_returns_defaults(self, tmp_path: Path):
        loaded = read_pilot_config(tmp_path)
        assert loaded.is_ready_to_send() is False
        assert loaded.template.subject == ""

    def test_corrupt_json_returns_defaults(self, tmp_path: Path):
        path = tmp_path / PILOT_CONFIG_FILENAME
        path.write_text("not json")
        loaded = read_pilot_config(tmp_path)
        assert loaded.is_ready_to_send() is False

    def test_password_never_persisted_to_disk(self, tmp_path: Path):
        """The dataclass exposes ``password_env`` (the env var name),
        never a literal password — but defense in depth: ensure no
        common password keyword leaks into the persisted JSON."""
        cfg = PilotSendConfig(
            imap=IMAPCredentials(
                host="imap.acme.com",
                username="bounces",
                password_env="MY_SECRET_VAR",
            ),
        )
        path = write_pilot_config(tmp_path, cfg)
        text = path.read_text(encoding="utf-8")
        assert "password_env" in text
        assert "MY_SECRET_VAR" in text
        # No literal "password" key in the persisted dict.
        parsed = json.loads(text)
        assert "password" not in parsed.get("imap", {})
