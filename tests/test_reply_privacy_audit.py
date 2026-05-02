"""Tests for reply draft privacy leak auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_privacy_audit import (
    audit_reply_privacy,
    audit_reply_privacy_text,
    format_reply_privacy_audit_json,
    format_reply_privacy_audit_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_reply_privacy.py"
spec = importlib.util.spec_from_file_location("audit_reply_privacy_script", SCRIPT_PATH)
audit_reply_privacy_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_reply_privacy_script)

NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, tweet_id: str, draft_text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=draft_text,
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_text_audit_flags_private_values_with_masked_evidence():
    findings = audit_reply_privacy_text(
        "Email jane.doe@example.com or call (415) 555-0199. "
        "The repro lives in /Users/taka/Project/private/report.md. "
        "Use api_key=abcdef1234567890 and session_id=sess_123456789.",
        reply_id=42,
    )

    by_detector = {finding.detector: finding for finding in findings}

    assert set(by_detector) == {
        "email",
        "phone",
        "local_path",
        "secret_token",
        "private_identifier",
    }
    assert by_detector["email"].evidence == "j...e@example.com"
    assert by_detector["phone"].evidence == "***-***-0199"
    assert by_detector["local_path"].evidence.endswith("/.../report.md")
    assert by_detector["secret_token"].evidence == "api_key=abcd...7890"
    assert by_detector["private_identifier"].evidence == "session_id=ses...789"
    assert "jane.doe" not in {finding.evidence for finding in findings}
    assert all(finding.reply_id == 42 for finding in findings)


def test_default_rules_do_not_overflag_urls_or_public_handles():
    findings = audit_reply_privacy_text(
        "Thanks @casey. This public URL is useful: "
        "https://example.com/posts/2026/04/reply-audit?ref=thread and @presence-dev can help."
    )

    assert findings == ()


def test_report_summarizes_counts_by_detector_and_severity(db):
    flagged = _insert_reply(
        db,
        "flagged",
        "Please do not include sk-proj-abcdefghijklmnopqrstuvwxyz012345 or "
        "c:/Users/Alice/secrets.env in the public reply.",
        inbound_author_handle="zoe",
    )
    benign = _insert_reply(
        db,
        "benign",
        "Thanks @zoe, the public docs at https://example.com/docs look right.",
    )
    _set_detected_at(db, flagged, "2026-04-23 10:00:00")
    _set_detected_at(db, benign, "2026-04-23 09:00:00")

    report = audit_reply_privacy(db, now=NOW)
    payload = json.loads(format_reply_privacy_audit_json(report))
    text = format_reply_privacy_audit_text(report)

    assert report.ok is False
    assert report.audited_count == 2
    assert report.finding_count == 2
    assert report.by_detector == {"secret_token": 1, "local_path": 1}
    assert report.by_severity == {"critical": 1, "medium": 1}
    assert payload["artifact_type"] == "reply_privacy_audit"
    assert list(payload) == sorted(payload)
    assert payload["blocking_issue_count"] == 2
    assert payload["items"][0]["reply_id"] == "flagged"
    assert "sk-proj-abcdefghijklmnopqrstuvwxyz012345" not in text
    assert "Reply Privacy Audit" in text
    assert "secret_token" in text


def test_status_days_and_limit_filters_are_applied(db):
    old = _insert_reply(db, "old", "Contact old@example.com", status="pending")
    approved = _insert_reply(db, "approved", "Contact approved@example.com", status="approved")
    pending = _insert_reply(db, "pending", "Contact pending@example.com", status="pending")
    _set_detected_at(db, old, "2026-04-01 10:00:00")
    _set_detected_at(db, approved, "2026-04-23 11:00:00")
    _set_detected_at(db, pending, "2026-04-23 10:00:00")

    pending_report = audit_reply_privacy(db, days=7, status="pending", now=NOW)
    approved_report = audit_reply_privacy(db, days=7, status="approved", now=NOW)
    all_report = audit_reply_privacy(db, days=30, status="all", limit=2, now=NOW)

    assert [item.reply_id for item in pending_report.items] == ["pending"]
    assert [item.reply_id for item in approved_report.items] == ["approved"]
    assert [item.reply_id for item in all_report.items] == ["approved", "pending"]


def test_cli_supports_json_status_days_and_limit(db, monkeypatch, capsys):
    reply_id = _insert_reply(
        db,
        "cli-approved",
        "Please remove customer_id=cus_123456789 before posting.",
        status="approved",
    )
    _set_detected_at(db, reply_id, "2026-05-02 10:00:00")
    monkeypatch.setattr(
        audit_reply_privacy_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = audit_reply_privacy_script.main(
        ["--json", "--status", "approved", "--days", "3", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"] == {"days": 3, "limit": 5, "status": "approved"}
    assert payload["finding_count"] == 1
    assert payload["items"][0]["findings"][0]["detector"] == "private_identifier"


def test_cli_exits_zero_when_no_findings(db, monkeypatch, capsys):
    _insert_reply(db, "safe", "Thanks @alice, https://example.com is a good public link.")
    monkeypatch.setattr(
        audit_reply_privacy_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert audit_reply_privacy_script.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["finding_count"] == 0
