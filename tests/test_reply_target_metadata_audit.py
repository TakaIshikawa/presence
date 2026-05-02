"""Tests for reply target metadata auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_target_metadata_audit import (
    build_reply_target_metadata_audit,
    format_reply_target_metadata_audit_json,
    format_reply_target_metadata_audit_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_reply_target_metadata.py"
spec = importlib.util.spec_from_file_location("audit_reply_target_metadata_script", SCRIPT_PATH)
audit_reply_target_metadata_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_reply_target_metadata_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="author-a",
        inbound_text="Can you clarify?",
        our_tweet_id="our-1",
        our_content_id=123,
        our_post_text="Original post",
        draft_text="Thanks for asking.",
        platform="x",
        inbound_url=f"https://x.com/alice/status/{inbound_id}",
        our_platform_id="our-1",
        platform_metadata=json.dumps(
            {"inbound_tweet_id": inbound_id, "our_platform_id": "our-1"},
            sort_keys=True,
        ),
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", (detected_at, reply_id))
    db.conn.commit()


def test_buckets_pending_rows_by_concrete_target_metadata_problems(db):
    reply_id = _insert_reply(
        db,
        "missing-context",
        our_content_id=None,
        our_post_text="",
        inbound_url=None,
        our_tweet_id="",
        our_platform_id=None,
        platform_metadata=None,
    )

    report = build_reply_target_metadata_audit(db, now=NOW)
    payload = json.loads(format_reply_target_metadata_audit_json(report))

    assert report.ok is False
    assert payload["finding_count"] == 4
    assert payload["by_bucket"] == {
        "missing_inbound_link": 1,
        "missing_original_post": 1,
        "missing_platform_ids": 1,
        "missing_platform_metadata": 1,
    }
    assert {finding["bucket"] for finding in payload["findings"]} == set(payload["by_bucket"])
    assert all(finding["reply_queue_id"] == reply_id for finding in payload["findings"])
    assert all(finding["inbound_author_handle"] == "alice" for finding in payload["findings"])
    assert all("our_content_id" in finding for finding in payload["findings"])


def test_severity_ordering_and_deterministic_formatting(db):
    _insert_reply(
        db,
        "mismatch",
        platform_metadata=json.dumps({"our_platform_id": "different"}, sort_keys=True),
    )
    parse_error = _insert_reply(db, "bad-json", platform_metadata="{not-json")
    _set_detected_at(db, parse_error, "2026-05-02 11:00:00")

    report = build_reply_target_metadata_audit(db, now=NOW)
    payload = json.loads(format_reply_target_metadata_audit_json(report))
    text = format_reply_target_metadata_audit_text(report)

    assert [finding["bucket"] for finding in payload["findings"]] == [
        "metadata_parse_error",
        "platform_id_mismatch",
    ]
    assert [finding["severity"] for finding in payload["findings"]] == ["critical", "high"]
    assert list(payload) == sorted(payload)
    assert "Reply Target Metadata Audit" in text
    assert "metadata_parse_error" in text
    assert "content_id=123" in text


def test_platform_filtering_days_and_limit_are_applied(db):
    old_x = _insert_reply(db, "old-x", platform_metadata=None)
    new_x = _insert_reply(db, "new-x", platform_metadata=None)
    bluesky = _insert_reply(
        db,
        "at://did:plc:alice/app.bsky.feed.post/reply",
        platform="bluesky",
        inbound_cid="cid-1",
        inbound_url="https://bsky.app/profile/alice/post/reply",
        platform_metadata=None,
    )
    _set_detected_at(db, old_x, "2026-04-01 10:00:00")
    _set_detected_at(db, new_x, "2026-05-02 10:00:00")
    _set_detected_at(db, bluesky, "2026-05-02 09:00:00")

    x_report = build_reply_target_metadata_audit(db, days=7, platform="x", now=NOW)
    all_limited = build_reply_target_metadata_audit(db, days=40, limit=2, now=NOW)

    assert [item.id for item in x_report.items] == [new_x]
    assert [item.id for item in all_limited.items] == [new_x, bluesky]


def test_missing_reply_queue_and_optional_columns_return_empty_reports():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing_table = build_reply_target_metadata_audit(conn, now=NOW)

    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, draft_text TEXT)")
    conn.execute("INSERT INTO reply_queue (id, draft_text) VALUES (1, 'Draft')")
    conn.commit()
    missing_columns = build_reply_target_metadata_audit(conn, now=NOW)

    assert missing_table.ok is True
    assert missing_table.audited_count == 0
    assert missing_table.missing_tables == ("reply_queue",)
    assert missing_columns.ok is True
    assert missing_columns.audited_count == 0
    assert missing_columns.findings == ()
    assert "inbound_url" in missing_columns.missing_columns["reply_queue"]


def test_malformed_platform_metadata_json_is_reported(db):
    reply_id = _insert_reply(db, "bad-json-only", platform_metadata="[1, 2, 3]")

    report = build_reply_target_metadata_audit(db, now=NOW)

    assert report.findings[0].reply_queue_id == reply_id
    assert report.findings[0].bucket == "metadata_parse_error"
    assert report.findings[0].reason == "platform_metadata must be a JSON object"


def test_cli_supports_text_json_platform_limit_and_validation(db, monkeypatch, capsys):
    _insert_reply(db, "cli-x", platform_metadata=None)
    _insert_reply(
        db,
        "cli-bsky",
        platform="bluesky",
        inbound_cid="cid-1",
        inbound_url="https://bsky.app/profile/alice/post/cli-bsky",
        platform_metadata=None,
    )
    monkeypatch.setattr(
        audit_reply_target_metadata_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = audit_reply_target_metadata_script.main(
        ["--json", "--platform", "bluesky", "--days", "3", "--limit", "1"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"] == {
        "days": 3,
        "limit": 1,
        "platform": ["bluesky"],
        "status": "pending",
    }
    assert payload["audited_count"] == 1
    assert payload["findings"][0]["platform"] == "bluesky"

    text_exit = audit_reply_target_metadata_script.main(["--platform", "x"])
    text = capsys.readouterr().out
    assert text_exit == 1
    assert "Reply Target Metadata Audit" in text

    invalid = audit_reply_target_metadata_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
