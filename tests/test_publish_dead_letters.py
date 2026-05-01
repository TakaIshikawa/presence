"""Tests for publish queue dead-letter exports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import csv
import importlib.util
from io import StringIO
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.publish_dead_letters import (
    CSV_HEADERS,
    build_publish_dead_letter_report,
    format_csv_report,
    format_json_report,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_publish_dead_letters.py"
spec = importlib.util.spec_from_file_location("export_publish_dead_letters_script", SCRIPT_PATH)
export_publish_dead_letters_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_publish_dead_letters_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _iso(days: int) -> str:
    return (NOW - timedelta(days=days)).isoformat()


def _content(db, text: str, *, retry_count: int = 0) -> int:
    cursor = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, retry_count, created_at)
           VALUES ('x_post', ?, 8, ?, ?)""",
        (text, retry_count, _iso(1)),
    )
    return int(cursor.lastrowid)


def _publication(
    db,
    content_id: int,
    *,
    platform: str = "x",
    error: str = "Unauthorized token",
    error_category: str = "auth",
    attempt_count: int = 1,
    last_error_days: int = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            last_error_at, updated_at)
           VALUES (?, ?, 'failed', ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            error,
            error_category,
            attempt_count,
            _iso(last_error_days),
            _iso(last_error_days),
        ),
    )
    return int(cursor.lastrowid)


def _queue(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "failed",
    error: str | None = "duplicate status",
    error_category: str | None = "duplicate",
    hold_reason: str | None = None,
    created_days: int = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category,
            hold_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            _iso(created_days),
            platform,
            status,
            error,
            error_category,
            hold_reason,
            _iso(created_days),
        ),
    )
    return int(cursor.lastrowid)


def test_terminal_publication_failures_group_by_reason_and_platform(db):
    auth_content = _content(db, "Fix auth")
    duplicate_content = _content(db, "Duplicate")
    network_content = _content(db, "Transient")
    _publication(db, auth_content, platform="x", error_category="auth")
    _publication(db, duplicate_content, platform="bluesky", error_category="duplicate")
    _publication(
        db,
        network_content,
        platform="x",
        error="Gateway timeout",
        error_category="network",
        attempt_count=1,
    )
    db.conn.commit()

    report = build_publish_dead_letter_report(db, days=7, now=NOW)

    assert [item["terminal_reason"] for item in report["items"]] == [
        "auth_error",
        "duplicate_error",
    ]
    assert report["totals"]["by_platform"] == {"bluesky": 1, "x": 1}
    assert report["totals"]["by_terminal_reason"] == {
        "auth_error": 1,
        "duplicate_error": 1,
    }


def test_max_retry_failures_are_terminal_and_merge_queue_identity(db):
    content_id = _content(db, "Network keeps failing")
    publication_id = _publication(
        db,
        content_id,
        platform="x",
        error="Gateway timeout",
        error_category="network",
        attempt_count=3,
    )
    queue_id = _queue(
        db,
        content_id,
        platform="x",
        status="failed",
        error="Gateway timeout",
        error_category="network",
    )
    db.conn.commit()

    report = build_publish_dead_letter_report(db, days=7, now=NOW)

    assert report["items"] == [
        {
            "content_id": content_id,
            "queue_id": queue_id,
            "publication_id": publication_id,
            "platform": "x",
            "terminal_reason": "max_retries",
            "last_error": "Gateway timeout",
            "failed_at": _iso(1),
            "retry_count": 3,
            "content_preview": "Network keeps failing",
            "operator_action": "manual_replay_or_cancel",
        }
    ]


def test_held_items_are_excluded_by_default_and_separate_when_included(db):
    failed_id = _content(db, "Credential failure")
    held_id = _content(db, "Needs legal review")
    _publication(db, failed_id, platform="x", error_category="auth")
    queue_id = _queue(
        db,
        held_id,
        platform="all",
        status="held",
        error=None,
        error_category=None,
        hold_reason="legal review",
    )
    db.conn.commit()

    default_report = build_publish_dead_letter_report(db, days=7, now=NOW)
    held_report = build_publish_dead_letter_report(
        db,
        days=7,
        include_held=True,
        now=NOW,
    )

    assert default_report["totals"]["held"] == 0
    assert held_report["totals"]["held"] == 2
    held_rows = [item for item in held_report["items"] if item["terminal_reason"] == "held"]
    assert {(row["queue_id"], row["platform"], row["last_error"]) for row in held_rows} == {
        (queue_id, "x", "legal review"),
        (queue_id, "bluesky", "legal review"),
    }


def test_csv_headers_and_content_preview_escaping_are_deterministic(db):
    content_id = _content(db, 'First line\n"quoted", with comma')
    _publication(db, content_id, platform="x", error_category="auth")
    db.conn.commit()

    report = build_publish_dead_letter_report(db, days=7, now=NOW)
    csv_text = format_csv_report(report)

    assert csv_text.splitlines()[0] == ",".join(CSV_HEADERS)
    parsed = list(csv.DictReader(StringIO(csv_text)))
    assert parsed[0]["content_preview"] == 'First line "quoted", with comma'
    assert '"First line ""quoted"", with comma"' in csv_text


def test_json_and_text_formatters_are_stable(db):
    content_id = _content(db, "Credential failure")
    _publication(db, content_id, platform="x", error_category="auth")
    db.conn.commit()

    report = build_publish_dead_letter_report(db, days=7, now=NOW)

    assert json.loads(format_json_report(report))["artifact_type"] == "publish_dead_letters"
    assert format_text_report(report) == "\n".join(
        [
            "Publish Dead-Letter Export",
            "Generated: 2026-05-01T12:00:00+00:00",
            "Filters: days=7 platform=all include_held=no",
            "Totals: items=1 failed=1 held=0",
            "",
            "Items",
            f"  - content={content_id} platform=x publication=1 reason=auth_error retries=1 failed_at=2026-04-30T12:00:00+00:00 action=fix_credentials",
        ]
    )


def test_missing_publication_tables_return_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content TEXT
           )"""
    )

    report = build_publish_dead_letter_report(conn, days=7, now=NOW)

    assert report["items"] == []
    assert report["totals"]["items"] == 0
    assert report["missing_required_tables"] == [
        "content_publications",
        "publish_queue",
    ]


def test_cli_supports_json_and_csv(db, monkeypatch, capsys):
    content_id = _content(db, "Credential failure")
    _publication(db, content_id, platform="x", error_category="auth")
    db.conn.commit()
    monkeypatch.setattr(
        export_publish_dead_letters_script,
        "script_context",
        lambda: _script_context(db),
    )

    json_exit = export_publish_dead_letters_script.main(
        ["--days", "7", "--format", "json"]
    )
    json_payload = json.loads(capsys.readouterr().out)
    csv_exit = export_publish_dead_letters_script.main(
        ["--days", "7", "--format", "csv"]
    )
    csv_output = capsys.readouterr().out

    assert json_exit == 0
    assert json_payload["totals"]["items"] == 1
    assert csv_exit == 0
    assert csv_output.startswith(",".join(CSV_HEADERS))
