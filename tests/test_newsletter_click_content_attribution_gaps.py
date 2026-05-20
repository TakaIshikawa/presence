"""Tests for newsletter click content attribution gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_click_content_attribution_gaps import (
    build_newsletter_click_content_attribution_gaps_report,
    build_newsletter_click_content_attribution_gaps_report_from_db,
    format_newsletter_click_content_attribution_gaps_json,
    format_newsletter_click_content_attribution_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_click_content_attribution_gaps.py"
spec = importlib.util.spec_from_file_location("newsletter_click_content_attribution_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: int = 1) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT,
               content TEXT
           );
           CREATE TABLE newsletter_link_clicks (
               id INTEGER PRIMARY KEY,
               newsletter_send_id INTEGER,
               issue_id TEXT NOT NULL,
               content_id INTEGER,
               source_kind TEXT,
               link_url TEXT NOT NULL,
               raw_url TEXT,
               clicks INTEGER,
               unique_clicks INTEGER,
               fetched_at TEXT NOT NULL,
               created_at TEXT
           );"""
    )
    return conn


def _click(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    issue_id: str = "issue-1",
    content_id: int | None = None,
    source_kind: str | None = None,
    link_url: str = "https://example.com/link",
    raw_url: str | None = None,
    clicks: int = 1,
    fetched_at: str | None = None,
    newsletter_send_id: int = 1,
) -> None:
    conn.execute(
        """INSERT INTO newsletter_link_clicks
           (id, newsletter_send_id, issue_id, content_id, source_kind, link_url, raw_url,
            clicks, unique_clicks, fetched_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            row_id,
            newsletter_send_id,
            issue_id,
            content_id,
            source_kind,
            link_url,
            raw_url,
            clicks,
            clicks,
            fetched_at or _ts(),
            fetched_at or _ts(),
        ),
    )


def test_builder_detects_required_gap_types_and_duplicates():
    report = build_newsletter_click_content_attribution_gaps_report(
        [
            {
                "id": 1,
                "issue_id": "issue-1",
                "source_kind": "generated_content",
                "content_id": None,
                "link_url": "https://example.com/a",
                "clicks": 3,
                "fetched_at": _ts(),
            },
            {
                "id": 2,
                "issue_id": "issue-1",
                "source_kind": "generated_content",
                "content_id": 99,
                "matched_content_id": None,
                "link_url": "https://example.com/b",
                "clicks": 2,
                "fetched_at": _ts(),
            },
            {
                "id": 3,
                "issue_id": "issue-1",
                "content_id": None,
                "link_url": "https://presence.example/blog/content/42",
                "clicks": 4,
                "fetched_at": _ts(),
            },
            {
                "id": 4,
                "issue_id": "issue-1",
                "content_id": 12,
                "matched_content_id": 12,
                "link_url": "https://example.com/dup",
                "clicks": 1,
                "fetched_at": _ts(),
            },
            {
                "id": 5,
                "issue_id": "issue-1",
                "content_id": 12,
                "matched_content_id": 12,
                "link_url": "https://example.com/dup",
                "clicks": 1,
                "fetched_at": _ts(),
            },
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "newsletter_click_content_attribution_gaps"
    assert report["summary"]["by_issue_type"] == {
        "duplicate_attribution_rows": 1,
        "internal_unattributed_link": 1,
        "missing_content_id": 1,
        "orphan_content_id": 1,
    }
    assert report["summary"]["gap_count"] == 4


def test_db_loader_flags_missing_orphan_internal_and_duplicate_rows():
    conn = _conn()
    conn.execute("INSERT INTO generated_content (id, content_type, content) VALUES (10, 'blog_post', 'ok')")
    _click(conn, row_id=1, source_kind="generated_content", link_url="https://example.com/missing", clicks=5)
    _click(conn, row_id=2, content_id=99, source_kind="generated_content", link_url="https://example.com/orphan", clicks=4)
    _click(conn, row_id=3, link_url="https://presence.example/posts/abc", raw_url="https://presence.example/posts/abc", clicks=3)
    _click(conn, row_id=4, content_id=10, link_url="https://example.com/dup", clicks=2, newsletter_send_id=1)
    _click(conn, row_id=5, content_id=10, link_url="https://example.com/dup", clicks=2, newsletter_send_id=2)
    _click(conn, row_id=6, issue_id="old", source_kind="generated_content", clicks=10, fetched_at=_ts(120))
    conn.commit()

    report = build_newsletter_click_content_attribution_gaps_report_from_db(conn, now=NOW)

    assert report["summary"]["rows_scanned"] == 5
    assert report["summary"]["gap_count"] == 4
    assert {issue for item in report["gap_items"] for issue in item["issue_types"]} == {
        "duplicate_attribution_rows",
        "internal_unattributed_link",
        "missing_content_id",
        "orphan_content_id",
    }
    duplicate = [item for item in report["gap_items"] if item["issue_types"] == ["duplicate_attribution_rows"]][0]
    assert duplicate["duplicate_row_ids"] == [4, 5]


def test_filters_limit_and_missing_tables_are_reported():
    conn = _conn()
    _click(conn, row_id=1, issue_id="issue-1", source_kind="generated_content", clicks=1)
    _click(conn, row_id=2, issue_id="issue-2", source_kind="generated_content", clicks=10)
    conn.commit()

    report = build_newsletter_click_content_attribution_gaps_report_from_db(
        conn,
        issue_id="issue-2",
        min_clicks=2,
        limit=1,
        now=NOW,
    )

    assert report["summary"]["rows_scanned"] == 1
    assert report["summary"]["shown_count"] == 1
    assert report["gap_items"][0]["issue_id"] == "issue-2"

    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing = build_newsletter_click_content_attribution_gaps_report_from_db(empty, now=NOW)
    assert missing["missing_tables"] == ["generated_content", "newsletter_link_clicks"]
    assert missing["gap_items"] == []


def test_formatters_and_cli_support_requested_arguments(tmp_path, monkeypatch, capsys):
    conn = _conn()
    _click(conn, row_id=1, issue_id="issue-1", source_kind="generated_content", clicks=3)
    conn.commit()
    db_path = tmp_path / "clicks.sqlite"
    backup = sqlite3.connect(db_path)
    conn.backup(backup)
    backup.close()

    assert script.main(["--db", str(db_path), "--issue-id", "issue-1", "--min-clicks", "2", "--window-days", "30", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_click_content_attribution_gaps"
    assert payload["summary"]["gap_count"] == 1

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Click Content Attribution Gaps" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["gap_items"] == []
    assert script.main(["--window-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_builder_rejects_invalid_filters():
    with pytest.raises(ValueError, match="min_clicks must be non-negative"):
        build_newsletter_click_content_attribution_gaps_report([], min_clicks=-1)
    with pytest.raises(ValueError, match="window_days must be positive"):
        build_newsletter_click_content_attribution_gaps_report([], window_days=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_newsletter_click_content_attribution_gaps_report([], limit=0)


def test_json_formatter_is_stable():
    report = build_newsletter_click_content_attribution_gaps_report([], now=NOW)
    assert list(json.loads(format_newsletter_click_content_attribution_gaps_json(report))) == sorted(report)
    assert "No newsletter click content attribution gaps found" in format_newsletter_click_content_attribution_gaps_text(report)
