"""Tests for proactive action draft completeness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.proactive_action_draft_completeness import (
    build_proactive_action_draft_completeness_report,
    build_proactive_action_draft_completeness_report_from_db,
    format_proactive_action_draft_completeness_json,
    format_proactive_action_draft_completeness_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_draft_completeness.py"
spec = importlib.util.spec_from_file_location("proactive_action_draft_completeness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE proactive_actions (
            id INTEGER PRIMARY KEY,
            action_type TEXT,
            target_tweet_text TEXT,
            target_author_handle TEXT,
            discovery_source TEXT,
            draft_text TEXT,
            status TEXT,
            posted_tweet_id TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _action(
    conn: sqlite3.Connection,
    action_id: int,
    *,
    action_type: str = "reply",
    target_tweet_text: str | None = "Interesting point",
    target_author_handle: str | None = "author",
    discovery_source: str | None = "search",
    draft_text: str | None = "Thanks for sharing",
    status: str = "pending",
    posted_tweet_id: str | None = None,
    created_at: str = "2026-05-03T10:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO proactive_actions
           (id, action_type, target_tweet_text, target_author_handle, discovery_source,
            draft_text, status, posted_tweet_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            action_id,
            action_type,
            target_tweet_text,
            target_author_handle,
            discovery_source,
            draft_text,
            status,
            posted_tweet_id,
            created_at,
        ),
    )
    conn.commit()


def test_clean_data_has_grouped_totals_without_findings():
    conn = _conn()
    _action(conn, 1, action_type="reply", discovery_source="search", status="pending")
    _action(conn, 2, action_type="like", discovery_source="list", status="approved", draft_text=None)
    _action(conn, 3, action_type="quote_tweet", discovery_source="search", status="posted", posted_tweet_id="tweet-1")

    report = build_proactive_action_draft_completeness_report_from_db(conn, now=NOW)

    assert report["artifact_type"] == "proactive_action_draft_completeness"
    assert report["totals"]["actions_matched"] == 3
    assert report["totals"]["finding_count"] == 0
    assert report["totals"]["by_action_type"] == {"like": 1, "quote_tweet": 1, "reply": 1}
    assert report["totals"]["by_discovery_source"] == {"list": 1, "search": 2}
    assert report["totals"]["by_status"] == {"approved": 1, "pending": 1, "posted": 1}
    assert report["findings"] == []


def test_classifies_each_completeness_issue():
    conn = _conn()
    _action(conn, 1, draft_text=" ", status="pending")
    _action(conn, 2, target_tweet_text=None, action_type="like", status="approved")
    _action(conn, 3, target_author_handle="", action_type="reply", status="approved", draft_text=None)
    _action(conn, 4, status="posted", posted_tweet_id=None)

    report = build_proactive_action_draft_completeness_report_from_db(conn, now=NOW)

    assert [finding["issue_type"] for finding in report["findings"]] == [
        "missing_draft_text",
        "missing_draft_text",
        "missing_target_text",
        "missing_target_author",
        "posted_without_platform_id",
    ]
    assert report["totals"]["missing_draft_text"] == 2
    assert report["totals"]["missing_target_text"] == 1
    assert report["totals"]["missing_target_author"] == 1
    assert report["totals"]["posted_without_platform_id"] == 1


def test_filters_limit_rows_and_groups():
    conn = _conn()
    _action(conn, 1, action_type="reply", discovery_source="search", status="pending", draft_text=None)
    _action(conn, 2, action_type="quote_tweet", discovery_source="timeline", status="approved", draft_text=None)
    _action(conn, 3, action_type="reply", discovery_source="timeline", status="posted", posted_tweet_id=None)

    report = build_proactive_action_draft_completeness_report_from_db(
        conn,
        status="pending,approved",
        action_type="reply",
        discovery_source="search",
        now=NOW,
    )

    assert report["totals"]["rows_scanned"] == 3
    assert report["totals"]["actions_matched"] == 1
    assert report["findings"][0]["action_id"] == 1
    assert report["filters"] == {
        "status": ["approved", "pending"],
        "action_type": ["reply"],
        "discovery_source": ["search"],
    }


def test_missing_schema_is_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    missing_table = build_proactive_action_draft_completeness_report_from_db(conn, now=NOW)
    assert missing_table["missing_tables"] == ["proactive_actions"]
    assert missing_table["totals"]["rows_scanned"] == 0

    conn.execute("CREATE TABLE proactive_actions (id INTEGER PRIMARY KEY, action_type TEXT)")
    missing_columns = build_proactive_action_draft_completeness_report_from_db(conn, now=NOW)
    assert missing_columns["missing_columns"]["proactive_actions"] == [
        "created_at",
        "discovery_source",
        "draft_text",
        "posted_tweet_id",
        "status",
        "target_author_handle",
        "target_tweet_text",
    ]


def test_formatters_are_stable_and_text_is_readable():
    report = build_proactive_action_draft_completeness_report(
        [
            {
                "id": 1,
                "action_type": "reply",
                "target_tweet_text": "",
                "target_author_handle": "author",
                "discovery_source": "search",
                "draft_text": "",
                "status": "pending",
                "posted_tweet_id": None,
                "created_at": "2026-05-03T10:00:00+00:00",
            }
        ],
        now=NOW,
    )

    payload = json.loads(format_proactive_action_draft_completeness_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "proactive_action_draft_completeness"
    text = format_proactive_action_draft_completeness_text(report)
    assert "Proactive Action Draft Completeness" in text
    assert "missing_draft_text action_id=1" in text
    assert "missing_target_text action_id=1" in text


def test_cli_supports_db_json_text_and_configured_context(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "actions.sqlite"
    conn = _conn()
    _action(conn, 1, draft_text=None, status="pending")
    conn.backup(sqlite3.connect(db_path))
    conn.close()

    assert script.main(["--db", str(db_path), "--status", "pending", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["missing_draft_text"] == 1

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Draft Completeness" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--action-type", "reply", "--discovery-source", "search"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
