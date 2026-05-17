"""Tests for newsletter subject promise fulfillment reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_subject_promise_fulfillment import (
    build_newsletter_subject_promise_fulfillment_report,
    format_newsletter_subject_promise_fulfillment_json,
    format_newsletter_subject_promise_fulfillment_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_promise_fulfillment.py"
spec = importlib.util.spec_from_file_location("newsletter_subject_promise_fulfillment_script", SCRIPT_PATH)
newsletter_subject_promise_fulfillment_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_subject_promise_fulfillment_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn(with_clicks: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY, issue_id TEXT, sent_at TEXT, status TEXT, source_content_ids TEXT
        );
        CREATE TABLE newsletter_subject_candidates (
            id INTEGER PRIMARY KEY, newsletter_send_id INTEGER, subject TEXT, selected INTEGER
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, title TEXT, content TEXT
        );"""
    )
    if with_clicks:
        conn.execute("CREATE TABLE newsletter_link_clicks (newsletter_send_id INTEGER, content_id INTEGER, clicks INTEGER)")
    return conn


def _issue(conn: sqlite3.Connection, send_id: int, subject: str, source_text: str, *, clicks: bool = True) -> None:
    conn.execute("INSERT INTO generated_content (id, title, content) VALUES (?, ?, ?)", (send_id, f"Title {send_id}", source_text))
    conn.execute(
        "INSERT INTO newsletter_sends VALUES (?, ?, ?, 'sent', ?)",
        (send_id, f"issue-{send_id}", (NOW - timedelta(days=send_id)).isoformat(), json.dumps([send_id])),
    )
    conn.execute("INSERT INTO newsletter_subject_candidates VALUES (?, ?, ?, 1)", (send_id, send_id, subject))
    if clicks:
        conn.execute("INSERT INTO newsletter_link_clicks VALUES (?, ?, 1)", (send_id, send_id))
    conn.commit()


def test_selected_subjects_are_ranked_by_overlap_and_click_risk():
    conn = _conn()
    _issue(conn, 1, "SQLite cadence diagnostics", "SQLite cadence diagnostics reduce newsletter drift", clicks=True)
    _issue(conn, 2, "AI launch surprise", "Publication retry budgets and platform backlogs", clicks=False)

    report = build_newsletter_subject_promise_fulfillment_report(conn, now=NOW, min_overlap=0.4)

    assert report["items"][0]["newsletter_send_id"] == 2
    assert report["items"][0]["risk_level"] == "high"
    assert report["items"][1]["risk_level"] == "ok"


def test_missing_optional_click_table_reports_schema_gap_but_scores_sources():
    conn = _conn(with_clicks=False)
    _issue(conn, 1, "SQLite cadence diagnostics", "SQLite cadence diagnostics reduce newsletter drift", clicks=False)

    report = build_newsletter_subject_promise_fulfillment_report(conn, now=NOW)

    assert report["missing_tables"] == ["newsletter_link_clicks"]
    assert report["items"][0]["overlap_score"] > 0


def test_json_text_cli_and_invalid_arguments(monkeypatch, capsys):
    conn = _conn()
    _issue(conn, 1, "SQLite cadence diagnostics", "SQLite cadence diagnostics reduce newsletter drift")
    report = build_newsletter_subject_promise_fulfillment_report(conn, now=NOW)

    assert json.loads(format_newsletter_subject_promise_fulfillment_json(report))["artifact_type"] == "newsletter_subject_promise_fulfillment"
    assert "Newsletter Subject Promise Fulfillment" in format_newsletter_subject_promise_fulfillment_text(report)
    monkeypatch.setattr(newsletter_subject_promise_fulfillment_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        newsletter_subject_promise_fulfillment_script,
        "build_newsletter_subject_promise_fulfillment_report",
        lambda db, **kwargs: build_newsletter_subject_promise_fulfillment_report(db, now=NOW, **kwargs),
    )
    assert newsletter_subject_promise_fulfillment_script.main(["--format", "text"]) == 0
    assert "Totals: issues=1" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        newsletter_subject_promise_fulfillment_script.parse_args(["--min-overlap", "2"])


def test_missing_required_schema_returns_gaps():
    report = build_newsletter_subject_promise_fulfillment_report(sqlite3.connect(":memory:"), now=NOW)
    assert "newsletter_subject_candidates" in report["missing_tables"]
    assert report["items"] == []
