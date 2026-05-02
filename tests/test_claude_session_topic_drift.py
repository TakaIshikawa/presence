"""Tests for Claude session topic-drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from ingestion.claude_session_topic_drift import (
    build_claude_session_topic_drift_report,
    format_claude_session_topic_drift_json,
    format_claude_session_topic_drift_text,
    jaccard_distance,
    tokenize_prompt_keywords,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_topic_drift.py"
spec = importlib.util.spec_from_file_location("claude_session_topic_drift_script", SCRIPT_PATH)
claude_session_topic_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_topic_drift_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    message_uuid: str,
    prompt_text: str,
    session_id: str = "sess-1",
    project_path: str = "/repo/presence",
    timestamp: datetime | None = None,
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=(timestamp or NOW).isoformat(),
        prompt_text=prompt_text,
    )


def test_tokenization_stopwords_and_jaccard_distance_are_deterministic():
    keywords = tokenize_prompt_keywords(
        "Please add the tests, tests, and runners for newsletter sections."
    )

    assert keywords == frozenset({"newsletter", "runner", "section", "test"})
    assert jaccard_distance(
        frozenset({"newsletter", "section"}),
        frozenset({"newsletter", "publish"}),
    ) == pytest.approx(2 / 3)


def test_report_flags_adjacent_prompt_drift_with_excerpts_and_totals(db):
    _add_message(
        db,
        message_uuid="uuid-a1",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=3),
        prompt_text="Tune newsletter section balance scoring and subject line candidate ranking.",
    )
    _add_message(
        db,
        message_uuid="uuid-a2",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=2),
        prompt_text="Tune newsletter section layout and subject line scoring tests.",
    )
    _add_message(
        db,
        message_uuid="uuid-a3",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=1),
        prompt_text="Investigate payment webhook retries, invoice reconciliation, and billing alerts.",
    )
    _add_message(
        db,
        message_uuid="uuid-b1",
        session_id="sess-b",
        timestamp=NOW - timedelta(hours=4),
        prompt_text="Refine the git commit summary exporter.",
    )

    report = build_claude_session_topic_drift_report(
        db,
        days=7,
        threshold=0.7,
        now=NOW,
    )
    payload = json.loads(format_claude_session_topic_drift_json(report))
    text = format_claude_session_topic_drift_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_session_topic_drift"
    assert payload["totals"] == {
        "messages_scanned": 4,
        "sessions_flagged": 1,
        "sessions_scanned": 2,
    }
    flagged = payload["sessions"][0]
    assert flagged["session_id"] == "sess-a"
    assert flagged["recommendation"] == "split_session_summary"
    assert flagged["first_timestamp"] == (NOW - timedelta(hours=3)).isoformat()
    assert flagged["last_timestamp"] == (NOW - timedelta(hours=1)).isoformat()
    assert len(flagged["drift_points"]) == 1
    point = flagged["drift_points"][0]
    assert point["from_message_uuid"] == "uuid-a2"
    assert point["to_message_uuid"] == "uuid-a3"
    assert point["drift_score"] >= 0.7
    assert point["recommendation"] == "split_session_summary"
    assert "newsletter section layout" in point["from_excerpt"]
    assert "payment webhook retries" in point["to_excerpt"]
    assert "Claude Session Topic Drift" in text
    assert "session=sess-a" in text


def test_single_message_sessions_are_preserved_without_false_positive(db):
    _add_message(
        db,
        message_uuid="uuid-single",
        session_id="sess-single",
        prompt_text="Audit one compact newsletter change.",
    )

    report = build_claude_session_topic_drift_report(db, days=7, now=NOW)

    assert report.totals["sessions_scanned"] == 1
    assert report.totals["sessions_flagged"] == 0
    assert report.sessions[0].drift_points == ()
    assert report.sessions[0].max_drift_score == 0.0
    assert report.sessions[0].recommendation == "preserve_single_session"


def test_project_filter_limit_and_json_sort_order(db):
    _add_message(
        db,
        message_uuid="uuid-low-1",
        session_id="sess-low",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=4),
        prompt_text="Draft newsletter issue topic and delivery notes.",
    )
    _add_message(
        db,
        message_uuid="uuid-low-2",
        session_id="sess-low",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=3),
        prompt_text="Draft release notes for newsletter delivery.",
    )
    _add_message(
        db,
        message_uuid="uuid-high-1",
        session_id="sess-high",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=2),
        prompt_text="Review auth middleware session cookies and login redirects.",
    )
    _add_message(
        db,
        message_uuid="uuid-high-2",
        session_id="sess-high",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=1),
        prompt_text="Prepare newsletter unsubscribe attribution and campaign cohorts.",
    )
    _add_message(
        db,
        message_uuid="uuid-other-1",
        session_id="sess-other",
        project_path="/repo/other",
        prompt_text="Webhook billing topic.",
    )

    report = build_claude_session_topic_drift_report(
        db,
        days=7,
        project_path="/repo/presence",
        threshold=0.5,
        limit=1,
        now=NOW,
    )
    payload = json.loads(format_claude_session_topic_drift_json(report))

    assert payload["filters"]["project_path_filter_applied"] is True
    assert payload["totals"]["sessions_scanned"] == 2
    assert len(payload["sessions"]) == 1
    assert payload["sessions"][0]["session_id"] == "sess-high"
    assert payload["sessions"][0]["max_drift_score"] == 1.0


def test_cli_json_and_invalid_args_are_stable(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-cli-1",
        session_id="sess-cli",
        timestamp=NOW - timedelta(minutes=2),
        prompt_text="Work on newsletter section analysis.",
    )
    _add_message(
        db,
        message_uuid="uuid-cli-2",
        session_id="sess-cli",
        timestamp=NOW - timedelta(minutes=1),
        prompt_text="Debug billing webhook reconciliation.",
    )
    monkeypatch.setattr(
        claude_session_topic_drift_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_session_topic_drift_script,
        "build_claude_session_topic_drift_report",
        lambda db, **kwargs: build_claude_session_topic_drift_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_session_topic_drift_script.main(
        ["--days", "7", "--threshold", "0.5", "--limit", "5", "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["sessions"][0]["session_id"] == "sess-cli"
    assert cli_payload["totals"]["sessions_flagged"] == 1

    assert claude_session_topic_drift_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_session_topic_drift_script.main(["--threshold", "1.2"]) == 2
    assert "value must be greater than 0 and at most 1" in capsys.readouterr().err


def test_empty_and_legacy_schema_return_valid_reports():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_session_topic_drift_report(conn, now=NOW)

    assert report.sessions == ()
    assert report.missing_tables == ("claude_messages",)
    assert report.totals["sessions_scanned"] == 0
    assert "Missing tables: claude_messages" in format_claude_session_topic_drift_text(report)

    conn.execute(
        """CREATE TABLE claude_messages (
               id INTEGER PRIMARY KEY,
               session_id TEXT,
               timestamp TEXT,
               prompt_text TEXT
           )"""
    )
    conn.execute(
        """INSERT INTO claude_messages (session_id, timestamp, prompt_text)
           VALUES ('sess-legacy', '2026-05-02T10:00:00+00:00', 'Newsletter topic')"""
    )
    legacy_report = build_claude_session_topic_drift_report(
        conn,
        project_path="/repo/presence",
        now=NOW,
    )

    assert legacy_report.missing_columns == {
        "claude_messages": ("message_uuid", "project_path")
    }
    assert legacy_report.filters["project_path_filter_applied"] is False
    assert legacy_report.totals["messages_scanned"] == 1
    assert legacy_report.sessions[0].project_path is None
