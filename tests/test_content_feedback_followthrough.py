"""Tests for content feedback followthrough reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_feedback_followthrough import (
    build_content_feedback_followthrough_report,
    build_content_feedback_followthrough_report_from_db,
    format_content_feedback_followthrough_json,
    format_content_feedback_followthrough_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_feedback_followthrough.py"
spec = importlib.util.spec_from_file_location("content_feedback_followthrough_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn(path: str | Path = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content TEXT,
            published INTEGER,
            created_at TEXT
        );
        CREATE TABLE content_feedback (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            feedback_type TEXT,
            notes TEXT,
            replacement_text TEXT,
            tags TEXT,
            created_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_stale_feedback_grouped_by_type_and_tag():
    report = build_content_feedback_followthrough_report(
        [
            {
                "feedback_id": 1,
                "content_id": 10,
                "feedback_type": "revise",
                "tags": '["evidence", "hook"]',
                "created_at": (NOW - timedelta(days=10)).isoformat(),
                "replacement_text": "Use a concrete detail.",
                "content_published": 0,
            }
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "content_feedback_followthrough"
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["tags"] == ["evidence", "hook"]
    assert report["findings"][0]["has_replacement_text"] is True
    assert {group["tag"] for group in report["groups"]} == {"evidence", "hook"}


def test_recent_feedback_and_observed_outcomes_are_excluded():
    report = build_content_feedback_followthrough_report(
        [
            {
                "feedback_id": 1,
                "content_id": 10,
                "feedback_type": "reject",
                "created_at": (NOW - timedelta(days=2)).isoformat(),
                "content_published": 0,
            },
            {
                "feedback_id": 2,
                "content_id": 11,
                "feedback_type": "revise",
                "created_at": (NOW - timedelta(days=20)).isoformat(),
                "content_published": 0,
                "newer_reuses_replacement_text": True,
            },
            {
                "feedback_id": 3,
                "content_id": 12,
                "feedback_type": "prefer",
                "created_at": (NOW - timedelta(days=20)).isoformat(),
                "content_published": 1,
            },
        ],
        now=NOW,
        min_age_days=7,
    )

    assert report["summary"]["finding_count"] == 0
    assert report["summary"]["scanned_feedback_count"] == 2


def test_malformed_tags_are_preserved_as_malformed_bucket():
    report = build_content_feedback_followthrough_report(
        [
            {
                "feedback_id": 1,
                "content_id": 10,
                "feedback_type": "reject",
                "tags": "not-json",
                "created_at": (NOW - timedelta(days=8)).isoformat(),
                "content_published": 0,
            }
        ],
        now=NOW,
    )

    assert report["summary"]["malformed_tag_rows"] == 1
    assert report["findings"][0]["tags"] == ["malformed"]
    assert report["groups"][0]["tag"] == "malformed"


def test_db_adapter_detects_replacement_text_and_topic_reuse():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 'Original vague story about build alerts.', 0, ?)", ((NOW - timedelta(days=20)).isoformat(),))
    conn.execute("INSERT INTO generated_content VALUES (2, 'New copy: Use a concrete alert detail.', 0, ?)", ((NOW - timedelta(days=5)).isoformat(),))
    conn.execute("INSERT INTO generated_content VALUES (3, 'A later note about deploy evidence.', 0, ?)", ((NOW - timedelta(days=4)).isoformat(),))
    conn.execute("INSERT INTO generated_content VALUES (4, 'No later reuse here.', 0, ?)", ((NOW - timedelta(days=19)).isoformat(),))
    conn.execute(
        "INSERT INTO content_feedback VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "revise", "Need alert detail", "Use a concrete alert detail.", '["alert"]', (NOW - timedelta(days=18)).isoformat()),
    )
    conn.execute(
        "INSERT INTO content_feedback VALUES (?, ?, ?, ?, ?, ?, ?)",
        (2, 4, "reject", "Needs deploy evidence", None, '["evidence"]', (NOW - timedelta(days=17)).isoformat()),
    )
    conn.commit()

    report = build_content_feedback_followthrough_report_from_db(conn, now=NOW)

    assert report["findings"] == []
    conn.close()


def test_json_text_and_cli_filters_are_stable(tmp_path, capsys, monkeypatch):
    db_path = tmp_path / "feedback.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO generated_content VALUES (1, 'Old copy', 0, ?)", ((NOW - timedelta(days=20)).isoformat(),))
    conn.execute(
        "INSERT INTO content_feedback VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "reject", "Too generic", "", '["tone"]', (NOW - timedelta(days=12)).isoformat()),
    )
    conn.commit()
    conn.close()

    report = build_content_feedback_followthrough_report_from_db(sqlite3.connect(db_path), now=NOW)
    payload = json.loads(format_content_feedback_followthrough_json(report))
    assert payload["artifact_type"] == "content_feedback_followthrough"
    assert list(payload) == sorted(payload)
    text = format_content_feedback_followthrough_text(report)
    assert "Content Feedback Followthrough" in text
    assert "feedback_id | content_id | type" in text

    assert script.main(["--db", str(db_path), "--min-age-days", "7", "--feedback-type", "reject", "--published-state", "unpublished", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "type=reject tag=tone" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["content_feedback", "generated_content"]
    with pytest.raises(SystemExit):
        script.parse_args(["--min-age-days", "0"])
