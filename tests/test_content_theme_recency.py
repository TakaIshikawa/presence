"""Tests for content theme recency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.content_theme_recency import (
    build_content_theme_recency_report,
    build_content_theme_recency_report_from_db,
    format_content_theme_recency_json,
    format_content_theme_recency_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_theme_recency.py"
spec = importlib.util.spec_from_file_location("content_theme_recency_script", SCRIPT_PATH)
content_theme_recency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_theme_recency_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _row(content_id: str, days_ago: int, *, theme: str = "Launch", angle: str = "proof", channel: str = "x", content_type: str = "post") -> dict:
    return {
        "content_id": content_id,
        "theme": theme,
        "angle": angle,
        "channel": channel,
        "content_type": content_type,
        "used_at": (NOW - timedelta(days=days_ago)).isoformat(),
    }


def test_empty_input_has_empty_state():
    report = build_content_theme_recency_report([], now=NOW)

    assert report["empty_state"]["is_empty"] is True
    assert report["totals"]["item_count"] == 0
    assert report["findings"] == []


def test_groups_theme_and_flags_recent_reuse():
    report = build_content_theme_recency_report(
        [
            _row("a", 1, theme="Launch", angle="Proof"),
            _row("b", 6, theme="launch", angle="proof"),
            _row("c", 30, theme="Ops", angle="Queue"),
        ],
        cooldown_days=14,
        now=NOW,
    )

    launch = next(group for group in report["theme_groups"] if group["theme_key"] == "launch proof")

    assert launch["use_count"] == 2
    assert launch["days_since_last_use"] == 5.0
    assert launch["reused_within_cooldown"] is True
    assert report["findings"][0]["theme_key"] == "launch proof"


def test_channel_and_content_type_filters_and_per_channel_summary():
    report = build_content_theme_recency_report(
        [
            _row("a", 1, channel="newsletter", content_type="digest"),
            _row("b", 4, channel="newsletter", content_type="digest"),
            _row("c", 2, channel="x", content_type="post"),
        ],
        channel="newsletter",
        content_type="digest",
        cooldown_days=7,
        now=NOW,
    )

    assert report["totals"]["item_count"] == 2
    assert report["summary"]["by_channel"] == [{"channel": "newsletter", "theme_count": 1, "reused_theme_count": 1}]


def test_db_loader_json_text_and_cli(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            channel TEXT,
            created_at TEXT
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            topic TEXT,
            angle TEXT
        );"""
    )
    conn.execute("INSERT INTO generated_content VALUES (1, 'blog', 'site', ?)", ((NOW - timedelta(days=1)).isoformat(),))
    conn.execute("INSERT INTO generated_content VALUES (2, 'blog', 'site', ?)", ((NOW - timedelta(days=3)).isoformat(),))
    conn.execute("INSERT INTO planned_topics VALUES (1, 1, 'Reliability', 'incident review')")
    conn.execute("INSERT INTO planned_topics VALUES (2, 2, 'reliability', 'incident review')")
    conn.commit()

    report = build_content_theme_recency_report_from_db(conn, cooldown_days=7, now=NOW)

    assert json.loads(format_content_theme_recency_json(report))["artifact_type"] == "content_theme_recency"
    assert "Reliability: incident review" in format_content_theme_recency_text(report)
    monkeypatch.setattr(content_theme_recency_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        content_theme_recency_script,
        "build_content_theme_recency_report_from_db",
        lambda db, **kwargs: build_content_theme_recency_report_from_db(db, now=NOW, **kwargs),
    )
    assert content_theme_recency_script.main(["--format", "text", "--cooldown-days", "7"]) == 0
    assert "Totals: items=2" in capsys.readouterr().out
