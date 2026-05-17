"""Tests for newsletter link rot priority reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_link_rot_priority import build_newsletter_link_rot_priority_report


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_rot_priority.py"
spec = importlib.util.spec_from_file_location("newsletter_link_rot_priority_script", SCRIPT_PATH)
newsletter_link_rot_priority_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_link_rot_priority_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_links (
            newsletter_id TEXT,
            url TEXT,
            status TEXT,
            status_code INTEGER,
            checked_at TEXT,
            clicks INTEGER,
            placement INTEGER,
            section TEXT
        )"""
    )
    return conn


def test_priority_ordering_combines_health_staleness_and_importance():
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_links VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("issue-1", "https://example.com/stale", "healthy", 200, "2026-04-01T00:00:00+00:00", 80, 1, "hero"),
            ("issue-2", "https://example.com/broken", "broken", 500, "2026-05-17T00:00:00+00:00", 12, 3, "body"),
            ("issue-3", "https://example.com/down-old", "timeout", None, "2026-03-01T00:00:00+00:00", 20, 2, "footer"),
        ],
    )

    report = build_newsletter_link_rot_priority_report(conn, stale_days=30, now=NOW)

    assert [item["url"] for item in report["findings"]] == [
        "https://example.com/down-old",
        "https://example.com/broken",
        "https://example.com/stale",
    ]
    assert report["findings"][0]["issue_reason"] == "failing_stale"


def test_missing_engagement_metrics_are_reported_with_zero_signal():
    conn = _conn()
    conn.execute(
        "INSERT INTO newsletter_links VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("issue-1", "https://example.com/missing", "broken", 404, "2026-05-17T00:00:00+00:00", None, None, ""),
    )

    report = build_newsletter_link_rot_priority_report(conn, now=NOW)

    finding = report["findings"][0]
    assert finding["engagement_missing"] is True
    assert finding["importance_signal"] == 0
    assert report["totals"]["missing_engagement_count"] == 1


def test_healthy_fresh_links_are_excluded():
    conn = _conn()
    conn.execute(
        "INSERT INTO newsletter_links VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("issue-1", "https://example.com/ok", "healthy", 200, "2026-05-17T00:00:00+00:00", 200, 1, "hero"),
    )

    report = build_newsletter_link_rot_priority_report(conn, now=NOW)

    assert report["findings"] == []


def test_cli_json_output_uses_script_context(capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        "INSERT INTO newsletter_links VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("issue-1", "https://example.com/down", "broken", 500, "2026-05-01T00:00:00+00:00", 5, 1, "hero"),
    )
    monkeypatch.setattr(newsletter_link_rot_priority_script, "script_context", lambda: _script_context(conn))

    assert newsletter_link_rot_priority_script.main(["--format", "json", "--stale-days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "newsletter_link_rot_priority"
    assert payload["findings"][0]["newsletter_id"] == "issue-1"
    assert payload["findings"][0]["issue_reason"] == "failing_stale"
