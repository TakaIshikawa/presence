"""Tests for newsletter CTA freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_cta_freshness import build_newsletter_cta_freshness_report


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_cta_freshness.py"
spec = importlib.util.spec_from_file_location("newsletter_cta_freshness_script", SCRIPT_PATH)
newsletter_cta_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_cta_freshness_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE newsletter_ctas (newsletter_id TEXT, cta_text TEXT, target_url TEXT, updated_at TEXT, performance_at TEXT)"
    )
    return conn


def test_stale_reuse_is_flagged():
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_ctas VALUES (?, ?, ?, ?, ?)",
        [
            ("n1", "Try it", "https://example.com", "2026-03-01T00:00:00+00:00", "2026-03-02T00:00:00+00:00"),
            ("n2", "Try it", "https://example.com", "2026-03-05T00:00:00+00:00", "2026-03-06T00:00:00+00:00"),
        ],
    )
    finding = build_newsletter_cta_freshness_report(conn, stale_days=30, now=NOW)["findings"][0]
    assert finding["severity"] == "stale"
    assert finding["issue_count"] == 2


def test_fresh_reuse_is_not_flagged():
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_ctas VALUES (?, ?, ?, ?, ?)",
        [
            ("n1", "Try it", "https://example.com", "2026-05-17T00:00:00+00:00", "2026-05-17T00:00:00+00:00"),
            ("n2", "Try it", "https://example.com", "2026-05-18T00:00:00+00:00", "2026-05-18T00:00:00+00:00"),
        ],
    )
    assert build_newsletter_cta_freshness_report(conn, now=NOW)["findings"] == []


def test_missing_performance_data_is_flagged():
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_ctas VALUES (?, ?, ?, ?, ?)",
        [
            ("n1", "Try it", "https://example.com", "2026-05-17T00:00:00+00:00", ""),
            ("n2", "Try it", "https://example.com", "2026-05-18T00:00:00+00:00", ""),
        ],
    )
    finding = build_newsletter_cta_freshness_report(conn, now=NOW)["findings"][0]
    assert finding["severity"] == "missing_performance"
    assert finding["missing_performance"] is True


def test_cli_json_output(capsys, monkeypatch):
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_ctas VALUES (?, ?, ?, ?, ?)",
        [
            ("n1", "Try it", "https://example.com", "2026-03-01T00:00:00+00:00", ""),
            ("n2", "Try it", "https://example.com", "2026-03-01T00:00:00+00:00", ""),
        ],
    )
    monkeypatch.setattr(newsletter_cta_freshness_script, "script_context", lambda: _script_context(conn))
    assert newsletter_cta_freshness_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_cta_freshness"
    assert payload["findings"][0]["cta_text"] == "Try it"
