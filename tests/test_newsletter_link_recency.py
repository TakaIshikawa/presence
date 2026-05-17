"""Tests for newsletter link recency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.newsletter_link_recency import build_newsletter_link_recency_report, build_newsletter_link_recency_report_from_db


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_recency.py"
spec = importlib.util.spec_from_file_location("newsletter_link_recency_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_extracts_links_and_buckets_by_source_age():
    report = build_newsletter_link_recency_report(
        [{"id": "n1", "body": "A https://fresh.example/x B https://old.example/y C https://unknown.example/z", "sent_at": "2026-05-01T00:00:00+00:00"}],
        [
            {"url": "https://fresh.example/x", "published_at": "2026-04-25T00:00:00+00:00"},
            {"url": "https://old.example/y", "published_at": "2025-12-01T00:00:00+00:00"},
        ],
        fresh_days=30,
        stale_days=90,
        now=NOW,
    )

    buckets = {row["url"]: row["recency_bucket"] for row in report["links"]}
    assert buckets["https://fresh.example/x"] == "fresh"
    assert buckets["https://old.example/y"] == "stale"
    assert buckets["https://unknown.example/z"] == "unknown_date"
    assert report["totals"]["link_count"] == 3
    assert report["totals"]["stale_rate"] == 0.3333


def test_aging_bucket_and_empty_state():
    report = build_newsletter_link_recency_report(
        [{"id": "n1", "html": "<a href='https://aging.example/x'>x</a>", "sent_at": "2026-05-01T00:00:00+00:00"}],
        [{"url": "https://aging.example/x", "source_date": "2026-03-01T00:00:00+00:00"}],
        now=NOW,
    )
    assert report["links"][0]["recency_bucket"] == "aging"

    empty = build_newsletter_link_recency_report([], now=NOW)
    assert empty["empty_state"]["is_empty"] is True


def test_db_adapter_loads_newsletter_and_source_metadata():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE newsletter_sends (id TEXT, body TEXT, sent_at TEXT)")
    conn.execute("CREATE TABLE newsletter_links (url TEXT, source_date TEXT)")
    conn.execute("INSERT INTO newsletter_sends VALUES ('n1', 'See https://example.com/a', '2026-05-01T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_links VALUES ('https://example.com/a', '2026-04-01T00:00:00+00:00')")

    report = build_newsletter_link_recency_report_from_db(conn, now=NOW)

    assert report["links"][0]["newsletter_id"] == "n1"
    assert report["links"][0]["age_days"] == 30.0


def test_cli_supports_flags_and_invalid_limit(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_newsletter_link_recency_report_from_db",
        lambda _db, **kwargs: build_newsletter_link_recency_report([{"id": "n1", "body": "https://example.com"}], now=NOW, **kwargs),
    )

    assert script.main(["--stale-days", "10", "--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_link_recency"
    assert script.main(["--table"]) == 0
    assert "newsletter_id | bucket" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
