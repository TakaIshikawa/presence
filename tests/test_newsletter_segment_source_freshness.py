"""Tests for newsletter segment source freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_segment_source_freshness import (
    build_newsletter_segment_source_freshness_report,
    build_newsletter_segment_source_freshness_report_from_db,
    format_newsletter_segment_source_freshness_table,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_segment_source_freshness.py"
spec = importlib.util.spec_from_file_location("newsletter_segment_source_freshness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(source_id: str, hours_ago: int) -> dict[str, str]:
    return {"source_id": source_id, "published_at": (NOW - timedelta(hours=hours_ago)).isoformat()}


def test_segments_are_bucketed_from_newest_and_oldest_source_timestamps():
    report = build_newsletter_segment_source_freshness_report(
        [
            {"newsletter_id": "n1", "segment_id": "missing", "sources": []},
            {"newsletter_id": "n1", "segment_id": "stale", "sources": [_source("a", 10), _source("b", 400)]},
            {"newsletter_id": "n1", "segment_id": "aging", "sources": [_source("c", 8), _source("d", 100)]},
            {"newsletter_id": "n1", "segment_id": "fresh", "sources": [_source("e", 2), _source("f", 6)]},
        ],
        fresh_hours=72,
        aging_hours=336,
        now=NOW,
    )
    by_segment = {row["segment_id"]: row for row in report["rows"]}

    assert by_segment["missing"]["source_count"] == 0
    assert by_segment["missing"]["freshness_bucket"] == "missing_source"
    assert by_segment["missing"]["risk_label"] == "missing_source"
    assert by_segment["stale"]["newest_source_age_hours"] == 10.0
    assert by_segment["stale"]["oldest_source_age_hours"] == 400.0
    assert by_segment["stale"]["freshness_bucket"] == "stale"
    assert by_segment["aging"]["freshness_bucket"] == "aging"
    assert by_segment["fresh"]["freshness_bucket"] == "fresh"
    assert report["summary"]["segment_count"] == 4


def test_db_loader_reads_metadata_segments_and_cli_outputs_json_and_table(db, monkeypatch, capsys):
    metadata = {
        "segments": [
            {"id": "lead", "title": "Lead", "sources": [_source("s1", 3), _source("s2", 5)]},
            {"id": "deep", "title": "Deep Dive", "sources": [_source("s3", 500)]},
        ]
    }
    db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, metadata, sent_at)
           VALUES (?, ?, ?, ?, ?)""",
        ("issue-1", "Subject", json.dumps([]), json.dumps(metadata), NOW.isoformat()),
    )
    db.conn.commit()

    report = build_newsletter_segment_source_freshness_report_from_db(db, fresh_hours=72, aging_hours=336, now=NOW)
    by_segment = {row["segment_id"]: row for row in report["rows"]}
    assert by_segment["lead"]["freshness_bucket"] == "fresh"
    assert by_segment["deep"]["risk_label"] == "stale_sources"
    assert "Newsletter Segment Source Freshness" in format_newsletter_segment_source_freshness_table(report)

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_newsletter_segment_source_freshness_report_from_db",
        lambda db, **kwargs: build_newsletter_segment_source_freshness_report_from_db(db, now=NOW, **kwargs),
    )

    assert script.main(["--fresh-hours", "72", "--aging-hours", "336"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_segment_source_freshness"
    assert payload["summary"]["segment_count"] == 2

    assert script.main(["--table"]) == 0
    assert "stale_sources" in capsys.readouterr().out
