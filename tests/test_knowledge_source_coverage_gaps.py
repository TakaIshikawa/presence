"""Tests for knowledge source coverage gaps reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from knowledge.source_coverage_gaps import (
    build_source_coverage_gaps_report,
    format_source_coverage_gaps_json,
    format_source_coverage_gaps_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_coverage_gaps.py"
spec = importlib.util.spec_from_file_location("knowledge_source_coverage_gaps_script", SCRIPT_PATH)
knowledge_source_coverage_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_source_coverage_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content_with_topic(db, topic: str, *, days_ago: int = 1) -> int:
    content_id = db.insert_generated_content("blog_post", [], [], f"Draft about {topic}", 7, "ok")
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), content_id),
    )
    db.insert_content_topics(content_id, [(topic, "", 1.0)])
    return content_id


def _source(db, text: str, *, days_ago: int, approved: int = 1) -> None:
    stamp = (NOW - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, content, insight, approved, published_at, ingested_at)
           VALUES ('curated_article', ?, ?, ?, ?, ?, ?)""",
        (f"src-{text}-{days_ago}", text, text, approved, stamp, stamp),
    )
    db.conn.commit()


def test_covered_themes_are_not_reported(db):
    _content_with_topic(db, "testing")
    _source(db, "testing strategy one", days_ago=5)
    _source(db, "testing strategy two", days_ago=7)

    report = build_source_coverage_gaps_report(db, now=NOW)

    assert report.gaps == ()
    assert "No knowledge source coverage gaps" in format_source_coverage_gaps_text(report)


def test_uncovered_themes_get_reason_code_and_action(db):
    _content_with_topic(db, "observability")

    report = build_source_coverage_gaps_report(db, now=NOW)

    assert report.gaps[0].theme == "observability"
    assert "uncovered_theme" in report.gaps[0].reason_codes
    assert "ingest new approved sources" in report.gaps[0].suggested_next_ingestion_action


def test_stale_only_coverage_is_flagged(db):
    _content_with_topic(db, "release")
    _source(db, "release process", days_ago=200)
    _source(db, "release checklist", days_ago=190)

    report = build_source_coverage_gaps_report(db, max_source_age_days=90, now=NOW)

    assert report.gaps[0].matched_source_count == 2
    assert report.gaps[0].freshest_source_age_days == 190
    assert "stale_only_coverage" in report.gaps[0].reason_codes


def test_formatter_and_cli_json_apply_min_gap_filter(db, monkeypatch, capsys):
    _content_with_topic(db, "ai agents")
    _source(db, "ai agents handbook", days_ago=3)
    monkeypatch.setattr(knowledge_source_coverage_gaps_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        knowledge_source_coverage_gaps_script,
        "build_source_coverage_gaps_report",
        lambda db, **kwargs: build_source_coverage_gaps_report(db, now=NOW, **kwargs),
    )

    report = build_source_coverage_gaps_report(db, min_gap_score=1, now=NOW)
    payload = json.loads(format_source_coverage_gaps_json(report))
    exit_code = knowledge_source_coverage_gaps_script.main(["--format", "json", "--min-gap-score", "1"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "knowledge_source_coverage_gaps"
    assert "low_source_count" in payload["gaps"][0]["reason_codes"]
    assert cli_payload["gap_count"] == 1
    assert exit_code == 0
