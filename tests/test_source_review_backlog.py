"""Tests for curated source review backlog reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_review_backlog import (
    AGING_BUCKET,
    FRESH_BUCKET,
    OVERDUE_BUCKET,
    build_source_review_backlog_report,
    format_source_review_backlog_json,
    format_source_review_backlog_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_review_backlog.py"
spec = importlib.util.spec_from_file_location("source_review_backlog_script", SCRIPT_PATH)
source_review_backlog_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_review_backlog_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(
    db,
    *,
    source_type: str = "blog",
    identifier: str,
    status: str = "candidate",
    discovery_source: str = "proactive_mining",
    created_at: datetime | None = NOW,
) -> int:
    source_id = db.insert_candidate_source(
        source_type=source_type,
        identifier=identifier,
        discovery_source=discovery_source,
    )
    assert source_id is not None
    db.conn.execute(
        """UPDATE curated_sources
           SET status = ?, created_at = ?
           WHERE id = ?""",
        (status, created_at.isoformat() if created_at else None, source_id),
    )
    db.conn.commit()
    return int(source_id)


def test_missing_curated_sources_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")

    report = build_source_review_backlog_report(conn, now=NOW)

    assert report.findings == ()
    assert report.oldest_created_at is None
    assert report.missing_tables == ("curated_sources",)
    assert report.totals["finding_count"] == 0
    assert "curated_sources" in format_source_review_backlog_text(report)


def test_status_filtering_only_includes_pending_like_sources(db):
    _source(db, identifier="candidate.example", status="candidate")
    _source(db, identifier="pending.example", status="pending")
    _source(db, identifier="needs.example", status="needs_review")
    for status in [
        "active",
        "approved",
        "rejected",
        "quarantined",
        "retired",
        "inactive",
        "paused",
    ]:
        _source(db, identifier=f"{status}.example", status=status)

    report = build_source_review_backlog_report(db, now=NOW)

    assert [finding.identifier for finding in report.findings] == [
        "candidate.example",
        "needs.example",
        "pending.example",
    ]
    assert report.totals["finding_count"] == 3
    assert report.totals["by_source_type"] == {"blog": 3}
    assert report.totals["by_discovery_source"] == {"proactive_mining": 3}


def test_age_buckets_and_oldest_created_at_are_deterministic(db):
    _source(
        db,
        identifier="overdue.example",
        discovery_source="search",
        created_at=NOW - timedelta(days=45),
    )
    _source(
        db,
        identifier="aging.example",
        discovery_source="cultivate",
        created_at=NOW - timedelta(days=10),
    )
    _source(
        db,
        identifier="fresh.example",
        discovery_source="proactive_mining",
        created_at=NOW - timedelta(days=2),
    )

    report = build_source_review_backlog_report(db, days=90, now=NOW)

    assert [finding.identifier for finding in report.findings] == [
        "overdue.example",
        "aging.example",
        "fresh.example",
    ]
    assert [finding.age_bucket for finding in report.findings] == [
        OVERDUE_BUCKET,
        AGING_BUCKET,
        FRESH_BUCKET,
    ]
    assert report.findings[0].age_days == 45
    assert report.oldest_created_at == (NOW - timedelta(days=45)).isoformat()
    assert report.totals["by_age_bucket"] == {
        FRESH_BUCKET: 1,
        AGING_BUCKET: 1,
        OVERDUE_BUCKET: 1,
    }


def test_days_and_source_type_filters_limit_scope(db):
    blog_id = _source(
        db,
        source_type="blog",
        identifier="recent.example",
        created_at=NOW - timedelta(days=5),
    )
    _source(
        db,
        source_type="blog",
        identifier="old.example",
        created_at=NOW - timedelta(days=95),
    )
    _source(
        db,
        source_type="x_account",
        identifier="alice",
        created_at=NOW - timedelta(days=5),
    )

    report = build_source_review_backlog_report(
        db,
        days=30,
        source_type="blog",
        now=NOW,
    )

    assert [finding.source_id for finding in report.findings] == [blog_id]
    assert report.filters["source_type"] == "blog"
    assert report.totals["by_source_type"] == {"blog": 1}


def test_formatters_emit_stable_text_and_json(db):
    source_id = _source(
        db,
        source_type="newsletter",
        identifier="weekly.dev",
        discovery_source="search",
        created_at=NOW - timedelta(days=31),
    )
    report = build_source_review_backlog_report(db, source_type="newsletter", now=NOW)

    text = format_source_review_backlog_text(report)
    payload = json.loads(format_source_review_backlog_json(report))

    assert "Curated Source Review Backlog" in text
    assert "newsletter:weekly.dev" in text
    assert "By age_bucket: overdue=1" in text
    assert "action: Escalate source review" in text
    assert payload["artifact_type"] == "source_review_backlog"
    assert payload["findings"][0]["source_id"] == source_id
    assert payload["findings"][0]["age_bucket"] == OVERDUE_BUCKET
    assert format_source_review_backlog_json(report) == format_source_review_backlog_json(report)


def test_cli_json_output(db, capsys, monkeypatch):
    _source(db, source_type="x_account", identifier="alice")
    _source(db, source_type="blog", identifier="example.com")
    monkeypatch.setattr(
        source_review_backlog_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert source_review_backlog_script.main(
        ["--source-type", "x_account", "--days", "30", "--format", "json"]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["source_type"] == "x_account"
    assert payload["findings"][0]["identifier"] == "alice"
