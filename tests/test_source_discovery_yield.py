"""Tests for curated source discovery yield reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import csv
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_discovery_yield import (
    build_source_discovery_yield_report,
    format_source_discovery_yield_csv,
    format_source_discovery_yield_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_discovery_yield.py"
spec = importlib.util.spec_from_file_location("source_discovery_yield_script", SCRIPT_PATH)
source_discovery_yield_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_discovery_yield_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(
    db,
    *,
    source_type: str = "blog",
    identifier: str,
    discovery_source: str | None = "search",
    status: str = "candidate",
    relevance_score: float | None = 0.5,
    sample_count: int | None = 5,
) -> int:
    source_id = db.insert_candidate_source(
        source_type=source_type,
        identifier=identifier,
        discovery_source=discovery_source or "temporary",
        relevance_score=relevance_score,
        sample_count=sample_count or 0,
    )
    assert source_id is not None
    db.conn.execute(
        """UPDATE curated_sources
           SET status = ?, discovery_source = ?, relevance_score = ?, sample_count = ?
           WHERE id = ?""",
        (status, discovery_source, relevance_score, sample_count, source_id),
    )
    db.conn.commit()
    return int(source_id)


def test_aggregates_review_outcomes_by_discovery_source_and_source_type(db):
    _source(db, identifier="candidate.example", discovery_source="search", status="candidate")
    _source(db, identifier="active.example", discovery_source="search", status="active")
    _source(db, identifier="rejected.example", discovery_source="search", status="rejected")
    _source(db, identifier="paused.example", discovery_source="search", status="paused")
    _source(
        db,
        source_type="newsletter",
        identifier="weekly.example",
        discovery_source="search",
        status="approved",
    )

    report = build_source_discovery_yield_report(db, now=NOW)

    blog = _row(report, "search", "blog")
    assert blog.total_count == 4
    assert blog.candidate_count == 1
    assert blog.active_count == 1
    assert blog.rejected_count == 1
    assert blog.paused_count == 1
    assert blog.reviewed_count == 3
    assert blog.conversion_rate == 0.25

    newsletter = _row(report, "search", "newsletter")
    assert newsletter.active_count == 1
    assert newsletter.reviewed_count == 1
    assert report.totals["total_count"] == 5
    assert report.totals["conversion_rate"] == 0.4


def test_unknown_discovery_source_and_averages_are_deterministic(db):
    _source(
        db,
        identifier="null.example",
        discovery_source=None,
        status="active",
        relevance_score=0.8,
        sample_count=4,
    )
    _source(
        db,
        identifier="blank.example",
        discovery_source="",
        status="candidate",
        relevance_score=0.6,
        sample_count=8,
    )

    report = build_source_discovery_yield_report(db, discovery_source="unknown", now=NOW)
    row = report.rows[0]

    assert row.discovery_source == "unknown"
    assert row.total_count == 2
    assert row.average_relevance_score == 0.7
    assert row.average_sample_count == 6.0
    assert row.conversion_rate == 0.5
    assert report.filters["discovery_source"] == "unknown"


def test_filters_by_source_type_discovery_source_and_min_samples(db):
    _source(
        db,
        source_type="blog",
        identifier="included.example",
        discovery_source="search",
        status="active",
        sample_count=10,
    )
    _source(
        db,
        source_type="blog",
        identifier="too-small.example",
        discovery_source="search",
        status="active",
        sample_count=2,
    )
    _source(
        db,
        source_type="newsletter",
        identifier="wrong-type.example",
        discovery_source="search",
        status="active",
        sample_count=10,
    )
    _source(
        db,
        source_type="blog",
        identifier="wrong-source.example",
        discovery_source="cultivate",
        status="active",
        sample_count=10,
    )

    report = build_source_discovery_yield_report(
        db,
        source_type="blog",
        discovery_source="search",
        min_samples=5,
        now=NOW,
    )

    assert len(report.rows) == 1
    assert report.rows[0].total_count == 1
    assert report.rows[0].discovery_source == "search"
    assert report.rows[0].source_type == "blog"
    assert report.filters["min_samples"] == 5


def test_zero_denominator_and_null_relevance_are_handled(db):
    _source(
        db,
        identifier="other-status.example",
        discovery_source="manual",
        status="ignored_status",
        relevance_score=None,
        sample_count=0,
    )

    report = build_source_discovery_yield_report(db, now=NOW)
    row = report.rows[0]

    assert row.candidate_count == 0
    assert row.reviewed_count == 0
    assert row.average_relevance_score is None
    assert row.average_sample_count == 0.0
    assert row.conversion_rate == 0.0
    assert report.totals["conversion_rate"] == 0.0


def test_csv_and_json_rendering_are_stable(db):
    _source(
        db,
        source_type="x_account",
        identifier="alice",
        discovery_source="following",
        status="active",
        relevance_score=0.9,
        sample_count=12,
    )
    report = build_source_discovery_yield_report(db, now=NOW)

    payload = json.loads(format_source_discovery_yield_json(report))
    rows = list(csv.DictReader(format_source_discovery_yield_csv(report).splitlines()))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "source_discovery_yield"
    assert payload["rows"][0]["discovery_source"] == "following"
    assert rows == [
        {
            "discovery_source": "following",
            "source_type": "x_account",
            "total_count": "1",
            "candidate_count": "0",
            "active_count": "1",
            "rejected_count": "0",
            "paused_count": "0",
            "reviewed_count": "1",
            "average_relevance_score": "0.9",
            "average_sample_count": "12.0",
            "conversion_rate": "1.0",
        }
    ]


def test_missing_curated_sources_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")

    report = build_source_discovery_yield_report(conn, now=NOW)

    assert report.rows == ()
    assert report.missing_tables == ("curated_sources",)
    assert report.totals["total_count"] == 0


def test_cli_json_csv_and_validation(db, capsys, monkeypatch):
    _source(db, source_type="blog", identifier="example.com", discovery_source="search")
    monkeypatch.setattr(
        source_discovery_yield_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        source_discovery_yield_script,
        "build_source_discovery_yield_report",
        lambda db, **kwargs: build_source_discovery_yield_report(db, now=NOW, **kwargs),
    )

    assert source_discovery_yield_script.main(["--min-samples", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    assert source_discovery_yield_script.main(
        [
            "--source-type",
            "blog",
            "--discovery-source",
            "search",
            "--min-samples",
            "1",
            "--format",
            "json",
        ]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["source_type"] == "blog"
    assert payload["filters"]["discovery_source"] == "search"
    assert payload["rows"][0]["total_count"] == 1

    assert source_discovery_yield_script.main(["--format", "csv"]) == 0
    assert "discovery_source,source_type,total_count" in capsys.readouterr().out


def _row(report, discovery_source: str, source_type: str):
    matches = [
        row
        for row in report.rows
        if row.discovery_source == discovery_source and row.source_type == source_type
    ]
    assert len(matches) == 1
    return matches[0]
