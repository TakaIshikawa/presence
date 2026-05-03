"""Tests for knowledge/curated source license conflict reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_license_conflicts import (
    build_source_license_conflict_report,
    format_source_license_conflict_json,
    format_source_license_conflict_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_license_conflicts.py"
spec = importlib.util.spec_from_file_location("source_license_conflicts_script", SCRIPT_PATH)
source_license_conflicts_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_license_conflicts_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(
    db,
    *,
    source_type: str = "blog",
    identifier: str,
    license_value: str = "attribution_required",
    feed_url: str | None = None,
    canonical_url: str | None = None,
) -> int:
    db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, license, feed_url, canonical_url, status)
           VALUES (?, ?, ?, ?, ?, ?, 'active')""",
        (source_type, identifier, identifier.title(), license_value, feed_url, canonical_url),
    )
    db.conn.commit()
    return int(
        db.conn.execute(
            "SELECT id FROM curated_sources WHERE source_type = ? AND identifier = ?",
            (source_type, identifier),
        ).fetchone()["id"]
    )


def _knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str = "source-1",
    source_url: str | None = None,
    author: str | None = None,
    license_value: str = "open",
    attribution_required: int = 0,
) -> int:
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            license, attribution_required, approved)
           VALUES (?, ?, ?, ?, 'content', 'insight', ?, ?, 1)""",
        (source_type, source_id, source_url, author, license_value, attribution_required),
    )
    db.conn.commit()
    return int(db.conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def test_exact_identifier_conflict_is_reported(db):
    source_id = _source(db, source_type="x_account", identifier="alice", license_value="restricted")
    knowledge_id = _knowledge(
        db,
        source_type="curated_x",
        source_id="tweet-1",
        author="@Alice",
        license_value="open",
        attribution_required=0,
    )

    report = build_source_license_conflict_report(db, now=NOW)

    assert report.finding_count == 1
    finding = report.findings[0]
    assert finding.severity == "high"
    assert finding.match_type == "identifier"
    assert finding.knowledge_id == knowledge_id
    assert finding.curated_source_id == source_id
    assert finding.knowledge_license == "open"
    assert finding.curated_license == "restricted"
    assert finding.recommended_correction == (
        "Update knowledge metadata to license='restricted' and attribution_required=1."
    )


def test_domain_match_reports_conflict_without_exact_identifier(db):
    _source(db, identifier="example.com", license_value="attribution_required")
    knowledge_id = _knowledge(
        db,
        source_id="article-1",
        source_url="https://www.example.com/posts/launch?ref=x",
        license_value="open",
        attribution_required=0,
    )

    report = build_source_license_conflict_report(db, now=NOW)

    assert report.finding_count == 1
    assert report.findings[0].knowledge_id == knowledge_id
    assert report.findings[0].severity == "medium"
    assert report.findings[0].match_type == "domain"
    assert report.findings[0].reason == "missing_required_attribution"


def test_matching_non_conflicts_are_ignored(db):
    _source(db, identifier="example.com", license_value="attribution_required")
    _knowledge(
        db,
        source_id="https://example.com/posts/launch",
        source_url="https://example.com/posts/launch",
        license_value="attribution_required",
        attribution_required=1,
    )

    report = build_source_license_conflict_report(db, now=NOW)

    assert report.findings == ()
    assert "No source license conflicts found." in format_source_license_conflict_text(report)


def test_duplicate_matches_for_same_knowledge_source_pair_are_suppressed(db):
    _source(
        db,
        identifier="example.com",
        license_value="restricted",
        canonical_url="https://example.com",
    )
    _knowledge(
        db,
        source_id="example.com",
        source_url="https://example.com/posts/launch",
        license_value="open",
        attribution_required=0,
    )

    report = build_source_license_conflict_report(db, now=NOW)

    assert report.finding_count == 1
    assert report.totals["high_count"] == 1


def test_restricted_curated_sources_are_high_when_license_or_attribution_is_open(db):
    _source(db, identifier="restricted.example", license_value="restricted")
    first = _knowledge(
        db,
        source_id="https://restricted.example/a",
        source_url="https://restricted.example/a",
        license_value="attribution_required",
        attribution_required=1,
    )
    second = _knowledge(
        db,
        source_id="https://restricted.example/b",
        source_url="https://restricted.example/b",
        license_value="restricted",
        attribution_required=0,
    )

    report = build_source_license_conflict_report(db, now=NOW)

    assert [finding.knowledge_id for finding in report.findings] == [first, second]
    assert {finding.severity for finding in report.findings} == {"high"}


def test_filters_and_limit_apply_to_curated_sources_and_findings(db):
    _source(db, source_type="blog", identifier="first.example", license_value="restricted")
    _source(db, source_type="newsletter", identifier="second.example", license_value="restricted")
    _source(db, source_type="blog", identifier="open.example", license_value="open")
    first = _knowledge(
        db,
        source_id="https://first.example/a",
        source_url="https://first.example/a",
        license_value="open",
        attribution_required=0,
    )
    _knowledge(
        db,
        source_type="curated_newsletter",
        source_id="https://second.example/a",
        source_url="https://second.example/a",
        license_value="open",
        attribution_required=0,
    )
    _knowledge(
        db,
        source_id="https://open.example/a",
        source_url="https://open.example/a",
        license_value="restricted",
        attribution_required=1,
    )

    report = build_source_license_conflict_report(
        db,
        license_filter="restricted",
        source_type="blog",
        limit=1,
        now=NOW,
    )

    assert report.filters == {"license": "restricted", "source_type": "blog", "limit": 1}
    assert [finding.knowledge_id for finding in report.findings] == [first]


def test_missing_tables_return_empty_report_with_availability_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        report = build_source_license_conflict_report(conn, now=NOW)
    finally:
        conn.close()

    assert report.findings == ()
    assert report.missing_required_tables == ("curated_sources", "knowledge")
    assert report.totals["finding_count"] == 0


def test_cli_json_output(db, monkeypatch, capsys):
    _source(db, identifier="example.com", license_value="restricted")
    _knowledge(
        db,
        source_url="https://example.com/post",
        license_value="open",
        attribution_required=0,
    )
    monkeypatch.setattr(
        source_license_conflicts_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        source_license_conflicts_script,
        "build_source_license_conflict_report",
        lambda db, **kwargs: build_source_license_conflict_report(db, now=NOW, **kwargs),
    )

    exit_code = source_license_conflicts_script.main(
        ["--license", "restricted", "--source-type", "blog", "--format", "json", "--limit", "5"]
    )

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_license_conflicts"
    assert payload["generated_at"] == "2026-05-03T12:00:00+00:00"
    assert payload["finding_count"] == 1
    assert payload["findings"][0]["severity"] == "high"
    assert payload["filters"] == {"license": "restricted", "limit": 5, "source_type": "blog"}


def test_invalid_cli_limit_returns_parse_error(capsys):
    exit_code = source_license_conflicts_script.main(["--limit", "0"])

    assert exit_code == 2
    assert "value must be positive" in capsys.readouterr().err


def test_json_output_is_deterministic(db):
    _source(db, identifier="open.example", license_value="open")
    _knowledge(
        db,
        source_url="https://open.example/post",
        license_value="restricted",
        attribution_required=1,
    )

    report = build_source_license_conflict_report(db, now=NOW)
    payload = json.loads(format_source_license_conflict_json(report))

    assert format_source_license_conflict_json(report) == format_source_license_conflict_json(report)
    assert payload["totals"] == {
        "finding_count": 1,
        "high_count": 0,
        "low_count": 1,
        "medium_count": 0,
    }
