"""Tests for curated source license summary reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.source_license_summary import (
    build_source_license_summary_report,
    format_source_license_summary_json,
    format_source_license_summary_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_license_summary.py"
spec = importlib.util.spec_from_file_location("source_license_summary", SCRIPT_PATH)
source_license_summary = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_license_summary)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(
    db,
    *,
    source_type: str,
    identifier: str,
    license_value: str | None = "attribution_required",
    reviewed_days_ago: int | None = 1,
) -> int:
    db.sync_config_sources(
        [{"identifier": identifier, "name": identifier.title(), "license": license_value}],
        source_type,
    )
    reviewed_at = (
        (NOW - timedelta(days=reviewed_days_ago)).isoformat()
        if reviewed_days_ago is not None
        else None
    )
    db.conn.execute(
        """UPDATE curated_sources
           SET reviewed_at = ?, last_success_at = ?
           WHERE source_type = ? AND identifier = ?""",
        (reviewed_at, reviewed_at, source_type, identifier),
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
    source_type: str,
    source_id: str,
    source_url: str | None = None,
    author: str | None = None,
) -> None:
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved)
           VALUES (?, ?, ?, ?, ?, ?, 1)""",
        (source_type, source_id, source_url, author, "source material", "insight"),
    )
    db.conn.commit()


def test_report_lists_license_reuse_attribution_stale_and_item_counts(db):
    _source(db, source_type="x_account", identifier="alice", license_value="open")
    _source(
        db,
        source_type="blog",
        identifier="example.com",
        license_value="attribution_required",
        reviewed_days_ago=120,
    )
    _source(db, source_type="newsletter", identifier="weekly.dev", license_value="restricted")
    _knowledge(db, source_type="curated_x", source_id="tweet-1", author="@alice")
    _knowledge(
        db,
        source_type="curated_article",
        source_id="https://example.com/post",
        source_url="https://example.com/post",
    )

    report = build_source_license_summary_report(db, stale_after_days=90, now=NOW)
    rows = {row.identifier: row for row in report.sources}

    assert rows["alice"].license_label == "open"
    assert rows["alice"].reuse_allowed is True
    assert rows["alice"].attribution_required is False
    assert rows["alice"].stale_license is False
    assert rows["alice"].item_count == 1
    assert rows["alice"].blocker_reason is None

    assert rows["example.com"].license_label == "attribution_required"
    assert rows["example.com"].reuse_allowed is False
    assert rows["example.com"].attribution_required is True
    assert rows["example.com"].stale_license is True
    assert rows["example.com"].item_count == 1
    assert rows["example.com"].blocker_reason == "stale_license_review"

    assert rows["weekly.dev"].license_label == "restricted"
    assert rows["weekly.dev"].reuse_allowed is False
    assert rows["weekly.dev"].attribution_required is True
    assert rows["weekly.dev"].blocker_reason == "restricted_license"
    assert report.totals == {
        "source_count": 3,
        "reuse_allowed_count": 1,
        "attribution_required_count": 2,
        "stale_license_count": 1,
        "blocked_count": 2,
        "item_count": 2,
    }


def test_source_type_filter_works_without_optional_knowledge_table():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE curated_sources (
            id INTEGER PRIMARY KEY,
            source_type TEXT NOT NULL,
            identifier TEXT NOT NULL,
            license TEXT,
            reviewed_at TEXT
        )"""
    )
    conn.executemany(
        "INSERT INTO curated_sources (source_type, identifier, license, reviewed_at) VALUES (?, ?, ?, ?)",
        [
            ("blog", "example.com", "open", NOW.isoformat()),
            ("x_account", "alice", "open", NOW.isoformat()),
        ],
    )

    try:
        report = build_source_license_summary_report(
            conn,
            source_type="blog",
            stale_after_days=30,
            now=NOW,
        )
    finally:
        conn.close()

    assert [row.identifier for row in report.sources] == ["example.com"]
    assert report.sources[0].item_count == 0
    assert report.missing_optional_tables == ("knowledge",)
    assert "curated_sources" in report.missing_optional_columns


def test_unknown_and_missing_licenses_are_explicit_blockers(db):
    _source(db, source_type="x_account", identifier="mystery", license_value="fair_use")
    _source(db, source_type="blog", identifier="unset.example", license_value=None)

    report = build_source_license_summary_report(db, now=NOW)
    rows = {row.identifier: row for row in report.sources}

    assert rows["mystery"].license_label == "fair_use"
    assert rows["mystery"].reuse_allowed is False
    assert rows["mystery"].blocker_reason == "unknown_license"
    assert rows["unset.example"].license_label == "unknown"
    assert rows["unset.example"].blocker_reason == "unknown_license"


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    _source(db, source_type="blog", identifier="example.com", license_value="open")
    report = build_source_license_summary_report(db, source_type="blog", now=NOW)

    assert format_source_license_summary_json(report) == format_source_license_summary_json(report)
    payload = json.loads(format_source_license_summary_json(report))
    assert sorted(payload) == [
        "filters",
        "generated_at",
        "missing_optional_columns",
        "missing_optional_tables",
        "missing_required_tables",
        "sources",
        "totals",
    ]
    assert payload["sources"][0]["license_label"] == "open"
    text = format_source_license_summary_text(report)
    assert "Source License Summary" in text
    assert "license=open" in text
    assert "reuse_allowed=yes" in text

    with patch.object(
        source_license_summary,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        source_license_summary,
        "build_source_license_summary_report",
        wraps=lambda db, **kwargs: build_source_license_summary_report(db, now=NOW, **kwargs),
    ):
        exit_code = source_license_summary.main(
            ["--source-type", "blog", "--stale-after-days", "90", "--format", "json"]
        )

    assert exit_code == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["source_type"] == "blog"
    assert cli_payload["totals"]["source_count"] == 1


def test_missing_curated_sources_returns_stable_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        report = build_source_license_summary_report(conn, now=NOW)
    finally:
        conn.close()

    assert report.sources == ()
    assert report.missing_required_tables == ("curated_sources",)
    assert report.totals["source_count"] == 0
    assert "Missing required tables: curated_sources" in format_source_license_summary_text(report)
