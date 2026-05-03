"""Tests for knowledge author coverage reporting."""

from __future__ import annotations

import csv
import importlib.util
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from knowledge.author_coverage_report import (
    MISSING_AUTHOR_LABEL,
    build_knowledge_author_coverage_report,
    format_knowledge_author_coverage_csv,
    format_knowledge_author_coverage_json,
    normalize_author,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "knowledge_author_coverage.py"
)
spec = importlib.util.spec_from_file_location("knowledge_author_coverage_script", SCRIPT_PATH)
knowledge_author_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_author_coverage_script)


def _add_knowledge(
    db,
    *,
    source_id: str,
    author: str | None,
    days_ago: int,
    source_type: str = "curated_x",
) -> int:
    timestamp = (NOW - timedelta(days=days_ago)).isoformat()
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, published_at, ingested_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
        (
            source_type,
            source_id,
            f"https://example.com/{source_id}",
            author,
            f"content {source_id}",
            f"insight {source_id}",
            timestamp,
            timestamp,
            timestamp,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_author_normalization_merges_case_handle_and_url_variants(db):
    ids = [
        _add_knowledge(db, source_id="alice-1", author="@Alice", days_ago=2),
        _add_knowledge(db, source_id="alice-2", author="ALICE", days_ago=4),
        _add_knowledge(db, source_id="alice-3", author="https://x.com/alice", days_ago=6),
        _add_knowledge(db, source_id="bob", author="@bob", days_ago=7),
    ]

    report = build_knowledge_author_coverage_report(
        db,
        days=30,
        min_entries=2,
        dominance_threshold=0.7,
        recent_days=10,
        now=NOW,
    )

    assert normalize_author(" @ALICE ") == "alice"
    assert normalize_author("https://twitter.com/Alice/") == "alice"
    assert report.rows[0].author == "alice"
    assert report.rows[0].status == "dominant"
    assert report.rows[0].entry_count == 3
    assert report.rows[0].recent_entry_count == 3
    assert report.rows[0].knowledge_ids == tuple(ids[:3])
    assert report.rows[1].author == "bob"
    assert report.rows[1].status == "underrepresented"


def test_statuses_use_min_dominance_and_recency_thresholds(db):
    for index in range(4):
        _add_knowledge(db, source_id=f"dominant-{index}", author="Dominant", days_ago=index + 1)
    for index in range(2):
        _add_knowledge(db, source_id=f"inactive-{index}", author="Inactive", days_ago=80 + index)
    for index in range(2):
        _add_knowledge(db, source_id=f"healthy-{index}", author="Healthy", days_ago=index + 3)
    _add_knowledge(db, source_id="missing", author=None, days_ago=1)

    report = build_knowledge_author_coverage_report(
        db,
        days=180,
        min_entries=2,
        dominance_threshold=0.4,
        recent_days=30,
        now=NOW,
    )

    statuses = {row.author: row.status for row in report.rows}
    assert statuses == {
        "dominant": "dominant",
        MISSING_AUTHOR_LABEL: "underrepresented",
        "inactive": "inactive",
        "healthy": "healthy",
    }
    assert report.totals["status_counts"] == {
        "dominant": 1,
        "healthy": 1,
        "inactive": 1,
        "underrepresented": 1,
    }
    assert report.totals["missing_author_entry_count"] == 1


def test_iterable_rows_json_and_csv_are_deterministic():
    rows = [
        {
            "id": 2,
            "source_type": "curated_article",
            "author": "Beta",
            "published_at": (NOW - timedelta(days=80)).isoformat(),
        },
        {
            "id": 1,
            "source_type": "curated_x",
            "author": "@Alpha",
            "published_at": (NOW - timedelta(days=1)).isoformat(),
        },
    ]

    report = build_knowledge_author_coverage_report(
        rows,
        days=90,
        min_entries=1,
        dominance_threshold=0.8,
        recent_days=30,
        now=NOW,
    )
    payload = json.loads(format_knowledge_author_coverage_json(report))
    csv_rows = list(csv.DictReader(format_knowledge_author_coverage_csv(report).splitlines()))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "knowledge_author_coverage"
    assert [row["author"] for row in csv_rows] == ["beta", "alpha"]
    assert csv_rows[0]["status"] == "inactive"
    assert csv_rows[0]["source_types"] == "curated_article"


def test_missing_table_and_cli_json_csv_and_validation(capsys, tmp_path):
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    report = build_knowledge_author_coverage_report(empty, now=NOW)
    assert report.rows == ()
    assert report.missing_tables == ("knowledge",)

    db_path = tmp_path / "knowledge.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            author TEXT,
            approved INTEGER,
            published_at TEXT
        );
        INSERT INTO knowledge (id, source_type, author, approved, published_at)
        VALUES (1, 'curated_x', '@CLI', 1, '2026-04-30T12:00:00+00:00');
        """
    )
    conn.close()

    assert knowledge_author_coverage_script.main(["--db", str(db_path), "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["author"] == "cli"

    assert knowledge_author_coverage_script.main(["--db", str(db_path), "--format", "csv"]) == 0
    assert capsys.readouterr().out.splitlines()[0].startswith("author,display_author,status")

    assert knowledge_author_coverage_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
