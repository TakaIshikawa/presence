"""Tests for content variant platform coverage gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_variant_platform_coverage_gaps import (
    build_content_variant_platform_coverage_gaps_report,
    build_content_variant_platform_coverage_gaps_report_from_db,
    format_content_variant_platform_coverage_gaps_json,
    format_content_variant_platform_coverage_gaps_text,
    parse_expected_pair,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_platform_coverage_gaps.py"
spec = importlib.util.spec_from_file_location("content_variant_platform_coverage_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn(path: str | Path = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            status TEXT,
            abandoned INTEGER,
            created_at TEXT
        );
        CREATE TABLE content_variants (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            variant_type TEXT,
            content TEXT
        );"""
    )
    return conn


def _content(
    conn: sqlite3.Connection,
    *,
    content_id: int,
    content_type: str = "article",
    status: str = "draft",
    abandoned: int = 0,
    days_ago: float = 1,
) -> None:
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?)",
        (content_id, content_type, status, abandoned, _ts(days_ago)),
    )
    conn.commit()


def _variant(conn: sqlite3.Connection, *, content_id: int, platform: str, variant_type: str) -> None:
    conn.execute(
        "INSERT INTO content_variants (content_id, platform, variant_type, content) VALUES (?, ?, ?, ?)",
        (content_id, platform, variant_type, f"{platform} {variant_type}"),
    )
    conn.commit()


def test_builder_reports_missing_matrix_pairs_and_grouped_summaries():
    report = build_content_variant_platform_coverage_gaps_report(
        [
            {
                "content_id": 1,
                "content_type": "article",
                "created_at": "2026-05-01 00:00:00",
                "variants": [{"platform": "x", "variant_type": "post"}],
            },
            {
                "content_id": 2,
                "content_type": "article",
                "created_at": "2026-05-01 00:00:00",
                "variants": [
                    {"platform": "x", "variant_type": "post"},
                    {"platform": "newsletter", "variant_type": "summary"},
                ],
            },
        ],
        expected_matrix=[("x", "post"), ("newsletter", "summary")],
        now=NOW,
    )

    assert report["artifact_type"] == "content_variant_platform_coverage_gaps"
    assert report["summary"]["content_rows_scanned"] == 2
    assert report["total_missing_items"] == 1
    assert report["missing_variant_items"][0]["content_id"] == 1
    assert report["missing_variant_items"][0]["missing_variants"] == [
        {"platform": "newsletter", "variant_type": "summary"}
    ]
    assert report["grouped_summaries"]["missing_by_expected_pair"] == {"newsletter:summary": 1}


def test_db_adapter_filters_recent_non_abandoned_content_and_content_type():
    conn = _conn()
    _content(conn, content_id=1, content_type="article", days_ago=1)
    _variant(conn, content_id=1, platform="x", variant_type="post")
    _content(conn, content_id=2, content_type="article", days_ago=40)
    _content(conn, content_id=3, content_type="article", status="abandoned", days_ago=1)
    _content(conn, content_id=4, content_type="article", abandoned=1, days_ago=1)
    _content(conn, content_id=5, content_type="note", days_ago=1)

    report = build_content_variant_platform_coverage_gaps_report_from_db(
        conn,
        expected_matrix=[("x", "post"), ("blog", "post")],
        content_type="article",
        lookback_days=7,
        now=NOW,
    )

    assert report["summary"]["content_rows_scanned"] == 1
    assert report["missing_variant_items"][0]["content_id"] == 1
    assert report["missing_variant_items"][0]["missing_variants"] == [{"platform": "blog", "variant_type": "post"}]


def test_missing_tables_return_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_variant_platform_coverage_gaps_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["generated_content", "content_variants"]
    assert report["missing_variant_items"] == []
    assert report["summary"]["content_rows_scanned"] == 0


def test_formatters_and_expected_pair_parser_are_stable():
    assert parse_expected_pair("X:Post") == ("x", "post")
    with pytest.raises(ValueError, match="platform:variant_type"):
        parse_expected_pair("x")

    report = build_content_variant_platform_coverage_gaps_report(
        [{"content_id": 1, "content_type": "post", "variants": []}],
        expected_matrix=[("x", "post")],
        now=NOW,
    )
    payload = json.loads(format_content_variant_platform_coverage_gaps_json(report))
    assert list(payload) == sorted(payload)
    assert payload["expected_matrix"] == [{"platform": "x", "variant_type": "post"}]
    text = format_content_variant_platform_coverage_gaps_text(report)
    assert "Content Variant Platform Coverage Gaps" in text
    assert "missing=x:post" in text


def test_cli_supports_db_json_text_context_expect_and_validation(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "coverage.sqlite"
    conn = _conn(db_path)
    _content(conn, content_id=1)
    _variant(conn, content_id=1, platform="x", variant_type="post")
    conn.close()

    assert script.main(["--db", str(db_path), "--expect", "x:post", "--expect", "blog:post", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["total_missing_items"] == 1
    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "5"]) == 0
    assert "Content Variant Platform Coverage Gaps" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_variant_items"] == []
    assert script.main(["--expect", "bad"]) == 2
    assert "platform:variant_type" in capsys.readouterr().err


def test_builder_rejects_invalid_thresholds():
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_content_variant_platform_coverage_gaps_report([], lookback_days=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_variant_platform_coverage_gaps_report([], limit=0)
    with pytest.raises(ValueError, match="at least one expected"):
        build_content_variant_platform_coverage_gaps_report([], expected_matrix=[])
