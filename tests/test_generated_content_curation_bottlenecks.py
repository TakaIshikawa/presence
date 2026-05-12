"""Tests for generated content curation bottleneck reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.generated_content_curation_bottlenecks import (
    build_generated_content_curation_bottlenecks_report,
    format_generated_content_curation_bottlenecks_json,
    format_generated_content_curation_bottlenecks_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "generated_content_curation_bottlenecks.py"
)
spec = importlib.util.spec_from_file_location(
    "generated_content_curation_bottlenecks_script",
    SCRIPT_PATH,
)
generated_content_curation_bottlenecks_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(generated_content_curation_bottlenecks_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    created_at: datetime,
    content_type: str = "x_post",
    eval_score: float | None = 7.0,
    curation_quality: str | None = None,
    published: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} copy",
        eval_score=eval_score,
        eval_feedback="review",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, curation_quality = ?, published = ?
           WHERE id = ?""",
        (created_at.isoformat(), curation_quality, published, content_id),
    )
    db.conn.commit()
    return content_id


def test_empty_db_has_stable_report(db):
    report = build_generated_content_curation_bottlenecks_report(db, days=7, now=NOW)
    payload = report.to_dict()

    assert payload["artifact_type"] == "generated_content_curation_bottlenecks"
    assert report.bottlenecks == ()
    assert report.totals["rows_scanned"] == 0
    assert report.age_buckets == {
        "0-1d": 0,
        "1-3d": 0,
        "3-7d": 0,
        "7-14d": 0,
        "14d+": 0,
        "unknown": 0,
    }


def test_groups_unpublished_rows_by_curation_score_type_and_age(db):
    first = _content(db, created_at=NOW - timedelta(hours=6), eval_score=8.5)
    second = _content(db, created_at=NOW - timedelta(hours=8), eval_score=9.0)
    medium = _content(
        db,
        created_at=NOW - timedelta(days=2),
        content_type="x_thread",
        eval_score=7.0,
        curation_quality="too_specific",
    )
    low = _content(db, created_at=NOW - timedelta(days=9), eval_score=5.5)
    _content(db, created_at=NOW - timedelta(hours=2), eval_score=9.5, published=1)
    _content(db, created_at=NOW - timedelta(hours=3), eval_score=9.5, published=-1)

    report = build_generated_content_curation_bottlenecks_report(db, days=14, now=NOW)
    payload = json.loads(format_generated_content_curation_bottlenecks_json(report))
    text = format_generated_content_curation_bottlenecks_text(report)

    assert payload["totals"]["rows_scanned"] == 4
    assert payload["totals"]["curation_state_counts"] == {
        "too_specific": 1,
        "unreviewed": 3,
    }
    assert payload["age_buckets"]["0-1d"] == 2
    assert payload["age_buckets"]["1-3d"] == 1
    assert payload["age_buckets"]["7-14d"] == 1
    assert payload["bottlenecks"][0] == {
        "age_bucket": "0-1d",
        "content_type": "x_post",
        "count": 2,
        "curation_state": "unreviewed",
        "eval_score_band": "high",
        "newest_created_at": (NOW - timedelta(hours=6)).isoformat(),
        "oldest_created_at": (NOW - timedelta(hours=8)).isoformat(),
        "representative_content_ids": [second, first],
    }
    assert {tuple(group["representative_content_ids"]) for group in payload["bottlenecks"]} >= {
        (medium,),
        (low,),
    }
    assert f"content_ids={second}, {first}" in text
    assert "curation=unreviewed score=high type=x_post age=0-1d count=2" in text


def test_null_eval_score_and_missing_columns_are_tolerated():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT,
            content TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO generated_content (content_type, content, created_at) VALUES (?, ?, ?)",
        ("blog_post", "Needs review", NOW.isoformat()),
    )
    conn.commit()

    try:
        report = build_generated_content_curation_bottlenecks_report(conn, days=7, now=NOW)
    finally:
        conn.close()

    assert report.totals["rows_scanned"] == 1
    assert report.bottlenecks[0].curation_state == "unreviewed"
    assert report.bottlenecks[0].eval_score_band == "unscored"
    assert report.missing_columns["generated_content"] == (
        "curation_quality",
        "eval_score",
        "published",
    )


def test_limit_controls_scanned_rows(db):
    first = _content(db, created_at=NOW - timedelta(days=3), eval_score=8.0)
    _content(db, created_at=NOW - timedelta(days=2), eval_score=8.0)
    _content(db, created_at=NOW - timedelta(days=1), eval_score=8.0)

    report = build_generated_content_curation_bottlenecks_report(
        db,
        days=7,
        limit=1,
        now=NOW,
    )

    assert report.totals["rows_scanned"] == 1
    assert report.bottlenecks[0].representative_content_ids == (first,)


def test_cli_supports_json_output(db, monkeypatch, capsys):
    content_id = _content(db, created_at=NOW - timedelta(hours=4), eval_score=8.0)
    monkeypatch.setattr(
        generated_content_curation_bottlenecks_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        generated_content_curation_bottlenecks_script,
        "build_generated_content_curation_bottlenecks_report",
        lambda db, **kwargs: build_generated_content_curation_bottlenecks_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = generated_content_curation_bottlenecks_script.main(
        ["--days", "7", "--limit", "5", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["limit"] == 5
    assert payload["bottlenecks"][0]["representative_content_ids"] == [content_id]


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(
        generated_content_curation_bottlenecks_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        generated_content_curation_bottlenecks_script,
        "build_generated_content_curation_bottlenecks_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    exit_code = generated_content_curation_bottlenecks_script.main([])

    assert exit_code == 1
    assert "error: db failed" in capsys.readouterr().err


def test_cli_rejects_invalid_positive_ints():
    with pytest.raises(SystemExit):
        generated_content_curation_bottlenecks_script.parse_args(["--days", "0"])
    with pytest.raises(SystemExit):
        generated_content_curation_bottlenecks_script.parse_args(["--limit", "-1"])
