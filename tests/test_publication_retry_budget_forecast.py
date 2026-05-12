"""Tests for publication retry budget forecasting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.publication_retry_budget import (
    build_publication_retry_budget_report,
    format_publication_retry_budget_json,
    format_publication_retry_budget_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_retry_budget.py"
spec = importlib.util.spec_from_file_location("publication_retry_budget_script", SCRIPT_PATH)
publication_retry_budget_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_retry_budget_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Retry budget post") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.commit()
    return content_id


def _at(hours: int) -> str:
    return (NOW + timedelta(hours=hours)).isoformat()


def _publication(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "failed",
    attempt_count: int = 1,
    last_error_hours: int = -1,
    next_retry_hours: int | None = None,
    error: str = "gateway timeout",
    error_category: str | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            status,
            error,
            error_category,
            attempt_count,
            _at(next_retry_hours) if next_retry_hours is not None else None,
            _at(last_error_hours),
            _at(last_error_hours),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_failed_rows_rank_by_remaining_attempts_then_oldest_error(db):
    first = _content(db, "first exhausted")
    second = _content(db, "second exhausted older")
    third = _content(db, "has room")
    _publication(db, third, attempt_count=1, last_error_hours=-100)
    _publication(db, first, attempt_count=3, last_error_hours=-2)
    _publication(db, second, platform="bluesky", attempt_count=4, last_error_hours=-5)

    report = build_publication_retry_budget_report(db, now=NOW)

    assert [row.content_id for row in report.rows] == [second, first, third]
    assert report.rows[0].remaining_attempts == 0
    assert report.rows[0].budget_status == "exhausted"
    assert report.rows[1].budget_status == "exhausted"
    assert report.rows[2].remaining_attempts == 2


def test_future_next_retry_rows_are_included_but_marked_not_due(db):
    content_id = _content(db)
    _publication(
        db,
        content_id,
        attempt_count=2,
        next_retry_hours=3,
        error="Rate limit exceeded",
    )

    report = build_publication_retry_budget_report(db, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.content_id == content_id
    assert row.due_status == "not_due"
    assert row.normalized_error_category == "rate_limit"
    assert row.remaining_attempts == 1


def test_summary_groups_by_platform_and_normalized_category(db):
    x_content = _content(db)
    bluesky_content = _content(db)
    _publication(db, x_content, platform="x", attempt_count=3, error="unauthorized")
    _publication(db, bluesky_content, platform="bluesky", attempt_count=1, error_category="media")

    report = build_publication_retry_budget_report(db, now=NOW)
    payload = json.loads(format_publication_retry_budget_json(report))
    text = format_publication_retry_budget_text(report)

    assert "x" in payload["summary_by_platform"]
    assert "auth=1" in payload["summary_by_platform"]["x"]
    assert "media=1" in payload["summary_by_platform"]["bluesky"]
    assert "Publication Retry Budget Forecast" in text


def test_published_rows_are_excluded(db):
    content_id = _content(db)
    _publication(db, content_id, status="published", attempt_count=3)

    report = build_publication_retry_budget_report(db, now=NOW)

    assert report.rows == ()


def test_cli_outputs_json(db, capsys, monkeypatch):
    _publication(db, _content(db), attempt_count=3)
    monkeypatch.setattr(publication_retry_budget_script, "script_context", lambda: _script_context(db))

    assert publication_retry_budget_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "publication_retry_budget"
    assert payload["rows"][0]["budget_status"] == "exhausted"


def test_invalid_limit_raises():
    with pytest.raises(ValueError, match="limit"):
        build_publication_retry_budget_report(SimpleNamespace(conn=None), limit=0)
