"""Tests for generated-content source attribution gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.generated_source_attribution_gaps import (
    build_generated_source_attribution_gaps_report,
    format_generated_source_attribution_gaps_json,
    format_generated_source_attribution_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generated_source_attribution_gaps.py"
spec = importlib.util.spec_from_file_location("generated_source_attribution_gaps_script", SCRIPT_PATH)
generated_source_attribution_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(generated_source_attribution_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    commits: list[str] | None = None,
    messages: list[str] | None = None,
    activity_ids: list[str] | None = None,
    created_at: str = "2026-05-01T00:00:00+00:00",
    published: int = 0,
    published_url: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=commits or [],
        source_messages=messages or [],
        source_activity_ids=activity_ids or [],
        content="Generated copy",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published = ?, published_url = ?
           WHERE id = ?""",
        (created_at, published, published_url, content_id),
    )
    db.conn.commit()
    return content_id


def test_reports_null_empty_invalid_and_published_source_less_rows(db):
    valid_id = _content(db, commits=["sha"], messages=["msg"], activity_ids=["repo#1:pull_request"])
    empty_id = _content(db)
    null_id = _content(db)
    invalid_id = _content(db)
    published_id = _content(
        db,
        published=1,
        published_url="https://example.test/post",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET source_commits = NULL, source_messages = NULL, source_activity_ids = NULL
           WHERE id = ?""",
        (null_id,),
    )
    db.conn.execute(
        "UPDATE generated_content SET source_commits = ? WHERE id = ?",
        ("[not-json", invalid_id),
    )
    db.conn.commit()

    report = build_generated_source_attribution_gaps_report(db, now=NOW)
    by_id = {row["content_id"]: row for row in report["rows"]}

    assert valid_id not in by_id
    assert by_id[empty_id]["bucket"] == "no_sources"
    assert by_id[null_id]["source_counts"] == {
        "source_commits": 0,
        "source_messages": 0,
        "source_activity_ids": 0,
    }
    assert by_id[invalid_id]["bucket"] == "no_sources"
    assert by_id[published_id]["bucket"] == "published_no_sources"
    assert by_id[published_id]["published"] is True
    assert report["counts"]["by_bucket"]["published_no_sources"] == 1
    assert any("malformed source_commits" in warning for warning in report["warnings"])


def test_buckets_stale_unpublished_source_less_rows(db):
    stale_id = _content(db, created_at="2026-03-01T00:00:00+00:00")
    fresh_id = _content(db, created_at="2026-04-28T00:00:00+00:00")

    report = build_generated_source_attribution_gaps_report(db, days=30, now=NOW)
    by_id = {row["content_id"]: row for row in report["rows"]}

    assert by_id[stale_id]["bucket"] == "stale_unpublished_no_sources"
    assert by_id[stale_id]["recommended_action"] == "Refresh or discard stale unpublished content before reuse."
    assert by_id[fresh_id]["bucket"] == "no_sources"


def test_content_type_filter_limits_rows_without_mutating_unfiltered_totals(db):
    _content(db, content_type="x_post")
    _content(db, content_type="blog_post")
    _content(db, content_type="blog_post", commits=["sha"])

    all_report = build_generated_source_attribution_gaps_report(db, now=NOW)
    blog_report = build_generated_source_attribution_gaps_report(
        db,
        content_type="blog_post",
        now=NOW,
    )

    assert all_report["counts"]["rows_scanned"] == 3
    assert all_report["counts"]["attribution_gaps"] == 2
    assert blog_report["counts"]["rows_scanned"] == 2
    assert blog_report["counts"]["attribution_gaps"] == 1
    assert {row["content_type"] for row in blog_report["rows"]} == {"blog_post"}


def test_json_and_text_output_are_deterministic(db):
    content_id = _content(db, published=1, published_url="https://example.test/post")

    report = build_generated_source_attribution_gaps_report(db, now=NOW)
    payload = json.loads(format_generated_source_attribution_gaps_json(report))
    text = format_generated_source_attribution_gaps_text(report)

    assert list(payload) == sorted(payload)
    assert payload["rows"][0]["content_id"] == content_id
    assert "Generated Source Attribution Gaps" in text
    assert "published=1" in text
    assert f"content_id={content_id}" in text
    assert "Backfill durable source attribution" in text


def test_missing_generated_content_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_generated_source_attribution_gaps_report(conn, now=NOW)

    assert report["counts"]["rows_scanned"] == 0
    assert report["rows"] == []
    assert report["missing_tables"] == ["generated_content"]
    assert "No generated content source attribution gaps found." in format_generated_source_attribution_gaps_text(report)


def test_cli_supports_json_output_and_validation(db, monkeypatch, capsys):
    _content(db)
    monkeypatch.setattr(
        generated_source_attribution_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )

    result = generated_source_attribution_gaps_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["counts"]["attribution_gaps"] == 1

    result = generated_source_attribution_gaps_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert result == 2
    assert "value must be positive" in captured.err
