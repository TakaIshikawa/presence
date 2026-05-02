"""Tests for unsupported claim-check digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.claim_check_unsupported_digest import (
    build_claim_check_unsupported_digest,
    format_claim_check_unsupported_digest_json,
    format_claim_check_unsupported_digest_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claim_check_unsupported_digest.py"
)
spec = importlib.util.spec_from_file_location(
    "claim_check_unsupported_digest_script",
    SCRIPT_PATH,
)
claim_check_unsupported_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claim_check_unsupported_digest_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    created_at: datetime = NOW,
    supported_count: int = 1,
    unsupported_count: int = 1,
    annotation_text: str | None = "metric: Unsupported claim not found in sources",
    updated_at: datetime = NOW,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} copy",
        eval_score=8.0,
        eval_feedback="ok",
        claim_check_summary={
            "supported_count": supported_count,
            "unsupported_count": unsupported_count,
            "annotation_text": annotation_text,
        },
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.execute(
        "UPDATE content_claim_checks SET created_at = ?, updated_at = ? WHERE content_id = ?",
        (updated_at.isoformat(), updated_at.isoformat(), content_id),
    )
    db.conn.commit()
    return int(content_id)


def test_digest_includes_unsupported_counts_and_annotation_only_findings(db):
    counted = _content(
        db,
        content_type="x_thread",
        supported_count=2,
        unsupported_count=2,
        annotation_text="metric: Unsupported conversion lift",
    )
    annotation_only = _content(
        db,
        content_type="blog_post",
        supported_count=3,
        unsupported_count=0,
        annotation_text="Needs evidence for the adoption claim.",
    )
    supported = _content(
        db,
        supported_count=4,
        unsupported_count=0,
        annotation_text="All claims supported.",
    )

    report = build_claim_check_unsupported_digest(db, days=30, now=NOW)
    payload = json.loads(format_claim_check_unsupported_digest_json(report))
    text = format_claim_check_unsupported_digest_text(report)

    assert [row.content_id for row in report.rows] == [counted, annotation_only]
    assert supported not in {row.content_id for row in report.rows}
    assert payload["artifact_type"] == "claim_check_unsupported_digest"
    assert payload["rows"][0]["content_id"] == counted
    assert payload["rows"][0]["unsupported_count"] == 2
    assert payload["rows"][0]["supported_count"] == 2
    assert "suggested_action" in payload["rows"][0]
    assert payload["buckets"] == {"blog_post": {"medium": 1}, "x_thread": {"high": 1}}
    assert f"content_id={counted}" in text
    assert "action:" in text


def test_rows_are_ordered_by_severity_updated_desc_then_content_id(db):
    high_old = _content(db, unsupported_count=1, updated_at=NOW - timedelta(hours=3))
    critical = _content(db, unsupported_count=3, updated_at=NOW - timedelta(days=1))
    high_new = _content(db, unsupported_count=1, updated_at=NOW - timedelta(hours=1))
    same_time_low_id = _content(db, unsupported_count=1, updated_at=NOW - timedelta(hours=1))

    report = build_claim_check_unsupported_digest(db, days=30, now=NOW)

    assert [row.content_id for row in report.rows] == [
        critical,
        high_new,
        same_time_low_id,
        high_old,
    ]
    assert report.rows[0].severity == "critical"


def test_days_and_limit_are_applied(db):
    recent = _content(db, updated_at=NOW - timedelta(days=2))
    _content(db, updated_at=NOW - timedelta(days=20))

    report = build_claim_check_unsupported_digest(db, days=7, limit=1, now=NOW)

    assert [row.content_id for row in report.rows] == [recent]
    assert report.filters["limit"] == 1


def test_missing_and_partial_schema_returns_structured_report():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row

    missing = build_claim_check_unsupported_digest(empty, now=NOW)

    assert missing.rows == ()
    assert missing.missing_tables == ("generated_content", "content_claim_checks")
    assert "Missing tables: generated_content, content_claim_checks" in (
        format_claim_check_unsupported_digest_text(missing)
    )

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT);
        CREATE TABLE content_claim_checks (
            content_id INTEGER PRIMARY KEY,
            annotation_text TEXT
        );
        INSERT INTO generated_content (id, content_type) VALUES (1, 'x_post');
        INSERT INTO content_claim_checks (content_id, annotation_text)
        VALUES (1, 'unsupported claim needs evidence');
        """
    )

    report = build_claim_check_unsupported_digest(partial, now=NOW)

    assert report.rows == ()
    assert report.missing_columns["generated_content"] == ("created_at",)
    assert report.missing_columns["content_claim_checks"] == (
        "created_at",
        "supported_count",
        "unsupported_count",
        "updated_at",
    )


def test_invalid_args_raise_value_error(db):
    with pytest.raises(ValueError, match="days must be positive"):
        build_claim_check_unsupported_digest(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_claim_check_unsupported_digest(db, limit=0, now=NOW)


def test_cli_outputs_text_and_json_for_valid_input(db, monkeypatch, capsys):
    _content(db, unsupported_count=1)
    monkeypatch.setattr(
        claim_check_unsupported_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claim_check_unsupported_digest_script,
        "build_claim_check_unsupported_digest",
        lambda db, **kwargs: build_claim_check_unsupported_digest(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert claim_check_unsupported_digest_script.main(["--days", "7", "--limit", "5"]) == 0
    assert "Unsupported Claim-check Digest" in capsys.readouterr().out

    assert claim_check_unsupported_digest_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["row_count"] == 1
