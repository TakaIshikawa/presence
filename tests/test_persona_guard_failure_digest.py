"""Tests for persona guard failure digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.persona_guard_failure_digest import (
    build_persona_guard_failure_digest,
    format_persona_guard_failure_digest_json,
    format_persona_guard_failure_digest_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "persona_guard_failure_digest.py"
spec = importlib.util.spec_from_file_location("persona_guard_failure_digest_script", SCRIPT_PATH)
persona_guard_failure_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(persona_guard_failure_digest_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, content_type: str = "x_post", created_at: datetime | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=7.0,
        eval_feedback="ok",
    )
    if created_at:
        db.conn.execute(
            "UPDATE generated_content SET created_at = ? WHERE id = ?",
            (created_at.isoformat(), content_id),
        )
        db.conn.commit()
    return content_id


def _guard(
    db,
    content_id: int,
    *,
    checked: int = 1,
    passed: int = 0,
    status: str = "failed",
    score: float = 0.4,
    reasons: object = None,
    metrics: object = None,
    checked_at: datetime | None = None,
) -> None:
    reasons_text = reasons if isinstance(reasons, str) else json.dumps(reasons if reasons is not None else [])
    metrics_text = metrics if isinstance(metrics, str) else json.dumps(metrics if metrics is not None else {})
    db.conn.execute(
        """INSERT INTO content_persona_guard
           (content_id, checked, passed, status, score, reasons, metrics, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            checked,
            passed,
            status,
            score,
            reasons_text,
            metrics_text,
            (checked_at or NOW).isoformat(),
            (checked_at or NOW).isoformat(),
        ),
    )
    db.conn.commit()


def test_failed_rows_are_emitted_with_operator_fields_and_sorted(db):
    older = _content(db, "Old failure " * 30, content_type="blog_post")
    newer = _content(db, "New failure with too much generic abstraction", content_type="x_thread")
    passing = _content(db, "Authentic passing copy", content_type="x_post")

    _guard(
        db,
        older,
        status="failed",
        score=0.35,
        reasons=["too generic"],
        metrics={"abstraction_ratio": 0.8},
        checked_at=NOW - timedelta(hours=5),
    )
    _guard(
        db,
        newer,
        status="failed",
        score=0.2,
        reasons=["weak phrase overlap"],
        metrics={"phrase_overlap": 0.1},
        checked_at=NOW - timedelta(hours=1),
    )
    _guard(db, passing, passed=1, status="passed", score=0.92, checked_at=NOW)

    report = build_persona_guard_failure_digest(db, now=NOW)

    assert [row.content_id for row in report.rows] == [newer, older]
    row = report.rows[0]
    assert row.content_id == newer
    assert row.status == "failed"
    assert row.score == 0.2
    assert row.reasons == ["weak phrase overlap"]
    assert row.metrics == {"phrase_overlap": 0.1}
    assert row.content_excerpt == "New failure with too much generic abstraction"
    assert report.totals["by_status"] == {"failed": 2}
    assert report.totals["by_content_type"] == {"blog_post": 1, "x_thread": 1}


def test_passing_rows_above_threshold_are_excluded_but_borderline_rows_are_included(db):
    low_pass = _content(db, "Technically passed but close to the threshold")
    high_pass = _content(db, "Clearly passing persona match")
    status_mismatch = _content(db, "Passed flag with non-passing status")
    unchecked_failure = _content(db, "Unchecked failure")

    _guard(db, low_pass, passed=1, status="passed", score=0.69)
    _guard(db, high_pass, passed=1, status="passed", score=0.91)
    _guard(db, status_mismatch, passed=1, status="warning", score=0.95)
    _guard(db, unchecked_failure, checked=0, passed=0, status="failed", score=0.1)

    report = build_persona_guard_failure_digest(db, min_score=0.7, now=NOW)

    assert [row.content_id for row in report.rows] == [low_pass, status_mismatch]
    assert report.totals["failed_count"] == 0
    assert report.totals["borderline_count"] == 2


def test_malformed_reasons_and_metrics_json_are_raw_text(db):
    content_id = _content(db, "Malformed persona guard payload")
    _guard(
        db,
        content_id,
        reasons="[not-json",
        metrics="{bad-metrics",
    )

    report = build_persona_guard_failure_digest(db, now=NOW)
    row = report.rows[0]

    assert row.reasons == "[not-json"
    assert row.metrics == "{bad-metrics"


def test_text_and_json_output_are_stable_and_include_summary_counts(db):
    failed = _content(db, "Failed x post", content_type="x_post")
    review = _content(db, "Review blog", content_type="blog_post")
    _guard(db, failed, status="failed", score=0.3, reasons=["too polished"])
    _guard(db, review, passed=1, status="review", score=0.9, reasons=[])

    report = build_persona_guard_failure_digest(db, limit=10, now=NOW)
    payload = json.loads(format_persona_guard_failure_digest_json(report))
    text = format_persona_guard_failure_digest_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["rows"] == sorted(
        payload["rows"],
        key=lambda row: (1 if row["passed"] else 0, row["score"], row["content_type"], row["content_id"]),
    )
    assert "Persona Guard Failure Digest" in text
    assert "By status: failed=1, review=1" in text
    assert "By content_type: blog_post=1, x_post=1" in text
    assert f"content_id={failed}" in text
    assert "reasons=too polished" in text


def test_missing_schema_and_invalid_args_are_reported(capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_persona_guard_failure_digest(conn, now=NOW)

    assert report.missing_tables == ("generated_content", "content_persona_guard")
    assert report.rows == ()

    with pytest.raises(ValueError, match="days must be positive"):
        build_persona_guard_failure_digest(conn, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_persona_guard_failure_digest(conn, limit=0, now=NOW)
    with pytest.raises(ValueError, match="min_score must be between 0 and 1"):
        build_persona_guard_failure_digest(conn, min_score=1.5, now=NOW)

    assert persona_guard_failure_digest_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert persona_guard_failure_digest_script.main(["--min-score", "1.5"]) == 2
    assert "score must be between 0 and 1" in capsys.readouterr().err
    conn.close()


def test_cli_outputs_text_and_json(db, monkeypatch, capsys):
    content_id = _content(db, "CLI failure")
    _guard(db, content_id, reasons=["voice drift"], checked_at=NOW)

    monkeypatch.setattr(
        persona_guard_failure_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        persona_guard_failure_digest_script,
        "build_persona_guard_failure_digest",
        lambda db, **kwargs: build_persona_guard_failure_digest(db, now=NOW, **kwargs),
    )

    assert persona_guard_failure_digest_script.main(["--format", "text"]) == 0
    assert "Persona Guard Failure Digest" in capsys.readouterr().out

    assert persona_guard_failure_digest_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["content_id"] == content_id
