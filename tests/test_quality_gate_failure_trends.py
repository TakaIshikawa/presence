"""Tests for quality-gate failure trend reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.quality_gate_failure_trends import (
    build_quality_gate_failure_trends_report,
    format_quality_gate_failure_trends_json,
    format_quality_gate_failure_trends_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "quality_gate_failure_trends.py"
spec = importlib.util.spec_from_file_location("quality_gate_failure_trends_script", SCRIPT_PATH)
quality_gate_failure_trends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(quality_gate_failure_trends_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post", *, days_ago: int = 1) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} body",
        eval_score=0.4,
        eval_feedback="needs work",
    )
    created_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _persona_failure(
    db,
    content_id: int,
    reasons,
    *,
    days_ago: int = 1,
    status: str = "failed",
    metrics=None,
) -> None:
    timestamp = NOW - timedelta(days=days_ago)
    db.save_persona_guard_summary(
        content_id,
        {
            "checked": True,
            "passed": False,
            "status": status,
            "score": 0.2,
            "reasons": reasons,
            "metrics": metrics or {},
        },
    )
    db.conn.execute(
        "UPDATE content_persona_guard SET created_at = ?, updated_at = ? WHERE content_id = ?",
        (timestamp.isoformat(), timestamp.isoformat(), content_id),
    )
    db.conn.commit()


def test_empty_db_returns_zeroed_report(db):
    report = build_quality_gate_failure_trends_report(db, now=NOW)

    assert report.rows == ()
    assert report.weekly_totals == {}
    assert report.totals == {
        "failure_count": 0,
        "affected_content_count": 0,
        "by_gate": {},
        "by_content_type": {},
    }
    assert "No quality-gate failures found" in format_quality_gate_failure_trends_text(report)


def test_grouped_failures_include_counts_ids_and_weekly_totals(db):
    first = _content(db, "x_post", days_ago=2)
    second = _content(db, "x_post", days_ago=1)
    third = _content(db, "blog_post", days_ago=9)
    _persona_failure(db, first, ["tone_mismatch"], days_ago=2)
    _persona_failure(db, second, ["tone_mismatch"], days_ago=1)
    _persona_failure(db, third, ["too_generic"], days_ago=9)
    db.save_claim_check_summary(third, supported_count=1, unsupported_count=2)
    db.conn.execute(
        "UPDATE content_claim_checks SET created_at = ?, updated_at = ? WHERE content_id = ?",
        ((NOW - timedelta(days=9)).isoformat(), (NOW - timedelta(days=9)).isoformat(), third),
    )
    db.conn.commit()

    report = build_quality_gate_failure_trends_report(db, days=30, min_count=1, now=NOW)

    assert report.totals["failure_count"] == 4
    assert report.totals["affected_content_count"] == 3
    assert report.totals["by_gate"] == {"claim_check": 1, "persona_guard": 3}
    rows = [row.to_dict() for row in report.rows]
    assert {
        "gate": "persona_guard",
        "reason_code": "tone_mismatch",
        "content_type": "x_post",
        "week": "2026-W20",
        "failure_count": 2,
        "affected_content_ids": [first, second],
    } in rows
    assert sum(report.weekly_totals.values()) == 4


def test_min_count_filter_hides_sparse_groups_but_keeps_totals(db):
    first = _content(db, "x_post")
    second = _content(db, "x_post")
    third = _content(db, "blog_post")
    _persona_failure(db, first, ["tone_mismatch"])
    _persona_failure(db, second, ["tone_mismatch"])
    _persona_failure(db, third, ["too_generic"])

    report = build_quality_gate_failure_trends_report(db, min_count=2, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].failure_count == 2
    assert report.totals["failure_count"] == 3


def test_malformed_metadata_falls_back_to_stable_reason_code(db):
    content_id = _content(db, "x_thread")
    _persona_failure(db, content_id, "{not-json", metrics="{bad-json")

    report = build_quality_gate_failure_trends_report(db, now=NOW)

    assert report.rows[0].reason_code == "{not-json"
    assert report.rows[0].affected_content_ids == (content_id,)


def test_formatter_output_is_deterministic_and_cli_supports_json(db, monkeypatch, capsys):
    content_id = _content(db, "x_post")
    _persona_failure(db, content_id, {"code": "too_salesy"})
    monkeypatch.setattr(
        quality_gate_failure_trends_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        quality_gate_failure_trends_script,
        "build_quality_gate_failure_trends_report",
        lambda db, **kwargs: build_quality_gate_failure_trends_report(db, now=NOW, **kwargs),
    )

    report = build_quality_gate_failure_trends_report(db, days=7, now=NOW)
    payload = json.loads(format_quality_gate_failure_trends_json(report))
    text = format_quality_gate_failure_trends_text(report)
    exit_code = quality_gate_failure_trends_script.main(
        ["--days", "7", "--min-count", "1", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "quality_gate_failure_trends"
    assert "Quality Gate Failure Trends" in text
    assert "reason=too_salesy" in text
    assert exit_code == 0
    assert cli_payload["filters"]["days"] == 7
    assert cli_payload["rows"][0]["reason_code"] == "too_salesy"


def test_missing_schema_gaps_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_quality_gate_failure_trends_report(conn, now=NOW)

    assert report.missing_tables == (
        "generated_content",
        "content_persona_guard",
        "content_claim_checks",
    )


def test_invalid_args_raise_or_return_errors(db, capsys):
    with pytest.raises(ValueError, match="days must be positive"):
        build_quality_gate_failure_trends_report(db, days=0)

    assert quality_gate_failure_trends_script.main(["--min-count", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
