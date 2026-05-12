"""Tests for generated content readiness triage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.generated_content_readiness import (
    build_generated_content_readiness_report,
    format_generated_content_readiness_json,
    format_generated_content_readiness_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generated_content_readiness.py"
spec = importlib.util.spec_from_file_location("generated_content_readiness_script", SCRIPT_PATH)
generated_content_readiness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(generated_content_readiness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content: str = "Ready draft",
    eval_score: float | None = 8.0,
    eval_feedback: str | None = "usable",
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    created_days_ago: int = 1,
    published: int = 0,
    claim_unsupported: int = 0,
    persona_passed: bool = True,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha"] if source_commits is None else source_commits,
        source_messages=["msg"] if source_messages is None else source_messages,
        content=content,
        eval_score=8.0 if eval_score is None else eval_score,
        eval_feedback="" if eval_feedback is None else eval_feedback,
    )
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published = ?, eval_score = ?, eval_feedback = ?
           WHERE id = ?""",
        (
            (NOW - timedelta(days=created_days_ago)).isoformat(),
            published,
            eval_score,
            eval_feedback,
            content_id,
        ),
    )
    if claim_unsupported:
        db.save_claim_check_summary(
            content_id,
            supported_count=1,
            unsupported_count=claim_unsupported,
            annotation_text="unsupported claim",
        )
    if not persona_passed:
        db.save_persona_guard_summary(
            content_id,
            {"checked": True, "passed": False, "status": "failed", "score": 0.2},
        )
    db.conn.commit()
    return content_id


def test_empty_and_missing_generated_content_return_stable_metadata(db):
    report = build_generated_content_readiness_report(db, now=NOW)
    payload = json.loads(format_generated_content_readiness_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "generated_content_readiness"
    assert payload["totals"] == {
        "blocker_groups": 0,
        "rows_scanned": 0,
        "rows_with_blockers": 0,
    }
    assert payload["age_buckets"] == {
        "stale_0_7_days": 0,
        "stale_8_14_days": 0,
        "stale_15_30_days": 0,
        "stale_31_plus_days": 0,
    }
    assert payload["blockers"] == []

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_generated_content_readiness_report(conn, now=NOW)
    assert missing["missing_tables"] == ["generated_content"]
    assert missing["totals"]["rows_scanned"] == 0


def test_unpublished_rows_are_grouped_by_blocker_with_representative_ids(db):
    missing_score = _content(db, eval_score=None, eval_feedback=None, created_days_ago=2)
    low_score = _content(db, eval_score=4.0, created_days_ago=9)
    missing_feedback = _content(db, eval_feedback=None, created_days_ago=16)
    empty = _content(db, content="  ", created_days_ago=31)
    no_sources = _content(db, source_commits=[], source_messages=[], created_days_ago=1)
    claim_failed = _content(db, claim_unsupported=2, created_days_ago=1)
    persona_failed = _content(db, persona_passed=False, created_days_ago=1)
    published = _content(db, eval_score=None, published=1, created_days_ago=40)

    report = build_generated_content_readiness_report(db, days=8, limit=2, now=NOW)
    payload = report
    blockers = {group["code"]: group for group in payload["blockers"]}

    assert payload["totals"]["rows_scanned"] == 7
    assert payload["age_buckets"] == {
        "stale_0_7_days": 4,
        "stale_8_14_days": 1,
        "stale_15_30_days": 1,
        "stale_31_plus_days": 1,
    }
    assert blockers["missing_eval_score"]["representative_content_ids"] == [missing_score]
    assert blockers["low_eval_score"]["representative_content_ids"] == [low_score]
    assert blockers["missing_eval_feedback"]["representative_content_ids"][:2] == [
        missing_score,
        missing_feedback,
    ]
    assert blockers["empty_content"]["representative_content_ids"] == [empty]
    assert blockers["missing_sources"]["representative_content_ids"] == [no_sources]
    assert blockers["claim_check_failed"]["representative_content_ids"] == [claim_failed]
    assert blockers["persona_guard_failed"]["representative_content_ids"] == [persona_failed]
    assert blockers["stale_8_14_days"]["representative_content_ids"] == [low_score]
    assert blockers["stale_15_30_days"]["representative_content_ids"] == [missing_feedback]
    assert blockers["stale_31_plus_days"]["representative_content_ids"] == [empty]
    assert published not in {
        content_id
        for group in payload["blockers"]
        for content_id in group["representative_content_ids"]
    }


def test_json_and_text_formatters_are_deterministic_and_include_core_fields(db):
    first = _content(db, eval_score=None, eval_feedback=None, created_days_ago=20)
    _content(db, eval_score=5.5, created_days_ago=20)

    report = build_generated_content_readiness_report(db, days=14, limit=1, now=NOW)
    json_payload = json.loads(format_generated_content_readiness_json(report))
    text = format_generated_content_readiness_text(report)

    assert list(json_payload) == sorted(json_payload)
    assert json_payload["artifact_type"] == "generated_content_readiness"
    assert json_payload["filters"]["days"] == 14
    assert json_payload["blockers"][0]["code"] == "missing_eval_score"
    assert json_payload["blockers"][0]["representative_content_ids"] == [first]
    assert "Generated Content Readiness" in text
    assert "stale_15_30_days=2" in text
    assert f"code=missing_eval_score count=1 representative_ids={first}" in text


def test_cli_supports_days_limit_and_json_format(db, monkeypatch, capsys):
    content_id = _content(db, eval_score=None, created_days_ago=20)
    monkeypatch.setattr(
        generated_content_readiness_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        generated_content_readiness_script,
        "build_generated_content_readiness_report",
        lambda db, **kwargs: build_generated_content_readiness_report(db, now=NOW, **kwargs),
    )

    assert generated_content_readiness_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = generated_content_readiness_script.main(
        ["--days", "10", "--limit", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["days"] == 10
    assert payload["filters"]["limit"] == 1
    assert payload["blockers"][0]["representative_content_ids"] == [content_id]
