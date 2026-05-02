"""Tests for stale generated draft cleanup planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.stale_draft_cleanup import (
    build_stale_draft_cleanup_plan,
    format_stale_draft_cleanup_json,
    format_stale_draft_cleanup_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "stale_draft_cleanup.py"
spec = importlib.util.spec_from_file_location("stale_draft_cleanup_script", SCRIPT_PATH)
stale_draft_cleanup_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(stale_draft_cleanup_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    text: str,
    *,
    days_old: float,
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    source_activity_ids: list[str] | None = None,
    eval_score: float = 8.0,
    published: int = 0,
    curation_quality: str | None = None,
    auto_quality: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=source_commits or [],
        source_messages=source_messages or [],
        source_activity_ids=source_activity_ids or [],
        content=text,
        eval_score=eval_score,
        eval_feedback="review me",
    )
    created_at = (NOW - timedelta(days=days_old)).isoformat()
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published = ?, curation_quality = ?, auto_quality = ?
           WHERE id = ?""",
        (created_at, published, curation_quality, auto_quality, content_id),
    )
    db.conn.commit()
    return content_id


def test_classifies_failed_gate_review_timeout_and_superseded_reasons(db):
    failed = _content(db, "failed", days_old=20, eval_score=3.5)
    timed_out = _content(db, "review", days_old=18, curation_quality="review")
    superseded = _content(db, "old source", days_old=17, source_commits=["abc123"])
    newer = _content(db, "new source", days_old=5, source_commits=["abc123"])
    plain = _content(db, "plain", days_old=16)
    fresh = _content(db, "fresh", days_old=6)
    published = _content(db, "published", days_old=30, published=1)
    db.save_persona_guard_summary(
        failed,
        {"checked": True, "passed": False, "status": "failed", "score": 0.2},
    )
    db.upsert_publication_queued(timed_out, "x")

    report = build_stale_draft_cleanup_plan(db, days=14, now=NOW)
    by_id = {row["draft_id"]: row for row in report["drafts"]}

    assert by_id[failed]["reason"] == "failed_gate"
    assert by_id[failed]["suggested_disposition"] == "regenerate"
    assert by_id[timed_out]["reason"] == "review_timeout"
    assert by_id[superseded]["reason"] == "superseded_draft"
    assert by_id[superseded]["superseded_by_draft_id"] == newer
    assert by_id[plain]["reason"] == "stale_unpublished"
    assert fresh not in by_id
    assert published not in by_id
    assert report["counts"]["by_reason"] == {
        "failed_gate": 1,
        "superseded_draft": 1,
        "review_timeout": 1,
        "stale_unpublished": 1,
    }


def test_threshold_boundary_includes_exact_age_and_excludes_younger(db):
    exact = _content(db, "exact", days_old=14)
    younger = _content(db, "younger", days_old=13.99)

    report = build_stale_draft_cleanup_plan(db, days=14, now=NOW)

    assert [row["draft_id"] for row in report["drafts"]] == [exact]
    assert report["drafts"][0]["age_days"] == 14
    assert younger not in [row["draft_id"] for row in report["drafts"]]


def test_ordering_is_deterministic_by_age_desc_then_draft_id(db):
    middle = _content(db, "middle", days_old=20)
    oldest = _content(db, "oldest", days_old=30)
    same_age_first = _content(db, "same first", days_old=20)

    report = build_stale_draft_cleanup_plan(db, days=14, now=NOW)

    assert [row["draft_id"] for row in report["drafts"]] == [
        oldest,
        middle,
        same_age_first,
    ]


def test_json_text_and_cli_render_dry_run_plan_without_mutation(db, monkeypatch, capsys):
    content_id = _content(db, "cli", days_old=21, curation_quality="review")
    before = db.conn.execute("SELECT published FROM generated_content WHERE id = ?", (content_id,)).fetchone()[0]
    report = build_stale_draft_cleanup_plan(db, days=14, now=NOW)

    payload = json.loads(format_stale_draft_cleanup_json(report))
    text = format_stale_draft_cleanup_text(report)

    assert list(payload) == sorted(payload)
    assert payload["filters"]["dry_run"] is True
    assert "Stale Draft Cleanup Plan" in text
    assert "dry-run=1" in text
    assert "review_timeout=1" in text

    monkeypatch.setattr(
        stale_draft_cleanup_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        stale_draft_cleanup_script,
        "build_stale_draft_cleanup_plan",
        lambda db_arg, **kwargs: build_stale_draft_cleanup_plan(
            db_arg,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = stale_draft_cleanup_script.main(
        ["--days", "14", "--limit", "5", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)
    after = db.conn.execute("SELECT published FROM generated_content WHERE id = ?", (content_id,)).fetchone()[0]

    assert exit_code == 0
    assert before == after == 0
    assert cli_payload["drafts"][0]["draft_id"] == content_id
    assert cli_payload["filters"]["dry_run"] is True
    assert cli_payload["filters"]["limit"] == 5


def test_invalid_arguments_and_missing_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_stale_draft_cleanup_plan(conn, now=NOW)
    assert report["missing_tables"] == ["generated_content"]
    assert report["drafts"] == []

    with pytest.raises(ValueError, match="days must be positive"):
        build_stale_draft_cleanup_plan(conn, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_stale_draft_cleanup_plan(conn, limit=0, now=NOW)
    with pytest.raises(ValueError, match="min_eval_score must be non-negative"):
        build_stale_draft_cleanup_plan(conn, min_eval_score=-1, now=NOW)

    assert stale_draft_cleanup_script.main(["--days", "0"]) == 2
