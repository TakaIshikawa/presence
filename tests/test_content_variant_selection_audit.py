"""Tests for content variant selection audit reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.content_variant_selection_audit import (
    ISSUE_MISSING_SELECTED_VARIANT,
    ISSUE_MULTIPLE_UNSELECTED_CANDIDATES,
    ISSUE_STALE_SELECTED_VARIANT,
    build_content_variant_selection_audit_report,
    format_content_variant_selection_audit_json,
    format_content_variant_selection_audit_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_selection_audit.py"
spec = importlib.util.spec_from_file_location("content_variant_selection_audit_script", SCRIPT_PATH)
content_variant_selection_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_variant_selection_audit_script)

NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(db, *, content: str = "Generated copy") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _set_variant_created_at(db, variant_id: int, value: str) -> None:
    db.conn.execute(
        "UPDATE content_variants SET created_at = ? WHERE id = ?",
        (value, variant_id),
    )
    db.conn.commit()


def _set_feedback_created_at(db, feedback_id: int, value: str) -> None:
    db.conn.execute(
        "UPDATE content_feedback SET created_at = ? WHERE id = ?",
        (value, feedback_id),
    )
    db.conn.commit()


def test_missing_selected_variant_is_reported(db):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "x", "post", "X copy")

    report = build_content_variant_selection_audit_report(db, now=NOW)

    assert report.totals["content_checked"] == 1
    assert report.totals["variant_groups_checked"] == 1
    assert report.totals["issues_found"] == 1
    assert report.issues[0].issue_type == ISSUE_MISSING_SELECTED_VARIANT
    assert report.issues[0].content_id == content_id
    assert report.issues[0].variant_ids == (variant_id,)
    assert report.issues[0].selected_variant_ids == ()


def test_multiple_unselected_platform_candidates_are_reported(db):
    content_id = _insert_content(db)
    first_id = db.upsert_content_variant(content_id, "linkedin", "post", "Post copy")
    second_id = db.upsert_content_variant(content_id, "linkedin", "summary", "Summary copy")

    report = build_content_variant_selection_audit_report(db, now=NOW)
    issue_types = [issue.issue_type for issue in report.issues]
    multiple = next(
        issue
        for issue in report.issues
        if issue.issue_type == ISSUE_MULTIPLE_UNSELECTED_CANDIDATES
    )

    assert ISSUE_MISSING_SELECTED_VARIANT in issue_types
    assert report.totals["issues_found"] == 2
    assert multiple.unselected_variant_ids == (first_id, second_id)


def test_selected_variant_older_than_latest_feedback_is_reported(db):
    content_id = _insert_content(db)
    selected_id = db.upsert_content_variant(content_id, "bluesky", "post", "Old copy")
    db.select_content_variant(content_id, "bluesky", "post")
    _set_variant_created_at(db, selected_id, "2026-05-01T09:00:00+00:00")
    feedback_id = db.add_content_feedback(content_id, "revise", "Tighten the opening.")
    _set_feedback_created_at(db, feedback_id, "2026-05-02T10:00:00+00:00")

    report = build_content_variant_selection_audit_report(db, now=NOW)

    assert report.totals["issues_found"] == 1
    issue = report.issues[0]
    assert issue.issue_type == ISSUE_STALE_SELECTED_VARIANT
    assert issue.stale_selected_variant_ids == (selected_id,)
    assert issue.latest_feedback_at == "2026-05-02T10:00:00+00:00"


def test_platform_filter_limits_variant_groups(db):
    x_content_id = _insert_content(db, content="X content")
    bluesky_content_id = _insert_content(db, content="Bluesky content")
    db.upsert_content_variant(x_content_id, "x", "post", "X copy")
    db.upsert_content_variant(bluesky_content_id, "bluesky", "post", "Bluesky copy")

    report = build_content_variant_selection_audit_report(db, platform="x", now=NOW)

    assert report.totals["content_checked"] == 2
    assert report.totals["variant_groups_checked"] == 1
    assert [issue.platform for issue in report.issues] == ["x"]
    assert report.issues[0].content_id == x_content_id


def test_json_output_includes_required_totals(db):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X copy")

    report = build_content_variant_selection_audit_report(db, days=14, now=NOW)
    payload = json.loads(format_content_variant_selection_audit_json(report))
    text = format_content_variant_selection_audit_text(report)

    assert payload["totals"] == {
        "content_checked": 1,
        "issues_found": 1,
        "variant_groups_checked": 1,
    }
    assert payload["filters"]["days"] == 14
    assert "Content Variant Selection Audit" in text
    assert f"content_id={content_id}" in text


def test_cli_supports_json_platform_days_and_fail_on_issues(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X copy")
    db.upsert_content_variant(content_id, "bluesky", "post", "Bluesky copy")
    monkeypatch.setattr(
        content_variant_selection_audit_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variant_selection_audit_script.main(
        ["--days", "7", "--platform", "x", "--format", "json", "--fail-on-issues"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["platform"] == "x"
    assert payload["totals"]["issues_found"] == 1
    assert payload["issues"][0]["content_id"] == content_id
