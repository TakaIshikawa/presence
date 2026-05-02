"""Tests for content idea dependency detection."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.content_idea_dependencies import (
    build_content_idea_dependency_report,
    format_content_idea_dependencies_json,
    format_content_idea_dependencies_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_dependencies.py"
spec = importlib.util.spec_from_file_location("content_idea_dependencies_script", SCRIPT_PATH)
content_idea_dependencies_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_dependencies_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _planned_topic(db, *, topic: str, status: str = "planned", content_id: int | None = None) -> int:
    cursor = db.conn.execute(
        """INSERT INTO planned_topics (topic, status, content_id)
           VALUES (?, ?, ?)""",
        (topic, status, content_id),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _github_activity(
    db,
    *,
    repo: str = "taka/presence",
    activity_type: str = "issue",
    number: int = 42,
    state: str = "open",
    closed_at: str | None = None,
    merged_at: str | None = None,
) -> int:
    return db.upsert_github_activity(
        repo_name=repo,
        activity_type=activity_type,
        number=number,
        title=f"{activity_type} {number}",
        body="",
        state=state,
        author="taka",
        url=f"https://github.com/{repo}/issues/{number}",
        updated_at="2026-05-01T12:00:00+00:00",
        closed_at=closed_at,
        merged_at=merged_at,
    )


def test_detects_dependency_language_and_unresolved_github_reference(db):
    _github_activity(db, number=42, state="open")
    idea_id = db.add_content_idea(
        "Write the rollout lesson after https://github.com/taka/presence/issues/42 lands.",
        topic="rollout",
        priority="high",
    )

    rows = build_content_idea_dependency_report(db, status="open")

    row = next(item for item in rows if item.idea_id == idea_id and item.dependency_type == "github_activity")
    assert row.reference_text == "taka/presence#42"
    assert row.local_resolved is False
    assert row.local_entity_status == "open"
    assert row.guidance.startswith("Wait before promotion")


def test_resolved_planned_topic_reference_gets_promote_guidance(db):
    planned_id = _planned_topic(db, topic="launch notes", status="generated", content_id=123)
    idea_id = db.add_content_idea(
        f"Follow-up depends on planned topic #{planned_id}.",
        topic="launch follow-up",
    )

    rows = build_content_idea_dependency_report(db)

    row = next(item for item in rows if item.idea_id == idea_id)
    assert row.dependency_type == "planned_topic"
    assert row.local_entity_id == planned_id
    assert row.local_resolved is True
    assert row.guidance.startswith("Promote is probably safe")


def test_structured_metadata_references_are_detected_and_resolved(db):
    activity_id = _github_activity(
        db,
        activity_type="pull_request",
        number=7,
        state="closed",
        merged_at="2026-05-01T13:00:00+00:00",
    )
    waiting_id = _planned_topic(db, topic="API migration", status="planned")
    idea_id = db.add_content_idea(
        "Turn the migration into a release note.",
        topic="API migration release",
        source_metadata={
            "github_activity_id": activity_id,
            "planned_topic_id": waiting_id,
        },
    )

    rows = [row for row in build_content_idea_dependency_report(db) if row.idea_id == idea_id]
    by_type = {row.dependency_type: row for row in rows}

    assert by_type["github_activity"].local_resolved is True
    assert by_type["planned_topic"].local_resolved is False
    assert by_type["planned_topic"].wait_reason.endswith("(planned)")


def test_status_limit_and_stable_formatting(db):
    _github_activity(db, number=11, state="open")
    open_id = db.add_content_idea("Blocked by issue #11.", topic="open")
    dismissed_id = db.add_content_idea("Blocked by a later launch.", topic="dismissed")
    db.dismiss_content_idea(dismissed_id)

    open_rows = build_content_idea_dependency_report(db, status="open", limit=1)
    all_rows = build_content_idea_dependency_report(db, status=None)
    payload = json.loads(format_content_idea_dependencies_json(open_rows))
    text = format_content_idea_dependencies_text(open_rows)

    assert [row.idea_id for row in open_rows] == [open_id]
    assert {row.idea_id for row in all_rows} == {open_id, dismissed_id}
    assert list(payload[0].keys()) == sorted(payload[0].keys())
    assert "Content Idea Dependency Report" in text
    assert "unresolved=1" in text


def test_cli_supports_status_limit_and_json_output(db, monkeypatch, capsys):
    _github_activity(db, number=88, state="open")
    idea_id = db.add_content_idea("Wait for issue #88 before publishing.", topic="cli")
    monkeypatch.setattr(
        content_idea_dependencies_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_idea_dependencies_script.main(
        ["--status", "open", "--limit", "5", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [item["idea_id"] for item in payload] == [idea_id]
    assert payload[0]["dependency_type"] == "github_activity"
