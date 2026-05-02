"""Tests for rejected/revised content feedback salvage exports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.content_feedback_salvage import (
    build_content_feedback_salvage_exports,
    format_content_feedback_salvage_json,
    format_content_feedback_salvage_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_content_feedback_salvage.py"
)
spec = importlib.util.spec_from_file_location("export_content_feedback_salvage_script", SCRIPT_PATH)
export_content_feedback_salvage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_content_feedback_salvage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(
    db,
    *,
    content_type: str = "x_post",
    content: str = "A generic draft about shipping better software.",
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    source_activity_ids: list[str] | None = None,
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=source_commits if source_commits is not None else ["abc123"],
        source_messages=source_messages if source_messages is not None else ["uuid-1"],
        source_activity_ids=source_activity_ids if source_activity_ids is not None else [],
        content=content,
        eval_score=6.0,
        eval_feedback="ok",
    )


def _add_feedback(
    db,
    content_id: int,
    feedback_type: str,
    notes: str = "",
    *,
    replacement_text: str | None = None,
    days_ago: int = 0,
) -> int:
    feedback_id = db.add_content_feedback(
        content_id,
        feedback_type,
        notes,
        replacement_text,
    )
    created_at = (NOW - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        "UPDATE content_feedback SET created_at = ? WHERE id = ?",
        (created_at, feedback_id),
    )
    db.conn.commit()
    return feedback_id


def test_replacement_text_exports_rewrite_recommendation_with_context(db):
    content_id = _insert_content(
        db,
        content_type="newsletter",
        content="We shipped an observability improvement.",
        source_commits=["abc123"],
        source_messages=["uuid-1", "uuid-2"],
        source_activity_ids=["issue-42"],
    )
    feedback_id = _add_feedback(
        db,
        content_id,
        "revise",
        "Good source, weak framing.",
        replacement_text="Turn this into a story about what the alert caught.",
    )

    exports = build_content_feedback_salvage_exports(db, days=7, now=NOW)

    assert len(exports) == 1
    export = exports[0]
    assert export.feedback_id == feedback_id
    assert export.content_id == content_id
    assert export.content_type == "newsletter"
    assert export.feedback_type == "revise"
    assert export.notes == "Good source, weak framing."
    assert export.replacement_text == "Turn this into a story about what the alert caught."
    assert export.generated_content == "We shipped an observability improvement."
    assert export.source_commits == ["abc123"]
    assert export.source_messages == ["uuid-1", "uuid-2"]
    assert export.source_activity_ids == ["issue-42"]
    assert export.salvage_recommendation == "rewrite_from_replacement"


def test_notes_only_revise_with_sources_revisits_source_material(db):
    content_id = _insert_content(db, source_commits=["def456"], source_messages=["uuid-revise"])
    _add_feedback(db, content_id, "revise", "The source story is better than the summary.")

    exports = build_content_feedback_salvage_exports(db, days=7, now=NOW)

    assert exports[0].notes == "The source story is better than the summary."
    assert exports[0].replacement_text == ""
    assert exports[0].salvage_recommendation == "revisit_source_material"


def test_notes_only_reject_exports_avoid_pattern(db):
    content_id = _insert_content(db, source_commits=[], source_messages=[])
    _add_feedback(db, content_id, "reject", "Avoid generic agent wisdom.")

    exports = build_content_feedback_salvage_exports(db, days=7, now=NOW)

    assert exports[0].feedback_type == "reject"
    assert exports[0].salvage_recommendation == "avoid_pattern"


def test_content_type_filter_excludes_other_generated_content(db):
    post = _insert_content(db, content_type="x_post", content="Post draft")
    thread = _insert_content(db, content_type="x_thread", content="Thread draft")
    _add_feedback(db, post, "reject", "Too vague")
    _add_feedback(db, thread, "revise", "Needs thread-specific structure")

    exports = build_content_feedback_salvage_exports(
        db,
        days=7,
        content_type="x_thread",
        now=NOW,
    )

    assert [export.content_id for export in exports] == [thread]
    assert exports[0].content_type == "x_thread"


def test_feedback_type_filter_excludes_prefer_and_other_salvage_types(db):
    reject = _insert_content(db, content="Reject draft")
    revise = _insert_content(db, content="Revise draft")
    prefer = _insert_content(db, content="Prefer draft")
    _add_feedback(db, reject, "reject", "Off voice")
    _add_feedback(db, revise, "revise", "Needs evidence")
    _add_feedback(db, prefer, "prefer", "Strong")

    exports = build_content_feedback_salvage_exports(
        db,
        days=7,
        feedback_type="reject",
        now=NOW,
    )

    assert [export.content_id for export in exports] == [reject]
    assert {export.feedback_type for export in exports} == {"reject"}


def test_limit_and_lookback_are_applied_after_stable_ordering(db):
    old = _insert_content(db, content="Old draft")
    first = _insert_content(db, content="First recent draft")
    second = _insert_content(db, content="Second recent draft")
    _add_feedback(db, old, "reject", "Old", days_ago=20)
    _add_feedback(db, first, "reject", "First", days_ago=2)
    _add_feedback(db, second, "revise", "Second", days_ago=1)

    exports = build_content_feedback_salvage_exports(db, days=7, limit=1, now=NOW)

    assert [export.content_id for export in exports] == [second]


def test_json_and_text_formatters_are_stable(db):
    content_id = _insert_content(db, content_type="x_post", content="Draft")
    _add_feedback(db, content_id, "reject", "Too abstract.")

    exports = build_content_feedback_salvage_exports(db, days=7, now=NOW)
    payload = json.loads(format_content_feedback_salvage_json(exports))
    text = format_content_feedback_salvage_text(exports)

    assert payload[0]["content_id"] == content_id
    assert payload[0]["salvage_recommendation"] == "avoid_pattern"
    assert "salvage_items=1" in text
    assert "avoid_pattern" in text


def test_empty_results_render_stable_empty_output(db):
    exports = build_content_feedback_salvage_exports(db, days=7, now=NOW)

    assert exports == []
    assert json.loads(format_content_feedback_salvage_json(exports)) == []
    assert "no salvage feedback" in format_content_feedback_salvage_text(exports)


def test_cli_supports_json_output_and_filters(db, monkeypatch, capsys):
    post = _insert_content(db, content_type="x_post", content="Post draft")
    _insert_content(db, content_type="x_thread", content="Thread draft")
    _add_feedback(db, post, "reject", "Too generic.")

    monkeypatch.setattr(
        export_content_feedback_salvage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        export_content_feedback_salvage_script,
        "build_content_feedback_salvage_exports",
        lambda db, **kwargs: build_content_feedback_salvage_exports(db, now=NOW, **kwargs),
    )

    exit_code = export_content_feedback_salvage_script.main(
        [
            "--days",
            "7",
            "--feedback-type",
            "reject",
            "--content-type",
            "x_post",
            "--limit",
            "5",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [item["content_id"] for item in payload] == [post]
    assert payload[0]["feedback_type"] == "reject"
