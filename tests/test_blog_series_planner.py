"""Tests for planning candidate blog series from related content."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.blog_series_planner import (
    build_blog_series_plan,
    format_blog_series_plan_json,
    format_blog_series_plan_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_blog_series.py"
spec = importlib.util.spec_from_file_location("plan_blog_series", SCRIPT_PATH)
plan_blog_series = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_blog_series)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    text: str = "Series source",
    topic: str | None = "architecture",
    created_at: str = "2026-04-20T12:00:00+00:00",
    published_at: str | None = "2026-04-21T12:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published = ?, published_at = ?
           WHERE id = ?""",
        (created_at, 1 if published_at else 0, published_at, content_id),
    )
    if topic:
        db.insert_content_topics(content_id, [(topic, "", 0.9)])
    db.conn.commit()
    return content_id


def _knowledge(db, source_id: str = "source") -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, content, insight, approved, ingested_at)
           VALUES ('curated_article', ?, 'Evidence body', 'Evidence insight', 1, ?)""",
        (source_id, "2026-04-01T12:00:00+00:00"),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_planner_groups_related_content_by_topic_knowledge_and_newsletter(db):
    shared_knowledge_id = _knowledge(db, "shared")
    first = _content(db, content_type="x_post", text="First architecture lesson")
    second = _content(db, content_type="x_thread", text="Thread architecture lesson")
    third = _content(db, content_type="newsletter", text="Newsletter architecture lesson")
    _content(db, topic="testing", text="Unrelated testing note")
    db.insert_content_knowledge_links(first, [(shared_knowledge_id, 0.9)])
    db.insert_content_knowledge_links(second, [(shared_knowledge_id, 0.8)])
    send_id = db.insert_newsletter_send("issue-1", "Architecture week", [first, third])
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2026-04-24T12:00:00+00:00", send_id),
    )
    db.conn.commit()

    plan = build_blog_series_plan(db, days=45, min_items=3, now=NOW)

    assert len(plan.candidates) == 1
    candidate = plan.candidates[0]
    assert candidate.title_suggestion == "Architecture Series"
    assert candidate.included_content_ids == (first, second, third)
    assert candidate.newsletter_send_ids == (send_id,)
    assert candidate.knowledge_ids == (shared_knowledge_id,)
    assert candidate.evidence_count == 5
    assert candidate.freshness == "fresh"
    assert candidate.recommended_next_artifact == "blog_series_outline"
    assert "single_content_type" not in candidate.missing_evidence_warnings


def test_weak_candidates_below_min_items_are_excluded(db):
    _content(db, text="One testing source", topic="testing")
    _content(db, text="Second testing source", topic="testing")
    _content(db, text="One architecture source", topic="architecture")

    plan = build_blog_series_plan(db, days=45, min_items=3, now=NOW)

    assert plan.candidates == ()
    assert plan.totals["excluded_weak_candidates"] == 2


def test_candidate_warns_about_missing_evidence_and_newsletter_sources(db):
    first = _content(db, text="Ops one", topic="ops")
    second = _content(db, content_type="x_thread", text="Ops two", topic="ops")
    send_id = db.insert_newsletter_send("issue-bad", "Ops links", [])
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ?, sent_at = ? WHERE id = ?",
        (json.dumps([first, "bad", 9999]), "2026-04-24T12:00:00+00:00", send_id),
    )
    db.conn.commit()

    plan = build_blog_series_plan(db, days=45, min_items=2, now=NOW)
    candidate = plan.candidates[0]

    assert candidate.included_content_ids == (first, second)
    assert "no_knowledge_links" in candidate.missing_evidence_warnings
    assert "malformed_source_content_ids" in candidate.missing_evidence_warnings
    assert "newsletter_has_missing_sources" in candidate.missing_evidence_warnings
    assert "newsletter_has_outside_sources" in candidate.missing_evidence_warnings


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    first = _content(db, topic="writing", text="Writing one")
    second = _content(db, content_type="bluesky_post", topic="writing", text="Writing two")

    plan = build_blog_series_plan(db, days=45, min_items=2, now=NOW)
    assert format_blog_series_plan_json(plan) == format_blog_series_plan_json(plan)
    payload = json.loads(format_blog_series_plan_json(plan))
    assert payload["filters"]["days"] == 45
    assert payload["candidates"][0]["included_content_ids"] == [first, second]
    assert "Blog Series Planner" in format_blog_series_plan_text(plan)

    with patch.object(
        plan_blog_series,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        plan_blog_series,
        "build_blog_series_plan",
        wraps=lambda db, **kwargs: build_blog_series_plan(db, now=NOW, **kwargs),
    ):
        assert plan_blog_series.main(["--days", "45", "--min-items", "2", "--json"]) == 0

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["min_items"] == 2
    assert cli_payload["candidates"][0]["title_suggestion"] == "Writing Series"


def test_missing_generated_content_table_returns_empty_plan():
    conn = sqlite3.connect(":memory:")
    try:
        plan = build_blog_series_plan(conn, now=NOW)
    finally:
        conn.close()

    assert plan.candidates == ()
    assert "generated_content" in plan.missing_tables
