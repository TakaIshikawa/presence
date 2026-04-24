"""Tests for knowledge retirement reporting and apply mode."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.retirement import KnowledgeRetirementPolicy, build_retirement_report
from retire_knowledge import main


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _insert_knowledge(
    db,
    *,
    source_id: str,
    source_type: str = "curated_x",
    license_value: str = "attribution_required",
    content: str = "Knowledge content",
    created_days_ago: int = 200,
    source_url: str | None = "https://source.example/post",
) -> int:
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
        (
            source_type,
            source_id,
            source_url,
            "Source Author",
            content,
            license_value,
            (NOW - timedelta(days=created_days_ago)).isoformat(),
        ),
    ).lastrowid


def _policy(**overrides) -> KnowledgeRetirementPolicy:
    params = {
        "older_than_days": 180,
        "min_unused_days": 30,
        "now": NOW,
    }
    params.update(overrides)
    return KnowledgeRetirementPolicy(**params)


def test_dry_run_reports_candidates_without_updates(db):
    knowledge_id = _insert_knowledge(db, source_id="old-unused")
    db.conn.commit()

    payload = build_retirement_report(db, _policy(), apply=False)

    assert payload["mode"] == "dry_run"
    assert payload["totals"]["retired"] == 1
    assert payload["items"][0]["id"] == knowledge_id
    assert payload["items"][0]["action"] == "retire"
    assert payload["items"][0]["reasons"] == ["old", "unused"]

    row = db.conn.execute(
        "SELECT approved FROM knowledge WHERE id = ?", (knowledge_id,)
    ).fetchone()
    assert row["approved"] == 1


def test_apply_marks_unapproved_and_preserves_content(db):
    knowledge_id = _insert_knowledge(
        db,
        source_id="restricted-old",
        license_value="restricted",
        content="Retain this source text",
    )
    db.conn.commit()

    payload = build_retirement_report(db, _policy(), apply=True)

    assert payload["mode"] == "apply"
    assert payload["totals"]["retired"] == 1
    row = db.conn.execute(
        "SELECT approved, content FROM knowledge WHERE id = ?", (knowledge_id,)
    ).fetchone()
    assert row["approved"] == 0
    assert row["content"] == "Retain this source text"


def test_recent_content_link_prevents_retirement_when_min_unused_days_not_met(db):
    knowledge_id = _insert_knowledge(db, source_id="recent-content-link")
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Generated content",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """INSERT INTO content_knowledge_links
           (content_id, knowledge_id, relevance_score, created_at)
           VALUES (?, ?, ?, ?)""",
        (content_id, knowledge_id, 0.8, (NOW - timedelta(days=5)).isoformat()),
    )
    db.conn.commit()

    payload = build_retirement_report(db, _policy(min_unused_days=30), apply=True)

    item = payload["items"][0]
    assert item["action"] == "retain"
    assert item["reasons"] == ["old"]
    assert item["retain_reasons"] == ["recent_usage"]
    assert payload["totals"]["retired"] == 0
    assert db.conn.execute(
        "SELECT approved FROM knowledge WHERE id = ?", (knowledge_id,)
    ).fetchone()["approved"] == 1


def test_recent_reply_link_prevents_retirement_when_min_unused_days_not_met(db):
    knowledge_id = _insert_knowledge(db, source_id="recent-reply-link")
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score, created_at)
           VALUES (?, ?, ?, ?)""",
        (999, knowledge_id, 0.7, (NOW - timedelta(days=3)).isoformat()),
    )
    db.conn.commit()

    payload = build_retirement_report(db, _policy(min_unused_days=30), apply=True)

    item = payload["items"][0]
    assert item["action"] == "retain"
    assert item["retain_reasons"] == ["recent_usage"]
    assert payload["totals"]["retired"] == 0
    assert db.conn.execute(
        "SELECT approved FROM knowledge WHERE id = ?", (knowledge_id,)
    ).fetchone()["approved"] == 1


def test_json_output_includes_totals_by_source_type_license_and_reason(db, capsys):
    _insert_knowledge(
        db,
        source_id="restricted",
        source_type="curated_article",
        license_value="restricted",
    )
    uncited_id = _insert_knowledge(
        db,
        source_id="uncited",
        source_type="curated_x",
        license_value="attribution_required",
        source_url="https://source.example/needs-cite",
        created_days_ago=10,
    )
    for index in range(2):
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content=f"Generated content without visible citation {index}",
            eval_score=7.0,
            eval_feedback="ok",
        )
        db.conn.execute(
            """INSERT INTO content_knowledge_links
               (content_id, knowledge_id, relevance_score, created_at)
               VALUES (?, ?, ?, ?)""",
            (content_id, uncited_id, 0.8, (NOW - timedelta(days=45)).isoformat()),
        )
    db.conn.commit()

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("retire_knowledge.script_context", fake_script_context), patch(
        "retire_knowledge.KnowledgeRetirementPolicy",
        lambda **kwargs: KnowledgeRetirementPolicy(**kwargs, now=NOW),
    ):
        main(["--older-than-days", "180", "--min-unused-days", "30", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["retired"] == 2
    assert payload["totals"]["by_source_type"] == {
        "curated_article": 1,
        "curated_x": 1,
    }
    assert payload["totals"]["by_license"] == {
        "attribution_required": 1,
        "restricted": 1,
    }
    assert payload["totals"]["by_reason"] == {
        "old": 1,
        "repeatedly_uncited": 1,
        "restricted": 1,
        "unused": 2,
    }
