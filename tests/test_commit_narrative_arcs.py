"""Tests for planning narrative arcs from commits and Claude sessions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.commit_narrative_arcs import (
    build_commit_narrative_arcs,
    format_commit_narrative_arcs_json,
    format_commit_narrative_arcs_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "commit_narrative_arcs.py"
spec = importlib.util.spec_from_file_location("commit_narrative_arcs", SCRIPT_PATH)
commit_narrative_arcs = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(commit_narrative_arcs)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _commit(
    db,
    *,
    repo: str = "acme/presence",
    sha: str = "abc123",
    message: str = "feat: improve narrative planner grouping",
    timestamp: str = "2026-04-28T10:00:00+00:00",
) -> int:
    return db.insert_commit(repo, sha, message, timestamp, "dev@example.com")


def _message(
    db,
    *,
    session: str = "sess-1",
    uuid: str = "msg-1",
    project: str = "/work/presence",
    prompt: str = "Summarize narrative planner grouping work for publishing",
    timestamp: str = "2026-04-28T10:05:00+00:00",
) -> int:
    return db.insert_claude_message(session, uuid, project, timestamp, prompt)


def test_groups_linked_commits_and_sessions_into_confident_arc(db):
    commit_id = _commit(db, sha="aaa111")
    _commit(
        db,
        sha="bbb222",
        message="test: cover narrative planner confidence scoring",
        timestamp="2026-04-28T11:00:00+00:00",
    )
    message_id = _message(db)
    db.conn.execute(
        "INSERT INTO commit_prompt_links (commit_id, message_id, confidence) VALUES (?, ?, ?)",
        (commit_id, message_id, 0.95),
    )
    db.conn.commit()

    plan = build_commit_narrative_arcs(db, lookback_days=14, min_items_per_arc=2, now=NOW)

    assert len(plan.arcs) == 1
    arc = plan.arcs[0]
    assert arc.primary_repo == "acme/presence"
    assert arc.title == "Narrative Grouping Narrative Arc"
    assert arc.source_ids == ("commit:aaa111", "session:sess-1", "commit:bbb222")
    assert arc.suggested_formats == ("newsletter_section", "x_thread")
    assert arc.confidence == 0.95
    assert plan.totals["commit_count"] == 2
    assert plan.totals["session_count"] == 1


def test_sparse_data_below_min_items_is_excluded(db):
    _commit(db, sha="solo", message="docs: explain billing migration")

    plan = build_commit_narrative_arcs(db, lookback_days=14, min_items_per_arc=2, now=NOW)

    assert plan.arcs == ()
    assert plan.totals["excluded_weak_arcs"] == 1


def test_missing_optional_tables_or_columns_return_empty_metadata():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE github_commits (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            commit_sha TEXT,
            timestamp TEXT
        )"""
    )
    try:
        plan = build_commit_narrative_arcs(conn, now=NOW)
    finally:
        conn.close()

    assert plan.arcs == ()
    assert plan.missing_tables == ("claude_messages",)
    assert plan.missing_columns == {"github_commits": ("commit_message",)}
    assert plan.totals["source_item_count"] == 0


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    _commit(
        db,
        sha="older",
        message="feat: add archive manifest grouping",
        timestamp="2026-04-27T09:00:00+00:00",
    )
    _message(
        db,
        session="sess-archive",
        uuid="uuid-archive",
        prompt="Turn archive manifest grouping into newsletter section",
        timestamp="2026-04-27T09:30:00+00:00",
    )
    _commit(
        db,
        repo="acme/tools",
        sha="tools",
        message="feat: add billing retry dashboard",
        timestamp="2026-04-28T09:00:00+00:00",
    )
    _message(
        db,
        session="sess-tools",
        uuid="uuid-tools",
        project="/work/tools",
        prompt="Shape billing retry dashboard work into an X thread",
        timestamp="2026-04-28T09:30:00+00:00",
    )

    plan = build_commit_narrative_arcs(db, lookback_days=14, min_items_per_arc=2, now=NOW)

    assert format_commit_narrative_arcs_json(plan) == format_commit_narrative_arcs_json(plan)
    payload = json.loads(format_commit_narrative_arcs_json(plan, limit=1))
    assert len(payload["arcs"]) == 1
    assert payload["filters"]["lookback_days"] == 14
    assert "Commit Narrative Arc Planner" in format_commit_narrative_arcs_text(plan)

    with patch.object(
        commit_narrative_arcs,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        commit_narrative_arcs,
        "build_commit_narrative_arcs",
        wraps=lambda db, **kwargs: build_commit_narrative_arcs(db, now=NOW, **kwargs),
    ):
        assert (
            commit_narrative_arcs.main(
                ["--days", "14", "--min-items", "2", "--format", "json", "--limit", "1"]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert len(cli_payload["arcs"]) == 1
    assert cli_payload["filters"]["min_items_per_arc"] == 2
