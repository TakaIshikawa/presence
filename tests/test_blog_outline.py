"""Tests for deterministic Claude session blog outlines."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from blog_outline import main as outline_cli
from storage.db import Database
from synthesis.blog_outline import (
    build_blog_outline,
    group_source_events,
    load_source_events,
)


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def outline_db(tmp_path):
    db = Database(str(tmp_path / "presence.db"))
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))
    yield db
    db.close()


def _insert_message(db, uuid, ts, text, project="/work/presence", session="sess-1"):
    return db.insert_claude_message(
        session_id=session,
        message_uuid=uuid,
        project_path=project,
        timestamp=ts,
        prompt_text=text,
    )


def _insert_commit(db, sha, ts, message, repo="taka/presence"):
    return db.insert_commit(
        repo_name=repo,
        commit_sha=sha,
        commit_message=message,
        timestamp=ts,
        author="taka",
    )


def test_groups_source_events_by_day_and_project(outline_db):
    _insert_message(
        outline_db,
        "msg-1",
        "2026-04-23T09:00:00+00:00",
        "Problem: blog generation lacks a reviewable outline.",
    )
    _insert_commit(
        outline_db,
        "abc123",
        "2026-04-23T10:00:00+00:00",
        "feat: add blog outline generator",
    )
    _insert_message(
        outline_db,
        "msg-2",
        "2026-04-24T09:00:00+00:00",
        "Implement CLI output handling.",
    )

    groups = group_source_events(load_source_events(outline_db, days=3, now=NOW))

    assert [(group["day"], group["project"]) for group in groups] == [
        ("2026-04-23", "presence"),
        ("2026-04-24", "presence"),
    ]
    assert len(groups[0]["messages"]) == 1
    assert len(groups[0]["commits"]) == 1


def test_build_outline_contains_titles_sections_and_sources(outline_db):
    msg_id = _insert_message(
        outline_db,
        "msg-1",
        "2026-04-23T09:00:00+00:00",
        "Problem: weekly blog generation lacks decisions before prose. We decided to add a deterministic outline.",
    )
    commit_id = _insert_commit(
        outline_db,
        "abc123",
        "2026-04-23T10:00:00+00:00",
        "fix: add outline JSON and verified CLI output",
    )
    outline_db.conn.execute(
        "INSERT INTO commit_prompt_links (commit_id, message_id, confidence) VALUES (?, ?, ?)",
        (commit_id, msg_id, 0.95),
    )
    outline_db.conn.commit()

    outline = build_blog_outline(outline_db, days=3, now=NOW).to_dict()

    assert outline["title_candidates"]
    assert [section["heading"].split(":")[0] for section in outline["sections"]] == [
        "The problem",
        "The approach",
        "The result",
    ]
    assert outline["source_references"][0]["identifier"] == "msg-1"
    assert any(ref["identifier"] == "abc123" for ref in outline["source_references"])
    assert outline["warnings"] == []


def test_warning_generation_for_missing_results_decisions_and_commits(outline_db):
    _insert_message(
        outline_db,
        "msg-1",
        "2026-04-23T09:00:00+00:00",
        "Please explore the blog outline generator shape.",
    )

    warnings = build_blog_outline(outline_db, days=3, now=NOW).warnings

    assert "Source material lacks clear results or verification evidence." in warnings
    assert "Source material lacks explicit decisions or tradeoffs." in warnings
    assert "Source material lacks linked commit references." in warnings


def test_date_and_repo_filtering(outline_db):
    _insert_message(
        outline_db,
        "old-msg",
        "2026-04-10T09:00:00+00:00",
        "Old work should be filtered.",
    )
    _insert_message(
        outline_db,
        "presence-msg",
        "2026-04-23T09:00:00+00:00",
        "Add outline for this repo.",
        project="/work/presence",
    )
    _insert_commit(
        outline_db,
        "other-sha",
        "2026-04-23T10:00:00+00:00",
        "feat: unrelated repo",
        repo="taka/other",
    )

    outline = build_blog_outline(outline_db, days=3, repo="presence", now=NOW).to_dict()

    assert outline["metadata"]["message_count"] == 1
    assert outline["metadata"]["commit_count"] == 0
    assert [ref["identifier"] for ref in outline["source_references"]] == ["presence-msg"]


def test_cli_writes_json_to_file(outline_db, tmp_path):
    _insert_message(
        outline_db,
        "msg-1",
        "2026-04-23T09:00:00+00:00",
        "Decision: use deterministic grouping for blog outlines.",
    )
    output_path = tmp_path / "outline.json"

    outline_cli([
        "--db",
        str(outline_db.db_path),
        "--days",
        "3",
        "--output",
        str(output_path),
        "--json",
    ])

    payload = json.loads(output_path.read_text())
    assert payload["metadata"]["days"] == 3
    assert payload["title_candidates"]


def test_cli_prints_json_to_stdout(outline_db, capsys):
    _insert_message(
        outline_db,
        "msg-1",
        "2026-04-23T09:00:00+00:00",
        "Decision: print the JSON outline to stdout.",
    )

    outline_cli(["--db", str(outline_db.db_path), "--days", "3", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["metadata"]["message_count"] == 1
