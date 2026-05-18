"""Tests for blog revision follow-up reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from evaluation.blog_revision_followup import build_blog_revision_followup_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_revision_followup.py"
spec = importlib.util.spec_from_file_location("blog_revision_followup_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_returns_unresolved_followup_items_with_ages_and_severity():
    posts = [
        {"id": "b1", "title": "Launch notes", "updated_at": "2026-05-10T12:00:00Z", "status": "draft"},
        {"id": "b2", "title": "Architecture", "updated_at": "2026-05-01T12:00:00Z", "status": "published"},
    ]
    notes = [
        {"id": "n1", "post_id": "b1", "created_at": "2026-05-12T12:00:00Z", "severity": "medium", "note": "Clarify source."},
        {"id": "n2", "post_id": "b2", "created_at": "2026-05-05T12:00:00Z", "severity": "high", "reviewer_note": "Stale claim."},
        {"id": "n3", "post_id": "b2", "created_at": "2026-05-06T12:00:00Z", "severity": "critical", "status": "resolved"},
    ]

    report = build_blog_revision_followup_report(posts, notes, now=NOW)

    assert report["summary"]["unresolved_notes"] == 2
    queue = report["followup_queue"]
    assert queue[0]["post_id"] == "b2"
    assert queue[0]["severity"] == "high"
    assert queue[0]["age_since_latest_note_days"] == 10
    assert queue[0]["age_since_last_blog_update_days"] == 14
    assert queue[1]["post_id"] == "b1"


def test_excludes_resolved_and_superseded_notes():
    posts = [{"id": "b1", "title": "Draft", "updated_at": "2026-05-01T00:00:00Z"}]
    notes = [
        {"id": "n1", "post_id": "b1", "created_at": "2026-05-02T00:00:00Z", "resolved_at": "2026-05-03T00:00:00Z"},
        {"id": "n2", "post_id": "b1", "created_at": "2026-05-04T00:00:00Z", "superseded_by": "n3"},
        {"id": "n3", "post_id": "b1", "created_at": "2026-05-05T00:00:00Z", "status": "open", "severity": "low"},
    ]

    report = build_blog_revision_followup_report(posts, notes, now=NOW)

    assert report["summary"]["unresolved_notes"] == 1
    assert report["followup_queue"][0]["notes"][0]["note_id"] == "n3"


def test_priority_ranking_accounts_for_severity_and_age():
    posts = [
        {"id": "old-low", "title": "Old low", "updated_at": "2026-04-01T00:00:00Z"},
        {"id": "new-high", "title": "New high", "updated_at": "2026-05-14T00:00:00Z"},
    ]
    notes = [
        {"id": "n1", "post_id": "old-low", "created_at": "2026-04-01T00:00:00Z", "severity": "low"},
        {"id": "n2", "post_id": "new-high", "created_at": "2026-05-14T00:00:00Z", "severity": "critical"},
    ]

    report = build_blog_revision_followup_report(posts, notes, now=NOW)

    assert report["followup_queue"][0]["post_id"] == "new-high"
    assert report["followup_queue"][0]["priority_score"] > report["followup_queue"][1]["priority_score"]


def test_cli_reads_posts_and_notes_and_outputs_json_and_table(tmp_path, capsys):
    posts_path = tmp_path / "posts.json"
    notes_path = tmp_path / "notes.json"
    posts_path.write_text(json.dumps([{"id": "b1", "title": "Draft", "updated_at": "2026-05-01T00:00:00Z"}]), encoding="utf-8")
    notes_path.write_text(json.dumps([{"id": "n1", "post_id": "b1", "created_at": "2026-05-02T00:00:00Z"}]), encoding="utf-8")

    assert script.main(["--posts-json", str(posts_path), "--notes-json", str(notes_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_revision_followup"
    assert script.main(["--posts-json", str(posts_path), "--notes-json", str(notes_path), "--table"]) == 0
    assert "Blog Revision Follow-up" in capsys.readouterr().out
