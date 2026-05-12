"""Tests for blog publication readiness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_publication_readiness import (
    build_blog_publication_readiness_report,
    format_blog_publication_readiness_json,
    format_blog_publication_readiness_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_publication_readiness.py"
spec = importlib.util.spec_from_file_location("blog_publication_readiness_script", SCRIPT_PATH)
blog_publication_readiness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_publication_readiness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _blog(db, content: str, commits=None, messages=None, published: int = 0) -> int:
    content_id = db.insert_generated_content("blog_post", commits or [], messages or [], content, 8, "ok")
    db.conn.execute("UPDATE generated_content SET published = ? WHERE id = ?", (published, content_id))
    db.conn.commit()
    return content_id


def _commit(db, sha: str, days_ago: int) -> None:
    db.insert_commit("acme/widget", sha, "commit", (NOW - timedelta(days=days_ago)).isoformat(), "dev")


def test_ready_posts_include_unpublished_ready_warning(db):
    _commit(db, "a", 2)
    _commit(db, "b", 3)
    _blog(db, "# Solid Post\nslug: solid-post\nSummary: A concise summary.\nBody", commits=["a", "b"])

    report = build_blog_publication_readiness_report(db, now=NOW)

    assert report.candidates[0].readiness_status == "ready"
    assert "unpublished_ready_candidate" in report.candidates[0].warning_codes


def test_missing_metadata_blocks_publication(db):
    _commit(db, "a", 2)
    _blog(db, "Summary: Has summary\nBody", commits=["a"])

    report = build_blog_publication_readiness_report(db, now=NOW)

    assert report.candidates[0].readiness_status == "blocked"
    assert "missing_title" in report.candidates[0].blocker_codes
    assert "missing_slug" in report.candidates[0].blocker_codes


def test_stale_sources_are_warnings(db):
    _commit(db, "a", 200)
    _commit(db, "b", 180)
    _blog(db, "# Old Post\nslug: old-post\nSummary: Old sources.\nBody", commits=["a", "b"])

    report = build_blog_publication_readiness_report(db, max_source_age_days=120, now=NOW)

    assert report.candidates[0].readiness_status == "ready"
    assert "stale_source_material" in report.candidates[0].warning_codes


def test_weak_grounding_blocks_candidate(db):
    _commit(db, "a", 2)
    _blog(db, "# Thin Post\nslug: thin-post\nSummary: Thin.\nBody", commits=["a"])

    report = build_blog_publication_readiness_report(db, now=NOW)

    assert "weak_source_grounding" in report.candidates[0].blocker_codes


def test_formatter_and_ready_only_cli_json(db, monkeypatch, capsys):
    _commit(db, "a", 2)
    _commit(db, "b", 3)
    _blog(db, "# Ready\nslug: ready\nSummary: Ready.\nBody", commits=["a", "b"])
    _blog(db, "No metadata")
    monkeypatch.setattr(blog_publication_readiness_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        blog_publication_readiness_script,
        "build_blog_publication_readiness_report",
        lambda db, **kwargs: build_blog_publication_readiness_report(db, now=NOW, **kwargs),
    )

    report = build_blog_publication_readiness_report(db, ready_only=True, now=NOW)
    payload = json.loads(format_blog_publication_readiness_json(report))
    text = format_blog_publication_readiness_text(report)
    exit_code = blog_publication_readiness_script.main(["--format", "json", "--ready-only"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["candidate_count"] == 1
    assert "Blog Publication Readiness" in text
    assert cli_payload["candidate_count"] == 1
    assert exit_code == 0
