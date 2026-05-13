"""Tests for blog internal link opportunities."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.blog_internal_link_opportunities import (
    build_blog_internal_link_opportunities_report,
    format_blog_internal_link_opportunities_json,
    format_blog_internal_link_opportunities_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_internal_link_opportunities.py"
spec = importlib.util.spec_from_file_location("blog_internal_link_opportunities_script", SCRIPT_PATH)
blog_internal_link_opportunities_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_internal_link_opportunities_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _blog(db, title: str, body: str, url: str) -> int:
    content_id = db.insert_generated_content("blog_post", [], [], f"# {title}\n{body}", 8.0, "ok")
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ?, published_at = ?, created_at = ? WHERE id = ?",
        (url, NOW.isoformat(), NOW.isoformat(), content_id),
    )
    db.conn.commit()
    return int(content_id)


def _campaign(db, content_id: int, campaign: str) -> None:
    campaign_id = db.insert_content_campaign(campaign, status="active")
    topic_id = db.insert_planned_topic(campaign_id=campaign_id, topic=campaign)
    db.conn.execute("UPDATE planned_topics SET content_id = ? WHERE id = ?", (content_id, topic_id))
    db.conn.commit()


def test_scores_campaign_domain_and_title_overlap_and_excludes_existing_links(db):
    source = _blog(db, "Evidence workflows for launches", "Use https://research.example/a for launch evidence.", "https://site.test/source")
    target = _blog(db, "Launch evidence playbook", "See https://research.example/b for the source.", "https://site.test/target")
    linked = _blog(db, "Already linked launch evidence", "Deep dive.", "https://site.test/already")
    db.conn.execute("UPDATE generated_content SET content = content || ' https://site.test/already' WHERE id = ?", (source,))
    db.conn.commit()
    _campaign(db, source, "launch")
    _campaign(db, target, "launch")
    _campaign(db, linked, "launch")

    report = build_blog_internal_link_opportunities_report(db, min_score=2, now=NOW)

    first = report.opportunities[0]
    assert first.source_post_id == source
    assert first.target_post_id == target
    assert first.score >= 6
    assert set(first.reason_labels) >= {"shared_campaign", "shared_source_domain", "title_token_overlap"}
    assert all(item.target_post_id != linked for item in report.opportunities if item.source_post_id == source)


def test_limit_json_text_and_missing_schema_are_stable(db):
    _blog(db, "Alpha topic", "shared token alpha", "https://site.test/a")
    _blog(db, "Alpha target", "shared token alpha", "https://site.test/b")
    report = build_blog_internal_link_opportunities_report(db, limit=1, min_score=1, now=NOW)
    payload = json.loads(format_blog_internal_link_opportunities_json(report))
    text = format_blog_internal_link_opportunities_text(report)

    assert payload["artifact_type"] == "blog_internal_link_opportunities"
    assert len(payload["opportunities"]) == 1
    assert "Blog Internal Link Opportunities" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_blog_internal_link_opportunities_report(conn, now=NOW)
    assert missing.schema_warnings == ("missing table: generated_content",)


def test_cli_outputs_json(db, monkeypatch, capsys):
    _blog(db, "CLI alpha", "alpha beta", "https://site.test/cli-a")
    _blog(db, "CLI beta", "alpha beta", "https://site.test/cli-b")
    monkeypatch.setattr(blog_internal_link_opportunities_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        blog_internal_link_opportunities_script,
        "build_blog_internal_link_opportunities_report",
        lambda db, **kwargs: build_blog_internal_link_opportunities_report(db, now=NOW, **kwargs),
    )

    assert blog_internal_link_opportunities_script.main(["--min-score", "1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_internal_link_opportunities"
