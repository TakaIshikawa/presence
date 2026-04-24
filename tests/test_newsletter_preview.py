"""Tests for local newsletter preview export."""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_preview import (
    NewsletterPreviewOptions,
    assemble_newsletter_preview,
    build_newsletter_preview,
    extract_outbound_links,
    format_preview_markdown,
    render_newsletter_preview_json,
    write_newsletter_preview,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "preview_newsletter.py"
spec = importlib.util.spec_from_file_location("preview_newsletter_script", SCRIPT_PATH)
preview_newsletter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = preview_newsletter
spec.loader.exec_module(preview_newsletter)


def _publish_content(
    db,
    content_type: str,
    content: str,
    published_at: str,
    url: str,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-preview"],
        source_messages=["msg-preview"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_at = ?, published_url = ?
           WHERE id = ?""",
        (published_at, url, content_id),
    )
    db.conn.commit()
    return content_id


def _insert_attribution_required_knowledge(db) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "source-1",
            "https://source.example/article",
            "Source Author",
            "External insight",
            "attribution_required",
            1,
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_extract_outbound_links_deduplicates_markdown_links():
    links = extract_outbound_links(
        "[Read](https://example.com/a) and [Read](https://example.com/a) plus "
        "[Other](https://example.com/b)."
    )
    assert links == [
        {"label": "Read", "url": "https://example.com/a"},
        {"label": "Other", "url": "https://example.com/b"},
    ]


def test_json_preview_is_deterministic_for_seeded_database(db):
    week_start = datetime(2026, 4, 13, tzinfo=timezone.utc)
    week_end = datetime(2026, 4, 20, tzinfo=timezone.utc)
    blog_id = _publish_content(
        db,
        "blog_post",
        "TITLE: Shipping Smaller AI Tools\n\nThe useful part is less surface area.",
        "2026-04-15T10:00:00+00:00",
        "https://takaishikawa.com/blog/smaller-tools.html",
    )
    post_id = _publish_content(
        db,
        "x_post",
        "Small interfaces make review faster.",
        "2026-04-16T10:00:00+00:00",
        "https://x.com/taka/status/1",
    )

    options = NewsletterPreviewOptions(
        utm_source="newsletter",
        utm_medium="email",
        utm_campaign_template="weekly-{week_end_compact}",
    )
    first = build_newsletter_preview(db, week_start, week_end, options)
    second = build_newsletter_preview(db, week_start, week_end, options)

    assert render_newsletter_preview_json(first) == render_newsletter_preview_json(second)
    data = json.loads(render_newsletter_preview_json(first))
    assert data["source_content_ids"] == [blog_id, post_id]


def test_compatibility_payload_and_markdown_render(db):
    week_end = datetime(2026, 4, 24, tzinfo=timezone.utc)
    week_start = week_end - timedelta(days=7)
    _publish_content(
        db,
        "x_post",
        "CLI preview body.",
        (week_end - timedelta(days=1)).isoformat(),
        "https://x.com/taka/status/456",
    )
    config = SimpleNamespace(
        newsletter=SimpleNamespace(
            enabled=True,
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign_template="weekly-{week_end_compact}",
            subject_override="",
            site_url="https://takaishikawa.com",
        )
    )

    payload = assemble_newsletter_preview(db, config, week_start, week_end)

    assert payload["body_markdown"]
    assert payload["outbound_links"][0]["url"] == "https://x.com/taka/status/456"
    assert "# Newsletter Preview" in format_preview_markdown(payload)


def test_missing_attribution_warns_without_crashing(db):
    week_start = datetime(2026, 4, 13, tzinfo=timezone.utc)
    week_end = datetime(2026, 4, 20, tzinfo=timezone.utc)
    content_id = _publish_content(
        db,
        "x_post",
        "A useful external idea without a visible citation.",
        "2026-04-15T10:00:00+00:00",
        "https://x.com/taka/status/3",
    )
    knowledge_id = _insert_attribution_required_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    preview = build_newsletter_preview(db, week_start, week_end)

    assert any(
        f"Content {content_id}: missing attribution for knowledge {knowledge_id}" in warning
        for warning in preview.warnings
    )


def test_cli_writes_markdown_and_json(monkeypatch, file_db, tmp_path):
    _publish_content(
        file_db,
        "x_post",
        "CLI preview post.",
        "2026-04-15T10:00:00+00:00",
        "https://x.com/taka/status/5",
    )
    config = SimpleNamespace(
        newsletter=SimpleNamespace(
            site_url="https://takaishikawa.com",
            utm_source="",
            utm_medium="",
            utm_campaign_template="",
            subject_override="",
        )
    )

    @contextmanager
    def fake_context():
        yield config, file_db

    monkeypatch.setattr(preview_newsletter, "script_context", fake_context)
    markdown = tmp_path / "preview.md"
    preview_json = tmp_path / "preview.json"

    assert preview_newsletter.main(
        [
            "--week-start",
            "2026-04-13",
            "--week-end",
            "2026-04-20",
            "--markdown-out",
            str(markdown),
            "--json-out",
            str(preview_json),
        ]
    ) == 0

    assert markdown.exists()
    assert preview_json.exists()
    assert "CLI preview post." in markdown.read_text(encoding="utf-8")
    assert json.loads(preview_json.read_text(encoding="utf-8"))["source_content_ids"]
