"""Tests for generated-content citation density reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from knowledge_citation_density import main  # noqa: E402
from knowledge.citation_density import (  # noqa: E402
    build_citation_density_report,
    format_citation_density_text,
)


NOW = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)


def _content(db, content_type: str, content: str) -> int:
    content_id = db.insert_generated_content(content_type, [], [], content, 7.0, "ok")
    db.conn.execute("UPDATE generated_content SET created_at = ? WHERE id = ?", (NOW.isoformat(), content_id))
    db.conn.commit()
    return content_id


def _knowledge(db, source_type: str = "curated_article") -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved, metadata)
           VALUES (?, ?, ?, ?, ?, 1, ?)""",
        (
            source_type,
            f"{source_type}-1",
            "https://example.com/source",
            "Alice",
            "source text",
            json.dumps({"link_metadata": {"canonical_url": "https://example.com/canonical"}}),
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_flags_long_form_with_too_few_citations_and_short_x_with_dense_urls(db):
    long = _content(db, "blog_post", " ".join(["word"] * 300))
    dense = _content(db, "x_post", "Tiny https://a.example/1 https://b.example/2")
    ok = _content(db, "newsletter", " ".join(["word"] * 100) + " https://inline.example/source")
    db.insert_content_knowledge_links(ok, [(_knowledge(db), 0.9)])

    report = build_citation_density_report(db, min_per_100=1.0, max_per_100=20.0, now=NOW)
    by_id = {item["content_id"]: item for item in report["items"]}

    assert by_id[long]["citation_count"] == 0
    assert by_id[long]["approximate_word_count"] == 300
    assert by_id[long]["citations_per_100_words"] == 0.0
    assert by_id[long]["expected_range"] == {"min_per_100": 1.0, "max_per_100": None}
    assert by_id[long]["issue_type"] == "too_few_citations"
    assert by_id[dense]["citation_count"] == 2
    assert by_id[dense]["issue_type"] == "too_dense_citations"
    assert ok not in by_id
    assert "Flagged content:" in format_citation_density_text(report)


def test_explicit_citations_count_only_curated_knowledge_links(db):
    content_id = _content(db, "blog_post", " ".join(["word"] * 120))
    curated = _knowledge(db, "curated_newsletter")
    own = _knowledge(db, "own_post")
    db.insert_content_knowledge_links(content_id, [(curated, 0.9), (own, 0.9)])

    report = build_citation_density_report(db, min_per_100=0.5, now=NOW)

    assert report["items"] == []
    all_report = build_citation_density_report(db, min_per_100=2.0, now=NOW)
    assert all_report["items"][0]["explicit_source_link_count"] == 1


def test_content_type_filter_and_cli_json(db, capsys):
    blog = _content(db, "blog_post", " ".join(["word"] * 300))
    _content(db, "x_post", "Tiny https://a.example/1 https://b.example/2")

    report = build_citation_density_report(db, content_type="blog_post", min_per_100=1.0, now=NOW)
    assert [item["content_id"] for item in report["items"]] == [blog]

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("knowledge_citation_density.script_context", fake_script_context):
        result = main(["--days", "30", "--content-type", "blog_post", "--min-per-100", "1", "--max-per-100", "20", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["items"][0]["content_id"] == blog
