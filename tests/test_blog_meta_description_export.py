"""Tests for blog meta-description export."""

from __future__ import annotations

from contextlib import contextmanager
import csv
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from output.blog_meta_description_export import (
    export_blog_meta_descriptions,
    export_blog_meta_descriptions_from_markdown,
    format_blog_meta_descriptions_csv,
    format_blog_meta_descriptions_json,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_blog_meta_descriptions.py"
spec = importlib.util.spec_from_file_location("export_blog_meta_descriptions_script", SCRIPT_PATH)
export_blog_meta_descriptions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_blog_meta_descriptions_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_record_export_uses_excerpt_and_enforces_length_bounds():
    rows = export_blog_meta_descriptions(
        [
            {
                "slug": "queue-reliability",
                "title": "Queue Reliability",
                "excerpt": (
                    "Queue Reliability: A practical look at the retry metrics, hold reasons, "
                    "and review signals that keep generated posts moving without hiding failures."
                ),
                "body": "Fallback body should not be needed for this record.",
            }
        ],
        min_chars=80,
        max_chars=150,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.slug == "queue-reliability"
    assert 80 <= row.character_count <= 150
    assert row.suggested_meta_description.startswith("A practical look")
    assert "Queue Reliability:" not in row.suggested_meta_description
    assert row.warnings == ("title_duplication",)


def test_pull_quotes_and_opening_paragraphs_feed_candidates_without_llm():
    rows = export_blog_meta_descriptions(
        [
            {
                "slug": "release-notes",
                "title": "Release Notes",
                "pull_quotes": [
                    "The useful signal is not that a release shipped, but which checks made the rollout boring."
                ],
                "body": (
                    "# Release Notes\n\n"
                    "Opening paragraphs explain how release coverage, queued follow-ups, and source "
                    "links help readers understand what changed before they click through."
                ),
            }
        ],
        min_chars=70,
        max_chars=150,
    )

    assert rows[0].suggested_meta_description == (
        "The useful signal is not that a release shipped, but which checks made the rollout boring."
    )
    assert rows[0].warnings == ()


def test_missing_title_and_short_content_emit_warnings():
    rows = export_blog_meta_descriptions(
        [{"slug": "tiny", "body": "Too short."}],
        min_chars=40,
        max_chars=80,
    )

    assert rows[0].suggested_meta_description == "Too short."
    assert rows[0].warnings == ("missing_title", "too_short_content")


def test_markdown_files_use_frontmatter_and_body(tmp_path):
    draft = tmp_path / "draft-post.md"
    draft.write_text(
        "\n".join(
            [
                "---",
                'title: "Draft Post"',
                'slug: "draft-post"',
                'description: "Draft Post - how frontmatter summaries and opening paragraphs become stable search snippets for generated posts."',
                'pull_quotes: ["A secondary quote should not beat a usable description."]',
                "---",
                "",
                "# Draft Post",
                "",
                "Body content is available as a fallback.",
            ]
        ),
        encoding="utf-8",
    )

    rows = export_blog_meta_descriptions_from_markdown([draft], min_chars=70, max_chars=140)

    assert rows[0].slug == "draft-post"
    assert rows[0].title == "Draft Post"
    assert rows[0].suggested_meta_description.startswith("how frontmatter summaries")
    assert rows[0].warnings == ("title_duplication",)


def test_database_export_reads_generated_blog_posts_and_variant_metadata(db):
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=(
            "TITLE: Database Blog\n\n"
            "The opening paragraph explains how generated blog drafts can carry metadata "
            "forward without changing the publication flow."
        ),
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.upsert_content_variant(
        content_id,
        "blog",
        "post",
        "Variant body",
        metadata={
            "slug": "database-blog",
            "summary": (
                "Generated blog drafts get concise metadata from existing excerpts, quotes, "
                "and opening paragraphs while staying deterministic."
            ),
        },
    )

    rows = export_blog_meta_descriptions(db, min_chars=90, max_chars=150)

    assert rows[0].slug == "database-blog"
    assert rows[0].title == "Database Blog"
    assert 90 <= rows[0].character_count <= 150


def test_json_csv_and_cli_outputs(tmp_path, capsys):
    draft = tmp_path / "cli-post.md"
    draft.write_text(
        "\n".join(
            [
                "---",
                'title: "CLI Post"',
                'slug: "cli-post"',
                "---",
                "",
                "A clear command line export turns generated blog draft text into metadata rows "
                "for search, sharing, and editorial review.",
            ]
        ),
        encoding="utf-8",
    )

    rows = export_blog_meta_descriptions_from_markdown([draft], min_chars=80, max_chars=150)
    payload = json.loads(format_blog_meta_descriptions_json(rows))
    csv_rows = list(csv.DictReader(io.StringIO(format_blog_meta_descriptions_csv(rows))))
    exit_code = export_blog_meta_descriptions_script.main(
        ["--markdown", str(draft), "--min-chars", "80", "--max-chars", "150", "--format", "csv"]
    )
    output_rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))

    assert payload[0]["slug"] == "cli-post"
    assert csv_rows[0]["suggested_meta_description"].startswith("A clear command line export")
    assert exit_code == 0
    assert output_rows[0]["character_count"] == str(rows[0].character_count)


def test_cli_uses_database_context_and_json_output(db, monkeypatch, capsys):
    db.insert_generated_content(
        content_type="blog_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=(
            "TITLE: Context Blog\n\n"
            "Context-backed exports keep the configured database path useful while still "
            "producing deterministic JSON rows for automation."
        ),
        eval_score=7.0,
        eval_feedback="ok",
    )
    monkeypatch.setattr(
        export_blog_meta_descriptions_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = export_blog_meta_descriptions_script.main(
        ["--min-chars", "80", "--max-chars", "150", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload[0]["title"] == "Context Blog"


def test_invalid_bounds_raise():
    with pytest.raises(ValueError, match="min_chars must be less than or equal to max_chars"):
        export_blog_meta_descriptions([], min_chars=200, max_chars=100)
