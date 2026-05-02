"""Tests for exporting blog pull-quote ideas."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.blog_pull_quote_export import (
    SOURCE_NAME,
    export_blog_pull_quotes,
    extract_blog_pull_quote_candidates,
    format_blog_pull_quotes_json,
    format_blog_pull_quotes_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_blog_pull_quotes.py"
spec = importlib.util.spec_from_file_location("export_blog_pull_quotes_script", SCRIPT_PATH)
export_blog_pull_quotes_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_blog_pull_quotes_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _blog_post(db, content: str, *, created_at: str = "2026-04-30T12:00:00+00:00") -> int:
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return content_id


def test_extracts_deterministic_candidates_with_source_id_and_character_counts(db):
    older_id = _blog_post(
        db,
        "This older paragraph is long enough to become a reusable pull quote for a social post.",
        created_at="2026-04-28T12:00:00+00:00",
    )
    newer_id = _blog_post(
        db,
        "\n\n".join(
            [
                "This newer paragraph captures a concrete lesson about reviewable operations and steady reuse.",
                "This second paragraph should be held behind the limit even though it also qualifies.",
            ]
        ),
        created_at="2026-04-30T12:00:00+00:00",
    )

    candidates = extract_blog_pull_quote_candidates(
        db,
        days=7,
        min_chars=60,
        max_chars=120,
        limit=2,
        now=NOW,
    )

    assert [candidate.source_content_id for candidate in candidates] == [newer_id, newer_id]
    assert candidates[0].quote.startswith("This newer paragraph")
    assert candidates[0].char_count == len(candidates[0].quote)
    assert candidates[0].position == 1
    assert older_id not in [candidate.source_content_id for candidate in candidates]


def test_filters_boilerplate_headings_code_blocks_and_link_only_lines(db):
    _blog_post(
        db,
        "\n".join(
            [
                "# A markdown heading that should not become a quote",
                "",
                "Subscribe to the newsletter for more operational notes.",
                "",
                "```",
                "This code block text is long enough but must not become reusable pull quote material.",
                "```",
                "",
                "https://example.com/only-a-link",
                "",
                "[Only a link](https://example.com)",
                "",
                "The useful line explains how small review loops turn approved long-form material into repeatable social prompts.",
            ]
        ),
    )

    candidates = extract_blog_pull_quote_candidates(
        db,
        days=7,
        min_chars=70,
        max_chars=130,
        now=NOW,
    )

    assert [candidate.quote for candidate in candidates] == [
        "The useful line explains how small review loops turn approved long-form material into repeatable social prompts."
    ]


def test_splits_long_paragraphs_into_sentence_sized_quotes(db):
    _blog_post(
        db,
        (
            "This opening sentence is intentionally too short. "
            "A reliable exporter should keep the sentence that fits the configured bounds and leave the surrounding material behind. "
            "A short closer."
        ),
    )

    candidates = extract_blog_pull_quote_candidates(
        db,
        days=7,
        min_chars=90,
        max_chars=130,
        now=NOW,
    )

    assert [candidate.quote for candidate in candidates] == [
        "A reliable exporter should keep the sentence that fits the configured bounds and leave the surrounding material behind."
    ]


def test_create_ideas_writes_deduplicated_content_ideas(db):
    content_id = _blog_post(
        db,
        "A pull quote becomes more useful when the source content id and character budget travel with it.",
    )

    first = export_blog_pull_quotes(
        db,
        days=7,
        min_chars=60,
        max_chars=120,
        create_ideas=True,
        now=NOW,
    )
    second = export_blog_pull_quotes(
        db,
        days=7,
        min_chars=60,
        max_chars=120,
        create_ideas=True,
        now=NOW,
    )

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    ideas = db.get_content_ideas(status=None, limit=10)
    assert len(ideas) == 1
    assert ideas[0]["source"] == SOURCE_NAME
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source_content_id"] == content_id
    assert metadata["char_count"] == len(metadata["quote"])


def test_default_export_is_read_only_and_formatters_are_stable(db):
    _blog_post(
        db,
        "Approved long-form material can become concise social prompts when the quote is specific and self-contained.",
    )

    results = export_blog_pull_quotes(
        db,
        days=7,
        min_chars=60,
        max_chars=120,
        now=NOW,
    )
    payload = json.loads(format_blog_pull_quotes_json(results))
    text = format_blog_pull_quotes_text(results)

    assert [result.status for result in results] == ["candidate"]
    assert db.get_content_ideas(status=None, limit=10) == []
    assert payload[0]["status"] == "candidate"
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME
    assert list(payload[0].keys()) == sorted(payload[0].keys())
    assert text.startswith("created=0 candidate=1 skipped=0")
    assert "Approved long-form material" in text


def test_cli_supports_requested_flags_and_json_output(db, monkeypatch, capsys):
    _blog_post(
        db,
        "CLI output should expose the same deterministic pull quote candidate and leave ideas untouched by default.",
    )
    monkeypatch.setattr(export_blog_pull_quotes_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        export_blog_pull_quotes_script,
        "export_blog_pull_quotes",
        lambda db, **kwargs: export_blog_pull_quotes(db, now=NOW, **kwargs),
    )

    exit_code = export_blog_pull_quotes_script.main(
        [
            "--days",
            "7",
            "--min-chars",
            "60",
            "--max-chars",
            "120",
            "--limit",
            "5",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload[0]["status"] == "candidate"
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME
    assert db.get_content_ideas(status=None, limit=10) == []
