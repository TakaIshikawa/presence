"""Tests for curated source quote extraction exports."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from knowledge.curated_quote_extract import (
    CuratedSourceRecord,
    extract_quote_candidates,
    format_curated_quotes_csv,
    format_curated_quotes_jsonl,
    is_quote_candidate,
    load_curated_source_records,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_curated_quotes.py"
spec = importlib.util.spec_from_file_location("export_curated_quotes_script", SCRIPT_PATH)
export_curated_quotes_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_curated_quotes_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_knowledge(
    db,
    *,
    source_id: str,
    content: str,
    title: str = "Source Title",
    source_url: str = "https://example.test/source",
    source_type: str = "curated_article",
    author: str = "Ada",
    approved: int = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            license, attribution_required, approved, metadata)
           VALUES (?, ?, ?, ?, ?, ?, 'attribution_required', 1, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            content,
            "summary",
            approved,
            json.dumps({"title": title}),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_extractor_returns_attribution_fields_and_offsets():
    text = (
        "Intro. "
        "Durable synthesis comes from carrying the source claim, its boundary, "
        "and the evidence together before drafting."
    )
    start = text.index("Durable")
    record = CuratedSourceRecord(
        source_id="source-a",
        title="Source A",
        url="https://example.test/a",
        text=text,
        knowledge_id=42,
        source_type="curated_article",
        author="Ada",
    )

    candidates = extract_quote_candidates([record], min_chars=70, max_chars=180)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.source_id == "source-a"
    assert candidate.title == "Source A"
    assert candidate.url == "https://example.test/a"
    assert candidate.knowledge_id == 42
    assert candidate.source_type == "curated_article"
    assert candidate.author == "Ada"
    assert candidate.start_offset == start
    assert candidate.end_offset == len(text)
    assert candidate.quote.startswith("Durable synthesis")


def test_filters_short_long_boilerplate_and_low_signal_lines():
    good = (
        "Reviewable quotes work best when they preserve one complete claim "
        "with enough context to verify it later."
    )
    too_short = "Useful but too short."
    too_long = " ".join(["This overly broad sentence keeps adding detail"] * 18) + "."

    assert is_quote_candidate(good, min_chars=70, max_chars=160)
    assert not is_quote_candidate(too_short, min_chars=70, max_chars=160)
    assert not is_quote_candidate(too_long, min_chars=70, max_chars=160)
    assert not is_quote_candidate(
        "Subscribe to the weekly briefing to receive every new source note in your browser.",
        min_chars=70,
        max_chars=160,
    )
    assert not is_quote_candidate(
        "2026 / 05 / 01 ----- https://example.test/source ----- 1234567890",
        min_chars=40,
        max_chars=160,
    )


def test_extractor_deduplicates_candidate_lines_across_sources():
    quote = (
        "Source-backed drafting improves when repeated claims are exported "
        "once and reviewed with their attribution attached."
    )
    records = [
        CuratedSourceRecord("a", "A", "https://example.test/a", quote),
        CuratedSourceRecord(
            "b",
            "B",
            "https://example.test/b",
            "  Source backed drafting improves when repeated claims are exported once "
            "and reviewed with their attribution attached!  ",
        ),
    ]

    candidates = extract_quote_candidates(records, min_chars=70, max_chars=180)

    assert len(candidates) == 1
    assert candidates[0].source_id == "a"


def test_database_loader_uses_curated_sources_and_metadata_title(db):
    quote = (
        "Curated material becomes easier to reuse when each candidate carries "
        "the original source URL and a human-readable title."
    )
    knowledge_id = _insert_knowledge(
        db,
        source_id="article-1",
        content=quote,
        title="Attribution Matters",
        source_url="https://example.test/article-1",
    )
    _insert_knowledge(
        db,
        source_id="own",
        content=quote,
        source_type="own_post",
    )
    _insert_knowledge(
        db,
        source_id="draft",
        content=quote,
        approved=0,
    )

    records = load_curated_source_records(db)
    candidates = extract_quote_candidates(records, min_chars=70, max_chars=180)

    assert [record.source_id for record in records] == ["article-1"]
    assert candidates[0].knowledge_id == knowledge_id
    assert candidates[0].title == "Attribution Matters"
    assert candidates[0].url == "https://example.test/article-1"


def test_formatters_emit_deterministic_jsonl_and_csv():
    record = CuratedSourceRecord(
        "source-a",
        "Source A",
        "https://example.test/a",
        "A concise quote candidate should be long enough to preserve context "
        "but short enough for a reviewer to scan quickly.",
        knowledge_id=7,
        source_type="curated_newsletter",
    )
    candidates = extract_quote_candidates([record], min_chars=70, max_chars=180)

    jsonl = format_curated_quotes_jsonl(candidates)
    csv_text = format_curated_quotes_csv(candidates)
    payload = json.loads(jsonl)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["source_id"] == "source-a"
    assert payload["start_offset"] == 0
    assert csv_text.startswith("source_id,knowledge_id,source_type,title,url,author,quote")
    assert "curated_newsletter" in csv_text


def test_cli_reads_fixture_jsonl_and_outputs_jsonl_and_csv(tmp_path, capsys):
    fixture = tmp_path / "records.jsonl"
    fixture.write_text(
        json.dumps(
            {
                "source_id": "fixture-a",
                "title": "Fixture A",
                "url": "https://example.test/fixture-a",
                "text": (
                    "Fixture quote exports should be stable so synthesis prompts "
                    "can consume reviewed source material without guessing."
                ),
            }
        )
        + "\n"
    )

    exit_code = export_curated_quotes_script.main(
        [str(fixture), "--min-chars", "70", "--max-chars", "180", "--format", "jsonl"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["source_id"] == "fixture-a"
    assert payload["title"] == "Fixture A"
    assert payload["url"] == "https://example.test/fixture-a"

    exit_code = export_curated_quotes_script.main(
        [str(fixture), "--min-chars", "70", "--max-chars", "180", "--format", "csv"]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert output.splitlines()[0].startswith("source_id,knowledge_id")
    assert "fixture-a" in output


def test_cli_reads_database_when_no_fixture_paths(db, monkeypatch, capsys):
    _insert_knowledge(
        db,
        source_id="db-source",
        content=(
            "Database quote exports should include approved curated knowledge "
            "records with source identifiers and titles intact."
        ),
        title="DB Source",
    )
    monkeypatch.setattr(
        export_curated_quotes_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = export_curated_quotes_script.main(
        ["--min-chars", "70", "--max-chars", "180"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["source_id"] == "db-source"
    assert payload["title"] == "DB Source"
