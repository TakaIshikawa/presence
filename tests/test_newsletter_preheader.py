"""Tests for deterministic newsletter preheader selection."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from output.newsletter_preheader import (
    extract_newsletter_fields_from_markdown,
    format_preheader_selection_json,
    format_preheader_selection_text,
    generate_preheader_candidates,
    score_preheader_candidate,
    select_newsletter_preheader,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "select_newsletter_preheader.py"
)
spec = importlib.util.spec_from_file_location(
    "select_newsletter_preheader_script",
    SCRIPT_PATH,
)
select_newsletter_preheader_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(select_newsletter_preheader_script)


def _payload() -> dict:
    return {
        "subject": "Weekly AI tooling notes",
        "title": "Smaller AI tools with faster review loops",
        "sections": [
            {
                "heading": "Review loops",
                "summary": (
                    "A concrete checklist for shipping smaller AI tools without "
                    "slowing operator review."
                ),
            },
            {
                "heading": "Source hygiene",
                "summary": "Fresh links and attribution checks keep the issue reproducible.",
            },
        ],
        "top_links": [
            {
                "label": "Checklist for deterministic eval review",
                "url": "https://example.com/evals",
            },
            {
                "label": "Source freshness report",
                "url": "https://example.com/freshness",
            },
        ],
        "cta": {"label": "Read the implementation notes"},
        "source_freshness_metadata": {
            "fresh_source_count": 4,
            "newest_source_at": "2026-05-01",
        },
    }


def test_candidate_scoring_penalizes_generic_and_duplicate_subject_text():
    duplicate = score_preheader_candidate(
        "Weekly AI tooling notes",
        subject="Weekly AI tooling notes",
        min_length=10,
        max_length=80,
    )
    generic = score_preheader_candidate(
        "Don't miss this weekly update and click here to learn more",
        subject="Weekly AI tooling notes",
        min_length=10,
        max_length=80,
    )
    specific = score_preheader_candidate(
        "Review the 4-step checklist for shipping smaller AI tools with clearer attribution.",
        subject="Weekly AI tooling notes",
        min_length=10,
        max_length=90,
    )

    assert duplicate.diagnostics["repetition_penalty"] > 0
    assert generic.diagnostics["generic_hits"]
    assert specific.score > generic.score
    assert specific.score > duplicate.score


def test_structured_payload_generates_bounded_candidates_and_selects_best():
    selection = select_newsletter_preheader(_payload(), min_length=45, max_length=95)

    assert 45 <= len(selection.selected.text) <= 95
    assert selection.selected.source in {
        "section_summary",
        "section_body",
        "section_roundup",
        "top_link",
        "top_links",
        "cta",
        "source_freshness",
    }
    assert all(45 <= len(candidate.text) <= 95 for candidate in selection.candidates)
    assert selection.candidates == tuple(
        generate_preheader_candidates(_payload(), min_length=45, max_length=95)
    )


def test_markdown_extraction_builds_fields_links_and_candidates():
    markdown = """Subject: AI tooling notes

# Smaller AI tools

Review the operator checklist for shipping focused features with traceable sources.

## Fresh links

Compare [the eval guide](https://example.com/evals) with
[the freshness report](https://example.com/fresh).

Read the implementation notes before the next send.
"""
    fields = extract_newsletter_fields_from_markdown(markdown)
    selection = select_newsletter_preheader(markdown, min_length=45, max_length=100)

    assert fields["subject"] == "AI tooling notes"
    assert fields["title"] == "Smaller AI tools"
    assert fields["sections"][0]["heading"] == "Smaller AI tools"
    assert fields["top_links"][0]["label"] == "the eval guide"
    assert selection.selected.text
    assert any(
        "operator checklist" in candidate.text
        for candidate in selection.candidates
    )


def test_formatting_helpers_are_cli_friendly_and_stable():
    selection = select_newsletter_preheader(_payload(), min_length=45, max_length=95)
    text = format_preheader_selection_text(selection)
    payload = json.loads(format_preheader_selection_json(selection))

    assert text.startswith("Selected preheader: ")
    assert "Candidates:" in text
    assert payload["selected"]["text"] == selection.selected.text
    assert list(payload.keys()) == sorted(payload.keys())


def test_cli_reads_json_file_outputs_json_and_rejects_malformed_input(tmp_path, capsys):
    payload_path = tmp_path / "draft.json"
    payload_path.write_text(json.dumps(_payload()), encoding="utf-8")

    result = select_newsletter_preheader_script.main(
        [
            str(payload_path),
            "--format",
            "json",
            "--min-length",
            "45",
            "--max-length",
            "95",
        ]
    )

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert 45 <= len(output["selected"]["text"]) <= 95
    assert output["candidates"]

    bad_path = tmp_path / "bad.json"
    bad_path.write_text('{"subject": ', encoding="utf-8")
    result = select_newsletter_preheader_script.main([str(bad_path)])

    captured = capsys.readouterr()
    assert result == 1
    assert "malformed JSON input" in captured.err
