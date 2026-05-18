"""Tests for knowledge citation yield reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from evaluation.knowledge_citation_yield import build_knowledge_citation_yield_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_citation_yield.py"
spec = importlib.util.spec_from_file_location("knowledge_citation_yield_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_computes_yield_by_domain_and_format_with_metadata_and_snippet_matches():
    retrievals = [
        {
            "generation_id": "g1",
            "format": "thread",
            "source_url": "https://example.com/research/a",
            "snippet": "Benchmark harness compares citation coverage against generated copy before publication.",
        },
        {
            "generation_id": "g2",
            "format": "thread",
            "source_domain": "docs.example.org",
            "citation_label": "Docs Example",
            "snippet": "Release notes describe the ingestion queue changes for embeddings.",
        },
        {
            "generation_id": "g3",
            "format": "blog",
            "source_url": "https://unused.test/item",
            "snippet": "A completely absent source should remain unused by generated content.",
        },
    ]
    outputs = [
        {"generation_id": "g1", "post_text": "See https://example.com/research/a for the benchmark details."},
        {
            "generation_id": "g2",
            "post_text": "The release notes describe ingestion queue changes for embeddings in the new flow.",
        },
        {"generation_id": "g3", "post_text": "This post relies on a different source."},
    ]

    report = build_knowledge_citation_yield_report(retrievals, outputs, now=NOW)

    assert report["summary"]["used_count"] == 2
    assert report["summary"]["unused_count"] == 1
    thread_example = next(group for group in report["by_domain_and_format"] if group["domain"] == "example.com")
    assert thread_example["format"] == "thread"
    assert thread_example["citation_yield_rate"] == 1.0
    unused = next(group for group in report["by_domain_and_format"] if group["domain"] == "unused.test")
    assert unused["used_count"] == 0
    assert unused["unused_count"] == 1


def test_flags_retrieved_items_repeatedly_unused_across_generations():
    retrievals = [
        {
            "generation_id": "g1",
            "format": "thread",
            "source_url": "https://stale.example/item",
            "snippet": "Unused source text about legacy launch notes.",
            "output_text": "No legacy launch material here.",
        },
        {
            "generation_id": "g2",
            "format": "blog",
            "source_url": "https://stale.example/item",
            "snippet": "Unused source text about legacy launch notes.",
            "output_text": "Still no mention.",
        },
    ]

    report = build_knowledge_citation_yield_report(retrievals, now=NOW)

    assert report["flagged_repeatedly_unused"][0]["domain"] == "stale.example"
    assert report["flagged_repeatedly_unused"][0]["unused_generations"] == ["g1", "g2"]
    assert report["flagged_repeatedly_unused"][0]["format_counts"] == {"blog": 1, "thread": 1}


def test_cli_reads_json_files_and_outputs_table(tmp_path, capsys):
    retrievals_path = tmp_path / "retrievals.json"
    outputs_path = tmp_path / "outputs.json"
    retrievals_path.write_text(
        json.dumps(
            [
                {
                    "generation_id": "g1",
                    "format": "post",
                    "domain": "example.com",
                    "snippet": "source snippet with unique citation material",
                }
            ]
        ),
        encoding="utf-8",
    )
    outputs_path.write_text(json.dumps([{"generation_id": "g1", "text": "example.com is cited."}]), encoding="utf-8")

    assert script.main(["--retrievals-json", str(retrievals_path), "--outputs-json", str(outputs_path), "--table"]) == 0
    output = capsys.readouterr().out
    assert "Knowledge Citation Yield" in output
    assert "domain=example.com" in output
