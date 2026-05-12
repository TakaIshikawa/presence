"""Tests for prompt template variable coverage analysis."""

from __future__ import annotations

import json

from synthesis.prompt_template_variable_coverage import (
    build_prompt_template_variable_coverage_report,
    format_prompt_template_variable_coverage_json,
    format_prompt_template_variable_coverage_text,
)


def test_reports_placeholder_inventory_and_missing_required(tmp_path):
    prompt = tmp_path / "x_post_v2.txt"
    prompt.write_text("Write {topic} for {audience}. Use {topic}.", encoding="utf-8")

    report = build_prompt_template_variable_coverage_report(
        [prompt],
        required_placeholders_by_content_type={
            "x_post": ["topic", "audience", "source_packet"]
        },
        known_placeholders_by_content_type={
            "x_post": ["topic", "audience", "source_packet"]
        },
        companion_test_paths=["test_x_post_v2.py"],
    )

    row = report.rows[0]
    assert row.content_type == "x_post"
    assert row.placeholders == ("audience", "topic")
    assert row.duplicate_placeholders == ("topic",)
    assert row.missing_required_placeholders == ("source_packet",)
    assert row.has_companion_test is True


def test_unknown_placeholders_and_missing_tests_are_stable(tmp_path):
    prompt = tmp_path / "blog_post.txt"
    prompt.write_text("Draft from {source} with {surprise}.", encoding="utf-8")

    report = build_prompt_template_variable_coverage_report(
        [prompt],
        required_placeholders_by_content_type={"blog_post": ["source"]},
        known_placeholders_by_content_type={"blog_post": ["source"]},
    )
    payload = json.loads(format_prompt_template_variable_coverage_json(report))
    text = format_prompt_template_variable_coverage_text(report)

    assert payload["artifact_type"] == "prompt_template_variable_coverage"
    assert payload["rows"][0]["unknown_placeholders"] == ["surprise"]
    assert payload["rows"][0]["missing_companion_test"] is True
    assert "unknown=surprise" in text


def test_accepts_only_explicit_template_paths(tmp_path):
    first = tmp_path / "a.txt"
    second = tmp_path / "b.txt"
    first.write_text("{one}", encoding="utf-8")
    second.write_text("{two}", encoding="utf-8")

    report = build_prompt_template_variable_coverage_report([first])

    assert len(report.rows) == 1
    assert report.rows[0].template_path == str(first)
