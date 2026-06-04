from __future__ import annotations

from evaluation.blog_draft_intro_hook_weakness import build_blog_draft_intro_hook_weakness_report


def test_generic_intro_is_flagged():
    report = build_blog_draft_intro_hook_weakness_report([
        {"draft_id": "d1", "body": "In today's fast-paced world, content strategy is important for every team."}
    ])
    row = report["findings"][0]
    assert "generic_framing" in row["reason_codes"]
    assert "missing_concrete_artifact" in row["reason_codes"]
    assert row["severity"] == "high"


def test_concrete_intro_is_ignored():
    report = build_blog_draft_intro_hook_weakness_report([
        {"draft_id": "d2", "body": "The migration report exposed a 400 ms latency regression in the approval workflow."}
    ])
    assert report["findings"] == []


def test_short_and_missing_intros_are_flagged():
    report = build_blog_draft_intro_hook_weakness_report([
        {"draft_id": "short", "body": "Release notes."},
        {"draft_id": "missing", "body": ""},
    ])
    reasons = {row["draft_id"]: row["reason_codes"] for row in report["findings"]}
    assert "too_short" in reasons["short"]
    assert "missing_intro" in reasons["missing"]
