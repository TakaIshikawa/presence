"""Tests for deterministic X thread continuity reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from synthesis.thread_continuity import (
    build_thread_continuity_report,
    format_thread_continuity_json,
    parse_thread_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "thread_continuity.py"
spec = importlib.util.spec_from_file_location("thread_continuity_script", SCRIPT_PATH)
thread_continuity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(thread_continuity_script)


def _types(report, thread_index=0):
    return [issue.issue_type for issue in report.threads[thread_index].issues]


def test_strong_continuity_scores_cleanly_with_thread_records():
    report = build_thread_continuity_report(
        [
            {
                "thread_id": "strong",
                "posts": [
                    "Queue audits catch publish drift before a release.",
                    "Those queue audits compare scheduled posts against publish state.",
                    "That publish state becomes the final release review takeaway.",
                ],
            }
        ],
        now=NOW,
    )
    payload = json.loads(format_thread_continuity_json(report))

    assert report.blocking_issue_count == 0
    assert report.threads[0].continuity_score == 100.0
    assert report.totals["aggregate_continuity_score"] == 100.0
    assert payload["artifact_type"] == "thread_continuity"
    assert payload["threads"][0]["thread_id"] == "strong"
    assert payload["threads"][0]["issues"] == []


def test_repeated_openings_include_current_and_previous_post_indexes():
    report = build_thread_continuity_report(
        [
            [
                "The queue audit failed because retries hid publish state.",
                "The queue audit failed when workers skipped the ledger.",
            ]
        ],
        min_overlap=0,
        now=NOW,
    )

    assert _types(report) == ["repeated_opening"]
    issue = report.threads[0].issues[0]
    assert issue.post_index == 2
    assert issue.previous_post_index == 1
    assert report.threads[0].issues_by_type == {"repeated_opening": 1}


def test_abrupt_topic_shift_is_flagged_between_adjacent_posts():
    report = build_thread_continuity_report(
        {
            "threads": [
                {
                    "id": 42,
                    "posts": [
                        "Queue publish retries need a durable audit before launch.",
                        "Garden soil temperature changes how basil seedlings recover.",
                    ],
                }
            ]
        },
        now=NOW,
    )

    assert _types(report) == ["abrupt_topic_shift"]
    issue = report.threads[0].issues[0]
    assert issue.post_index == 2
    assert issue.previous_post_index == 1
    assert "key-term overlap" in issue.detail


def test_missing_transition_cue_and_orphaned_ending_are_reported():
    report = build_thread_continuity_report(
        [
            {
                "thread_id": "weak",
                "posts": [
                    "Queue audits catch publish drift before releases.",
                    "Publish drift needs release review before launch.",
                    "Also publish drift in the queue dashboard should show retries.",
                ],
            }
        ],
        now=NOW,
    )

    assert _types(report) == ["missing_transition_cue", "orphaned_ending"]
    assert report.threads[0].issues_by_type == {
        "missing_transition_cue": 1,
        "orphaned_ending": 1,
    }
    assert report.threads[0].continuity_score == 78.0


def test_single_post_input_has_no_issues_and_plain_text_threads_parse():
    report = build_thread_continuity_report(
        ["A single X post should not be penalized."],
        now=NOW,
    )

    assert report.totals["thread_count"] == 1
    assert report.threads[0].post_count == 1
    assert report.threads[0].issues == ()
    assert parse_thread_text(
        "TWEET 1:\nFirst point.\n\nTWEET 2:\nSecond point."
    ) == ["First point.", "Second point."]


def test_aggregate_scoring_limit_and_cli_json_fixture(tmp_path, capsys):
    fixture = tmp_path / "threads.json"
    fixture.write_text(
        json.dumps(
            {
                "threads": [
                    {
                        "id": "clean",
                        "posts": [
                            "Queue audits catch publish drift before a release.",
                            "Those audits keep publish state visible.",
                            "The takeaway is to review the queue before launch.",
                        ],
                    },
                    {
                        "id": "shift",
                        "posts": [
                            "Queue publish retries need a durable audit before launch.",
                            "Garden soil temperature changes how basil seedlings recover.",
                        ],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = thread_continuity_script.main(
        ["--limit", "2", "--min-overlap", "0.18", str(fixture)]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["filters"]["limit"] == 2
    assert payload["totals"]["thread_count"] == 2
    assert payload["totals"]["threads_with_issues"] == 1
    assert payload["totals"]["aggregate_continuity_score"] == 89.0
    assert payload["threads"][1]["issues_by_type"] == {"abrupt_topic_shift": 1}


def test_argument_validation_and_bad_source():
    with pytest.raises(ValueError, match="min_overlap must be non-negative"):
        build_thread_continuity_report([], min_overlap=-1)
    with pytest.raises(ValueError, match="max_opening_tokens must be positive"):
        build_thread_continuity_report([], max_opening_tokens=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_thread_continuity_report([], limit=0)

    assert thread_continuity_script.main(["--limit", "0", "missing.json"]) == 2
