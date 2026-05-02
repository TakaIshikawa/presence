"""Tests for X thread continuity audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from output.x_thread_continuity_audit import (
    audit_x_thread_content,
    build_x_thread_continuity_audit_report,
    format_x_thread_continuity_audit_json,
    format_x_thread_continuity_audit_text,
    parse_ordered_thread_posts,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "x_thread_continuity_audit.py"
spec = importlib.util.spec_from_file_location("x_thread_continuity_audit_script", SCRIPT_PATH)
x_thread_continuity_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(x_thread_continuity_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    content: str,
    *,
    content_type: str = "x_thread",
    published: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ? WHERE id = ?",
        (published, content_id),
    )
    db.conn.commit()
    return content_id


def _types(findings):
    return [finding.finding_type for finding in findings]


def test_healthy_thread_and_single_post_content_have_no_findings(db):
    healthy = (
        "TWEET 1:\nQueue audits catch publish drift before a release.\n\n"
        "TWEET 2:\nThose queue audits compare scheduled posts against publish state.\n\n"
        "TWEET 3:\nThat publish state makes the release review easier to trust."
    )
    single = "A single generated X post should not be treated as a broken thread."
    _content(db, healthy)
    _content(db, single)

    report = build_x_thread_continuity_audit_report(db, now=NOW)

    assert report.totals["threads_scanned"] == 2
    assert report.findings == ()
    assert audit_x_thread_content(single, thread_id=99) == []


def test_flags_overlong_posts_with_fix_and_post_index():
    content = "TWEET 1:\n" + ("x" * 281) + "\n\nTWEET 2:\nSecond x detail"

    findings = audit_x_thread_content(content, thread_id=12)

    assert _types(findings) == ["overlong_post"]
    assert findings[0].thread_id == 12
    assert findings[0].post_index == 1
    assert findings[0].severity == "error"
    assert "Trim post 1" in findings[0].suggested_fix


def test_flags_duplicate_openings():
    content = (
        "TWEET 1:\nThe queue audit failed because the queue state drifted.\n\n"
        "TWEET 2:\nThe queue audit failed when retries hid the publish state."
    )

    findings = audit_x_thread_content(content, thread_id=7, min_overlap=0)

    assert _types(findings) == ["duplicate_opening"]
    assert findings[0].post_index == 2
    assert "post 1" in findings[0].detail


def test_flags_missing_order_metadata_for_unnumbered_multi_post_text():
    content = "Queue audit caught the missing publish state.\n\nRetry audit explained the failed handoff."

    posts, order_issue = parse_ordered_thread_posts(content)
    findings = audit_x_thread_content(content, thread_id=8, min_overlap=0)

    assert [post.index for post in posts] == [1, 2]
    assert order_issue == "Multi-post thread content is missing explicit ordering metadata."
    assert _types(findings) == ["missing_order_metadata"]
    assert findings[0].suggested_fix.startswith("Add sequential TWEET N")


def test_flags_non_sequential_markers_as_missing_order_metadata():
    content = "TWEET 1:\nFirst publish queue point.\n\nTWEET 3:\nSecond publish queue point."

    findings = audit_x_thread_content(content, thread_id=9, min_overlap=0)

    assert _types(findings) == ["missing_order_metadata"]
    assert findings[0].detail == "Thread markers must be sequential starting at TWEET 1."


def test_flags_low_continuity_transition():
    content = (
        "TWEET 1:\nQueue publish retries need a durable queue audit before launch.\n\n"
        "TWEET 2:\nGarden soil temperature changes how basil seedlings recover."
    )

    findings = audit_x_thread_content(content, thread_id=10)

    assert _types(findings) == ["low_continuity_transition"]
    assert findings[0].post_index == 2
    assert "lexical overlap" in findings[0].detail
    assert "bridge phrase" in findings[0].suggested_fix


def test_flags_broken_reply_chain_metadata_for_structured_posts():
    content = json.dumps(
        {
            "posts": [
                {"index": 1, "text": "Publish queues need an audit before release."},
                {"index": 2, "text": "That audit keeps publish queues coherent."},
                {
                    "index": 3,
                    "text": "Those publish queues then produce safer release notes.",
                    "in_reply_to_tweet_id": "tw-2",
                },
            ]
        }
    )

    findings = audit_x_thread_content(content, thread_id=11, min_overlap=0)

    assert _types(findings) == ["broken_reply_chain_metadata"]
    assert findings[0].post_index == 2
    assert "reply-chain metadata" in findings[0].detail


def test_json_text_and_cli_report_problematic_thread_drafts(db, monkeypatch, capsys):
    content_id = _content(
        db,
        "TWEET 1:\nQueue retries need clearer publish state.\n\n"
        "TWEET 2:\nGarden seedlings need warmer soil.",
    )
    _content(
        db,
        "TWEET 1:\nPublished thread should be skipped.\n\nTWEET 2:\nUnrelated topic.",
        published=1,
    )
    report = build_x_thread_continuity_audit_report(db, now=NOW)

    payload = json.loads(format_x_thread_continuity_audit_json(report))
    text = format_x_thread_continuity_audit_text(report)

    assert payload["artifact_type"] == "x_thread_continuity_audit"
    assert payload["findings"][0]["thread_id"] == content_id
    assert payload["findings"][0]["suggested_fix"]
    assert "X Thread Continuity Audit" in text
    assert "low_continuity_transition" in text

    monkeypatch.setattr(
        x_thread_continuity_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        x_thread_continuity_audit_script,
        "build_x_thread_continuity_audit_report",
        lambda db_arg, **kwargs: build_x_thread_continuity_audit_report(
            db_arg,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = x_thread_continuity_audit_script.main(
        ["--limit", "5", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_payload["totals"]["threads_scanned"] == 1
    assert cli_payload["filters"]["limit"] == 5
    assert cli_payload["findings"][0]["thread_id"] == content_id


def test_invalid_arguments_and_missing_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_x_thread_continuity_audit_report(conn, now=NOW)

    assert report.missing_tables == ("generated_content",)
    assert report.findings == ()

    with pytest.raises(ValueError, match="limit must be positive"):
        build_x_thread_continuity_audit_report(conn, limit=0)
    with pytest.raises(ValueError, match="max_chars must be positive"):
        build_x_thread_continuity_audit_report(conn, max_chars=0)
    with pytest.raises(ValueError, match="min_overlap must be non-negative"):
        build_x_thread_continuity_audit_report(conn, min_overlap=-1)

    assert x_thread_continuity_audit_script.main(["--limit", "0"]) == 2
