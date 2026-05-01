"""Tests for platform caption policy linting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.caption_policy_linter import (
    RULE_CTA_DENSITY,
    RULE_DUPLICATE_MENTION,
    RULE_EMPTY_THREAD_PART,
    RULE_MISSING_SELECTED_VARIANT,
    RULE_TOO_MANY_HASHTAGS,
    RULE_TOO_MANY_LINKS,
    format_json_report,
    format_text_report,
    lint_caption_policy,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "lint_captions.py"
spec = importlib.util.spec_from_file_location("lint_captions_script", SCRIPT_PATH)
lint_captions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(lint_captions_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(
    db,
    text: str,
    *,
    content_type: str = "x_post",
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8,
        eval_feedback="ok",
    )


def _queue_item(db, content_id: int, platform: str = "x") -> int:
    cursor = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status)
           VALUES (?, '2026-05-01T12:00:00+00:00', ?, 'queued')""",
        (content_id, platform),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_lints_original_and_selected_platform_variant(db):
    content_id = _insert_content(
        db,
        "Read more and follow @Dev @dev https://a.test https://b.test",
    )
    db.upsert_content_variant(
        content_id,
        "x",
        "post",
        "Try it and subscribe #one #two #three #four #five",
    )
    db.select_content_variant(content_id, "x", "post")

    report = lint_caption_policy(db, content_id=content_id, platform="x")
    codes = [issue["code"] for issue in report["issues"]]

    assert report["counts"]["subjects"] == 2
    assert RULE_DUPLICATE_MENTION in codes
    assert RULE_TOO_MANY_LINKS in codes
    assert RULE_CTA_DENSITY in codes
    assert RULE_TOO_MANY_HASHTAGS in codes
    assert any(issue["source"] == "variant" for issue in report["issues"])


def test_missing_selected_variant_warns_and_strict_promotes(db):
    content_id = _insert_content(db, "Plain copy")
    db.upsert_content_variant(content_id, "bluesky", "post", "Bluesky copy")

    normal = lint_caption_policy(db, content_id=content_id, platform="bluesky")
    strict = lint_caption_policy(
        db,
        content_id=content_id,
        platform="bluesky",
        strict=True,
    )

    assert normal["issues"][0]["code"] == RULE_MISSING_SELECTED_VARIANT
    assert normal["issues"][0]["severity"] == "warning"
    assert strict["issues"][0]["code"] == RULE_MISSING_SELECTED_VARIANT
    assert strict["issues"][0]["severity"] == "error"
    assert strict["status"] == "blocked"


def test_empty_thread_parts_are_blocking_errors(db):
    content_id = _insert_content(
        db,
        "TWEET 1:\nFirst\n\nTWEET 2:\n\nTWEET 3:\nThird",
        content_type="x_thread",
    )

    report = lint_caption_policy(db, content_id=content_id, platform="x")
    empty = [issue for issue in report["issues"] if issue["code"] == RULE_EMPTY_THREAD_PART]

    assert empty[0]["severity"] == "error"
    assert empty[0]["segment_index"] == 2
    assert empty[0]["segment_total"] == 3


def test_queue_all_expands_to_supported_platforms(db):
    content_id = _insert_content(db, "Plain copy")
    queue_id = _queue_item(db, content_id, platform="all")

    report = lint_caption_policy(db, queue_id=queue_id, platform="all")

    assert report["platforms"] == ["x", "bluesky"]
    assert report["queue"]["queue_id"] == queue_id


def test_queue_single_platform_limits_all_filter(db):
    content_id = _insert_content(db, "Plain copy")
    queue_id = _queue_item(db, content_id, platform="bluesky")

    report = lint_caption_policy(db, queue_id=queue_id, platform="all")

    assert report["platforms"] == ["bluesky"]


def test_text_and_json_formatters_are_deterministic(db):
    content_id = _insert_content(db, "Read more and follow now")
    report = lint_caption_policy(db, content_id=content_id, platform="x")

    assert json.loads(format_json_report(report))["artifact_type"] == "caption_policy_lint"
    assert format_text_report(report) == "\n".join(
        [
            "Caption Policy Lint",
            f"Content: {content_id} (x_post)",
            "Filters: queue_id=- platform=x strict=no",
            "Counts: subjects=1 issues=1 warnings=1 blocking_errors=0",
            "",
            "Issues",
            "  - warning CAP_CTA_DENSITY [x generated segment=1/1]: 2 call-to-action phrases in one post.",
        ]
    )


def test_cli_supports_json_content_level_checks(db, monkeypatch, capsys):
    content_id = _insert_content(db, "Plain copy")
    monkeypatch.setattr(
        lint_captions_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = lint_captions_script.main(
        [
            "--content-id",
            str(content_id),
            "--platform",
            "x",
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["content"]["id"] == content_id


def test_cli_supports_queue_level_checks(db, monkeypatch, capsys):
    content_id = _insert_content(db, "Plain copy")
    queue_id = _queue_item(db, content_id, platform="x")
    monkeypatch.setattr(
        lint_captions_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = lint_captions_script.main(
        [
            "--queue-id",
            str(queue_id),
            "--platform",
            "all",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert f"Queue: {queue_id} x queued 2026-05-01T12:00:00+00:00" in output
