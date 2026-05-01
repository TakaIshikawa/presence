"""Tests for the queued-content redaction publish guard."""

from __future__ import annotations

import importlib.util
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.redaction_publish_guard import (
    build_redaction_publish_guard_report,
    export_to_json,
    format_text_report,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "check_redaction_publish.py"
spec = importlib.util.spec_from_file_location("check_redaction_publish_script", SCRIPT_PATH)
check_redaction_publish_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(check_redaction_publish_script)


@contextmanager
def _script_context(config, db):
    yield config, db


def _queued_content(
    db,
    content: str,
    *,
    platform: str = "x",
    status: str = "queued",
) -> tuple[int, int]:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-25T12:00:00+00:00",
        platform=platform,
    )
    if status != "queued":
        db.conn.execute("UPDATE publish_queue SET status = ? WHERE id = ?", (status, queue_id))
        db.conn.commit()
    return queue_id, content_id


def _queue_rows(db) -> list[dict]:
    return [
        dict(row)
        for row in db.conn.execute("SELECT * FROM publish_queue ORDER BY id").fetchall()
    ]


def test_blocks_queued_content_with_sanitized_secret_path_email_and_custom_matches(db):
    raw_values = {
        "github": "ghp_abcdefghijklmnopqrstuvwxyz123456",
        "path": "/Users/taka/Project/private/.env",
        "email": "dev@example.com",
        "ticket": "ticket-1234",
    }
    queue_id, content_id = _queued_content(
        db,
        (
            f"Do not publish {raw_values['github']} from {raw_values['path']} "
            f"or email {raw_values['email']} about {raw_values['ticket']}."
        ),
    )
    before = _queue_rows(db)

    report = build_redaction_publish_guard_report(
        db,
        patterns=[
            *(
                {
                    "name": "ticket",
                    "pattern": r"ticket-\d+",
                    "placeholder": "[REDACTED_TICKET]",
                },
            ),
        ],
    )
    default_report = build_redaction_publish_guard_report(
        db,
        patterns=[
            {
                "name": "github_token",
                "pattern": r"\bghp_[A-Za-z0-9_]{20,}\b",
                "placeholder": "[REDACTED_SECRET]",
            },
            {
                "name": "macos_user_path",
                "pattern": r"(?<!\w)/Users/[^/\s]+(?:/[^\s,;:'\")\]]+)*",
                "placeholder": "[REDACTED_PATH]",
            },
            {
                "name": "email",
                "pattern": r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
                "placeholder": "[REDACTED_EMAIL]",
                "flags": "IGNORECASE",
            },
            {
                "name": "ticket",
                "pattern": r"ticket-\d+",
                "placeholder": "[REDACTED_TICKET]",
            },
        ],
    )

    assert report.blocked_count == 1
    assert report.items[0].queue_id == queue_id
    assert report.items[0].content_id == content_id
    assert report.items[0].status == "blocked"
    assert default_report.items[0].matched_rule_codes == [
        "email",
        "github_token",
        "macos_user_path",
        "ticket",
    ]
    payload = export_to_json(default_report)
    for raw_value in raw_values.values():
        assert raw_value not in payload
    assert "[REDACTED_SECRET]" in payload
    assert "[REDACTED_PATH]" in payload
    assert "[REDACTED_EMAIL]" in payload
    assert "[REDACTED_TICKET]" in payload
    assert _queue_rows(db) == before


def test_clean_queued_content_passes_with_stable_text_and_json(db):
    queue_id, content_id = _queued_content(db, "A clean launch note with public context.")

    report = build_redaction_publish_guard_report(db)
    payload = json.loads(export_to_json(report))
    text = format_text_report(report)

    assert report.blocked_count == 0
    assert report.passed_count == 1
    assert payload["artifact_type"] == "redaction_publish_guard"
    assert payload["blocked_count"] == 0
    assert payload["items"][0]["queue_id"] == queue_id
    assert payload["items"][0]["content_id"] == content_id
    assert payload["items"][0]["status"] == "passed"
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Redaction Publish Guard" in text
    assert f"queue #{queue_id} content #{content_id} x: passed" in text
    assert "rules=-" in text


def test_filters_to_queued_rows_queue_id_and_platform(db):
    queued_x, _ = _queued_content(db, "token=abcdefghijklmnopqrstuvwxyz", platform="x")
    queued_bsky, _ = _queued_content(db, "token=abcdefghijklmnopqrstuvwxyz", platform="bluesky")
    failed_x, _ = _queued_content(
        db,
        "token=abcdefghijklmnopqrstuvwxyz",
        platform="x",
        status="failed",
    )

    platform_report = build_redaction_publish_guard_report(db, platform="bluesky")
    queue_report = build_redaction_publish_guard_report(db, queue_id=queued_x)
    failed_report = build_redaction_publish_guard_report(db, queue_id=failed_x)

    assert [item.queue_id for item in platform_report.items] == [queued_bsky]
    assert [item.queue_id for item in queue_report.items] == [queued_x]
    assert failed_report.scanned_count == 0


def test_blocks_standalone_token_like_strings(db):
    token = "abc123def456ghi789jkl012mno345"
    _queued_content(db, f"Debug credential: {token}")

    report = build_redaction_publish_guard_report(db)
    output = export_to_json(report)

    assert report.items[0].status == "blocked"
    assert "token_like" in report.items[0].matched_rule_codes
    assert token not in output
    assert "[REDACTED_SECRET]" in output


def test_include_warnings_controls_warning_severity_rules(db):
    _queued_content(db, "This references public-id-123.")
    patterns = [
        {
            "name": "public_id",
            "pattern": r"public-id-\d+",
            "placeholder": "[REDACTED_PUBLIC_ID]",
            "severity": "warning",
        }
    ]

    hidden = build_redaction_publish_guard_report(db, patterns=patterns)
    included = build_redaction_publish_guard_report(
        db,
        patterns=patterns,
        include_warnings=True,
    )

    assert hidden.items[0].status == "passed"
    assert included.items[0].status == "warning"
    assert included.warning_count == 1
    assert included.blocked_count == 0
    assert included.items[0].matched_rule_codes == ["public_id"]


def test_cli_outputs_json_and_fails_only_with_fail_on_blocked(db, capsys):
    _queued_content(db, "Bearer abcdefghijklmnopqrstuvwxyz")
    config = SimpleNamespace(
        privacy=SimpleNamespace(
            redaction_patterns=[
                {
                    "name": "bearer_token",
                    "pattern": r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}",
                    "placeholder": "[REDACTED_BEARER]",
                }
            ]
        )
    )

    with patch.object(
        check_redaction_publish_script,
        "script_context",
        return_value=_script_context(config, db),
    ):
        exit_code = check_redaction_publish_script.main(["--format", "json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["blocked_count"] == 1
    assert "abcdefghijklmnopqrstuvwxyz" not in json.dumps(payload)

    with patch.object(
        check_redaction_publish_script,
        "script_context",
        return_value=_script_context(config, db),
    ):
        exit_code = check_redaction_publish_script.main(["--fail-on-blocked"])

    assert exit_code == 1
    assert "Blocked: 1" in capsys.readouterr().out
