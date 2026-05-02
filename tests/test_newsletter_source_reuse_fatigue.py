"""Tests for newsletter source reuse fatigue reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_source_reuse_fatigue import (
    build_newsletter_source_reuse_fatigue_report,
    format_newsletter_source_reuse_fatigue_json,
    format_newsletter_source_reuse_fatigue_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_source_reuse_fatigue.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_source_reuse_fatigue_script",
    SCRIPT_PATH,
)
newsletter_source_reuse_fatigue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_source_reuse_fatigue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.commit()
    return content_id


def _send(
    db,
    source_content_ids,
    *,
    issue_id: str,
    sent_at: datetime = NOW,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Newsletter {issue_id}",
        content_ids=[],
        subscriber_count=10,
    )
    raw_value = (
        source_content_ids
        if isinstance(source_content_ids, str) or source_content_ids is None
        else json.dumps(source_content_ids)
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ?, sent_at = ? WHERE id = ?",
        (raw_value, sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def test_reused_content_ids_are_grouped_with_send_and_issue_context(db):
    reused = _content(db, "reused")
    other = _content(db, "other")
    single_use = _content(db, "single use")
    first_send = _send(
        db,
        [reused, other, reused],
        issue_id="issue-a",
        sent_at=NOW - timedelta(days=2),
    )
    second_send = _send(
        db,
        [reused],
        issue_id="issue-b",
        sent_at=NOW - timedelta(days=1),
    )
    _send(db, [single_use], issue_id="issue-c", sent_at=NOW)
    _send(
        db,
        [reused],
        issue_id="old",
        sent_at=NOW - timedelta(days=40),
    )

    report = build_newsletter_source_reuse_fatigue_report(
        db,
        days=7,
        min_reuses=2,
        now=NOW,
    )

    assert report.has_issues is True
    assert len(report.reused_sources) == 1
    group = report.reused_sources[0]
    assert group.content_id == reused
    assert group.reuse_count == 2
    assert group.issue_count == 2
    assert group.send_ids == (first_send, second_send)
    assert group.issue_ids == ("issue-a", "issue-b")
    assert group.first_seen == (NOW - timedelta(days=2)).isoformat()
    assert group.last_seen == (NOW - timedelta(days=1)).isoformat()
    assert "Replace or refresh" in group.recommendation
    assert report.totals["parsed_reference_count"] == 5
    assert "content_id=" in format_newsletter_source_reuse_fatigue_text(report)


def test_min_reuses_filters_groups(db):
    content_id = _content(db, "threshold")
    _send(db, [content_id], issue_id="issue-a", sent_at=NOW - timedelta(days=2))
    _send(db, [content_id], issue_id="issue-b", sent_at=NOW - timedelta(days=1))

    report = build_newsletter_source_reuse_fatigue_report(
        db,
        days=7,
        min_reuses=3,
        now=NOW,
    )

    assert report.reused_sources == ()
    assert report.has_issues is False
    assert "No newsletter source reuse fatigue issues" in (
        format_newsletter_source_reuse_fatigue_text(report)
    )


def test_malformed_blank_and_invalid_values_become_findings(db):
    content_id = _content(db, "valid")
    blank_send = _send(db, "   ", issue_id="blank")
    malformed_send = _send(db, "{not-json", issue_id="malformed")
    object_send = _send(db, '{"id": 1}', issue_id="object")
    invalid_item_send = _send(db, [content_id, "2", 0], issue_id="invalid-item")

    report = build_newsletter_source_reuse_fatigue_report(db, days=7, now=NOW)
    findings = {(finding.newsletter_send_id, finding.finding_type) for finding in report.findings}

    assert (blank_send, "blank_source_content_ids") in findings
    assert (malformed_send, "malformed_source_content_ids") in findings
    assert (object_send, "malformed_source_content_ids") in findings
    assert (invalid_item_send, "invalid_source_content_id") in findings
    assert report.totals["by_finding_type"] == {
        "blank_source_content_ids": 1,
        "invalid_source_content_id": 2,
        "malformed_source_content_ids": 2,
    }


def test_json_formatter_is_deterministic_and_serializable(db):
    content_id = _content(db, "json")
    _send(db, [content_id], issue_id="issue-a", sent_at=NOW - timedelta(days=1))
    _send(db, [content_id], issue_id="issue-b", sent_at=NOW)

    report = build_newsletter_source_reuse_fatigue_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_source_reuse_fatigue_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_source_reuse_fatigue"
    assert payload["has_issues"] is True
    assert payload["reused_sources"][0]["content_id"] == content_id
    assert payload["reused_sources"][0]["send_ids"]


def test_cli_outputs_json_and_fail_on_issues_exit_behavior(db, monkeypatch, capsys):
    content_id = _content(db, "cli")
    _send(db, [content_id], issue_id="issue-a")
    _send(db, [content_id], issue_id="issue-b")
    monkeypatch.setattr(
        newsletter_source_reuse_fatigue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_source_reuse_fatigue_script,
        "build_newsletter_source_reuse_fatigue_report",
        lambda db, **kwargs: build_newsletter_source_reuse_fatigue_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = newsletter_source_reuse_fatigue_script.main(
        ["--days", "7", "--min-reuses", "2", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["reused_source_count"] == 1

    exit_code = newsletter_source_reuse_fatigue_script.main(
        ["--days", "7", "--min-reuses", "2", "--fail-on-issues"]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Reused source content" in captured.out


def test_cli_fail_on_issues_returns_zero_without_findings(db, monkeypatch, capsys):
    content_id = _content(db, "clean")
    _send(db, [content_id], issue_id="clean")
    monkeypatch.setattr(
        newsletter_source_reuse_fatigue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_source_reuse_fatigue_script,
        "build_newsletter_source_reuse_fatigue_report",
        lambda db, **kwargs: build_newsletter_source_reuse_fatigue_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = newsletter_source_reuse_fatigue_script.main(["--fail-on-issues"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No newsletter source reuse fatigue issues" in captured.out


def test_missing_required_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_source_reuse_fatigue_report(conn, now=NOW)

    assert report.reused_sources == ()
    assert report.findings == ()
    assert report.missing_tables == ("newsletter_sends", "generated_content")
