from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_issue_readiness import (
    build_newsletter_issue_readiness_report,
    format_newsletter_issue_readiness_json,
    format_newsletter_issue_readiness_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_issue_readiness.py"
spec = importlib.util.spec_from_file_location("newsletter_issue_readiness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(db, *, subject: str, sources: str | list[int] | None, metadata: dict, status: str = "sent", days_ago: int = 1) -> int:
    raw_sources = json.dumps(sources) if isinstance(sources, list) else sources
    cursor = db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, status, metadata, sent_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("issue", subject, raw_sources, status, json.dumps(metadata), (NOW - timedelta(days=days_ago)).isoformat()),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_returns_blockers_and_warnings_for_draft_and_sent_rows(db):
    blocked = _send(db, subject=" ", sources=None, metadata={}, status="draft", days_ago=9)
    warning = _send(db, subject="Good", sources=[10], metadata={"preview": "p", "body": "b", "sections": ["only"]})

    report = build_newsletter_issue_readiness_report(db, now=NOW)
    by_id = {item["newsletter_send_id"]: item for item in report["issues"]}

    assert by_id[blocked]["status"] == "blocked"
    assert by_id[blocked]["blocker_codes"] == [
        "missing_subject",
        "missing_preview",
        "missing_source_content_ids",
        "empty_body_metadata",
    ]
    assert "stale_draft" in by_id[blocked]["warning_codes"]
    assert by_id[warning]["status"] == "warning"
    assert by_id[warning]["source_diversity"] == 1
    assert "low_source_diversity" in by_id[warning]["warning_codes"]


def test_source_ids_parse_json_or_delimited_text_and_require_sources_flag(db):
    ready = _send(db, subject="S", sources="1, 2;3", metadata={"preview_text": "p", "body_text": "b", "sections": ["a", "b"]})
    no_sources = _send(db, subject="S", sources="", metadata={"preview": "p", "body": "b", "sections": ["a", "b"]})

    report = build_newsletter_issue_readiness_report(db, require_sources=False, now=NOW)
    by_id = {item["newsletter_send_id"]: item for item in report["issues"]}

    assert by_id[ready]["source_content_ids"] == [1, 2, 3]
    assert by_id[ready]["status"] == "ready"
    assert by_id[no_sources]["status"] == "warning"
    assert by_id[no_sources]["warning_codes"] == ["missing_source_content_ids"]


def test_formatters_and_cli(db, monkeypatch, capsys):
    sid = _send(db, subject="", sources=None, metadata={})
    report = build_newsletter_issue_readiness_report(db, now=NOW)
    payload = json.loads(format_newsletter_issue_readiness_json(report))
    text = format_newsletter_issue_readiness_text(report)

    assert list(payload) == sorted(payload)
    assert f"send_id={sid}" in text

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_newsletter_issue_readiness_report",
        lambda db, **kwargs: build_newsletter_issue_readiness_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "30", "--limit", "1", "--no-require-sources", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["require_sources"] is False
    assert cli_payload["filters"]["limit"] == 1
