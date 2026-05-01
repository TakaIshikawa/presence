"""Tests for the newsletter archive manifest exporter."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_archive_manifest import (
    build_newsletter_archive_manifest,
    format_newsletter_archive_manifest_json,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_newsletter_archive.py"
)
spec = importlib.util.spec_from_file_location("export_newsletter_archive", SCRIPT_PATH)
export_newsletter_archive = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_newsletter_archive)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, content_type: str = "x_post") -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET content_format = 'tip', published_at = ?, published_url = ?
           WHERE id = ?""",
        (
            "2026-04-15T12:00:00+00:00",
            f"https://example.test/content/{content_id}",
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def test_manifest_exports_one_issue_per_filtered_newsletter_send(db):
    first_id = _content(db, "First source preview\nwith whitespace.")
    second_id = _content(db, "Second source preview", content_type="blog_post")
    send_id = db.insert_newsletter_send(
        "issue-1",
        "Weekly archive",
        [first_id, second_id, first_id],
        subscriber_count=100,
    )
    old_send_id = db.insert_newsletter_send(
        "issue-old",
        "Old issue",
        [first_id],
        subscriber_count=100,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2026-04-20T09:00:00+00:00", send_id),
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2025-12-01T09:00:00+00:00", old_send_id),
    )
    db.conn.commit()

    manifest = build_newsletter_archive_manifest(db, days=30, now=NOW)

    assert [issue.newsletter_send_id for issue in manifest.issues] == [send_id]
    issue = manifest.issues[0]
    assert issue.issue_id == "issue-1"
    assert issue.subject == "Weekly archive"
    assert issue.source_content_ids == (first_id, second_id, first_id)
    assert issue.canonical_source_content_ids == (first_id, second_id)
    assert [source.content_id for source in issue.sources] == [first_id, second_id]
    assert issue.sources[0].content_preview == "First source preview with whitespace."
    assert issue.sources[1].content_type == "blog_post"
    assert manifest.summary["issue_count"] == 1
    assert manifest.summary["source_count"] == 3
    assert manifest.summary["canonical_source_count"] == 2


def test_manifest_uses_latest_engagement_and_link_snapshots(db):
    content_id = _content(db, "Clicked source")
    send_id = db.insert_newsletter_send(
        "issue-metrics",
        "Metrics issue",
        [content_id],
        subscriber_count=200,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2026-04-25T09:00:00+00:00", send_id),
    )
    db.insert_newsletter_engagement(
        send_id,
        "issue-metrics",
        opens=10,
        clicks=2,
        unsubscribes=1,
        fetched_at="2026-04-25T10:00:00+00:00",
    )
    db.insert_newsletter_engagement(
        send_id,
        "issue-metrics",
        opens=80,
        clicks=12,
        unsubscribes=2,
        fetched_at="2026-04-26T10:00:00+00:00",
    )
    db.insert_newsletter_link_clicks(
        send_id,
        "issue-metrics",
        [{"url": "https://example.test/content/1", "clicks": 1, "unique_clicks": 1}],
        fetched_at="2026-04-25T10:00:00+00:00",
    )
    db.insert_newsletter_link_clicks(
        send_id,
        "issue-metrics",
        [
            {
                "url": "https://example.test/content/1",
                "raw_url": "https://example.test/content/1?utm_campaign=weekly",
                "clicks": 7,
                "unique_clicks": 5,
                "raw_metrics": {"label": "primary"},
            }
        ],
        fetched_at="2026-04-26T10:00:00+00:00",
    )

    issue = build_newsletter_archive_manifest(db, days=30, now=NOW).issues[0]

    assert issue.engagement is not None
    assert issue.engagement.opens == 80
    assert issue.engagement.clicks == 12
    assert issue.engagement.open_rate == 0.4
    assert issue.engagement.click_rate == 0.06
    assert len(issue.links) == 1
    assert issue.links[0].clicks == 7
    assert issue.links[0].unique_clicks == 5
    assert issue.links[0].content_id == content_id
    assert issue.links[0].source_kind == "internal_url"
    assert issue.links[0].raw_metrics == {"label": "primary"}


def test_manifest_marks_missing_and_malformed_sources(db):
    content_id = _content(db, "Valid source")
    send_id = db.insert_newsletter_send(
        "issue-bad-sources",
        "Bad sources",
        [],
        subscriber_count=25,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ?, sent_at = ? WHERE id = ?",
        (
            json.dumps([content_id, "bad", 9999, 0]),
            "2026-04-25T09:00:00+00:00",
            send_id,
        ),
    )
    db.conn.commit()

    issue = build_newsletter_archive_manifest(db, days=30, now=NOW).issues[0]

    assert issue.source_content_ids == (content_id, 9999)
    assert issue.canonical_source_content_ids == (content_id, 9999)
    assert issue.source_parse_warnings == ("malformed_source_content_ids",)
    assert issue.sources[0].missing is False
    assert issue.sources[1].missing is True


def test_json_and_cli_stdout_or_output_path_are_deterministic(db, tmp_path, capsys):
    content_id = _content(db, "CLI source")
    send_id = db.insert_newsletter_send(
        "issue-cli",
        "CLI issue",
        [content_id],
        subscriber_count=10,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        ("2026-04-25T09:00:00+00:00", send_id),
    )
    db.conn.commit()

    manifest = build_newsletter_archive_manifest(
        db,
        days=30,
        issue_id="issue-cli",
        now=NOW,
    )
    assert format_newsletter_archive_manifest_json(manifest) == (
        format_newsletter_archive_manifest_json(manifest)
    )

    with patch.object(
        export_newsletter_archive,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        export_newsletter_archive,
        "build_newsletter_archive_manifest",
        wraps=lambda db, **kwargs: build_newsletter_archive_manifest(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert (
            export_newsletter_archive.main(
                ["--days", "30", "--issue-id", "issue-cli"]
            )
            == 0
        )
        stdout_payload = json.loads(capsys.readouterr().out)
        output_path = tmp_path / "archive.json"
        assert export_newsletter_archive.main(
            ["--days", "30", "--issue-id", "issue-cli", "--output", str(output_path)]
        ) == 0

    file_payload = json.loads(output_path.read_text())
    assert stdout_payload == file_payload
    assert stdout_payload["issues"][0]["newsletter_send_id"] == send_id
    assert stdout_payload["issues"][0]["source_content_ids"] == [content_id]


def test_missing_newsletter_sends_returns_empty_manifest():
    conn = sqlite3.connect(":memory:")
    try:
        manifest = build_newsletter_archive_manifest(conn, now=NOW)
    finally:
        conn.close()

    assert manifest.issues == ()
    assert "newsletter_sends" in manifest.missing_tables
    assert manifest.summary["issue_count"] == 0


def test_rejects_invalid_days(db):
    try:
        build_newsletter_archive_manifest(db, days=0, now=NOW)
    except ValueError as exc:
        assert "days must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")
