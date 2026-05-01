"""Tests for newsletter source freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_source_freshness import (
    build_newsletter_source_freshness,
    format_newsletter_source_freshness_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_source_freshness.py"
)
spec = importlib.util.spec_from_file_location("newsletter_source_freshness", SCRIPT_PATH)
newsletter_source_freshness_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(newsletter_source_freshness_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _insert_content(
    db,
    *,
    content_type: str = "x_post",
    created_at: str = "2026-04-30T12:00:00+00:00",
    published: bool = True,
    publication_status: str | None = "published",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} source",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at, 1 if published else 0, content_id),
    )
    if publication_status is not None:
        if publication_status == "published":
            db.upsert_publication_success(
                content_id,
                "x",
                platform_post_id=f"post-{content_id}",
                published_at=created_at,
            )
        elif publication_status == "queued":
            db.upsert_publication_queued(content_id, "x")
        else:
            db.upsert_publication_failure(content_id, "x", "failed")
    db.conn.commit()
    return content_id


def _insert_send(
    db,
    *,
    issue_id: str,
    content_ids: list[int],
    sent_at: str,
    subject: str = "Newsletter",
    status: str = "sent",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=content_ids,
        status=status,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    db.conn.commit()
    return send_id


def _send_by_issue(report: dict) -> dict[str, dict]:
    return {send["issue_id"]: send for send in report["sends"]}


def test_fresh_published_sources_have_no_warnings(db):
    content_id = _insert_content(
        db,
        created_at="2026-04-30T12:00:00+00:00",
        published=True,
    )
    _insert_send(
        db,
        issue_id="fresh",
        content_ids=[content_id],
        sent_at="2026-05-01T12:00:00+00:00",
    )

    report = build_newsletter_source_freshness(db, now=NOW)

    send = report["sends"][0]
    source = send["sources"][0]
    assert send["warnings"] == []
    assert source["content_id"] == content_id
    assert source["source_age_days"] == 1.0
    assert source["reuse_count"] == 1
    assert source["publication_status"] == "published"
    assert source["warnings"] == []


def test_stale_repeated_and_unpublished_sources_are_flagged(db):
    repeated = _insert_content(
        db,
        created_at="2026-04-01T12:00:00+00:00",
        published=False,
        publication_status="queued",
    )
    other = _insert_content(db, created_at="2026-04-29T12:00:00+00:00")
    _insert_send(
        db,
        issue_id="a",
        content_ids=[repeated, other],
        sent_at="2026-05-01T12:00:00+00:00",
    )
    _insert_send(
        db,
        issue_id="b",
        content_ids=[repeated],
        sent_at="2026-04-30T12:00:00+00:00",
    )
    _insert_send(
        db,
        issue_id="c",
        content_ids=[repeated],
        sent_at="2026-04-29T12:00:00+00:00",
    )

    report = build_newsletter_source_freshness(
        db,
        max_source_age_days=14,
        max_reuse_count=2,
        now=NOW,
    )

    assert report["summary"]["stale_source_count"] == 3
    assert report["summary"]["repeated_source_count"] == 3
    assert report["summary"]["unpublished_source_count"] == 3
    for issue in ("a", "b", "c"):
        source = next(
            item for item in _send_by_issue(report)[issue]["sources"]
            if item["content_id"] == repeated
        )
        assert source["reuse_count"] == 3
        assert source["publication_status"] == "queued"
        assert source["warnings"] == [
            "stale_source",
            "repeated_source",
            "unpublished_source",
        ]


def test_malformed_json_missing_ids_and_missing_rows_warn_explicitly(db):
    missing_ids_send = _insert_send(
        db,
        issue_id="missing-ids",
        content_ids=[],
        sent_at="2026-05-01T12:00:00+00:00",
    )
    malformed_send = _insert_send(
        db,
        issue_id="malformed",
        content_ids=[],
        sent_at="2026-05-01T11:00:00+00:00",
    )
    missing_row_send = _insert_send(
        db,
        issue_id="missing-row",
        content_ids=[9999],
        sent_at="2026-05-01T10:00:00+00:00",
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        (None, missing_ids_send),
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        ("not-json", malformed_send),
    )
    db.conn.commit()

    report = build_newsletter_source_freshness(db, now=NOW)
    sends = _send_by_issue(report)

    assert sends["missing-ids"]["warnings"] == ["missing_source_content_ids"]
    assert sends["missing-ids"]["sources"] == []
    assert sends["malformed"]["warnings"] == ["malformed_source_content_ids"]
    assert sends["malformed"]["sources"] == []
    assert sends["missing-row"]["sources"][0]["publication_status"] == "missing"
    assert sends["missing-row"]["warnings"] == ["missing_source_row"]
    assert missing_row_send


def test_issue_and_lookback_filters_are_deterministic(db):
    selected = _insert_content(db)
    excluded_issue = _insert_content(db)
    old = _insert_content(db)
    _insert_send(
        db,
        issue_id="selected",
        content_ids=[selected],
        sent_at="2026-04-30T12:00:00+00:00",
    )
    _insert_send(
        db,
        issue_id="other",
        content_ids=[excluded_issue],
        sent_at="2026-04-30T12:00:00+00:00",
    )
    _insert_send(
        db,
        issue_id="selected",
        content_ids=[old],
        sent_at="2026-03-01T12:00:00+00:00",
    )

    report = build_newsletter_source_freshness(
        db,
        days=7,
        issue_id="selected",
        now=NOW,
    )

    assert [send["issue_id"] for send in report["sends"]] == ["selected"]
    assert report["sends"][0]["source_content_ids"] == [selected]
    assert report["filters"]["issue_id"] == "selected"


def test_cli_text_and_json_output(db, capsys):
    content_id = _insert_content(db, created_at="2026-04-01T12:00:00+00:00")
    _insert_send(
        db,
        issue_id="cli",
        content_ids=[content_id],
        sent_at="2026-05-01T12:00:00+00:00",
    )

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(
        newsletter_source_freshness_cli,
        "script_context",
        fake_script_context,
    ), patch.object(
        newsletter_source_freshness_cli,
        "build_newsletter_source_freshness",
        wraps=lambda db, **kwargs: build_newsletter_source_freshness(
            db, now=NOW, **kwargs
        ),
    ):
        assert newsletter_source_freshness_cli.main(
            [
                "--days",
                "30",
                "--max-source-age-days",
                "14",
                "--max-reuse-count",
                "1",
                "--issue-id",
                "cli",
                "--format",
                "json",
            ]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["issue_id"] == "cli"
    assert payload["sends"][0]["sources"][0]["warnings"] == ["stale_source"]

    text = format_newsletter_source_freshness_text(payload)
    assert "Newsletter source freshness report" in text
    assert "issue=cli" in text
    assert "stale_source" in text
