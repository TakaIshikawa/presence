"""Tests for published X media attachment audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.x_media_attachment_audit import (
    build_x_media_attachment_audit_report,
    format_x_media_attachment_audit_csv,
    format_x_media_attachment_audit_json,
    normalize_media_reference,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "x_media_attachment_audit.py"
spec = importlib.util.spec_from_file_location("x_media_attachment_audit_script", SCRIPT_PATH)
x_media_attachment_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(x_media_attachment_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _published_at(days_ago: int = 1) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _content(
    db,
    *,
    content_type: str = "x_visual",
    image_path: str | None = "https://cdn.example.com/media/card.png",
    image_prompt: str | None = "Annotated release chart",
    image_alt_text: str | None = "Annotated release chart with trend labels.",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="Published visual post",
        eval_score=8.0,
        eval_feedback="ok",
        image_path=image_path,
        image_prompt=image_prompt,
        image_alt_text=image_alt_text,
    )
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_post_id, platform_url, published_at)
           VALUES (?, 'x', 'published', ?, ?, ?)""",
        (
            content_id,
            f"tw-{content_id}",
            f"https://x.com/test/status/tw-{content_id}",
            _published_at(),
        ),
    )
    db.conn.commit()
    return content_id


def test_reports_missing_alt_broken_path_malformed_url_and_orphaned_prompt(db, tmp_path):
    missing_file = tmp_path / "missing.png"
    missing_alt_id = _content(db, image_alt_text="")
    broken_path_id = _content(
        db,
        image_path=str(missing_file),
        image_prompt="Local card",
        image_alt_text="Local card with labels.",
    )
    malformed_id = _content(
        db,
        image_path="ftp://cdn.example.com/card.png",
        image_alt_text="Remote card with labels.",
    )
    orphaned_id = _content(
        db,
        image_path=None,
        image_prompt="Prompt without generated media",
        image_alt_text="",
    )

    report = build_x_media_attachment_audit_report(db, days=7, now=NOW)
    issues = {(row.content_id, row.issue_type) for row in report.rows}

    assert (missing_alt_id, "missing_alt_text") in issues
    assert (broken_path_id, "broken_local_path") in issues
    assert (malformed_id, "malformed_media_url") in issues
    assert (orphaned_id, "orphaned_media_prompt") in issues
    assert report.totals["published_posts_scanned"] == 4
    assert report.totals["by_issue_type"]["missing_alt_text"] == 1


def test_empty_result_set_has_csv_header_and_json_totals(db):
    report = build_x_media_attachment_audit_report(db, days=7, now=NOW)

    payload = json.loads(format_x_media_attachment_audit_json(report))
    csv_output = format_x_media_attachment_audit_csv(report)

    assert payload["artifact_type"] == "x_media_attachment_audit"
    assert payload["totals"]["finding_count"] == 0
    assert payload["rows"] == []
    assert csv_output.startswith("issue_type,content_id,published_at")


def test_detects_duplicate_media_urls_and_prompt_references(db):
    first_id = _content(
        db,
        image_path="HTTPS://cdn.example.com/media/card.png?b=2&a=1#fragment",
        image_prompt="Annotated Release Chart",
    )
    second_id = _content(
        db,
        image_path="https://cdn.example.com/media/card.png?a=1&b=2",
        image_prompt=" annotated   release chart ",
    )
    _content(
        db,
        image_path="https://cdn.example.com/media/unique.png",
        image_prompt="Unique diagram",
    )

    report = build_x_media_attachment_audit_report(db, days=7, now=NOW)
    duplicate_media = [
        row for row in report.rows if row.issue_type == "duplicate_media_url"
    ]
    duplicate_prompts = [
        row for row in report.rows if row.issue_type == "duplicate_media_prompt"
    ]

    assert {row.content_id for row in duplicate_media} == {first_id, second_id}
    assert {row.content_id for row in duplicate_prompts} == {first_id, second_id}
    assert duplicate_media[0].normalized_media_reference == normalize_media_reference(
        "https://cdn.example.com/media/card.png?a=1&b=2"
    )
    assert all(row.duplicate_group for row in duplicate_media + duplicate_prompts)


def test_lookback_filters_old_published_posts(db):
    recent_id = _content(db, image_alt_text="")
    old_id = _content(db, image_alt_text="")
    db.conn.execute(
        "UPDATE content_publications SET published_at = ? WHERE content_id = ?",
        (_published_at(days_ago=40), old_id),
    )
    db.conn.commit()

    report = build_x_media_attachment_audit_report(db, days=7, now=NOW)

    assert [row.content_id for row in report.rows] == [recent_id]


def test_cli_prints_json_by_default_and_writes_csv_output(db, tmp_path, capsys):
    _content(db, image_alt_text="")
    output_path = tmp_path / "audit.csv"

    with patch.object(x_media_attachment_audit_script, "script_context") as mock_context:
        mock_context.return_value = _script_context(db)
        json_code = x_media_attachment_audit_script.main([])
    payload = json.loads(capsys.readouterr().out)

    with patch.object(x_media_attachment_audit_script, "script_context") as mock_context:
        mock_context.return_value = _script_context(db)
        csv_code = x_media_attachment_audit_script.main(
            ["--format", "csv", "--output", str(output_path)]
        )
    stdout = capsys.readouterr().out

    assert json_code == 0
    assert payload["totals"]["finding_count"] == 1
    assert csv_code == 0
    assert stdout == ""
    assert output_path.read_text(encoding="utf-8").startswith(
        "issue_type,content_id,published_at"
    )

