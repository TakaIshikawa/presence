"""Tests for visual asset orphan reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.visual_asset_orphans import (
    build_visual_asset_orphans_report,
    format_visual_asset_orphans_json,
    format_visual_asset_orphans_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "visual_asset_orphans.py"
spec = importlib.util.spec_from_file_location("visual_asset_orphans_script", SCRIPT_PATH)
visual_asset_orphans_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(visual_asset_orphans_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    image_path: str,
    created_at: str = "2026-04-30T12:00:00+00:00",
    image_alt_text: str | None = "A dashboard screenshot with trend annotations.",
    published: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        source_activity_ids=[],
        content="Visual post",
        eval_score=8.0,
        eval_feedback="ok",
        image_path=image_path,
        image_prompt="A dashboard screenshot",
        image_alt_text=image_alt_text,
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at, published, content_id),
    )
    db.conn.commit()
    return content_id


def _queue(db, content_id: int, *, status: str = "queued") -> None:
    db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, "2026-05-02T12:00:00+00:00", "x", status),
    )
    db.conn.commit()


def test_classifies_stale_unpublished_visuals(db, tmp_path):
    stale_path = tmp_path / "stale.png"
    stale_path.write_bytes(b"png")
    fresh_path = tmp_path / "fresh.png"
    fresh_path.write_bytes(b"png")
    stale_id = _content(db, image_path=str(stale_path), created_at="2026-03-01T00:00:00+00:00")
    fresh_id = _content(db, image_path=str(fresh_path), created_at="2026-04-30T00:00:00+00:00")

    report = build_visual_asset_orphans_report(db, days=30, now=NOW)
    by_id = {row["content_id"]: row for row in report["rows"]}

    assert by_id[stale_id]["category"] == "unpublished_stale_visual"
    assert by_id[stale_id]["age_days"] == 61
    assert by_id[stale_id]["publication_status"] == "unpublished"
    assert fresh_id not in by_id


def test_classifies_queued_visuals_without_alt_text(db, tmp_path):
    queued_id = _content(db, image_path=str(tmp_path / "queued.png"), image_alt_text="")
    _queue(db, queued_id)

    report = build_visual_asset_orphans_report(db, days=30, now=NOW)
    row = report["rows"][0]

    assert row["content_id"] == queued_id
    assert row["category"] == "queued_without_alt_text"
    assert row["queue_status"] == "queued"
    assert row["recommended_action"] == "Add alt text before publishing the queued visual."


def test_classifies_published_assets_from_content_publications(db, tmp_path):
    published_id = _content(db, image_path=str(tmp_path / "published.png"))
    db.upsert_publication_success(
        published_id,
        "x",
        platform_post_id="tw-1",
        platform_url="https://example.test/post",
        published_at="2026-04-30T13:00:00+00:00",
    )

    report = build_visual_asset_orphans_report(db, days=30, now=NOW)
    row = report["rows"][0]

    assert row["content_id"] == published_id
    assert row["category"] == "published_asset"
    assert row["publication_status"] == "published"


def test_missing_file_reference_requires_check_files(db, tmp_path):
    missing_id = _content(db, image_path=str(tmp_path / "missing.png"))

    unchecked = build_visual_asset_orphans_report(db, days=30, check_files=False, now=NOW)
    checked = build_visual_asset_orphans_report(db, days=30, check_files=True, now=NOW)

    assert missing_id not in {row["content_id"] for row in unchecked["rows"]}
    assert checked["rows"][0]["content_id"] == missing_id
    assert checked["rows"][0]["category"] == "missing_file_reference"
    assert checked["rows"][0]["file_exists"] is False


def test_existing_file_is_not_missing_when_check_files_enabled(db, tmp_path):
    existing = tmp_path / "existing.png"
    existing.write_bytes(b"png")
    content_id = _content(db, image_path=str(existing))

    report = build_visual_asset_orphans_report(db, days=30, check_files=True, now=NOW)

    assert content_id not in {row["content_id"] for row in report["rows"]}


def test_formatters_and_limit_are_deterministic(db, tmp_path):
    first_id = _content(db, image_path=str(tmp_path / "first.png"), created_at="2026-03-01T00:00:00+00:00")
    _content(db, image_path=str(tmp_path / "second.png"), created_at="2026-03-02T00:00:00+00:00")

    report = build_visual_asset_orphans_report(db, days=30, limit=1, now=NOW)
    payload = json.loads(format_visual_asset_orphans_json(report))
    text = format_visual_asset_orphans_text(report)

    assert payload["rows"][0]["content_id"] == first_id
    assert list(payload) == sorted(payload)
    assert payload["counts"]["findings"] == 1
    assert payload["counts"]["findings_before_limit"] == 2
    assert "Visual Asset Orphans" in text
    assert f"content_id={first_id}" in text


def test_missing_generated_content_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_visual_asset_orphans_report(conn, now=NOW)

    assert report["missing_tables"] == ["generated_content"]
    assert report["counts"]["visual_rows_scanned"] == 0
    assert report["rows"] == []


def test_cli_supports_json_check_files_limit_and_validation(db, tmp_path, monkeypatch, capsys):
    _content(db, image_path=str(tmp_path / "missing.png"))
    _content(db, image_path=str(tmp_path / "missing-too.png"))
    monkeypatch.setattr(
        visual_asset_orphans_script,
        "script_context",
        lambda: _script_context(db),
    )

    result = visual_asset_orphans_script.main(
        ["--format", "json", "--check-files", "--limit", "1"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["filters"]["check_files"] is True
    assert payload["counts"]["findings"] == 1
    assert payload["counts"]["findings_before_limit"] == 2

    result = visual_asset_orphans_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert result == 2
    assert "value must be positive" in captured.err
