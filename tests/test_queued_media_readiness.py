"""Tests for queued visual media readiness audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.queued_media_readiness import (
    build_queued_media_readiness_report,
    format_queued_media_readiness_json,
    format_queued_media_readiness_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_queued_media.py"
spec = importlib.util.spec_from_file_location("audit_queued_media", SCRIPT_PATH)
audit_queued_media = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_queued_media)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_visual",
    image_path: str | None = None,
    image_alt_text: str | None = "Annotated release chart with labeled trend lines.",
    image_prompt: str | None = "Annotated release chart with labeled trend lines",
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="Queued visual post",
        eval_score=8.0,
        eval_feedback="ok",
        image_path=image_path,
        image_alt_text=image_alt_text,
        image_prompt=image_prompt,
    )


def _queue(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "queued",
    created_at: datetime | None = None,
) -> int:
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            (NOW + timedelta(hours=1)).isoformat(),
            platform,
            status,
            (created_at or (NOW - timedelta(hours=1))).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return int(queue_id)


def test_classifies_ready_warning_and_blocked_visual_queue_items(db, tmp_path):
    ready_path = tmp_path / "ready.png"
    ready_path.write_bytes(b"png")
    warning_path = tmp_path / "warning.jpg"
    warning_path.write_bytes(b"jpg")
    unsupported_path = tmp_path / "blocked.bmp"
    unsupported_path.write_bytes(b"bmp")

    ready_content = _content(db, image_path=str(ready_path))
    warning_content = _content(db, image_path=str(warning_path), image_prompt="")
    blocked_content = _content(db, image_path=str(unsupported_path), image_alt_text="")
    missing_file_content = _content(db, image_path=str(tmp_path / "missing.png"))

    ready_queue = _queue(db, ready_content)
    warning_queue = _queue(db, warning_content)
    blocked_queue = _queue(db, blocked_content)
    missing_queue = _queue(db, missing_file_content)

    report = build_queued_media_readiness_report(db, days=7, now=NOW)
    by_queue = {item.queue_id: item for item in report.items}

    assert by_queue[ready_queue].status == "ready"
    assert by_queue[ready_queue].reasons == ()
    assert by_queue[warning_queue].status == "warning"
    assert [reason.code for reason in by_queue[warning_queue].reasons] == [
        "missing_image_prompt"
    ]
    assert by_queue[blocked_queue].status == "blocked"
    assert {reason.code for reason in by_queue[blocked_queue].reasons} == {
        "unsupported_image_extension",
        "missing_alt_text",
    }
    assert by_queue[missing_queue].status == "blocked"
    assert by_queue[missing_queue].reasons[0].code == "missing_image_file"
    assert report.totals == {"ready": 1, "warning": 1, "blocked": 2, "total": 4}
    assert "Queued Media Readiness Audit" in format_queued_media_readiness_text(report)


def test_detects_oversized_files_for_target_platform_when_size_is_available(db, tmp_path):
    image_path = tmp_path / "large.png"
    image_path.write_bytes(b"x" * (1024 * 1024 + 1))
    content_id = _content(db, image_path=str(image_path))
    queue_id = _queue(db, content_id, platform="bluesky")

    report = build_queued_media_readiness_report(db, platform="bluesky", days=7, now=NOW)

    item = report.items[0]
    assert item.queue_id == queue_id
    assert item.status == "blocked"
    assert item.file_size_bytes == 1024 * 1024 + 1
    assert item.reasons[0].code == "image_file_too_large"
    assert item.reasons[0].platform == "bluesky"


def test_platform_filters_support_all_required_values_without_clients(db, tmp_path):
    for platform in ("x", "bluesky", "linkedin", "mastodon"):
        image_path = tmp_path / f"{platform}.png"
        image_path.write_bytes(platform.encode())
        _queue(db, _content(db, image_path=str(image_path)), platform=platform)

    report = build_queued_media_readiness_report(
        db,
        platform="linkedin",
        days=7,
        now=NOW,
    )

    assert [item.platform for item in report.items] == ["linkedin"]
    assert report.items[0].target_platforms == ("linkedin",)
    for platform in ("all", "x", "bluesky", "linkedin", "mastodon"):
        build_queued_media_readiness_report(db, platform=platform, days=7, now=NOW)


def test_filters_recent_window_and_ignores_non_visual_content(db, tmp_path):
    recent_path = tmp_path / "recent.png"
    recent_path.write_bytes(b"recent")
    recent_queue = _queue(db, _content(db, image_path=str(recent_path)))
    old_path = tmp_path / "old.png"
    old_path.write_bytes(b"old")
    _queue(
        db,
        _content(db, image_path=str(old_path)),
        created_at=NOW - timedelta(days=40),
    )
    text_content = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Plain post",
        eval_score=8.0,
        eval_feedback="ok",
    )
    _queue(db, text_content)

    report = build_queued_media_readiness_report(db, days=7, now=NOW)

    assert [item.queue_id for item in report.items] == [recent_queue]


def test_failed_platform_state_adds_platform_specific_blocker(db, tmp_path):
    image_path = tmp_path / "visual.png"
    image_path.write_bytes(b"visual")
    content_id = _content(db, image_path=str(image_path))
    _queue(db, content_id, platform="mastodon")
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category)
           VALUES (?, 'mastodon', 'failed', 'media rejected', 'media')""",
        (content_id,),
    )
    db.conn.commit()

    report = build_queued_media_readiness_report(
        db,
        platform="mastodon",
        days=7,
        now=NOW,
    )

    assert report.items[0].status == "blocked"
    assert report.items[0].reasons[0].code == "platform_publication_blocker"
    assert report.items[0].reasons[0].platform == "mastodon"


def test_json_formatter_is_deterministic(db, tmp_path):
    image_path = tmp_path / "visual.png"
    image_path.write_bytes(b"visual")
    content_id = _content(db, image_path=str(image_path))
    queue_id = _queue(db, content_id)

    report = build_queued_media_readiness_report(db, days=7, now=NOW)
    payload = json.loads(format_queued_media_readiness_json(report))

    assert payload["items"][0]["queue_id"] == queue_id
    assert payload["items"][0]["image_alt_text_present"] is True
    assert payload["items"][0]["image_prompt_present"] is True
    assert payload["totals"] == {"blocked": 0, "ready": 1, "total": 1, "warning": 0}


def test_cli_returns_one_only_for_blockers_when_requested(db, tmp_path, capsys):
    missing_file_content = _content(db, image_path=str(tmp_path / "missing.png"))
    _queue(db, missing_file_content)

    with patch.object(audit_queued_media, "script_context") as mock_context:
        mock_context.return_value = _script_context(db)
        no_fail_code = audit_queued_media.main(["--format", "json"])
    first_payload = json.loads(capsys.readouterr().out)

    with patch.object(audit_queued_media, "script_context") as mock_context:
        mock_context.return_value = _script_context(db)
        fail_code = audit_queued_media.main(["--format", "json", "--fail-on-blocker"])
    second_payload = json.loads(capsys.readouterr().out)

    assert no_fail_code == 0
    assert fail_code == 1
    assert first_payload["totals"]["blocked"] == 1
    assert second_payload["totals"]["blocked"] == 1


def test_cli_warning_does_not_fail_when_fail_on_blocker_is_set(db, tmp_path):
    image_path = tmp_path / "visual.png"
    image_path.write_bytes(b"visual")
    warning_content = _content(db, image_path=str(image_path), image_prompt="")
    _queue(db, warning_content)

    with patch.object(audit_queued_media, "script_context") as mock_context:
        mock_context.return_value = _script_context(db)
        exit_code = audit_queued_media.main(["--fail-on-blocker"])

    assert exit_code == 0
