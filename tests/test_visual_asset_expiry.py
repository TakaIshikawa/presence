"""Tests for dry-run visual asset expiry planning."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.visual_asset_expiry import (
    build_visual_asset_expiry_plan,
    format_visual_asset_expiry_json,
    format_visual_asset_expiry_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_visual_asset_expiry.py"
spec = importlib.util.spec_from_file_location("plan_visual_asset_expiry_script", SCRIPT_PATH)
plan_visual_asset_expiry_script = importlib.util.module_from_spec(spec)
sys.modules["plan_visual_asset_expiry_script"] = plan_visual_asset_expiry_script
assert spec and spec.loader
spec.loader.exec_module(plan_visual_asset_expiry_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _touch(path: Path, *, age_days: int, content: bytes = b"png") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = NOW.timestamp() - (age_days * 86400)
    os.utime(path, (timestamp, timestamp))
    return path


def _content(
    db,
    *,
    image_path: str,
    content_type: str = "x_visual",
    created_at: str = "2026-03-01T12:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content="Visual post",
        eval_score=8.0,
        eval_feedback="ok",
        image_path=image_path,
        image_prompt="A dashboard screenshot",
        image_alt_text="A dashboard screenshot with annotations.",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
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


def test_classifies_keep_review_and_archive_with_reasons(db, tmp_path):
    published_path = _touch(tmp_path / "published.png", age_days=90)
    queued_path = _touch(tmp_path / "queued.png", age_days=90)
    draft_path = _touch(tmp_path / "draft.png", age_days=90)
    recent_orphan = _touch(tmp_path / "recent-orphan.png", age_days=3)
    old_orphan = _touch(tmp_path / "old-orphan.png", age_days=45)

    published_id = _content(db, image_path=str(published_path))
    queued_id = _content(db, image_path=str(queued_path))
    draft_id = _content(db, image_path=str(draft_path))
    db.upsert_publication_success(
        published_id,
        "x",
        platform_post_id="tw-1",
        published_at="2026-03-02T12:00:00+00:00",
    )
    _queue(db, queued_id)

    report = build_visual_asset_expiry_plan(
        db,
        root_path=tmp_path,
        minimum_age_days=30,
        include_unpublished=True,
        now=NOW,
    )
    by_path = {Path(item.asset_path).name: item for item in report.items}

    assert by_path["published.png"].action == "keep"
    assert by_path["published.png"].publication_status == "published"
    assert by_path["queued.png"].action == "keep"
    assert by_path["queued.png"].publication_status == "queued"
    assert by_path["draft.png"].action == "archive"
    assert by_path["draft.png"].content_id == draft_id
    assert by_path["draft.png"].reasons
    assert by_path["recent-orphan.png"].action == "review"
    assert by_path["recent-orphan.png"].orphan is True
    assert by_path["old-orphan.png"].action == "archive"
    assert old_orphan in {Path(item.asset_path) for item in report.orphan_files}
    assert recent_orphan in {Path(item.asset_path) for item in report.orphan_files}


def test_unpublished_assets_are_kept_unless_policy_includes_them(db, tmp_path):
    draft_path = _touch(tmp_path / "draft.png", age_days=60)
    _content(db, image_path=str(draft_path))

    protected = build_visual_asset_expiry_plan(
        db,
        root_path=tmp_path,
        minimum_age_days=30,
        include_unpublished=False,
        now=NOW,
    )
    included = build_visual_asset_expiry_plan(
        db,
        root_path=tmp_path,
        minimum_age_days=30,
        include_unpublished=True,
        now=NOW,
    )

    assert protected.items[0].action == "keep"
    assert "protected" in protected.items[0].reasons[0]
    assert included.items[0].action == "archive"


def test_missing_files_are_reported_separately_from_orphans(db, tmp_path):
    missing_path = tmp_path / "missing.png"
    existing_path = _touch(tmp_path / "existing.png", age_days=60)
    orphan_path = _touch(tmp_path / "orphan.png", age_days=60)
    missing_id = _content(db, image_path=str(missing_path))
    _content(db, image_path=str(existing_path))

    report = build_visual_asset_expiry_plan(
        db,
        root_path=tmp_path,
        minimum_age_days=30,
        include_unpublished=True,
        now=NOW,
    )

    assert [item.content_id for item in report.missing_files] == [missing_id]
    assert report.missing_files[0].action == "review"
    assert report.missing_files[0].exists is False
    assert [Path(item.asset_path) for item in report.orphan_files] == [orphan_path]
    assert report.missing_file_count == 1
    assert report.orphan_file_count == 1


def test_variant_metadata_assets_are_included_and_json_is_deterministic(db, tmp_path):
    primary = _touch(tmp_path / "primary.png", age_days=60)
    variant = _touch(tmp_path / "variant.png", age_days=60)
    content_id = _content(db, image_path=str(primary))
    db.upsert_content_variant(
        content_id,
        "x",
        "post",
        "Variant",
        metadata={"visual_assets": [{"path": str(variant)}]},
    )

    report = build_visual_asset_expiry_plan(
        db,
        root_path=tmp_path,
        minimum_age_days=30,
        include_unpublished=True,
        now=NOW,
    )
    payload = json.loads(format_visual_asset_expiry_json(report))
    text = format_visual_asset_expiry_text(report)

    assert {Path(item["asset_path"]).name for item in payload["items"]} == {
        "primary.png",
        "variant.png",
    }
    assert payload["summary"]["actions"]["archive"] == 2
    assert list(payload) == sorted(payload)
    assert "VISUAL ASSET EXPIRY PLAN" in text
    assert "Archive candidates:" in text


def test_cli_outputs_json_and_reports_validation_errors(db, tmp_path, capsys):
    _touch(tmp_path / "draft.png", age_days=60)
    _content(db, image_path=str(tmp_path / "draft.png"))

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(plan_visual_asset_expiry_script, "script_context", fake_script_context):
        result = plan_visual_asset_expiry_script.main(
            [
                "--root-path",
                str(tmp_path),
                "--minimum-age-days",
                "30",
                "--include-unpublished",
                "--json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["summary"]["actions"]["archive"] == 1
    assert payload["items"][0]["reasons"]

    result = plan_visual_asset_expiry_script.main(["--minimum-age-days", "-1"])
    captured = capsys.readouterr()
    assert result == 1
    assert "minimum_age_days must be non-negative" in captured.err
