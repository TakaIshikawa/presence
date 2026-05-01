"""Tests for visual asset usage ledgers."""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.visual_asset_ledger import (
    build_visual_asset_ledger,
    format_visual_asset_ledger_json,
    format_visual_asset_ledger_table,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "visual_asset_ledger.py"
spec = importlib.util.spec_from_file_location("visual_asset_ledger", SCRIPT_PATH)
visual_asset_ledger = importlib.util.module_from_spec(spec)
sys.modules["visual_asset_ledger"] = visual_asset_ledger
spec.loader.exec_module(visual_asset_ledger)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    *,
    image_path: str,
    content_type: str = "x_visual",
    created_at: str = "2026-04-30T12:00:00+00:00",
    published: int = 0,
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
        image_alt_text="A dashboard screenshot with trend annotations.",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at, published, content_id),
    )
    db.conn.commit()
    return content_id


def _queue(db, content_id: int, *, platform: str = "x", status: str = "queued") -> None:
    db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, "2026-05-02T12:00:00+00:00", platform, status),
    )
    db.conn.commit()


def test_ledger_classifies_assets_by_publication_and_queue_state(db, tmp_path):
    published_path = tmp_path / "published.png"
    published_path.write_bytes(b"png")
    queued_path = tmp_path / "queued.png"
    queued_path.write_bytes(b"png")
    draft_path = tmp_path / "draft.png"
    draft_path.write_bytes(b"png")

    published_id = _content(db, image_path=str(published_path))
    queued_id = _content(db, image_path=str(queued_path))
    draft_id = _content(db, image_path=str(draft_path))
    db.upsert_publication_success(
        published_id,
        "x",
        platform_post_id="tw-1",
        published_at="2026-04-30T13:00:00+00:00",
    )
    _queue(db, queued_id, platform="bluesky")

    rows = build_visual_asset_ledger(db, days=7, now=NOW)
    by_content = {(row.content_id, row.platform): row for row in rows}

    assert by_content[(published_id, "x")].status == "published"
    assert by_content[(queued_id, "bluesky")].status == "queued"
    assert by_content[(draft_id, "all")].status == "draft_only"


def test_extracts_variant_metadata_assets_and_ignores_malformed_json(db, tmp_path):
    local_path = tmp_path / "variant.png"
    local_path.write_bytes(b"png")
    content_id = _content(db, image_path=str(tmp_path / "primary.png"))
    db.upsert_content_variant(
        content_id,
        "x",
        "post",
        "Variant",
        metadata={
            "visual_assets": [
                {"path": str(local_path)},
                {"url": "https://cdn.example.com/card.png"},
            ]
        },
    )
    db.conn.execute(
        """INSERT INTO content_variants
           (content_id, platform, variant_type, content, metadata)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, "bluesky", "post", "Bad metadata", "{not-json"),
    )
    db.conn.commit()

    rows = build_visual_asset_ledger(db, days=7, now=NOW)
    artifacts = {row.artifact: row for row in rows}

    assert str(local_path) in artifacts
    assert artifacts[str(local_path)].source == "content_variants.metadata.visual_assets[0].path"
    assert "https://cdn.example.com/card.png" in artifacts
    assert all("{not-json" not in row.artifact for row in rows)


def test_missing_file_warnings_only_apply_to_filesystem_paths(db, tmp_path):
    missing_id = _content(db, image_path=str(tmp_path / "missing.png"))
    url_id = _content(db, image_path="https://cdn.example.com/remote.png")

    rows = build_visual_asset_ledger(db, days=7, now=NOW)
    by_content = {row.content_id: row for row in rows}

    assert by_content[missing_id].warnings == (
        f"missing_file: {tmp_path / 'missing.png'}",
    )
    assert by_content[missing_id].artifact_kind == "path"
    assert by_content[url_id].warnings == ()
    assert by_content[url_id].artifact_kind == "url"


def test_missing_only_filters_rows_with_missing_asset_warnings(db, tmp_path):
    missing_id = _content(db, image_path=str(tmp_path / "missing.png"))
    existing = tmp_path / "existing.png"
    existing.write_bytes(b"png")
    existing_id = _content(db, image_path=str(existing))

    rows = build_visual_asset_ledger(
        db,
        days=7,
        missing_only=True,
        now=NOW,
    )

    assert [row.content_id for row in rows] == [missing_id]
    assert existing_id not in {row.content_id for row in rows}


def test_status_filter_and_formatters(db, tmp_path):
    queued_id = _content(db, image_path=str(tmp_path / "queued.png"))
    _queue(db, queued_id, platform="x")

    rows = build_visual_asset_ledger(db, days=7, status="queued", now=NOW)
    payload = json.loads(format_visual_asset_ledger_json(rows))
    table = format_visual_asset_ledger_table(rows, days=7)

    assert len(payload) == 1
    assert payload[0]["content_id"] == queued_id
    assert payload[0]["status"] == "queued"
    assert list(payload[0]) == sorted(payload[0])
    assert "Visual Asset Ledger (last 7 days)" in table
    assert "queued" in table


def test_cli_supports_json_and_missing_only(db, tmp_path, capsys):
    _content(db, image_path=str(tmp_path / "missing.png"))
    existing = tmp_path / "existing.png"
    existing.write_bytes(b"png")
    _content(db, image_path=str(existing))

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(visual_asset_ledger, "script_context", fake_script_context):
        visual_asset_ledger.main(["--days", "7", "--missing-only", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert len(payload) == 1
    assert payload[0]["warnings"]
    assert payload[0]["artifact"].endswith("missing.png")
