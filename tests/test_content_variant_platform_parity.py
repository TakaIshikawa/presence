"""Tests for content variant platform parity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.content_variant_platform_parity import (
    build_content_variant_platform_parity_report,
    format_content_variant_platform_parity_json,
    format_content_variant_platform_parity_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_platform_parity.py"
spec = importlib.util.spec_from_file_location("content_variant_platform_parity_script", SCRIPT_PATH)
content_variant_platform_parity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_variant_platform_parity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, created_at: str = "2026-05-01T09:00:00+00:00") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return int(content_id)


def _variant(
    db,
    content_id: int,
    platform: str,
    *,
    variant_type: str = "post",
    created_at: str = "2026-05-01T10:00:00+00:00",
) -> int:
    variant_id = db.upsert_content_variant(
        content_id,
        platform=platform,
        variant_type=variant_type,
        content=f"{platform} copy",
    )
    db.conn.execute(
        "UPDATE content_variants SET created_at = ? WHERE id = ?",
        (created_at, variant_id),
    )
    db.conn.commit()
    return int(variant_id)


def _edited_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            created_at TEXT,
            updated_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE content_variants (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            variant_type TEXT,
            content TEXT,
            created_at TEXT
        )"""
    )
    return conn


def test_expected_platform_filtering_groups_existing_variants_per_content(db):
    complete = _content(db, "complete")
    _variant(db, complete, "x")
    _variant(db, complete, "bluesky")
    ignored_gap = _content(db, "linkedin intentionally ignored")
    _variant(db, ignored_gap, "x")
    _variant(db, ignored_gap, "bluesky")
    partial = _content(db, "missing bluesky")
    _variant(db, partial, "x")
    _variant(db, partial, "mastodon")

    report = build_content_variant_platform_parity_report(
        db,
        days=7,
        platforms=["bluesky", "x", "x"],
        limit=10,
        now=NOW,
    )
    payload = json.loads(format_content_variant_platform_parity_json(report))

    assert payload["filters"]["platforms"] == ["bluesky", "x"]
    assert [item["content_id"] for item in payload["items"]] == [partial]
    item = payload["items"][0]
    assert item["existing_platforms"] == ["mastodon", "x"]
    assert item["missing_platforms"] == ["bluesky"]
    assert item["recommended_generation_targets"] == ["bluesky"]
    assert payload["summary"]["missing_by_platform"] == {"bluesky": 1}


def test_stale_variant_detection_uses_variant_created_at_against_source_edit_timestamp():
    conn = _edited_conn()
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, content, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            10,
            "x_post",
            "edited source",
            "2026-04-20T09:00:00+00:00",
            "2026-05-01T09:00:00+00:00",
        ),
    )
    conn.execute(
        """INSERT INTO content_variants
           (id, content_id, platform, variant_type, content, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (20, 10, "x", "post", "old x", "2026-04-25T09:00:00+00:00"),
    )
    conn.execute(
        """INSERT INTO content_variants
           (id, content_id, platform, variant_type, content, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (21, 10, "bluesky", "post", "fresh bsky", "2026-05-02T09:00:00+00:00"),
    )
    conn.commit()

    report = build_content_variant_platform_parity_report(
        conn,
        days=30,
        platforms=["x", "bluesky"],
        stale_threshold_days=1,
        now=NOW,
    )

    assert len(report.items) == 1
    item = report.items[0]
    assert item.source_edit_at == "2026-05-01T09:00:00+00:00"
    assert item.missing_platforms == ()
    assert item.stale_variants == (
        {
            "created_at": "2026-04-25T09:00:00+00:00",
            "platform": "x",
            "variant_id": 20,
            "variant_type": "post",
        },
    )
    assert item.recommended_generation_targets == ("x",)


def test_output_ordering_prefers_more_missing_platforms(db):
    one_missing = _content(db, "one missing")
    _variant(db, one_missing, "x")
    _variant(db, one_missing, "bluesky")
    two_missing = _content(db, "two missing")
    _variant(db, two_missing, "x")
    _variant(db, two_missing, "mastodon")
    no_missing = _content(db, "covered")
    _variant(db, no_missing, "x")
    _variant(db, no_missing, "bluesky")
    _variant(db, no_missing, "linkedin")
    _variant(db, no_missing, "mastodon")

    report = build_content_variant_platform_parity_report(
        db,
        days=7,
        platforms=["x", "bluesky", "linkedin", "mastodon"],
        limit=10,
        now=NOW,
    )

    assert [item.content_id for item in report.items] == [one_missing, two_missing]
    assert [len(item.missing_platforms) for item in report.items] == [2, 2]

    higher_missing = _content(db, "three missing")
    _variant(db, higher_missing, "x")
    report = build_content_variant_platform_parity_report(
        db,
        days=7,
        platforms=["x", "bluesky", "linkedin", "mastodon"],
        limit=10,
        now=NOW,
    )

    assert report.items[0].content_id == higher_missing
    assert len(report.items[0].missing_platforms) == 3


def test_text_formatter_missing_schema_and_cli_json(db, monkeypatch, capsys):
    content_id = _content(db, "cli gap")
    _variant(db, content_id, "x")
    monkeypatch.setattr(
        content_variant_platform_parity_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        content_variant_platform_parity_script,
        "build_content_variant_platform_parity_report",
        lambda db, **kwargs: build_content_variant_platform_parity_report(db, now=NOW, **kwargs),
    )

    exit_code = content_variant_platform_parity_script.main(
        [
            "--format",
            "json",
            "--days",
            "7",
            "--limit",
            "5",
            "--platform",
            "x",
            "--platform",
            "bluesky",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert list(payload) == sorted(payload)
    assert payload["filters"]["platforms"] == ["bluesky", "x"]
    assert payload["items"][0]["content_id"] == content_id

    text = format_content_variant_platform_parity_text(
        build_content_variant_platform_parity_report(db, platforms=["x", "bluesky"], now=NOW)
    )
    assert "Content Variant Platform Parity" in text
    assert "missing=bluesky" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_content_variant_platform_parity_report(conn, now=NOW)
    assert missing.missing_tables == ("generated_content", "content_variants")
    assert "Missing tables: generated_content, content_variants" in (
        format_content_variant_platform_parity_text(missing)
    )

    invalid = content_variant_platform_parity_script.main(["--platform", " "])
    captured = capsys.readouterr()
    assert invalid == 1
    assert "at least one platform is required" in captured.err
