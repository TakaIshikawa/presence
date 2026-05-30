"""Tests for content feedback revision adoption reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_feedback_revision_adoption import (
    build_content_feedback_revision_adoption_report,
    build_content_feedback_revision_adoption_report_from_db,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_feedback_revision_adoption.py"
spec = importlib.util.spec_from_file_location("content_feedback_revision_adoption_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_adoption_unadopted_and_conflicts():
    report = build_content_feedback_revision_adoption_report(
        [
            {"id": 1, "content_id": 1, "feedback_type": "revise", "replacement_text": "Use sharper copy", "source_content": "Use sharper   copy now", "created_at": "2026-05-19T00:00:00+00:00"},
            {"id": 2, "content_id": 2, "feedback_type": "prefer", "replacement_text": "Variant wins", "variant_content": "variant WINS", "created_at": "2026-05-19T00:00:00+00:00"},
            {"id": 3, "content_id": 3, "feedback_type": "revise", "replacement_text": "Lost change", "source_content": "old", "created_at": "2026-05-18T00:00:00+00:00"},
            {"id": 4, "content_id": 3, "feedback_type": "prefer", "replacement_text": "Other change", "created_at": "2026-05-18T00:00:00+00:00"},
        ],
        min_age_hours=24,
        now=NOW,
    )
    assert report["artifact_type"] == "content_feedback_revision_adoption"
    assert report["totals"]["by_reason"]["adopted_in_source_content"] == 1
    assert report["totals"]["by_reason"]["adopted_in_variant"] == 1
    assert report["totals"]["by_reason"]["unadopted_revision"] == 2
    assert report["totals"]["by_reason"]["conflicting_replacements"] == 1


def test_db_loading_missing_schema_and_cli(tmp_path, capsys):
    db_path = tmp_path / "feedback.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content TEXT);
        CREATE TABLE content_feedback (id INTEGER PRIMARY KEY, content_id INTEGER, feedback_type TEXT, replacement_text TEXT, created_at TEXT);
        CREATE TABLE content_variants (id INTEGER PRIMARY KEY, content_id INTEGER, content TEXT);
        CREATE TABLE content_publications (id INTEGER PRIMARY KEY, content_id INTEGER, platform_url TEXT, platform_post_id TEXT, status TEXT);
        INSERT INTO generated_content VALUES (1, 'Original with exact replacement');
        INSERT INTO content_feedback VALUES (1, 1, 'revise', 'exact replacement', '2026-05-18T00:00:00+00:00');
        INSERT INTO generated_content VALUES (2, 'Original');
        INSERT INTO content_feedback VALUES (2, 2, 'prefer', 'Published Token', '2026-05-18T00:00:00+00:00');
        INSERT INTO content_publications VALUES (1, 2, 'https://example.test/Published-Token', NULL, 'published');
        """
    )
    conn.commit()
    report = build_content_feedback_revision_adoption_report_from_db(conn, min_age_hours=1, now=NOW)
    assert report["totals"]["by_reason"]["adopted_in_source_content"] == 1
    assert report["totals"]["by_reason"]["adopted_in_variant"] == 1
    assert build_content_feedback_revision_adoption_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == [
        "content_feedback",
        "generated_content",
    ]
    assert script.main(["--db", str(db_path), "--format", "json", "--min-age-hours", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "content_feedback_revision_adoption"
    assert script.main(["--limit", "0"]) == 2
    assert script.main(["--min-age-hours", "-1"]) == 2
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_feedback_revision_adoption_report([], limit=0)
