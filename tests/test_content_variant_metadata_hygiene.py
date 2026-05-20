"""Tests for content variant metadata hygiene reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_variant_metadata_hygiene import (
    build_content_variant_metadata_hygiene_report,
    build_content_variant_metadata_hygiene_report_from_db,
    format_content_variant_metadata_hygiene_json,
    format_content_variant_metadata_hygiene_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_metadata_hygiene.py"
spec = importlib.util.spec_from_file_location("content_variant_metadata_hygiene_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE content_variants (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            variant_type TEXT,
            metadata TEXT,
            selected INTEGER
        )"""
    )
    return conn


def test_builder_detects_metadata_hygiene_issue_types():
    rows = [
        {"variant_id": 1, "metadata": "{bad", "platform": "x", "variant_type": "post", "selected": 0},
        {"variant_id": 2, "metadata": "[1, 2]", "platform": "x", "variant_type": "post", "selected": 0},
        {"variant_id": 3, "metadata": "{}", "platform": "x", "variant_type": "post", "selected": 1},
        {"variant_id": 4, "metadata": '{"source_url": "https://example.test", "platform": "bluesky"}', "platform": "x", "variant_type": "post", "selected": 1},
        {"variant_id": 5, "metadata": '{"source_id": "s1", "variant_type": "thread"}', "platform": "x", "variant_type": "post", "selected": 1},
    ]

    report = build_content_variant_metadata_hygiene_report(rows, now=NOW)
    payload = json.loads(format_content_variant_metadata_hygiene_json(report))

    assert payload["artifact_type"] == "content_variant_metadata_hygiene"
    assert payload["summary"]["row_count"] == 5
    assert payload["summary"]["issue_count"] == 5
    assert payload["missing_tables"] == []
    assert "provenance_fields" in payload["thresholds"]
    assert [item["issue_type"] for item in payload["issue_items"]] == [
        "malformed_metadata",
        "non_object_metadata",
        "missing_selected_provenance",
        "platform_conflict",
        "variant_type_conflict",
    ]


def test_from_db_loads_optional_columns_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO content_variants VALUES (1, 10, 'x', 'post', ?, 1)", ('{"source_url": "https://example.test"}',))
    conn.execute("INSERT INTO content_variants VALUES (2, 11, 'x', 'post', ?, 1)", ("{}",))
    conn.commit()

    report = build_content_variant_metadata_hygiene_report_from_db(conn, now=NOW)
    assert report["summary"]["issue_count"] == 1
    assert report["issue_items"][0]["variant_id"] == 2
    assert "Content Variant Metadata Hygiene" in format_content_variant_metadata_hygiene_text(report)

    minimal = sqlite3.connect(":memory:")
    minimal.row_factory = sqlite3.Row
    minimal.executescript(
        """
        CREATE TABLE content_variants (id INTEGER PRIMARY KEY, metadata TEXT);
        INSERT INTO content_variants (id, metadata) VALUES (1, '{"platform": "x"}');
        """
    )
    minimal_report = build_content_variant_metadata_hygiene_report_from_db(minimal, now=NOW)
    assert minimal_report["summary"]["issue_count"] == 0

    db_path = tmp_path / "variants.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["issue_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Variant Metadata Hygiene" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])

    missing = build_content_variant_metadata_hygiene_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_variants"]
