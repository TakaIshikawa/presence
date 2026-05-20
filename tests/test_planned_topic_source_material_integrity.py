from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.planned_topic_source_material_integrity import (
    build_planned_topic_source_material_integrity_report,
    build_planned_topic_source_material_integrity_report_from_db,
    format_planned_topic_source_material_integrity_json,
    format_planned_topic_source_material_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "planned_topic_source_material_integrity.py"
spec = importlib.util.spec_from_file_location("planned_topic_source_material_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_source_material_and_lifecycle_issues():
    report = build_planned_topic_source_material_integrity_report(
        [
            {
                "planned_topic_id": 1,
                "campaign_id": 10,
                "topic": "Malformed",
                "status": "planned",
                "target_date": "2026-05-21",
                "source_material": '{"bad"',
                "content_id": None,
            },
            {
                "planned_topic_id": 2,
                "campaign_id": 10,
                "topic": "Generated",
                "status": "generated",
                "target_date": "2026-05-22",
                "source_material": "free text",
                "content_id": None,
            },
            {
                "planned_topic_id": 3,
                "campaign_id": 10,
                "topic": "Planned",
                "status": "planned",
                "target_date": "not-a-date",
                "source_material": "[1, 2]",
                "content_id": 44,
            },
            {
                "planned_topic_id": 4,
                "campaign_id": 11,
                "topic": "Skipped",
                "status": "skipped",
                "target_date": "2026-05-23",
                "source_material": None,
                "content_id": 45,
            },
        ],
        now=NOW,
    )
    payload = json.loads(format_planned_topic_source_material_integrity_json(report))

    assert payload["artifact_type"] == "planned_topic_source_material_integrity"
    assert payload["summary"]["by_issue_type"] == {
        "generated_missing_content_id": 1,
        "invalid_target_date": 1,
        "malformed_source_material_json": 1,
        "planned_with_content_id": 1,
        "skipped_with_content_id": 1,
    }
    assert payload["findings"]["malformed_source_material_json"][0]["planned_topic_id"] == 1
    assert payload["findings"]["invalid_target_date"][0]["topic"] == "Planned"
    assert "Planned Topic Source Material Integrity" in format_planned_topic_source_material_integrity_text(report)


def test_builder_leaves_optional_free_text_source_material_clean():
    report = build_planned_topic_source_material_integrity_report(
        [
            {
                "planned_topic_id": 1,
                "campaign_id": None,
                "topic": "Clean",
                "status": "planned",
                "target_date": "2026-05-21",
                "source_material": "commit abc123 and notes",
                "content_id": None,
            }
        ],
        now=NOW,
    )

    assert report["summary"]["issue_count"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_from_db_supports_filters_and_schema_gaps(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            status TEXT,
            target_date TEXT,
            source_material TEXT,
            content_id INTEGER,
            created_at TEXT
        );
        INSERT INTO planned_topics VALUES (1, 10, 'Old', 'planned', '2026-01-01', '{bad', 1, '2026-01-01T00:00:00+00:00');
        INSERT INTO planned_topics VALUES (2, 10, 'Included', 'planned', '2026-05-21', NULL, 22, '2026-05-19T00:00:00+00:00');
        INSERT INTO planned_topics VALUES (3, 11, 'Other campaign', 'planned', '2026-05-21', NULL, 33, '2026-05-19T00:00:00+00:00');
        INSERT INTO planned_topics VALUES (4, 10, 'Other status', 'skipped', '2026-05-21', NULL, 44, '2026-05-19T00:00:00+00:00');
        """
    )

    report = build_planned_topic_source_material_integrity_report_from_db(
        conn,
        status="planned",
        campaign_id=10,
        days=7,
        now=NOW,
    )
    assert report["summary"]["planned_topic_count"] == 1
    assert report["findings"]["planned_with_content_id"][0]["planned_topic_id"] == 2

    db_path = tmp_path / "planned.sqlite"
    file_conn = sqlite3.connect(db_path)
    conn.backup(file_conn)
    file_conn.close()
    assert script.main(["--db", str(db_path), "--status", "planned", "--campaign-id", "10", "--days", "7", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["planned_topic_count"] == 1

    missing_table = build_planned_topic_source_material_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["planned_topics"]

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE planned_topics (id INTEGER PRIMARY KEY, topic TEXT)")
    missing_columns = build_planned_topic_source_material_integrity_report_from_db(partial, now=NOW)
    assert "content_id" in missing_columns["missing_columns"]["planned_topics"]
