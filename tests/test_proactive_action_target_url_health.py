"""Tests for proactive action target URL health reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.proactive_action_target_url_health import (
    build_proactive_action_target_url_health_report,
    build_proactive_action_target_url_health_report_from_db,
    format_proactive_action_target_url_health_json,
    format_proactive_action_target_url_health_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_target_url_health.py"
spec = importlib.util.spec_from_file_location("proactive_action_target_url_health_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_url_health_flags_missing_malformed_mismatch_and_duplicates():
    rows = [
        {"id": 1, "platform": "x", "status": "pending", "target_url": None},
        {"id": 2, "platform": "x", "status": "pending", "target_url": "not-a-url"},
        {"id": 3, "platform": "x", "status": "pending", "target_url": "https://bsky.app/profile/a/post/1"},
        {"id": 4, "platform": "github", "status": "pending", "target_url": "https://github.com/org/repo/issues/1"},
        {"id": 5, "platform": "github", "status": "pending", "target_url": "https://github.com/org/repo/issues/1/"},
    ]
    report = build_proactive_action_target_url_health_report(rows, now=NOW)
    assert report["artifact_type"] == "proactive_action_target_url_health"
    assert report["summary"]["by_issue_type"] == {
        "duplicate_target_url": 2,
        "malformed_target_url": 1,
        "missing_target_url": 1,
        "platform_url_mismatch": 1,
    }
    assert {item["issue_type"] for item in report["findings"]} >= {"missing_target_url", "malformed_target_url", "platform_url_mismatch", "duplicate_target_url"}


def test_db_adapter_action_queue_metadata_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE action_queue (id INTEGER PRIMARY KEY, platform TEXT, state TEXT, metadata TEXT)")
    conn.execute("INSERT INTO action_queue VALUES (1, 'linkedin', 'queued', ?)", (json.dumps({"target_url": "https://example.com/post"}),))
    report = build_proactive_action_target_url_health_report_from_db(conn, status="queued", now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["issue_type"] == "platform_url_mismatch"
    assert json.loads(format_proactive_action_target_url_health_json(report))["artifact_type"] == "proactive_action_target_url_health"
    assert "Proactive Action Target URL Health" in format_proactive_action_target_url_health_text(report)

    db_path = tmp_path / "actions.sqlite"
    disk = sqlite3.connect(db_path)
    conn.commit()
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--status", "queued"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--status", "queued", "--format", "text"]) == 0
    assert "Proactive Action Target URL Health" in capsys.readouterr().out


def test_missing_schema_platform_filter_and_cli_validation():
    assert build_proactive_action_target_url_health_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["proactive_actions|action_queue"]
    rows = [
        {"id": 1, "platform": "x", "status": "pending", "target_url": "https://example.com/a"},
        {"id": 2, "platform": "github", "status": "pending", "target_url": "https://example.com/b"},
    ]
    report = build_proactive_action_target_url_health_report(rows, platform="github", now=NOW)
    assert [item["action_id"] for item in report["findings"]] == [2]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
