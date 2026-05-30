from __future__ import annotations
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.publish_queue_manual_override_audit import (
    build_publish_queue_manual_override_audit_report,
    build_publish_queue_manual_override_audit_report_from_db,
    format_publish_queue_manual_override_audit_text,
)

NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_manual_override_audit.py"
spec = importlib.util.spec_from_file_location("publish_queue_manual_override_audit_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_flags_override_gaps_independently():
    report = build_publish_queue_manual_override_audit_report(
        [
            {
                "queue_id": 1,
                "status": "queued",
                "manual_override": True,
                "quality_status": "failed",
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        max_age_hours=24,
        lookback_days=None,
        now=NOW,
    )
    assert report["artifact_type"] == "publish_queue_manual_override_audit"
    assert {"missing_actor", "missing_reason", "stale_override", "quality_gate_conflict"} <= {
        finding["gap_reason"] for finding in report["findings"]
    }


def test_db_metadata_fallbacks():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE publish_queue (id INTEGER, status TEXT, metadata TEXT, created_at TEXT)")
    conn.execute(
        "INSERT INTO publish_queue VALUES (1,'queued',?,?)",
        (json.dumps({"manual_override": True, "actor": "u"}), NOW.isoformat()),
    )
    report = build_publish_queue_manual_override_audit_report_from_db(conn, now=NOW)
    assert "missing_reason" in {finding["gap_reason"] for finding in report["findings"]}


def test_override_audit_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE publish_queue (id TEXT, content_id TEXT, override_type TEXT, override_actor TEXT, override_reason TEXT, status TEXT, metadata TEXT, updated_at TEXT)"
    )
    conn.execute("INSERT INTO publish_queue VALUES ('q1','c1','priority',NULL,NULL,NULL,'{}','2026-05-24T00:00:00+00:00')")
    conn.execute(
        "INSERT INTO publish_queue VALUES ('q2','c2',NULL,NULL,NULL,NULL,'{\"manual_override\":\"status\",\"actor\":\"ann\"}','2026-05-24T00:00:00+00:00')"
    )
    report = build_publish_queue_manual_override_audit_report_from_db(
        conn, now=datetime(2026, 5, 25, tzinfo=timezone.utc)
    )
    assert {"missing_actor", "missing_reason", "missing_publication_outcome"} <= {
        item["issue_type"] for item in report["findings"]
    }
    assert "Manual Override" in format_publish_queue_manual_override_audit_text(report)
    conn.commit()
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as out:
        conn.backup(out)
    assert script.main(["--db", str(path), "--format", "text"]) == 0
    assert "Manual Override" in capsys.readouterr().out
    assert script.main(["--db", str(path), "--limit", "0"]) == 2


def test_override_schema_gap():
    assert build_publish_queue_manual_override_audit_report_from_db(sqlite3.connect(":memory:"))["missing_tables"] == [
        "publish_queue"
    ]
