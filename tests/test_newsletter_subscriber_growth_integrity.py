from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.newsletter_subscriber_growth_integrity import (
    build_newsletter_subscriber_growth_integrity_report,
    build_newsletter_subscriber_growth_integrity_report_from_db,
    format_newsletter_subscriber_growth_integrity_json,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subscriber_growth_integrity.py"
spec = importlib.util.spec_from_file_location("newsletter_subscriber_growth_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _metric(db, subscriber_count, active_count, churn_rate, net_change, fetched_at, *, unsubscribes=0, new_subscribers=0):
    db.conn.execute(
        """INSERT INTO newsletter_subscriber_metrics
           (subscriber_count, active_subscriber_count, unsubscribes, churn_rate,
            new_subscribers, net_subscriber_change, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (subscriber_count, active_count, unsubscribes, churn_rate, new_subscribers, net_change, fetched_at),
    )
    db.conn.commit()


def test_builder_groups_negative_values_active_over_total_churn_and_net_mismatch():
    report = build_newsletter_subscriber_growth_integrity_report(
        [
            {
                "id": 1,
                "subscriber_count": 100,
                "active_subscriber_count": 80,
                "unsubscribes": 0,
                "churn_rate": 0.02,
                "new_subscribers": 2,
                "net_subscriber_change": None,
                "fetched_at": "2026-05-18T00:00:00+00:00",
            },
            {
                "id": 2,
                "subscriber_count": 110,
                "active_subscriber_count": 120,
                "unsubscribes": -1,
                "churn_rate": 1.2,
                "new_subscribers": -5,
                "net_subscriber_change": 4,
                "fetched_at": "2026-05-19T00:00:00+00:00",
            },
        ],
        now=NOW,
    )
    payload = json.loads(format_newsletter_subscriber_growth_integrity_json(report))

    assert payload["artifact_type"] == "newsletter_subscriber_growth_integrity"
    assert payload["summary"]["by_issue_type"] == {
        "active_exceeds_total": 1,
        "churn_rate_out_of_range": 1,
        "negative_metric": 2,
        "net_subscriber_change_mismatch": 1,
    }
    assert {item["metric_name"] for item in payload["findings"]["negative_metric"]} == {"unsubscribes", "new_subscribers"}
    mismatch = payload["findings"]["net_subscriber_change_mismatch"][0]
    assert mismatch["expected_value"] == 10
    assert mismatch["previous_metric_id"] == 1


def test_from_db_orders_by_fetched_at_and_filters_clean_data(db):
    _metric(db, 120, 100, 0.03, 20, "2026-05-19T00:00:00+00:00")
    _metric(db, 100, 90, 0.01, None, "2026-05-18T00:00:00+00:00")

    report = build_newsletter_subscriber_growth_integrity_report_from_db(db, now=NOW, days=5)

    assert report["summary"]["metric_count"] == 2
    assert report["summary"]["finding_count"] == 0
    assert report["empty_state"]["is_empty"] is True


def test_from_db_reports_missing_schema_without_crashing():
    missing_table = build_newsletter_subscriber_growth_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["newsletter_subscriber_metrics"]

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE newsletter_subscriber_metrics (id INTEGER PRIMARY KEY, subscriber_count INTEGER, fetched_at TEXT)")
    missing_columns = build_newsletter_subscriber_growth_integrity_report_from_db(conn, now=NOW)
    assert "active_subscriber_count" in missing_columns["missing_columns"]["newsletter_subscriber_metrics"]
    assert missing_columns["summary"]["finding_count"] == 0


def test_cli_json_output_exits_successfully_for_empty_database(tmp_path, capsys):
    db_path = tmp_path / "empty.sqlite"
    sqlite3.connect(db_path).close()

    assert script.main(["--db", str(db_path), "--days", "7", "--limit", "5", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_subscriber_growth_integrity"
    assert payload["filters"] == {"days": 7, "limit": 5}
    assert payload["missing_tables"] == ["newsletter_subscriber_metrics"]
