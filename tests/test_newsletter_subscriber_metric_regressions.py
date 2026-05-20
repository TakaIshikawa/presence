"""Tests for newsletter subscriber metric regression reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_subscriber_metric_regressions import (
    build_newsletter_subscriber_metric_regressions_report,
    build_newsletter_subscriber_metric_regressions_report_from_db,
    format_newsletter_subscriber_metric_regressions_json,
    format_newsletter_subscriber_metric_regressions_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subscriber_metric_regressions.py"
spec = importlib.util.spec_from_file_location("newsletter_subscriber_metric_regressions_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE newsletter_subscriber_metrics (
            id INTEGER PRIMARY KEY,
            subscriber_count INTEGER,
            active_subscriber_count INTEGER,
            unsubscribes INTEGER,
            churn_rate REAL,
            new_subscribers INTEGER,
            net_subscriber_change INTEGER,
            raw_metrics TEXT,
            fetched_at TEXT,
            created_at TEXT
        );
        """
    )
    return conn


def _metric(conn: sqlite3.Connection, row: tuple[object, ...]) -> None:
    conn.execute(
        """INSERT INTO newsletter_subscriber_metrics
           (id, subscriber_count, active_subscriber_count, unsubscribes, churn_rate,
            new_subscribers, net_subscriber_change, raw_metrics, fetched_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        row,
    )


def test_builder_flags_regressions_raw_metrics_and_ordering_anomalies():
    report = build_newsletter_subscriber_metric_regressions_report(
        [
            {"id": 2, "active_subscriber_count": 120, "churn_rate": 0.01, "raw_metrics": {"source": "buttondown"}, "fetched_at": "2026-05-19T00:00:00+00:00"},
            {"id": 3, "active_subscriber_count": 105, "churn_rate": 0.04, "raw_metrics": "", "fetched_at": "2026-05-20T00:00:00+00:00"},
            {"id": 1, "active_subscriber_count": 106, "churn_rate": 0.041, "raw_metrics": "{bad", "fetched_at": "2026-05-21T00:00:00+00:00"},
            {"id": 4, "active_subscriber_count": 107, "churn_rate": 0.042, "raw_metrics": "{}", "fetched_at": "not-a-date"},
        ],
        drop_threshold=10,
        churn_delta_threshold=0.02,
        now=NOW,
    )
    payload = json.loads(format_newsletter_subscriber_metric_regressions_json(report))

    assert payload["artifact_type"] == "newsletter_subscriber_metric_regressions"
    assert payload["totals"]["by_reason"] == {
        "active_subscriber_drop": 1,
        "churn_spike": 1,
        "fetched_at_ordering_anomaly": 2,
        "missing_raw_metrics": 3,
    }
    assert [group["reason"] for group in payload["findings"]] == [
        "active_subscriber_drop",
        "churn_spike",
        "missing_raw_metrics",
        "fetched_at_ordering_anomaly",
    ]
    assert payload["findings"][0]["items"][0]["active_delta"] == -15
    assert "reason | metric_id | previous_metric_id" in format_newsletter_subscriber_metric_regressions_text(report)


def test_from_db_compares_consecutive_snapshots_and_reports_schema_gaps():
    conn = _conn()
    _metric(conn, (1, 100, 98, 1, 0.01, 2, 2, '{"source":"buttondown"}', "2026-05-18T00:00:00+00:00", "2026-05-18T00:01:00+00:00"))
    _metric(conn, (2, 101, 96, 2, 0.015, 1, 1, '{"source":"buttondown"}', "2026-05-19T00:00:00+00:00", "2026-05-19T00:01:00+00:00"))
    _metric(conn, (3, 99, 80, 5, 0.05, 0, -2, None, "2026-05-20T00:00:00+00:00", "2026-05-20T00:01:00+00:00"))

    report = build_newsletter_subscriber_metric_regressions_report_from_db(
        conn,
        drop_threshold=10,
        churn_delta_threshold=0.02,
        limit=2,
        now=NOW,
    )
    assert report["totals"]["snapshot_count"] == 3
    assert report["totals"]["finding_count"] == 3
    assert report["totals"]["shown_count"] == 2
    assert report["findings"][0]["items"][0]["metric_id"] == 3

    missing_table = build_newsletter_subscriber_metric_regressions_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["newsletter_subscriber_metrics"]

    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE newsletter_subscriber_metrics (id INTEGER PRIMARY KEY, fetched_at TEXT)")
    gaps = build_newsletter_subscriber_metric_regressions_report_from_db(partial, now=NOW)
    assert gaps["missing_columns"]["newsletter_subscriber_metrics"] == [
        "active_subscriber_count",
        "churn_rate",
        "created_at",
        "net_subscriber_change",
        "new_subscribers",
        "raw_metrics",
        "subscriber_count",
        "unsubscribes",
    ]


def test_cli_db_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "newsletter.sqlite"
    conn = _conn(db_path)
    _metric(conn, (1, 100, 100, 1, 0.01, 1, 1, '{"source":"buttondown"}', "2026-05-19T00:00:00+00:00", None))
    _metric(conn, (2, 90, 80, 3, 0.05, 0, -10, "", "2026-05-20T00:00:00+00:00", None))
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--drop-threshold", "10", "--churn-delta-threshold", "0.02"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["drop_threshold"] == 10
    assert payload["totals"]["by_reason"]["active_subscriber_drop"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Subscriber Metric Regressions" in capsys.readouterr().out
    assert script.main(["--limit", "0"]) == 2
    assert script.main(["--drop-threshold", "-1"]) == 2
    assert script.main(["--churn-delta-threshold", "-0.1"]) == 2
    with pytest.raises(ValueError, match="limit must be positive"):
        build_newsletter_subscriber_metric_regressions_report([], limit=0)
