from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3

from evaluation.newsletter_subscriber_growth_inflection import (
    build_newsletter_subscriber_growth_inflection_report,
    format_newsletter_subscriber_growth_inflection_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def _metric(db, subscriber_count, active_count, churn_rate, fetched_at):
    db.conn.execute(
        """INSERT INTO newsletter_subscriber_metrics
           (subscriber_count, active_subscriber_count, churn_rate, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (subscriber_count, active_count, churn_rate, fetched_at),
    )
    db.conn.commit()


def test_metric_points_are_ordered_and_compute_deltas(db):
    _metric(db, 100, 90, 0.01, "2026-05-10T00:00:00+00:00")
    _metric(db, 180, 160, 0.015, "2026-05-11T00:00:00+00:00")
    _metric(db, 140, 120, 0.05, "2026-05-12T00:00:00+00:00")
    _metric(db, 145, None, 0.051, "2026-05-13T00:00:00+00:00")

    payload = json.loads(format_newsletter_subscriber_growth_inflection_json(
        build_newsletter_subscriber_growth_inflection_report(
            db,
            growth_spike_delta=50,
            growth_drop_delta=-25,
            churn_spike_delta=0.02,
            now=NOW,
        )
    ))

    assert payload["artifact_type"] == "newsletter_subscriber_growth_inflection"
    points = payload["metric_points"]
    assert [point["subscriber_delta"] for point in points] == [None, 80, -40, 5]
    assert [point["active_delta"] for point in points] == [None, 70, -40, None]
    assert points[2]["churn_rate_delta"] == 0.035
    finding_types = {finding["finding_type"] for finding in payload["findings"]}
    assert {"growth_spike", "growth_drop", "churn_spike", "missing_active_count"}.issubset(finding_types)


def test_thresholds_are_configurable(db):
    _metric(db, 100, 90, 0.01, "2026-05-10T00:00:00+00:00")
    _metric(db, 115, 100, 0.015, "2026-05-11T00:00:00+00:00")

    default = build_newsletter_subscriber_growth_inflection_report(db, now=NOW)
    lower = build_newsletter_subscriber_growth_inflection_report(db, growth_spike_delta=10, now=NOW)

    assert default["findings"] == []
    assert lower["findings"][0]["finding_type"] == "growth_spike"


def test_missing_metrics_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_newsletter_subscriber_growth_inflection_report(conn, now=NOW)

    assert report["missing_tables"] == ["newsletter_subscriber_metrics"]
    assert report["metric_points"] == []
