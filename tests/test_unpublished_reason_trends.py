"""Tests for unpublished reason trend reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.unpublished_reason_trends import (
    build_unpublished_reason_trends_report,
    build_unpublished_reason_trends_report_from_db,
    format_unpublished_reason_trends_json,
    format_unpublished_reason_trends_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "unpublished_reason_trends.py"
spec = importlib.util.spec_from_file_location("unpublished_reason_trends_script", SCRIPT_PATH)
unpublished_reason_trends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(unpublished_reason_trends_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _row(content_id: str, days_ago: int, reason: str, *, channel: str = "x", stage: str = "review", status: str = "rejected") -> dict:
    return {
        "content_id": content_id,
        "created_at": (NOW - timedelta(days=days_ago)).isoformat(),
        "reason": reason,
        "channel": channel,
        "pipeline_stage": stage,
        "status": status,
    }


def test_aggregates_reasons_by_configurable_time_window():
    report = build_unpublished_reason_trends_report(
        [
            _row("a", 1, "unsupported claims"),
            _row("b", 2, "unsupported claims"),
            _row("c", 9, "persona mismatch"),
        ],
        days=14,
        window_days=7,
        now=NOW,
    )

    assert report["totals"]["record_count"] == 3
    assert report["totals"]["reason_counts"]["unsupported_claims"] == 2
    assert len(report["trend_rows"]) == 2
    assert report["trend_rows"][1]["reason"] == "unsupported_claims"
    assert report["trend_rows"][1]["count"] == 2


def test_includes_channel_and_pipeline_stage_breakdowns():
    report = build_unpublished_reason_trends_report(
        [
            _row("a", 1, "duplicate", channel="x", stage="quality"),
            _row("b", 1, "duplicate", channel="newsletter", stage="quality"),
        ],
        days=7,
        window_days=7,
        now=NOW,
    )

    row = report["trend_rows"][0]

    assert row["channels"] == {"newsletter": 1, "x": 1}
    assert row["pipeline_stages"] == {"quality": 2}
    assert report["totals"]["channel_counts"]["newsletter"] == 1


def test_skips_published_rows_and_outside_window():
    report = build_unpublished_reason_trends_report(
        [_row("a", 1, "duplicate", status="published"), _row("b", 40, "duplicate"), _row("c", 1, "duplicate")],
        days=30,
        now=NOW,
    )

    assert report["totals"]["record_count"] == 1
    assert report["totals"]["published_or_accepted"] == 1
    assert report["totals"]["outside_window"] == 1


def test_db_loader_json_text_and_cli(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE pipeline_runs (
            id TEXT,
            content_id TEXT,
            outcome TEXT,
            rejection_reason TEXT,
            channel TEXT,
            stage TEXT,
            created_at TEXT
        )"""
    )
    conn.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?)", ("r1", "c1", "rejected", "low quality", "blog", "review", NOW.isoformat()))
    conn.commit()

    report = build_unpublished_reason_trends_report_from_db(conn, now=NOW)

    assert json.loads(format_unpublished_reason_trends_json(report))["artifact_type"] == "unpublished_reason_trends"
    assert "reason=low_quality" in format_unpublished_reason_trends_text(report)
    monkeypatch.setattr(unpublished_reason_trends_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        unpublished_reason_trends_script,
        "build_unpublished_reason_trends_report_from_db",
        lambda db, **kwargs: build_unpublished_reason_trends_report_from_db(db, now=NOW, **kwargs),
    )
    assert unpublished_reason_trends_script.main(["--table"]) == 0
    assert "Unpublished Reason Trends" in capsys.readouterr().out
