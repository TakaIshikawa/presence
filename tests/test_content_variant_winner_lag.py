"""Tests for content variant winner lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.content_variant_winner_lag import (
    build_content_variant_winner_lag_report,
    build_content_variant_winner_lag_report_from_db,
    format_content_variant_winner_lag_json,
    format_content_variant_winner_lag_text,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_winner_lag.py"
spec = importlib.util.spec_from_file_location("content_variant_winner_lag_script", SCRIPT_PATH)
content_variant_winner_lag_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_variant_winner_lag_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_computes_lags_and_flags_stale_winners():
    rows = [
        {
            "variant_id": "v1",
            "content_id": "c1",
            "selected": 1,
            "channel": "x",
            "content_type": "post",
            "selected_at": (NOW - timedelta(hours=10)).isoformat(),
            "reviewed_at": (NOW - timedelta(hours=8)).isoformat(),
            "approved_at": (NOW - timedelta(hours=4)).isoformat(),
            "published_at": (NOW - timedelta(hours=1)).isoformat(),
        },
        {
            "variant_id": "v2",
            "selected": 1,
            "channel": "x",
            "content_type": "post",
            "selected_at": (NOW - timedelta(hours=72)).isoformat(),
        },
        {"variant_id": "v3", "selected": 0, "channel": "x", "selected_at": NOW.isoformat()},
    ]

    report = build_content_variant_winner_lag_report(rows, stale_hours=48, now=NOW)

    assert report["winners"][0]["selected_to_review_hours"] == 2.0
    assert report["winners"][0]["selected_to_approved_hours"] == 6.0
    assert report["winners"][0]["selected_to_published_hours"] == 9.0
    assert report["stale_winners"][0]["variant_id"] == "v2"
    assert report["summary_by_channel_content_type"][0]["unknown_review_count"] == 1
    assert json.loads(format_content_variant_winner_lag_json(report))["totals"]["winner_count"] == 2


def test_missing_timestamps_are_unknown():
    report = build_content_variant_winner_lag_report(
        [{"variant_id": "v1", "selected": 1, "channel": "newsletter", "content_type": "summary"}],
        now=NOW,
    )

    assert report["winners"][0]["lag_status"] == "unknown"
    assert report["winners"][0]["selected_to_review_hours"] is None
    assert "unknown" in format_content_variant_winner_lag_text(report)


def test_db_loader_reads_variant_metadata(db):
    content_id = db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, published, published_at, created_at)
           VALUES ('Copy', 'x_post', 1, ?, ?)""",
        ((NOW - timedelta(hours=2)).isoformat(), (NOW - timedelta(hours=80)).isoformat()),
    ).lastrowid
    db.conn.execute(
        """INSERT INTO content_variants
           (content_id, platform, variant_type, content, metadata, selected, created_at)
           VALUES (?, 'x', 'post', 'Copy', ?, 1, ?)""",
        (
            content_id,
            json.dumps({"reviewed_at": (NOW - timedelta(hours=70)).isoformat(), "approved_at": (NOW - timedelta(hours=60)).isoformat()}),
            (NOW - timedelta(hours=80)).isoformat(),
        ),
    )
    db.conn.commit()

    report = build_content_variant_winner_lag_report_from_db(db, stale_hours=48, now=NOW)

    assert report["winners"][0]["selected_to_review_hours"] == 10.0
    assert report["winners"][0]["selected_to_published_hours"] == 78.0
    assert report["stale_winners"] == []


def test_cli_outputs_json_and_text(db, file_db, capsys):
    for database in (db, file_db):
        content_id = database.conn.execute(
            """INSERT INTO generated_content (content, content_type, published, created_at)
               VALUES ('Copy', 'x_post', 0, ?)""",
            ((NOW - timedelta(hours=72)).isoformat(),),
        ).lastrowid
        database.conn.execute(
            """INSERT INTO content_variants
               (content_id, platform, variant_type, content, selected, created_at)
               VALUES (?, 'x', 'post', 'Copy', 1, ?)""",
            (content_id, (NOW - timedelta(hours=72)).isoformat()),
        )
        database.conn.commit()

    with patch.object(
        content_variant_winner_lag_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        content_variant_winner_lag_script,
        "build_content_variant_winner_lag_report_from_db",
        wraps=lambda db, **kwargs: build_content_variant_winner_lag_report_from_db(db, now=NOW, **kwargs),
    ):
        assert content_variant_winner_lag_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["stale_winner_count"] == 1

    assert content_variant_winner_lag_script.main(["--db", str(file_db.db_path), "--format", "text"]) == 0
    assert "Content Variant Winner Lag" in capsys.readouterr().out
    assert content_variant_winner_lag_script.main(["--stale-hours", "0"]) == 2
