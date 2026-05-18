"""Tests for reply draft evidence age mix reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engagement.reply_draft_evidence_age_mix import (
    build_reply_draft_evidence_age_mix_report,
    build_reply_draft_evidence_age_mix_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_evidence_age_mix.py"
spec = importlib.util.spec_from_file_location("reply_draft_evidence_age_mix_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_reports_stale_single_band_missing_dates_and_no_evidence():
    report = build_reply_draft_evidence_age_mix_report(
        [
            {"id": "d1", "target_author": "alice", "created_at": "2026-04-30T00:00:00+00:00"},
            {"id": "d2", "target_author": "bob", "created_at": "2026-04-30T00:00:00+00:00"},
            {"id": "d3", "target_author": "cyd", "created_at": "2026-04-30T00:00:00+00:00"},
        ],
        [
            {"draft_id": "d1", "published_at": "2025-01-01T00:00:00+00:00"},
            {"draft_id": "d1", "published_at": "2025-01-15T00:00:00+00:00"},
            {"draft_id": "d2"},
        ],
        now=NOW,
    )

    by_id = {row["draft_id"]: row for row in report["draft_risks"]}
    assert by_id["d1"]["target_author"] == "alice"
    assert by_id["d1"]["evidence_count"] == 2
    assert by_id["d1"]["oldest_evidence_age_days"] == 485
    assert by_id["d1"]["median_evidence_age_days"] == 478
    assert by_id["d1"]["freshness_mix"] == "single_age_band"
    assert by_id["d1"]["reasons"] == ["single_age_band", "stale_evidence"]
    assert by_id["d2"]["reasons"] == ["missing_evidence_dates"]
    assert by_id["d3"]["reasons"] == ["no_evidence"]
    assert report["totals"]["reason_counts"]["no_evidence"] == 1


def test_mixed_fresh_evidence_is_not_flagged():
    report = build_reply_draft_evidence_age_mix_report(
        [{"id": "d1", "target_author": "alice", "created_at": "2026-04-30T00:00:00+00:00"}],
        [
            {"draft_id": "d1", "published_at": "2026-04-15T00:00:00+00:00"},
            {"draft_id": "d1", "published_at": "2026-02-01T00:00:00+00:00"},
        ],
        now=NOW,
    )

    assert report["draft_risks"] == []
    assert report["empty_state"]["is_empty"] is True


def test_db_adapter_loads_reply_drafts_and_evidence():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_drafts (id TEXT, target_author TEXT, created_at TEXT)")
    conn.execute("CREATE TABLE reply_draft_evidence (draft_id TEXT, source_published_at TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES ('d1', 'alice', '2026-04-30T00:00:00+00:00')")
    conn.execute("INSERT INTO reply_draft_evidence VALUES ('d1', '2025-01-01T00:00:00+00:00')")
    conn.execute("INSERT INTO reply_draft_evidence VALUES ('d1', '2025-01-02T00:00:00+00:00')")

    report = build_reply_draft_evidence_age_mix_report_from_db(conn, now=NOW)

    assert report["draft_risks"][0]["draft_id"] == "d1"
    assert "stale_evidence" in report["draft_risks"][0]["reasons"]


def test_invalid_filters():
    with pytest.raises(ValueError):
        build_reply_draft_evidence_age_mix_report([], stale_days=0)
    with pytest.raises(ValueError):
        build_reply_draft_evidence_age_mix_report([], single_band_min_evidence=0)
    with pytest.raises(ValueError):
        build_reply_draft_evidence_age_mix_report([], limit=0)


def test_cli_supports_json_and_text(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_reply_draft_evidence_age_mix_report_from_db",
        lambda _db, **kwargs: build_reply_draft_evidence_age_mix_report(
            [{"id": "d1", "target_author": "alice", "created_at": "2026-04-30T00:00:00+00:00"}],
            [],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_draft_evidence_age_mix"
    assert script.main(["--table"]) == 0
    assert "draft_id | author | evidence" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
