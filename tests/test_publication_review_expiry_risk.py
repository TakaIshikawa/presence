"""Tests for publication review expiry risk reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.publication_review_expiry_risk import (
    build_publication_review_expiry_risk_report,
    build_publication_review_expiry_risk_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_review_expiry_risk.py"
spec = importlib.util.spec_from_file_location("publication_review_expiry_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_ranks_old_approval_old_evidence_missed_window_and_no_attempt():
    report = build_publication_review_expiry_risk_report(
        [
            {
                "id": "c1",
                "content_type": "newsletter",
                "status": "approved",
                "approved_at": "2026-04-01T00:00:00+00:00",
                "scheduled_at": "2026-04-15T00:00:00+00:00",
            },
            {"id": "c2", "content_type": "x_post", "status": "approved", "approved_at": "2026-04-30T00:00:00+00:00", "publish_attempt_count": 1},
        ],
        [],
        [{"content_id": "c1", "source_published_at": "2026-03-01T00:00:00+00:00"}],
        now=NOW,
    )

    row = report["risks"][0]
    assert row["content_id"] == "c1"
    assert row["content_type"] == "newsletter"
    assert row["review_status"] == "approved"
    assert row["age_days"] == 30
    assert row["evidence_age_days"] == 61
    assert row["scheduled_at"] == "2026-04-15T00:00:00+00:00"
    assert row["risk_score"] > 0
    assert row["reasons"] == ["old_approval", "old_evidence", "missed_scheduled_window", "no_publish_attempt"]
    assert [item["content_id"] for item in report["risks"]] == ["c1"]


def test_uses_review_rows_and_excludes_published_content():
    report = build_publication_review_expiry_risk_report(
        [
            {"id": "c1", "content_type": "blog", "created_at": "2026-04-01T00:00:00+00:00"},
            {"id": "c2", "content_type": "blog", "status": "approved", "published_at": "2026-04-20T00:00:00+00:00"},
        ],
        [{"content_id": "c1", "decision": "approved", "decided_at": "2026-04-01T00:00:00+00:00"}],
        now=NOW,
    )

    assert [row["content_id"] for row in report["risks"]] == ["c1"]
    assert "old_approval" in report["risks"][0]["reasons"]


def test_db_adapter_is_schema_tolerant():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, status TEXT, approved_at TEXT, scheduled_at TEXT)")
    conn.execute("CREATE TABLE content_reviews (content_id TEXT, decision TEXT, decided_at TEXT)")
    conn.execute("CREATE TABLE generated_content_sources (content_id TEXT, source_published_at TEXT)")
    conn.execute("INSERT INTO generated_content VALUES ('c1', 'thread', 'approved', '2026-04-01T00:00:00+00:00', '2026-04-05T00:00:00+00:00')")
    conn.execute("INSERT INTO generated_content_sources VALUES ('c1', '2026-03-01T00:00:00+00:00')")

    report = build_publication_review_expiry_risk_report_from_db(conn, now=NOW)

    assert report["totals"]["reviewable_count"] == 1
    assert report["risks"][0]["content_id"] == "c1"


def test_empty_state_and_invalid_filters():
    report = build_publication_review_expiry_risk_report([], [], [], now=NOW)

    assert report["empty_state"]["is_empty"] is True
    with pytest.raises(ValueError):
        build_publication_review_expiry_risk_report([], approval_max_age_days=0)
    with pytest.raises(ValueError):
        build_publication_review_expiry_risk_report([], evidence_max_age_days=0)
    with pytest.raises(ValueError):
        build_publication_review_expiry_risk_report([], limit=0)


def test_cli_supports_json_and_text(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_publication_review_expiry_risk_report_from_db",
        lambda _db, **kwargs: build_publication_review_expiry_risk_report(
            [{"id": "c1", "status": "approved", "approved_at": "2026-04-01T00:00:00+00:00"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_review_expiry_risk"
    assert script.main(["--table"]) == 0
    assert "content_id | type | status" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--approval-max-age-days", "0"])
