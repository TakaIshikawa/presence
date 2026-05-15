"""Tests for source claim reuse risk reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.source_claim_reuse_risk import (
    build_source_claim_reuse_risk_report,
    build_source_claim_reuse_risk_report_from_db,
    format_source_claim_reuse_risk_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_claim_reuse_risk.py"
spec = importlib.util.spec_from_file_location("source_claim_reuse_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _claim(content_id: str, content_type: str, text: str, *, source: str = "https://example.com/a", days: int = 1) -> dict:
    return {
        "content_id": content_id,
        "content_type": content_type,
        "source_url": source,
        "claim_text": text,
        "used_at": (NOW - timedelta(days=days)).isoformat(),
    }


def test_groups_similar_claim_text_under_same_source():
    rows = [
        _claim("post-1", "blog", "The launch reduced latency by 40%."),
        _claim("news-1", "newsletter", "Launch reduced the latency by 40 percent."),
        _claim("draft-1", "draft", "The launch reduced latency by 40%."),
    ]

    report = build_source_claim_reuse_risk_report(rows, medium_threshold=2, high_threshold=4, now=NOW)

    assert len(report["risks"]) == 2
    repeated = report["risks"][0]
    assert repeated["reuse_count"] == 2
    assert repeated["risk_level"] == "medium"
    assert repeated["content_ids"] == ["draft-1", "post-1"]
    assert "blog" in repeated["content_types"]
    assert "claim" in format_source_claim_reuse_risk_text(report).lower()


def test_window_and_thresholds_control_risk_levels():
    rows = [
        _claim("a", "blog", "Same claim", days=1),
        _claim("b", "blog", "Same claim", days=2),
        _claim("c", "blog", "Same claim", days=40),
    ]

    report = build_source_claim_reuse_risk_report(
        rows,
        window_days=30,
        medium_threshold=2,
        high_threshold=2,
        now=NOW,
    )

    assert report["risks"][0]["reuse_count"] == 2
    assert report["risks"][0]["risk_level"] == "high"


def test_output_includes_source_claim_content_ids_and_mitigation():
    report = build_source_claim_reuse_risk_report(
        [_claim("1", "post", "Teams need clear rollback plans")],
        now=NOW,
    )

    risk = report["risks"][0]
    assert risk["source_identifier"] == "https://example.com/a"
    assert risk["normalized_claim"] == "teams need clear rollback plans"
    assert risk["content_ids"] == ["1"]
    assert risk["recommended_action"] == "track reuse"


def test_db_loader_and_cli_json_output(monkeypatch, capsys, tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "claims.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE source_claims (
           id INTEGER PRIMARY KEY,
           content_id TEXT,
           content_type TEXT,
           source_url TEXT,
           claim_text TEXT,
           created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO source_claims (content_id, content_type, source_url, claim_text, created_at) VALUES (?, ?, ?, ?, ?)",
        ("post-1", "blog", "https://example.com/a", "Same claim", NOW.isoformat()),
    )
    conn.commit()

    report = build_source_claim_reuse_risk_report_from_db(conn, now=NOW)
    assert report["risks"][0]["content_ids"] == ["post-1"]

    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_source_claim_reuse_risk_report_from_db",
        lambda db, **kwargs: build_source_claim_reuse_risk_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "source_claim_reuse_risk"

    assert script.main(["--table"]) == 0
    assert "Source Claim Reuse Risk" in capsys.readouterr().out
