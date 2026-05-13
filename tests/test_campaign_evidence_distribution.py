"""Tests for campaign evidence distribution reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.campaign_evidence_distribution import (
    build_campaign_evidence_distribution_report,
    format_campaign_evidence_distribution_json,
    format_campaign_evidence_distribution_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_evidence_distribution.py"
spec = importlib.util.spec_from_file_location("campaign_evidence_distribution_script", SCRIPT_PATH)
campaign_evidence_distribution_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_evidence_distribution_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, campaign: str) -> int:
    content_id = db.insert_generated_content("blog_post", [], [], "Campaign content", 8.0, "ok")
    db.conn.execute("UPDATE generated_content SET created_at = ? WHERE id = ?", (NOW.isoformat(), content_id))
    campaign_id = db.insert_content_campaign(campaign, status="active")
    topic_id = db.insert_planned_topic(campaign_id=campaign_id, topic=campaign)
    db.conn.execute("UPDATE planned_topics SET content_id = ? WHERE id = ?", (content_id, topic_id))
    db.conn.commit()
    return int(content_id)


def _evidence(db, content_id: int, url: str, source_type: str = "stat") -> None:
    row = db.conn.execute(
        """INSERT INTO knowledge (source_type, source_id, source_url, content, ingested_at)
           VALUES (?, ?, ?, 'Evidence', ?)""",
        (source_type, f"{content_id}-{url}", url, NOW.isoformat()),
    )
    db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, 0.9)",
        (content_id, row.lastrowid),
    )
    db.conn.commit()


def test_flags_missing_and_concentrated_evidence(db):
    one = _content(db, "launch")
    two = _content(db, "launch")
    three = _content(db, "launch")
    _evidence(db, one, "https://same.example/a")
    _evidence(db, two, "https://same.example/a")

    report = build_campaign_evidence_distribution_report(db, campaign="launch", now=NOW)
    labels = {(finding.risk_type, finding.label) for finding in report.findings}

    assert ("missing_evidence", "no cited evidence") in labels
    assert ("over_reused_domain", "same.example") in labels
    assert ("over_reused_url", "https://same.example/a") in labels
    assert report.totals["content_count"] == 3


def test_formatters_limit_and_missing_schema(db):
    one = _content(db, "focus")
    two = _content(db, "focus")
    _evidence(db, one, "https://same.example/a")
    _evidence(db, two, "https://same.example/a")
    report = build_campaign_evidence_distribution_report(db, limit=1, now=NOW)
    payload = json.loads(format_campaign_evidence_distribution_json(report))
    text = format_campaign_evidence_distribution_text(report)

    assert payload["artifact_type"] == "campaign_evidence_distribution"
    assert len(payload["findings"]) == 1
    assert "Campaign Evidence Distribution" in text

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_campaign_evidence_distribution_report(conn, now=NOW)
    assert missing.schema_warnings == ("missing table: generated_content",)


def test_cli_outputs_json(db, monkeypatch, capsys):
    one = _content(db, "cli")
    two = _content(db, "cli")
    _evidence(db, one, "https://same.example/a")
    _evidence(db, two, "https://same.example/a")
    monkeypatch.setattr(campaign_evidence_distribution_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        campaign_evidence_distribution_script,
        "build_campaign_evidence_distribution_report",
        lambda db, **kwargs: build_campaign_evidence_distribution_report(db, now=NOW, **kwargs),
    )
    assert campaign_evidence_distribution_script.main(["--campaign", "cli", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"]
