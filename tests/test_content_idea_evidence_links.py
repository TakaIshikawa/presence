"""Tests for content idea evidence link audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from synthesis.content_idea_evidence_links import (
    build_content_idea_evidence_link_report,
    format_content_idea_evidence_link_json,
    format_content_idea_evidence_link_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_evidence_links.py"
spec = importlib.util.spec_from_file_location("content_idea_evidence_links_script", SCRIPT_PATH)
content_idea_evidence_links_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_evidence_links_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _idea(
    db,
    *,
    topic: str = "testing",
    priority: str = "normal",
    status: str = "open",
    source: str = "manual",
    source_metadata: dict | None = None,
) -> int:
    return db.add_content_idea(
        "A seed idea",
        topic=topic,
        priority=priority,
        status=status,
        source=source,
        source_metadata=source_metadata,
    )


def test_open_ideas_with_no_evidence_url_are_flagged(db):
    idea_id = _idea(db, source_metadata={"source": "scratchpad"})

    report = build_content_idea_evidence_link_report(db, now=NOW)

    assert len(report.findings) == 1
    finding = report.findings[0]
    assert finding.idea_id == idea_id
    assert finding.topic == "testing"
    assert finding.priority == "normal"
    assert finding.source == "manual"
    assert finding.extracted_urls == ()
    assert finding.reasons == ("no_evidence_url",)


@pytest.mark.parametrize(
    "source_metadata",
    [
        {"url": "https://example.com/a"},
        {"source_url": "https://example.com/source"},
        {"evidence_url": "https://example.com/evidence"},
        {"evidence_urls": ["https://example.com/one", "https://example.com/two"]},
        {"links": [{"url": "https://example.com/link"}]},
        {"references": [{"href": "https://example.com/reference"}]},
        {"notes": "See https://example.com/plain-text for details."},
    ],
)
def test_supported_metadata_shapes_with_valid_urls_are_not_flagged(db, source_metadata):
    _idea(db, source_metadata=source_metadata)

    report = build_content_idea_evidence_link_report(db, now=NOW)

    assert report.findings == ()
    assert report.totals["rows_scanned"] == 1


def test_invalid_duplicate_and_social_profile_urls_are_reported(db):
    invalid_id = _idea(db, topic="invalid", source_metadata={"url": "not-a-url"})
    duplicate_id = _idea(
        db,
        topic="duplicate",
        source_metadata={
            "evidence_urls": [
                "https://example.com/post",
                "https://example.com/post/",
            ]
        },
    )
    social_id = _idea(db, topic="social", source_metadata={"url": "https://x.com/presence"})
    _idea(db, topic="deep social", source_metadata={"url": "https://x.com/presence/status/1"})

    report = build_content_idea_evidence_link_report(db, now=NOW)
    by_id = {finding.idea_id: finding for finding in report.findings}

    assert by_id[invalid_id].reasons == ("invalid_url", "no_evidence_url")
    assert by_id[invalid_id].invalid_urls == ("not-a-url",)
    assert by_id[duplicate_id].reasons == ("duplicate_url",)
    assert by_id[duplicate_id].duplicate_urls == ("https://example.com/post/",)
    assert by_id[social_id].reasons == ("social_profile_homepage", "no_evidence_url")
    assert "https://x.com/presence/status/1" not in {
        url for finding in report.findings for url in finding.extracted_urls
    }


def test_malformed_source_metadata_is_reported_without_raising(db):
    idea_id = _idea(db, source_metadata={"url": "https://example.com/good"})
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        ("not-json", idea_id),
    )
    db.conn.commit()

    report = build_content_idea_evidence_link_report(db, now=NOW)

    assert len(report.findings) == 1
    assert report.findings[0].idea_id == idea_id
    assert report.findings[0].reasons == ("malformed_metadata", "no_evidence_url")


def test_status_priority_limit_and_formats(db):
    first_id = _idea(db, priority="high", source_metadata=None)
    _idea(db, priority="low", source_metadata=None)
    _idea(db, priority="high", status="dismissed", source_metadata=None)

    report = build_content_idea_evidence_link_report(
        db,
        status="open",
        priority="high",
        limit=1,
        now=NOW,
    )
    payload = json.loads(format_content_idea_evidence_link_json(report))
    text = format_content_idea_evidence_link_text(report)

    assert [finding.idea_id for finding in report.findings] == [first_id]
    assert payload["artifact_type"] == "content_idea_evidence_links"
    assert payload["filters"]["priority"] == "high"
    assert payload["totals"]["rows_scanned"] == 1
    assert payload["findings"][0]["created_at"]
    assert "idea_id=" in text
    assert "no_evidence_url=1" in text


def test_cli_supports_status_priority_limit_and_json(db, monkeypatch, capsys):
    _idea(db, priority="high", source_metadata=None)
    _idea(db, priority="low", source_metadata=None)
    monkeypatch.setattr(
        content_idea_evidence_links_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        content_idea_evidence_links_script,
        "build_content_idea_evidence_link_report",
        lambda db, **kwargs: build_content_idea_evidence_link_report(db, now=NOW, **kwargs),
    )

    assert content_idea_evidence_links_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    assert (
        content_idea_evidence_links_script.main(
            ["--status", "open", "--priority", "high", "--limit", "1", "--format", "json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["status"] == "open"
    assert payload["filters"]["priority"] == "high"
    assert payload["totals"]["rows_scanned"] == 1
    assert payload["findings"][0]["priority"] == "high"
