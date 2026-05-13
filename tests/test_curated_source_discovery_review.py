"""Tests for curated source discovery review reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from knowledge.curated_source_discovery_review import (
    build_curated_source_discovery_review_report,
    format_curated_source_discovery_review_json,
    format_curated_source_discovery_review_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "curated_source_discovery_review.py"
spec = importlib.util.spec_from_file_location("curated_source_discovery_review_script", SCRIPT_PATH)
curated_source_discovery_review_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(curated_source_discovery_review_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _source(db, *, identifier: str, status: str, discovery: str | None, created: str, reviewed: str | None = None, source_type: str = "blog", license: str = "open") -> int:
    cursor = db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, status, discovery_source, relevance_score,
            reviewed_at, created_at, license)
           VALUES (?, ?, ?, ?, 0.8, ?, ?, ?)""",
        (source_type, identifier, status, discovery, reviewed, created, license),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_classifies_discovered_sources_by_review_state(db):
    needs = _source(db, identifier="new.example", status="candidate", discovery="search", created="2026-05-18T00:00:00+00:00")
    stale = _source(db, identifier="old.example", status="candidate", discovery="search", created="2026-04-01T00:00:00+00:00")
    reviewed = _source(db, identifier="ok.example", status="active", discovery="proactive_mining", created="2026-05-01T00:00:00+00:00", reviewed="2026-05-02T00:00:00+00:00")
    paused = _source(db, identifier="pause.example", status="paused", discovery="search", created="2026-05-01T00:00:00+00:00")
    rejected = _source(db, identifier="reject.example", status="rejected", discovery="search", created="2026-05-01T00:00:00+00:00")

    report = build_curated_source_discovery_review_report(db, stale_days=30, now=NOW)

    by_id = {item["source_id"]: item for item in report["items"]}
    assert by_id[needs]["review_status"] == "needs_review"
    assert by_id[stale]["review_status"] == "stale_candidate"
    assert by_id[reviewed]["review_status"] == "reviewed"
    assert by_id[paused]["review_status"] == "paused"
    assert by_id[rejected]["review_status"] == "rejected"
    assert by_id[stale]["age_bucket"] == "31-90d"


def test_default_filter_includes_discovery_or_candidate_and_excludes_config(db):
    included = _source(db, identifier="candidate.example", status="candidate", discovery=None, created="2026-05-18T00:00:00+00:00")
    _source(db, identifier="config.example", status="active", discovery="config", created="2026-05-18T00:00:00+00:00")
    _source(db, identifier="manual.example", status="active", discovery=None, created="2026-05-18T00:00:00+00:00")

    report = build_curated_source_discovery_review_report(db, now=NOW)

    assert [item["source_id"] for item in report["items"]] == [included]


def test_totals_include_source_type_status_discovery_and_license(db):
    _source(db, identifier="a.example", status="candidate", discovery="search", created="2026-05-18T00:00:00+00:00", source_type="blog", license="open")
    _source(db, identifier="b.example", status="rejected", discovery="proactive_mining", created="2026-05-18T00:00:00+00:00", source_type="newsletter", license="restricted")

    report = build_curated_source_discovery_review_report(db, now=NOW)

    assert report["totals"]["by_source_type"] == {"blog": 1, "newsletter": 1}
    assert report["totals"]["by_status"]["needs_review"] == 1
    assert report["totals"]["by_status"]["rejected"] == 1
    assert report["totals"]["by_discovery_source"] == {"proactive_mining": 1, "search": 1}
    assert report["totals"]["by_license"] == {"open": 1, "restricted": 1}


def test_json_text_and_cli(db, monkeypatch, capsys):
    _source(db, identifier="a.example", status="candidate", discovery="search", created="2026-05-18T00:00:00+00:00")
    report = build_curated_source_discovery_review_report(db, limit=1, now=NOW)
    assert len(report["items"]) == 1
    assert list(json.loads(format_curated_source_discovery_review_json(report)).keys()) == sorted(report.keys())
    assert "Curated Source Discovery Review" in format_curated_source_discovery_review_text(report)

    monkeypatch.setattr(curated_source_discovery_review_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        curated_source_discovery_review_script,
        "build_curated_source_discovery_review_report",
        lambda db, **kwargs: build_curated_source_discovery_review_report(db, now=NOW, **kwargs),
    )
    assert curated_source_discovery_review_script.main(["--stale-days", "14", "--limit", "5", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["stale_days"] == 14
    assert curated_source_discovery_review_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    assert build_curated_source_discovery_review_report(conn, now=NOW)["missing_tables"] == ["curated_sources"]
    with pytest.raises(ValueError, match="stale_days must be positive"):
        build_curated_source_discovery_review_report(conn, stale_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_curated_source_discovery_review_report(conn, limit=0, now=NOW)
    conn.close()
