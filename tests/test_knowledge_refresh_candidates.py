from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from knowledge.refresh_candidates import (
    build_knowledge_refresh_candidates_report,
    format_knowledge_refresh_candidates_json,
    format_knowledge_refresh_candidates_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_refresh_candidates.py"
spec = importlib.util.spec_from_file_location("knowledge_refresh_candidates_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _knowledge(db, *, metadata=None, ingested_days=1, embedding=b"x", approved=1) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, content, approved, metadata, embedding, ingested_at)
           VALUES ('curated_article', ?, ?, 'content', ?, ?, ?, ?)""",
        (
            f"k-{ingested_days}-{approved}",
            f"https://example.com/{ingested_days}",
            approved,
            json.dumps(metadata) if metadata is not None else None,
            embedding,
            (NOW - timedelta(days=ingested_days)).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_finds_missing_and_stale_knowledge_refresh_reasons(db):
    stale = _knowledge(db, metadata={}, ingested_days=80, embedding=None)
    fresh = _knowledge(
        db,
        metadata={
            "embedding_generated_at": NOW.isoformat(),
            "link_metadata_refreshed_at": NOW.isoformat(),
        },
        ingested_days=1,
    )

    report = build_knowledge_refresh_candidates_report(db, days=30, min_priority=20, now=NOW)
    by_id = {item["knowledge_id"]: item for item in report["candidates"]}

    assert stale in by_id
    assert "missing_metadata" in by_id[stale]["reason_codes"]
    assert "stale_ingested_at" in by_id[stale]["reason_codes"]
    assert "missing_embedding" in by_id[stale]["reason_codes"]
    assert fresh not in by_id
    assert report["totals"]["reason_counts"]["missing_metadata"] == 1


def test_stale_embedding_link_metadata_and_last_citation_are_reported(db):
    kid = _knowledge(
        db,
        metadata={
            "embedding_generated_at": (NOW - timedelta(days=45)).isoformat(),
            "link_metadata_refreshed_at": (NOW - timedelta(days=50)).isoformat(),
        },
        ingested_days=10,
    )
    cid = db.insert_generated_content("x_post", [], [], "draft", 7, "ok")
    db.insert_content_knowledge_links(cid, [(kid, 0.8)])
    db.conn.execute(
        "UPDATE content_knowledge_links SET created_at = ? WHERE knowledge_id = ?",
        ((NOW - timedelta(days=40)).isoformat(), kid),
    )
    db.conn.commit()

    report = build_knowledge_refresh_candidates_report(db, days=30, min_priority=20, now=NOW)
    item = report["candidates"][0]

    assert item["knowledge_id"] == kid
    assert item["reason_codes"] == [
        "stale_embedding_metadata",
        "stale_link_metadata",
        "stale_last_citation",
    ]


def test_min_priority_filter_formatters_and_cli(db, monkeypatch, capsys):
    kid = _knowledge(db, metadata={}, ingested_days=60, embedding=None)
    low = build_knowledge_refresh_candidates_report(db, min_priority=0, now=NOW)
    high = build_knowledge_refresh_candidates_report(db, min_priority=999, now=NOW)
    payload = json.loads(format_knowledge_refresh_candidates_json(low))
    text = format_knowledge_refresh_candidates_text(low)

    assert list(payload) == sorted(payload)
    assert high["candidates"] == []
    assert f"knowledge_id={kid}" in text

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_knowledge_refresh_candidates_report",
        lambda db, **kwargs: build_knowledge_refresh_candidates_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--days", "30", "--limit", "1", "--min-priority", "0", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["min_priority"] == 0
    assert cli_payload["filters"]["limit"] == 1
