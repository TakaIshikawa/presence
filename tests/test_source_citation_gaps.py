"""Tests for knowledge source citation gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_citation_gaps import (
    build_source_citation_gap_report,
    format_source_citation_gap_json,
    format_source_citation_gap_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_citation_gaps.py"
spec = importlib.util.spec_from_file_location("source_citation_gaps_script", SCRIPT_PATH)
source_citation_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_citation_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _add_knowledge(
    db,
    *,
    source_id: str,
    days_ago: int,
    source_type: str = "curated_article",
    metadata: dict | None = None,
    approved: int = 1,
) -> int:
    metadata_value = json.dumps(
        metadata or {"title": source_id.title(), "trust_score": 0.5},
        sort_keys=True,
    )
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, published_at, ingested_at, metadata)
           VALUES (?, ?, ?, 'Ada', ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            f"https://example.test/{source_id}",
            f"content {source_id}",
            f"insight {source_id}",
            approved,
            _ts(days_ago),
            _ts(days_ago),
            metadata_value,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _content(db, knowledge_id: int, *, link_days_ago: int) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"post with knowledge {knowledge_id}",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    db.conn.execute(
        "UPDATE content_knowledge_links SET created_at = ? WHERE content_id = ?",
        (_ts(link_days_ago), content_id),
    )
    db.conn.commit()
    return int(content_id)


def test_unused_sources_are_reported_with_total_count_and_gap_entries(db):
    unused_id = _add_knowledge(
        db,
        source_id="unused",
        days_ago=40,
        metadata={"title": "Unused Source", "trust_score": 0.8},
    )
    own_id = _add_knowledge(db, source_id="own", days_ago=40, source_type="own_post")

    report = build_source_citation_gap_report(
        db,
        days=30,
        min_age_days=7,
        limit=10,
        now=NOW,
    )
    payload = json.loads(format_source_citation_gap_json(report))

    assert payload["artifact_type"] == "source_citation_gaps"
    assert report.total_source_count == 1
    assert tuple(gap.knowledge_id for gap in report.gaps) == (unused_id,)
    assert report.gaps[0].source_age_days == 40
    assert report.gaps[0].usage_count == 0
    assert own_id


def test_sources_cited_within_lookback_are_excluded(db):
    recent_id = _add_knowledge(db, source_id="recently-cited", days_ago=60)
    old_id = _add_knowledge(db, source_id="old-cited", days_ago=60)
    _content(db, recent_id, link_days_ago=3)
    _content(db, old_id, link_days_ago=45)

    report = build_source_citation_gap_report(db, days=30, min_age_days=7, now=NOW)

    assert [gap.knowledge_id for gap in report.gaps] == [old_id]
    assert report.gaps[0].usage_count == 1
    assert report.gaps[0].recent_usage_count == 0
    assert report.gaps[0].last_cited_at == _ts(45)


def test_stale_high_trust_zero_usage_sources_sort_first(db):
    low_trust_old = _add_knowledge(
        db,
        source_id="low-trust-old",
        days_ago=120,
        metadata={"title": "Low", "trust_score": 0.1},
    )
    high_trust_old = _add_knowledge(
        db,
        source_id="high-trust-old",
        days_ago=90,
        metadata={"title": "High", "quality_score": 0.95},
    )
    used_stale = _add_knowledge(
        db,
        source_id="used-stale",
        days_ago=140,
        metadata={"title": "Used", "trust_score": 0.95},
    )
    _content(db, used_stale, link_days_ago=80)

    report = build_source_citation_gap_report(db, days=30, min_age_days=7, now=NOW)

    assert [gap.knowledge_id for gap in report.gaps] == [
        high_trust_old,
        used_stale,
        low_trust_old,
    ]
    assert report.gaps[0].priority_score > report.gaps[1].priority_score
    assert report.gaps[1].usage_count == 1


def test_optional_usage_tables_missing_returns_conservative_empty_counts():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            source_id TEXT,
            content TEXT,
            approved INTEGER,
            published_at TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO knowledge
           (id, source_type, source_id, content, approved, published_at, metadata)
           VALUES (1, 'curated_article', 'missing-usage', 'content', 1, ?, ?)""",
        (_ts(30), json.dumps({"trust_score": 1.0})),
    )

    report = build_source_citation_gap_report(conn, days=14, min_age_days=7, now=NOW)

    assert report.usage_table_availability["content_knowledge_links"] is False
    assert report.gaps[0].knowledge_id == 1
    assert report.gaps[0].usage_count == 0
    assert report.gaps[0].last_cited_at is None


def test_newsletter_references_count_as_recent_citations(db):
    knowledge_id = _add_knowledge(db, source_id="newsletter-cited", days_ago=50)
    content_id = _content(db, knowledge_id, link_days_ago=80)
    db.conn.execute(
        """INSERT INTO newsletter_sends (subject, source_content_ids, sent_at)
           VALUES ('Issue', ?, ?)""",
        (json.dumps([content_id]), _ts(2)),
    )
    db.conn.commit()

    report = build_source_citation_gap_report(db, days=30, min_age_days=7, now=NOW)

    assert report.gaps == ()


def test_formatters_and_cli_flags_are_deterministic(db, monkeypatch, capsys):
    _add_knowledge(
        db,
        source_id="cli-source",
        days_ago=30,
        metadata={"title": "CLI Source", "trust_tier": "gold"},
    )
    monkeypatch.setattr(
        source_citation_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        source_citation_gaps_script,
        "build_source_citation_gap_report",
        lambda db, **kwargs: build_source_citation_gap_report(db, now=NOW, **kwargs),
    )

    assert source_citation_gaps_script.main(["--min-age-days", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    exit_code = source_citation_gaps_script.main(
        ["--days", "30", "--min-age-days", "7", "--limit", "5", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)
    text = format_source_citation_gap_text(
        build_source_citation_gap_report(db, days=30, min_age_days=7, limit=5, now=NOW)
    )

    assert exit_code == 0
    assert list(payload) == sorted(payload)
    assert payload["filters"] == {"days": 30, "limit": 5, "min_age_days": 7}
    assert payload["gaps"][0]["source_id"] == "cli-source"
    assert "Source Citation Gaps" in text
    assert "cli-source" in text or "CLI Source" in text
