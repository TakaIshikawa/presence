"""Tests for high-value uncited knowledge source reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.knowledge_high_value_uncited_sources import (
    build_knowledge_high_value_uncited_sources_report,
    build_knowledge_high_value_uncited_sources_report_from_db,
    format_knowledge_high_value_uncited_sources_json,
    format_knowledge_high_value_uncited_sources_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_high_value_uncited_sources.py"
spec = importlib.util.spec_from_file_location("knowledge_high_value_uncited_sources_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_metadata_metrics_and_recent_curated_sources_are_reported():
    report = build_knowledge_high_value_uncited_sources_report(
        [
            {
                "id": 1,
                "source_type": "curated_x",
                "author": "Ada",
                "content": "Useful thread about publishing",
                "metadata": {"public_metrics": {"like_count": 75}},
            },
            {
                "id": 2,
                "source_type": "curated_article",
                "author": "Bo",
                "content": "Recent long-form source",
                "published_at": "2026-04-20T00:00:00+00:00",
                "metadata": {"title": "Fresh Research"},
            },
            {
                "id": 3,
                "source_type": "curated_newsletter",
                "content": "Old source",
                "published_at": "2026-02-01T00:00:00+00:00",
                "metadata": {},
            },
        ],
        set(),
        now=NOW,
    )

    by_id = {row["knowledge_id"]: row for row in report["sources"]}
    assert sorted(by_id) == [1, 2]
    assert by_id[1]["high_value_reason"] == "likes>=50"
    assert by_id[1]["title_or_excerpt"] == "Useful thread about publishing"
    assert by_id[2]["title_or_excerpt"] == "Fresh Research"
    assert by_id[2]["high_value_reason"] == "recent_curated_article"


def test_db_adapter_excludes_cited_knowledge_and_json_is_deterministic():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE knowledge (
            id INTEGER, source_type TEXT, author TEXT, content TEXT, published_at TEXT, metadata TEXT
        )"""
    )
    conn.execute("CREATE TABLE content_knowledge_links (content_id INTEGER, knowledge_id INTEGER)")
    conn.execute(
        "INSERT INTO knowledge VALUES (1, 'curated_x', 'Ada', 'Highly liked source', NULL, ?)",
        (json.dumps({"likes": 100}),),
    )
    conn.execute(
        "INSERT INTO knowledge VALUES (2, 'curated_x', 'Bo', 'Already used source', NULL, ?)",
        (json.dumps({"bookmarks": 30}),),
    )
    conn.execute("INSERT INTO content_knowledge_links VALUES (10, 2)")

    report = build_knowledge_high_value_uncited_sources_report_from_db(conn, now=NOW)
    payload = json.loads(format_knowledge_high_value_uncited_sources_json(report))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "knowledge_high_value_uncited_sources"
    assert [row["knowledge_id"] for row in payload["sources"]] == [1]
    assert payload["totals"]["cited_knowledge_count"] == 1


def test_empty_state_cli_and_invalid_arguments(monkeypatch, capsys):
    report = build_knowledge_high_value_uncited_sources_report([], set(), now=NOW)
    assert "No high-value uncited knowledge sources found." in format_knowledge_high_value_uncited_sources_text(report)
    with pytest.raises(ValueError, match="metric thresholds"):
        build_knowledge_high_value_uncited_sources_report([], likes_threshold=-1)
    with pytest.raises(ValueError, match="recent_curated_days"):
        build_knowledge_high_value_uncited_sources_report([], recent_curated_days=0)
    with pytest.raises(ValueError, match="limit"):
        build_knowledge_high_value_uncited_sources_report([], limit=0)
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])

    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_knowledge_high_value_uncited_sources_report_from_db",
        lambda _db, **kwargs: build_knowledge_high_value_uncited_sources_report(
            [{"id": 1, "source_type": "curated_x", "content": "Source", "metadata": {"clicks": 200}}],
            set(),
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--format", "json", "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["sources"][0]["high_value_reason"] == "clicks>=100"
    assert script.main(["--table"]) == 0
    assert "knowledge_id | source_type" in capsys.readouterr().out
