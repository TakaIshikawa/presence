"""Tests for source domain rotation gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.source_domain_rotation_gaps import (
    build_source_domain_rotation_gaps_report,
    build_source_domain_rotation_gaps_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_domain_rotation_gaps.py"
spec = importlib.util.spec_from_file_location("source_domain_rotation_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_flags_excessive_share_and_streak_with_alternatives():
    rows = [
        {"id": "c1", "created_at": "2026-04-01T00:00:00+00:00", "source_urls": ["https://a.example/1"]},
        {"id": "c2", "created_at": "2026-04-02T00:00:00+00:00", "source_urls": ["https://a.example/2"]},
        {"id": "c3", "created_at": "2026-04-03T00:00:00+00:00", "source_urls": ["https://b.example/1"]},
        {"id": "c4", "created_at": "2026-04-04T00:00:00+00:00", "source_urls": ["https://a.example/3"]},
        {"id": "c5", "created_at": "2026-04-05T00:00:00+00:00", "source_urls": ["https://a.example/4"]},
    ]

    report = build_source_domain_rotation_gaps_report(
        rows,
        [{"url": "https://b.example/source"}, {"domain": "c.example"}, {"domain": "a.example"}],
        window_size=5,
        max_rolling_share=0.5,
        now=NOW,
    )

    row = report["gaps"][0]
    assert row["domain"] == "a.example"
    assert row["recent_count"] == 4
    assert row["rolling_share"] == 0.8
    assert row["longest_streak"] == 2
    assert row["affected_content_ids"] == ["c1", "c2", "c4", "c5"]
    assert row["rotation_gap_score"] > 0
    assert row["candidate_alternative_domains"] == ["b.example", "c.example"]


def test_extracts_domains_from_newsletter_text_and_metadata():
    report = build_source_domain_rotation_gaps_report(
        [
            {"newsletter_id": "n1", "sent_at": "2026-04-01T00:00:00+00:00", "html": "Read https://www.same.test/a"},
            {"newsletter_id": "n2", "sent_at": "2026-04-02T00:00:00+00:00", "metadata": '{"source_domains":["same.test"]}'},
            {"newsletter_id": "n3", "sent_at": "2026-04-03T00:00:00+00:00", "citation_domains": "same.test"},
        ],
        window_size=3,
        max_rolling_share=0.7,
        now=NOW,
    )

    assert report["gaps"][0]["domain"] == "same.test"
    assert report["gaps"][0]["longest_streak"] == 3


def test_db_adapter_loads_generated_content_and_knowledge():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id TEXT, created_at TEXT, content TEXT, source_urls TEXT)")
    conn.execute("CREATE TABLE knowledge_sources (domain TEXT, url TEXT)")
    conn.execute("INSERT INTO generated_content VALUES ('c1', '2026-04-01T00:00:00+00:00', '', '[\"https://a.test/1\"]')")
    conn.execute("INSERT INTO generated_content VALUES ('c2', '2026-04-02T00:00:00+00:00', 'https://a.test/2', NULL)")
    conn.execute("INSERT INTO knowledge_sources VALUES ('b.test', NULL)")

    report = build_source_domain_rotation_gaps_report_from_db(conn, window_size=2, max_rolling_share=0.5, now=NOW)

    assert report["gaps"][0]["domain"] == "a.test"
    assert report["gaps"][0]["candidate_alternative_domains"] == ["b.test"]


def test_empty_state_and_invalid_filters():
    report = build_source_domain_rotation_gaps_report([], [], now=NOW)

    assert report["empty_state"]["is_empty"] is True
    with pytest.raises(ValueError):
        build_source_domain_rotation_gaps_report([], window_size=0)
    with pytest.raises(ValueError):
        build_source_domain_rotation_gaps_report([], max_rolling_share=0)
    with pytest.raises(ValueError):
        build_source_domain_rotation_gaps_report([], limit=0)


def test_cli_supports_json_and_text(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_source_domain_rotation_gaps_report_from_db",
        lambda _db, **kwargs: build_source_domain_rotation_gaps_report(
            [{"id": "c1", "source_urls": ["https://a.test/1"]}, {"id": "c2", "source_urls": ["https://a.test/2"]}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--window-size", "2", "--max-rolling-share", "0.5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "source_domain_rotation_gaps"
    assert script.main(["--table"]) == 0
    assert "domain | recent_count" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--max-rolling-share", "2"])
