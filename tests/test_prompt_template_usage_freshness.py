"""Tests for prompt template usage freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.prompt_template_usage_freshness import (
    build_prompt_template_usage_freshness_report,
    build_prompt_template_usage_freshness_report_from_db,
    format_prompt_template_usage_freshness_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "prompt_template_usage_freshness.py"
spec = importlib.util.spec_from_file_location("prompt_template_usage_freshness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_content_level_usage_gaps_are_classified():
    report = build_prompt_template_usage_freshness_report(
        [
            {"content_id": 1, "created_at": "2026-05-20T10:00:00+00:00", "prompt_template_id": None, "prompt_version_id": 10},
            {"content_id": 2, "created_at": "2026-05-20T10:00:00+00:00", "prompt_template_id": 1, "prompt_version_id": None},
            {"content_id": 3, "created_at": "2026-05-20T10:00:00+00:00", "prompt_template_id": 2, "prompt_version_id": 20, "template_status": "inactive"},
        ],
        [],
        now=NOW,
    )
    by_id = {row["content_id"]: row["reason_codes"] for row in report["content_usage_gaps"]}
    assert by_id[1] == ["missing_prompt_template_id"]
    assert by_id[2] == ["missing_prompt_version_id"]
    assert by_id[3] == ["inactive_template"]


def test_stale_unused_templates_and_active_recent_usage():
    report = build_prompt_template_usage_freshness_report(
        [
            {"content_id": 1, "created_at": "2026-05-18T10:00:00+00:00", "prompt_template_id": 1, "prompt_version_id": 10},
            {"content_id": 2, "created_at": "2026-04-01T10:00:00+00:00", "prompt_template_id": 2, "prompt_version_id": 20},
        ],
        [
            {"prompt_template_id": 1, "prompt_version_id": 10, "template_active": 1, "created_at": "2026-01-01T00:00:00+00:00"},
            {"prompt_template_id": 2, "prompt_version_id": 20, "template_active": 1, "created_at": "2026-01-01T00:00:00+00:00"},
            {"prompt_template_id": 3, "prompt_version_id": 30, "template_active": 0, "created_at": "2026-01-01T00:00:00+00:00"},
        ],
        stale_days=30,
        now=NOW,
    )

    assert report["content_usage_gaps"] == []
    assert [row["prompt_template_id"] for row in report["stale_unused_templates"]] == ["2"]


def test_db_loader_detects_inactive_missing_identifiers_and_stale_unused_template():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """CREATE TABLE prompt_templates (
               id INTEGER PRIMARY KEY,
               name TEXT,
               active INTEGER,
               status TEXT,
               created_at TEXT
           );
           CREATE TABLE prompt_versions (
               id INTEGER PRIMARY KEY,
               prompt_template_id INTEGER,
               version TEXT,
               active INTEGER,
               created_at TEXT
           );
           CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT,
               prompt_template_id INTEGER,
               prompt_version_id INTEGER,
               created_at TEXT
           );"""
    )
    conn.execute("INSERT INTO prompt_templates VALUES (1, 'active-template', 1, 'active', '2026-01-01T00:00:00+00:00')")
    conn.execute("INSERT INTO prompt_templates VALUES (2, 'inactive-template', 0, 'inactive', '2026-01-01T00:00:00+00:00')")
    conn.execute("INSERT INTO prompt_templates VALUES (3, 'unused-template', 1, 'active', '2026-01-01T00:00:00+00:00')")
    conn.execute("INSERT INTO prompt_versions VALUES (10, 1, '1', 1, '2026-01-01T00:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', 1, 10, '2026-05-20T10:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'x_post', 2, 20, '2026-05-20T10:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES (3, 'x_post', NULL, NULL, '2026-05-20T10:00:00+00:00')")
    conn.commit()

    report = build_prompt_template_usage_freshness_report_from_db(conn, lookback_days=7, stale_days=30, now=NOW)
    reasons = {row["content_id"]: row["reason_codes"] for row in report["content_usage_gaps"]}

    assert reasons[2] == ["inactive_template"]
    assert reasons[3] == ["missing_prompt_template_id", "missing_prompt_version_id"]
    assert {row["prompt_template_id"] for row in report["stale_unused_templates"]} >= {"3"}
    assert "Prompt Template Usage Freshness" in format_prompt_template_usage_freshness_text(report)


def test_cli_json_output(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_prompt_template_usage_freshness_report_from_db",
        lambda _db, **kwargs: build_prompt_template_usage_freshness_report(
            [{"content_id": 1, "created_at": "2026-05-20T10:00:00+00:00"}],
            [],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--lookback-days", "7", "--stale-days", "30", "--limit", "10", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "prompt_template_usage_freshness"
    assert payload["content_usage_gaps"][0]["reason_codes"] == ["missing_prompt_template_id", "missing_prompt_version_id"]
