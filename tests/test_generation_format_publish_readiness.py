"""Tests for generation format publish readiness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.generation_format_publish_readiness import (
    build_generation_format_publish_readiness_report,
    format_generation_format_publish_readiness_json,
    format_generation_format_publish_readiness_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generation_format_publish_readiness.py"
spec = importlib.util.spec_from_file_location("generation_format_publish_readiness_script", SCRIPT_PATH)
generation_format_publish_readiness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(generation_format_publish_readiness_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, content_type TEXT, content_format TEXT, created_at TEXT, evaluation_score REAL
        );
        CREATE TABLE content_publications (
            content_id INTEGER, status TEXT
        );"""
    )
    return conn


def _content(conn: sqlite3.Connection, content_id: int, fmt: str | None, score: float, status: str | None = None) -> None:
    conn.execute("INSERT INTO generated_content VALUES (?, 'x_post', ?, ?, ?)", (content_id, fmt, (NOW - timedelta(days=1)).isoformat(), score))
    if status is not None:
        conn.execute("INSERT INTO content_publications VALUES (?, ?)", (content_id, status))
    conn.commit()


def test_format_groups_rates_statuses_and_unknown_format():
    conn = _conn()
    _content(conn, 1, "short", 9.0, "published")
    _content(conn, 2, "short", 5.0, "failed")
    _content(conn, 3, None, 8.0, None)

    report = build_generation_format_publish_readiness_report(conn, now=NOW, min_eval_score=7)
    groups = {(g["content_type"], g["content_format"]): g for g in report["format_groups"]}

    assert groups[("x_post", "short")]["published_count"] == 1
    assert groups[("x_post", "short")]["failed_count"] == 1
    assert groups[("x_post", "unknown")]["missing_publication_count"] == 1
    assert report["totals"]["high_score_count"] == 2


def test_json_text_cli_and_schema_gaps(monkeypatch, capsys):
    conn = _conn()
    _content(conn, 1, "short", 9.0, "queued")
    report = build_generation_format_publish_readiness_report(conn, now=NOW)

    assert json.loads(format_generation_format_publish_readiness_json(report))["artifact_type"] == "generation_format_publish_readiness"
    assert "Generation Format Publish Readiness" in format_generation_format_publish_readiness_text(report)
    monkeypatch.setattr(generation_format_publish_readiness_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        generation_format_publish_readiness_script,
        "build_generation_format_publish_readiness_report",
        lambda db, **kwargs: build_generation_format_publish_readiness_report(db, now=NOW, **kwargs),
    )
    assert generation_format_publish_readiness_script.main(["--format", "text", "--min-eval-score", "8"]) == 0
    assert "Totals: generated=1" in capsys.readouterr().out

    missing = build_generation_format_publish_readiness_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["generated_content", "content_publications"]
