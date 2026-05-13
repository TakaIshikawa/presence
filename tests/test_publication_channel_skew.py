"""Tests for publication channel skew reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publication_channel_skew import (
    build_publication_channel_skew_report,
    format_publication_channel_skew_json,
    format_publication_channel_skew_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_channel_skew.py"
spec = importlib.util.spec_from_file_location("publication_channel_skew_script", SCRIPT_PATH)
publication_channel_skew_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_channel_skew_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str) -> int:
    content_id = db.insert_generated_content(content_type, [], [], "content", 8.0, "ok")
    db.conn.execute("UPDATE generated_content SET created_at = ? WHERE id = ?", (NOW.isoformat(), content_id))
    db.conn.commit()
    return int(content_id)


def test_computes_status_and_actions_from_generated_and_publication_rows(db):
    blog = _content(db, "blog_post")
    _content(db, "x_post")
    _content(db, "x_post")
    db.conn.execute(
        "INSERT INTO content_publications (content_id, platform, status, updated_at, published_at) VALUES (?, 'blog', 'published', ?, ?)",
        (blog, NOW.isoformat(), NOW.isoformat()),
    )
    db.conn.commit()

    report = build_publication_channel_skew_report(db, target={"blog": 0.2, "x_post": 0.8}, now=NOW)
    rows = {row.channel: row for row in report.channels}

    assert rows["blog"].published_count == 1
    assert rows["blog"].status == "overused"
    assert rows["x_post"].status == "underused"


def test_json_text_limit_missing_schema_and_bad_cli_target(db, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_publication_channel_skew_report(conn, now=NOW)
    assert missing.schema_warnings == ("missing table: generated_content",)

    assert publication_channel_skew_script.main(["--target", "not-json"]) == 2
    assert "target must be a JSON object" in capsys.readouterr().err


def test_cli_outputs_json(db, monkeypatch, capsys):
    _content(db, "newsletter")
    monkeypatch.setattr(publication_channel_skew_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        publication_channel_skew_script,
        "build_publication_channel_skew_report",
        lambda db, **kwargs: build_publication_channel_skew_report(db, now=NOW, **kwargs),
    )
    assert publication_channel_skew_script.main(["--target", '{"newsletter": 1}', "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "publication_channel_skew"
    assert "Publication Channel Skew" in format_publication_channel_skew_text(build_publication_channel_skew_report(db, now=NOW))
    assert json.loads(format_publication_channel_skew_json(build_publication_channel_skew_report(db, now=NOW)))["channels"]
