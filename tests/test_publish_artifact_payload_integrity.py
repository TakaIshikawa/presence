"""Tests for publish artifact payload integrity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publish_artifact_payload_integrity import (
    build_publish_artifact_payload_integrity_report,
    format_publish_artifact_payload_integrity_json,
    format_publish_artifact_payload_integrity_text,
    parse_length_limit,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_artifact_payload_integrity.py"
spec = importlib.util.spec_from_file_location("publish_artifact_payload_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content TEXT
           );
           CREATE TABLE content_variants (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               variant_type TEXT,
               content TEXT,
               metadata TEXT,
               selected INTEGER DEFAULT 0
           );"""
    )
    conn.execute("INSERT INTO generated_content VALUES (1, 'hello world')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'newsletter body')")
    conn.commit()
    return conn


def _variant(
    conn: sqlite3.Connection,
    *,
    variant_id: int,
    content_id: int = 1,
    platform: str = "x",
    content: str = "hello world",
    payload: object = None,
    selected: int = 0,
) -> None:
    metadata = payload if isinstance(payload, str) else json.dumps({"payload": payload or {"text": content}})
    conn.execute(
        """INSERT INTO content_variants
           (id, content_id, platform, variant_type, content, metadata, selected)
           VALUES (?, ?, ?, 'post', ?, ?, ?)""",
        (variant_id, content_id, platform, content, metadata, selected),
    )
    conn.commit()


def test_clean_payload_has_no_findings():
    conn = _conn()
    _variant(conn, variant_id=1, payload={"text": "hello world", "platform": "x"}, selected=1)

    report = build_publish_artifact_payload_integrity_report(conn, now=NOW)

    assert report["findings"] == []
    assert "No publish artifact payload integrity issues" in format_publish_artifact_payload_integrity_text(report)


def test_all_integrity_gap_types_are_flagged():
    conn = _conn()
    _variant(conn, variant_id=1, payload="{bad json")
    _variant(conn, variant_id=2, payload={"platform": "x"})
    _variant(conn, variant_id=3, content="source text", payload={"text": "different text", "platform": "x"})
    _variant(conn, variant_id=4, content="short", payload={"text": "too long", "platform": "x"})
    _variant(conn, variant_id=5, content="hi", payload={"text": "hi", "platform": "linkedin"})

    report = build_publish_artifact_payload_integrity_report(conn, length_limits={"x": 5}, now=NOW)
    reasons = {row["artifact_id"]: row["reason_codes"] for row in report["findings"]}

    assert reasons["1"] == ["malformed_payload_json"]
    assert reasons["2"] == ["missing_required_platform_fields"]
    assert "payload_content_text_mismatch" in reasons["3"]
    assert "payload_exceeds_platform_length_limit" in reasons["4"]
    assert reasons["5"] == ["platform_conflicts_with_selected_variant"]
    assert report["summary"]["by_reason"]["malformed_payload_json"] == 1


def test_selected_variant_platform_conflict_and_platform_filtering():
    conn = _conn()
    _variant(conn, variant_id=1, platform="x", selected=1, payload={"text": "hello world", "platform": "x"})
    _variant(conn, variant_id=2, platform="linkedin", payload={"text": "hello world", "platform": "linkedin"})

    report = build_publish_artifact_payload_integrity_report(conn, platform="linkedin", now=NOW)

    assert [row["artifact_id"] for row in report["findings"]] == ["2"]
    assert report["findings"][0]["reason_codes"] == ["platform_conflicts_with_selected_variant"]
    assert report["filters"]["platform"] == "linkedin"


def test_publish_artifacts_table_is_supported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE content_variants (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               content TEXT
           );
           CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content TEXT
           );
           CREATE TABLE publish_artifacts (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               variant_id INTEGER,
               platform TEXT,
               payload_json TEXT
           );
           INSERT INTO generated_content VALUES (1, 'artifact text');
           INSERT INTO content_variants VALUES (10, 1, 'x', 'artifact text');
           INSERT INTO publish_artifacts VALUES (100, 1, 10, 'x', '{"text":"artifact text","platform":"x"}');"""
    )

    report = build_publish_artifact_payload_integrity_report(conn, now=NOW)

    assert report["findings"] == []
    assert report["summary"]["payload_rows_scanned"] == 1


def test_schema_gaps_and_json_are_deterministic():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE content_variants (id INTEGER PRIMARY KEY, platform TEXT)")

    report = build_publish_artifact_payload_integrity_report(conn, now=NOW)
    payload = json.loads(format_publish_artifact_payload_integrity_json(report))

    assert list(payload) == sorted(payload)
    assert payload["schema_gaps"]["missing_columns"] == {"content_variants": ["content", "content_id"]}
    assert payload["findings"] == []


def test_cli_json_text_limits_and_validation(monkeypatch, capsys):
    conn = _conn()
    _variant(conn, variant_id=1, payload={"text": "too long", "platform": "x"})
    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_publish_artifact_payload_integrity_report",
        lambda db, **kwargs: build_publish_artifact_payload_integrity_report(db, now=NOW, **kwargs),
    )

    assert script.main(["--platform", "x", "--length-limit", "x:5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"][0]["artifact_id"] == "1"

    assert script.main(["--table"]) == 0
    assert "Publish Artifact Payload Integrity" in capsys.readouterr().out

    assert parse_length_limit("x:12") == ("x", 12)
    with pytest.raises(ValueError, match="platform:limit"):
        parse_length_limit("x")
    assert script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
