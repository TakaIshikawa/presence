"""Tests for generated content source diversity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.generated_content_source_diversity import (
    build_generated_content_source_diversity_report,
    build_generated_content_source_diversity_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generated_content_source_diversity.py"
spec = importlib.util.spec_from_file_location("generated_content_source_diversity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_classifies_multi_source_content():
    report = build_generated_content_source_diversity_report(
        [{"id": "c1", "source_activity_ids": "a1,a2", "source_commits": '["abc"]', "source_notes": "brief"}],
        now=NOW,
    )

    row = report["contents"][0]
    assert row["source_type_counts"] == {"activity": 2, "commit": 1, "url": 0, "note": 1}
    assert row["source_classification"] == "multi_source_type"
    assert report["totals"]["multi_source_type_rate"] == 1.0


def test_classifies_single_source_content():
    report = build_generated_content_source_diversity_report([{"id": "c1", "source_notes": "one note"}], now=NOW)

    assert report["contents"][0]["source_classification"] == "single_source_type"
    assert report["totals"]["single_source_type"] == 1


def test_embedded_urls_count_as_url_sources():
    report = build_generated_content_source_diversity_report(
        [{"id": "c1", "content": "Read https://example.com/a and https://example.com/b"}],
        now=NOW,
    )

    assert report["contents"][0]["source_type_counts"]["url"] == 2
    assert report["contents"][0]["source_classification"] == "single_source_type"


def test_empty_input_returns_empty_state():
    report = build_generated_content_source_diversity_report([], now=NOW)

    assert report["artifact_type"] == "generated_content_source_diversity"
    assert report["empty_state"]["is_empty"] is True
    assert report["totals"]["content_count"] == 0


def test_db_adapter_tolerates_missing_optional_columns():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO generated_content (id) VALUES (1)")

    report = build_generated_content_source_diversity_report_from_db(conn, now=NOW)

    assert report["totals"]["content_count"] == 1
    assert report["contents"][0]["source_classification"] == "no_source_type"


def test_cli_supports_json_text_table_and_rejects_invalid_limit(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_generated_content_source_diversity_report_from_db",
        lambda _db, **kwargs: build_generated_content_source_diversity_report([{"id": "c1", "source_urls": "https://example.com"}], now=NOW, **kwargs),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["content_count"] == 1
    assert script.main(["--format", "text"]) == 0
    assert "content_id | type" in capsys.readouterr().out
    assert script.main(["--table"]) == 0
    assert "Generated Content Source Diversity" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
