"""Tests for newsletter link click conflict reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_link_click_conflicts import (
    build_newsletter_link_click_conflicts_report,
    build_newsletter_link_click_conflicts_report_from_db,
    format_newsletter_link_click_conflicts_json,
    format_newsletter_link_click_conflicts_text,
    normalize_newsletter_click_url,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_click_conflicts.py"
spec = importlib.util.spec_from_file_location("newsletter_link_click_conflicts_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_url_normalization_is_deterministic_without_network_calls():
    assert normalize_newsletter_click_url("HTTP://Example.COM/Path/?utm_source=x&b=2&a=1#frag") == "https://example.com/Path?a=1&b=2"
    assert normalize_newsletter_click_url("example.com/path/") == "https://example.com/path"


def test_builder_detects_attribution_and_destination_conflicts():
    report = build_newsletter_link_click_conflicts_report(
        [
            {"newsletter_send_id": 1, "issue_id": "i1", "link_url": "https://e.test/a?utm_source=n", "content_id": 10, "clicks": 5},
            {"newsletter_send_id": 1, "issue_id": "i1", "link_url": "http://e.test/a/", "content_id": 11, "clicks": 3},
            {"newsletter_send_id": 1, "issue_id": "i1", "link_url": "https://e.test/b", "content_id": 10, "clicks": 2},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "newsletter_link_click_conflicts"
    assert report["attribution_conflicts"][0]["content_ids"] == ["10", "11"]
    assert report["attribution_conflicts"][0]["click_total"] == 8
    assert report["destination_conflicts"][0]["content_id"] == "10"
    assert report["issue_summary"][0]["click_total"] == 10


def test_db_loader_missing_schema_and_cli(tmp_path, capsys):
    empty = sqlite3.connect(":memory:")
    assert build_newsletter_link_click_conflicts_report_from_db(empty, now=NOW)["missing_schema"]["missing_tables"] == ["newsletter_link_clicks"]

    db_path = tmp_path / "clicks.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE newsletter_link_clicks (id INTEGER PRIMARY KEY, newsletter_send_id INTEGER, issue_id TEXT, link_url TEXT, content_id INTEGER, clicks INTEGER)")
    conn.execute("INSERT INTO newsletter_link_clicks VALUES (?, ?, ?, ?, ?, ?)", (1, 1, "i1", "https://e.test/a", 1, 4))
    conn.execute("INSERT INTO newsletter_link_clicks VALUES (?, ?, ?, ?, ?, ?)", (2, 1, "i1", "https://e.test/a?utm_medium=email", 2, 5))
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["attribution_conflicts"][0]["click_total"] == 9
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Link Click Conflicts" in capsys.readouterr().out


def test_formatters_and_invalid_arguments():
    report = build_newsletter_link_click_conflicts_report([], now=NOW)
    assert json.loads(format_newsletter_link_click_conflicts_json(report))["artifact_type"] == "newsletter_link_click_conflicts"
    assert "Attribution conflicts" in format_newsletter_link_click_conflicts_text(report)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_newsletter_link_click_conflicts_report([], limit=0)
