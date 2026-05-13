"""Tests for visual asset usage balance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.visual_asset_usage_balance import (
    build_visual_asset_usage_balance_report,
    format_visual_asset_usage_balance_json,
    format_visual_asset_usage_balance_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "visual_asset_usage_balance.py"
spec = importlib.util.spec_from_file_location("visual_asset_usage_balance_script", SCRIPT_PATH)
visual_asset_usage_balance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(visual_asset_usage_balance_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str, image_path: str | None, days_ago: int = 0) -> int:
    content_id = db.insert_generated_content(content_type, [], [], "content", 8.0, "ok", image_path=image_path)
    db.conn.execute("UPDATE generated_content SET created_at = ? WHERE id = ?", ((NOW - timedelta(days=days_ago)).isoformat(), content_id))
    db.conn.commit()
    return int(content_id)


def test_flags_reuse_cooldown_low_coverage_and_missing_assets(db):
    _content(db, "blog_post", "/assets/a.png", 3)
    _content(db, "blog_post", "/assets/a.png", 2)
    _content(db, "blog_post", "/assets/a.png", 1)
    _content(db, "blog_post", None, 1)
    _content(db, "newsletter", None, 1)
    _content(db, "newsletter", None, 1)

    report = build_visual_asset_usage_balance_report(db, reuse_window_days=7, now=NOW)
    kinds = {finding.finding_type for finding in report.findings}

    assert {"over_reused_asset", "cooldown_reuse", "low_visual_coverage", "missing_asset_identifier"} <= kinds
    assert report.totals["content_count"] == 6


def test_formatters_missing_schema_and_cli_validation(db, monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing = build_visual_asset_usage_balance_report(conn, now=NOW)
    assert missing.schema_warnings == ("missing table: generated_content",)

    assert visual_asset_usage_balance_script.main(["--days", "0"]) == 2


def test_cli_outputs_json(db, monkeypatch, capsys):
    _content(db, "blog_post", None)
    monkeypatch.setattr(visual_asset_usage_balance_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        visual_asset_usage_balance_script,
        "build_visual_asset_usage_balance_report",
        lambda db, **kwargs: build_visual_asset_usage_balance_report(db, now=NOW, **kwargs),
    )
    assert visual_asset_usage_balance_script.main(["--reuse-window-days", "3", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "visual_asset_usage_balance"
    assert "Visual Asset Usage Balance" in format_visual_asset_usage_balance_text(build_visual_asset_usage_balance_report(db, now=NOW))
    assert json.loads(format_visual_asset_usage_balance_json(build_visual_asset_usage_balance_report(db, now=NOW)))["findings"]
