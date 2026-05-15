"""Tests for blog update staleness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_update_staleness import (
    build_blog_update_staleness_report,
    build_blog_update_staleness_report_from_db,
    format_blog_update_staleness_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_update_staleness.py"
spec = importlib.util.spec_from_file_location("blog_update_staleness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _post(post_id: str, title: str, *, updated_days: int | None, published_days: int, source_days: int) -> dict:
    return {
        "id": post_id,
        "title": title,
        "status": "published",
        "published_at": (NOW - timedelta(days=published_days)).isoformat(),
        "last_updated": (NOW - timedelta(days=updated_days)).isoformat() if updated_days is not None else None,
        "newest_source_at": (NOW - timedelta(days=source_days)).isoformat(),
    }


def test_ranks_posts_by_staleness_severity():
    rows = [
        _post("fresh", "Fresh", updated_days=5, published_days=10, source_days=3),
        _post("refresh", "Refresh", updated_days=100, published_days=120, source_days=80),
        _post("urgent", "Urgent", updated_days=200, published_days=220, source_days=10),
        _post("monitor", "Monitor", updated_days=35, published_days=40, source_days=20),
    ]

    report = build_blog_update_staleness_report(rows, now=NOW)

    assert [item["post_id"] for item in report["posts"]] == ["urgent", "refresh", "monitor", "fresh"]
    assert report["posts"][0]["classification"] == "urgent"
    assert report["totals"]["refresh_due"] == 1
    assert "class" in format_blog_update_staleness_text(report)


def test_configurable_thresholds_classify_posts():
    report = build_blog_update_staleness_report(
        [_post("a", "A", updated_days=20, published_days=20, source_days=20)],
        monitor_days=10,
        refresh_days=15,
        urgent_days=30,
        now=NOW,
    )

    assert report["posts"][0]["classification"] == "refresh_due"


def test_missing_update_timestamp_is_explicit():
    report = build_blog_update_staleness_report(
        [_post("missing", "Missing", updated_days=None, published_days=8, source_days=5)],
        now=NOW,
    )

    post = report["posts"][0]
    assert post["missing_update_timestamp"] is True
    assert post["classification"] == "monitor"
    assert "last_updated" in post["recommended_action"]


def test_db_loader_and_cli_json_output(db, monkeypatch, capsys):
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content="A stale blog post",
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        ((NOW - timedelta(days=100)).isoformat(), content_id),
    )
    db.conn.commit()

    report = build_blog_update_staleness_report_from_db(db, now=NOW)
    assert report["posts"][0]["post_id"] == str(content_id)
    assert report["posts"][0]["classification"] == "refresh_due"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_blog_update_staleness_report_from_db",
        lambda db, **kwargs: build_blog_update_staleness_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "blog_update_staleness"

    assert script.main(["--table"]) == 0
    assert "Blog Update Staleness" in capsys.readouterr().out
