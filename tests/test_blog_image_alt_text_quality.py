"""Tests for blog image alt text quality."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_image_alt_text_quality import build_blog_image_alt_text_quality_report_from_db


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_image_alt_text_quality.py"
spec = importlib.util.spec_from_file_location("blog_image_alt_text_quality_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_extracts_markdown_html_and_metadata_alt_text_findings(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE blog_posts (id TEXT, body TEXT, metadata TEXT, created_at TEXT)")
    conn.execute(
        "INSERT INTO blog_posts VALUES (?, ?, ?, ?)",
        (
            "b1",
            "![](missing.png) ![ok](short.png) ![hero image](hero-image.jpg) <img src='generic.png' alt='image'>",
            json.dumps({"images": [{"src": "dup1.png", "alt": "Same useful alt text"}, {"src": "dup2.png", "alt": "Same useful alt text"}]}),
            NOW.isoformat(),
        ),
    )
    db = SimpleNamespace(conn=conn)

    report = build_blog_image_alt_text_quality_report_from_db(db, now=NOW, min_chars=6)

    reasons = {item["reason_code"] for item in report["findings"]}
    assert {"missing", "too_short", "filename_like", "generic", "duplicated"}.issubset(reasons)
    assert report["summary"]["images_scanned"] == 6

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_blog_image_alt_text_quality_report_from_db",
        lambda db, **kwargs: build_blog_image_alt_text_quality_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json", "--min-chars", "6"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_image_alt_text_quality"
    assert script.main(["--table", "--min-chars", "6"]) == 0
    assert "reason=missing" in capsys.readouterr().out
