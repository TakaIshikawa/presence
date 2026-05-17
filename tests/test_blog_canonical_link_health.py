"""Tests for blog canonical link health reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_canonical_link_health import (
    build_blog_canonical_link_health_report,
    format_blog_canonical_link_health_json,
    format_blog_canonical_link_health_table,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_canonical_link_health.py"
spec = importlib.util.spec_from_file_location("blog_canonical_link_health_script", SCRIPT_PATH)
blog_canonical_link_health_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_canonical_link_health_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _blog(db, content: str) -> int:
    return db.insert_generated_content("blog_post", [], [], content, 8.0, "ok")


def test_valid_and_missing_canonicals_are_reported(db):
    valid = _blog(db, "canonical_url: https://example.com/blog/valid\n# Valid")
    missing = _blog(db, "# Missing")

    report = build_blog_canonical_link_health_report(db, site_base_url="https://example.com", now=NOW)
    payload = {post["content_id"]: post for post in json.loads(format_blog_canonical_link_health_json(report))["posts"]}

    assert payload[valid]["canonical_status"] == "healthy"
    assert payload[valid]["canonical_url"] == "https://example.com/blog/valid"
    assert payload[valid]["issue_codes"] == []
    assert payload[missing]["issue_codes"] == ["missing_canonical_url"]


def test_malformed_duplicate_and_wrong_site_canonicals_are_flagged_separately(db):
    malformed = _blog(db, "canonical_url: /relative-url\n# Malformed")
    duplicate_a = _blog(db, "canonical_url: https://example.com/blog/shared\n# A")
    duplicate_b = _blog(db, "canonical_url: https://EXAMPLE.com/blog/shared/\n# B")
    wrong_site = _blog(db, "canonical_url: https://other.example/blog/post\n# Wrong")

    report = build_blog_canonical_link_health_report(db, site_base_url="https://example.com", now=NOW)
    payload = {post.content_id: post.to_dict() for post in report.posts}

    assert payload[malformed]["issue_codes"] == ["malformed_canonical_url"]
    assert payload[duplicate_a]["issue_codes"] == ["duplicate_canonical_url"]
    assert payload[duplicate_a]["duplicate_group"] == f"{duplicate_a},{duplicate_b}"
    assert payload[duplicate_b]["issue_codes"] == ["duplicate_canonical_url"]
    assert payload[wrong_site]["issue_codes"] == ["wrong_site_canonical_url"]


def test_table_and_cli_output(db, monkeypatch, capsys):
    content_id = _blog(db, "canonical: https://example.com/blog/cli\n# CLI")
    report = build_blog_canonical_link_health_report(db, site_base_url="https://example.com", now=NOW)
    assert f"{content_id} | healthy" in format_blog_canonical_link_health_table(report)

    monkeypatch.setattr(blog_canonical_link_health_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        blog_canonical_link_health_script,
        "build_blog_canonical_link_health_report",
        lambda db, **kwargs: build_blog_canonical_link_health_report(db, now=NOW, **kwargs),
    )
    assert blog_canonical_link_health_script.main(["--site-base-url", "https://example.com", "--format", "table"]) == 0
    assert "Blog Canonical Link Health" in capsys.readouterr().out
