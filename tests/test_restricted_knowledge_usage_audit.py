"""Tests for restricted knowledge usage audit reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from knowledge.restricted_usage_audit import (
    LICENSE_ATTRIBUTION_REQUIRED,
    LICENSE_RESTRICTED,
    REASON_MISSING_ATTRIBUTION,
    REASON_RESTRICTED,
    build_restricted_usage_audit_report,
    format_restricted_usage_audit_json,
    format_restricted_usage_audit_text,
    has_visible_attribution,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "audit_restricted_knowledge_usage.py"
)
spec = importlib.util.spec_from_file_location("audit_restricted_script", SCRIPT_PATH)
audit_restricted_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_restricted_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(
    db,
    content: str,
    *,
    content_type: str = "x_post",
    published: int = 0,
    created_at: str = "2026-05-01T10:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, created_at = ? WHERE id = ?",
        (published, created_at, content_id),
    )
    db.conn.commit()
    return content_id


def _insert_knowledge(
    db,
    *,
    license_value,
    source_url: str | None = "https://source.example/post",
    author: str | None = "Source Author",
) -> int:
    next_id = db.conn.execute("SELECT COUNT(*) + 1 FROM knowledge").fetchone()[0]
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"source-{next_id}-{license_value}-{source_url}",
            source_url,
            author,
            "Useful source context",
            license_value,
            1,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _link(db, content_id: int, knowledge_id: int, relevance: float = 0.9) -> None:
    db.insert_content_knowledge_links(content_id, [(knowledge_id, relevance)])


def test_reports_restricted_links_for_unpublished_content(db):
    content_id = _insert_content(db, "Draft based on private source notes.")
    knowledge_id = _insert_knowledge(db, license_value="restricted")
    _link(db, content_id, knowledge_id)

    report = build_restricted_usage_audit_report(db, now=NOW)

    assert report.finding_count == 1
    assert report.findings[0].as_dict() == {
        "content_id": content_id,
        "content_type": "x_post",
        "knowledge_id": knowledge_id,
        "source_url": "https://source.example/post",
        "author": "Source Author",
        "license": LICENSE_RESTRICTED,
        "reason": REASON_RESTRICTED,
    }


def test_attribution_required_links_report_only_without_visible_attribution(db):
    missing_content_id = _insert_content(db, "Draft repeats the source idea without credit.")
    credited_by_author_id = _insert_content(
        db,
        "Via Source Author: this draft credits the original source.",
    )
    credited_by_url_id = _insert_content(
        db,
        "Source: https://source.example/post for the original research.",
    )
    missing_knowledge_id = _insert_knowledge(db, license_value="attribution_required")
    author_knowledge_id = _insert_knowledge(
        db,
        license_value="attribution_required",
        source_url="https://source.example/author",
    )
    url_knowledge_id = _insert_knowledge(db, license_value="attribution_required")
    _link(db, missing_content_id, missing_knowledge_id)
    _link(db, credited_by_author_id, author_knowledge_id)
    _link(db, credited_by_url_id, url_knowledge_id)

    report = build_restricted_usage_audit_report(db, now=NOW)

    assert [(item.content_id, item.knowledge_id, item.reason) for item in report.findings] == [
        (missing_content_id, missing_knowledge_id, REASON_MISSING_ATTRIBUTION)
    ]
    assert has_visible_attribution(
        "H/T Source Author for the example.",
        author="Source Author",
    )
    assert has_visible_attribution(
        "Read more at source.example/post",
        source_url="https://source.example/post",
    )


def test_license_filter_limits_reported_findings(db):
    restricted_content_id = _insert_content(db, "Restricted source draft.")
    attribution_content_id = _insert_content(db, "Attribution source draft.")
    restricted_knowledge_id = _insert_knowledge(db, license_value=" restricted ")
    attribution_knowledge_id = _insert_knowledge(db, license_value="attribution-required")
    _link(db, restricted_content_id, restricted_knowledge_id)
    _link(db, attribution_content_id, attribution_knowledge_id)

    restricted_report = build_restricted_usage_audit_report(
        db,
        license_filter=LICENSE_RESTRICTED,
        now=NOW,
    )
    attribution_report = build_restricted_usage_audit_report(
        db,
        license_filter=LICENSE_ATTRIBUTION_REQUIRED,
        now=NOW,
    )

    assert [item.knowledge_id for item in restricted_report.findings] == [
        restricted_knowledge_id
    ]
    assert [item.knowledge_id for item in attribution_report.findings] == [
        attribution_knowledge_id
    ]


def test_published_filter_excludes_published_content_unless_included(db):
    unpublished_id = _insert_content(db, "Unpublished restricted draft.", published=0)
    published_id = _insert_content(db, "Published restricted post.", published=1)
    queued_published_id = _insert_content(db, "Queued retry restricted post.", published=1)
    unpublished_knowledge_id = _insert_knowledge(
        db,
        license_value="restricted",
        source_url="https://source.example/unpublished",
    )
    published_knowledge_id = _insert_knowledge(
        db,
        license_value="restricted",
        source_url="https://source.example/published",
    )
    queued_knowledge_id = _insert_knowledge(
        db,
        license_value="restricted",
        source_url="https://source.example/queued",
    )
    _link(db, unpublished_id, unpublished_knowledge_id)
    _link(db, published_id, published_knowledge_id)
    _link(db, queued_published_id, queued_knowledge_id)
    db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (queued_published_id, "2026-05-02T10:00:00+00:00", "x", "queued"),
    )
    db.conn.commit()

    default_report = build_restricted_usage_audit_report(db, now=NOW)
    include_report = build_restricted_usage_audit_report(
        db,
        include_published=True,
        now=NOW,
    )

    assert {item.content_id for item in default_report.findings} == {
        unpublished_id,
        queued_published_id,
    }
    assert {item.content_id for item in include_report.findings} == {
        unpublished_id,
        published_id,
        queued_published_id,
    }


def test_malformed_null_and_open_license_values_are_ignored(db):
    for license_value in (None, "", "unknown", "open"):
        content_id = _insert_content(db, f"Draft for {license_value!r}.")
        knowledge_id = _insert_knowledge(
            db,
            license_value=license_value,
            source_url=f"https://source.example/{license_value or 'null'}",
        )
        _link(db, content_id, knowledge_id)

    report = build_restricted_usage_audit_report(db, now=NOW)

    assert report.findings == []


def test_days_filter_and_formatters_are_stable(db):
    recent_content_id = _insert_content(db, "Recent restricted draft.")
    old_content_id = _insert_content(
        db,
        "Old restricted draft.",
        created_at="2026-03-01T10:00:00+00:00",
    )
    recent_knowledge_id = _insert_knowledge(
        db,
        license_value="restricted",
        source_url="https://source.example/recent",
    )
    old_knowledge_id = _insert_knowledge(
        db,
        license_value="restricted",
        source_url="https://source.example/old",
    )
    _link(db, recent_content_id, recent_knowledge_id)
    _link(db, old_content_id, old_knowledge_id)

    report = build_restricted_usage_audit_report(db, days=30, now=NOW)
    payload = json.loads(format_restricted_usage_audit_json(report))
    text = format_restricted_usage_audit_text(report)

    assert [item.knowledge_id for item in report.findings] == [recent_knowledge_id]
    assert payload["finding_count"] == 1
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Restricted Knowledge Usage Audit" in text
    assert f"knowledge #{recent_knowledge_id}" in text


def test_cli_supports_json_license_filter_and_include_published(db, monkeypatch, capsys):
    content_id = _insert_content(db, "Published missing attribution.", published=1)
    knowledge_id = _insert_knowledge(db, license_value="attribution_required")
    _link(db, content_id, knowledge_id)
    monkeypatch.setattr(
        audit_restricted_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = audit_restricted_script.main(
        [
            "--include-published",
            "--license",
            "attribution_required",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["license_filter"] == "attribution_required"
    assert payload["findings"][0]["content_id"] == content_id
    assert payload["findings"][0]["knowledge_id"] == knowledge_id
