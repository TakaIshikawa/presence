"""Tests for auditing stored knowledge source redirect drift."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.source_redirect_audit import (
    audit_knowledge_source_redirects,
    domain_for_url,
    format_source_redirect_audit_json,
    format_source_redirect_audit_text,
    normalize_audit_url,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_knowledge_source_redirects.py"
spec = importlib.util.spec_from_file_location("audit_knowledge_source_redirects_cli", SCRIPT_PATH)
audit_knowledge_source_redirects_cli = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_knowledge_source_redirects_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _metadata(
    *,
    canonical_url: str,
    final_url: str | None = None,
) -> str:
    link_metadata = {"canonical_url": canonical_url}
    if final_url is not None:
        link_metadata["final_url"] = final_url
    return json.dumps({"link_metadata": link_metadata}, sort_keys=True)


def _insert_knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str = "article-1",
    source_url: str = "https://example.com/posts/article?utm_source=feed",
    canonical_url: str = "https://example.com/posts/article",
    final_url: str | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            attribution_required, license, approved, metadata)
           VALUES (?, ?, ?, 'Author', 'Content', 'Insight', 1, 'attribution_required', 1, ?)""",
        (
            source_type,
            source_id,
            source_url,
            _metadata(canonical_url=canonical_url, final_url=final_url),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _insert_curated_source(
    db,
    *,
    source_type: str = "blog",
    identifier: str = "example.com",
    feed_url: str = "https://example.com/feed?utm_medium=rss",
    canonical_url: str = "https://example.com/feed",
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, feed_url, canonical_url, status)
           VALUES (?, ?, 'Example', ?, ?, 'active')""",
        (source_type, identifier, feed_url, canonical_url),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_url_normalization_is_deterministic_for_tracking_case_and_domains():
    assert (
        normalize_audit_url("HTTPS://WWW.Example.com:443/path/?b=2&utm_source=x&a=1#section")
        == "https://www.example.com/path?a=1&b=2"
    )
    assert domain_for_url("https://www.Example.com/path") == "example.com"


def test_audit_classifies_domain_change_and_same_domain_cleanup_separately(db):
    domain_id = _insert_knowledge(
        db,
        source_id="domain-change",
        source_url="https://old.example.com/article?utm_campaign=x",
        canonical_url="https://old.example.com/article",
        final_url="https://new.example.org/article",
    )
    cleanup_id = _insert_curated_source(
        db,
        identifier="cleanup.example",
        feed_url="https://cleanup.example/rss/?utm_source=rss",
        canonical_url="https://cleanup.example/feed",
    )
    _insert_knowledge(
        db,
        source_id="tracking-only",
        source_url="https://same.example/post?utm_source=rss&a=1",
        canonical_url="https://same.example/post?a=1",
    )

    report = audit_knowledge_source_redirects(db, now=NOW)

    assert report.summary == {
        "scanned_count": 3,
        "finding_count": 2,
        "returned_count": 2,
        "domain_change_count": 1,
        "canonical_cleanup_count": 1,
    }
    by_key = {(finding.source_table, finding.row_id): finding for finding in report.findings}
    domain_finding = by_key[("knowledge", domain_id)]
    cleanup_finding = by_key[("curated_sources", cleanup_id)]
    assert domain_finding.classification == "domain_change_redirect"
    assert domain_finding.severity == "high"
    assert domain_finding.old_domain == "old.example.com"
    assert domain_finding.new_domain == "new.example.org"
    assert domain_finding.domain_changed is True
    assert cleanup_finding.classification == "canonical_cleanup"
    assert cleanup_finding.severity == "low"
    assert cleanup_finding.domain_changed is False


def test_domain_change_only_source_type_and_limit_filters(db):
    _insert_curated_source(db, identifier="blog.example")
    _insert_knowledge(
        db,
        source_id="newsletter-1",
        source_type="curated_newsletter",
        source_url="https://newsletter.example/item",
        canonical_url="https://newsletter.example/item",
        final_url="https://archive.example/item",
    )
    _insert_knowledge(
        db,
        source_id="article-1",
        source_url="https://article.example/item",
        canonical_url="https://article.example/item",
        final_url="https://mirror.example/item",
    )

    report = audit_knowledge_source_redirects(
        db,
        source_type="curated_newsletter",
        domain_change_only=True,
        limit=1,
        now=NOW,
    )

    assert report.filters == {
        "source_type": "curated_newsletter",
        "domain_change_only": True,
        "limit": 1,
    }
    assert report.summary["scanned_count"] == 1
    assert report.summary["finding_count"] == 1
    assert report.findings[0].source_id == "newsletter-1"


def test_json_and_text_output_are_stable(db):
    _insert_knowledge(
        db,
        source_id="domain-change",
        source_url="https://old.example.com/article",
        canonical_url="https://old.example.com/article",
        final_url="https://new.example.org/article",
    )

    report = audit_knowledge_source_redirects(db, source_type="knowledge", limit=5, now=NOW)
    payload = json.loads(format_source_redirect_audit_json(report))
    text = format_source_redirect_audit_text(report)

    assert payload["artifact_type"] == "knowledge_source_redirect_audit"
    assert payload["filters"] == {
        "domain_change_only": False,
        "limit": 5,
        "source_type": "knowledge",
    }
    assert payload["summary"]["domain_change_count"] == 1
    assert payload["findings"][0]["old_domain"] == "old.example.com"
    assert payload["findings"][0]["new_domain"] == "new.example.org"
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Knowledge source redirect audit" in text
    assert "domain_change_redirect" in text
    assert "old.example.com -> new.example.org" in text


def test_reads_sqlite_connection_without_database_wrapper():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT,
            source_url TEXT,
            metadata TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute(
        """INSERT INTO knowledge (source_type, source_id, source_url, metadata)
           VALUES ('curated_article', 'raw', 'https://old.example/a', ?)""",
        (_metadata(canonical_url="https://old.example/a", final_url="https://new.example/a"),),
    )
    conn.commit()

    report = audit_knowledge_source_redirects(conn, source_type="knowledge", now=NOW)

    assert len(report.findings) == 1
    assert report.findings[0].classification == "domain_change_redirect"


def test_cli_supports_requested_flags(db, capsys):
    _insert_knowledge(
        db,
        source_id="domain-change",
        source_url="https://old.example.com/article",
        canonical_url="https://old.example.com/article",
        final_url="https://new.example.org/article",
    )

    with patch.object(
        audit_knowledge_source_redirects_cli,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        audit_knowledge_source_redirects_cli,
        "audit_knowledge_source_redirects",
        wraps=lambda db, **kwargs: audit_knowledge_source_redirects(db, now=NOW, **kwargs),
    ):
        assert (
            audit_knowledge_source_redirects_cli.main(
                [
                    "--source-type",
                    "knowledge",
                    "--domain-change-only",
                    "--limit",
                    "5",
                    "--json",
                ]
            )
            == 0
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["source_type"] == "knowledge"
    assert payload["filters"]["domain_change_only"] is True
    assert payload["summary"]["finding_count"] == 1
