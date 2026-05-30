from __future__ import annotations

import sqlite3

from evaluation.newsletter_archive_broken_links import (
    build_newsletter_archive_broken_links_report,
    build_newsletter_archive_broken_links_report_from_db,
    format_newsletter_archive_broken_links_json,
    format_newsletter_archive_broken_links_text,
)


def test_flags_bad_status_codes_from_supplied_crawl_results():
    report = build_newsletter_archive_broken_links_report(
        [{"archive_id": "a1", "archive_url": "https://n.test/a1", "canonical_url": "https://n.test/a1", "content": '<a href="https://n.test/missing">x</a>'}],
        [{"url": "https://n.test/missing", "status_code": 404}],
    )
    assert report["issues"][0]["issue_type"] == "bad_crawl_status"
    assert report["issues"][0]["status"] == 404


def test_flags_missing_canonical_urls():
    report = build_newsletter_archive_broken_links_report([{"archive_id": "a1", "archive_url": "https://n.test/a1", "content": ""}])
    assert report["issues"][0]["issue_type"] == "missing_canonical_url"
    assert report["issues"][0]["recommendation"]


def test_flags_broken_internal_archive_links():
    rows = [
        {"archive_id": "a1", "archive_url": "https://n.test/a1", "canonical_url": "https://n.test/a1", "content": "[old](https://n.test/old)"},
        {"archive_id": "a2", "archive_url": "https://n.test/a2", "canonical_url": "https://n.test/a2", "content": ""},
    ]
    report = build_newsletter_archive_broken_links_report(rows)
    assert any(i["issue_type"] == "broken_internal_archive_link" and i["url"] == "https://n.test/old" for i in report["issues"])


def test_clean_archives_and_formatters():
    rows = [{"archive_id": "a1", "archive_url": "https://n.test/a1", "canonical_url": "https://n.test/a1", "content": "Read https://external.test/page"}]
    report = build_newsletter_archive_broken_links_report(rows, [{"url": "https://external.test/page", "status_code": 200}])
    assert report["issues"] == []
    assert "newsletter_archive_broken_links" in format_newsletter_archive_broken_links_json(report)
    assert "No newsletter archive" in format_newsletter_archive_broken_links_text(report)


def test_db_reads_archives_and_crawl_results():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE newsletter_archives (id TEXT, archive_url TEXT, canonical_url TEXT, html TEXT)")
    conn.execute("CREATE TABLE newsletter_archive_crawl_results (url TEXT, status_code INTEGER, ok INTEGER)")
    conn.execute("INSERT INTO newsletter_archives VALUES ('a1', 'https://n.test/a1', 'https://n.test/a1', '<a href=\"https://n.test/bad\">bad</a>')")
    conn.execute("INSERT INTO newsletter_archive_crawl_results VALUES ('https://n.test/bad', 500, 0)")
    report = build_newsletter_archive_broken_links_report_from_db(conn)
    assert report["issues"][0]["status"] == 500
