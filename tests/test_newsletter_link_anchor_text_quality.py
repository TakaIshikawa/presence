from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.newsletter_link_anchor_text_quality import (
    build_newsletter_link_anchor_text_quality_report,
    build_newsletter_link_anchor_text_quality_report_from_db,
    format_newsletter_link_anchor_text_quality_json,
    format_newsletter_link_anchor_text_quality_text,
)

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_link_anchor_text_quality.py"
spec = importlib.util.spec_from_file_location("newsletter_link_anchor_text_quality_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_flags_generic_bare_long_mismatch_and_duplicate_anchor_domains():
    rows = [
        {"issue_id": "n2", "link_url": "https://example.com/a", "anchor_text": "click here"},
        {"issue_id": "n1", "link_url": "https://alpha.test/a", "anchor_text": "Partner"},
        {"issue_id": "n1", "link_url": "https://beta.test/b", "anchor_text": "Partner"},
        {"issue_id": "n1", "link_url": "https://real.test", "anchor_text": "https://real.test"},
        {"issue_id": "n1", "link_url": "https://target.test", "anchor_text": "Read this on other.test"},
        {"issue_id": "n1", "link_url": "https://long.test", "anchor_text": "A" * 90},
    ]
    report = build_newsletter_link_anchor_text_quality_report(rows)
    codes = {(row["link_url"], row["issue_code"]) for row in report["issues"]}
    assert ("https://example.com/a", "generic_anchor") in codes
    assert ("https://real.test", "bare_url_anchor") in codes
    assert ("https://target.test", "anchor_domain_mismatch") in codes
    assert ("https://long.test", "overly_long_anchor") in codes
    assert ("https://alpha.test/a", "duplicate_anchor_cross_domain") in codes
    assert [row["issue_id"] for row in report["issues"]] == sorted(row["issue_id"] for row in report["issues"])


def test_formatters_and_db_cli(tmp_path):
    db = tmp_path / "links.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE newsletter_link_inventory (issue_id TEXT, url TEXT, link_text TEXT)")
    conn.execute("INSERT INTO newsletter_link_inventory VALUES ('i1', 'https://example.com', 'read more')")
    conn.commit()
    report = build_newsletter_link_anchor_text_quality_report_from_db(conn)
    assert report["issues"][0]["issue_code"] == "generic_anchor"
    payload = json.loads(format_newsletter_link_anchor_text_quality_json(report))
    text = format_newsletter_link_anchor_text_quality_text(report)
    assert payload["artifact_type"] == "newsletter_link_anchor_text_quality"
    assert "Newsletter Link Anchor Text Quality" in text
    assert script.main(["--db", str(db), "--format", "text"]) == 0
