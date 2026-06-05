from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_image_alt_text_gaps import build_newsletter_image_alt_text_gaps_report, build_newsletter_image_alt_text_gaps_report_from_db, format_newsletter_image_alt_text_gaps_json, format_newsletter_image_alt_text_gaps_text

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_image_alt_text_gaps.py"
spec = importlib.util.spec_from_file_location("script_newsletter_image_alt_text_gaps", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_flags_html_missing_empty_generic_and_filename_like_alt_text():
    rows = [
        {
            "id": "draft-1",
            "subject": "Weekly",
            "html": '<img src="https://cdn.test/missing.png"><img src="https://cdn.test/empty.png" alt=""><img src="https://cdn.test/chart.png" alt="chart"><img src="https://cdn.test/product-shot.png" alt="product shot">',
            "updated_at": "2026-06-01T00:00:00+00:00",
        }
    ]
    report = build_newsletter_image_alt_text_gaps_report(rows)
    assert [gap["reason"] for gap in report["gaps"]] == ["missing_alt", "empty_alt", "generic_alt", "filename_like_alt"]
    assert report["gaps"][0]["draft_id"] == "draft-1"
    assert report["gaps"][0]["image_url"] == "https://cdn.test/missing.png"


def test_flags_markdown_generic_but_excludes_valid_alt_text():
    rows = [{"id": "campaign-1", "campaign_id": "campaign-1", "body": "![image](https://cdn.test/a.png)\n![Launch funnel diagram](https://cdn.test/b.png)"}]
    report = build_newsletter_image_alt_text_gaps_report(rows)
    assert len(report["gaps"]) == 1
    assert report["gaps"][0]["campaign_id"] == "campaign-1"
    assert report["gaps"][0]["reason"] == "generic_alt"
    assert report["summary"]["image_count"] == 2


def test_db_fallback_reads_draft_and_campaign_tables_and_cli_json(capsys, tmp_path):
    db = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE newsletter_drafts (id TEXT, subject TEXT, body TEXT, updated_at TEXT);
        CREATE TABLE newsletter_campaigns (campaign_id TEXT, title TEXT, html TEXT, updated_at TEXT);
        INSERT INTO newsletter_drafts VALUES ('d1', 'Draft', '![photo](https://cdn.test/p.png)', '2026-06-01');
        INSERT INTO newsletter_campaigns VALUES ('c1', 'Campaign', '<img src="https://cdn.test/c.png" alt="Campaign hero chart">', '2026-06-01');
        """
    )
    conn.commit()
    conn.close()
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        report = build_newsletter_image_alt_text_gaps_report_from_db(c)
    assert [gap["record_id"] for gap in report["gaps"]] == ["d1"]
    assert script.main(["--db", str(db)]) == 0
    assert json.loads(capsys.readouterr().out)["gaps"][0]["reason"] == "generic_alt"


def test_no_hit_cases_and_formatters():
    report = build_newsletter_image_alt_text_gaps_report([{"id": "ok", "html": '<img src="https://cdn.test/ok.png" alt="Launch cohort retention chart">'}])
    assert report["gaps"] == []
    assert "No newsletter image alt text gaps found." in format_newsletter_image_alt_text_gaps_text(report)
    assert json.loads(format_newsletter_image_alt_text_gaps_json(report))["artifact_type"] == "newsletter_image_alt_text_gaps"
