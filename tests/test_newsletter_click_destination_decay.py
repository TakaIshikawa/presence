from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.newsletter_click_destination_decay import build_newsletter_click_destination_decay_report, build_newsletter_click_destination_decay_report_from_db, format_newsletter_click_destination_decay_json, format_newsletter_click_destination_decay_text

NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_click_destination_decay.py"
spec = importlib.util.spec_from_file_location("script_newsletter_click_destination_decay", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_compares_baseline_and_recent_windows_with_required_fields():
    rows = [
        {"url": "https://Example.com/a", "click_count": 90, "clicked_at": "2026-04-15T00:00:00+00:00"},
        {"url": "https://other.test/a", "click_count": 10, "clicked_at": "2026-04-15T00:00:00+00:00"},
        {"url": "https://example.com/new", "click_count": 5, "clicked_at": "2026-05-25T00:00:00+00:00"},
        {"url": "https://other.test/new", "click_count": 95, "clicked_at": "2026-05-25T00:00:00+00:00"},
        {"url": "https://example.com/too-old", "click_count": 999, "clicked_at": "2025-12-01T00:00:00+00:00"},
    ]
    report = build_newsletter_click_destination_decay_report(rows, recent_days=14, baseline_days=60, min_baseline_clicks=10, min_share_drop=0.2, now=NOW)
    finding = report["decays"][0]
    assert finding == {
        "domain": "example.com",
        "baseline_clicks": 90,
        "recent_clicks": 5,
        "baseline_share": 0.9,
        "recent_share": 0.05,
        "share_delta": -0.85,
        "decay_ratio": 0.0556,
        "newest_click_at": "2026-05-25T00:00:00+00:00",
    }


def test_normalizes_domains_filters_minimums_and_skips_bad_urls():
    rows = [
        {"url": "https://www.Example.com/a", "clicks": 9, "clicked_at": "2026-04-20"},
        {"url": "https://example.com/b", "clicks": 1, "clicked_at": "2026-04-21"},
        {"url": "not a url", "clicks": 5, "clicked_at": "2026-04-21"},
        {"url": "https://other.test/a", "clicks": 100, "clicked_at": "2026-04-21"},
        {"url": "https://other.test/b", "clicks": 100, "clicked_at": "2026-05-28"},
    ]
    low_min = build_newsletter_click_destination_decay_report(rows, recent_days=14, baseline_days=60, min_baseline_clicks=10, min_share_drop=0.01, domain=["www.example.com"], now=NOW)
    high_min = build_newsletter_click_destination_decay_report(rows, recent_days=14, baseline_days=60, min_baseline_clicks=11, min_share_drop=0.01, domain="example.com", now=NOW)
    assert [d["domain"] for d in low_min["decays"]] == ["example.com"]
    assert high_min["decays"] == []
    assert low_min["summary"]["skipped_url_count"] == 1


def test_db_fallback_and_cli_text(capsys, tmp_path):
    db = tmp_path / "db.sqlite"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE newsletter_clicks (destination_url TEXT, clicks INTEGER, date TEXT)")
    c.executemany("INSERT INTO newsletter_clicks VALUES (?,?,?)", [("https://example.com/a", 30, "2026-04-15"), ("https://other.test/a", 10, "2026-04-15"), ("https://example.com/b", 1, "2026-05-28"), ("https://other.test/b", 39, "2026-05-28")])
    c.commit()
    c.close()
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        report = build_newsletter_click_destination_decay_report_from_db(conn, recent_days=14, baseline_days=60, min_baseline_clicks=10, min_share_drop=0.2, now=NOW)
    assert report["decays"][0]["domain"] == "example.com"
    assert script.main(["--db", str(db), "--format", "text", "--recent-days", "14", "--baseline-days", "60", "--min-baseline-clicks", "10", "--min-share-drop", "0.2"]) == 0
    assert "example.com" in capsys.readouterr().out


def test_formatters_emit_json_and_empty_text():
    report = build_newsletter_click_destination_decay_report([], now=NOW)
    assert json.loads(format_newsletter_click_destination_decay_json(report))["artifact_type"] == "newsletter_click_destination_decay"
    assert "No newsletter click destination decay found." in format_newsletter_click_destination_decay_text(report)
