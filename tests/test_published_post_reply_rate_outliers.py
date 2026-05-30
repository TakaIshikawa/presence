from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.published_post_reply_rate_outliers import build_published_post_reply_rate_outliers_report, build_published_post_reply_rate_outliers_report_from_db, format_published_post_reply_rate_outliers_json, format_published_post_reply_rate_outliers_text
NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "published_post_reply_rate_outliers.py"
spec = importlib.util.spec_from_file_location("script_published_post_reply_rate_outliers", SCRIPT)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_baseline_minimum_sample_high_low_and_channel_filter():
    rows = [
        {"id": "base1", "channel": "x", "content_type": "post", "reply_count": 10, "impressions": 100},
        {"id": "base2", "channel": "x", "content_type": "post", "reply_count": 10, "impressions": 100},
        {"id": "high", "channel": "x", "content_type": "post", "reply_count": 40, "impressions": 100},
        {"id": "low", "channel": "x", "content_type": "post", "reply_count": 1, "impressions": 100},
        {"id": "other", "channel": "y", "content_type": "post", "reply_count": 99, "impressions": 100},
    ]
    report = build_published_post_reply_rate_outliers_report(rows, now=NOW, min_sample=3, ratio=2.0, channel="x")
    assert {o["post_id"] for o in report["outliers"]} == {"high", "low"}
    assert {o["direction"] for o in report["outliers"]} == {"high", "low"}
    guarded = build_published_post_reply_rate_outliers_report(rows[:2], now=NOW, min_sample=3)
    assert guarded["outliers"] == []
    assert json.loads(format_published_post_reply_rate_outliers_json(report))["artifact_type"] == "published_post_reply_rate_outliers"
    assert "Published Post Reply Rate Outliers" in format_published_post_reply_rate_outliers_text(report)

def test_db_builder_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE published_posts (id TEXT, channel TEXT, content_type TEXT, reply_count INTEGER, impressions INTEGER)")
    for row in [("p1","x","post",10,100),("p2","x","post",10,100),("p3","x","post",40,100),("p4","x","post",1,100)]:
        conn.execute("INSERT INTO published_posts VALUES (?, ?, ?, ?, ?)", row)
    report = build_published_post_reply_rate_outliers_report_from_db(conn, now=NOW, min_sample=3)
    assert report["outliers"]
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); conn.commit(); conn.backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text", "--min-sample", "3"]) == 0
    assert "Published Post Reply Rate Outliers" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--ratio", "0"]) == 2
