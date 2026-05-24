from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.newsletter_segment_growth_anomalies import build_newsletter_segment_growth_anomalies_report_from_db, format_newsletter_segment_growth_anomalies_json, format_newsletter_segment_growth_anomalies_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_segment_growth_anomalies.py"; spec=importlib.util.spec_from_file_location("script_newsletter_segment_growth_anomalies",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE newsletter_subscribers (id TEXT, subscribed_at TEXT, segment TEXT);")
    for i in range(6): c.execute("INSERT INTO newsletter_subscribers VALUES (?,?,?)",(f"r{i}","2026-05-23T00:00:00+00:00","founders"))
    c.execute("INSERT INTO newsletter_subscribers VALUES (?,?,?)",("b1","2026-05-10T00:00:00+00:00","founders")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_newsletter_segment_growth_anomalies_report_from_db(_db(),now=datetime(2026,5,25,tzinfo=timezone.utc))
    assert r["artifact_type"]=="newsletter_segment_growth_anomalies"; assert r["anomalies"]; assert r["anomalies"][0]["segment_id"]=="founders"
    assert {"segment_id","segment_name","recent_count","baseline_count","growth_delta","growth_ratio","recommended_action"}<=set(r["anomalies"][0])
    assert json.loads(format_newsletter_segment_growth_anomalies_json(r))["artifact_type"]=="newsletter_segment_growth_anomalies"; assert "Newsletter" in format_newsletter_segment_growth_anomalies_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--format","text","--window-days","7","--baseline-days","14"])==0; assert capsys.readouterr().out
    assert script.main(["--db",str(db),"--window-days","0"])==2
def test_missing_schema():
    r=build_newsletter_segment_growth_anomalies_report_from_db(sqlite3.connect(":memory:"))
    assert r["missing_tables"] or r["missing_columns"]
