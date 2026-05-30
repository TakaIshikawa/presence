from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.publication_attempt_success_after_retry import build_publication_attempt_success_after_retry_report, build_publication_attempt_success_after_retry_report_from_db, format_publication_attempt_success_after_retry_json, format_publication_attempt_success_after_retry_text

NOW=datetime(2026,5,24,12,tzinfo=timezone.utc)
SCRIPT_PATH=Path(__file__).resolve().parent.parent/"scripts"/"publication_attempt_success_after_retry.py"
spec=importlib.util.spec_from_file_location("publication_attempt_success_after_retry_script",SCRIPT_PATH); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_finds_success_after_failure_and_sorts_by_attempts_then_elapsed():
    rows=[
        {"id":1,"content_id":"a","platform":"x","attempted_at":"2026-05-01T00:00:00+00:00","success":0,"error_category":"rate_limit"},
        {"id":2,"content_id":"a","platform":"x","attempted_at":"2026-05-01T02:00:00+00:00","success":1},
        {"id":3,"content_id":"b","platform":"x","attempted_at":"2026-05-01T00:00:00+00:00","status":"failed","error_category":"auth"},
        {"id":4,"content_id":"b","platform":"x","attempted_at":"2026-05-01T01:00:00+00:00","status":"failed","error_category":"timeout"},
        {"id":5,"content_id":"b","platform":"x","attempted_at":"2026-05-01T05:00:00+00:00","status":"published"},
        {"id":6,"content_id":"c","platform":"blog","attempted_at":"2026-05-01T00:00:00+00:00","status":"published"},
        {"id":7,"content_id":"d","platform":"blog","attempted_at":"2026-05-01T00:00:00+00:00","status":"failed"},
    ]
    r=build_publication_attempt_success_after_retry_report(rows,now=NOW)
    assert [f["content_id"] for f in r["findings"]]==["b","a"]
    assert r["findings"][0]["attempts_before_success"]==2
    assert r["findings"][0]["elapsed_hours"]==5.0
    assert r["findings"][0]["prior_error_categories"]=={"auth":1,"timeout":1}

def test_db_formatters_cli_and_schema(tmp_path,capsys):
    conn=sqlite3.connect(":memory:"); conn.execute("CREATE TABLE publication_attempts (id INTEGER, content_id TEXT, platform TEXT, attempted_at TEXT, status TEXT, error_category TEXT)")
    conn.executemany("INSERT INTO publication_attempts VALUES (?,?,?,?,?,?)",[(1,"a","x","2026-05-01T00:00:00+00:00","failed","auth"),(2,"a","x","2026-05-01T03:00:00+00:00","published",None)])
    r=build_publication_attempt_success_after_retry_report_from_db(conn,now=NOW)
    assert r["findings"][0]["content_id"]=="a"
    assert json.loads(format_publication_attempt_success_after_retry_json(r))["artifact_type"]=="publication_attempt_success_after_retry"
    assert "Publication Attempt Success After Retry" in format_publication_attempt_success_after_retry_text(r)
    db=tmp_path/"pub.sqlite"; conn.commit(); out=sqlite3.connect(db); conn.backup(out); out.close()
    assert script.main(["--db",str(db),"--limit","1"])==0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"]==1
    assert script.main(["--db",str(db),"--format","text"])==0
    assert "Publication Attempt Success After Retry" in capsys.readouterr().out
    assert build_publication_attempt_success_after_retry_report_from_db(sqlite3.connect(":memory:"),now=NOW)["missing_tables"]==["publication_attempts"]
